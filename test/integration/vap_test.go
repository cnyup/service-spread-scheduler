//go:build integration

// Behavioral tests for the shipped CEL ValidatingAdmissionPolicy that blocks
// presetting spec.nodeName on pods using the managed scheduler
// (config/manager/vap-block-preset-nodename.yaml, design doc 3.2 /
// dev-design §7.2). The manifest itself is applied to the envtest API server
// so the CEL that ships is the CEL that is tested, on Kubernetes 1.28
// (v1beta1 + feature gate, exactly as documented in the manifest header).
//
// Run with: make test-integration   (needs KUBEBUILDER_ASSETS)
package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

const managedScheduler = "service-spread-scheduler"

var (
	kubeClient *kubernetes.Clientset
	dynClient  dynamic.Interface
	testNS     = "vap-tests"
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("SKIP: KUBEBUILDER_ASSETS is not set; run `make envtest-binaries` first")
		os.Exit(0)
	}

	env := &envtest.Environment{
		ControlPlane: envtest.ControlPlane{
			APIServer: &envtest.APIServer{
				Args: []string{
					// VAP is beta and off by default on 1.28; the shipped
					// manifest documents these exact flags for real clusters.
					"--feature-gates=ValidatingAdmissionPolicy=true",
					"--runtime-config=admissionregistration.k8s.io/v1beta1=true",
				},
			},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "starting envtest: %v\n", err)
		os.Exit(1)
	}
	defer env.Stop()

	kubeClient, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "clientset: %v\n", err)
		os.Exit(1)
	}
	dynClient, err = dynamic.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dynamic client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if _, err := kubeClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNS},
	}, metav1.CreateOptions{}); err != nil {
		fmt.Fprintf(os.Stderr, "namespace: %v\n", err)
		os.Exit(1)
	}

	// A pre-policy bound pod: represents the existing fleet when the VAP is
	// first rolled out. Routine updates to it (nodeName present but
	// unchanged) must keep working — that is the oldObject-comparison branch.
	if _, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("legacy-bound", managedScheduler, "node-a"), metav1.CreateOptions{}); err != nil {
		fmt.Fprintf(os.Stderr, "legacy pod: %v\n", err)
		os.Exit(1)
	}

	if err := applyVAPManifest(ctx, filepath.Join("..", "..", "config", "manager", "vap-block-preset-nodename.yaml")); err != nil {
		fmt.Fprintf(os.Stderr, "applying VAP manifest: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}

// applyVAPManifest installs every document of the multi-doc YAML through the
// dynamic client, preserving the apiVersion/kind it ships with.
func applyVAPManifest(ctx context.Context, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, doc := range strings.Split(string(raw), "\n---") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		u := &unstructured.Unstructured{}
		if err := yaml.Unmarshal([]byte(doc), &u.Object); err != nil {
			return fmt.Errorf("parsing document: %w", err)
		}
		gvr := schema.GroupVersionResource{
			Group:    u.GroupVersionKind().Group,
			Version:  u.GroupVersionKind().Version,
			Resource: pluralFor(u.GetKind()),
		}
		if _, err := dynClient.Resource(gvr).Create(ctx, u, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("creating %s %s: %w", u.GetKind(), u.GetName(), err)
		}
	}
	return nil
}

func pluralFor(kind string) string {
	switch kind {
	case "ValidatingAdmissionPolicy":
		return "validatingadmissionpolicies"
	case "ValidatingAdmissionPolicyBinding":
		return "validatingadmissionpolicybindings"
	default:
		return strings.ToLower(kind) + "s"
	}
}

func pod(name string, schedulerName, nodeName string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
	}
	if schedulerName != "" {
		p.Spec.SchedulerName = schedulerName
	}
	if nodeName != "" {
		p.Spec.NodeName = nodeName
	}
	return p
}

// waitVAPActive polls with a violating pod until the policy takes effect
// (CEL compilation/activation is asynchronous).
func waitVAPActive(ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("vap-probe", managedScheduler, "node-x"), metav1.CreateOptions{})
		if err == nil {
			// Policy not active yet; delete the probe and keep waiting.
			_ = kubeClient.CoreV1().Pods(testNS).Delete(ctx, "vap-probe", metav1.DeleteOptions{})
			return false, nil
		}
		return apierrors.IsForbidden(err) || apierrors.IsInvalid(err), nil
	})
}

func TestVAPBlocksPresetNodeName(t *testing.T) {
	ctx := context.Background()
	if err := waitVAPActive(ctx); err != nil {
		t.Fatalf("VAP never became active: %v", err)
	}

	t.Run("create managed pod with nodeName denied", func(t *testing.T) {
		_, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("managed-bound", managedScheduler, "node-a"), metav1.CreateOptions{})
		if err == nil {
			t.Fatal("expected denial")
		}
		if !strings.Contains(err.Error(), "bypasses") {
			t.Fatalf("expected the policy message, got: %v", err)
		}
	})

	t.Run("create managed pod without nodeName allowed", func(t *testing.T) {
		if _, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("managed-free", managedScheduler, ""), metav1.CreateOptions{}); err != nil {
			t.Fatalf("unexpected denial: %v", err)
		}
	})

	t.Run("create default-scheduler pod with nodeName allowed", func(t *testing.T) {
		if _, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("manual-bound", "default-scheduler", "node-a"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("manual scheduling must stay legal: %v", err)
		}
	})

	t.Run("create schedulerless pod with nodeName allowed", func(t *testing.T) {
		// Mirror-pod style: no schedulerName at all.
		if _, err := kubeClient.CoreV1().Pods(testNS).Create(ctx, pod("mirror", "", "node-a"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("unexpected denial: %v", err)
		}
	})

	t.Run("update setting nodeName denied", func(t *testing.T) {
		p, err := kubeClient.CoreV1().Pods(testNS).Get(ctx, "managed-free", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Spec.NodeName = "node-b"
		_, err = kubeClient.CoreV1().Pods(testNS).Update(ctx, p, metav1.UpdateOptions{})
		// Core Kubernetes already rejects nodeName changes via UPDATE
		// (updatablePodSpecFields allowlist), so the denial may come from
		// core validation instead of the VAP; what matters is that
		// self-binding through the pod object is impossible either way.
		// The VAP's UPDATE coverage stays as defense-in-depth.
		if err == nil {
			t.Fatal("self-binding via update must be denied")
		}
	})

	t.Run("update keeping nodeName allowed", func(t *testing.T) {
		p, err := kubeClient.CoreV1().Pods(testNS).Get(ctx, "legacy-bound", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// Routine update on an already-bound managed pod: nodeName present
		// but unchanged — must NOT trip the policy (oldObject comparison).
		p.Labels = map[string]string{"touched": "true"}
		if _, err := kubeClient.CoreV1().Pods(testNS).Update(ctx, p, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update keeping nodeName must be allowed: %v", err)
		}
	})

	t.Run("update changing nodeName denied", func(t *testing.T) {
		p, err := kubeClient.CoreV1().Pods(testNS).Get(ctx, "legacy-bound", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Spec.NodeName = "node-z"
		// See "update setting nodeName denied": the denial may come from
		// core validation (field allowlist) rather than the VAP.
		_, err = kubeClient.CoreV1().Pods(testNS).Update(ctx, p, metav1.UpdateOptions{})
		if err == nil {
			t.Fatal("changing nodeName via update must be denied")
		}
	})
}
