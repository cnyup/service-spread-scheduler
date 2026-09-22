//go:build integration

// End-to-end admission tests against a real (envtest) API server:
// CRD schema + ValidatingWebhookConfiguration as shipped in config/, the
// webhook served in-process exactly like cmd/webhook wires it.
//
// Run with: make test-integration   (needs KUBEBUILDER_ASSETS)
package webhook_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	sspv1alpha1 "gitlab.ttyuyin.com/T5428/service-spread-scheduler/api/v1alpha1"
	"gitlab.ttyuyin.com/T5428/service-spread-scheduler/internal/webhook"
)

var (
	testEnv   *envtest.Environment
	testNS    = "policy-tests"
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("SKIP: KUBEBUILDER_ASSETS is not set; run `make envtest-binaries` first")
		os.Exit(0)
	}
	ctrl.SetLogger(zap.New(zap.WriteTo(testWriter{}), zap.UseDevMode(true)))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "webhook")},
		},
	}

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "starting envtest: %v\n", err)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sspv1alpha1.AddToScheme(scheme))

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}

	// Namespace + the shared config ConfigMap the webhook (and scheduler)
	// read cluster-level configuration from.
	ctx := context.Background()
	if err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}}); err != nil {
		fmt.Fprintf(os.Stderr, "namespace: %v\n", err)
		os.Exit(1)
	}
	sharedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: webhook.SharedConfigMapName},
		Data: map[string]string{
			"serviceLabelKey":      "app.kubernetes.io/name",
			"managedSchedulerName": "service-spread-scheduler",
		},
	}
	if err := k8sClient.Create(ctx, sharedCM); err != nil {
		fmt.Fprintf(os.Stderr, "configmap: %v\n", err)
		os.Exit(1)
	}

	// In-process webhook server, same wiring as cmd/webhook/main.go.
	webhookOpts := testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: crwebhook.NewServer(crwebhook.Options{
			Host:    webhookOpts.LocalServingHost,
			Port:    webhookOpts.LocalServingPort,
			CertDir: webhookOpts.LocalServingCertDir,
		}),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "manager: %v\n", err)
		os.Exit(1)
	}
	configSrc, err := webhook.NewConfigMapSource(cfg, testNS, webhook.SharedConfigMapName, ctrl.Log.WithName("test-config"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "config source: %v\n", err)
		os.Exit(1)
	}
	if err := mgr.Add(configSrc); err != nil {
		fmt.Fprintf(os.Stderr, "add config source: %v\n", err)
		os.Exit(1)
	}
	if err := webhook.RegisterPolicyWebhook(mgr, configSrc); err != nil {
		fmt.Fprintf(os.Stderr, "register webhook: %v\n", err)
		os.Exit(1)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			fmt.Fprintf(os.Stderr, "manager exited: %v\n", err)
		}
	}()

	// Wait for the webhook server to accept connections.
	addr := fmt.Sprintf("%s:%d", webhookOpts.LocalServingHost, webhookOpts.LocalServingPort)
	if err := waitUntilTCP(addr, 15*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "webhook server never came up: %v\n", err)
		os.Exit(1)
	}

	// Wait for the apiserver to activate the freshly installed webhook
	// configuration: admission configs propagate asynchronously, and requests
	// issued in the first moments would silently bypass the webhook. Probe
	// with a deliberately mis-named (otherwise valid) policy until admission
	// starts rejecting it.
	probe := envtestPolicy("ssp-activation-probe-000", "webhook-activation-probe", nil)
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		err := k8sClient.Create(ctx, probe.DeepCopy())
		return err != nil && apierrors.IsInvalid(err), nil
	}); err != nil {
		fmt.Fprintf(os.Stderr, "webhook never became active at the apiserver\n")
		testEnv.Stop()
		os.Exit(1)
	}

	code := m.Run()

	cancel()
	testEnv.Stop()
	os.Exit(code)
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return os.Stderr.Write(p) }

func waitUntilTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out dialing %s", addr)
}

func envtestPolicy(name, labelValue string, mutate func(*sspv1alpha1.ServiceSpreadPolicy)) *sspv1alpha1.ServiceSpreadPolicy {
	if name == "" {
		name = webhook.ExpectedPolicyName(testNS, "service-spread-scheduler", "app.kubernetes.io/name", labelValue)
	}
	p := &sspv1alpha1.ServiceSpreadPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Spec: sspv1alpha1.ServiceSpreadPolicySpec{
			SchedulerName: "service-spread-scheduler",
			ServiceSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": labelValue},
			},
			MaxSkew:        1,
			MaxPodsPerNode: 9,
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func TestEnvtestWebhookAdmission(t *testing.T) {
	ctx := context.Background()

	t.Run("valid policy admitted", func(t *testing.T) {
		if err := k8sClient.Create(ctx, envtestPolicy("", "image-processing", nil)); err != nil {
			t.Fatalf("create: %v", err)
		}
	})

	t.Run("deterministic name mismatch rejected with expected name", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("my-policy", "frontend", nil))
		assertInvalid(t, err, "deterministic name")
		expected := webhook.ExpectedPolicyName(testNS, "service-spread-scheduler", "app.kubernetes.io/name", "frontend")
		if err != nil && !strings.Contains(err.Error(), expected) {
			t.Errorf("error should name the expected deterministic name %q: %v", expected, err)
		}
	})

	t.Run("selector extra key rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "backend", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.ServiceSelector.MatchLabels["tier"] = "prod"
		}))
		assertInvalid(t, err, "exactly one entry")
	})

	t.Run("selector missing rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "worker", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.ServiceSelector.MatchLabels = nil
		}))
		assertInvalid(t, err, "exactly one entry")
	})

	t.Run("matchExpressions rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "cache", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.ServiceSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{
				Key: "app.kubernetes.io/name", Operator: metav1.LabelSelectorOpIn, Values: []string{"cache"},
			}}
		}))
		assertInvalid(t, err, "matchExpressions")
	})

	t.Run("schedulerName mismatch rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "gateway", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.SchedulerName = "default-scheduler"
		}))
		assertInvalid(t, err, "managed scheduler name")
	})

	t.Run("maxSkew zero rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "metrics", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.MaxSkew = 0
		}))
		if err == nil {
			t.Fatal("expected rejection")
		}
	})

	t.Run("maxPodsPerNode zero rejected", func(t *testing.T) {
		err := k8sClient.Create(ctx, envtestPolicy("", "logging", func(p *sspv1alpha1.ServiceSpreadPolicy) {
			p.Spec.MaxPodsPerNode = 0
		}))
		if err == nil {
			t.Fatal("expected rejection")
		}
	})

	t.Run("schema requires maxSkew field", func(t *testing.T) {
		// Sent as unstructured with the field genuinely absent: the
		// structural schema (Required) rejects it before any webhook runs,
		// proving the schema-level constraint is in the shipped CRD.
		raw := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "scheduling.soyup.top/v1alpha1",
			"kind":       "ServiceSpreadPolicy",
			"metadata": map[string]interface{}{
				"name":      "ssp-aaaaaaaaaaaa",
				"namespace": testNS,
			},
			"spec": map[string]interface{}{
				"schedulerName": "service-spread-scheduler",
				"serviceSelector": map[string]interface{}{
					"matchLabels": map[string]interface{}{"app.kubernetes.io/name": "audit"},
				},
				"maxPodsPerNode": 5,
			},
		}}
		err := k8sClient.Create(ctx, raw)
		if err == nil || !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid from structural schema, got %v", err)
		}
		if !strings.Contains(err.Error(), "maxSkew") {
			t.Errorf("error should mention maxSkew: %v", err)
		}
	})

	t.Run("valid update admitted", func(t *testing.T) {
		p := envtestPolicy("", "updatable", nil)
		if err := k8sClient.Create(ctx, p); err != nil {
			t.Fatalf("create: %v", err)
		}
		p.Spec.MaxSkew = 2
		if err := k8sClient.Update(ctx, p); err != nil {
			t.Fatalf("update: %v", err)
		}
	})

	t.Run("identity change on update rejected", func(t *testing.T) {
		p := envtestPolicy("", "frozen", nil)
		if err := k8sClient.Create(ctx, p); err != nil {
			t.Fatalf("create: %v", err)
		}
		p.Spec.SchedulerName = "some-other-scheduler"
		assertInvalid(t, k8sClient.Update(ctx, p), "managed scheduler name")
	})
}

// The webhook must reject everything while the shared config is unusable, and
// recover automatically once the ConfigMap becomes valid again.
func TestEnvtestFailClosedOnBadSharedConfig(t *testing.T) {
	ctx := context.Background()

	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: webhook.SharedConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	original := cm.DeepCopy()

	// Break the config: no serviceLabelKey anymore.
	cm.Data = map[string]string{"managedSchedulerName": "service-spread-scheduler"}
	if err := k8sClient.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}

	policy := envtestPolicy("", "during-outage", nil)
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, false, func(ctx context.Context) (bool, error) {
		lastErr = k8sClient.Create(ctx, policy.DeepCopy())
		return lastErr != nil && strings.Contains(lastErr.Error(), "failing closed"), nil
	})
	if err != nil {
		t.Fatalf("webhook did not fail closed while config was broken; last error: %v", lastErr)
	}

	// Restore the config: the webhook must recover without a restart.
	cm.Data = original.Data
	if err := k8sClient.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, false, func(ctx context.Context) (bool, error) {
		lastErr = k8sClient.Create(ctx, envtestPolicy("", "after-recovery", nil))
		return lastErr == nil, nil
	})
	if err != nil {
		t.Fatalf("webhook did not recover after config restore; last error: %v", lastErr)
	}
}

func assertInvalid(t *testing.T, err error, wantSub string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected rejection containing %q, got success", wantSub)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid error, got: %v", err)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("error %q does not contain %q", err, wantSub)
	}
}
