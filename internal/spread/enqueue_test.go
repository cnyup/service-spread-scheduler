package spread

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func hintPlugin(t *testing.T) *ServiceSpread {
	t.Helper()
	pl, _, _ := newTestPlugin(t, nil)
	return pl
}

func TestEventsToRegisterShape(t *testing.T) {
	pl := hintPlugin(t)
	evts := pl.EventsToRegister()
	if len(evts) != 5 {
		t.Fatalf("events = %d, want 5 (pod/node/policy/rs/dep)", len(evts))
	}
	want := map[string]bool{
		string(framework.Pod): false, string(framework.Node): false,
		string(policyEventGVK): false, string(replicaSetEventGVK): false,
		string(deploymentEventGVK): false,
	}
	for _, e := range evts {
		if _, ok := want[string(e.Event.Resource)]; !ok {
			t.Fatalf("unexpected event resource %q", e.Event.Resource)
		}
		want[string(e.Event.Resource)] = true
	}
	for r, seen := range want {
		if !seen {
			t.Fatalf("missing event resource %q", r)
		}
	}
	// Pod and Node actions must include Delete (design §5.7).
	for _, e := range evts {
		if e.Event.Resource == framework.Pod || e.Event.Resource == framework.Node {
			if e.Event.ActionType&framework.Delete == 0 {
				t.Fatalf("%s must register Delete", e.Event.Resource)
			}
		}
	}
}

func TestPodHintQueueSkip(t *testing.T) {
	pl := hintPlugin(t)
	log := klog.Background()
	pending := schedPod("p1", "alpha")

	// Same service, same domain → Queue.
	same := schedPod("e1", "alpha")
	same.Spec.NodeName = "n1"
	if got := pl.podHint(log, pending, nil, same); got != framework.QueueAfterBackoff {
		t.Fatalf("same-service pod event = %v, want Queue", got)
	}

	// Same service, different domain (nodeSelector differs) → still same
	// quotaKey → Queue (quota is shared across domains).
	otherDomain := schedPod("e2", "alpha")
	otherDomain.Spec.NodeName = "n1"
	otherDomain.Spec.NodeSelector = map[string]string{"zone": "z9"}
	if got := pl.podHint(log, pending, nil, otherDomain); got != framework.QueueAfterBackoff {
		t.Fatalf("same quotaKey event = %v, want Queue", got)
	}

	// Different service → QueueSkip.
	diff := schedPod("e3", "beta")
	diff.Spec.NodeName = "n1"
	if got := pl.podHint(log, pending, nil, diff); got != framework.QueueSkip {
		t.Fatalf("other service = %v, want QueueSkip", got)
	}

	// Unmanaged scheduler → QueueSkip.
	unmanaged := schedPod("e4", "alpha")
	unmanaged.Spec.SchedulerName = "default-scheduler"
	if got := pl.podHint(log, pending, nil, unmanaged); got != framework.QueueSkip {
		t.Fatalf("unmanaged = %v, want QueueSkip", got)
	}

	// Delete event (newObj nil, oldObj carries the pod).
	deleted := schedPod("e5", "alpha")
	if got := pl.podHint(log, pending, deleted, nil); got != framework.QueueAfterBackoff {
		t.Fatalf("same-service delete = %v, want Queue", got)
	}

	// Unexpected object type → fail open to Queue.
	if got := pl.podHint(log, pending, nil, "garbage"); got != framework.QueueAfterBackoff {
		t.Fatalf("garbage = %v, want Queue (fail open)", got)
	}
}

func TestPolicyHint(t *testing.T) {
	pl := hintPlugin(t)
	log := klog.Background()
	pending := schedPod("p1", "alpha")

	policyObj := func(ns, schedName, labelValue string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("scheduling.soyup.top/v1alpha1")
		u.SetKind("ServiceSpreadPolicy")
		u.SetNamespace(ns)
		u.SetName("ssp-x")
		_ = unstructured.SetNestedField(u.Object, schedName, "spec", "schedulerName")
		_ = unstructured.SetNestedMap(u.Object, map[string]interface{}{"svc": labelValue}, "spec", "serviceSelector", "matchLabels")
		return u
	}

	if got := pl.policyHint(log, pending, nil, policyObj("ns", "service-spread-scheduler", "alpha")); got != framework.QueueAfterBackoff {
		t.Fatalf("matching policy = %v, want Queue", got)
	}
	if got := pl.policyHint(log, pending, nil, policyObj("other", "service-spread-scheduler", "alpha")); got != framework.QueueSkip {
		t.Fatalf("other namespace = %v, want QueueSkip", got)
	}
	if got := pl.policyHint(log, pending, nil, policyObj("ns", "other-scheduler", "alpha")); got != framework.QueueSkip {
		t.Fatalf("other scheduler = %v, want QueueSkip", got)
	}
	if got := pl.policyHint(log, pending, nil, policyObj("ns", "service-spread-scheduler", "beta")); got != framework.QueueSkip {
		t.Fatalf("other service = %v, want QueueSkip", got)
	}
	if got := pl.policyHint(log, pending, nil, "garbage"); got != framework.QueueAfterBackoff {
		t.Fatalf("garbage = %v, want Queue (fail open)", got)
	}
}

func TestSameNamespaceHint(t *testing.T) {
	log := klog.Background()
	pending := schedPod("p1", "alpha")
	rs := &unstructured.Unstructured{}
	rs.SetNamespace("ns")
	if got := sameNamespaceHint(log, pending, nil, rs); got != framework.QueueAfterBackoff {
		t.Fatalf("same ns = %v, want Queue", got)
	}
	rs2 := &unstructured.Unstructured{}
	rs2.SetNamespace("elsewhere")
	if got := sameNamespaceHint(log, pending, nil, rs2); got != framework.QueueSkip {
		t.Fatalf("other ns = %v, want QueueSkip", got)
	}
	if got := sameNamespaceHint(log, pending, "garbage", nil); got != framework.QueueAfterBackoff {
		t.Fatalf("garbage = %v, want Queue (fail open)", got)
	}
}
