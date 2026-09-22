package spread

import (
	"context"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	schedulingv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
	configv1alpha1 "github.com/cnyup/service-spread-scheduler/apis/config/v1alpha1"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ---- test doubles ----

var tCtx = context.Background()

type fakePolicies struct {
	items  []*schedulingv1alpha1.ServiceSpreadPolicy
	synced bool
}

func (f *fakePolicies) List(namespace string) ([]*schedulingv1alpha1.ServiceSpreadPolicy, error) {
	var out []*schedulingv1alpha1.ServiceSpreadPolicy
	for _, p := range f.items {
		if p.Namespace == namespace {
			out = append(out, p)
		}
	}
	return out, nil
}
func (f *fakePolicies) HasSynced() bool { return f.synced }

type fakeOwners struct{ owned bool }

func (f fakeOwners) OwnedByDeployment(string, string, []metav1.OwnerReference) bool { return f.owned }
func (f fakeOwners) HasSynced() bool                                                { return true }

func testPolicy(ns string, maxSkew, maxPods int32) *schedulingv1alpha1.ServiceSpreadPolicy {
	return &schedulingv1alpha1.ServiceSpreadPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "ssp-test", Namespace: ns},
		Spec: schedulingv1alpha1.ServiceSpreadPolicySpec{
			SchedulerName: "service-spread-scheduler",
			ServiceSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"svc": "alpha"},
			},
			MaxSkew:        maxSkew,
			MaxPodsPerNode: maxPods,
		},
	}
}

// newTestPlugin builds a plugin with fully-controlled deps plus the nodes
// n1..n3 (all ready, no taints) and 0 starting counts.
func newTestPlugin(t *testing.T, mutate func(deps *pluginDeps, args *configv1alpha1.ServiceSpreadArgs)) (*ServiceSpread, *framework.CycleState, *spreadState) {
	t.Helper()
	st := newSpreadState()
	deps := pluginDeps{
		policies: &fakePolicies{items: []*schedulingv1alpha1.ServiceSpreadPolicy{testPolicy("ns", 1, 2)}},
		owners:   fakeOwners{owned: true},
		nodes: fakeNodes{[]*v1.Node{
			domNode("n1"), domNode("n2"), domNode("n3"),
		}},
		state:  st,
		events: newStateEvents(st),
		synced: func() bool { return true },
	}
	args := configv1alpha1.ServiceSpreadArgs{
		ServiceLabelKey:      "svc",
		ManagedSchedulerName: "service-spread-scheduler",
	}
	if mutate != nil {
		mutate(&deps, &args)
	}
	pl, err := NewWithDeps(args, deps)
	if err != nil {
		t.Fatal(err)
	}
	return pl.(*ServiceSpread), framework.NewCycleState(), st
}

func schedPod(uid, labelValue string) *v1.Pod {
	p := domPod()
	p.UID = types.UID(uid)
	if labelValue != "" {
		p.Labels = map[string]string{"svc": labelValue}
	}
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "rs", Controller: ptr(true)}}
	return p
}

func nodeInfoOf(name string) *framework.NodeInfo {
	ni := framework.NewNodeInfo()
	ni.SetNode(domNode(name))
	return ni
}

// ---- PreFilter rejection reasons ----

func TestPreFilterCacheNotSynced(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.synced = func() bool { return false }
	})
	_, st := pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "CacheNotSynced") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

func TestPreFilterMissingLabelUnresolvable(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, nil)
	_, st := pl.PreFilter(tCtx, cs, schedPod("u1", ""))
	if st.Code() != framework.UnschedulableAndUnresolvable || !strings.Contains(st.Message(), "MissingRequiredServiceLabel") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

func TestPreFilterUnsupportedOwner(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.owners = fakeOwners{owned: false}
	})
	_, st := pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.UnschedulableAndUnresolvable || !strings.Contains(st.Message(), "UnsupportedWorkloadOwner") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

func TestPreFilterPolicyResolution(t *testing.T) {
	// No policy + requirePolicy (default true).
	pl, cs, _ := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.policies = &fakePolicies{}
	})
	_, st := pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.UnschedulableAndUnresolvable || !strings.Contains(st.Message(), "ServiceSpreadPolicyNotFound") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}

	// No policy + requirePolicy=false → allowed (policy stays nil: any
	// placement would then be unconstrained; first release defaults to
	// require, this path is config-only).
	pl, cs, _ = newTestPlugin(t, func(d *pluginDeps, a *configv1alpha1.ServiceSpreadArgs) {
		d.policies = &fakePolicies{}
		a.RequirePolicy = ptr(false)
	})
	_, st = pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.Success {
		t.Fatalf("requirePolicy=false should pass PreFilter, got %v (%s)", st.Code(), st.Message())
	}

	// Two policies → ambiguous.
	pl, cs, _ = newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.policies = &fakePolicies{items: []*schedulingv1alpha1.ServiceSpreadPolicy{
			testPolicy("ns", 1, 2), testPolicy("ns", 2, 3),
		}}
	})
	_, st = pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.UnschedulableAndUnresolvable || !strings.Contains(st.Message(), "AmbiguousServiceSpreadPolicy") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

func TestPreFilterEmptyDomain(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.nodes = fakeNodes{}
	})
	_, st := pl.PreFilter(tCtx, cs, schedPod("u1", "alpha"))
	if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "EmptySpreadDomain") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

// ---- Filter constraint boundaries ----

// place: PreFilter + Filter + (optional) Reserve for one pod/node pair.
func runFilter(pl *ServiceSpread, cs *framework.CycleState, pod *v1.Pod, node string) *framework.Status {
	if _, st := pl.PreFilter(tCtx, cs, pod); !st.IsSuccess() {
		return st
	}
	return pl.Filter(tCtx, cs, pod, nodeInfoOf(node))
}

func TestFilterOutsideDomain(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, nil)
	st := runFilter(pl, cs, schedPod("u1", "alpha"), "unknown-node")
	if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "NodeOutsideSpreadDomain") {
		t.Fatalf("code=%v msg=%s", st.Code(), st.Message())
	}
}

func TestFilterMaxSkewBoundary(t *testing.T) {
	pl, _, _ := newTestPlugin(t, nil)
	// Seed real counts: two same-service pods already bound on n1
	// (maxSkew=1 → min=0 on n2/n3, n1 has 2).
	pl2, _, st := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		// maxPodsPerNode high enough that only the skew rule can reject.
		d.policies = &fakePolicies{items: []*schedulingv1alpha1.ServiceSpreadPolicy{testPolicy("ns", 1, 5)}}
	})
	_ = pl
	seedRef := func() podRef {
		seedPod := schedPod("seed", "alpha")
		seedPod.Spec.NodeName = "n1"
		ref, err := PodRefFor(seedPod, "service-spread-scheduler", "svc")
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}()
	ev := newStateEvents(st)
	for _, uid := range []string{"a", "b"} {
		p := boundPod(uid, "n1", v1.PodRunning)
		ev.OnPodAddOrUpdate(p, seedRef)
	}

	// n1: 2+1-0=3 > 1 → skew-rejected.
	st1 := runFilter(pl2, framework.NewCycleState(), schedPod("u1", "alpha"), "n1")
	if st1.Code() != framework.Unschedulable || !strings.Contains(st1.Message(), "MaxSkewExceeded") {
		t.Fatalf("code=%v msg=%s", st1.Code(), st1.Message())
	}
	// n2: 0+1-0=1 ≤ 1 → passes and becomes the preferred (lowest-count) node.
	if st2 := runFilter(pl2, framework.NewCycleState(), schedPod("u2", "alpha"), "n2"); !st2.IsSuccess() {
		t.Fatalf("n2 should pass: %s", st2.Message())
	}
}

func TestFilterMaxPodsPerNodeBoundary(t *testing.T) {
	// maxPodsPerNode=2; the quota key is shared across domains but the cap
	// itself is per node: two same-service pods on n1 reject a third on n1
	// while n2 stays feasible.
	pl, _, st := newTestPlugin(t, nil)
	seedRef := func() podRef {
		seedPod := schedPod("seed", "alpha")
		seedPod.Spec.NodeName = "n1"
		ref, err := PodRefFor(seedPod, "service-spread-scheduler", "svc")
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}()
	ev := newStateEvents(st)
	for _, uid := range []string{"a", "b"} {
		ev.OnPodAddOrUpdate(boundPod(uid, "n1", v1.PodRunning), seedRef)
	}

	st1 := runFilter(pl, framework.NewCycleState(), schedPod("u1", "alpha"), "n1")
	if st1.Code() != framework.Unschedulable || !strings.Contains(st1.Message(), "MaxPodsPerNodeExceeded") {
		t.Fatalf("code=%v msg=%s", st1.Code(), st1.Message())
	}
	if st2 := runFilter(pl, framework.NewCycleState(), schedPod("u2", "alpha"), "n2"); !st2.IsSuccess() {
		t.Fatalf("n2 should pass: %s", st2.Message())
	}
}

// ---- Reserve revalidation ----

func TestReserveRevalidationFailure(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, nil)
	pod := schedPod("u1", "alpha")
	if _, st := pl.PreFilter(tCtx, cs, pod); !st.IsSuccess() {
		t.Fatalf("prefilter: %s", st.Message())
	}
	// Node left the domain between Filter and Reserve.
	pl.deps.nodes = fakeNodes{}
	if s := pl.Reserve(tCtx, cs, pod, "n1"); s.IsSuccess() || !strings.Contains(s.Message(), "ReserveRevalidationFailed") {
		t.Fatalf("reserve should fail: %v %s", s.Code(), s.Message())
	}
	// Quota recheck failure: maxPodsPerNode=2 is a per-node cap — two
	// reservations on n1 must reject a third on the same node.
	pl2, _, _ := newTestPlugin(t, func(d *pluginDeps, _ *configv1alpha1.ServiceSpreadArgs) {
		d.policies = &fakePolicies{items: []*schedulingv1alpha1.ServiceSpreadPolicy{testPolicy("ns", 3, 2)}}
	})
	for _, uid := range []string{"x", "y"} {
		p := schedPod(uid, "alpha")
		c := framework.NewCycleState()
		if _, st := pl2.PreFilter(tCtx, c, p); !st.IsSuccess() {
			t.Fatal(st.Message())
		}
		if s := pl2.Reserve(tCtx, c, p, "n1"); !s.IsSuccess() {
			t.Fatalf("setup reserve %s: %s", uid, s.Message())
		}
	}
	p2 := schedPod("z", "alpha")
	c2 := framework.NewCycleState()
	if _, st := pl2.PreFilter(tCtx, c2, p2); !st.IsSuccess() {
		t.Fatal(st.Message())
	}
	// Third reservation on n1: quota 2+1 > 2 AND skew 2+1-0 > 1 — the
	// recheck must refuse without writing state.
	if s := pl2.Reserve(tCtx, c2, p2, "n1"); s.IsSuccess() {
		t.Fatal("reserve must fail the recheck")
	}
	if pl2.deps.state.reservationsLen() != 2 {
		t.Fatalf("reservations = %d, want 2", pl2.deps.state.reservationsLen())
	}
}

func TestUnreserveAndPostBind(t *testing.T) {
	pl, cs, st := newTestPlugin(t, nil)
	pod := schedPod("u1", "alpha")
	if _, pst := pl.PreFilter(tCtx, cs, pod); !pst.IsSuccess() {
		t.Fatal(pst.Message())
	}
	if s := pl.Reserve(tCtx, cs, pod, "n1"); !s.IsSuccess() {
		t.Fatal(s.Message())
	}
	pl.PostBind(tCtx, cs, pod, "n1")
	if ph, ok := st.reservationPhase("u1"); !ok || ph != resvBoundObs {
		t.Fatalf("phase = %v,%v want BoundObs", ph, ok)
	}
	// Unreserve on BoundObs is a no-op by design.
	pl.Unreserve(tCtx, cs, pod, "n1")
	if st.reservationsLen() != 1 {
		t.Fatal("Unreserve must not drop BoundObs")
	}
	// Fresh Reserved reservation is droppable.
	pod2 := schedPod("u2", "alpha")
	cs2 := framework.NewCycleState()
	if _, pst := pl.PreFilter(tCtx, cs2, pod2); !pst.IsSuccess() {
		t.Fatal(pst.Message())
	}
	if s := pl.Reserve(tCtx, cs2, pod2, "n2"); !s.IsSuccess() {
		t.Fatal(s.Message())
	}
	pl.Unreserve(tCtx, cs2, pod2, "n2")
	if st.reservationsLen() != 1 {
		t.Fatal("Unreserve must drop Reserved")
	}
}

// ---- Score normalization ----

func TestScoreNormalizationMonotonicity(t *testing.T) {
	pl, _, st := newTestPlugin(t, nil)
	pod := schedPod("u1", "alpha")
	// Skew the domain: one same-service pod already bound on n1 (real
	// count, real schedKey derived from the same pod shape).
	oldPod := schedPod("old", "alpha")
	oldPod.Spec.NodeName = "n1"
	ref, err := PodRefFor(oldPod, "service-spread-scheduler", "svc")
	if err != nil {
		t.Fatal(err)
	}
	newStateEvents(st).OnPodAddOrUpdate(boundPod("old", "n1", v1.PodRunning), ref)

	cs2 := framework.NewCycleState()
	if _, pst := pl.PreFilter(tCtx, cs2, pod); !pst.IsSuccess() {
		t.Fatal(pst.Message())
	}
	nodes := []*v1.Node{domNode("n1"), domNode("n2")}
	if ps := pl.PreScore(tCtx, cs2, pod, nodes); !ps.IsSuccess() {
		t.Fatal(ps.Message())
	}
	s1, st1 := pl.Score(tCtx, cs2, pod, "n1")
	s2, st2 := pl.Score(tCtx, cs2, pod, "n2")
	if !st1.IsSuccess() || !st2.IsSuccess() {
		t.Fatalf("score statuses: %s %s", st1.Message(), st2.Message())
	}
	// Formula: (cmax-count)*100/(cmax-cmin+1) → n1:0, n2:(1)*100/2=50.
	// Monotonic: strictly lower count ⇒ strictly higher score; the
	// min-count node holds a decisive edge over native-plugin deltas of
	// a few points (dev-design §5.5 rationale).
	if s1 != 0 || s2 != 50 {
		t.Fatalf("scores = (%d,%d), want (0,50)", s1, s2)
	}
}

func TestScoreSkipWhenEqualCounts(t *testing.T) {
	pl, cs, _ := newTestPlugin(t, nil)
	pod := schedPod("u1", "alpha")
	if _, pst := pl.PreFilter(tCtx, cs, pod); !pst.IsSuccess() {
		t.Fatal(pst.Message())
	}
	if ps := pl.PreScore(tCtx, cs, pod, []*v1.Node{domNode("n1"), domNode("n2")}); !ps.IsSuccess() {
		t.Fatal(ps.Message())
	}
	s1, _ := pl.Score(tCtx, cs, pod, "n1")
	s2, _ := pl.Score(tCtx, cs, pod, "n2")
	if s1 != s2 || s1 != 0 {
		t.Fatalf("equal counts must tie at 0: (%d,%d)", s1, s2)
	}
}
