package spread

import (
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	listersautoscalingv2 "k8s.io/client-go/listers/autoscaling/v2"
)

// ---- fakes for the observer's read surface ----

type fakeTargetSource struct {
	hpas map[string][]*autoscalingv2.HorizontalPodAutoscaler // key: deployment name
	so   map[string]int32                                    // key: deployment name -> minReplicaCount (KEDA-managed)
	errs map[string]error                                    // key: deployment name -> read error
}

func (f *fakeTargetSource) HPAsFor(_ string, dep string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	if err, ok := f.errs["hpa:"+dep]; ok {
		return nil, err
	}
	return f.hpas[dep], nil
}

func (f *fakeTargetSource) ScaledObjectMinReplicas(_ string, dep string) (int32, bool, error) {
	if err, ok := f.errs["so:"+dep]; ok {
		return 0, false, err
	}
	min, ok := f.so[dep]
	return min, ok, nil
}

func mkDep(name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

func mkHPA(name string, target string, desired int32, max int32) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: name},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: target},
			MaxReplicas:    max,
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{DesiredReplicas: desired},
	}
}

// ---- resolveDeploymentTarget: priority chain (dev-design §6) ----

func TestResolveTarget_DeploymentFallback(t *testing.T) {
	src := &fakeTargetSource{}
	got, source := resolveDeploymentTarget(mkDep("d1", 3), src, nil)
	if got != 3 || source != targetSourceDeployment {
		t.Fatalf("want 3/deployment, got %d/%s", got, source)
	}
}

func TestResolveTarget_KEDAFallbackTakesMaxOfMinAndReplicas(t *testing.T) {
	// deviation §9-6: kedaFallback = max(minReplicaCount, replicas).
	src := &fakeTargetSource{so: map[string]int32{"d1": 2}}
	got, source := resolveDeploymentTarget(mkDep("d1", 5), src, nil)
	if got != 5 || source != targetSourceKEDAFallback {
		t.Fatalf("want 5/kedaFallback (replicas>min), got %d/%s", got, source)
	}
	src2 := &fakeTargetSource{so: map[string]int32{"d2": 7}}
	got2, _ := resolveDeploymentTarget(mkDep("d2", 4), src2, nil)
	if got2 != 7 {
		t.Fatalf("want 7 (min>replicas), got %d", got2)
	}
}

func TestResolveTarget_HPAPriority(t *testing.T) {
	// HPA desiredReplicas wins over KEDA and deployment replicas.
	src := &fakeTargetSource{
		hpas: map[string][]*autoscalingv2.HorizontalPodAutoscaler{
			"d1": {mkHPA("h1", "d1", 9, 20)},
		},
		so: map[string]int32{"d1": 2},
	}
	got, source := resolveDeploymentTarget(mkDep("d1", 3), src, nil)
	if got != 9 || source != targetSourceHPA {
		t.Fatalf("want 9/hpa, got %d/%s", got, source)
	}
}

func TestResolveTarget_MultipleHPAsTakeMax(t *testing.T) {
	src := &fakeTargetSource{
		hpas: map[string][]*autoscalingv2.HorizontalPodAutoscaler{
			"d1": {mkHPA("h1", "d1", 4, 10), mkHPA("h2", "d1", 6, 12)},
		},
	}
	got, source := resolveDeploymentTarget(mkDep("d1", 3), src, nil)
	if got != 6 || source != targetSourceFallback {
		t.Fatalf("want 6/ReplicaTargetFallback, got %d/%s", got, source)
	}
}

func TestResolveTarget_HPAReadError_UsesLastGood(t *testing.T) {
	src := &fakeTargetSource{
		hpas: map[string][]*autoscalingv2.HorizontalPodAutoscaler{
			"d1": {mkHPA("h1", "d1", 9, 20)},
		},
	}
	lastGood := map[string]deploymentTarget{"ns1/d1": {value: 9, source: targetSourceHPA}}

	// First read works.
	got, _ := resolveDeploymentTarget(mkDep("d1", 3), src, lastGood)
	if got != 9 {
		t.Fatalf("initial read: want 9, got %d", got)
	}
	// Now the read errors: lastGood must serve.
	src.errs = map[string]error{"hpa:d1": errors.New("boom")}
	got2, source := resolveDeploymentTarget(mkDep("d1", 3), src, lastGood)
	if got2 != 9 || source != targetSourceLastGood {
		t.Fatalf("error path: want 9/lastGood, got %d/%s", got2, source)
	}
}

func TestResolveTarget_HPAReadError_NoLastGood_FallsBackToDeployment(t *testing.T) {
	src := &fakeTargetSource{errs: map[string]error{"hpa:d1": errors.New("boom")}}
	got, source := resolveDeploymentTarget(mkDep("d1", 3), src, nil)
	if got != 3 || source != targetSourceDeployment {
		t.Fatalf("want 3/deployment on read error without lastGood, got %d/%s", got, source)
	}
}

// ---- domain aggregation: protection floor (design 6.3) ----

func TestDomainAggregate_ProtectiveFloor(t *testing.T) {
	// desired=3 but 5 pods already observed -> reported must be 5.
	agg := aggregateDomain(3, 5)
	if agg != 5 {
		t.Fatalf("protective floor: want 5, got %d", agg)
	}
	agg2 := aggregateDomain(7, 2)
	if agg2 != 7 {
		t.Fatalf("desired above observed: want 7, got %d", agg2)
	}
}

// ---- capacity deficit (design 10.3 / dev-design §6) ----

func TestCapacityDeficit(t *testing.T) {
	// deficit = hpaMax - eligibleNodes*cap ; floor at 0.
	if d := capacityDeficit(12, 2, 5); d != 2 {
		t.Fatalf("want 2, got %d", d)
	}
	if d := capacityDeficit(6, 2, 5); d != 0 {
		t.Fatalf("want 0 (no deficit), got %d", d)
	}
}

// ---- observed pods counting feeds the floor ----

func mkPodOn(node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			UID:       types.UID(node + "-pod"),
			Labels:    map[string]string{"app.kubernetes.io/name": "svc-a"},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestObserveCycle_ProducesDomainSeries(t *testing.T) {
	// One service, one deployment target=4, two observed pods -> series
	// carries replica_target=4, observed_pods=2, bound_pods=2.
	src := &fakeTargetSource{}
	ob := NewReplicaTargetObserver(ObserverDeps{
		ServiceLabelKey: "app.kubernetes.io/name",
		Targets:         src,
	})
	deps := []*appsv1.Deployment{mkDep("d1", 4)}
	pods := []*corev1.Pod{mkPodOn("n1"), mkPodOn("n2")}
	series := ob.Compute(deps, map[string]string{"ns1/d1": "svc-a"}, pods)
	if len(series) != 1 {
		t.Fatalf("want 1 domain series, got %d", len(series))
	}
	s := series[0]
	if s.ReplicaTarget != 4 || s.ObservedPods != 2 || s.BoundPods != 2 {
		t.Fatalf("want target=4 observed=2 bound=2, got %+v", s)
	}
	if s.Namespace != "ns1" || s.Service != "svc-a" {
		t.Fatalf("labels wrong: %+v", s)
	}
}

// ---- informerTargetSource (production source) ----

func newTestInformerSource(t *testing.T, hpas []*autoscalingv2.HorizontalPodAutoscaler, soIndexer cache.Indexer, soSynced cache.InformerSynced, kedaKnown bool) *informerTargetSource {
	t.Helper()
	hpaIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, h := range hpas {
		if err := hpaIndexer.Add(h); err != nil {
			t.Fatal(err)
		}
	}
	hpaLister := listersautoscalingv2.NewHorizontalPodAutoscalerLister(hpaIndexer)
	return newInformerTargetSource(hpaLister, nil, func() (cache.Indexer, cache.InformerSynced, bool) {
		return soIndexer, soSynced, kedaKnown
	})
}

func mkScaledObject(ns, name, target string, min int64) *unstructured.Unstructured {
	so := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "keda.sh/v1alpha1",
			"kind":       "ScaledObject",
			"metadata":   map[string]interface{}{"namespace": ns, "name": name},
			"spec": map[string]interface{}{
				"scaleTargetRef": map[string]interface{}{"kind": "Deployment", "name": target},
				"minReplicaCount": min,
			},
		},
	}
	so.SetGroupVersionKind(schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"})
	return so
}

func TestInformerSource_HPAMatchByScaleTargetRef(t *testing.T) {
	hpas := []*autoscalingv2.HorizontalPodAutoscaler{
		mkHPA("h1", "web", 5, 10),
		mkHPA("h2", "other", 3, 8), // different target: must NOT match
	}
	src := newTestInformerSource(t, hpas, nil, nil, false)
	got, err := src.HPAsFor("ns1", "web")
	if err != nil || len(got) != 1 || got[0].Name != "h1" {
		t.Fatalf("want exactly h1, got %v err=%v", got, err)
	}
}

func TestInformerSource_KEDAAbsent_GroupNotInDiscovery(t *testing.T) {
	src := newTestInformerSource(t, nil, nil, nil, false)
	min, managed, err := src.ScaledObjectMinReplicas("ns1", "web")
	if managed || err != nil || min != 0 {
		t.Fatalf("absent KEDA: want (0,false,nil), got (%d,%v,%v)", min, managed, err)
	}
}

func TestInformerSource_KEDAPresent_MatchesByScaleTargetRef(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := idx.Add(mkScaledObject("ns1", "so1", "web", 2)); err != nil {
		t.Fatal(err)
	}
	if err := idx.Add(mkScaledObject("ns1", "so2", "other", 9)); err != nil {
		t.Fatal(err)
	}
	src := newTestInformerSource(t, nil, idx, func() bool { return true }, true)
	min, managed, err := src.ScaledObjectMinReplicas("ns1", "web")
	if err != nil || !managed || min != 2 {
		t.Fatalf("want (2,true,nil), got (%d,%v,%v)", min, managed, err)
	}
}

func TestInformerSource_KEDANotSynced_IsDegraded(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	src := newTestInformerSource(t, nil, idx, func() bool { return false }, true)
	_, _, err := src.ScaledObjectMinReplicas("ns1", "web")
	if err == nil {
		t.Fatalf("unsynced KEDA cache must surface degraded error, got nil")
	}
}
