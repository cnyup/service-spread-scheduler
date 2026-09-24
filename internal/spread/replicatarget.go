package spread

import (
	"context"
	"fmt"
	"sync"
	"time"

	schedulingv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	listersappsv1 "k8s.io/client-go/listers/apps/v1"
	listersautoscalingv2 "k8s.io/client-go/listers/autoscaling/v2"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Replica-target observer (dev-design §6; design doc §6/§10.3): metrics
// only, never participates in placement decisions. Runs inside the
// scheduler process sharing the same informers (dev-design §9 deviation 4).

// targetSource labels where a deployment's replica target came from.
type targetSource string

const (
	targetSourceHPA          targetSource = "hpa"
	targetSourceKEDAFallback targetSource = "kedaFallback"
	targetSourceDeployment   targetSource = "deployment"
	targetSourceLastGood     targetSource = "lastGood"
	targetSourceFallback     targetSource = "ReplicaTargetFallback"
)

// deploymentTarget is the resolved (value, provenance) pair of one
// deployment. It doubles as the lastGood cache entry.
type deploymentTarget struct {
	value  int32
	source targetSource
}

// TargetSource is the read surface the observer needs for HPA/KEDA data.
// Implementations wrap informer listers (production) or fakes (tests).
// All resources are namespaced: callers pass the DEPLOYMENT's namespace.
type TargetSource interface {
	// HPAsFor returns the HPAs whose scaleTargetRef points at the named
	// deployment in the same namespace (nil/empty when none).
	HPAsFor(namespace, deployment string) ([]*autoscalingv2.HorizontalPodAutoscaler, error)
	// ScaledObjectMinReplicas returns (minReplicaCount, true, nil) when a
	// KEDA ScaledObject targets the deployment. Implementations decide
	// "KEDA absent" (group not in discovery -> not managed, nil error)
	// versus "degraded" (registered but unreadable -> error) — dev-design
	// §6: degraded must not silently read as absent.
	ScaledObjectMinReplicas(namespace, deployment string) (int32, bool, error)
}

// DomainSeries is one exported sample per (namespace, service, domain).
type DomainSeries struct {
	Namespace       string
	Service         string
	Domain          string
	ReplicaTarget   int32 // reportedReplicaTarget (protective floor applied)
	ObservedPods    int32 // Pending+Running incl. unbound
	BoundPods       int32 // bound-node count total
	FallbackActive  bool  // reason=ReplicaTargetFallback on any component
	CapacityDeficit int32 // HPA maxReplicas − eligibleNodes×maxPodsPerNode, floored at 0 (design 10.3); exported only when computable
}

// ObserverDeps carries observer configuration (mirrors ServiceSpreadArgs
// fields relevant here).
type ObserverDeps struct {
	ServiceLabelKey string
	Targets         TargetSource
	// Nodes and Policies enable the capacity-deficit export (design doc
	// 10.3); when either is nil the deficit is skipped entirely.
	Nodes                NodeReader
	Policies             PolicyReader
	ManagedSchedulerName string
}

// ReplicaTargetObserver resolves deployment targets and aggregates domain
// series. Safe for concurrent use; the production loop calls Compute on a
// ticker from one goroutine (dev-design §6: event-driven + periodic export).
type ReplicaTargetObserver struct {
	deps     ObserverDeps
	lastGood map[string]deploymentTarget // key: namespace/name
	mu       sync.Mutex
}

func NewReplicaTargetObserver(deps ObserverDeps) *ReplicaTargetObserver {
	return &ReplicaTargetObserver{
		deps:     deps,
		lastGood: map[string]deploymentTarget{},
	}
}

// resolveDeploymentTarget implements the priority chain of dev-design §6:
//
//	HPA desiredReplicas (max over multiple HPAs, reason=ReplicaTargetFallback)
//	> KEDA fallback max(minReplicaCount, replicas)
//	> deployment replicas
//
// Read errors fall back to lastGood when present, else to deployment
// replicas. Successful reads refresh lastGood.
func resolveDeploymentTarget(dep *appsv1.Deployment, src TargetSource, lastGood map[string]deploymentTarget) (int32, targetSource) {
	key := dep.Namespace + "/" + dep.Name
	replicas := int32(0)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}

	hpas, hpaErr := src.HPAsFor(dep.Namespace, dep.Name)
	if hpaErr == nil {
		if len(hpas) > 0 {
			maxDesired := int32(-1)
			for _, h := range hpas {
				if h.Status.DesiredReplicas > maxDesired {
					maxDesired = h.Status.DesiredReplicas
				}
			}
			if maxDesired >= 0 {
				src := targetSourceHPA
				if len(hpas) > 1 {
					src = targetSourceFallback
				}
				if lastGood != nil {
					lastGood[key] = deploymentTarget{value: maxDesired, source: src}
				}
				return maxDesired, src
			}
		}
	}

	if min, managed, soErr := src.ScaledObjectMinReplicas(dep.Namespace, dep.Name); soErr == nil && managed {
		v := replicas
		if min > v {
			v = min
		}
		if lastGood != nil {
			lastGood[key] = deploymentTarget{value: v, source: targetSourceKEDAFallback}
		}
		return v, targetSourceKEDAFallback
	}

	// Read errors (hpaErr != nil or soErr != nil): prefer lastGood.
	if hpaErr != nil {
		if lg, ok := lastGood[key]; ok {
			return lg.value, targetSourceLastGood
		}
	}

	if lastGood != nil {
		lastGood[key] = deploymentTarget{value: replicas, source: targetSourceDeployment}
	}
	return replicas, targetSourceDeployment
}

// aggregateDomain applies the protective floor of design doc 6.3:
// reportedReplicaTarget = max(domainDesired, observedPods).
func aggregateDomain(domainDesired, observedPods int32) int32 {
	if domainDesired > observedPods {
		return domainDesired
	}
	return observedPods
}

// capacityDeficit = hpaMaxReplicas − eligibleNodeCount × maxPodsPerNode,
// floored at 0 (design doc 10.3; node union per deployment constraints is
// computed by the caller).
func capacityDeficit(hpaMax int32, eligibleNodes int32, maxPodsPerNode int32) int32 {
	d := hpaMax - eligibleNodes*maxPodsPerNode
	if d < 0 {
		return 0
	}
	return d
}

// svcAgg is the per-service aggregation state of one Compute round.
type svcAgg struct {
	desired  int32
	observed int32
	bound    int32
	fallback bool
	hpaMax   int32 // max HPA MaxReplicas over the service's deployments; 0 = none
	deps     []*appsv1.Deployment
}

// Compute resolves every deployment, groups pods to (namespace, service)
// via svcOf (deployment key -> service label value), and produces one
// DomainSeries per service. Pods are Pending+Running (bound or not) and
// carry the service label themselves; the observer never guesses.
func (o *ReplicaTargetObserver) Compute(
	deployments []*appsv1.Deployment,
	svcOf map[string]string, // "namespace/depName" -> service value
	pods []*corev1.Pod,
) []DomainSeries {
	o.mu.Lock()
	defer o.mu.Unlock()

	aggs := map[string]*svcAgg{}
	nsOf := map[string]string{}

	for _, dep := range deployments {
		svc, ok := svcOf[dep.Namespace+"/"+dep.Name]
		if !ok {
			continue
		}
		key := dep.Namespace + "/" + svc
		if aggs[key] == nil {
			aggs[key] = &svcAgg{}
			nsOf[key] = dep.Namespace
		}
		v, src := resolveDeploymentTarget(dep, o.deps.Targets, o.lastGood)
		aggs[key].desired += v
		if src == targetSourceFallback || src == targetSourceLastGood {
			aggs[key].fallback = true
		}
		// Track the deployment's HPA ceiling for the capacity deficit
		// (max over multiple HPAs, mirroring resolveDeploymentTarget).
		if hpas, err := o.deps.Targets.HPAsFor(dep.Namespace, dep.Name); err == nil {
			for _, h := range hpas {
				if h.Spec.MaxReplicas > aggs[key].hpaMax {
					aggs[key].hpaMax = h.Spec.MaxReplicas
				}
			}
		}
		aggs[key].deps = append(aggs[key].deps, dep)
	}

	for _, p := range pods {
		if p.Status.Phase != corev1.PodPending && p.Status.Phase != corev1.PodRunning {
			continue
		}
		svc, ok := p.Labels[o.deps.ServiceLabelKey]
		if !ok {
			continue
		}
		key := p.Namespace + "/" + svc
		if aggs[key] == nil {
			continue // pods of services without deployments are not exported
		}
		aggs[key].observed++
		if p.Spec.NodeName != "" {
			aggs[key].bound++
		}
	}

	series := make([]DomainSeries, 0, len(aggs))
	for key, a := range aggs {
		svc := key[len(nsOf[key])+1:]
		var deficit int32
		if o.deps.Nodes != nil && o.deps.Policies != nil {
			deficit = o.capacityDeficitFor(nsOf[key], svc, a)
		}
		series = append(series, DomainSeries{
			Namespace:       nsOf[key],
			Service:         svc,
			Domain:          "default", // single-domain until multi-domain export lands (§6 detail metric)
			ReplicaTarget:   aggregateDomain(a.desired, a.observed),
			ObservedPods:    a.observed,
			BoundPods:       a.bound,
			FallbackActive:  a.fallback,
			CapacityDeficit: deficit,
		})
	}
	return series
}

// capacityDeficitFor implements design doc 10.3: HPA maxReplicas minus the
// node union (per-deployment stable constraints) times maxPodsPerNode,
// floored at 0. Returns 0 (no export) when the policy is missing/ambiguous
// or no HPA bounds the service — absence of data is not a deficit.
func (o *ReplicaTargetObserver) capacityDeficitFor(namespace, service string, a *svcAgg) int32 {
	if a.hpaMax <= 0 {
		return 0
	}
	policies, err := o.deps.Policies.List(namespace)
	if err != nil {
		klog.Background().Error(err, "observer: list policies for capacity deficit", "namespace", namespace)
		return 0
	}
	var policy *schedulingv1alpha1.ServiceSpreadPolicy
	for _, p := range policies {
		if p.Spec.SchedulerName != o.deps.ManagedSchedulerName {
			continue
		}
		if selectorMatchesService(p.Spec.ServiceSelector, o.deps.ServiceLabelKey, service) {
			if policy != nil {
				return 0 // ambiguous: same fail-closed rule as resolvePolicy
			}
			policy = p
		}
	}
	if policy == nil {
		return 0
	}
	// Node union over the service's deployments: a node counts when it is
	// inside the stable domain of ANY deployment's pod template.
	nodes, err := o.deps.Nodes.List()
	if err != nil {
		klog.Background().Error(err, "observer: list nodes for capacity deficit")
		return 0
	}
	union := map[string]struct{}{}
	for _, dep := range a.deps {
		synth := &corev1.Pod{Spec: dep.Spec.Template.Spec}
		for _, n := range nodes {
			if nodeInStableDomain(synth, n) {
				union[n.Name] = struct{}{}
			}
		}
	}
	return capacityDeficit(a.hpaMax, int32(len(union)), policy.Spec.MaxPodsPerNode)
}

// Observer metrics (label names follow design doc 10.2).
var (
	replicaTarget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "service_spread_replica_target",
		Help: "reportedReplicaTarget per (namespace,service,domain) with protective floor applied",
	}, []string{"namespace", "service", "domain"})
	observedPodsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "service_spread_observed_pods",
		Help: "Pending+Running pods (incl. unbound) per service domain",
	}, []string{"namespace", "service", "domain"})
	boundPodsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "service_spread_bound_pods",
		Help: "pods bound to nodes per service domain",
	}, []string{"namespace", "service", "domain"})
	fallbackActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "service_spread_fallback_active",
		Help: "1 when any replica-target component fell back (ReplicaTargetFallback or lastGood)",
	}, []string{"namespace", "service", "domain"})
	capacityDeficitGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "service_spread_capacity_deficit",
		Help: "HPA maxReplicas minus eligible-node count times maxPodsPerNode, floored at 0",
	}, []string{"namespace", "service"})
)

func init() {
	prometheus.MustRegister(replicaTarget, observedPodsGauge, boundPodsGauge, fallbackActive, capacityDeficitGauge)
}

// ExportSeries pushes one Compute round into the prometheus gauges.
func (o *ReplicaTargetObserver) ExportSeries(series []DomainSeries) {
	for _, s := range series {
		replicaTarget.WithLabelValues(s.Namespace, s.Service, s.Domain).Set(float64(s.ReplicaTarget))
		observedPodsGauge.WithLabelValues(s.Namespace, s.Service, s.Domain).Set(float64(s.ObservedPods))
		boundPodsGauge.WithLabelValues(s.Namespace, s.Service, s.Domain).Set(float64(s.BoundPods))
		f := 0.0
		if s.FallbackActive {
			f = 1.0
		}
		fallbackActive.WithLabelValues(s.Namespace, s.Service, s.Domain).Set(f)
		if s.CapacityDeficit > 0 {
			capacityDeficitGauge.WithLabelValues(s.Namespace, s.Service).Set(float64(s.CapacityDeficit))
		}
	}
}

// ---- production TargetSource ----

// informerTargetSource serves TargetSource from shared informer caches.
// KEDA ScaledObjects are consumed via a dynamic informer; KEDA-absence is
// decided once via discovery and surfaces as "not managed" (no error), a
// registered-but-broken group surfaces as degraded (error).
type informerTargetSource struct {
	hpas listersautoscalingv2.HorizontalPodAutoscalerLister

	soIndexer cache.Indexer // *unstructured.Unstructured ScaledObjects
	soSynced  cache.InformerSynced
	kedaKnown func() bool // discovery probe: keda.sh group registered?
}

// scaledObjectGVR is the KEDA CRD reference (keda.sh/v1alpha1).
var scaledObjectGVR = schema.GroupVersionResource{
	Group:    "keda.sh",
	Version:  "v1alpha1",
	Resource: "scaledobjects",
}

// newInformerTargetSource builds the production source. hpaInformer must be
// started by the caller (shared informer factory). When the KEDA group is
// absent from discovery the dynamic informer is not created at all and
// ScaledObjectMinReplicas always reports "not managed" without error.
func newInformerTargetSource(
	hpas listersautoscalingv2.HorizontalPodAutoscalerLister,
	hpaInformer cache.SharedIndexInformer,
	kedaLister func() (cache.Indexer, cache.InformerSynced, bool),
) *informerTargetSource {
	src := &informerTargetSource{hpas: hpas, kedaKnown: func() bool { return false }}
	_ = hpaInformer // started by the shared factory; lister reads the cache
	if idx, synced, ok := kedaLister(); ok && idx != nil {
		src.soIndexer = idx
		src.soSynced = synced
		src.kedaKnown = func() bool { return true }
	}
	return src
}

// HPAsFor matches scaleTargetRef Kind=Deployment Name=<deployment> within
// the namespace (design 6.2: multiple HPAs on one target take the max).
func (s *informerTargetSource) HPAsFor(namespace, deployment string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	all, err := s.hpas.HorizontalPodAutoscalers(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	out := make([]*autoscalingv2.HorizontalPodAutoscaler, 0, len(all))
	for _, h := range all {
		ref := h.Spec.ScaleTargetRef
		if ref.Kind == "Deployment" && ref.Name == deployment {
			out = append(out, h)
		}
	}
	return out, nil
}

// ScaledObjectMinReplicas reports the minReplicaCount of the ScaledObject
// targeting the deployment. keda.sh not in discovery -> (0,false,nil).
// Cache not yet synced -> (0,false,degraded-error).
func (s *informerTargetSource) ScaledObjectMinReplicas(namespace, deployment string) (int32, bool, error) {
	if !s.kedaKnown() {
		return 0, false, nil
	}
	if s.soSynced != nil && !s.soSynced() {
		return 0, false, fmt.Errorf("scaledobject informer not synced (degraded)")
	}
	objs, err := s.soIndexer.ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return 0, false, fmt.Errorf("scaledobject list %s: %w", namespace, err)
	}
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		kind, _, _ := unstructured.NestedString(u.Object, "spec", "scaleTargetRef", "kind")
		name, _, _ := unstructured.NestedString(u.Object, "spec", "scaleTargetRef", "name")
		if kind != "Deployment" || name != deployment {
			continue
		}
		min, found, err := unstructured.NestedInt64(u.Object, "spec", "minReplicaCount")
		if err != nil || !found {
			return 0, true, nil // managed; unset minReplicaCount defaults to 0
		}
		if min < 0 {
			min = 0
		}
		return int32(min), true, nil
	}
	return 0, false, nil
}

// ---- observation loop ----

// ObserverLoopDeps feeds the periodic Compute cycle from informer caches.
type ObserverLoopDeps struct {
	Deployments listersappsv1.DeploymentLister
	Pods        listerscorev1.PodLister
	TargetSource
	ServiceLabelKey string
	ExportInterval  time.Duration
	// Nodes + Policies + ManagedSchedulerName enable the capacity-deficit
	// export (design 10.3); nil Nodes/Policies skips it.
	Nodes                NodeReader
	Policies             PolicyReader
	ManagedSchedulerName string
}

// RunObserverLoop resolves services -> series on a ticker until ctx ends.
// One goroutine per process (started in depsFromHandle); leader/follower
// both export: metrics are local gauges, exporting is side-effect free.
func RunObserverLoop(ctx context.Context, d ObserverLoopDeps) {
	ob := NewReplicaTargetObserver(ObserverDeps{
		ServiceLabelKey:      d.ServiceLabelKey,
		Targets:              d.TargetSource,
		Nodes:                d.Nodes,
		Policies:             d.Policies,
		ManagedSchedulerName: d.ManagedSchedulerName,
	})
	interval := d.ExportInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	export := func() {
		deps, err := d.Deployments.List(labels.Everything())
		if err != nil {
			klog.Background().Error(err, "observer: list deployments")
			return
		}
		svcOf := make(map[string]string, len(deps))
		for _, dep := range deps {
			if v, ok := dep.Spec.Template.Labels[d.ServiceLabelKey]; ok {
				svcOf[dep.Namespace+"/"+dep.Name] = v
			}
		}
		pods, err := d.Pods.List(labels.Everything())
		if err != nil {
			klog.Background().Error(err, "observer: list pods")
			return
		}
		ob.ExportSeries(ob.Compute(deps, svcOf, pods))
	}
	export() // first cycle immediately at startup
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			export()
		}
	}
}
