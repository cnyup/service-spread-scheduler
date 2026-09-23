package spread

import (
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"github.com/prometheus/client_golang/prometheus"
)

// Replica-target observer (dev-design §6; design doc §6/§10.3): metrics
// only, never participates in placement decisions. Runs inside the
// scheduler process sharing the same informers (dev-design §9 deviation 4).

// targetSource labels where a deployment's replica target came from.
type targetSource string

const (
	targetSourceHPA         targetSource = "hpa"
	targetSourceKEDAFallback targetSource = "kedaFallback"
	targetSourceDeployment  targetSource = "deployment"
	targetSourceLastGood    targetSource = "lastGood"
	targetSourceFallback    targetSource = "ReplicaTargetFallback"
)

// deploymentTarget is the resolved (value, provenance) pair of one
// deployment. It doubles as the lastGood cache entry.
type deploymentTarget struct {
	value  int32
	source targetSource
}

// TargetSource is the read surface the observer needs for HPA/KEDA data.
// Implementations wrap informer listers (production) or fakes (tests).
// KEDA is consumed through a minimal projection: ScaledObjectMinReplicas
// reports (minReplicaCount, managed) for the ScaledObject whose
// scaleTargetRef matches the deployment.
type TargetSource interface {
	// HPAsFor returns the HPAs whose scaleTargetRef points at the
	// deployment (nil/empty when none).
	HPAsFor(deployment string) ([]*autoscalingv2.HorizontalPodAutoscaler, error)
	// ScaledObjectMinReplicas returns (minReplicaCount, true, nil) when a
	// KEDA ScaledObject targets the deployment.
	ScaledObjectMinReplicas(deployment string) (int32, bool, error)
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
}

// ObserverDeps carries observer configuration (mirrors ServiceSpreadArgs
// fields relevant here).
type ObserverDeps struct {
	ServiceLabelKey string
	Targets         TargetSource
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

	hpas, hpaErr := src.HPAsFor(dep.Name)
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

	if min, managed, soErr := src.ScaledObjectMinReplicas(dep.Name); soErr == nil && managed {
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

	type svcAgg struct {
		desired   int32
		observed  int32
		bound     int32
		fallback  bool
	}
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
		series = append(series, DomainSeries{
			Namespace:      nsOf[key],
			Service:        key[len(nsOf[key])+1:],
			Domain:         "default", // single-domain until multi-domain export lands (§6 detail metric)
			ReplicaTarget:  aggregateDomain(a.desired, a.observed),
			ObservedPods:   a.observed,
			BoundPods:      a.bound,
			FallbackActive: a.fallback,
		})
	}
	return series
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
	}
}
