package spread

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/prometheus/client_golang/prometheus"

	schedulingv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
	configv1alpha1 "github.com/cnyup/service-spread-scheduler/apis/config/v1alpha1"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Plugin-side metrics (dev-design §5.2 step 7; canonical reason names per
// design doc 10.1). Registered on the default registry so the M4 metrics
// server exposes them without extra plumbing.
var (
	filterRejections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "service_spread_filter_rejections_total",
		Help: "Filter rejections by the ServiceSpread plugin, by canonical reason.",
	}, []string{"reason"})
	policyResolutionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "service_spread_policy_resolution_failures_total",
		Help: "PreFilter policy-resolution failures by canonical reason.",
	}, []string{"reason"})
)

func init() {
	prometheus.MustRegister(filterRejections, policyResolutionFailures)
}

const (
	// preFilterStateKey / preScoreStateKey index CycleState.
	preFilterStateKey framework.StateKey = Name + "/preFilter"
	preScoreStateKey  framework.StateKey = Name + "/preScore"
)

// preFilterState is written in PreFilter and consumed by Filter/Score/Reserve.
type preFilterState struct {
	quotaKey  string
	schedKey  string
	policy    *schedulingv1alpha1.ServiceSpreadPolicy
	domainSet map[string]struct{}
	minCount  int32            // min effDomain over the domain
	effDomain map[string]int32 // node -> effective schedKey count
	effQuota  map[string]int32 // node -> effective quotaKey count
}

func (s *preFilterState) Clone() framework.StateData {
	c := *s
	c.domainSet = map[string]struct{}{}
	for k := range s.domainSet {
		c.domainSet[k] = struct{}{}
	}
	c.effDomain = map[string]int32{}
	for k, v := range s.effDomain {
		c.effDomain[k] = v
	}
	c.effQuota = map[string]int32{}
	for k, v := range s.effQuota {
		c.effQuota[k] = v
	}
	return &c
}

// preScoreState carries the feasible-set count extrema for score
// normalization (dev-design §5.5).
type preScoreState struct {
	skip bool
	cmin int32
	cmax int32
}

func (s *preScoreState) Clone() framework.StateData { c := *s; return &c }

// ServiceSpread is the out-of-tree scheduler plugin (dev-design §5).
type ServiceSpread struct {
	args configv1alpha1.ServiceSpreadArgs
	deps pluginDeps
}

var _ framework.PreFilterPlugin = (*ServiceSpread)(nil)
var _ framework.FilterPlugin = (*ServiceSpread)(nil)
var _ framework.PreScorePlugin = (*ServiceSpread)(nil)
var _ framework.ScorePlugin = (*ServiceSpread)(nil)
var _ framework.ReservePlugin = (*ServiceSpread)(nil)
var _ framework.PostBindPlugin = (*ServiceSpread)(nil)
var _ framework.EnqueueExtensions = (*ServiceSpread)(nil)

// NewWithDeps builds the plugin from pre-resolved dependencies (factory or
// tests).
func NewWithDeps(args configv1alpha1.ServiceSpreadArgs, deps pluginDeps) (framework.Plugin, error) {
	if args.ServiceLabelKey == "" {
		return nil, fmt.Errorf("%s: serviceLabelKey is required", Name)
	}
	if args.ManagedSchedulerName == "" {
		args.ManagedSchedulerName = configv1alpha1.DefaultManagedSchedulerName
	}
	if deps.synced == nil {
		deps.synced = func() bool { return true }
	}
	return &ServiceSpread{args: args, deps: deps}, nil
}

func (pl *ServiceSpread) Name() string { return Name }

// getPreFilterState reads CycleState, returning nil for absent data.
func getPreFilterState(cycleState *framework.CycleState) (*preFilterState, *framework.Status) {
	data, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		return nil, framework.NewStatus(framework.Error, "reading preFilterState")
	}
	s, ok := data.(*preFilterState)
	if !ok || s == nil {
		return nil, framework.NewStatus(framework.Error, "invalid preFilterState")
	}
	return s, nil
}

// PreFilter implements dev-design §5.2.
func (pl *ServiceSpread) PreFilter(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// 0. Placement-critical caches must be synced (retryable condition).
	if !pl.deps.synced() {
		return nil, framework.NewStatus(framework.Unschedulable, "CacheNotSynced")
	}

	// 1. Defensive: profile isolation routes only our pods here.
	if pod.Spec.SchedulerName != pl.args.ManagedSchedulerName {
		return nil, framework.NewStatus(framework.Success)
	}

	// 2. Service label is mandatory (quotaKey needs its value).
	labelValue, ok := pod.Labels[pl.args.ServiceLabelKey]
	if !ok {
		policyResolutionFailures.WithLabelValues("MissingRequiredServiceLabel").Inc()
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"MissingRequiredServiceLabel: pod misses required label "+pl.args.ServiceLabelKey)
	}

	// 3. Owner chain Pod→RS→Deployment.
	if !pl.deps.owners.OwnedByDeployment(string(pod.UID), pod.Namespace, pod.OwnerReferences) {
		policyResolutionFailures.WithLabelValues("UnsupportedWorkloadOwner").Inc()
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"UnsupportedWorkloadOwner: pod must be owned by a Deployment via a ReplicaSet")
	}

	// 4. Resolve the policy: same namespace, matching schedulerName and
	// exact single-key selector hit.
	policy, st := pl.resolvePolicy(pod.Namespace, labelValue)
	if st != nil {
		return nil, st
	}

	// 5. Stable spread domain.
	domainSet := computeStableDomain(pod, pl.deps.nodes)
	if len(domainSet) == 0 {
		return nil, framework.NewStatus(framework.Unschedulable, "EmptySpreadDomain")
	}

	// 6. CycleState.
	quotaKey := QuotaKey(pod.Namespace, pl.args.ManagedSchedulerName, pl.args.ServiceLabelKey, labelValue)
	hash, err := DomainHash(pod)
	if err != nil {
		return nil, framework.NewStatus(framework.Error, "computing schedulingDomainHash: "+err.Error())
	}
	schedKey := SchedKey(quotaKey, hash)

	state := &preFilterState{
		quotaKey:  quotaKey,
		schedKey:  schedKey,
		policy:    policy,
		domainSet: domainSet,
		effDomain: map[string]int32{},
		effQuota:  map[string]int32{},
	}
	minCount := int32(-1)
	for node := range domainSet {
		domainN, quotaN := pl.deps.state.effCount(node, schedKey, quotaKey)
		state.effDomain[node] = domainN
		state.effQuota[node] = quotaN
		if minCount < 0 || domainN < minCount {
			minCount = domainN
		}
	}
	state.minCount = minCount
	cycleState.Write(preFilterStateKey, state)
	return nil, framework.NewStatus(framework.Success)
}

func (pl *ServiceSpread) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// resolvePolicy finds the unique policy selecting this service
// (namespace + managed schedulerName + single-key label hit).
func (pl *ServiceSpread) resolvePolicy(namespace, labelValue string) (*schedulingv1alpha1.ServiceSpreadPolicy, *framework.Status) {
	policies, err := pl.deps.policies.List(namespace)
	if err != nil {
		policyResolutionFailures.WithLabelValues("PolicyListError").Inc()
		return nil, framework.NewStatus(framework.Error, "listing ServiceSpreadPolicies: "+err.Error())
	}
	var matches []*schedulingv1alpha1.ServiceSpreadPolicy
	for _, p := range policies {
		if p.Spec.SchedulerName != pl.args.ManagedSchedulerName {
			continue
		}
		if selectorMatchesService(p.Spec.ServiceSelector, pl.args.ServiceLabelKey, labelValue) {
			matches = append(matches, p)
		}
	}
	requirePolicy := true
	if pl.args.RequirePolicy != nil {
		requirePolicy = *pl.args.RequirePolicy
	}
	switch {
	case len(matches) == 0:
		if requirePolicy {
			policyResolutionFailures.WithLabelValues("ServiceSpreadPolicyNotFound").Inc()
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"ServiceSpreadPolicyNotFound: no policy selects this service in namespace "+namespace)
		}
		return nil, nil
	case len(matches) > 1:
		policyResolutionFailures.WithLabelValues("AmbiguousServiceSpreadPolicy").Inc()
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("AmbiguousServiceSpreadPolicy: %d policies select this service", len(matches)))
	default:
		return matches[0], nil
	}
}

// selectorMatchesService evaluates the webhook-enforced single-key
// matchLabels selector against the pod's service label value: exactly one
// matchLabels entry keyed by the cluster-wide serviceLabelKey, no
// matchExpressions. Policies that violate the single-key rule (webhook
// bypassed) never match, which is fail-closed.
func selectorMatchesService(sel metav1.LabelSelector, serviceLabelKey, labelValue string) bool {
	if len(sel.MatchExpressions) != 0 || len(sel.MatchLabels) != 1 {
		return false
	}
	v, ok := sel.MatchLabels[serviceLabelKey]
	return ok && v == labelValue
}

// Filter implements dev-design §5.3.
func (pl *ServiceSpread) Filter(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	state, st := getPreFilterState(cycleState)
	if st != nil {
		return st
	}
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	// 1. Outside the stable spread domain (e.g. NotReady node that still
	// passes native filters thanks to its auto-injected toleration).
	if _, in := state.domainSet[node.Name]; !in {
		filterRejections.WithLabelValues("ServiceSpreadConstraint").Inc()
		return framework.NewStatus(framework.Unschedulable, "NodeOutsideSpreadDomain: node="+node.Name)
	}

	// 2. maxPodsPerNode hard cap (quota key, shared across domains).
	if quota := state.effQuota[node.Name] + 1; quota > state.policy.Spec.MaxPodsPerNode {
		filterRejections.WithLabelValues("ServiceSpreadConstraint").Inc()
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("MaxPodsPerNodeExceeded: node=%s quota=%d/%d limit=%d",
				node.Name, state.effQuota[node.Name], quota, state.policy.Spec.MaxPodsPerNode))
	}

	// 3. maxSkew hard constraint against the domain minimum.
	count := state.effDomain[node.Name]
	if skew := count + 1 - state.minCount; skew > state.policy.Spec.MaxSkew {
		filterRejections.WithLabelValues("ServiceSpreadConstraint").Inc()
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("MaxSkewExceeded: node=%s count=%d min=%d skew=%d maxSkew=%d",
				node.Name, count, state.minCount, skew, state.policy.Spec.MaxSkew))
	}
	return framework.NewStatus(framework.Success)
}

// PreScore implements dev-design §5.5: compute the count extrema over the
// feasible set for score normalization.
func (pl *ServiceSpread) PreScore(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	state, st := getPreFilterState(cycleState)
	if st != nil {
		return st
	}
	if len(nodes) == 0 {
		return framework.NewStatus(framework.Unschedulable, "no feasible nodes")
	}
	cmin, cmax := int32(-1), int32(-1)
	for _, n := range nodes {
		c := state.effDomain[n.Name]
		if cmin < 0 || c < cmin {
			cmin = c
		}
		if c > cmax {
			cmax = c
		}
	}
	ss := &preScoreState{cmin: cmin, cmax: cmax, skip: cmin == cmax}
	cycleState.Write(preScoreStateKey, ss)
	return framework.NewStatus(framework.Success)
}

func (pl *ServiceSpread) Score(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	state, st := getPreFilterState(cycleState)
	if st != nil {
		return 0, st
	}
	data, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, "reading preScoreState")
	}
	ss, ok := data.(*preScoreState)
	if !ok {
		return 0, framework.NewStatus(framework.Error, "invalid preScoreState")
	}
	if ss.skip {
		return 0, framework.NewStatus(framework.Success)
	}
	count := state.effDomain[nodeName]
	score := int64(ss.cmax-count) * framework.MaxNodeScore / int64(ss.cmax-ss.cmin+1)
	return score, framework.NewStatus(framework.Success)
}

func (pl *ServiceSpread) ScoreExtensions() framework.ScoreExtensions { return nil }

// Reserve implements dev-design §5.6: revalidate everything against the
// freshest state inside the reservation critical section.
func (pl *ServiceSpread) Reserve(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	state, st := getPreFilterState(cycleState)
	if st != nil {
		return st
	}

	// Domain membership recheck with the latest node view.
	domain := computeStableDomain(pod, pl.deps.nodes)
	if _, in := domain[nodeName]; !in {
		return framework.NewStatus(framework.Unschedulable,
			"ReserveRevalidationFailed: node="+nodeName+" left the spread domain")
	}

	ok := pl.deps.state.TryReserveChecked(pod.UID, podRef{
		QuotaKey: state.quotaKey, SchedKey: state.schedKey, NodeName: nodeName,
	}, func(g *reserveGuard) bool {
		domainN, quotaN := g.EffCount(nodeName, state.schedKey, state.quotaKey)
		if quotaN+1 > state.policy.Spec.MaxPodsPerNode {
			return false
		}
		// min' recomputed over the current domain with reservations.
		min := int32(-1)
		for n := range domain {
			d, _ := g.EffCount(n, state.schedKey, state.quotaKey)
			if n == nodeName {
				d = domainN
			}
			if min < 0 || d < min {
				min = d
			}
		}
		return domainN+1-min <= state.policy.Spec.MaxSkew
	})
	if !ok {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("ReserveRevalidationFailed: node=%s quota/skew recheck failed", nodeName))
	}
	return framework.NewStatus(framework.Success)
}

func (pl *ServiceSpread) Unreserve(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) {
	pl.deps.state.Unreserve(pod.UID)
}

// PostBind only flips Reserved→BoundObs; release waits for the informer to
// observe the binding (dev-design §4.4).
func (pl *ServiceSpread) PostBind(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) {
	pl.deps.state.MarkBound(pod.UID)
}
