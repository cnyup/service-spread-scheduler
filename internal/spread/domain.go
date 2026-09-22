package spread

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
	v1helper "k8s.io/component-helpers/scheduling/corev1"

	nodeutil "k8s.io/kubernetes/pkg/util/node"
)

// NodeReader abstracts node listing for domain computation and tests.
type NodeReader interface {
	List() ([]*v1.Node, error)
}

type nodeListerReader struct {
	nodes corev1listers.NodeLister
}

func (n nodeListerReader) List() ([]*v1.Node, error) { return n.nodes.List(labels.Everything()) }

// NewNodeReader adapts a core/v1 NodeLister to NodeReader.
func NewNodeReader(l corev1listers.NodeLister) NodeReader { return nodeListerReader{nodes: l} }

// nodeInStableDomain reports whether the node belongs to the pod's stable
// spread domain (dev-design §5.4): Ready, schedulable, nodeSelector and
// required node affinity match, and every NoSchedule/NoExecute taint is
// tolerated. Transient conditions (resources, volumes, images) are
// deliberately excluded — they are the native Filter chain's business.
func nodeInStableDomain(pod *v1.Pod, node *v1.Node) bool {
	if node == nil {
		return false
	}
	if !nodeutil.IsNodeReady(node) || node.Spec.Unschedulable {
		return false
	}
	if !matchesNodeSelector(pod, node) {
		return false
	}
	// Only NoSchedule/NoExecute taints gate placement; PreferNoSchedule is
	// advisory and does not shape the stable domain.
	if _, untolerated := v1helper.FindMatchingUntoleratedTaint(
		node.Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
		}); untolerated {
		return false
	}
	return true
}

// matchesNodeSelector checks pod.spec.nodeSelector and required node
// affinity (including matchFields) against the node.
func matchesNodeSelector(pod *v1.Pod, node *v1.Node) bool {
	nodeLabels := labels.Set(node.Labels)
	for k, v := range pod.Spec.NodeSelector {
		if nodeLabels[k] != v {
			return false
		}
	}
	if na := pod.Spec.Affinity; na != nil && na.NodeAffinity != nil &&
		na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		// An explicitly empty term list matches no node (k8s semantics).
		terms := na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(terms) == 0 {
			return false
		}
		for _, term := range terms {
			if matchNodeSelectorTerm(term, node) {
				return true
			}
		}
		return false
	}
	return true
}

// matchNodeSelectorTerm implements the AND-of-requirements semantics of one
// NodeSelectorTerm (matchExpressions and matchFields) for a concrete node.
func matchNodeSelectorTerm(term v1.NodeSelectorTerm, node *v1.Node) bool {
	metaLabels := labels.Set(node.ObjectMeta.Labels)
	for _, r := range term.MatchExpressions {
		v := metaLabels.Get(r.Key)
		if !requirementHolds(r, v) {
			return false
		}
	}
	// matchFields supports only metadata.name in practice.
	for _, r := range term.MatchFields {
		v := ""
		if r.Key == "metadata.name" {
			v = node.Name
		}
		if !requirementHolds(r, v) {
			return false
		}
	}
	return true
}

// requirementHolds evaluates one requirement against a single value;
// absent key means "".
func requirementHolds(r v1.NodeSelectorRequirement, value string) bool {
	present := value != ""
	switch r.Operator {
	case v1.NodeSelectorOpIn:
		return present && contains(r.Values, value)
	case v1.NodeSelectorOpNotIn:
		return !contains(r.Values, value)
	case v1.NodeSelectorOpExists:
		return present
	case v1.NodeSelectorOpDoesNotExist:
		return !present
	case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
		if !present || len(r.Values) != 1 {
			return false
		}
		// Numeric comparisons on label values are only meaningful for
		// well-formed integers; parse failures reject the node.
		n, ok := atoi64(value)
		lim, ok2 := atoi64(r.Values[0])
		if !ok || !ok2 {
			return false
		}
		if r.Operator == v1.NodeSelectorOpGt {
			return n > lim
		}
		return n < lim
	default:
		return false
	}
}

func contains(vs []string, s string) bool {
	for _, v := range vs {
		if v == s {
			return true
		}
	}
	return false
}

func atoi64(s string) (int64, bool) {
	var n int64
	var neg bool
	if s == "" {
		return 0, false
	}
	i := 0
	if s[0] == '-' {
		neg = true
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int64(s[i]-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

// computeStableDomain scans every node and returns the stable spread domain
// of the pod as a name set. Errors surface as an empty domain; callers
// (PreFilter) then reject with EmptySpreadDomain, which Node events
// eventually resolve.
func computeStableDomain(pod *v1.Pod, nodes NodeReader) map[string]struct{} {
	domain := map[string]struct{}{}
	list, err := nodes.List()
	if err != nil {
		return domain
	}
	for _, n := range list {
		if nodeInStableDomain(pod, n) {
			domain[n.Name] = struct{}{}
		}
	}
	return domain
}

// domainNodes returns the names of a domain set for iteration-free checks.
func domainNodes(domain map[string]struct{}) []string {
	out := make([]string, 0, len(domain))
	for n := range domain {
		out = append(out, n)
	}
	return out
}
