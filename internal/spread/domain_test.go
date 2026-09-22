package spread

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func domPod() *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: v1.PodSpec{SchedulerName: "service-spread-scheduler"}}
}

func domNode(name string, mutators ...func(*v1.Node)) *v1.Node {
	n := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: v1.NodeStatus{Conditions: []v1.NodeCondition{
			{Type: v1.NodeReady, Status: v1.ConditionTrue},
		}}}
	for _, m := range mutators {
		m(n)
	}
	return n
}

func withUnschedulable(n *v1.Node) { n.Spec.Unschedulable = true }

func withNotReady(n *v1.Node) {
	for i := range n.Status.Conditions {
		n.Status.Conditions[i].Status = v1.ConditionFalse
	}
}

func TestDomainReadyCordonMatrix(t *testing.T) {
	p := domPod()
	if nodeInStableDomain(p, domNode("ok")) != true {
		t.Fatal("ready schedulable node must be in domain")
	}
	if nodeInStableDomain(p, domNode("cordoned", withUnschedulable)) {
		t.Fatal("cordoned node must be excluded")
	}
	if nodeInStableDomain(p, domNode("notready", withNotReady)) {
		t.Fatal("NotReady node must be excluded")
	}
	if nodeInStableDomain(p, nil) {
		t.Fatal("nil node must be excluded")
	}
}

func TestDomainNodeSelectorMatching(t *testing.T) {
	p := domPod()
	p.Spec.NodeSelector = map[string]string{"zone": "z1"}

	in := domNode("in", func(n *v1.Node) { n.Labels["zone"] = "z1" })
	out := domNode("out", func(n *v1.Node) { n.Labels["zone"] = "z2" })
	missing := domNode("missing")

	if !nodeInStableDomain(p, in) || nodeInStableDomain(p, out) || nodeInStableDomain(p, missing) {
		t.Fatal("nodeSelector matching broken")
	}
}

func TestDomainRequiredAffinityMatching(t *testing.T) {
	p := domPod()
	p.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: "gpu", Operator: v1.NodeSelectorOpIn, Values: []string{"a100"}},
				},
			}},
		},
	}}
	in := domNode("in", func(n *v1.Node) { n.Labels["gpu"] = "a100" })
	out := domNode("out", func(n *v1.Node) { n.Labels["gpu"] = "v100" })
	if !nodeInStableDomain(p, in) || nodeInStableDomain(p, out) {
		t.Fatal("required affinity matching broken")
	}

	// Empty term list matches no node.
	empty := domPod()
	empty.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{},
	}}
	if nodeInStableDomain(empty, domNode("any")) {
		t.Fatal("empty term list must match no node")
	}
}

func TestDomainTaintTolerationMatrix(t *testing.T) {
	noSchedule := v1.Taint{Key: "k", Value: "v", Effect: v1.TaintEffectNoSchedule}
	pod := domPod()
	tainted := domNode("t", func(n *v1.Node) { n.Spec.Taints = []v1.Taint{noSchedule} })
	if nodeInStableDomain(pod, tainted) {
		t.Fatal("untolerated NoSchedule taint must exclude node")
	}

	pod.Spec.Tolerations = []v1.Toleration{{Key: "k", Operator: v1.TolerationOpEqual, Value: "v", Effect: v1.TaintEffectNoSchedule}}
	if !nodeInStableDomain(pod, tainted) {
		t.Fatal("tolerated taint must keep node in domain")
	}

	// PreferNoSchedule never excludes.
	adv := domNode("adv", func(n *v1.Node) {
		n.Spec.Taints = []v1.Taint{{Key: "k", Effect: v1.TaintEffectPreferNoSchedule}}
	})
	if !nodeInStableDomain(domPod(), adv) {
		t.Fatal("PreferNoSchedule must not shape the domain")
	}
}

type fakeNodes struct{ nodes []*v1.Node }

func (f fakeNodes) List() ([]*v1.Node, error) { return f.nodes, nil }

func TestComputeStableDomain(t *testing.T) {
	p := domPod()
	p.Spec.NodeSelector = map[string]string{"zone": "z1"}
	nodes := fakeNodes{[]*v1.Node{
		domNode("n1", func(n *v1.Node) { n.Labels["zone"] = "z1" }),
		domNode("n2", func(n *v1.Node) { n.Labels["zone"] = "z2" }),
		domNode("n3", func(n *v1.Node) { n.Labels["zone"] = "z1" }, withUnschedulable),
	}}
	d := computeStableDomain(p, nodes)
	if len(d) != 1 {
		t.Fatalf("domain = %v, want [n1]", d)
	}
	if _, ok := d["n1"]; !ok {
		t.Fatal("n1 missing")
	}
}
