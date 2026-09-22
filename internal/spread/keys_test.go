package spread

import (
	"encoding/json"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func basePod() *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns1", Name: "p", UID: "uid-1",
		Labels: map[string]string{"svc": "alpha"},
	}}
}

func TestQuotaKeyAndSchedKey(t *testing.T) {
	q := QuotaKey("ns1", "service-spread-scheduler", "svc", "alpha")
	want := "ns1/service-spread-scheduler/svc=alpha"
	if q != want {
		t.Fatalf("quotaKey = %q, want %q", q, want)
	}
	s := SchedKey(q, "abcdef012345")
	if want := q + "/abcdef012345"; s != want {
		t.Fatalf("schedKey = %q, want %q", s, want)
	}
}

func TestDomainHashLengthAndStability(t *testing.T) {
	h, err := DomainHash(basePod())
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != HashChars {
		t.Fatalf("hash length = %d, want %d", len(h), HashChars)
	}
	h2, _ := DomainHash(basePod())
	if h != h2 {
		t.Fatalf("hash not stable: %q vs %q", h, h2)
	}
}

func TestDomainHashMapOrderIndependent(t *testing.T) {
	a := basePod()
	a.Spec.NodeSelector = map[string]string{"zone": "z1", "rack": "r7", "team": "x"}
	b := basePod()
	b.Spec.NodeSelector = map[string]string{"team": "x", "rack": "r7", "zone": "z1"}
	assertSameHash(t, a, b, "nodeSelector key order")

	// Requirement values order inside one matchExpressions entry.
	term := func(values []string) *v1.Pod {
		p := basePod()
		p.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: "k", Operator: v1.NodeSelectorOpIn, Values: values},
					},
				}},
			},
		}}
		return p
	}
	assertSameHash(t, term([]string{"a", "b", "c"}), term([]string{"c", "a", "b"}), "values order")
}

func TestDomainHashTolerationOrderIndependent(t *testing.T) {
	mk := func(order []v1.Toleration) *v1.Pod {
		p := basePod()
		p.Spec.Tolerations = order
		return p
	}
	tols := []v1.Toleration{
		{Key: "t1", Operator: v1.TolerationOpEqual, Value: "v", Effect: v1.TaintEffectNoSchedule},
		{Key: "t2", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoExecute, TolerationSeconds: ptr(int64(30))},
	}
	assertSameHash(t, mk(tols), mk([]v1.Toleration{tols[1], tols[0]}), "toleration order")
}

func TestDomainHashTermOrderIndependent(t *testing.T) {
	terms := []v1.NodeSelectorTerm{
		{MatchExpressions: []v1.NodeSelectorRequirement{{Key: "a", Operator: v1.NodeSelectorOpIn, Values: []string{"1"}}}},
		{MatchFields: []v1.NodeSelectorRequirement{{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{"n"}}}},
	}
	mk := func(swapped bool) *v1.Pod {
		p := basePod()
		ts := append([]v1.NodeSelectorTerm{}, terms...)
		if swapped {
			ts[0], ts[1] = ts[1], ts[0]
		}
		p.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: ts},
		}}
		return p
	}
	assertSameHash(t, mk(false), mk(true), "term order")
}

func TestDomainHashNilVsEmptyConsistent(t *testing.T) {
	a := basePod() // nil nodeSelector, no affinity, no tolerations
	b := basePod()
	b.Spec.NodeSelector = map[string]string{}
	b.Spec.Tolerations = []v1.Toleration{}
	assertSameHash(t, a, b, "nil vs empty")
}

func TestDomainHashPreferredAffinityIgnored(t *testing.T) {
	a := basePod()
	b := basePod()
	b.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
			{Weight: 10, Preference: v1.NodeSelectorTerm{
				MatchExpressions: []v1.NodeSelectorRequirement{{Key: "pref", Operator: v1.NodeSelectorOpExists}},
			}},
		},
	}}
	assertSameHash(t, a, b, "preferred affinity must not affect hash")
}

func TestDomainHashDistinguishesDomains(t *testing.T) {
	a := basePod()
	a.Spec.NodeSelector = map[string]string{"zone": "z1"}
	b := basePod()
	b.Spec.NodeSelector = map[string]string{"zone": "z2"}
	if ha, _ := DomainHash(a); ha == mustHash(b) {
		t.Fatal("different node selectors must yield different hashes")
	}
	// Empty required-affinity term list vs no affinity at all.
	c := basePod()
	c.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{},
	}}
	if mustHash(c) == mustHash(basePod()) {
		t.Fatal("explicit empty term set must be distinguishable from absent affinity")
	}
}

func TestDomainHashCanonicalJSON(t *testing.T) {
	// Guard the canonical form itself: re-marshalling the built spec must be
	// byte-stable (sorted keys via encoding/json map ordering).
	spec := domainSpec{NodeSelector: map[string]string{"b": "2", "a": "1"}}
	b1, _ := json.Marshal(spec)
	spec2 := domainSpec{NodeSelector: map[string]string{"a": "1", "b": "2"}}
	b2, _ := json.Marshal(spec2)
	if !reflect.DeepEqual(b1, b2) {
		t.Fatalf("canonical JSON not stable: %s vs %s", b1, b2)
	}
}

func TestPodRefFor(t *testing.T) {
	p := basePod()
	p.Spec.NodeSelector = map[string]string{"zone": "z1"}
	ref, err := PodRefFor(p, "service-spread-scheduler", "svc")
	if err != nil {
		t.Fatal(err)
	}
	if ref.QuotaKey != "ns1/service-spread-scheduler/svc=alpha" {
		t.Fatalf("quotaKey = %q", ref.QuotaKey)
	}
	want := SchedKey(ref.QuotaKey, mustHash(p))
	if ref.SchedKey != want {
		t.Fatalf("schedKey = %q, want %q", ref.SchedKey, want)
	}

	noLabel := basePod()
	delete(noLabel.Labels, "svc")
	if _, err := PodRefFor(noLabel, "s", "svc"); err == nil {
		t.Fatal("missing service label must error")
	}
}

func assertSameHash(t *testing.T, a, b *v1.Pod, what string) {
	t.Helper()
	ha, err := DomainHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := DomainHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("%s changed the hash: %q vs %q", what, ha, hb)
	}
}

func mustHash(p *v1.Pod) string {
	h, err := DomainHash(p)
	if err != nil {
		panic(err)
	}
	return h
}

func ptr[T any](v T) *T { return &v }
