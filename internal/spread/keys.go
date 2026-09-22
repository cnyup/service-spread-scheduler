// Package spread holds the ServiceSpread scheduler plugin state machinery.
//
// M2 scope (dev-design §4): service quota/scheduling keys,
// schedulingDomainHash, COW count snapshots and the reservation state
// machine. The plugin extension points (Filter/Reserve/...) land in M3 and
// are the only callers of the exported API here.
package spread

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// QuotaKey is the service quota key (dev-design §4.2):
//
//	quotaKey = ns + "/" + schedulerName + "/" + labelKey + "=" + labelValue
//
// It is the counting dimension of the maxPodsPerNode hard cap and is shared
// across all scheduling domains of the service.
func QuotaKey(namespace, schedulerName, labelKey, labelValue string) string {
	return namespace + "/" + schedulerName + "/" + labelKey + "=" + labelValue
}

// SchedKey is the service scheduling key: quotaKey plus the 12-hex-char
// schedulingDomainHash suffix. It is the aggregation dimension of maxSkew
// balancing and of replica-target observation.
func SchedKey(quotaKey, domainHash string) string {
	return quotaKey + "/" + domainHash
}

// HashChars is the length of the schedulingDomainHash hex string: SHA-256
// over the canonical domainSpec, first 6 bytes (dev-design §4.3).
const HashChars = 12

// domainSpec is the canonical, order-independent form of every pod field
// that determines the stable spread domain (design doc 5.2). Only fields
// that shape the *candidate node set* participate; preferred affinity and
// other soft constraints are intentionally absent.
type domainSpec struct {
	// NodeSelector is pod.spec.nodeSelector with keys sorted by json.Marshal.
	// nil and empty maps normalize identically (dev-design §8.1).
	NodeSelector map[string]string `json:"nodeSelector"`

	// RequiredAffinity is the OR-of-terms normalization of
	// requiredDuringSchedulingIgnoredDuringExecution, including matchFields
	// merged per term. Terms are sorted by their canonical serialization;
	// requirements inside a term are sorted by key/operator/values.
	// No omitempty: nil (absent affinity -> matches all nodes), [] (empty
	// term list -> matches no node) and populated lists must stay distinct.
	RequiredAffinity [][]normRequirement `json:"requiredAffinity"`

	// Tolerations is pod.spec.tolerations sorted by canonical serialization.
	Tolerations []normToleration `json:"tolerations,omitempty"`

	// AddedAffinity is reserved for profile-level added node affinity. v1
	// explicitly forbids configuring it (dev-design §9 deviation 8), so it
	// is always nil and exists only as an extension point.
	AddedAffinity *json.RawMessage `json:"addedAffinity,omitempty"`
}

// normRequirement is one node selector requirement (matchExpressions or
// matchFields entry) in canonical form.
type normRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitempty"`
}

// normToleration is one toleration in canonical form.
type normToleration struct {
	Key               string `json:"key"`
	Operator          string `json:"operator"`
	Value             string `json:"value"`
	Effect            string `json:"effect"`
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty"`
}

// DomainHash returns the 12-hex-char schedulingDomainHash of every stable
// domain field of the pod. The result is deterministic across restarts and
// independent of map or list ordering in the pod object.
func DomainHash(pod *v1.Pod) (string, error) {
	spec := domainSpec{NodeSelector: pod.Spec.NodeSelector}
	if spec.NodeSelector == nil {
		spec.NodeSelector = map[string]string{}
	}

	if na := pod.Spec.Affinity; na != nil && na.NodeAffinity != nil &&
		na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		// An explicitly empty term list matches NO node, while absent
		// affinity matches every node — keep them distinguishable with an
		// explicit empty-term marker.
		spec.RequiredAffinity = [][]normRequirement{}
		for _, term := range na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			reqs := make([]normRequirement, 0, len(term.MatchExpressions)+len(term.MatchFields))
			for _, r := range term.MatchExpressions {
				reqs = append(reqs, normRequirement{Key: r.Key, Operator: string(r.Operator), Values: sortedCopy(r.Values)})
			}
			for _, r := range term.MatchFields {
				reqs = append(reqs, normRequirement{Key: r.Key, Operator: string(r.Operator), Values: sortedCopy(r.Values)})
			}
			if len(reqs) == 0 {
				// An empty term matches no node; keep it as a distinct term
				// ([]) so the hash distinguishes it from "no affinity".
				spec.RequiredAffinity = append(spec.RequiredAffinity, []normRequirement{})
				continue
			}
			sortReqs(reqs)
			spec.RequiredAffinity = append(spec.RequiredAffinity, reqs)
		}
		sortTermSlice(spec.RequiredAffinity)
	}

	for _, t := range pod.Spec.Tolerations {
		spec.Tolerations = append(spec.Tolerations, normToleration{
			Key:               t.Key,
			Operator:          string(t.Operator),
			Value:             t.Value,
			Effect:            string(t.Effect),
			TolerationSeconds: t.TolerationSeconds,
		})
	}
	sortTols(spec.Tolerations)

	buf, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshal domainSpec: %w", err)
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])[:HashChars], nil
}

// PodRefFor computes the quota/scheduling keys of a managed pod. It returns
// an error when the service label is missing — such pods are rejected by the
// plugin before any state is touched, so callers never store partial refs.
// NodeName is carried from the pod so per-node counting works from the same
// value; for unbound pods it is empty and the events layer refuses to count.
func PodRefFor(pod *v1.Pod, schedulerName, serviceLabelKey string) (ref podRef, err error) {
	value, ok := pod.Labels[serviceLabelKey]
	if !ok {
		return podRef{}, fmt.Errorf("pod %s/%s misses required service label %q", pod.Namespace, pod.Name, serviceLabelKey)
	}
	quota := QuotaKey(pod.Namespace, schedulerName, serviceLabelKey, value)
	hash, err := DomainHash(pod)
	if err != nil {
		return podRef{}, err
	}
	return podRef{QuotaKey: quota, SchedKey: SchedKey(quota, hash), NodeName: pod.Spec.NodeName}, nil
}

func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

func sortReqs(reqs []normRequirement) {
	sort.Slice(reqs, func(i, j int) bool {
		a, b := reqs[i], reqs[j]
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		if a.Operator != b.Operator {
			return a.Operator < b.Operator
		}
		return joinValues(a.Values) < joinValues(b.Values)
	})
}

func joinValues(vs []string) string {
	return strings.Join(vs, "\x00")
}

func sortTermSlice(terms [][]normRequirement) {
	canonical := make([]string, len(terms))
	for i, t := range terms {
		b, _ := json.Marshal(t)
		canonical[i] = string(b)
	}
	sort.SliceStable(terms, func(i, j int) bool {
		return canonical[i] < canonical[j]
	})
}

func sortTols(tols []normToleration) {
	canonical := make([]string, len(tols))
	for i, t := range tols {
		b, _ := json.Marshal(t)
		canonical[i] = string(b)
	}
	sort.SliceStable(tols, func(i, j int) bool {
		return canonical[i] < canonical[j]
	})
}

// types.UID re-export keeps plugin-facing signatures stable.
type PodUID = types.UID
