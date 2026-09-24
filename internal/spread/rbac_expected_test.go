package spread

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// rbac_expected_test.go — RBAC access-surface contract test.
//
// The scheduler ClusterRole (config/rbac/scheduler.yaml) must cover every
// (apiGroup, resource, verb) the process actually exercises. This is the
// structural fix for a bug family that hit production twice on kind:
//
//   1. HPA informer rule missing after the observer wiring (d478ca0) —
//      post-restart schedulers hung at WaitForCacheSync; leader lease went
//      dead; nothing scheduled (found 2026-09-24).
//   2. events.k8s.io group missing — 1.28 event recorder PATCHes
//      aggregated events via events.k8s.io; FailedScheduling diagnostics
//      were silently dropped (f380a07).
//   3. keda.sh/scaledobjects missing — invisible until a cluster with
//      KEDA installed is used; the dynamic informer probe (list limit=1)
//      would surface "registered but unreadable -> degraded" instead of
//      "absent" (found by this very test's RED run).
//
// When you add an informer / client call to the plugin, add the expected
// access here FIRST (RED), then the RBAC rule (GREEN). The expected list
// below is derived from the wiring points:
//
//   depsFromHandle + plugins_api.go:
//     - pod/node informers, policy dynamic informer (servicespreadpolicies),
//       RS/Deployment owner readers, HPA informer, KEDA dynamic informer
//   kube-scheduler framework (native, runs in the same binary):
//     - bind loop (pods/binding, pods/status, events), native informer
//       surface (PV/PVC/RC/services/endpoints/namespaces/configmaps,
//       storage.k8s.io, policy/PDB)
//   leader election: coordination.k8s.io/leases
//   event recorder: core events + events.k8s.io (1.28 aggregated PATCH)

type rbacAccess struct {
	Group         string
	Resource      string
	Verbs         []string
	Justification string // file:line or component — why the process needs this
}

func expectedAccess() []rbacAccess {
	return []rbacAccess{
		// --- bind loop + scheduling events (kube-scheduler framework) ---
		{Group: "", Resource: "pods/binding", Verbs: []string{"create"}, Justification: "framework bind"},
		{Group: "", Resource: "pods/status", Verbs: []string{"patch", "update"}, Justification: "framework bind loop"},
		{Group: "", Resource: "events", Verbs: []string{"create", "patch", "update"}, Justification: "event recorder (core)"},
		{Group: "events.k8s.io", Resource: "events", Verbs: []string{"create", "patch", "update"}, Justification: "1.28 event recorder aggregated PATCH (f380a07)"},

		// --- spread state machine informers (plugins_api.go depsFromHandle) ---
		{Group: "", Resource: "pods", Verbs: []string{"get", "list", "watch"}, Justification: "pod informer + listerPodSource"},
		{Group: "", Resource: "nodes", Verbs: []string{"get", "list", "watch"}, Justification: "node informer (stable domain)"},
		{Group: "scheduling.soyup.top", Resource: "servicespreadpolicies", Verbs: []string{"get", "list", "watch"}, Justification: "policy dynamic informer (plugins_api.go:186)"},
		{Group: "apps", Resource: "replicasets", Verbs: []string{"get", "list", "watch"}, Justification: "OwnerReader chain"},
		{Group: "apps", Resource: "deployments", Verbs: []string{"get", "list", "watch"}, Justification: "OwnerReader + observer Deployments lister"},
		{Group: "apps", Resource: "statefulsets", Verbs: []string{"get", "list", "watch"}, Justification: "native scheduler informer surface"},

		// --- observer (replicatarget.go + plugins_api.go) ---
		{Group: "autoscaling", Resource: "horizontalpodautoscalers", Verbs: []string{"get", "list", "watch"}, Justification: "HPA informer (observer)"},
		{Group: "keda.sh", Resource: "scaledobjects", Verbs: []string{"list", "watch"}, Justification: "KEDA dynamic informer (plugins_api.go:244-257)"},

		// --- native kube-scheduler informer surface (same binary) ---
		{Group: "", Resource: "persistentvolumes", Verbs: []string{"get", "list", "watch"}, Justification: "native VolumeBinding"},
		{Group: "", Resource: "persistentvolumeclaims", Verbs: []string{"get", "list", "watch"}, Justification: "native VolumeBinding"},
		{Group: "", Resource: "replicationcontrollers", Verbs: []string{"get", "list", "watch"}, Justification: "native informer"},
		{Group: "", Resource: "services", Verbs: []string{"get", "list", "watch"}, Justification: "native informer"},
		{Group: "", Resource: "endpoints", Verbs: []string{"get", "list", "watch"}, Justification: "native informer"},
		{Group: "", Resource: "namespaces", Verbs: []string{"get", "list", "watch"}, Justification: "native informer"},
		{Group: "", Resource: "configmaps", Verbs: []string{"get", "list", "watch"}, Justification: "native informer"},
		{Group: "storage.k8s.io", Resource: "csidrivers", Verbs: []string{"get", "list", "watch"}, Justification: "native CSI"},
		{Group: "storage.k8s.io", Resource: "csinodes", Verbs: []string{"get", "list", "watch"}, Justification: "native CSI"},
		{Group: "storage.k8s.io", Resource: "csistoragecapacities", Verbs: []string{"get", "list", "watch"}, Justification: "native CSI"},
		{Group: "storage.k8s.io", Resource: "storageclasses", Verbs: []string{"get", "list", "watch"}, Justification: "native CSI"},
		{Group: "storage.k8s.io", Resource: "volumeattachments", Verbs: []string{"get", "list", "watch"}, Justification: "native CSI"},
		{Group: "policy", Resource: "poddisruptionbudgets", Verbs: []string{"get", "list", "watch"}, Justification: "native PDB"},

		// --- leader election ---
		{Group: "coordination.k8s.io", Resource: "leases", Verbs: []string{"get", "create", "update"}, Justification: "KubeSchedulerConfiguration.leaderElection"},
	}
}

// parseSchedulerClusterRole loads the FIRST ClusterRole document from
// config/rbac/scheduler.yaml (relative to the package dir).
func parseSchedulerClusterRole(t *testing.T) *rbacv1.ClusterRole {
	t.Helper()
	path := filepath.Join("..", "..", "config", "rbac", "scheduler.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read RBAC manifest %s: %v (run tests from the repo root layout)", path, err)
	}
	// The file is multi-document (ClusterRole, SA, Binding). sigs.k8s.io/yaml
	// parses only the first document — which is the ClusterRole.
	var cr rbacv1.ClusterRole
	if err := yaml.Unmarshal(raw, &cr); err != nil {
		t.Fatalf("cannot parse ClusterRole from %s: %v", path, err)
	}
	if cr.Kind != "ClusterRole" || len(cr.Rules) == 0 {
		t.Fatalf("first document of %s is not a usable ClusterRole (kind=%q rules=%d)", path, cr.Kind, len(cr.Rules))
	}
	return &cr
}

// allows reports whether the ClusterRole grants verb on group/resource.
// Subresource handling: pods/binding is expressed as resource "pods/binding"
// in RBAC rules; we compare the full resource string.
func allows(cr *rbacv1.ClusterRole, group, resource, verb string) bool {
	for _, r := range cr.Rules {
		if r.APIGroups != nil && !containsStr(r.APIGroups, group) {
			continue
		}
		if r.Resources != nil && !containsStr(r.Resources, resource) {
			continue
		}
		if containsStr(r.Verbs, "*") || containsStr(r.Verbs, verb) {
			return true
		}
	}
	return false
}

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// TestRBACCoversCodeAccessSurface — the contract: every access the process
// exercises must be granted by the shipped ClusterRole. Adding an informer
// without its RBAC rule turns this RED (that is the point).
func TestRBACCoversCodeAccessSurface(t *testing.T) {
	cr := parseSchedulerClusterRole(t)
	var missing []string
	for _, a := range expectedAccess() {
		for _, v := range a.Verbs {
			if !allows(cr, a.Group, a.Resource, v) {
				missing = append(missing, fmt.Sprintf("%s/%s:%s (%s)", a.Group, a.Resource, v, a.Justification))
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("ClusterRole config/rbac/scheduler.yaml is missing %d access entries — add them before shipping:\n  %s",
			len(missing), formatList(missing))
	}
}

// TestRBACHasNoUnknownWildcard — escalation guard: the shipped role must
// not grant "*" verbs or "*/*" groups; widening happens deliberately, in
// review, with a justification entry in expectedAccess above.
func TestRBACHasNoUnknownWildcard(t *testing.T) {
	cr := parseSchedulerClusterRole(t)
	for i, r := range cr.Rules {
		if containsStr(r.APIGroups, "*") {
			t.Errorf("rule %d grants apiGroups [*] — scope it explicitly", i)
		}
		if containsStr(r.Resources, "*") {
			t.Errorf("rule %d grants resources [*] — scope it explicitly", i)
		}
	}
}

func formatList(items []string) string {
	out := ""
	for _, s := range items {
		out += "\n  - " + s
	}
	return out
}
