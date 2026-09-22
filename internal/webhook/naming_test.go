package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"testing"
)

// sha256Hex mirrors the canonical construction independently, so the golden
// vector pins the encoding itself, not the implementation's internals.
func sha256Hex(t *testing.T, fields ...string) string {
	t.Helper()
	canonical, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])[:12]
}

var namePattern = regexp.MustCompile(`^ssp-[0-9a-f]{12}$`)

func TestExpectedPolicyNameFormat(t *testing.T) {
	name := ExpectedPolicyName("production", "service-spread-scheduler", "app.kubernetes.io/name", "image-processing")
	if !namePattern.MatchString(name) {
		t.Fatalf("name %q does not match ssp-<12 hex>", name)
	}
	if len(name) != len("ssp-")+12 {
		t.Fatalf("name %q has wrong length %d", name, len(name))
	}
}

func TestExpectedPolicyNameDeterministic(t *testing.T) {
	a := ExpectedPolicyName("production", "service-spread-scheduler", "app.kubernetes.io/name", "image-processing")
	b := ExpectedPolicyName("production", "service-spread-scheduler", "app.kubernetes.io/name", "image-processing")
	if a != b {
		t.Fatalf("same identity must produce the same name, got %q and %q", a, b)
	}
}

func TestExpectedPolicyNameDistinctInputs(t *testing.T) {
	base := ExpectedPolicyName("ns", "sched", "key", "value")
	cases := []struct {
		name string
		got  string
	}{
		{"namespace", ExpectedPolicyName("ns2", "sched", "key", "value")},
		{"schedulerName", ExpectedPolicyName("ns", "sched2", "key", "value")},
		{"labelKey", ExpectedPolicyName("ns", "sched", "key2", "value")},
		{"labelValue", ExpectedPolicyName("ns", "sched", "key", "value2")},
		{"empty value", ExpectedPolicyName("ns", "sched", "key", "")},
	}
	for _, tc := range cases {
		if tc.got == base {
			t.Errorf("%s change must change the name, both are %q", tc.name, base)
		}
		if !namePattern.MatchString(tc.got) {
			t.Errorf("%s: name %q malformed", tc.name, tc.got)
		}
	}
}

// The canonical encoding must be injective for arbitrary field values: even
// tuples whose fields contain the classic separator characters must never
// collide.
func TestExpectedPolicyNameSeparatorUnambiguous(t *testing.T) {
	a := ExpectedPolicyName("ns", "sc|hed", "k", "v")
	b := ExpectedPolicyName("ns", "sc", "hed|k", "v")
	if a == b {
		t.Fatalf("field-boundary ambiguity: (%q) and (%q) collided on %q", "sc|hed", "hed|k", a)
	}
	c := ExpectedPolicyName("ns", "sc\",", "k", "v")
	d := ExpectedPolicyName("ns", "sc", ",k", "v")
	if c == d {
		t.Fatalf("quote/comma ambiguity collided on %q", c)
	}
}

// Reference vector: guards against accidental changes of the canonical
// encoding, which would silently invalidate every existing policy name.
func TestExpectedPolicyNameGoldenVector(t *testing.T) {
	got := ExpectedPolicyName("production", "service-spread-scheduler", "app.kubernetes.io/name", "image-processing")
	want := "ssp-" + sha256Hex(t, "production", "service-spread-scheduler", "app.kubernetes.io/name", "image-processing")
	if got != want {
		t.Fatalf("name changed: got %q want %q", got, want)
	}
}
