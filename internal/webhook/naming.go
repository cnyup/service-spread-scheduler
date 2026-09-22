package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	sspv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
)

// ExpectedPolicyName returns the deterministic ServiceSpreadPolicy object name
// for a service identity: "ssp-" + first 12 hex characters (48 bits) of
// SHA-256 over the JSON encoding of the identity tuple
// [namespace, schedulerName, labelKey, labelValue] (design doc 4.3,
// dev-design §7.1.1).
//
// The canonical form is a JSON array rather than a "|"‑joined string: JSON
// string escaping makes the encoding injective for ANY field values, so the
// mapping from identity to name is collision-free by construction and does
// not have to rely on Kubernetes input validation excluding "|" from the
// fields. Go's json.Marshal of a []string is deterministic (fixed element
// order, fixed escaping rules).
func ExpectedPolicyName(namespace, schedulerName, labelKey, labelValue string) string {
	canonical, err := json.Marshal([4]string{namespace, schedulerName, labelKey, labelValue})
	if err != nil {
		// json.Marshal of a [4]string cannot fail; fall back to a clearly
		// non-canonical encoding rather than panicking inside admission.
		canonical = []byte(namespace + "/" + schedulerName + "/" + labelKey + "/" + labelValue)
	}
	sum := sha256.Sum256(canonical)
	return sspv1alpha1.PolicyNamePrefix + hex.EncodeToString(sum[:])[:12]
}
