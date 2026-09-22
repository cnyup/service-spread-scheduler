// Package v1alpha1 defines ServiceSpreadArgs: the `pluginConfig.args` payload
// of the ServiceSpread plugin inside a KubeSchedulerConfiguration profile.
//
// This is NOT a CRD. The type is registered into a private scheme
// (apis/config/v1alpha1.SchemeBuilder) and decoded strictly via
// DecodeServiceSpreadArgs so that unknown fields are rejected fail-closed
// (dev-design §3.2). The scheduler binary itself never serves this API.
//
// +kubebuilder:object:generate=true
package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DefaultManagedSchedulerName is the schedulerName of the profile the
	// ServiceSpread plugin registers into when not configured otherwise.
	DefaultManagedSchedulerName = "service-spread-scheduler"
)

// Defaults applied by SetDefaults_ServiceSpreadArgs (dev-design §3.2 /
// design doc 12.1).
const (
	DefaultFallbackCacheTTL = 10 * time.Minute
	DefaultReservationTTL   = 5 * time.Minute
	DefaultReconcilePeriod  = 10 * time.Minute
)

// ServiceSpreadArgs holds the configuration of the ServiceSpread plugin.
//
// +kubebuilder:object:generate=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ServiceSpreadArgs struct {
	metav1.TypeMeta `json:",inline"`

	// ServiceLabelKey is the pod label key that identifies a service. Pods
	// using the managed scheduler that miss this label are rejected with
	// MissingRequiredServiceLabel; there is intentionally no
	// requireServiceLabel switch (dev-design §9 deviation 7).
	// Required.
	ServiceLabelKey string `json:"serviceLabelKey"`

	// RequirePolicy rejects (ServiceSpreadPolicyNotFound) pods that hit no
	// ServiceSpreadPolicy when true. Defaults to true: first release refuses
	// to schedule unmanaged services rather than silently bypassing the hard
	// constraints.
	// +optional
	RequirePolicy *bool `json:"requirePolicy,omitempty"`

	// ManagedSchedulerName is the schedulerName of the profile this plugin
	// serves. Only pods whose spec.schedulerName equals it are handled.
	// Defaults to "service-spread-scheduler".
	// +optional
	ManagedSchedulerName string `json:"managedSchedulerName,omitempty"`

	// FallbackCacheTTL bounds how long the replica-target observer may serve
	// its last good value after HPA/KEDA read failures before marking the
	// metric stale. Replica targets never influence placement decisions, so
	// this timeout never blocks scheduling (design doc 6.2/12.1).
	// Defaults to 10m.
	// +optional
	FallbackCacheTTL metav1.Duration `json:"fallbackCacheTTL,omitempty"`

	// ReservationTTL is the fallback expiry of Reserve-phase placeholder
	// counts. The janitor always verifies before releasing; it never deletes
	// blind (design doc 8.3). Defaults to 5m.
	// +optional
	ReservationTTL metav1.Duration `json:"reservationTTL,omitempty"`

	// ReconcilePeriod is how often the snapshot of per-node pod counts is
	// rebuilt from the pod lister. It only repairs count drift and never
	// touches reservations. Defaults to 10m.
	// +optional
	ReconcilePeriod metav1.Duration `json:"reconcilePeriod,omitempty"`
}
