// Package v1alpha1 contains the ServiceSpreadPolicy API: the CRD that binds a
// service (namespace + service label value) scheduled by the managed
// ServiceSpread scheduler to its spread constraints.
//
// API group: scheduling.soyup.top/v1alpha1.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PolicyNamePrefix is the mandatory prefix of every ServiceSpreadPolicy
	// object name. The remainder is 12 hex characters derived deterministically
	// from (namespace, schedulerName, serviceLabelKey, serviceLabelValue); see
	// internal/webhook.ExpectedPolicyName. Object-name uniqueness in the
	// namespace then guarantees at most one policy per
	// namespace+schedulerName+service (design doc 4.3).
	PolicyNamePrefix = "ssp-"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=ssp
// +kubebuilder:printcolumn:name="Scheduler",type=string,JSONPath=`.spec.schedulerName`
// +kubebuilder:printcolumn:name="Max Skew",type=integer,JSONPath=`.spec.maxSkew`
// +kubebuilder:printcolumn:name="Max Pods/Node",type=integer,JSONPath=`.spec.maxPodsPerNode`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=`.metadata.creationTimestamp`

// ServiceSpreadPolicy declares the spread constraints applied to all pods of
// one service: the pods in one namespace that carry the cluster-wide
// configured service label with a given value and that specify the managed
// scheduler in `spec.schedulerName`.
//
// The object name is deterministic (`ssp-` + 12 hex of the service identity)
// and is enforced by the validating webhook, which also enforces the
// selector single-key rule that cannot be expressed in the structural schema.
type ServiceSpreadPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Desired spread behaviour for the selected service.
	Spec ServiceSpreadPolicySpec `json:"spec,omitempty"`
}

// ServiceSpreadPolicySpec defines the desired spread constraints for the
// selected service.
type ServiceSpreadPolicySpec struct {
	// SchedulerName must exactly match `spec.schedulerName` of the pods this
	// policy selects, and must equal the managed scheduler name configured
	// cluster-wide (shared ConfigMap `service-spread-scheduler-config`).
	// Pods scheduled by any other scheduler never match this policy.
	//
	// +kubebuilder:validation:MinLength=1
	SchedulerName string `json:"schedulerName"`

	// ServiceSelector selects the pods of the service.
	//
	// First release supports exact label matching only: MatchLabels must
	// contain exactly one entry whose key equals the cluster-wide configured
	// serviceLabelKey, and MatchExpressions must be empty. Both rules are
	// enforced by the validating webhook because serviceLabelKey is cluster
	// level configuration and cannot be baked into the structural schema.
	ServiceSelector metav1.LabelSelector `json:"serviceSelector"`

	// MaxSkew is the maximum allowed difference of same-service pod count
	// between any two nodes of the stable spread domain (including the pod
	// being placed). Must be >= 1: with the hard inequality enforced by the
	// plugin, maxSkew=0 could not place even the first pod into an empty
	// domain (design doc 4.2).
	//
	// +kubebuilder:validation:Minimum=1
	MaxSkew int32 `json:"maxSkew"`

	// MaxPodsPerNode is the absolute per-node cap of same-service pods,
	// counted per service quota key and shared across all scheduling domains
	// of the service (design doc 7.1). Must be >= 1. HPA scale-up targets
	// never bypass this cap; scale beyond `eligibleNodes * maxPodsPerNode`
	// is surfaced by capacity alerting instead.
	//
	// +kubebuilder:validation:Minimum=1
	MaxPodsPerNode int32 `json:"maxPodsPerNode"`
}

// +kubebuilder:object:root=true

// ServiceSpreadPolicyList is the list type of ServiceSpreadPolicy.
type ServiceSpreadPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ServiceSpreadPolicy `json:"items"`
}
