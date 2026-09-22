package webhook

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	sspv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
)

// serviceSelectorValue validates spec.serviceSelector under the cluster-level
// serviceLabelKey and returns the selected service label value on success.
//
// First release supports exact matching on exactly one label (design doc 4.2):
// MatchLabels must contain exactly the serviceLabelKey entry and
// MatchExpressions must be empty. This cannot live in the structural schema
// because serviceLabelKey is cluster-level configuration.
func serviceSelectorValue(sel metav1.LabelSelector, cfg PluginConfig, basePath *field.Path) (string, field.ErrorList) {
	var allErrs field.ErrorList
	if len(sel.MatchExpressions) > 0 {
		allErrs = append(allErrs, field.Forbidden(
			basePath.Child("matchExpressions"),
			"matchExpressions are not supported; the service selector must be a single exact label match",
		))
	}
	switch {
	case len(sel.MatchLabels) == 1:
		if value, ok := sel.MatchLabels[cfg.ServiceLabelKey]; ok {
			return value, allErrs
		}
		allErrs = append(allErrs, field.Invalid(
			basePath.Child("matchLabels"),
			sel.MatchLabels,
			fmt.Sprintf("must contain exactly one entry with key %q (the cluster-wide serviceLabelKey)", cfg.ServiceLabelKey),
		))
	case len(sel.MatchLabels) == 0:
		allErrs = append(allErrs, field.Required(
			basePath.Child("matchLabels"),
			fmt.Sprintf("must contain exactly one entry with key %q (the cluster-wide serviceLabelKey)", cfg.ServiceLabelKey),
		))
	default:
		allErrs = append(allErrs, field.Invalid(
			basePath.Child("matchLabels"),
			sel.MatchLabels,
			fmt.Sprintf("must contain exactly one entry with key %q; multi-label selectors are ambiguous with the service identity", cfg.ServiceLabelKey),
		))
	}
	return "", allErrs
}

// ValidatePolicy performs the full static validation of a ServiceSpreadPolicy
// against the shared cluster-level configuration (design doc 4.2/4.3,
// dev-design §7.1):
//
//  1. spec.schedulerName must equal the managed scheduler name;
//  2. spec.serviceSelector must select exactly one serviceLabelKey match;
//  3. maxSkew >= 1 and maxPodsPerNode >= 1 (also in the CRD schema; the
//     webhook re-checks defensively in case the CRD is ever relaxed);
//  4. metadata.name must equal the deterministic name derived from the
//     service identity, which makes "one policy per namespace + schedulerName
//     + service" guaranteed by API object-name uniqueness.
//
// It never performs capacity checks: those need per-deployment domain node
// counts that are unavailable at admission time (design doc 12.2).
func ValidatePolicy(policy *sspv1alpha1.ServiceSpreadPolicy, cfg PluginConfig) field.ErrorList {
	var allErrs field.ErrorList
	specPath := field.NewPath("spec")

	// 1. schedulerName: non-empty and exactly the managed scheduler.
	if policy.Spec.SchedulerName == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("schedulerName"), "must not be empty"))
	} else if policy.Spec.SchedulerName != cfg.ManagedSchedulerName {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("schedulerName"),
			policy.Spec.SchedulerName,
			fmt.Sprintf("must exactly equal the managed scheduler name %q (design doc 4.2)", cfg.ManagedSchedulerName),
		))
	}

	// 2. serviceSelector: single exact serviceLabelKey match.
	labelValue, selectorErrs := serviceSelectorValue(policy.Spec.ServiceSelector, cfg, specPath.Child("serviceSelector"))
	allErrs = append(allErrs, selectorErrs...)

	// 3. Numeric constraints (schema-first; defensive re-check).
	if policy.Spec.MaxSkew < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("maxSkew"), policy.Spec.MaxSkew, "must be an integer >= 1"))
	}
	if policy.Spec.MaxPodsPerNode < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("maxPodsPerNode"), policy.Spec.MaxPodsPerNode, "must be an integer >= 1"))
	}

	// 4. Deterministic naming, checked as soon as the identity is derivable.
	if policy.Spec.SchedulerName != "" && labelValue != "" {
		expected := ExpectedPolicyName(policy.Namespace, policy.Spec.SchedulerName, cfg.ServiceLabelKey, labelValue)
		if policy.Name != expected {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("metadata", "name"),
				policy.Name,
				fmt.Sprintf("must equal the deterministic name %q (ssp- + first 12 hex of SHA-256 over the JSON encoding of [namespace, schedulerName, serviceLabelKey, serviceLabelValue])", expected),
			))
		}
	}

	return allErrs
}

// selectsSameService reports whether an existing policy selects the same
// service (same namespace + schedulerName + serviceLabelKey=value) as the one
// being admitted. True overlaps are impossible between correctly named
// policies (they would need identical names); this exists to catch policies
// created while the webhook was unavailable, whose presence would make the
// plugin reject scheduling with AmbiguousServiceSpreadPolicy (design doc 4.3
// runtime fallback).
func selectsSameService(existing, incoming *sspv1alpha1.ServiceSpreadPolicy, cfg PluginConfig) bool {
	if existing.Name == incoming.Name {
		return false
	}
	if existing.Spec.SchedulerName != incoming.Spec.SchedulerName {
		return false
	}
	if len(existing.Spec.ServiceSelector.MatchLabels) != 1 ||
		len(existing.Spec.ServiceSelector.MatchExpressions) != 0 {
		return false
	}
	existingValue, ok := existing.Spec.ServiceSelector.MatchLabels[cfg.ServiceLabelKey]
	if !ok {
		return false
	}
	incomingValue, ok := incoming.Spec.ServiceSelector.MatchLabels[cfg.ServiceLabelKey]
	return ok && existingValue == incomingValue
}
