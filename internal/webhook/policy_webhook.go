package webhook

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sspv1alpha1 "gitlab.ttyuyin.com/T5428/service-spread-scheduler/api/v1alpha1"
)

// PolicyValidator implements admission.CustomValidator for CREATE/UPDATE of
// ServiceSpreadPolicy (dev-design §7.1). It fails closed: if the shared
// configuration is not loaded (or invalid), requests are rejected with an
// error instead of guessed at.
type PolicyValidator struct {
	// Cfg serves the shared cluster-level configuration.
	Cfg ConfigSource
	// Reader lists existing policies for the overlap check. It should be a
	// manager APIReader (live API, no cache lag): the check must see policies
	// created while this webhook was unavailable.
	Reader client.Reader
}

var policyGroupKind = schema.GroupKind{Group: sspv1alpha1.GroupVersion.Group, Kind: "ServiceSpreadPolicy"}

var _ admission.CustomValidator = (*PolicyValidator)(nil)

// ValidateCreate validates new policies.
func (v *PolicyValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, v.validate(ctx, obj)
}

// ValidateUpdate validates updated policies. The deterministic-name rule
// automatically rejects identity changes (schedulerName / selector value):
// the object name is immutable, so any identity change breaks the name match.
func (v *PolicyValidator) ValidateUpdate(ctx context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return nil, v.validate(ctx, newObj)
}

// ValidateDelete is a no-op: deletion is always allowed.
func (v *PolicyValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *PolicyValidator) validate(ctx context.Context, obj runtime.Object) error {
	policy, ok := obj.(*sspv1alpha1.ServiceSpreadPolicy)
	if !ok {
		return fmt.Errorf("expected a ServiceSpreadPolicy, got %T", obj)
	}

	// Fail closed until the shared configuration is available and valid.
	cfg, err := v.Cfg.Get()
	if err != nil {
		return fmt.Errorf("webhook configuration not usable, failing closed: %w", err)
	}

	errs := ValidatePolicy(policy, cfg)
	if len(errs) == 0 {
		errs = append(errs, v.checkOverlap(ctx, policy, cfg)...)
	}
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(policyGroupKind, policy.Name, errs)
}

// checkOverlap rejects admission when a different object already selects the
// same service. Between correctly named policies this is unreachable (same
// identity ⇒ same deterministic name ⇒ same object); it exists to surface
// rogue policies created while the webhook was unavailable instead of letting
// the plugin fail later with AmbiguousServiceSpreadPolicy (design doc 4.3).
func (v *PolicyValidator) checkOverlap(ctx context.Context, policy *sspv1alpha1.ServiceSpreadPolicy, cfg PluginConfig) field.ErrorList {
	if v.Reader == nil {
		return nil
	}
	var list sspv1alpha1.ServiceSpreadPolicyList
	if err := v.Reader.List(ctx, &list, client.InNamespace(policy.Namespace)); err != nil {
		// Fail closed on list errors: we cannot prove absence of overlap.
		return field.ErrorList{field.InternalError(field.NewPath("metadata", "name"),
			fmt.Errorf("overlap pre-check failed (failing closed): %w", err))}
	}
	for i := range list.Items {
		existing := &list.Items[i]
		if selectsSameService(existing, policy, cfg) {
			return field.ErrorList{field.Invalid(field.NewPath("metadata", "name"),
				policy.Name,
				fmt.Sprintf("policy %q already selects the same service (namespace %q, schedulerName %q, %s=%s); delete it first",
					existing.Name, policy.Namespace, policy.Spec.SchedulerName, cfg.ServiceLabelKey,
					policy.Spec.ServiceSelector.MatchLabels[cfg.ServiceLabelKey]))}
		}
	}
	return nil
}
