package v1alpha1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// ArgsKind is the kind under which ServiceSpreadArgs is registered in
// SchemeBuilder. pluginConfig.args blocks may carry
// apiVersion: config.scheduling.soyup.top/v1alpha1 and kind: ServiceSpreadArgs
// explicitly; they are defaulted when omitted.
const ArgsKind = "ServiceSpreadArgs"

var (
	// GroupVersion is the group/version of the ServiceSpreadArgs config API.
	GroupVersion = schema.GroupVersion{Group: "config.scheduling.soyup.top", Version: "v1alpha1"}

	// SchemeBuilder registers ServiceSpreadArgs into a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds apis/config/v1alpha1 types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&ServiceSpreadArgs{})
}

// DecodeServiceSpreadArgs converts the runtime object handed to the plugin
// factory by the scheduler framework into ServiceSpreadArgs, applying defaults
// and validating the result.
//
// The framework passes pluginConfig.args as *runtime.Unknown holding raw JSON
// (k8s 1.28 frameworkruntime.DecodeInto has the same contract). We do NOT use
// frameworkruntime.DecodeInto directly because it decodes non-strictly:
// unknown fields would be silently ignored, violating the fail-closed
// requirement in dev-design §3.2 — a typo in an args key must break startup
// loudly instead of silently running with defaults. The TypeMeta inside the
// raw payload is optional; if present it must identify this group/version/kind.
func DecodeServiceSpreadArgs(obj runtime.Object) (*ServiceSpreadArgs, error) {
	if obj == nil {
		return nil, fmt.Errorf("ServiceSpread plugin requires pluginConfig args (missing pluginConfig entry for %q)", DefaultManagedSchedulerName)
	}

	args := &ServiceSpreadArgs{}
	switch t := obj.(type) {
	case *ServiceSpreadArgs:
		args = t.DeepCopy()
	case *runtime.Unknown:
		dec := json.NewDecoder(bytes.NewReader(t.Raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(args); err != nil {
			return nil, fmt.Errorf("decoding ServiceSpreadArgs: %w (unknown fields are rejected)", err)
		}
	default:
		return nil, fmt.Errorf("unsupported pluginConfig args type %T", obj)
	}

	// TypeMeta, when present, must identify this API.
	if args.APIVersion != "" && args.APIVersion != GroupVersion.String() {
		return nil, fmt.Errorf("ServiceSpreadArgs apiVersion %q is not supported (expected %q)",
			args.APIVersion, GroupVersion.String())
	}
	if args.Kind != "" && args.Kind != ArgsKind {
		return nil, fmt.Errorf("ServiceSpreadArgs kind %q is not supported (expected %q)", args.Kind, ArgsKind)
	}

	SetDefaults_ServiceSpreadArgs(args)
	if err := ValidateServiceSpreadArgs(args); err != nil {
		return nil, err
	}
	return args, nil
}

// SetDefaults_ServiceSpreadArgs fills the documented defaults (design doc
// 12.1): requirePolicy=true, managedSchedulerName=service-spread-scheduler,
// fallbackCacheTTL=10m, reservationTTL=5m, reconcilePeriod=10m.
func SetDefaults_ServiceSpreadArgs(args *ServiceSpreadArgs) {
	if args.ManagedSchedulerName == "" {
		args.ManagedSchedulerName = DefaultManagedSchedulerName
	}
	if args.RequirePolicy == nil {
		requirePolicy := true
		args.RequirePolicy = &requirePolicy
	}
	if args.FallbackCacheTTL.Duration == 0 {
		args.FallbackCacheTTL.Duration = DefaultFallbackCacheTTL
	}
	if args.ReservationTTL.Duration == 0 {
		args.ReservationTTL.Duration = DefaultReservationTTL
	}
	if args.ReconcilePeriod.Duration == 0 {
		args.ReconcilePeriod.Duration = DefaultReconcilePeriod
	}
}

// ValidateServiceSpreadArgs checks fields that placement-critical code paths
// depend on. It rejects empty/invalid serviceLabelKey (the service quota key
// cannot be built without it, design doc 12.1), invalid managed scheduler
// names, and negative durations.
func ValidateServiceSpreadArgs(args *ServiceSpreadArgs) error {
	var errs []string
	if args.ServiceLabelKey == "" {
		errs = append(errs, "serviceLabelKey is required")
	} else if msg := validation.IsQualifiedName(args.ServiceLabelKey); len(msg) > 0 {
		errs = append(errs, fmt.Sprintf("serviceLabelKey: %s", strings.Join(msg, ", ")))
	}
	if args.ManagedSchedulerName == "" {
		errs = append(errs, "managedSchedulerName is required (after defaulting)")
	} else if msg := validation.IsQualifiedName(args.ManagedSchedulerName); len(msg) > 0 {
		errs = append(errs, fmt.Sprintf("managedSchedulerName: %s", strings.Join(msg, ", ")))
	}
	for name, d := range map[string]metav1.Duration{
		"fallbackCacheTTL": args.FallbackCacheTTL,
		"reservationTTL":   args.ReservationTTL,
		"reconcilePeriod":  args.ReconcilePeriod,
	} {
		if d.Duration < 0 || d.Duration > 24*time.Hour {
			errs = append(errs, fmt.Sprintf("%s must be between 0 and 24h, got %s", name, d.Duration))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid ServiceSpreadArgs: %s", strings.Join(errs, "; "))
	}
	return nil
}
