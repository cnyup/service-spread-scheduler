package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
)

func unknown(t *testing.T, payload any) *runtime.Unknown {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &runtime.Unknown{Raw: raw, ContentType: runtime.ContentTypeJSON}
}

func TestDecodeServiceSpreadArgsFull(t *testing.T) {
	requirePolicy := false
	args, err := DecodeServiceSpreadArgs(unknown(t, map[string]any{
		"apiVersion":           "config.scheduling.soyup.top/v1alpha1",
		"kind":                 "ServiceSpreadArgs",
		"serviceLabelKey":      "app.kubernetes.io/name",
		"requirePolicy":        requirePolicy,
		"managedSchedulerName": "my-scheduler",
		"fallbackCacheTTL":     "15m",
		"reservationTTL":       "2m",
		"reconcilePeriod":      "1h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if args.ServiceLabelKey != "app.kubernetes.io/name" {
		t.Errorf("ServiceLabelKey = %q", args.ServiceLabelKey)
	}
	if args.RequirePolicy == nil || *args.RequirePolicy {
		t.Errorf("RequirePolicy = %v, want false", args.RequirePolicy)
	}
	if args.ManagedSchedulerName != "my-scheduler" {
		t.Errorf("ManagedSchedulerName = %q", args.ManagedSchedulerName)
	}
	if args.FallbackCacheTTL.Duration != 15*time.Minute {
		t.Errorf("FallbackCacheTTL = %s", args.FallbackCacheTTL.Duration)
	}
	if args.ReservationTTL.Duration != 2*time.Minute {
		t.Errorf("ReservationTTL = %s", args.ReservationTTL.Duration)
	}
	if args.ReconcilePeriod.Duration != time.Hour {
		t.Errorf("ReconcilePeriod = %s", args.ReconcilePeriod.Duration)
	}
}

func TestDecodeServiceSpreadArgsDefaults(t *testing.T) {
	args, err := DecodeServiceSpreadArgs(unknown(t, map[string]any{
		"serviceLabelKey": "svc",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if args.ManagedSchedulerName != DefaultManagedSchedulerName {
		t.Errorf("ManagedSchedulerName default = %q", args.ManagedSchedulerName)
	}
	if args.RequirePolicy == nil || !*args.RequirePolicy {
		t.Errorf("RequirePolicy default = %v, want true", args.RequirePolicy)
	}
	if args.FallbackCacheTTL.Duration != DefaultFallbackCacheTTL {
		t.Errorf("FallbackCacheTTL default = %s", args.FallbackCacheTTL.Duration)
	}
	if args.ReservationTTL.Duration != DefaultReservationTTL {
		t.Errorf("ReservationTTL default = %s", args.ReservationTTL.Duration)
	}
	if args.ReconcilePeriod.Duration != DefaultReconcilePeriod {
		t.Errorf("ReconcilePeriod default = %s", args.ReconcilePeriod.Duration)
	}
}

func TestDecodeServiceSpreadArgsFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		obj     runtime.Object
		wantSub string
	}{
		{
			"nil args",
			nil,
			"requires pluginConfig args",
		},
		{
			// A typo in a key must break startup loudly, not run with defaults.
			"unknown field",
			unknown(t, map[string]any{
				"serviceLabelKey":  "svc",
				"serviceLableKeyX": "typo",
			}),
			"unknown field",
		},
		{
			"missing required serviceLabelKey",
			unknown(t, map[string]any{"managedSchedulerName": "m"}),
			"serviceLabelKey is required",
		},
		{
			"invalid label key",
			unknown(t, map[string]any{"serviceLabelKey": "bad key!"}),
			"serviceLabelKey",
		},
		{
			"wrong apiVersion",
			unknown(t, map[string]any{
				"apiVersion":      "config.scheduling.soyup.top/v1beta1",
				"serviceLabelKey": "svc",
			}),
			"apiVersion",
		},
		{
			"wrong kind",
			unknown(t, map[string]any{
				"kind":            "SomethingElse",
				"serviceLabelKey": "svc",
			}),
			"kind",
		},
		{
			"negative duration",
			unknown(t, map[string]any{
				"serviceLabelKey": "svc",
				"reservationTTL":  "-5m",
			}),
			"reservationTTL",
		},
		{
			"unsupported type",
			&runtime.Unknown{Raw: []byte("{not-json"), ContentType: runtime.ContentTypeJSON},
			"decoding",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args, err := DecodeServiceSpreadArgs(tc.obj)
			if err == nil {
				t.Fatalf("expected error containing %q, got args %+v", tc.wantSub, args)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err, tc.wantSub)
			}
		})
	}
}

func TestDecodeServiceSpreadArgsPassesTypedObject(t *testing.T) {
	typed := &ServiceSpreadArgs{ServiceLabelKey: "svc"}
	// Object passes through defaults+validation unchanged in identity.
	got, err := DecodeServiceSpreadArgs(typed)
	if err != nil {
		t.Fatal(err)
	}
	if got.ServiceLabelKey != "svc" {
		t.Fatalf("got %+v", got)
	}
}

func TestServiceSpreadArgsSchemeRegistration(t *testing.T) {
	sch := runtime.NewScheme()
	if err := AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	if !sch.Recognizes(GroupVersion.WithKind(ArgsKind)) {
		t.Fatalf("scheme does not recognize %s %s", GroupVersion, ArgsKind)
	}
}
