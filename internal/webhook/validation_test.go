package webhook

import (
	"context"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sspv1alpha1 "gitlab.ttyuyin.com/T5428/service-spread-scheduler/api/v1alpha1"
)

// errSource simulates an unusable configuration source (fail closed).
type errSource struct{}

func (errSource) Get() (PluginConfig, error) {
	return PluginConfig{}, fmt.Errorf("config not loaded")
}

var testCfg = PluginConfig{
	ServiceLabelKey:      "app.kubernetes.io/name",
	ManagedSchedulerName: "service-spread-scheduler",
}

func validPolicy(ns, labelValue string) *sspv1alpha1.ServiceSpreadPolicy {
	return &sspv1alpha1.ServiceSpreadPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      ExpectedPolicyName(ns, testCfg.ManagedSchedulerName, testCfg.ServiceLabelKey, labelValue),
		},
		Spec: sspv1alpha1.ServiceSpreadPolicySpec{
			SchedulerName: testCfg.ManagedSchedulerName,
			ServiceSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{testCfg.ServiceLabelKey: labelValue},
			},
			MaxSkew:        1,
			MaxPodsPerNode: 9,
		},
	}
}

func TestValidatePolicyAcceptsValid(t *testing.T) {
	errs := ValidatePolicy(validPolicy("production", "image-processing"), testCfg)
	if len(errs) != 0 {
		t.Fatalf("valid policy rejected: %v", errs.ToAggregate())
	}
}

func TestValidatePolicyRejections(t *testing.T) {
	pattern := "production"
	cases := []struct {
		name   string
		mutate func(*sspv1alpha1.ServiceSpreadPolicy)
		want   string
	}{
		{
			"name mismatch",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Name = "ssp-000000000000" },
			"must equal the deterministic name",
		},
		{
			"arbitrary name",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Name = "my-policy" },
			"must equal the deterministic name",
		},
		{
			// Also breaks the deterministic-name match; both errors are
			// reported, the assertion only checks the scheduler mismatch.
			"wrong scheduler name",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Spec.SchedulerName = "other-scheduler" },
			"must exactly equal the managed scheduler name",
		},
		{
			"empty scheduler name",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Spec.SchedulerName = "" },
			"must not be empty",
		},
		{
			"selector missing",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Spec.ServiceSelector.MatchLabels = nil },
			"must contain exactly one entry",
		},
		{
			"selector extra key",
			func(p *sspv1alpha1.ServiceSpreadPolicy) {
				p.Spec.ServiceSelector.MatchLabels["tier"] = "frontend"
			},
			"must contain exactly one entry",
		},
		{
			"selector wrong key",
			func(p *sspv1alpha1.ServiceSpreadPolicy) {
				p.Spec.ServiceSelector.MatchLabels = map[string]string{"app": "image-processing"}
			},
			"must contain exactly one entry",
		},
		{
			"selector matchExpressions",
			func(p *sspv1alpha1.ServiceSpreadPolicy) {
				p.Spec.ServiceSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{
					Key: testCfg.ServiceLabelKey, Operator: "In", Values: []string{"image-processing"},
				}}
			},
			"matchExpressions are not supported",
		},
		{
			"maxSkew zero",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Spec.MaxSkew = 0 },
			"maxSkew",
		},
		{
			"maxPodsPerNode zero",
			func(p *sspv1alpha1.ServiceSpreadPolicy) { p.Spec.MaxPodsPerNode = 0 },
			"maxPodsPerNode",
		},
		{
			"negative values",
			func(p *sspv1alpha1.ServiceSpreadPolicy) {
				p.Spec.MaxSkew = -1
				p.Spec.MaxPodsPerNode = -3
			},
			"maxSkew",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validPolicy(pattern, "image-processing")
			tc.mutate(p)
			errs := ValidatePolicy(p, testCfg)
			if len(errs) == 0 {
				t.Fatalf("expected rejection containing %q, got none", tc.want)
			}
			joined := errs.ToAggregate().Error()
			if !strings.Contains(joined, tc.want) {
				t.Fatalf("errors %q do not contain %q", joined, tc.want)
			}
		})
	}
}

// Changing the selector value or schedulerName breaks the deterministic-name
// match, so identity fields are effectively immutable (the name itself is
// immutable at the API level).
func TestValidatePolicyIdentityChangeRejected(t *testing.T) {
	p := validPolicy("production", "image-processing")
	p.Spec.ServiceSelector.MatchLabels[testCfg.ServiceLabelKey] = "other-service"
	errs := ValidatePolicy(p, testCfg)
	if len(errs) == 0 {
		t.Fatal("selector value change must be rejected via deterministic-name mismatch")
	}
	if !strings.Contains(errs.ToAggregate().Error(), "deterministic name") {
		t.Fatalf("got %v", errs.ToAggregate())
	}
}

func TestSelectsSameService(t *testing.T) {
	incoming := validPolicy("production", "image-processing")
	rogue := validPolicy("production", "image-processing")
	rogue.Name = "ssp-rogue"

	if !selectsSameService(rogue, incoming, testCfg) {
		t.Error("rogue policy selecting the same service must be detected")
	}

	other := validPolicy("production", "another-service")
	if selectsSameService(other, incoming, testCfg) {
		t.Error("policy selecting a different service must not be flagged")
	}

	self := incoming.DeepCopy()
	if selectsSameService(self, incoming, testCfg) {
		t.Error("the incoming object itself must be skipped")
	}

	badSelector := validPolicy("production", "image-processing")
	badSelector.Name = "ssp-rogue2"
	badSelector.Spec.ServiceSelector.MatchLabels["extra"] = "key"
	if selectsSameService(badSelector, incoming, testCfg) {
		t.Error("multi-key selector cannot be judged as same-service")
	}
}

func TestPolicyValidatorOverlapFailsClosed(t *testing.T) {
	rogue := validPolicy("production", "image-processing")
	rogue.Name = "ssp-rogue"
	scheme := runtime.NewScheme()
	if err := sspv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fakeReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rogue).Build()

	v := &PolicyValidator{Cfg: StaticConfigSource{Config: testCfg}, Reader: fakeReader}
	err := v.validate(context.Background(), validPolicy("production", "image-processing"))
	if err == nil {
		t.Fatal("admission must be rejected when a rogue policy overlaps")
	}
	if !strings.Contains(err.Error(), "already selects the same service") {
		t.Fatalf("unexpected error: %v", err)
	}

	// A policy for a different service is admitted.
	if err := v.validate(context.Background(), validPolicy("production", "another-service")); err != nil {
		t.Fatalf("non-overlapping policy rejected: %v", err)
	}
}

func TestPolicyValidatorFailsClosedWithoutConfig(t *testing.T) {
	v := &PolicyValidator{
		Cfg:    errSource{},
		Reader: fake.NewClientBuilder().Build(),
	}
	err := v.validate(context.Background(), validPolicy("production", "image-processing"))
	if err == nil {
		t.Fatal("webhook must fail closed when config is unusable")
	}
	if !strings.Contains(err.Error(), "failing closed") {
		t.Fatalf("expected fail-closed error, got: %v", err)
	}
}
