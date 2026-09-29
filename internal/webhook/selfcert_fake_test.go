package webhook

import (
	"context"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Ensure against a fake clientset: first pass creates the Secret and
// patches the caBundle; second pass is a no-op (fresh 90d leaf must not
// re-issue under the 30d rotation threshold).
func TestCertRotator_EnsureIdempotent(t *testing.T) {
	vwc := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "service-spread-policy-validator"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "validate.servicespreadpolicy.scheduling.soyup.top",
			ClientConfig: admissionv1.WebhookClientConfig{
				Service: &admissionv1.ServiceReference{
					Name:      "service-spread-webhook-service",
					Namespace: "service-spread-system",
					Path:      strPtr("/validate-scheduling-soyup-top-v1alpha1-servicespreadpolicy"),
				},
			},
		}},
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: WebhookDeploymentName, Namespace: "service-spread-system"}}
	client := fake.NewSimpleClientset(vwc, dep)

	r := &CertRotator{
		client:          client,
		secretNamespace: "service-spread-system",
		webhookCfgName:  "service-spread-policy-validator",
		dnsNames:        []string{"service-spread-webhook-service.service-spread-system.svc"},
	}

	ctx := context.Background()
	issued, err := r.Ensure(ctx)
	if err != nil || !issued {
		t.Fatalf("first Ensure: issued=%v err=%v", issued, err)
	}

	sec, err := client.CoreV1().Secrets("service-spread-system").Get(ctx, ServingCertSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret not created: %v", err)
	}
	if len(sec.Data["tls.crt"]) == 0 || len(sec.Data["ca.crt"]) == 0 {
		t.Fatal("secret missing cert material")
	}

	patched, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, "service-spread-policy-validator", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bundle := patched.Webhooks[0].ClientConfig.CABundle
	if len(bundle) == 0 {
		t.Fatal("caBundle not patched")
	}

	// Second pass: fresh leaf must not re-issue.
	issued2, err := r.Ensure(ctx)
	if err != nil {
		t.Fatalf("second Ensure err: %v", err)
	}
	if issued2 {
		t.Fatal("second Ensure re-issued a fresh certificate — idempotence broken")
	}
}

func strPtr(s string) *string { return &s }
