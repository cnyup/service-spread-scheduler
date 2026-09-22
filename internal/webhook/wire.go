package webhook

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sspv1alpha1 "gitlab.ttyuyin.com/T5428/service-spread-scheduler/api/v1alpha1"
)

// PolicyWebhookPath is the HTTPS path under which the ServiceSpreadPolicy
// validating webhook is served. It must match the ValidatingWebhookConfiguration
// in config/webhook/manifests.yaml.
const PolicyWebhookPath = "/validate-scheduling-soyup-top-v1alpha1-serviceSpreadPolicy"

// RegisterPolicyWebhook registers the ServiceSpreadPolicy validating webhook
// (CREATE/UPDATE) on the manager's webhook server. cfg supplies the shared
// cluster-level configuration; the manager's APIReader is used for the
// overlap pre-check.
func RegisterPolicyWebhook(mgr manager.Manager, cfg ConfigSource) error {
	validator := &PolicyValidator{
		Cfg:    cfg,
		Reader: mgr.GetAPIReader(),
	}
	handler := admission.WithCustomValidator(mgr.GetScheme(), &sspv1alpha1.ServiceSpreadPolicy{}, validator)
	mgr.GetWebhookServer().Register(PolicyWebhookPath, handler)
	return nil
}
