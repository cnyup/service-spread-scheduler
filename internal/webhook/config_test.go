package webhook

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sspconfig "gitlab.ttyuyin.com/T5428/service-spread-scheduler/apis/config/v1alpha1"
)

func baseConfigMap(data map[string]string) *corev1.ConfigMap {
	if data == nil {
		data = map[string]string{}
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "service-spread-system", Name: SharedConfigMapName},
		Data:       data,
	}
}

func TestParseConfigMapValid(t *testing.T) {
	cfg, err := ParseConfigMap(baseConfigMap(map[string]string{
		"serviceLabelKey":      "app.kubernetes.io/name",
		"managedSchedulerName": "service-spread-scheduler",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServiceLabelKey != "app.kubernetes.io/name" {
		t.Errorf("ServiceLabelKey = %q", cfg.ServiceLabelKey)
	}
	if cfg.ManagedSchedulerName != "service-spread-scheduler" {
		t.Errorf("ManagedSchedulerName = %q", cfg.ManagedSchedulerName)
	}
}

func TestParseConfigMapDefaultsSchedulerName(t *testing.T) {
	cfg, err := ParseConfigMap(baseConfigMap(map[string]string{
		"serviceLabelKey": "app.kubernetes.io/name",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ManagedSchedulerName != sspconfig.DefaultManagedSchedulerName {
		t.Errorf("ManagedSchedulerName default = %q, want %q",
			cfg.ManagedSchedulerName, sspconfig.DefaultManagedSchedulerName)
	}
}

func TestParseConfigMapFailClosed(t *testing.T) {
	cases := []struct {
		name string
		cm   *corev1.ConfigMap
		want string
	}{
		{"nil configmap", nil, "nil"},
		{"missing serviceLabelKey", baseConfigMap(nil), "serviceLabelKey is required"},
		{"empty serviceLabelKey", baseConfigMap(map[string]string{"serviceLabelKey": "  "}), "serviceLabelKey is required"},
		{"invalid label key", baseConfigMap(map[string]string{"serviceLabelKey": "bad key!"}), "invalid"},
		{"invalid scheduler name", baseConfigMap(map[string]string{
			"serviceLabelKey":      "app.kubernetes.io/name",
			"managedSchedulerName": "bad name!",
		}), "invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfigMap(tc.cm)
			if err == nil {
				t.Fatalf("expected fail-closed error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestStaticConfigSource(t *testing.T) {
	src := StaticConfigSource{Config: PluginConfig{ServiceLabelKey: "k", ManagedSchedulerName: "m"}}
	cfg, err := src.Get()
	if err != nil || cfg.ServiceLabelKey != "k" {
		t.Fatalf("got (%v, %v)", cfg, err)
	}
}
