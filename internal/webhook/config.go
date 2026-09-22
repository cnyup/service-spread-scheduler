package webhook

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	sspconfig "gitlab.ttyuyin.com/T5428/service-spread-scheduler/apis/config/v1alpha1"
)

// SharedConfigMapName is the name of the ConfigMap from which both the
// scheduler and this webhook read their shared cluster-level configuration
// (serviceLabelKey, managedSchedulerName). Both Deployments mount/watch the
// same object; keeping it in one place guarantees the webhook and the plugin
// agree on what a valid policy looks like (dev-design §7.1).
const SharedConfigMapName = "service-spread-scheduler-config"

// PluginConfig is the webhook's view of the shared configuration.
type PluginConfig struct {
	// ServiceLabelKey is the single pod label key that identifies a service.
	ServiceLabelKey string
	// ManagedSchedulerName is the schedulerName pods must specify to be
	// managed by the ServiceSpread scheduler.
	ManagedSchedulerName string
}

// ParseConfigMap validates and converts the shared ConfigMap. An absent or
// invalid serviceLabelKey is an error: the webhook cannot validate selectors
// without it and must fail closed rather than guess.
func ParseConfigMap(cm *corev1.ConfigMap) (PluginConfig, error) {
	if cm == nil {
		return PluginConfig{}, fmt.Errorf("shared config ConfigMap is nil")
	}
	labelKey := strings.TrimSpace(cm.Data["serviceLabelKey"])
	if labelKey == "" {
		return PluginConfig{}, fmt.Errorf("%s/%s: data.serviceLabelKey is required (fail closed)", cm.Namespace, cm.Name)
	}
	if msgs := validation.IsQualifiedName(labelKey); len(msgs) > 0 {
		return PluginConfig{}, fmt.Errorf("%s/%s: data.serviceLabelKey %q invalid: %s",
			cm.Namespace, cm.Name, labelKey, strings.Join(msgs, ", "))
	}

	schedulerName := strings.TrimSpace(cm.Data["managedSchedulerName"])
	if schedulerName == "" {
		// Same default as ServiceSpreadArgs (design doc 12.1).
		schedulerName = sspconfig.DefaultManagedSchedulerName
	}
	if msgs := validation.IsQualifiedName(schedulerName); len(msgs) > 0 {
		return PluginConfig{}, fmt.Errorf("%s/%s: data.managedSchedulerName %q invalid: %s",
			cm.Namespace, cm.Name, schedulerName, strings.Join(msgs, ", "))
	}

	return PluginConfig{ServiceLabelKey: labelKey, ManagedSchedulerName: schedulerName}, nil
}

// ConfigSource serves the currently loaded PluginConfig. Get returns an error
// while the source is not synced or holds invalid data — callers (the
// validating webhook) treat that as fail-closed.
type ConfigSource interface {
	Get() (PluginConfig, error)
}

// StaticConfigSource is a ConfigSource with a fixed value, for tests and for
// wiring a non-ConfigMap configuration source later.
type StaticConfigSource struct {
	Config PluginConfig
}

func (s StaticConfigSource) Get() (PluginConfig, error) { return s.Config, nil }

// ConfigMapSource watches the shared ConfigMap (namespace/name) with a
// single-object informer and publishes the latest valid PluginConfig
// atomically. It implements manager.Runnable (leader-election independent)
// so it can be added to a controller-runtime manager.
type ConfigMapSource struct {
	namespace string
	name      string

	state atomic.Pointer[cmState]

	hasSynced func() bool
	run       func(ctx context.Context)
}

type cmState struct {
	cfg PluginConfig
	err error
}

// NewConfigMapSource builds a ConfigMapSource watching <namespace>/<name>.
// Call Start (usually via manager.Add) before Get is expected to succeed.
func NewConfigMapSource(cfg *rest.Config, namespace, name string, logger klog.Logger) (*ConfigMapSource, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building clientset for config source: %w", err)
	}
	src := &ConfigMapSource{namespace: namespace, name: name}

	listWatch := cache.NewListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		"configmaps",
		namespace,
		fields.OneTermEqualSelector("metadata.name", name),
	)
	informer := cache.NewSharedInformer(listWatch, &corev1.ConfigMap{}, 0)
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { src.store(obj) },
		UpdateFunc: func(_, obj interface{}) { src.store(obj) },
		DeleteFunc: func(interface{}) {
			src.state.Store(&cmState{err: fmt.Errorf("shared config ConfigMap %s/%s was deleted (fail closed)", namespace, name)})
			logger.Info("shared config ConfigMap deleted; webhook fails closed until it reappears")
		},
	})
	src.hasSynced = informer.HasSynced
	src.run = func(ctx context.Context) { informer.Run(ctx.Done()) }
	return src, nil
}

func (s *ConfigMapSource) store(obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return
	}
	if cfg, err := ParseConfigMap(cm); err != nil {
		s.state.Store(&cmState{err: err})
	} else {
		s.state.Store(&cmState{cfg: cfg})
	}
}

// Get returns the latest valid PluginConfig, or an error while unsynced,
// deleted or invalid (fail closed).
func (s *ConfigMapSource) Get() (PluginConfig, error) {
	st := s.state.Load()
	if st == nil {
		return PluginConfig{}, fmt.Errorf("shared config ConfigMap %s/%s not observed yet (fail closed)", s.namespace, s.name)
	}
	return st.cfg, st.err
}

// HasSynced reports whether the backing informer finished its initial list.
func (s *ConfigMapSource) HasSynced() bool { return s.hasSynced() }

var _ manager.Runnable = (*ConfigMapSource)(nil)
var _ manager.LeaderElectionRunnable = (*ConfigMapSource)(nil)

// Start runs the informer until the context is cancelled.
func (s *ConfigMapSource) Start(ctx context.Context) error {
	s.run(ctx)
	return nil
}

// NeedLeaderElection is false: every webhook replica must serve, so the
// config source runs regardless of leader election.
func (s *ConfigMapSource) NeedLeaderElection() bool { return false }
