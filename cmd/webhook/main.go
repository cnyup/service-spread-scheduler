// Command webhook runs the ServiceSpreadPolicy validating admission webhook:
// deterministic naming plus schema-level validation of ServiceSpreadPolicy
// CREATE/UPDATE requests (dev-design §7.1). It shares its cluster-level
// configuration (serviceLabelKey, managedSchedulerName) with the scheduler
// through the `service-spread-scheduler-config` ConfigMap.
package main

import (
	"crypto/tls"
	"flag"
	"net/http"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	sspv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
	sspwebhook "github.com/cnyup/service-spread-scheduler/internal/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sspv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var webhookCertDir string
	var webhookCertName string
	var webhookKeyName string
	var configMapNamespace string
	var configMapName string
	var leaderElect bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address the metrics endpoint binds to (\"0\" disables)")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address the probe endpoints bind to")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir", "", "directory containing the TLS certificate (tls.crt/tls.key by default); watched for rotation")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "certificate file name inside --webhook-cert-dir")
	flag.StringVar(&webhookKeyName, "webhook-cert-key-name", "tls.key", "key file name inside --webhook-cert-dir")
	flag.StringVar(&configMapNamespace, "config-map-namespace", "", "namespace of the shared config ConfigMap (required)")
	flag.StringVar(&configMapName, "config-map-name", sspwebhook.SharedConfigMapName, "name of the shared config ConfigMap")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election (not needed for webhooks: every replica serves)")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if configMapNamespace == "" {
		setupLog.Error(nil, "--config-map-namespace is required")
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	restCfg := ctrl.GetConfigOrDie()

	webhookOpts := webhook.Options{Port: 9443}
	var certWatcher *certwatcher.CertWatcher
	if webhookCertDir != "" {
		webhookOpts.CertDir = webhookCertDir
		certPath := filepath.Join(webhookCertDir, webhookCertName)
		keyPath := filepath.Join(webhookCertDir, webhookKeyName)
		var err error
		certWatcher, err = certwatcher.New(certPath, keyPath)
		if err != nil {
			setupLog.Error(err, "failed to initialize certificate watcher", "cert", certPath, "key", keyPath)
			os.Exit(1)
		}
		webhookOpts.TLSOpts = append(webhookOpts.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = certWatcher.GetCertificate
		})
		setupLog.Info("webhook TLS certificate watcher initialized", "cert", certPath)
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		WebhookServer:          webhook.NewServer(webhookOpts),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "service-spread-webhook.leader-election",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// The manager runs the cert watcher goroutine, reloading on rotation.
	if certWatcher != nil {
		if err := mgr.Add(certWatcher); err != nil {
			setupLog.Error(err, "unable to add certificate watcher to manager")
			os.Exit(1)
		}
	}

	configSrc, err := sspwebhook.NewConfigMapSource(restCfg, configMapNamespace, configMapName, setupLog)
	if err != nil {
		setupLog.Error(err, "unable to create shared config source")
		os.Exit(1)
	}
	if err := mgr.Add(configSrc); err != nil {
		setupLog.Error(err, "unable to add shared config source to manager")
		os.Exit(1)
	}

	if err := sspwebhook.RegisterPolicyWebhook(mgr, configSrc); err != nil {
		setupLog.Error(err, "unable to register ServiceSpreadPolicy webhook")
		os.Exit(1)
	}

	// Readiness reflects the shared config: the webhook fails closed until the
	// ConfigMap is observed, parsed and valid.
	if err := mgr.AddReadyzCheck("shared-config", func(_ *http.Request) error {
		_, err := configSrc.Get()
		return err
	}); err != nil {
		setupLog.Error(err, "unable to register readyz check")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to register healthz check")
		os.Exit(1)
	}

	setupLog.Info("starting ServiceSpreadPolicy webhook",
		"configMap", configMapNamespace+"/"+configMapName, "webhookPath", sspwebhook.PolicyWebhookPath)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "webhook manager exited with error")
		os.Exit(1)
	}
}
