// Command webhook runs the ServiceSpreadPolicy validating admission webhook:
// deterministic naming plus schema-level validation of ServiceSpreadPolicy
// CREATE/UPDATE requests (dev-design §7.1). It shares its cluster-level
// configuration (serviceLabelKey, managedSchedulerName) with the scheduler
// through the `service-spread-scheduler-config` ConfigMap.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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

// leaderOnly adapts a CertRotator to manager.Runnable. controller-runtime
// starts Runnables only on the elected leader when LeaderElection is on,
// which is exactly the single-writer semantics the rotator needs.
func leaderOnly(r *sspwebhook.CertRotator) manager.Runnable {
	return manager.RunnableFunc(func(ctx context.Context) error {
		r.Run(ctx, 12*time.Hour)
		return nil
	})
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
	var selfSignCerts bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address the metrics endpoint binds to (\"0\" disables)")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address the probe endpoints bind to")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir", "", "directory containing the TLS certificate (tls.crt/tls.key by default); watched for rotation")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "certificate file name inside --webhook-cert-dir")
	flag.StringVar(&webhookKeyName, "webhook-cert-key-name", "tls.key", "key file name inside --webhook-cert-dir")
	flag.StringVar(&configMapNamespace, "config-map-namespace", "", "namespace of the shared config ConfigMap (required)")
	flag.StringVar(&configMapName, "config-map-name", sspwebhook.SharedConfigMapName, "name of the shared config ConfigMap")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election (required with -self-sign-certs: only the leader rotates certificates)")
	flag.BoolVar(&selfSignCerts, "self-sign-certs", false, "generate and rotate the serving certificate in-process (KEDA-style, no cert-manager). Write path: Secret -> kubelet volume propagation -> certwatcher hot reload; caBundle patched on the ValidatingWebhookConfiguration")
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

	// KEDA-style self-signed certificate rotation: the rotator runs as a
	// leader-only Runnable — controller-runtime starts it on the elected
	// leader replica only, so two replicas never fight over the Secret.
	// Rotation writes the Secret; the kubelet propagates the revision into
	// the mounted volume and the certwatcher (above) hot-reloads the pair.
	if selfSignCerts {
		if !leaderElect {
			setupLog.Error(nil, "-self-sign-certs requires -leader-elect (multi-replica deployments must not race on the serving-cert Secret)")
			os.Exit(1)
		}
		rotator, err := sspwebhook.NewCertRotator(restCfg,
			"service-spread-policy-validator",
			sspwebhook.ServingCertDNSNames(configMapNamespace),
		)
		if err != nil {
			setupLog.Error(err, "unable to create certificate rotator")
			os.Exit(1)
		}
		if err := mgr.Add(leaderOnly(rotator)); err != nil {
			setupLog.Error(err, "unable to add certificate rotator to manager")
			os.Exit(1)
		}
		setupLog.Info("self-signed certificate rotation enabled",
			"secret", sspwebhook.ServingCertSecretName, "namespace", sspwebhook.WebhookNamespace())
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
