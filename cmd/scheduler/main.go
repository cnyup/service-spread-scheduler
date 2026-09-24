// Command scheduler is the custom kube-scheduler binary of the ServiceSpread
// out-of-tree plugin: a standard kube-scheduler that additionally registers
// the ServiceSpread plugin, deployed with a dedicated profile
// `service-spread-scheduler` (see config/scheduler/kubeconfig.yaml).
package main

import (
	"context"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"net/http"

	"github.com/cnyup/service-spread-scheduler/internal/spread"
)

func main() {
	// M4 observer metrics: plain HTTP endpoint for the observer gauges on
	// the default prometheus registry (plugin counters included). Runs in
	// every replica; scraping can be leader-agnostic.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9100", mux); err != nil {
			klog.Background().Error(err, "observer metrics server")
		}
	}()

	command := app.NewSchedulerCommand(
		app.WithPlugin(spread.Name, func(configuration runtime.Object, h framework.Handle) (framework.Plugin, error) {
			// The 1.28 plugin factory carries no context; informers owned
			// by the plugin (policy CRD) live for the process lifetime.
			return spread.New(context.Background(), configuration, h)
		}),
	)
	code := cli.Run(command)
	os.Exit(code)
}
