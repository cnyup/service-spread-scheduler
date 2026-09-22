// Command scheduler is the custom kube-scheduler binary of the ServiceSpread
// out-of-tree plugin: a standard kube-scheduler that additionally registers
// the ServiceSpread plugin, deployed with a dedicated profile
// `service-spread-scheduler` (see config/scheduler/kubeconfig.yaml).
package main

import (
	"context"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"gitlab.ttyuyin.com/T5428/service-spread-scheduler/internal/spread"
)

func main() {
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
