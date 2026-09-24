# ServiceSpread Scheduler

Out-of-tree Kubernetes scheduler plugin that enforces service-level pod
spreading: pods of the same service (namespace + service label +
`schedulerName`) are spread with a hard `maxSkew` inside their stable spread
domain and a hard `maxPodsPerNode` cap shared across the service's scheduling
domains. Design: see `../service-spread-scheduler-design.md` (requirements)
and `../service-spread-scheduler-dev-design.md` (implementation design).

- Target Kubernetes: **1.28.x** (`k8s.io/kubernetes v1.28.15` + staging
  replaces in `go.mod`; no plugin ABI across minor versions).
- Module: `github.com/cnyup/service-spread-scheduler`.
- CRD group: `scheduling.soyup.top/v1alpha1` (`ServiceSpreadPolicy`, short
  name `ssp`).

## Milestone status

| Milestone | Scope | Status |
| --- | --- | --- |
| M1 | API scaffolding, `ServiceSpreadPolicy` CRD, `ServiceSpreadArgs` config API, policy webhook (deterministic naming + validation + shared ConfigMap), CEL `nodeName` VAP manifests | **done** |
| M2 | service quota/scheduling keys, `schedulingDomainHash`, COW counters + reservation state machine | **done** |
| M3 | scheduler plugin extension points, EnqueueExtensions, cluster-event hints | **done** (e2e matrix 9 PASS + chaos drills 4/4 on kind) |
| M4 | replica-target observer, capacity alerting, metrics, TTL janitor wiring, reconciler wiring, production manifests, kind e2e | **mostly done** (2026-09-24): observer + alerts + janitor/reconciler wiring + chaos §8.4 verified on kind; remaining: production-grade manifests (image tags, resources, PDB) |

M1/M2 did not change scheduling behaviour. M3 registers the plugin:
`cmd/scheduler` wires `app.WithPlugin(spread.Name, spread.New)`, so the
example `config/scheduler/kubeconfig.yaml` now loads a profile with the
`ServiceSpread` plugin (keep `DefaultPreemption` disabled and do not
configure `addedAffinity` on the profile's NodeAffinity plugin — §9
deviation 8). Scheduling-cycle scenarios are covered by plugin-level unit
tests; the framework TestFramework-style in-process integration harness
does not exist in 1.28 (§9 deviation 14) and end-to-end validation runs
through the kind e2e matrix.

## Layout

```text
api/v1alpha1/        ServiceSpreadPolicy types (CRD)
apis/config/v1alpha1 ServiceSpreadArgs (pluginConfig.args, strict decoding)
cmd/scheduler/       custom kube-scheduler binary (ServiceSpread registered)
cmd/webhook/         ServiceSpreadPolicy validating webhook
internal/spread/     keys, COW snapshot, reservations, domain, plugin, hints
internal/webhook/    deterministic naming, shared-config source, validation
config/crd/bases/    generated CRD manifest
config/webhook/      ValidatingWebhookConfiguration + serving Service/certs
config/manager/      namespace, shared ConfigMap, Deployments, CEL VAP
config/rbac/         webhook RBAC
config/scheduler/    KubeSchedulerConfiguration example (M3+)
```

## Build & test

```sh
make generate manifests   # deepcopy + CRD (controller-gen, see Makefile pin)
make test-unit            # go test ./...
make envtest-binaries     # download kube-apiserver/etcd for envtest (1.28.x)
make test-integration     # admission matrix + VAP behaviour against envtest
```

Integration tests skip themselves when `KUBEBUILDER_ASSETS` is not set.
`make test` runs both tiers.

## Cluster-level configuration

The scheduler and the webhook share one ConfigMap,
`service-spread-scheduler-config` (see `config/manager/configmap.yaml`):

```yaml
data:
  serviceLabelKey: "app.kubernetes.io/name"
  managedSchedulerName: "service-spread-scheduler"
```

`serviceLabelKey` must equal `ServiceSpreadArgs.serviceLabelKey` in the
scheduler kubeconfig. The webhook fails closed while the ConfigMap is missing
or invalid.

## Applying M1 to a cluster

1. Install the CRD: `config/crd/bases/`.
2. Namespace + shared ConfigMap: `config/manager/namespace.yaml`,
   `config/manager/configmap.yaml`.
3. Webhook (needs cert-manager or an equivalent CA injection for
   `service-spread-webhook-serving-cert`): `config/rbac/webhook.yaml`,
   `config/webhook/service.yaml`, `config/manager/webhook.yaml`,
   `config/webhook/manifests.yaml`.
4. nodeName bypass protection (K8s 1.28: enable
   `--feature-gates=ValidatingAdmissionPolicy=true` and
   `--runtime-config=admissionregistration.k8s.io/v1beta1=true` on
   kube-apiserver): `config/manager/vap-block-preset-nodename.yaml`.

Creating a policy requires the deterministic name enforced by the webhook:

```sh
# name = ssp- + first 12 hex of SHA-256 over the JSON array
#        [namespace, schedulerName, serviceLabelKey, serviceLabelValue]
kubectl -n production create -f - <<EOF
apiVersion: scheduling.soyup.top/v1alpha1
kind: ServiceSpreadPolicy
metadata:
  name: ssp-$(...computed...)
spec:
  schedulerName: service-spread-scheduler
  serviceSelector:
    matchLabels:
      app.kubernetes.io/name: image-processing
  maxSkew: 1
  maxPodsPerNode: 9
EOF
```

## Monitoring

The scheduler process exports Prometheus metrics on `:9100/metrics` (see `cmd/scheduler/main.go`). The observer loop resolves every deployment's replica target (HPA → KEDA fallback → deployment replicas, with lastGood protection) and exports one series per (namespace, service, domain) every 30s.

**Observer metrics** (`internal/spread/replicatarget.go`):

| Metric | Labels | Meaning |
| --- | --- | --- |
| `service_spread_replica_target` | namespace, service, domain | Reported replica target with protective floor `max(desired, observedPods)` |
| `service_spread_observed_pods` | namespace, service, domain | Pending+Running pods of the service (incl. unbound) |
| `service_spread_bound_pods` | namespace, service, domain | Pods bound to nodes |
| `service_spread_fallback_active` | namespace, service, domain | 1 when any replica-target component fell back (ReplicaTargetFallback or lastGood) |
| `service_spread_capacity_deficit` | namespace, service | `HPA maxReplicas − eligibleNodes × maxPodsPerNode`, floored at 0; exported only when an HPA and an unambiguous policy exist for the service |

**Plugin metrics** (`internal/spread/plugin.go`):

| Metric | Labels | Meaning |
| --- | --- | --- |
| `service_spread_filter_rejections_total` | reason | PreFilter/Filter rejections by reason (`ServiceSpreadConstraint`, …) |
| `service_spread_policy_resolution_failures_total` | reason | Policy resolution failures (`MissingRequiredServiceLabel`, `ServiceSpreadPolicyNotFound`, …) |

**Alerting** — `config/monitoring/prometheusrule.yaml` ships four rules matching the design doc (§10.3/§12.2):

- `ServiceSpreadCapacityDeficit` (warning): HPA maxReplicas exceeds what the eligible-node union can host at `maxPodsPerNode` — scaling up would leave pods permanently Pending.
- `ServiceSpreadReplicaTargetFallback` (warning): replica-target reads fell back (HPA/KEDA unreadable, or lastGood) for 10m — the observer is running on degraded data.
- `ServiceSpreadFilterRejectionRate` (warning): filter rejections sustained above 0.5/s for 10m — pods are systematically unschedulable under current spread constraints.
- `ServiceSpreadObserverMetricsStale` (critical): `service_spread_replica_target` absent for 10m — the observer loop or the metrics endpoint is down.

Note: prometheus-operator loads the PrometheusRule only when its `ruleSelector` matches the object's labels — adjust `release: kube-prometheus-stack` to your setup. Gauge rules aggregate with `max by (...)` and counter rules with `sum by (...)` because both scheduler replicas export the same series (leader and followers); `absent()` fires only on total loss — per-replica detection needs the instance/job label from your scrape config. `hack/e2e/verify-observer.sh` validates the export path end-to-end on a kind cluster (rebuild → rollout → port-forward → metric assertions); `hack/e2e/chaos.sh` runs the §8.4 failure drills.

## Toolchain notes

- `controller-gen` is pinned to **v0.16.5**: the 1.28-era v0.13.0 no longer
  builds under Go ≥ 1.23 and its binaries do not run on macOS ≥ 26. Output is
  standard `apiextensions.k8s.io/v1` and is exercised against the envtest
  1.28 apiserver in `make test-integration`.
- `setup-envtest` is installed `@latest` (standalone module): the
  release-0.16 tag predates the GCS → GitHub-releases migration. On
  darwin/arm64 `1.28.x` resolves to the 1.28.3 bundle, the last published for
  that platform.
