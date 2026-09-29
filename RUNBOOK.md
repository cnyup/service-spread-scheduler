# Production Deployment Runbook — Service Spread Scheduler

Operational guide for deploying, upgrading, and troubleshooting the
ServiceSpread scheduler + policy webhook on a production cluster.
Design context: see `service-spread-scheduler-design.md` and
`service-spread-scheduler-dev-design.md`; this file covers the operational
surface only. Target: Kubernetes 1.28.x (see version notes at the bottom
for 1.30+).

## Components at a glance

| Component | Manifest(s) | Replicas | Failure mode when down |
| --- | --- | --- | --- |
| scheduler | `config/manager/scheduler.yaml` | 2 (leader election) | No new pods scheduled for managed workloads; running pods unaffected |
| webhook | `config/manager/webhook.yaml` | 2 + PDB minAvailable=1 | Policy CREATE/UPDATE **fails closed** (`failurePolicy: Fail`) |
| RBAC | `config/rbac/scheduler.yaml`, `config/rbac/webhook.yaml` | — | Informers loop Forbidden; scheduler can stall at cache sync (see Known issues #3) |
| VAP (preset-nodename block) | `config/manager/vap-block-preset-nodename.yaml` | — | Pods may bypass the scheduler via preset `spec.nodeName` |
| PrometheusRule | `config/monitoring/prometheusrule.yaml` | — | No alerts (silently unloaded when ruleSelector mismatch) |

## 1. Prerequisites

- Kubernetes 1.28.x with access to edit the kube-apiserver static Pod
  manifest (needed for the VAP feature gates — step 4).
- Image pull access to `ghcr.io/cnyup/service-spread-scheduler/*` (or a
  mirrored registry; override `image:` accordingly).
- cert-manager **or** a manual certificate path for the webhook serving
  cert (step 3).

## 2bis. Helm install (preferred)

```bash
# Pre-create the namespace with kubectl (verified reliable on ACK with
# OpenYurt: --create-namespace raced the namespace controller there and
# all 11 resources failed with ns-not-found, 2026-09-29). The chart
# deliberately renders NO Namespace object.
kubectl create ns service-spread-system
helm install ssp charts/service-spread-scheduler \
  -f charts/service-spread-scheduler/values-acr.yaml
# ACR personal edition needs auth for every pull: create the secret, then
# roll the system deployments once so they pick it up.
kubectl -n service-spread-system create secret docker-registry acr-regcred \
  --docker-server=registry.cn-hangzhou.aliyuncs.com \
  --docker-username=<user> --docker-password=<password>
kubectl -n service-spread-system rollout restart deploy/service-spread-scheduler deploy/service-spread-webhook
```

Workloads sharing the namespace: `defaultImagePullSecrets` in the values
patches the namespace's default ServiceAccount, so test/business pods
pull from ACR without per-workload imagePullSecrets.

Helm replaces the ordered steps below (2.1-2.10 fold into one command:
ordering, RBAC re-apply on upgrade, and cert readiness are handled by
the chart and the in-process self-sign rotator). Notes:

- CRD lives in the chart's `crds/` dir: installed on first install,
  NOT updated on upgrade — schema changes need
  `kubectl apply --server-side -f charts/service-spread-scheduler/crds/`.
- `helm upgrade` re-applies RBAC every time (the drift family killed
  by this is documented in §2.3 below).
- kubeconfig arg changes roll the scheduler automatically (checksum
  annotation on the pod template).
- Uninstall keeps the CRD (`crds/` semantics) — remove manually with
  kubectl when the API surface is truly retired.
- A FAILED first install can leave an unlabeled CRD that blocks
  retries with an ownership error (helm pre-flight): delete the CRD
  (`kubectl delete crd servicespreadpolicies.scheduling.soyup.top`)
  and retry — observed on helm 3.15.4 during the 2026-09-29 validation.

Full lifecycle verified live on ACK (2026-09-29): install (ACR images,
self-sign cold start converged after 2 restarts), upgrade with arg
change (exportNodePods -> node_pods series live, including other
labeled workloads on the cluster), rollback, uninstall clean.

## 2. Install order

The order matters: the webhook's ConfigMap readiness gate and the
scheduler's CRD/RBAC dependencies make some steps blocking.

```bash
# 2.1 namespace
kubectl apply -f config/manager/namespace.yaml

# 2.2 CRDs (blocking: everything else resolves this GVK)
kubectl apply -f config/crd/bases/

# 2.3 RBAC — BOTH files, every time either manifest changes.
# Three production incidents (2026-09) came from "manifest updated,
# cluster stale" on this step. The e2e entry scripts re-apply these
# automatically; production upgrades must not skip it.
kubectl apply -f config/rbac/scheduler.yaml
kubectl apply -f config/rbac/webhook.yaml

# 2.4 shared config (readiness gate: webhook fails closed without it)
kubectl apply -f config/manager/configmap.yaml
# Edit data.serviceLabelKey/managedSchedulerName if you do not use the
# defaults; serviceLabelKey must stay in sync with
# config/scheduler/kubeconfig.yaml.

# 2.5 webhook: certificate path — pick ONE
# (a) In-process self-signed (DEFAULT since v0.1): nothing to do here.
#     config/manager/webhook.yaml already runs with -self-sign-certs
#     -leader-elect: the leader replica generates/rotates the CA+serving
#     cert, writes the Secret, patches the caBundle. Zero external
#     dependencies (KEDA-style). Expect a few seconds of fail-closed
#     policy writes on very first install until the caBundle lands.
# (b) cert-manager: kubectl apply -f config/webhook/certificates.yaml
#     AND remove the two flags from webhook.yaml (do not run both —
#     whoever writes the Secret last wins).
# (c) Manual: create a TLS secret named service-spread-webhook-serving-cert,
#     then patch the caBundle (the e2e bootstrap has a working example).

kubectl apply -f config/webhook/service.yaml
kubectl apply -f config/manager/webhook.yaml
kubectl apply -f config/webhook/manifests.yaml   # after the cert is Ready

# 2.6 scheduler
kubectl apply -f config/manager/scheduler.yaml

# 2.7 VAP (edit kube-apiserver static-pod manifest, see step 4)
```

## 3. Image pinning

Manifests ship with `:latest` because the kind e2e flow depends on it.
For production, pin to a release tag:

```bash
kubectl apply -f - < <(sed 's/:latest/:v0.1.0-alpha.1/' config/manager/scheduler.yaml)
kubectl apply -f - < <(sed 's/:latest/:v0.1.0-alpha.1/' config/manager/webhook.yaml)
```

(or use a kustomize overlay with an `images:` transformer). Tags are
produced by the `release` workflow: pushing `v*` builds multi-arch
images and a floating `:latest` to ghcr.io. Never deploy `:latest` with
`imagePullPolicy: Always` against a moving upstream — the manifests
already use `IfNotPresent`, keep it that way when pinning by tag; switch
to `IfNotPresent`+digest (`@sha256:...`) for immutable pinning.

### Mainland clusters (ACR mirror)

ghcr.io is unreachable from many mainland clusters — verified 2026-09-28
on an ACK Beijing cluster: pulls hang indefinitely (no error event, no
backoff) while quay.io works, so it is a ghcr-specific network path
issue, not a general egress problem. Distribution route for mainland:

1. Enable the `push-acr` job (repo Settings → Secrets and variables →
   Actions): set variable `ENABLE_ACR=true`, and secrets `ACR_REGISTRY`
   (e.g. `registry.cn-hangzhou.aliyuncs.com` — personal edition has NO
   `.cr` segment; the enterprise-style domain does not resolve), a BARE
   `ACR_NAMESPACE` (slashes are rejected by the config check), `ACR_USERNAME`,
   `ACR_PASSWORD`. Every `v*` tag then also mirrors the images to ACR
   (public-internet push from GitHub runners).
2. Deployments reference the same ACR domain. NOTE: personal edition
   does not offer `-vpc` endpoints (enterprise only) and cross-region
   clusters pull over the public endpoint; same-region personal edition
   still goes through the default domain.
3. **imagePullSecret is ALWAYS required** — a personal-edition "public"
   repository is NOT anonymously pullable (verified 2026-09-28: anonymous
   token grants no pull scope, 401). Create the secret in
   `service-spread-system` from the ACR credential and add it to
   `spec.template.spec.imagePullSecrets` of both Deployments.
4. One-off bootstrap before the next release: push the current tag
   manually once (`docker pull ghcr.io/... && docker tag && docker push`)
   — the workflow only mirrors on new tags.

## 4. VAP feature gates (Kubernetes 1.28)

`vap-block-preset-nodename.yaml` uses `admissionregistration.k8s.io/v1beta1`,
which 1.28 serves **only** when both flags are on (either alone is not
enough — verified the hard way on kind):

```
--feature-gates=ValidatingAdmissionPolicy=true
--runtime-config=admissionregistration.k8s.io/v1beta1=true
```

Add both to the kube-apiserver static Pod manifest (e.g.
`/etc/kubernetes/manifests/kube-apiserver.yaml` on kubeadm). The
apiserver rolls; expect a leader-election blip (~90s observed on kind).
On 1.30+ switch the two documents in the manifest to
`admissionregistration.k8s.io/v1` and drop the runtime-config flag.

The scheduler name literal inside the VAP expressions must equal
`configmap.yaml:data.managedSchedulerName` — keep in sync.

## 5. Monitoring

- Metrics: scheduler pods expose observer gauges on `:9100/metrics`
  (`service_spread_{replica_target,observed_pods,bound_pods,fallback_active,capacity_deficit}`,
  plus `service_spread_filter_rejections_total{reason}` and
  `service_spread_policy_resolution_failures_total{reason}` from the
  plugin). Add a scrape job / ServiceMonitor for port `obs-metrics`.
- PrometheusRule: `config/monitoring/prometheusrule.yaml` carries
  `release: kube-prometheus-stack` — **adjust to your Prometheus
  ruleSelector or the rules load silently**. Multi-replica aggregation
  (`max by (...)`) is already in the expressions; do not "simplify" them
  to per-instance alerts (each replica exports the same logical gauges).
- Health: `:10259/healthz` (HTTPS) gates the endpoint, NOT leader status —
  a follower is Ready by design. To check "someone is actually
  scheduling", watch the leader Lease
  `service-spread-system/service-spread-scheduler` `renewTime` (stale
  renewTime = no active leader; this exact signature caught a real RBAC
  stall in 2026-09):
  ```bash
  kubectl -n service-spread-system get lease service-spread-scheduler \
    -o jsonpath='{.spec.renewTime}{"\n"}'
  ```

## 6. Upgrade

1. Push the new tag (release workflow builds it) or note the digest.
2. Re-apply RBAC (step 2.3) — code changes routinely add informer
   permissions; skipping this is the top historical failure source.
3. Roll the scheduler, wait for rollout, then roll the webhook:
   ```bash
   kubectl -n service-spread-system rollout status deploy/service-spread-scheduler --timeout=180s
   kubectl -n service-spread-system rollout restart deploy/service-spread-webhook
   ```
   Rolling the webhook last keeps policy admission available throughout.
4. Verify: leader Lease renewTime is fresh; a canary deployment with
   `schedulerName: service-spread-scheduler` and the service label
   schedules; `kubectl get events` shows no FailedScheduling loops.

## 7. Rollback

Rolling back the images (re-run step 3 with the old tag) is safe — no
data migrations exist; reservations live in memory and rebuild from real
pod counts after restart (reconciler, design §4.4; regression-tested by
`hack/e2e/chaos.sh` c1). CRD/RBAC rollbacks are NOT required for an
image-only rollback.

If a rollback spans a CRD schema change, treat the CRD as one-way:
older binaries reading a newer CRD may mis-decode optional fields. Pin
CRD + images together in that case.

## 8. Known issues (operational)

1. **KEDA installed after the scheduler started is invisible** until the
   scheduler restarts (one-shot discovery probe in
   `internal/spread/plugins_api.go newKEDAIndexer`). Symptom: replica
   targets stay on the deployment source after adding KEDA. Fix:
   `kubectl -n service-spread-system rollout restart deploy/service-spread-scheduler`.
   The e2e script `hack/e2e/keda.sh` automates this detection+restart.
2. **Gauge series outlive their objects**: deleting a service namespace
   does not remove its `service_spread_*` label sets from `/metrics`
   (GaugeVec has no expiry). Long-lived clusters accumulate stale series;
   cosmetic today, revisit if scrape cardinality matters.
3. **RBAC drift stalls are near-silent**: a missing informer permission
   can wedge the scheduler at cache sync while the metrics endpoint
   stays up. The lease renewTime (step 5) is the reliable health signal.
   The RBAC contract test (`internal/spread/rbac_expected_test.go`)
   guards the manifest; the e2e entry scripts re-apply it onto clusters;
   nothing guards a production cluster automatically — re-apply on every
   upgrade (step 6.2).
4. **Webhook cert expiry** (cert-manager renews automatically; the manual
   path does not). Symptom of expiry: policy writes fail with x509 or
   connection refused errors while the webhook pods are Running.

## 9. Verification suite (kind)

- `hack/e2e/matrix.sh` — acceptance matrix (cases 1–6, 9, 11–13)
- `hack/e2e/chaos.sh` — failure drills (restart, cold-start, KEDA-absent,
  webhook-down)
- `hack/e2e/keda.sh` — observer KEDA-present paths (needs KEDA ≥ 2.12)
- `hack/e2e/verify-observer.sh` — observer wiring probe

All three entry scripts re-apply RBAC automatically (drift guard).
