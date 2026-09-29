# Deployment Checklist

Condensed from RUNBOOK.md with the 2026-09-28 ACK field verification
baked in. Read RUNBOOK §2 for the full install-order rationale.

## Target cluster profile

- [ ] Kubernetes 1.28.x (1.30+ works; see RUNBOOK §4 VAP notes)
- [ ] Nodes can reach the image registry (mainland: ACR, NOT ghcr — see below)
- [ ] cert-manager present OR the manual-cert path (RUNBOOK §2.5)

## Install (in order — dependencies are real)

- [ ] 1. `kubectl apply -f config/manager/namespace.yaml`
- [ ] 2. `kubectl apply -f config/crd/bases/` (blocking for everything)
- [ ] 3. `kubectl apply -f config/rbac/scheduler.yaml` AND `config/rbac/webhook.yaml`
      — EVERY upgrade re-applies BOTH (three 2026-09 drift incidents)
- [ ] 4. `kubectl apply -f config/manager/configmap.yaml` (webhook readiness gate)
- [ ] 5. cert-manager path: `kubectl apply -f config/webhook/certificates.yaml`,
      then `kubectl -n service-spread-system wait --for=condition=Ready \
      certificate/service-spread-webhook-serving-cert --timeout=120s`
      (Manual path: self-signed secret + kubectl patch caBundle — RUNBOOK §2.5)
- [ ] 6. `kubectl apply -f config/webhook/service.yaml`
- [ ] 7. Deployments with the PINNED image tag (never :latest in production):
      mainland clusters use the ACR mirror —
      `sed 's|ghcr.io/cnyup/service-spread-scheduler/|registry.cn-hangzhou.aliyuncs.com/cnyup/|; s/:latest/:vX.Y.Z/'`
      on both config/manager/*.yaml before applying
- [ ] 8. **imagePullSecret is REQUIRED for ACR personal edition** (public ≠
      anonymous): create it in service-spread-system and add
      `spec.template.spec.imagePullSecrets` to BOTH Deployments
- [ ] 9. `kubectl apply -f config/webhook/manifests.yaml` (after cert Ready —
      caBundle injection needs the Certificate to exist)
- [ ] 10. VAP (OPTIONAL, cluster-dependent): 1.28 needs BOTH apiserver flags
      (RUNBOOK §4); clusters that cannot set them skip this step — the
      scheduler-side Filter still guards the constraint

## Post-install verification

- [ ] `kubectl -n service-spread-system get pods` → scheduler 2/2, webhook 2/2
- [ ] `kubectl -n service-spread-system get lease service-spread-scheduler \
      -o jsonpath='{.spec.renewTime}'` → fresh timestamp (THE health signal;
      /healthz stays up even when scheduling is wedged — RUNBOOK §5)
- [ ] caBundle non-empty:
      `kubectl get validatingwebhookconfiguration service-spread-policy-validator \
      -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | wc -c` → >200
- [ ] Admission probe (should be REJECTED with an ssp-<hash> name hint):
      apply a ServiceSpreadPolicy named "wrong-name" with schedulerName set
- [ ] Canary: a deployment with `schedulerName: service-spread-scheduler` +
      the service label schedules across nodes within maxSkew

## Monitoring (if prometheus-operator is present)

- [ ] PrometheusRule labels MUST match the target Prometheus ruleSelector
      (this cluster uses `prometheus: <instance-name>`, NOT `release:`)
      AND the rule must live in the same namespace the operator watches
      (ruleNamespaceSelector nil = Prometheus CR's own namespace only)
      — verified live on ACK: monitoring ns + `prometheus: k8s` → 4/4 rules loaded
- [ ] Scrape config/ServiceMonitor for scheduler :9100 (obs-metrics port)
- [ ] Verify from the Prometheus API: `/api/v1/rules` shows 4 ServiceSpread rules

## Rollback

- [ ] Image-only: re-apply Deployments with the previous tag (RUNBOOK §7)
- [ ] Full: delete ns + the five cluster-scoped objects (CRD, 2×ClusterRole/
      Binding, ValidatingWebhookConfiguration, VAP) — no finalizer hooks
