#!/usr/bin/env bash
# KEDA observer e2e (dev-design §6/§10): proves the replica-target
# observer's KEDA-present semantics on a real cluster, mirroring the
# hand-run evidence recorded in HANDOFF 2026-09-28.
#
#   k1  KEDA present + ScaledObject  — KEDA creates an HPA; the observer
#                                    must report the HPA's desiredReplicas
#                                    (priority-chain top) with
#                                    fallback_active=0.
#   k2  KEDA present, no SO          — a deployment without a ScaledObject
#                                    stays on the deployment source
#                                    (replica_target == replicas,
#                                    fallback_active=0): presence of KEDA
#                                    must not perturb unmanaged services.
#
# Skips (with the ledger line printed) when KEDA is not installed — chaos.sh
# c3 owns the KEDA-absent leg.
#
# Self-healing entry conditions (each was a real 2026-09-28 incident):
#   - RBAC drift: config/rbac/scheduler.yaml is re-applied (idempotent)
#     before anything runs. Three same-family incidents (HPA informer,
#     events.k8s.io, keda.sh) came from "manifest updated, cluster stale".
#   - Late KEDA: newKEDAIndexer (internal/spread/plugins_api.go) probes
#     discovery ONCE at scheduler start. KEDA installed after the scheduler
#     started is invisible until restart, so we restart when the
#     scaledobjects CRD is younger than the running scheduler pods.
#
# Usage (remote host, cluster bootstrapped, KEDA installed):
#   cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/keda.sh
#   bash hack/e2e/keda.sh 1        # only k1
set -uo pipefail   # NOT -e: failures must not abort the suite

SSP_SCHED=service-spread-scheduler
SSP_NS=service-spread-system
DEMO_IMAGE=ghcr.io/cnyup/service-spread-scheduler/webhook:latest
METRICS_PORT=${METRICS_PORT:-9100}
LOCAL_PORT=${LOCAL_PORT:-19400}
EXPORT_INTERVAL=${EXPORT_INTERVAL:-30}   # seconds; matches plugins_api.go
PASS=0; FAIL=0; SKIP=0
KEDA_NS=keda
NS1=keda-e2e-k1   # distinct namespaces: exported gauge series outlive ns
NS2=keda-e2e-k2   # deletion (GaugeVec never removes label sets), so k1's
                  # series must not collide with k2's grep

log()  { echo "[keda] $*"; }
pass() { echo "PASS keda$1: $2"; PASS=$((PASS+1)); }
fail() { echo "FAIL keda$1: $2"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP keda$1: $2"; SKIP=$((SKIP+1)); }

wait_running() { # <ns> <label> <expect> [timeout]
  local deadline=$((SECONDS+${4:-120}))
  while [ $SECONDS -lt $deadline ]; do
    local n
    n=$(kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
    [ "$n" = "$3" ] && return 0
    sleep 3
  done
  return 1
}

mkdeploy() { # <ns> <name> <replicas>
  kubectl -n "$1" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $2
  labels: {app.kubernetes.io/name: $2}
spec:
  replicas: $3
  selector: {matchLabels: {app.kubernetes.io/name: $2}}
  template:
    metadata: {labels: {app.kubernetes.io/name: $2}}
    spec:
      schedulerName: $SSP_SCHED
      containers:
      - name: c
        image: $DEMO_IMAGE
        imagePullPolicy: IfNotPresent
        command: ["sleep", "7200"]
EOF
}

# scrape_metrics <grep-pattern>: port-forwards the leader-eligible scheduler
# pod and prints matching metric lines. Exits non-zero when the endpoint is
# unreachable (caller decides pass/fail/skip semantics).
scrape_metrics() { # <pattern>
  local pf pid out
  pf=$(kubectl -n "$SSP_NS" get pods -l app=$SSP_SCHED -o name | head -1)
  [ -n "$pf" ] || return 1
  kubectl -n "$SSP_NS" port-forward "$pf" ${LOCAL_PORT}:${METRICS_PORT} >/dev/null 2>&1 &
  pid=$!
  sleep 3
  out=$(curl -s localhost:${LOCAL_PORT}/metrics 2>/dev/null | grep -E "$1" || true)
  kill "$pid" 2>/dev/null
  printf '%s\n' "$out"
}

# gauge <lines> <metric> <namespace>: value of <metric> for the namespace, or ""
gauge() { # <lines> <metric> <namespace>
  printf '%s\n' "$1" | grep "^$2" | grep "namespace=\"$3\"" | awk '{print $2}' | head -1
}

# ---- entry conditions ----

# RBAC drift guard: the cluster role is compared against the manifest, not
# trusted. apply is idempotent; a "configured" outcome means drift existed.
if [ -f config/rbac/scheduler.yaml ]; then
  rbac_out=$(kubectl apply -f config/rbac/scheduler.yaml 2>&1 | grep -c ' configured$' || true)
  [ "$rbac_out" -gt 0 ] && log "RBAC drift detected and repaired (config/rbac/scheduler.yaml)"
fi

if ! kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1; then
  skip 1 "KEDA not installed — chaos.sh c3 owns the KEDA-absent leg"
  skip 2 "KEDA not installed"
  log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"
  exit 0
fi

# Late-KEDA guard: restart the scheduler when its pods predate the CRD, so
# the one-shot discovery probe in newKEDAIndexer re-runs. Compare unix
# seconds; ties restart (safe, cheap, converges in one rollout).
crd_created=$(kubectl get crd scaledobjects.keda.sh -o jsonpath='{.metadata.creationTimestamp}')
crd_s=$(date -d "$crd_created" +%s 2>/dev/null || echo 0)
oldest_start=$(kubectl -n "$SSP_NS" get pods -l app=$SSP_SCHED \
  -o jsonpath='{range .items[*]}{.status.startTime}{"\n"}{end}' 2>/dev/null | sort | head -1)
pod_s=$(date -d "$oldest_start" +%s 2>/dev/null || echo 0)
if [ "$crd_s" -gt 0 ] && [ "$pod_s" -gt 0 ] && [ "$pod_s" -le "$crd_s" ]; then
  log "scheduler predates KEDA CRD ($oldest_start <= $crd_created): restarting for discovery re-probe"
  kubectl -n "$SSP_NS" rollout restart deploy/$SSP_SCHED >/dev/null
  kubectl -n "$SSP_NS" rollout status deploy/$SSP_SCHED --timeout=180s >/dev/null 2>&1 \
    || { fail 0 "scheduler rollout stuck after late-KEDA restart"; log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"; exit 1; }
  sleep 10   # informers sync before the first export tick
fi

keda_pods=$(kubectl -n "$KEDA_NS" get pods --no-headers 2>/dev/null | grep -c Running || true)
if [ "$keda_pods" -lt 3 ]; then
  fail 0 "KEDA pods not all Running ($keda_pods/3) — fix keda namespace first"
  log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"
  exit 1
fi

# ns_reset <ns>: delete and wait for actual disappearance. Namespace
# deletion is asynchronous (finalizers); recreating immediately races with
# "object is being deleted" / "namespace is being terminated".
ns_reset() {
  kubectl delete ns "$1" --wait=false >/dev/null 2>&1
  local deadline=$((SECONDS+60))
  while [ $SECONDS -lt $deadline ]; do
    kubectl get ns "$1" >/dev/null 2>&1 || return 0
    sleep 2
  done
  log "WARN: ns $1 still terminating after 60s"
}

# ---- k1: present + ScaledObject -> HPA source ----

k1() {
  ns_reset "$NS1"
  kubectl create ns "$NS1" >/dev/null
  mkdeploy "$NS1" kedaweb 2
  # cron trigger: start/end must be 5-field cron expressions; KEDA's HPA
  # creation fails on short forms ("expected exactly 5 fields").
  kubectl -n "$NS1" apply -f - >/dev/null <<EOF
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kedaweb-so
spec:
  scaleTargetRef: {name: kedaweb}
  minReplicaCount: 4
  maxReplicaCount: 6
  cooldownPeriod: 300
  triggers:
  - type: cron
    metadata:
      timezone: Asia/Shanghai
      start: "0 0 * * *"
      end: "0 1 * * *"
      desiredReplicas: "5"
EOF
  # Wait until KEDA has materialised the HPA AND settled desiredReplicas to
  # a positive value: status.desiredReplicas is transiently "0" right after
  # the HPA object appears, which is not a usable assertion oracle.
  # (No ServiceSpreadPolicy here: cap/skew are not under test; the pods stay
  # Pending, which is fine — the observer counts Pending+Running with the
  # service label, and replica_target reads the HPA, not the pods.)
  local hpa_desired=""
  local deadline=$((SECONDS+120))
  while [ $SECONDS -lt $deadline ]; do
    hpa_desired=$(kubectl -n "$NS1" get hpa keda-hpa-kedaweb-so \
      -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || true)
    [ -n "$hpa_desired" ] && [ "$hpa_desired" -gt 0 ] 2>/dev/null && break
    sleep 3
  done
  if [ -z "$hpa_desired" ] || [ "$hpa_desired" -le 0 ] 2>/dev/null; then
    fail 1 "KEDA never settled hpa desiredReplicas>0 (got '$hpa_desired'; check keda-operator logs)"
    ns_reset "$NS1"
    return
  fi

  sleep $((EXPORT_INTERVAL + 10))   # one export tick past the HPA settling

  # Re-read desired AFTER the settle window: the value sampled at loop exit
  # may predate KEDA's final scale decision.
  hpa_desired=$(kubectl -n "$NS1" get hpa keda-hpa-kedaweb-so \
    -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || echo 0)

  local out rt fa pods
  out=$(scrape_metrics '^service_spread_(replica_target|observed_pods|fallback_active)')
  rt=$(gauge "$out" service_spread_replica_target "$NS1")
  fa=$(gauge "$out" service_spread_fallback_active "$NS1")
  pods=$(gauge "$out" service_spread_observed_pods "$NS1")
  if [ "$rt" = "$hpa_desired" ] && [ "$fa" = "0" ]; then
    pass 1 "KEDA present + SO: replica_target=$rt (HPA source), fallback_active=0, observed_pods=$pods"
  else
    fail 1 "want replica_target=$hpa_desired fallback_active=0, got rt='$rt' fa='$fa' pods='$pods'"
  fi
  ns_reset "$NS1"
}

# ---- k2: present, no SO -> deployment source ----

k2() {
  ns_reset "$NS2"
  kubectl create ns "$NS2" >/dev/null
  mkdeploy "$NS2" plainweb 3
  # No policy either: unmanaged-by-KEDA must behave exactly like the
  # KEDA-absent world (chaos.sh c3). Pods Pending is fine (no policy);
  # replica_target must equal spec.replicas.
  sleep $((EXPORT_INTERVAL + 10))

  local out rt fa
  out=$(scrape_metrics '^service_spread_(replica_target|fallback_active)')
  rt=$(gauge "$out" service_spread_replica_target "$NS2")
  fa=$(gauge "$out" service_spread_fallback_active "$NS2")
  if [ "$rt" = "3" ] && [ "$fa" = "0" ]; then
    pass 2 "KEDA present, no SO: deployment source, replica_target=3, fallback_active=0"
  else
    fail 2 "want replica_target=3 fallback_active=0, got rt='$rt' fa='$fa'"
  fi
  ns_reset "$NS2"
}

if [ $# -eq 0 ]; then set -- 1 2; fi
for c in "$@"; do
  case $c in
    1) k1 ;; 2) k2 ;;
    *) log "unknown case $c" ;;
  esac
done
log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"
[ "$FAIL" -eq 0 ]
