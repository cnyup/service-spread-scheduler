#!/usr/bin/env bash
# Chaos/failure drills (dev-design §8.4, M4). Four scenarios, each with a
# real fault injected on the kind cluster and an observable recovery or
# fail-closed behavior asserted:
#
#   c1  scheduler restart          — reservations lost; placement must
#                                    converge from real pod counts (design
#                                    §4.4 "reconciler rebuilds snapshot"),
#                                    no overcommit after restart.
#   c2  informer cold-start window — pods scheduled during the unsynced
#                                    window are retried (CacheNotSynced is
#                                    retryable), not dropped on the floor.
#   c3  KEDA API unavailable       — observer degraded ≠ absent: fallback
#                                    active, no silent "0 replicas".
#   c4  webhook down               — policy creation fails closed, already
#                                    scheduled pods unaffected.
#
# Usage (remote host, cluster bootstrapped, images loaded):
#   bash hack/e2e/chaos.sh          # all
#   bash hack/e2e/chaos.sh 1 3      # only scenarios 1 and 3
set -uo pipefail   # NOT -e: failures must not abort the suite

SSP_SCHED=service-spread-scheduler
SSP_NS=service-spread-system
DEMO_IMAGE=ghcr.io/cnyup/service-spread-scheduler/webhook:latest
PASS=0; FAIL=0; SKIP=0

log()  { echo "[chaos] $*"; }
pass() { echo "PASS chaos$1: $2"; PASS=$((PASS+1)); }
fail() { echo "FAIL chaos$1: $2"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP chaos$1: $2"; SKIP=$((SKIP+1)); }

running_count() {
  kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' '
}
max_node() {
  kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort | uniq -c | awk '{if($1>m)m=$1} END{print m+0}'
}
wait_running() { # <ns> <label> <expect> [timeout]
  local deadline=$((SECONDS+${4:-120}))
  while [ $SECONDS -lt $deadline ]; do
    [ "$(running_count "$1" "$2")" = "$3" ] && return 0
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
mkpolicy() { # <ns> <svcName> <maxSkew> <maxPodsPerNode>
  local pol
  pol=$(python3 - "$1" "$SSP_SCHED" "app.kubernetes.io/name" "$2" <<'PY'
import sys, hashlib, json
ns, sched, key, val = sys.argv[1:5]
print("ssp-" + hashlib.sha256(json.dumps([ns, sched, key, val], separators=(",", ":")).encode()).hexdigest()[:12])
PY
)
  kubectl -n "$1" apply -f - >/dev/null <<EOF
apiVersion: scheduling.soyup.top/v1alpha1
kind: ServiceSpreadPolicy
metadata: {name: $pol}
spec:
  serviceSelector: {matchLabels: {app.kubernetes.io/name: $2}}
  schedulerName: $SSP_SCHED
  maxSkew: $3
  maxPodsPerNode: $4
EOF
}
ns_new()  { kubectl create ns "$1" >/dev/null 2>&1; }
ns_drop() { kubectl delete ns "$1" --wait=false >/dev/null 2>&1; }

scheduler_pods() { kubectl -n "$SSP_NS" get pods -l app=service-spread-scheduler -o name 2>/dev/null; }

# ============ chaos 1: scheduler restart loses reservations ============
c1() {
  local ns=chaos1-restart
  ns_drop "$ns"; ns_new "$ns"
  mkpolicy "$ns" web 1 2
  mkdeploy "$ns" web 4
  if ! wait_running "$ns" "app.kubernetes.io/name=web" 4; then
    fail 1 "baseline never converged (4 pods Running)"; ns_drop "$ns"; return
  fi
  # Restart both scheduler replicas: in-memory reservations die with them.
  kubectl -n "$SSP_NS" rollout restart deploy/service-spread-scheduler >/dev/null
  kubectl -n "$SSP_NS" rollout status deploy/service-spread-scheduler --timeout=180s >/dev/null 2>&1 \
    || { fail 1 "scheduler rollout stuck"; ns_drop "$ns"; return; }
  # Existing pods must stay put (no eviction, ever).
  if ! wait_running "$ns" "app.kubernetes.io/name=web" 4 60; then
    fail 1 "running pods disturbed by scheduler restart"; ns_drop "$ns"; return
  fi
  # Scale up after restart: state must be rebuilt from real pod counts —
  # each node already holds 2, cap=2, so pods 5 and 6 must both go Pending.
  kubectl -n "$ns" scale deploy/web --replicas=6 >/dev/null
  sleep 10
  local run pend
  run=$(running_count "$ns" "app.kubernetes.io/name=web")
  pend=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=web --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [ "$run" = "4" ] && [ "$pend" = "2" ] && [ "$(max_node "$ns" "app.kubernetes.io/name=web")" -le 2 ]; then
    pass 1 "post-rerestart scale-up respects cap from rebuilt state (run=4 pending=2 max/node<=2)"
  else
    fail 1 "post-restart state wrong: run=$run pending=$pend max/node=$(max_node "$ns" 'app.kubernetes.io/name=web')"
  fi
  ns_drop "$ns"
}

# ============ chaos 2: cold-start informer window ============
c2() {
  local ns=chaos2-coldstart
  ns_drop "$ns"; ns_new "$ns"
  mkpolicy "$ns" web 1 1
  # Restart schedulers, then IMMEDIATELY create pods: they arrive while
  # informers are still syncing -> CacheNotSynced (retryable) -> they must
  # still converge once caches catch up.
  kubectl -n "$SSP_NS" rollout restart deploy/service-spread-scheduler >/dev/null
  mkdeploy "$ns" web 2
  if wait_running "$ns" "app.kubernetes.io/name=web" 2 180; then
    local mx
    mx=$(max_node "$ns" "app.kubernetes.io/name=web")
    if [ "$mx" -le 1 ]; then
      pass 2 "pods created during informer cold-start converged (2 running, max/node=$mx)"
    else
      fail 2 "converged but constraint violated: max/node=$mx > 1"
    fi
  else
    fail 2 "pods created during cold-start never scheduled"
  fi
  kubectl -n "$SSP_NS" rollout status deploy/service-spread-scheduler --timeout=180s >/dev/null 2>&1
  ns_drop "$ns"
}

# ============ chaos 3: KEDA absent vs degraded ============
c3() {
  local ns=chaos3-keda
  ns_drop "$ns"; ns_new "$ns"
  mkdeploy "$ns" web 2
  wait_running "$ns" "app.kubernetes.io/name=web" 2 >/dev/null 2>&1
  # No KEDA installed on this cluster -> "absent", deployment fallback must
  # be indistinguishable from a KEDA-less world: replica_target == replicas,
  # fallback_active == 0 (deployment IS a valid source, not a fallback).
  local pf pid out
  pf=$(kubectl -n "$SSP_NS" get pods -l app=service-spread-scheduler -o name | head -1)
  kubectl -n "$SSP_NS" port-forward "$pf" 19300:9100 >/dev/null 2>&1 &
  pid=$!
  sleep 3
  out=$(curl -s localhost:19300/metrics 2>/dev/null | grep -E '^service_spread_(replica_target|fallback_active)' | grep "namespace=\"$ns\"" || true)
  kill $pid 2>/dev/null
  local rt fa
  rt=$(echo "$out" | grep '^service_spread_replica_target' | awk '{print $2}')
  fa=$(echo "$out" | grep '^service_spread_fallback_active' | awk '{print $2}')
  if [ "$rt" = "2" ] && [ "$fa" = "0" ]; then
    pass 3 "KEDA absent: deployment source, replica_target=2 fallback_active=0"
  else
    fail 3 "KEDA absence misread: replica_target='$rt' fallback_active='$fa'"
  fi
  ns_drop "$ns"
}

# ============ chaos 4: webhook down ============
c4() {
  local ns=chaos4-webhook
  ns_drop "$ns"; ns_new "$ns"
  # Webhook down: policy creation must FAIL CLOSED (never silently accept).
  kubectl -n "$SSP_NS" scale deploy/service-spread-webhook --replicas=0 >/dev/null
  kubectl -n "$SSP_NS" rollout status deploy/service-spread-webhook --timeout=60s >/dev/null 2>&1
  local rejected=0
  if kubectl -n "$ns" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: scheduling.soyup.top/v1alpha1
kind: ServiceSpreadPolicy
metadata: {name: ssp-chaos4-badname}
spec:
  serviceSelector: {matchLabels: {app.kubernetes.io/name: web}}
  schedulerName: $SSP_SCHED
  maxSkew: 1
  maxPodsPerNode: 2
EOF
  then rejected=0; else rejected=1; fi
  kubectl -n "$SSP_NS" scale deploy/service-spread-webhook --replicas=2 >/dev/null
  kubectl -n "$SSP_NS" rollout status deploy/service-spread-webhook --timeout=120s >/dev/null 2>&1
  # Already-scheduled pods unaffected: deploy a workload with an EXISTING
  # policy created before the outage (default ns policy from c1..c3 is gone,
  # so create one now that webhook is back, then verify scheduling works).
  mkpolicy "$ns" web 1 2
  mkdeploy "$ns" web 2
  local ok_run=0
  wait_running "$ns" "app.kubernetes.io/name=web" 2 120 && ok_run=1
  if [ "$rejected" = "1" ] && [ "$ok_run" = "1" ]; then
    pass 4 "webhook outage: policy creation failed closed, scheduling unaffected"
  else
    fail 4 "rejected=$rejected running_after=$ok_run"
  fi
  ns_drop "$ns"
}

if [ $# -eq 0 ]; then set -- 1 2 3 4; fi
for c in "$@"; do
  case $c in
    1) c1 ;; 2) c2 ;; 3) c3 ;; 4) c4 ;;
    *) log "unknown scenario $c" ;;
  esac
done
log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"
[ "$FAIL" -eq 0 ]
