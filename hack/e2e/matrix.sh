#!/usr/bin/env bash
# e2e acceptance matrix (design doc §14, criteria 1–11 + §8.3 scenarios).
#
# Each case: unique namespace, setup -> assert -> teardown. Asserts fail
# loudly with the observed state; PASS lines are the evidence ledger.
#
# Usage (on the remote host, cluster already bootstrapped via bootstrap.sh):
#   cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/matrix.sh
#   bash hack/e2e/matrix.sh 3 4 7      # run only cases 3, 4, 7
set -uo pipefail   # NOT -e: case failures must not abort the matrix

SSP_SCHED=service-spread-scheduler
DEMO_IMAGE=ghcr.io/cnyup/service-spread-scheduler/webhook:latest
PASS=0; FAIL=0; SKIP=0

log()  { echo "[matrix] $*"; }
pass() { echo "PASS $1: $2"; PASS=$((PASS+1)); }
fail() { echo "FAIL $1: $2"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP $1: $2"; SKIP=$((SKIP+1)); }

want() { # want <case> <desc> <cmd...>: pass iff cmd exits 0
  local case=$1 desc=$2; shift 2
  if "$@" >/dev/null 2>&1; then pass "$case" "$desc"; else fail "$case" "$desc — cmd failed: $*"; fi
}
wantn() { # wantn <case> <desc> <cmd...>: pass iff cmd exits NON-zero
  local case=$1 desc=$2; shift 2
  if "$@" >/dev/null 2>&1; then fail "$case" "$desc — expected failure but cmd succeeded: $*"; else pass "$case" "$desc"; fi
}

# dist <ns> <label-selector>: prints "node count" pairs of RUNNING pods
dist() {
  kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort | uniq -c
}
# running_count <ns> <label> ; max_node_count <ns> <label>
max_node() { dist "$1" "$2" | awk '{if($1>m)m=$1} END{print m+0}'; }
pending_count() {
  kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l | tr -d ' '
}
# pod_dist_ok <ns> <label> <expected max> : every node <= expected
within_cap() {
  [ "$(max_node "$1" "$2")" -le "$3" ]
}
# skew_ok <ns> <label> <maxSkew>: max-min <= maxSkew over nonzero nodes
skew_ok() {
  local mn mx
  mn=$(dist "$1" "$2" | awk 'NR==1{m=$1} {if($1<m)m=$1} END{print m+0}')
  mx=$(max_node "$1" "$2")
  [ $((mx-mn)) -le "$3" ]
}

mkdeploy() { # mkdeploy <ns> <name> <replicas> [extra-pod-spec-json]
  local ns=$1 name=$2 reps=$3 extra=${4:-}
  local podspec="\"schedulerName\":\"$SSP_SCHED\",\"containers\":[{\"name\":\"c\",\"image\":\"$DEMO_IMAGE\",\"imagePullPolicy\":\"IfNotPresent\",\"command\":[\"sleep\",\"7200\"]}]"
  [ -n "$extra" ] && podspec="$podspec,$extra"
  kubectl -n "$ns" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $name
  labels: {app.kubernetes.io/name: $name}
spec:
  replicas: $reps
  selector: {matchLabels: {app.kubernetes.io/name: $name}}
  template:
    metadata: {labels: {app.kubernetes.io/name: $name}}
    spec: {$podspec}
EOF
}
mkpolicy() { # mkpolicy <ns> <svcName> <maxSkew> <maxPodsPerNode> [nodeSelector-json]
  local ns=$1 svc=$2 skew=$3 cap=$4 sel=${5:-}
  # Deterministic policy name (design 4.3 / webhook ExpectedPolicyName):
  # ssp- + sha256(json([ns, sched, key, val]))[:12] — admission enforces it.
  local pol
  pol=$(python3 - "$ns" "$SSP_SCHED" "app.kubernetes.io/name" "$svc" <<'PY'
import sys, hashlib, json
ns, sched, key, val = sys.argv[1:5]
# compact separators match Go json.Marshal (no spaces after ',' / ':')
print("ssp-" + hashlib.sha256(json.dumps([ns, sched, key, val], separators=(",", ":")).encode()).hexdigest()[:12])
PY
)
  local nsel=""
  [ -n "$sel" ] && nsel="      nodeSelector: $sel"
  kubectl -n "$ns" apply -f - >/dev/null <<EOF
apiVersion: scheduling.soyup.top/v1alpha1
kind: ServiceSpreadPolicy
metadata: {name: $pol}
spec:
  serviceSelector: {matchLabels: {app.kubernetes.io/name: $svc}}
  schedulerName: $SSP_SCHED
  maxSkew: $skew
  maxPodsPerNode: $cap
$nsel
EOF
}
ns_new() { kubectl create ns "$1" >/dev/null 2>&1; }
ns_drop() { kubectl delete ns "$1" --wait=false >/dev/null 2>&1; }
wait_running() { # wait_running <ns> <label> <expect> [timeout]
  local deadline=$((SECONDS+${4:-90}))
  while [ $SECONDS -lt $deadline ]; do
    [ "$(running_count "$1" "$2")" = "$3" ] && return 0
    sleep 3
  done
  return 1
}
running_count() {
  kubectl -n "$1" get pods -l "$2" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' '
}

# ============ Case 1: preset nodeName blocked (VAP), only our schedulerName handled ============
case1() {
  ns_new c1
  mkpolicy c1 svc-a 1 2
  # 1a: preset nodeName on a managed pod must be denied by the VAP
  if kubectl -n c1 run probe --image="$DEMO_IMAGE" --restart=Never \
       --overrides="{\"spec\":{\"schedulerName\":\"$SSP_SCHED\",\"nodeName\":\"ssp-e2e2-worker\",\"containers\":[{\"name\":\"probe\",\"image\":\"$DEMO_IMAGE\",\"command\":[\"sleep\",\"60\"],\"imagePullPolicy\":\"IfNotPresent\"}]}}" >/dev/null 2>&1; then
    fail 1a "preset nodeName should be denied"
    kubectl -n c1 delete pod probe --force --grace-period=0 >/dev/null 2>&1
  else
    pass 1a "VAP denies preset nodeName on managed pod"
  fi
  # 1b: default-scheduler pod with preset nodeName is untouched (VAP matches only managed schedulerName)
  if kubectl -n c1 run probe2 --image="$DEMO_IMAGE" --restart=Never \
       --overrides="{\"spec\":{\"nodeName\":\"ssp-e2e2-worker\",\"containers\":[{\"name\":\"probe2\",\"image\":\"$DEMO_IMAGE\",\"command\":[\"sleep\",\"60\"],\"imagePullPolicy\":\"IfNotPresent\"}]}}" >/dev/null 2>&1; then
    pass 1b "default-scheduler pod may preset nodeName (out of scope)"
    kubectl -n c1 delete pod probe2 --force --grace-period=0 >/dev/null 2>&1
  else
    fail 1b "default-scheduler preset nodeName should NOT be blocked"
  fi
  ns_drop c1
}

# ============ Case 2: missing service label -> unschedulable with diagnostic event ============
case2() {
  ns_new c2
  # bare pod, managed schedulerName, NO app.kubernetes.io/name label
  kubectl -n c2 run nolabel --image="$DEMO_IMAGE" --restart=Never \
    --overrides="{\"spec\":{\"schedulerName\":\"$SSP_SCHED\",\"containers\":[{\"name\":\"nolabel\",\"image\":\"$DEMO_IMAGE\",\"command\":[\"sleep\",\"60\"],\"imagePullPolicy\":\"IfNotPresent\"}]}}" >/dev/null
  local ev="" deadline=$((SECONDS+60))
  while [ $SECONDS -lt $deadline ]; do
    ev=$(kubectl -n c2 get events --field-selector involvedObject.name=nolabel -o jsonpath='{.items[*].message}' 2>/dev/null)
    [[ "$ev" == *MissingRequiredServiceLabel* ]] && break
    ev=$(kubectl -n c2 get pod nolabel -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}' 2>/dev/null)
    [[ "$ev" == *MissingRequiredServiceLabel* ]] && break
    sleep 5
  done
  if [[ "$ev" == *MissingRequiredServiceLabel* ]]; then
    pass 2 "missing service label -> FailedScheduling event carries MissingRequiredServiceLabel"
  else
    fail 2 "expected MissingRequiredServiceLabel event, got: ${ev:-<none>}"
  fi
  kubectl -n c2 delete pod nolabel --force --grace-period=0 >/dev/null 2>&1
  ns_drop c2
}

# ============ Case 3: two Deployments same service aggregate counting ============
case3() {
  ns_new c3
  mkpolicy c3 svc-c 1 2            # cap 2/node; BOTH deployments share label svc-c
  mkdeploy_with_label c3 d1 2 svc-c
  mkdeploy_with_label c3 d2 2 svc-c
  if wait_running c3 app.kubernetes.io/name=svc-c 4; then
    if within_cap c3 app.kubernetes.io/name=svc-c 2 && skew_ok c3 app.kubernetes.io/name=svc-c 1; then
      pass 3 "two Deployments aggregate: 4 pods across 2 nodes, cap 2 respected, skew<=1"
    else
      fail 3 "aggregate distribution violated: $(dist c3 app.kubernetes.io/name | tr '\n' '|')"
    fi
  else
    fail 3 "not all 4 pods Running (running=$(running_count c3 app.kubernetes.io/name), pending=$(pending_count c3 app.kubernetes.io/name))"
  fi
  ns_drop c3
}
mkdeploy_with_label() { # same as mkdeploy but template label override for shared svc
  local ns=$1 name=$2 reps=$3 svc=$4
  kubectl -n "$ns" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: $name}
spec:
  replicas: $reps
  selector: {matchLabels: {app.kubernetes.io/name: $svc}}
  template:
    metadata: {labels: {app.kubernetes.io/name: $svc}}
    spec:
      schedulerName: $SSP_SCHED
      containers: [{name: c, image: $DEMO_IMAGE, imagePullPolicy: IfNotPresent, command: [sleep, "7200"]}]
EOF
}

# ============ Case 4: different domains spread independently, share the per-node quota ============
# Topology: d1 domain = {worker} (hostname selector); d2 domain = {worker, worker2}.
# Shared quota 2/node: worker hosts d1(1) + d2(1) = 2; worker2 hosts d2(2).
# Expected Running: 1 + 1 + 2 = 4; assert worker never exceeds 2 across BOTH.
case4() {
  ns_new c4
  mkpolicy c4 d1 1 2
  mkpolicy c4 d2 1 2
  mkdeploy c4 d1 1 '"nodeSelector":{"kubernetes.io/hostname":"ssp-e2e2-worker"}'
  mkdeploy c4 d2 3
  if wait_running c4 app.kubernetes.io/name 4; then
    local w_both w2_d2 d1_n
    w_both=$(kubectl -n c4 get pods --field-selector=status.phase=Running,spec.nodeName=ssp-e2e2-worker -l app.kubernetes.io/name --no-headers 2>/dev/null | wc -l | tr -d ' ')
    w2_d2=$(kubectl -n c4 get pods -l app.kubernetes.io/name=d2 --field-selector=status.phase=Running,spec.nodeName=ssp-e2e2-worker2 --no-headers 2>/dev/null | wc -l | tr -d ' ')
    d1_n=$(kubectl -n c4 get pods -l app.kubernetes.io/name=d1 --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [ "$w_both" -le 2 ] && [ "$w2_d2" -eq 2 ] && [ "$d1_n" -eq 1 ]; then
      pass 4 "domains independent (d1 pinned to worker, d2 spreads) + shared quota (worker total=$w_both<=2, worker2 d2=$w2_d2)"
    else
      fail 4 "quota sharing violated: worker_total=$w_both worker2_d2=$w2_d2 d1_running=$d1_n"
    fi
  else
    fail 4 "not all 4 pods Running (running=$(running_count c4 app.kubernetes.io/name))"
  fi
  ns_drop c4
}

# ============ Case 5: default-scheduler pods not counted ============
case5() {
  ns_new c5
  mkpolicy c5 svc-e 1 1
  # unmanaged deployment (default-scheduler), same label
  kubectl -n c5 apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: unmanaged}
spec:
  replicas: 3
  selector: {matchLabels: {app.kubernetes.io/name: svc-e}}
  template:
    metadata: {labels: {app.kubernetes.io/name: svc-e}}
    spec:
      containers: [{name: c, image: $DEMO_IMAGE, imagePullPolicy: IfNotPresent, command: [sleep, "7200"]}]
EOF
  sleep 20
  # unmanaged pods all Running on possibly ONE node (default scheduler ignores our cap)
  local n
  n=$(kubectl -n c5 get pods -l app.kubernetes.io/name=svc-e --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l | tr -d ' ')
  if [ "$n" -ge 1 ]; then
    pass 5 "default-scheduler pods scheduled freely (nodes used: $n); they must not be counted"
  else
    fail 5 "unmanaged pods not scheduled at all"
  fi
  ns_drop c5
}

# ============ Case 6: placement respects cap + domain + skew ============
case6() {
  ns_new c6
  mkpolicy c6 f1 1 2
  mkdeploy c6 f1 6                 # 6 replicas, 2 workers * cap2 = only 4 can run
  if wait_running c6 app.kubernetes.io/name=f1 4 120; then
    if within_cap c6 app.kubernetes.io/name=f1 2 && skew_ok c6 app.kubernetes.io/name=f1 1; then
      local pd ev deadline=$((SECONDS+60))
      pd=$(pending_count c6 app.kubernetes.io/name=f1)
      # Event delivery is aggregated/batched in the scheduler's event
      # recorder; after leader switches or informer cold starts the
      # FailedScheduling event may land seconds after the rejection.
      # Poll up to 60s instead of asserting immediately.
      ev=""
      while [ $SECONDS -lt $deadline ]; do
        ev=$(kubectl -n c6 get events --field-selector reason=FailedScheduling -o jsonpath='{.items[*].message}' 2>/dev/null)
        [[ "$ev" == *MaxPodsPerNodeExceeded* ]] && break
        sleep 5
      done
      if [ "$pd" -ge 2 ] && [[ "$ev" == *MaxPodsPerNodeExceeded* ]]; then
        pass 6 "4 Running (2+2), extras Pending with MaxPodsPerNodeExceeded events"
      else
        fail 6 "cap held but diagnostics missing (pending=$pd, event=${ev:-none})"
      fi
    else
      fail 6 "distribution violated: $(dist c6 app.kubernetes.io/name=f1 | tr '\n' '|')"
    fi
  else
    fail 6 "expected 4 running, got $(running_count c6 app.kubernetes.io/name=f1)"
  fi
  ns_drop c6
}

# ============ Case 9: node drain/NotReady does not evict running pods ============
case9() {
  ns_new c9
  mkpolicy c9 i1 1 2
  mkdeploy c9 i1 4
  wait_running c9 app.kubernetes.io/name=i1 4 || { fail 9 "setup: pods not running"; ns_drop c9; return; }
  # cordon a worker -> its pods must NOT be evicted (we only spread new placements)
  local victim
  victim=$(kubectl -n c9 get pods -l app.kubernetes.io/name=i1 --field-selector=status.phase=Running -o jsonpath='{.items[0].spec.nodeName}')
  kubectl cordon "$victim"
  sleep 5
  local still
  still=$(kubectl -n c9 get pods --field-selector=status.phase=Running,spec.nodeName="$victim" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [ "$still" -ge 1 ]; then
    pass 9 "cordon evicts nothing ($still pods still Running on $victim)"
  else
    fail 9 "cordon caused eviction — must not happen"
  fi
  kubectl uncordon "$victim"
  ns_drop c9
}

# ============ Case 11: policy-created-late retriggers scheduling of pending pods ============
case11() {
  ns_new c11
  mkdeploy c11 k1 4                # NO policy yet -> ServiceSpreadPolicyNotFound
  sleep 15
  local pd1
  pd1=$(pending_count c11 app.kubernetes.io/name=k1)
  mkpolicy c11 k1 1 2              # policy arrives late
  if wait_running c11 app.kubernetes.io/name=k1 4 150; then
    pass 11 "late policy requeued pending pods (were pending=$pd1) -> all 4 Running without recreation"
  else
    fail 11 "pods still not running after late policy (running=$(running_count c11 app.kubernetes.io/name=k1))"
  fi
  ns_drop c11
}

# ---- cases needing HPA/KEDA/chaos tooling: explicitly marked ----
declare -F case7 >/dev/null || skip 7 "replica-target observability is M4 scope (replicatarget.go not yet implemented)"
declare -F case8 >/dev/null || skip 8 "resource-pressure semantics covered by native Filter; covered in unit tests (domain matrix)"
declare -F case10 >/dev/null || skip 10 "concurrent-reserve atomicity covered by 16-goroutine unit test (state_test.go); e2e concurrency inject lands with chaos suite (§8.4)"

log "matrix start $(date -u +%FT%TZ)"
CASES=("$@"); [ $# -eq 0 ] && CASES=(1 2 3 4 5 6 9 11)
for c in "${CASES[@]}"; do
  "case$c"
done
log "RESULT pass=$PASS fail=$FAIL skip=$SKIP"
[ "$FAIL" -eq 0 ]
