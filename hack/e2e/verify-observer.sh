#!/usr/bin/env bash
# Observer end-to-end probe (M4): proves the replica-target observer
# (internal/spread/replicatarget.go) is wired into the scheduler process and
# that its gauges are scrapable off the /metrics endpoint served by
# cmd/scheduler/main.go on :9100.
#
# Flow (everything runs on the remote host, mirroring hack/e2e/build.sh and
# hack/e2e/bootstrap.sh):
#   1. rebuild the scheduler image and load it into the kind cluster
#   2. rollout-restart the scheduler deployment, wait until Ready
#   3. create a throwaway deployment that carries the service label so the
#      observer has at least one series to export. Without a labelled
#      deployment the gauge vectors have zero children and prometheus emits
#      neither the HELP/TYPE nor any sample line — a bare name grep would be
#      vacuous. The probe namespace is deleted on exit.
#   4. port-forward the observer metrics port and poll /metrics until both
#      service_spread_replica_target and service_spread_observed_pods appear
#   5. print the exported series and PASS/FAIL
#
# Idempotent: safe to re-run; the probe namespace is torn down on exit.
#
# Usage (on the remote host):
#   ssh yup-dev "cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/verify-observer.sh"
set -euo pipefail

CLUSTER=${CLUSTER:-ssp-e2e2}
NS=${NS:-service-spread-system}
DEPLOY=${DEPLOY:-service-spread-scheduler}
IMAGE=${IMAGE:-ghcr.io/cnyup/service-spread-scheduler/scheduler:latest}
DEMO_IMAGE=${DEMO_IMAGE:-ghcr.io/cnyup/service-spread-scheduler/webhook:latest}
METRICS_PORT=${METRICS_PORT:-9100}
LOCAL_PORT=${LOCAL_PORT:-19100}
WAIT=${WAIT:-300}
# Observer config must match config/scheduler/kubeconfig.yaml: the exporter
# keys deployments/pods on this label, and the probe pod uses the managed
# schedulerName so it exercises the observed-pods path.
SVC_LABEL_KEY=${SVC_LABEL_KEY:-app.kubernetes.io/name}
MANAGED_SCHED=${MANAGED_SCHED:-service-spread-scheduler}
PROBE_NS=${PROBE_NS:-ssp-observer-probe}
PROBE_NAME=${PROBE_NAME:-observer-probe}

log() { echo "[verify-observer] $*"; }

PF_LOG=/tmp/verify-observer-port-forward.log
SCRAPE=/tmp/verify-observer-metrics.txt
PF_PID=""
cleanup() {
  if [ -n "$PF_PID" ] && kill -0 "$PF_PID" 2>/dev/null; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
  fi
  kubectl delete ns "$PROBE_NS" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- 1. rebuild + load the scheduler image ---
log "building $IMAGE"
docker build -q -f build/Dockerfile.scheduler -t "$IMAGE" . >/dev/null
log "loading $IMAGE into kind cluster $CLUSTER"
kind load docker-image "$IMAGE" --name "$CLUSTER"

# --- 2. roll the deployment onto the freshly loaded image ---
kubectl -n "$NS" rollout restart deploy/"$DEPLOY"
kubectl -n "$NS" rollout status  deploy/"$DEPLOY" --timeout="${WAIT}s"
log "scheduler deployment ready"

# --- 3. label-carrying probe deployment (source of observer series) ---
kubectl create ns "$PROBE_NS" >/dev/null 2>&1 || true
kubectl -n "$PROBE_NS" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $PROBE_NAME
spec:
  replicas: 1
  selector: {matchLabels: {$SVC_LABEL_KEY: $PROBE_NAME}}
  template:
    metadata: {labels: {$SVC_LABEL_KEY: $PROBE_NAME}}
    spec:
      schedulerName: $MANAGED_SCHED
      containers:
        - name: c
          image: $DEMO_IMAGE
          imagePullPolicy: IfNotPresent
          command: ["sleep", "7200"]
EOF

# --- 4. port-forward the observer metrics endpoint ---
log "port-forward $NS/$DEPLOY :$METRICS_PORT -> 127.0.0.1:$LOCAL_PORT"
kubectl -n "$NS" port-forward deploy/"$DEPLOY" "$LOCAL_PORT:$METRICS_PORT" \
  >"$PF_LOG" 2>&1 &
PF_PID=$!

fetch() { curl -fsS "http://127.0.0.1:$LOCAL_PORT/metrics" -o "$SCRAPE" 2>/dev/null; }

# --- 5. poll until both gauges appear (observer exports on a 30s ticker,
#        first cycle immediately at startup) ---
deadline=$((SECONDS+90))
until fetch && grep -q '^service_spread_replica_target' "$SCRAPE" \
              && grep -q '^service_spread_observed_pods'   "$SCRAPE"; do
  if [ $SECONDS -ge $deadline ]; then
    log "timed out waiting for observer gauges"
    log "port-forward log:"
    sed 's/^/    /' "$PF_LOG" 2>/dev/null || true
    if [ -s "$SCRAPE" ]; then
      log "service_spread_* lines in last scrape:"
      grep '^service_spread_' "$SCRAPE" | sed 's/^/    /' || true
    fi
    echo "FAIL: observer metrics not found on /metrics"
    exit 1
  fi
  sleep 3
done

# --- 6. report the exported series as evidence ---
log "observer series exported:"
grep '^service_spread_' "$SCRAPE" | sed 's/^/    /' || true
echo "PASS: /metrics exports service_spread_replica_target and service_spread_observed_pods"
