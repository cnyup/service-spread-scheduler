#!/usr/bin/env bash
# e2e bootstrap for the ServiceSpread scheduler on a kind cluster.
# Usage: ssh yup-dev "cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/bootstrap.sh"
#
# Idempotent-ish: safe to re-run; existing resources are updated/replaced.
set -euo pipefail

NS=service-spread-system
SVC=service-spread-webhook
SECRET=service-spread-webhook-serving-cert
CERT_DIR=${CERT_DIR:-/tmp/ssp-e2e-certs}
WAIT=${WAIT:-300}

log() { echo "[bootstrap] $*"; }

# --- 1. namespace + CRD first ---
kubectl apply -f config/manager/namespace.yaml
kubectl apply -f config/crd/bases/

# --- 2. RBAC ---
kubectl apply -f config/rbac/scheduler.yaml
kubectl apply -f config/rbac/webhook.yaml

# --- 3. images: load from remote docker (built by hack/e2e/build.sh) ---
# kind load docker-image ghcr.io/cnyup/service-spread-scheduler/scheduler:latest
# kind load docker-image ghcr.io/cnyup/service-spread-scheduler/webhook:latest
# (performed by build.sh; here we only wait for rollout)

# --- 4. webhook cert (self-signed, injected as secret) ---
mkdir -p "$CERT_DIR"
if [[ ! -f "$CERT_DIR/tls.key" ]]; then
  log "generating self-signed cert for $SVC.$NS.svc"
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -subj "/CN=$SVC.$NS.svc" \
    -addext "subjectAltName=DNS:$SVC,DNS:$SVC.$NS,DNS:$SVC.$NS.svc,DNS:$SVC.$NS.svc.cluster.local" \
    -keyout "$CERT_DIR/tls.key" -out "$CERT_DIR/tls.crt" 2>/dev/null
fi
kubectl -n "$NS" create secret generic "$SECRET" \
  --from-file=tls.key="$CERT_DIR/tls.key" --from-file=tls.crt="$CERT_DIR/tls.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- 5. webhook deployment + service ---
kubectl apply -f config/manager/webhook.yaml
kubectl -n "$NS" rollout status deploy/$SVC --timeout=${WAIT}s

# --- 6. ValidatingAdmissionPolicy (CEL) ---
kubectl apply -f config/manager/vap-block-preset-nodename.yaml

# --- 7. scheduler configmap + deployment ---
kubectl -n "$NS" create configmap service-spread-scheduler-kubeconfig \
  --from-file=config.yaml=config/scheduler/kubeconfig.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f config/manager/scheduler.yaml
kubectl -n "$NS" rollout status deploy/service-spread-scheduler --timeout=${WAIT}s

log "bootstrap complete"
