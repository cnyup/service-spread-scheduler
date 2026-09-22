#!/usr/bin/env bash
# e2e bootstrap for the ServiceSpread scheduler on a kind cluster.
#
# Verified working flow (2026-09-22, kind v0.25.0 / kindest/node:v1.28.15):
# every step below mirrors what was actually executed and observed on the
# ssp-e2e2 cluster. Idempotent: safe to re-run against an existing cluster.
#
# Usage: ssh yup-dev "cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/bootstrap.sh"
# Assumes: hack/e2e/build.sh already built+loaded images, kubectl points at
# the target cluster, and a control-plane node named <cluster>-control-plane.
#
# Known 1.28 specifics baked in:
# - VAP needs BOTH --feature-gates=ValidatingAdmissionPolicy=true AND
#   --runtime-config=admissionregistration.k8s.io/v1beta1=true (beta API is
#   not served by default); injected via the apiserver static-pod manifest.
# - The apiserver restart triggered by the manifest patch briefly severs
#   leader election (~60-90s); it recovers on its own.
set -euo pipefail

CLUSTER=${CLUSTER:-ssp-e2e2}
NS=service-spread-system
SVC=service-spread-webhook-service
SECRET=service-spread-webhook-serving-cert
CERT_DIR=${CERT_DIR:-/tmp/ssp-e2e-certs}
WAIT=${WAIT:-300}
CP_NODE="${CLUSTER}-control-plane"

log() { echo "[bootstrap] $*"; }

# --- 1. namespace + CRD first ---
kubectl apply -f config/manager/namespace.yaml
kubectl apply -f config/crd/bases/

# --- 2. RBAC (scheduler ClusterRole covers bind loop + informer surface) ---
kubectl apply -f config/rbac/scheduler.yaml
kubectl apply -f config/rbac/webhook.yaml

# --- 3. webhook serving cert: self-signed, injected as a Secret ---
# (cert-manager Issuer/Certificate in config/webhook/certificates.yaml is
# the production path; e2e skips it — no cert-manager CRDs on kind.)
mkdir -p "$CERT_DIR"
log "generating self-signed cert for $SVC.$NS.svc"
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=$SVC.$NS.svc" \
  -addext "subjectAltName=DNS:$SVC,DNS:$SVC.$NS,DNS:$SVC.$NS.svc,DNS:$SVC.$NS.svc.cluster.local" \
  -keyout "$CERT_DIR/tls.key" -out "$CERT_DIR/tls.crt" 2>/dev/null
kubectl -n "$NS" create secret generic "$SECRET" \
  --from-file=tls.key="$CERT_DIR/tls.key" --from-file=tls.crt="$CERT_DIR/tls.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- 4. webhook Service + Deployment, then roll to pick up the secret ---
kubectl apply -f config/webhook/service.yaml
kubectl apply -f config/manager/configmap.yaml   # readiness gate: shared config
kubectl apply -f config/manager/webhook.yaml
kubectl -n "$NS" rollout status deploy/service-spread-webhook --timeout=${WAIT}s

# --- 5. ValidatingWebhookConfiguration with caBundle patched in ---
# manifests.yaml keeps the cert-manager annotation for production; here we
# strip the annotation and inject the CA with a JSON patch file (patch-file
# avoids shell-quoting issues with base64 newlines — a sed substitution
# landed the caBundle in the wrong YAML section and silently produced an
# empty bundle).
kubectl apply -f config/webhook/manifests.yaml
CA=$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.tls\\.crt}")
printf '[{"op":"add","path":"/webhooks/0/clientConfig/caBundle","value":"%s"}]' "$CA" > /tmp/ssp-ca-patch.json
kubectl patch validatingwebhookconfiguration service-spread-policy-validator \
  --type=json --patch-file /tmp/ssp-ca-patch.json
rm -f /tmp/ssp-ca-patch.json

# --- 6. scheduler configmap + deployment ---
kubectl -n "$NS" create configmap service-spread-scheduler-kubeconfig \
  --from-file=config.yaml=config/scheduler/kubeconfig.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f config/manager/scheduler.yaml
kubectl -n "$NS" rollout status deploy/service-spread-scheduler --timeout=${WAIT}s

# --- 7. VAP: enable feature gate + runtime-config on 1.28, then apply ---
# The static-pod manifest is watched by kubelet; the apiserver rolls (~90s).
# Idempotent: skip the patch when the flags are already present.
if ! docker exec "$CP_NODE" grep -q -- '--feature-gates=ValidatingAdmissionPolicy=true' \
    /etc/kubernetes/manifests/kube-apiserver.yaml 2>/dev/null; then
  log "patching apiserver manifest for VAP (feature-gate + runtime-config)"
  docker exec "$CP_NODE" cat /etc/kubernetes/manifests/kube-apiserver.yaml > /tmp/ssp-kube-apiserver.yaml
  sed -i.bak \
    -e 's|^    - kube-apiserver$|    - kube-apiserver\n    - --feature-gates=ValidatingAdmissionPolicy=true\n    - --runtime-config=admissionregistration.k8s.io/v1beta1=true|' \
    /tmp/ssp-kube-apiserver.yaml
  grep -q -- '--feature-gates=ValidatingAdmissionPolicy=true' /tmp/ssp-kube-apiserver.yaml \
    || { echo "FATAL: apiserver command line not found in manifest"; exit 1; }
  docker cp /tmp/ssp-kube-apiserver.yaml "$CP_NODE":/etc/kubernetes/manifests/kube-apiserver.yaml
  log "waiting for apiserver to roll and serve admissionregistration/v1beta1"
  for i in $(seq 1 30); do
    kubectl api-resources 2>/dev/null | grep -q validatingadmissionpolicies && break
    sleep 10
  done
fi
kubectl apply -f config/manager/vap-block-preset-nodename.yaml

log "bootstrap complete — verify with:"
log "  kubectl -n $NS get pods   # 2 scheduler + 2 webhook, all Running"
