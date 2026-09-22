#!/usr/bin/env bash
# Build both images on the remote host and load them into the kind cluster.
# Usage: ssh yup-dev "cd /root/code/scheduling/service-spread-scheduler && bash hack/e2e/build.sh"
set -euo pipefail

KIND_CLUSTER=${KIND_CLUSTER:-ssp-e2e}

# BuildKit cache mounts need DOCKER_BUILDKIT=1 (default on modern docker).
docker build -f build/Dockerfile.scheduler -t ghcr.io/cnyup/service-spread-scheduler/scheduler:latest .
docker build -f build/Dockerfile.webhook   -t ghcr.io/cnyup/service-spread-scheduler/webhook:latest .

kind load docker-image ghcr.io/cnyup/service-spread-scheduler/scheduler:latest --name "$KIND_CLUSTER"
kind load docker-image ghcr.io/cnyup/service-spread-scheduler/webhook:latest   --name "$KIND_CLUSTER"
echo "[build] images built and loaded into kind cluster $KIND_CLUSTER"
