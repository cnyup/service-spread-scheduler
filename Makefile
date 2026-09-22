# ServiceSpread Scheduler — M1 build/test entrypoints.
# Version pins follow the target Kubernetes minor version (dev-design §0):
# k8s 1.28.x ↔ controller-runtime v0.16.x ↔ controller-tools v0.13.x.

GO ?= go
LOCALBIN ?= $(shell pwd)/bin

# controller-gen pinned for reproducible CRD/deepcopy generation.
# v0.13.0 (the exact k8s-1.28/kubebuilder-4.0 pairing) cannot build under
# Go >= 1.23 toolchains (x/tools tokeninternal) nor run on macOS >= 26
# (LC_UUID); v0.16.5 is the oldest version that does. Output is standard
# apiextensions.k8s.io/v1 and is verified against envtest 1.28 in
# `make test-integration`.
CONTROLLER_TOOLS_VERSION ?= v0.16.5
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen

# envtest binaries (kube-apiserver/etcd) for integration tests.
# setup-envtest is installed from its standalone module @latest — the
# release-0.16 tag predates the GCS→GitHub-releases migration and can no
# longer list/download binaries (401). The binaries it downloads are plain
# kube-apiserver/etcd builds; 1.28.x resolves to 1.28.3 on darwin/arm64,
# the last 1.28 bundle published for that platform.
ENVTEST_VERSION ?= latest
ENVTEST_K8S_VERSION ?= 1.28.x
ENVTEST ?= $(LOCALBIN)/setup-envtest

.PHONY: all
all: generate manifests fmt vet build test-unit

## bin: create the local tools directory.
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## controller-gen: install controller-gen into bin/.
.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

## generate: run deepcopy code generation for api/ and apis/.
.PHONY: generate
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..." paths="./apis/..."

## manifests: generate the ServiceSpreadPolicy CRD manifest.
.PHONY: manifests
manifests: controller-gen
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

## setup-envtest: install the setup-envtest helper into bin/.
.PHONY: setup-envtest
setup-envtest: $(ENVTEST)
$(ENVTEST): $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

## envtest-binaries: download kube-apiserver/etcd assets for ENVTEST_K8S_VERSION.
.PHONY: envtest-binaries
envtest-binaries: setup-envtest
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)

## test-unit: unit tests only (no cluster binaries needed).
## Integration tests live behind the `integration` build tag.
.PHONY: test-unit
test-unit:
	$(GO) test ./... -count=1

## test-integration: envtest-based tests (webhook admission matrix, VAP CEL).
## Skipped automatically when KUBEBUILDER_ASSETS is not set.
.PHONY: test-integration
test-integration: envtest-binaries
	KUBEBUILDER_ASSETS=$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path --bin-dir $(LOCALBIN)) \
		$(GO) test -tags=integration ./internal/webhook/... ./test/... -count=1

## test: everything.
.PHONY: test
test: test-unit test-integration

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: build
build:
	$(GO) build ./...

## tidy: dependency hygiene check anchor.
.PHONY: tidy
tidy:
	$(GO) mod tidy
