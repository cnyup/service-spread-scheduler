# Release Checklist

Every step has an automated verification; do not skip to the tag.

## Pre-flight (once per release)

- [ ] `go build ./...` clean on the release commit
- [ ] `go vet ./...` clean
- [ ] `gofmt -l internal/ cmd/ api/ apis/` empty (generated files excluded)
- [ ] `go test ./... -race` green (envtest cases auto-skip without KUBEBUILDER_ASSETS)
- [ ] `git status` clean; HEAD == origin/main
- [ ] RBAC contract test green (it runs in the suite; it is the guard that
      scheduler.yaml matches the code's actual access surface)
- [ ] HANDOFF.md "下一步" reviewed — no pending blocking items

## Tag & pipelines

- [ ] `git tag vX.Y.Z && git push origin vX.Y.Z`
- [ ] GitHub Actions `release` run: **success** (multi-arch ghcr push)
- [ ] `push-acr` job: Config check prints 5×SET + `ACR_NAMESPACE slash-count: 0`
- [ ] `push-acr` job: **success** (mirrors to `registry.cn-hangzhou.aliyuncs.com/cnyup/{scheduler,webhook}:X.Y.Z`)
- [ ] Verify the mirror landed (any machine with the ACR credential):
      `docker manifest inspect registry.cn-hangzhou.aliyuncs.com/cnyup/scheduler:vX.Y.Z`
      → returns a manifest JSON

## Post-release spot check (kind, ~10 min)

- [ ] `bash hack/e2e/build.sh` (rebuild :latest from the tag commit)
- [ ] `bash hack/e2e/bootstrap.sh` (idempotent; re-applies RBAC)
- [ ] `bash hack/e2e/matrix.sh` → 0 FAIL (cases 1-6, 9, 11-13; skip 7/8/10 are ledgered)
- [ ] `bash hack/e2e/chaos.sh` → 4/4 PASS
- [ ] `bash hack/e2e/keda.sh` → 2/2 PASS (needs KEDA; skips cleanly without)

## Known non-blockers (do NOT hold the release)

- VAP leg of acceptance #1 (cluster without ValidatingAdmissionPolicy):
  scheduler-side Filter still guards; RUNBOOK §4 documents the trade-off.
- `service_spread_node_pods` / `replica_target_detail` debug gauges:
  default-off by design (dev-design §6), tracked for a future release.
