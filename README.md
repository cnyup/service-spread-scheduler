# ServiceSpread Scheduler

Kubernetes out-of-tree 自定义调度器：按「namespace + 服务标签 + 调度域」聚合同一服务的全部 Pod，在稳定均摊节点池内强制执行 `maxSkew` 均摊与 `maxPodsPerNode` 跨域硬上限。不驱逐任何已运行 Pod；副本目标（HPA / KEDA）仅用于观测与容量告警，不参与放置决策。

[English version](README_EN.md)

## 这是什么，解决什么问题

**问题**：Kubernetes 原生调度器对「同一个服务的 Pod 要均匀摊开、且每个节点不能超配」没有直接表达。原生 `topologySpreadConstraints` 只在单个 workload 内生效——当同一服务拆成多个 Deployment（不同调度约束、滚动升级新旧共存、跨团队共池部署）时，各自独立计算均摊，叠加后照样倾斜甚至单节点超载。`podAntiAffinity` 是软约束的相似物，无法给出硬上限。业务侧常见的自研脚本巡检+手动迁移，滞后且有误伤风险。

**ServiceSpread 的做法**：接管显式声明 `schedulerName: service-spread-scheduler` 的 Pod，以**服务**（而非 workload）为粒度聚合计数：

| 能力 | 语义 |
| --- | --- |
| `maxPodsPerNode` | 同一服务（同 namespace + 服务标签）的 Pod 在**任何节点**上的数量硬上限——跨 Deployment、跨调度域合并计数 |
| `maxSkew` | 服务在「稳定均摊域」（Ready 且满足 selector/亲和性/toleration 的节点集合）内的最大倾斜度硬约束 |
| 不驱逐 | 只约束新放置；cordon、缩容、策略变更都不触发已运行 Pod 的迁移 |
| 独立调度域 | 不同 pod 约束（nodeSelector/亲和性）哈希出不同 `schedulingDomainHash`，各域独立均摊，但共享 `maxPodsPerNode` 配额 |
| 观测不干预 | HPA / KEDA ScaledObject / Deployment 的副本目标仅导出指标与容量告警，永不改变放置决策 |

安全边界（fail-closed 设计）：缺服务标签的受管 Pod 一律拒绝；策略缺失/歧义拒绝（可配）；webhook 宕机时策略创建失败而非放行；`ServiceSpreadArgs` 严格解码（未知字段报错）。

## 使用与部署

### 安装（推荐：Helm）

```bash
# 大陆 ACK 集群（镜像走 ACR）：预建 ns → install → secret → 滚动
kubectl create ns service-spread-system
helm install ssp charts/service-spread-scheduler -f charts/service-spread-scheduler/values-acr.yaml
kubectl -n service-spread-system create secret docker-registry acr-regcred \
  --docker-server=registry.cn-hangzhou.aliyuncs.com \
  --docker-username=<你的ACR用户名> --docker-password=<固定密码>
# ACR 私有仓库所有拉取都要认证：滚动系统组件 + 给 ns 默认 SA 挂凭证
kubectl -n service-spread-system rollout restart deploy/service-spread-scheduler deploy/service-spread-webhook
kubectl -n service-spread-system patch serviceaccount default \
  -p '{"imagePullSecrets":[{"name":"acr-regcred"}]}'

# 海外集群（默认 values，ghcr 镜像，无需 secret）
kubectl create ns service-spread-system
helm install ssp charts/service-spread-scheduler
```

说明：
- **预建 ns（kubectl）是实测的可靠路径**：chart 不渲染 Namespace 对象（曾与预建 ns 的 helm ownership 冲突）；`--create-namespace` 在带 OpenYurt 控制面的 ACK 上与 ns 管控存在兼容问题（ACK 2026-09-29 实测 11 资源全报 ns not found），故文档统一预建。
- **同 ns 的测试/业务负载拉 ACR 也需认证**：建好 secret 后补一条命令，把 ns 的 default ServiceAccount 挂上凭证（后续负载无需逐个配 imagePullSecrets）：
  ```bash
  kubectl -n service-spread-system patch serviceaccount default \
    -p '{"imagePullSecrets":[{"name":"acr-regcred"}]}'
  ```
- CRD 走 chart 的 `crds/` 目录（install 时自动装、**upgrade 不更新**——schema 变更时手动 `kubectl apply --server-side -f charts/service-spread-scheduler/crds/`）。
- 证书默认自签（零依赖），`webhook.certSource: certManager` 可切换。改参数示例：`helm upgrade ssp charts/... --set scheduler.args.exportNodePods=true`（kubeconfig 变更自动滚动 Pod）。

#### Helm values 配置项完整参考

完整默认值见 charts/service-spread-scheduler/values.yaml，大陆场景示例见 values-acr.yaml。常用项：

**global（全局）**

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `global.namespace` | `service-spread-system` | 全部组件的命名空间 |
| `global.schedulerName` | `service-spread-scheduler` | 接管的调度器名；改它意味着存量策略需迁移 |
| `global.serviceLabelKey` | `app.kubernetes.io/name` | 服务标签键；chart 同值注入插件 Args 与 webhook 共享 ConfigMap（双处同步由 chart 保证） |

**scheduler（调度器）**

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `scheduler.image.{registry,repository,tag,pullPolicy}` | ghcr / 对应仓库 / Chart.appVersion / IfNotPresent | 镜像；大陆集群改 ACR（见 values-acr） |
| `scheduler.replicas` | `2` | 副本数；多副本共享同一 leader Lease，只有 leader 决策 |
| `scheduler.resources` | 100m/128Mi ~ 500m/384Mi | 资源请求/限制 |
| `scheduler.imagePullSecrets` | `[]` | 拉取凭证，如 `[{name: acr-regcred}]`（ACR 必配） |
| `scheduler.args.requirePolicy` | `true` | 无策略命中的 Pod 拒绝调度（fail-closed） |
| `scheduler.args.fallbackCacheTTL` | `10m` | 副本目标 lastGood 缓存上限 |
| `scheduler.args.reservationTTL` | `5m` | Reserve 预占兜底过期（janitor 验证后释放） |
| `scheduler.args.reconcilePeriod` | `10m` | 计数快照重建周期 |
| `scheduler.args.exportNodePods` | `false` | 调试指标 `service_spread_node_pods`（高基数，慎开） |
| `scheduler.args.exportTargetDetail` | `false` | 调试指标 `service_spread_replica_target_detail` |

**webhook（策略准入）**

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `webhook.image.*` / `webhook.replicas` / `webhook.resources` / `webhook.imagePullSecrets` | 同上模式 | 与 scheduler 一致 |
| `webhook.certSource` | `selfSign` | 证书来源：`selfSign`（进程内自签，零依赖）/ `certManager`（需 cert-manager，chart 渲染 Issuer+Certificate）。**二选一不可同开** |

**可选组件开关**

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `vap.enabled` | `false` | VAP（阻止受管 Pod 预设 nodeName）；1.28 需 apiserver 双 feature gate，改不了 flags 的集群保持关闭 |
| `prometheusRule.enabled` | `false` | 4 条告警规则 |
| `prometheusRule.labels` | `{} ` | **必须匹配你 Prometheus 的 ruleSelector**（如 ACK 实测为 `prometheus: k8s`） |
| `prometheusRule.namespace` | `""`（= release ns） | ruleNamespaceSelector 为 nil 的 operator 只收自己 ns——放 `monitoring` |

**修改生效语义**：镜像/资源/副本 → `helm upgrade` 直接滚动；`scheduler.args.*` → checksum 注解自动滚动 Pod；`global.schedulerName`/`serviceLabelKey` → 涉及策略与工作负载匹配，变更需迁移评估。

<details><summary>裸清单安装（无 Helm）</summary>

### 前置要求

- Kubernetes 1.28.x（1.30+ 可用，VAP 清单需升 v1，见 [RUNBOOK §4](RUNBOOK.md)）
- 节点可达镜像仓库（大陆集群用 ACR 镜像，见下文「镜像分发」）
- cert-manager（生产证书路径）或手动自签（e2e 同款，见 RUNBOOK §2.5）

### 安装（顺序有依赖，详见 hack/deploy-checklist.md）

```bash
kubectl apply -f config/manager/namespace.yaml
kubectl apply -f config/crd/bases/
kubectl apply -f config/rbac/scheduler.yaml
kubectl apply -f config/rbac/webhook.yaml
kubectl apply -f config/manager/configmap.yaml

# 证书（cert-manager 路径）
kubectl apply -f config/webhook/certificates.yaml
kubectl -n service-spread-system wait --for=condition=Ready \
  certificate/service-spread-webhook-serving-cert --timeout=120s

# webhook + 调度器（镜像钉版本，见「镜像分发」）
kubectl apply -f config/webhook/service.yaml
kubectl apply -f config/manager/webhook.yaml
kubectl apply -f config/webhook/manifests.yaml
kubectl apply -f config/manager/scheduler.yaml

# VAP（可选：阻止受管 Pod 预设 nodeName 绕过；1.28 需开双 feature gate）
kubectl apply -f config/manager/vap-block-preset-nodename.yaml
```

</details>

### 使用：三步接入一个服务

```yaml
# 1. Deployment 声明接管（schedulerName）
apiVersion: apps/v1
kind: Deployment
metadata: {name: demo-web, namespace: default}
spec:
  replicas: 4
  template:
    metadata:
      labels: {app.kubernetes.io/name: demo-web}   # 2. 服务标签
    spec:
      schedulerName: service-spread-scheduler      # 1. 接管声明
      containers: [...]
---
# 3. 策略（名字必须是确定性名 ssp-<hash>，直接 apply 会被 webhook 拒绝并提示正确名字）
apiVersion: scheduling.soyup.top/v1alpha1
kind: ServiceSpreadPolicy
metadata:
  name: ssp-aa7b27a1fa82        # webhook 提示的确定性名
  namespace: default
spec:
  schedulerName: service-spread-scheduler
  serviceSelector:
    matchLabels: {app.kubernetes.io/name: demo-web}
  maxSkew: 1
  maxPodsPerNode: 2
```

提示：先故意用错误名字 apply 一次，webhook 的报错会给出应用的确定性名（`ssp-` + JSON 规范化编码的 SHA-256 前 12 hex）。

### 配置项

**ServiceSpreadPolicy（CRD，每服务一条）**——完整字段见 [api/v1alpha1](api/v1alpha1/types.go)：

| 字段 | 含义 |
| --- | --- |
| `schedulerName` | 接管的调度器名，须与 scheduler profile 一致 |
| `serviceSelector` | 单键 selector，选中即纳入该服务的聚合计数 |
| `maxSkew` | 稳定均摊域内最大倾斜（硬约束） |
| `maxPodsPerNode` | 每节点硬上限（跨域共享计数） |

**ServiceSpreadArgs（scheduler 插件参数，config/scheduler/kubeconfig.yaml）**：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `serviceLabelKey` | 必填 | 服务标签键；受管 Pod 缺此标签一律拒绝 |
| `requirePolicy` | `true` | 无策略命中的 Pod 拒绝调度（fail-closed） |
| `managedSchedulerName` | `service-spread-scheduler` | profile 调度器名 |
| `fallbackCacheTTL` | `10m` | 副本目标 lastGood 缓存上限（超时标记 stale 告警） |
| `reservationTTL` | `5m` | Reserve 预占的兜底过期（janitor 逐条验证后释放，绝不盲删） |
| `reconcilePeriod` | `10m` | 计数快照重建周期（只修漂移，不碰预占） |
| `exportNodePods` | `false` | 调试指标 `service_spread_node_pods`（高基数，慎开） |
| `exportTargetDetail` | `false` | 调试指标 `service_spread_replica_target_detail` |

**共享配置**（config/manager/configmap.yaml）：`serviceLabelKey` 与 `managedSchedulerName`——webhook 与调度器共同读取；`serviceLabelKey` 必须与 Args 保持一致。

### 镜像分发

- 海外/可达 ghcr.io 的集群：`ghcr.io/cnyup/service-spread-scheduler/{scheduler,webhook}:vX.Y.Z`
- **大陆集群（ACK 实测）**：ghcr 直拉会无限挂起；release workflow 自动双推阿里云 ACR 个人版 `registry.cn-hangzhou.aliyuncs.com/cnyup/{scheduler,webhook}`。**个人版「公有」仓库≠匿名可拉，必须配 imagePullSecret**——完整大陆部署路径见 [RUNBOOK §3](RUNBOOK.md)

### 监控

调度器在 `:9100` 暴露观测指标（`service_spread_replica_target` / `observed_pods` / `bound_pods` / `reservations` / `fallback_active` / `capacity_deficit`，插件侧 `filter_rejections_total{reason}` / `policy_resolution_failures_total{reason}`）。告警规则（4 条）在 config/monitoring/prometheusrule.yaml——**标签必须匹配你 Prometheus 的 ruleSelector，且规则须放在 operator 监听的 namespace**（ACK 实测：`prometheus: k8s` 标签 + monitoring ns → 4/4 加载）。健康判定看 leader Lease 的 `renewTime`，不是 `/healthz`（后者在调度卡死时仍存活）。详见 [RUNBOOK §5](RUNBOOK.md)。

### 运维

升级（RBAC 必须每次 re-apply——三次同族事故的教训）、回滚、故障排查、已知问题（KEDA 后装需重启、gauge 序列基数、RBAC 静默卡死、证书续期）见 RUNBOOK.md。发版流程见 hack/release-checklist.md。

## 开发进度与技术栈

### 里程碑

| 里程碑 | 范围 | 状态 |
| --- | --- | --- |
| M1 | CRD / 配置 API / 策略 webhook（确定性命名+校验+重叠拒绝）/ CEL VAP 清单 | ✅ |
| M2 | 配额/调度键、调度域哈希、COW 计数 + 预占状态机 | ✅ |
| M3 | 调度插件全扩展点、EnqueueExtensions、集群事件 hints | ✅（kind e2e 矩阵 + 混沌 4/4） |
| M4 | 副本目标观测器、容量告警、TTL janitor / reconciler 装配、生产清单 | ✅（告警三层验证 + ACK 真集群三项验证） |

验收状态：设计文档验收标准 11/11 有证据（kind e2e + 混沌 + 单测），偏差表 14/14 落地。发版管线（ghcr 多架构 + ACR 大陆镜像）已在真实 tag 上全绿验证。

### 验证资产

| 套件 | 内容 |
| --- | --- |
| `go test ./... -race` | 单元 + envtest 集成（webhook 准入矩阵、状态机并发原子性 16 goroutine） |
| `hack/e2e/matrix.sh` | kind 验收矩阵（12 用例：接管/拒绝/聚合计数/跨域/并发上限/滚动升级/扩容回填…） |
| `hack/e2e/chaos.sh` | 故障演练（调度器重启 / informer 冷启动 / KEDA absent / webhook 宕机） |
| `hack/e2e/keda.sh` | 观测器 KEDA present 路径（自动检测后装 KEDA 并重启调度器） |
| `internal/spread/rbac_expected_test.go` | RBAC 契约测试（代码访问面 vs 清单，防止权限漂移） |

### 技术栈

- Go + Kubernetes scheduler framework（out-of-tree 插件，非 fork、非 extender）
- controller-runtime v0.16.6 / client-go informer（版本锚定 k8s 1.28.15）
- controller-gen（CRD 生成）、envtest（kube-apiserver 真进程集成测试）
- Prometheus client_golang（观测指标）、keda.sh CRD（ScaledObject 动态 informer）
- kind（e2e 真集群）、GitHub Actions（CI + 多架构发版 + ACR 大陆镜像双推）

### 仓库结构

```
api/v1alpha1/            ServiceSpreadPolicy CRD
apis/config/v1alpha1/    ServiceSpreadArgs（严格解码）
cmd/scheduler|webhook/   入口
internal/spread/         核心：keys / state（COW+预占）/ domain / plugin / lifecycle / replicatarget
internal/webhook/        策略准入（确定性命名/校验/重叠拒绝/共享 ConfigMap fail-closed）
config/                  部署清单（manager/rbac/webhook/monitoring）
hack/e2e/                kind e2e 套件（bootstrap/matrix/chaos/keda/verify-observer）
docs（根目录上级）        设计文档：方案 + 开发设计（含 §9 偏差表 14 条）
```

设计与开发文档（术语、规则边界、算法伪代码、测试矩阵）在仓库根的上级目录：`service-spread-scheduler-design.md`（方案）与 `service-spread-scheduler-dev-design.md`（开发设计）。
