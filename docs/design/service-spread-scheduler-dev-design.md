# Service Spread Scheduler 开发设计（实现设计）

上游需求与架构方案见 [service-spread-scheduler-design.md](service-spread-scheduler-design.md)（下称“方案文档”）。本文档将其落为可执行的工程实现设计：仓库结构、API 与类型、核心数据结构、关键算法、再入队、准入、部署清单与测试矩阵。

## 0. 版本锁定与假设

| 项 | 决策 | 说明 |
| --- | --- | --- |
| 目标 Kubernetes | **1.28.x**（已确认，M1 实施锚定 v1.28.15） | 方案文档 12.3 要求锁定小版本。本文 Framework 接口签名以 1.28 为准（与 1.31 的差异集中在插件工厂是否带 `ctx`、`EnqueueExtensions` 是否有 `ClusterEventWithHint`，触点已在 §0/§5.1 隔离）。 |
| Go | 1.22+ | k8s 1.28 要求 ≥1.20；仓库 go 指令为 1.22。 |
| 插件接入 | out-of-tree 插件 + `cmd/kube-scheduler/app` `WithPlugin` 编译自定义 scheduler 二进制 | kubernetes-sigs/scheduler-plugins 的标准模式（`app.WithPlugin` 已在 1.28 源码确认存在）。 |
| CRD / Webhook | kubebuilder（controller-runtime v0.16.6）与 scheduler 同仓 | controller-runtime v0.16 与 k8s 1.28 配对。 |
| go.mod | `require k8s.io/kubernetes v1.28.15` + 全量 staging `replace` 到 `v0.28.15`（含 `dynamic-resource-allocation`/`endpointslice`/`kms`/`gengo`） | 见 §2.3。 |
| 升级到 1.29+ | `PreFilter`/插件工厂签名、`ClusterEventWithHint` 等触点集中在 `internal/spread/plugins_api.go` 与 `main.go` | 迁移时只改这两个文件加 go.mod。 |

## 1. 交付物与里程碑

方案文档 Phase 1–3 映射为可合并的 PR 序列：

| 里程碑 | 内容 | 对应方案文档 |
| --- | --- | --- |
| M1：API 骨架 | 仓库脚手架、`ServiceSpreadPolicy` CRD、`ServiceSpreadArgs` 配置 API、webhook（确定性命名 + 校验）、CEL `nodeName` 策略清单 | Phase 1、4.3、3.2 |
| M2：状态与键 | 服务配额键 / 服务调度键、`schedulingDomainHash`、COW 计数器与 reservation 状态机（含单测） | Phase 1、5、7.1 |
| M3：调度插件 | `PreFilter`/`Filter`/`Score`/`Reserve`/`Unreserve`/`PostBind`、EnqueueExtensions、集成测试 | Phase 2、8 |
| M4：观测与生产化 | 副本目标观测器 + 容量校验与告警（方案文档 10.3、12.2）、metrics、TTL janitor、reconcile、leader election、RBAC/Deployment 清单、e2e | Phase 3、9、10、11 |

M1+M2 不改变调度行为（无插件注册），可独立合入。M3 起在测试集群灰度。

## 2. 仓库结构与技术选型

### 2.1 目录

```text
service-spread-scheduler/
├── api/
│   └── v1alpha1/
│       ├── types.go              # ServiceSpreadPolicy
│       ├── zz_generated.deepcopy.go
│       └── groupversion_info.go
├── apis/
│   └── config/v1alpha1/
│       ├── types.go              # ServiceSpreadArgs（pluginConfig.args）
│       └── zz_generated.deepcopy.go
├── cmd/
│   ├── scheduler/main.go         # 自定义 kube-scheduler 入口
│   └── webhook/main.go           # 准入 webhook（观测器与容量告警在 scheduler 进程内，见 §6、§9 偏差 4）
├── internal/
│   ├── spread/
│   │   ├── plugin.go             # ServiceSpread 插件（全部扩展点入口）
│   │   ├── plugins_api.go        # Framework 接口签名隔离层（版本迁移点）
│   │   ├── keys.go               # 服务配额键 / 服务调度键 / schedulingDomainHash
│   │   ├── domain.go             # 稳定均摊域节点匹配
│   │   ├── state.go              # COW 计数快照 + reservation 状态机
│   │   ├── lifecycle.go          # Pod 事件驱动的预占释放 + TTL janitor + reconcile
│   │   ├── enqueue.go            # EnqueueExtensions + QueueingHint
│   │   └── replicatarget.go      # HPA/KEDA/Deployment 副本目标观测（仅指标）
│   └── webhook/
│       ├── policy_webhook.go     # ServiceSpreadPolicy 校验 + 确定性命名
│       └── wire.go
├── config/
│   ├── crd/bases/
│   ├── rbac/
│   ├── manager/                  # scheduler Deployment、webhook Deployment、VAP 清单
│   └── scheduler/kubeconfig.yaml # KubeSchedulerConfiguration 示例
└── test/
    ├── unit/                     # （或各包内 _test.go）
    ├── integration/              # scheduler framework testing.TestFramework
    └── e2e/                      # kind + KEDA，验收标准矩阵
```

### 2.2 cmd/scheduler/main.go（骨架）

```go
func main() {
    command := app.NewSchedulerCommand(
        app.WithPlugin(spread.Name, spread.New), // "ServiceSpread"
    )
    code := cli.Run(command)
    os.Exit(code)
}
```

`app.WithPlugin` 即 `multiPoint.enable` 的注册来源；profile 排序由 `KubeSchedulerConfiguration` 控制，插件在每个扩展点自动排在原生插件之后（方案文档 3.1）。

### 2.3 go.mod（版本锚定）

```text
require k8s.io/kubernetes v1.28.15
replace (
    k8s.io/api             => k8s.io/api v0.28.15
    k8s.io/apimachinery    => k8s.io/apimachinery v0.28.15
    k8s.io/client-go       => k8s.io/client-go v0.28.15
    k8s.io/component-base  => k8s.io/component-base v0.28.15
    k8s.io/code-generator  => k8s.io/code-generator v0.28.15
    // 其余 staging 模块同法替换（scheduler-plugins 模式）；
    // 1.28 额外必须显式 replace dynamic-resource-allocation、endpointslice、
    // kms、gengo（k8s.io/kubernetes 自身 go.mod 里它们是 v0.0.0 占位）
)
require sigs.k8s.io/controller-runtime v0.16.6
```

CI 增加一步 `go mod verify` + 与 1.28 镜像的兼容性构建。

## 3. API 定义

### 3.1 ServiceSpreadPolicy（api/v1alpha1）

```go
// +genclient
// +kubebuilder:resource:scope=Namespaced,shortName=ssp
// +kubebuilder:object:root=true
type ServiceSpreadPolicy struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec ServiceSpreadPolicySpec `json:"spec"`
}

type ServiceSpreadPolicySpec struct {
    // 必须与 Pod.spec.schedulerName 完全一致。minLength=1。
    SchedulerName string `json:"schedulerName"`
    // 只允许且必须包含 serviceLabelKey 一个键（webhook 强校验）。
    ServiceSelector metav1.LabelSelector `json:"serviceSelector"`
    // >=1。硬校验。
    MaxSkew int32 `json:"maxSkew"`
    // >=1。硬校验。统计维度为服务配额键（跨调度域共享）。
    MaxPodsPerNode int32 `json:"maxPodsPerNode"`
}
```

CRD schema 通过 `controller-gen` 生成；`MaxSkew >= 1`、`MaxPodsPerNode >= 1`、selector 单键约束写 CEL/结构性 schema 之外的部分由 webhook 兜底（selector 的“只含 serviceLabelKey”依赖集群级 `serviceLabelKey` 配置，无法静态表达，见 §7.1）。

### 3.2 ServiceSpreadArgs（apis/config/v1alpha1）

```go
// 注册进 scheme 供 framework.DecodeInto 使用
type ServiceSpreadArgs struct {
    metav1.TypeMeta `json:",inline"`
    ServiceLabelKey      string          `json:"serviceLabelKey"`                // 必填
    RequirePolicy        *bool           `json:"requirePolicy,omitempty"`        // 默认 true
    ManagedSchedulerName string          `json:"managedSchedulerName"`          // 默认 "service-spread-scheduler"
    FallbackCacheTTL     metav1.Duration `json:"fallbackCacheTTL,omitempty"`    // 默认 10m，仅影响观测指标 stale 标记
    ReservationTTL       metav1.Duration `json:"reservationTTL,omitempty"`      // 默认 5m，预占兜底清理
    ReconcilePeriod      metav1.Duration `json:"reconcilePeriod,omitempty"`     // 默认 10m，计数重建
}
```

`New` 工厂内 `framework.DecodeInto(configuration, &args)`，未知字段报错（fail-closed）。

## 4. 核心状态与数据结构（internal/spread/state.go）

### 4.1 设计原则

- 热路径（Filter/Score）**无锁读**：真实计数维护在不可变快照中，`atomic.Pointer` 交换；Pod 事件处理器写时复制（COW）生成新快照。
- 预占（reservation）与真实计数**同层叠加**：有效计数 = 快照计数 + 预占叠加。`Reserve` 在互斥锁内完成“读最新快照 → 复核 → 写入预占”，是唯一的安全边界。
- 观察到绑定后释放预占时**短暂双重计数**（informer 已计数、预占未删）：宁可高估一个名额也不低估（方案文档 8.1 PostBind 语义）。
- 计数纳入标准（方案文档 7.1）：`phase ∈ {Pending, Running}` 且 `spec.nodeName` 非空的 Pod 才进入 per-node 计数快照；未绑定的 Pending Pod 不影响节点计数，仅在观测器 `observed_pods` 指标中统计。

### 4.2 数据结构

```go
type podRef struct {
    QuotaKey string // 服务配额键
    SchedKey string // 服务调度键（含 schedulingDomainHash）
    NodeName string
}

// 不可变快照：由 Pod informer 事件 COW 维护
type snapshot struct {
    pods       map[types.UID]podRef
    nodeQuota  map[string]map[string]int32 // node -> quotaKey -> n
    nodeDomain map[string]map[string]int32 // node -> schedKey  -> n
}

type resvPhase int
const (
    resvReserved resvPhase = iota // Reserve 成功，未 Bind
    resvBoundObs                  // PostBind 后，等待 informer 观察到绑定
)

type reservation struct {
    UID      types.UID
    QuotaKey string
    SchedKey string
    NodeName string
    Phase    resvPhase
    Since    time.Time // TTL janitor 依据
}

type spreadState struct {
    mu   sync.Mutex                  // 仅保护 resv 与“复核+写入”临界区
    snap atomic.Pointer[snapshot]    // 无锁读
    resv map[types.UID]*reservation  // 受 mu 保护
}

// 有效计数（读路径，含预占叠加）
func (s *spreadState) effCount(node, schedKey, quotaKey string) (domainN, quotaN int32)
```

键规则（keys.go）：

```text
quotaKey = ns + "/" + schedulerName + "/" + serviceLabelKey + "=" + serviceLabelValue
schedKey = quotaKey + "/" + hex(schedulingDomainHash)[:12]
```

### 4.3 schedulingDomainHash（keys.go）

规范化结构体（全部字段排序后序列化，SHA-256 取前 6 字节即 12 个 hex 字符，与 §4.2 schedKey 一致）：

```go
type domainSpec struct {
    NodeSelector     map[string]string          // key 排序；nil -> {}（区别于空 map 显式给出）
    RequiredAffinity []normNodeSelectorTerm     // RequiredNodeAffinity 规范化
    Tolerations      []normToleration           // 排序键：序列化字节串
    AddedAffinity    *normNodeSelector          // v1 恒为 nil：profile 显式禁用 addedAffinity（§5.4、§9 偏差 8），字段保留为扩展点
}
```

- `RequiredAffinity` 复用 `nodeaffinity.GetRequiredNodeAffinity(pod)` 取值后做 term 级规范化（matchExpressions 按 key/operator/values 排序）。
- 只覆盖影响候选节点集合的字段（方案文档 5.2）。
- 单测必须包含：map 顺序无关、toleration 列表顺序无关、空 vs nil 一致性、preferred affinity 不影响哈希。

### 4.4 reservation 生命周期（lifecycle.go）

```text
Reserved ──Bind 成功(PostBind)──▶ BoundObs ──informer 观察到该 UID 绑定到预期节点──▶ 释放
Reserved ──Bind 失败──▶ Unreserve ──▶ 释放
Reserved/BoundObs ──Pod 删除(Delete 事件或 TTL 检查发现 UID 消失)──▶ 释放
BoundObs ──TTL 到期，lister 验证 Pod 未绑定到预期节点──▶ 释放
BoundObs ──TTL 到期，Pod 已绑定预期节点但快照未观察到──▶ 先强制 reconcile，确认真实计数已含该 Pod 后再释放
验证无法完成（Pod informer 本身异常）──▶ 保留预占 + service_spread_reservation_stuck 告警
```

释放动作在 `mu` 内执行；真实计数部分由 Pod Add/Update 处理器维护，两者幂等可乱序：

- 绑定事件先于 PostBind 到达：处理器看到 `resvReserved` 且节点匹配 → 直接释放。
- PostBind 先执行：仅做相位转移，不删预占。

后台两个协程：

- **TTL janitor**（`ReservationTTL`，默认 5m）：逐条**验证后处置，绝不盲删**（方案文档 8.3）。若快照已含该绑定，幂等释放路径早已触发；TTL 分支只在快照缺失（低估窗口）时才有意义，此时直接释放会突破上限。Pod 不存在或未绑定预期节点 → 释放；已绑定但快照未含 → 强制 reconcile 重建快照后释放；验证不可完成 → 保留并告警。
- **Reconciler**（`ReconcilePeriod`，默认 10m）：从 Pod lister 全量重建 `snapshot` 后原子替换，仅修复计数漂移；**不触碰、不按年龄丢弃预占**——预占的移除永远走上述验证路径。

## 5. 调度插件（internal/spread/plugin.go）

### 5.1 插件结构

```go
type ServiceSpread struct {
    args     config.ServiceSpreadArgs
    handle   framework.Handle
    st       *spreadState
    policies listerssv1a.ServiceSpreadPolicyLister   // namespaced lister
    rss      listersappsv1.ReplicaSetLister
    nodes    corev1listers.NodeLister
    observed *replicaTargetObserver                  // §6（HPA/Deployment/ScaledObject lister 下沉到 observer）
}

var _ framework.PreFilterPlugin = (*ServiceSpread)(nil)
var _ framework.FilterPlugin      = (*ServiceSpread)(nil)
var _ framework.PreScorePlugin    = (*ServiceSpread)(nil)  // 分数归一化（§5.5）
var _ framework.ScorePlugin       = (*ServiceSpread)(nil)
var _ framework.ReservePlugin     = (*ServiceSpread)(nil)
var _ framework.PostBindPlugin    = (*ServiceSpread)(nil)  // 相位转移 Reserved→BoundObs（§5.6）
var _ framework.EnqueueExtensions = (*ServiceSpread)(nil)
```

Framework 签名集中在 `plugins_api.go` 以 1.31 为准（`PreFilter(ctx, state, p) (*PreFilterResult, *Status)` 等），版本升级仅改此文件。

### 5.2 PreFilter

```text
0. 放置关键 informer（Pod / Node / ServiceSpreadPolicy）任一 HasSynced false →
   Unschedulable "CacheNotSynced"（普通 Unschedulable，可随同步完成重试；
   不能用 AndUnresolvable——缓存同步是瞬时条件，且后续步骤都依赖 cache 查询，
   必须前置到第 0 步，否则启动窗口内会对正常 Pod 误报 PolicyNotFound。
   readiness 探针只挡流量，不阻止 leader 当选后的调度循环，不能替代此检查）
   门范围仅限这三类（方案文档 12.1）：HPA/KEDA informer 未同步不阻断调度
   （副本目标只影响指标，走 stale）；RS/Deployment 未同步时 owner 解析失败
   落入第 3 步的可重试拒绝，由同步后的事件重排恢复。
1. pod.spec.schedulerName != args.ManagedSchedulerName：
   不可能发生（profile 隔离），防御性返回 Success（framework 只路由本 profile 的 Pod）。
2. 取服务标签 pod.Labels[args.ServiceLabelKey]：
   缺失 → UnschedulableAndUnresolvable "MissingRequiredServiceLabel"（含 labelKey）。
   无论 requireServiceLabel 开关如何都拒绝：quotaKey 的构造依赖 labelValue，
   缺失时无法继续。requireServiceLabel 配置项因此删除（见 §3.2）。
3. 解析 owner 链 Pod→ReplicaSet→Deployment（informer cache）：
   失败 → UnschedulableAndUnresolvable "UnsupportedWorkloadOwner"
   （依赖稍后补齐时由 Node/Pod/RS/Deploy 事件重排，见 §5.7）
4. 计算 quotaKey/schedKey；查同 namespace、`spec.schedulerName == args.ManagedSchedulerName`
   且标签命中 `serviceSelector` 的策略（方案文档 4.2 的完全匹配要求：webhook 缺位
   期间其他 scheduler 的同名标签策略须被忽略而非误判 Ambiguous）：
   - 0 条且 requirePolicy=true → UnschedulableAndUnresolvable "ServiceSpreadPolicyNotFound"
   - >1 条 → UnschedulableAndUnresolvable "AmbiguousServiceSpreadPolicy"
5. 计算稳定均摊域 domainSet（§5.4，node lister 全扫）：
   - domainSet 为空 → Unschedulable "EmptySpreadDomain"（可重试：Node 事件重排）
6. 写 CycleState：
   preState{quotaKey, schedKey, policy, domainSet,
            minCount=effDomainMin(domainSet, schedKey),
            effDomain=map[node]effCount, effQuota=map[node]effQuota}
7. 插件侧计数埋点：service_spread_filter_rejections_total{reason} /
   service_spread_policy_resolution_failures_total{reason}（方案文档 10.2/10.3
   告警依赖，来自插件路径而非观测器）。reason 采用方案文档 10.1 规范名
   （MissingRequiredServiceLabel / ServiceSpreadPolicyNotFound / AmbiguousServiceSpreadPolicy /
   UnsupportedWorkloadOwner / ServiceSpreadConstraint）；§5.3 的细分约束名只出现在
   Filter message，不进 label
```

`minCount` 在快照 + 预占之上计算；`PreFilterResult` 返回 nil（不跳过节点）。

### 5.3 Filter

```text
输入 node；读 CycleState preState
1. node.Name ∉ domainSet →
   Unschedulable "NodeOutsideSpreadDomain"（NotReady+临时 toleration 场景，方案文档 7.2）
2. quota := preState.effQuota[node]+1 > policy.MaxPodsPerNode →
   Unschedulable "MaxPodsPerNodeExceeded: node=x quota=n/i limit=L"
3. skew := preState.effDomain[node]+1 - preState.minCount > policy.MaxSkew →
   Unschedulable "MaxSkewExceeded: node=x count=n min=m skew=k maxSkew=K"
Success
```

message 必须携带数值与约束名（方案文档 10.1，供 `FailedScheduling` Event 直接诊断）。

### 5.4 稳定均摊域（domain.go）

节点须同时满足：

- `nodeutils.IsNodeReady(node)` 且 `!node.Spec.Unschedulable`；
- `nodeSelector` 全匹配（`nodeaffinity` / `helpers.MatchNodeSelectorTerms` 系 helper）；
- required node affinity 匹配（含 profile `addedAffinity`，若配置）；
- 全部 taints 被 pod tolerations 覆盖（`taint.TolerationsTolerateTaint`，只按 NoSchedule/NoExecute）。

**不含**资源、卷、镜像等瞬时条件。v1 采用方案文档 5.2 允许的“显式禁止”分支：本 profile 的 NodeAffinity 插件**不得配置 `addedAffinity`**——out-of-tree 插件读取不到其他插件的 pluginConfig，无法将其纳入哈希与域计算。由部署清单模板（不含该字段）+ CI 校验 kubeconfig 内容保证，违反即部署失败；`domainSpec.AddedAffinity` 字段保留为将来支持镜像该配置时的扩展点（§9 偏差 8）。

### 5.5 PreScore + Score

```text
PreScore(feasibleNodes)：
  在可行集上计算 cmin/cmax = min/max(effDomain[n])
  写入 CycleState（scoreState{cmin, cmax}）
  cmin==cmax（计数全相等或单节点）→ 置 skip 标记，Score 全部返回同分

score(node) = skip ? 0 :
  (cmax - effDomain[node]) * MaxNodeScore / (cmax - cmin + 1)
```

计数越少分越高；**分数归一化到可行集内的计数极差**：最小计数节点接近满分、最大计数节点接近 0 分。这是恢复 `PreScore` 的原因——逐节点 Score 看不到其他节点，无法自行归一化；若直接用 `MaxNodeScore - count`，节点间原始分差仅 1–2 分，会被 `NodeResourcesFit`（权重 1、常见分差 10–50）稳定压过，均摊倾向名存实亡。归一化后即使权重 1–2，最小计数节点也具有压倒性优势。计数相同则同分，交给其余原生 Score 插件区分。不实现 NormalizeScore。profile 中可为 `ServiceSpread` 配 score weight（建议 1–2，归一化后已足够）。

### 5.6 Reserve / Unreserve / PostBind

```text
Reserve(node)：
  mu.Lock()
  snap := st.load(); 重算 effCount(node)（快照+全部 resv）
  复核三条：node∈domain（用最新 node lister 重算域，节点状态可能已变）、
            quota+1<=L、domain+1-min'<=K（min' 含最新预占后重算）
  失败 → Unlock, return Unschedulable("ReserveRevalidationFailed: ...")
  成功 → resv[uid] = {.., Reserved, now}; Unlock; return Success

Unreserve(node)：
  mu.Lock(); delete(resv, uid); Unlock()

PostBind(node)：
  mu.Lock(); if r:=resv[uid]; r!=nil && r.Phase==Reserved { r.Phase=BoundObs }; Unlock()
```

预占读取（`effCount`）以 `mu` 临界区内为准，写路径安全；读路径（Filter/Score）接受轻度过期视图，安全边界在 Reserve 复核（方案文档 8.3）。

### 5.7 再入队（enqueue.go）

实现 `EnqueueExtensions.EventsToRegister()`，返回（1.31 `ClusterEventWithHint`）：

| 资源 | 动作 | QueueingHint |
| --- | --- | --- |
| Pod | Add/Update/**Delete** | pod 绑定/删除改变计数 → 仅同 schedKey 或同 quotaKey 的 unschedulable pod Queue，否则 QueueSkip。**Delete 必须注册**：HPA 缩容/驱逐/正常退出释放配额后，同服务 Pending Pod 依赖该事件重排（验收 11）；漏注册会退化为 unschedulable 队列约 30s–1m 的周期 flush |
| Node | Add/Update/Delete | 节点 Ready/标签/taint 变化影响 domainSet → 无法廉价判定归属，返回 Queue（保守） |
| ServiceSpreadPolicy（自定义 GVK） | Add/Update/Delete | namespace 相同且标签命中 → Queue，否则 QueueSkip |
| Deployment / ReplicaSet（自定义 GVK） | Add/Update/Delete | owner 链补齐（Add/Update）与 owner 消失清理（Delete，方案文档 8.3 全动作注册）：同 namespace 的 pending pod Queue |

（HPA / ScaledObject 不注册：副本目标不驱动放置决策，注册后永远 QueueSkip 等于没注册；观测器自行消费其事件。）

自定义资源用 `framework.ClusterEvent{Resource: framework.Resource("servicespreadpolicies.scheduling.example.io"), ...}` 形式注册。hint 签名 `func(pod, oldObj, newObj) QueueingHintFnStatus`，异常时返回 `Queue`（fail-open 到重试而非死等）。

## 6. 副本目标观测器（replicatarget.go，仅指标）

独立 goroutine，事件驱动 + 周期导出，不做任何调度参与：

```text
resolveDeploymentTarget(dep)：
  so := ScaledObjectLister 按 scaleTargetRef==dep 匹配（≥1 视为 KEDA 管理）
  hpa := HPALister 按 scaleTargetRef==dep 匹配
  hpa 有效 desiredReplicas → (值, source=hpa)
  否则有 so → max(so.minReplicaCount 或 0, dep.spec.replicas) → (值, source=kedaFallback)
  否则 → dep.spec.replicas → (值, source=deployment)
  多 HPA 命中 → 取最大值, reason=ReplicaTargetFallback
  读取异常 → lastGood[dep]+fallback_active，TTL 后 stale
  （kedaFallback 取 max(minReplicaCount, replicas)：HPA 未就绪窗口内实际副本
  可能已高于 min，取更接近现状的观测值；与方案文档 6.2 第 2 条字面不同，见 §9 偏差 6）

域级汇总（方案文档 6.3 保护下限）：
  domainDesiredReplicas = sum(域内各 Deployment 目标)
  observedPods          = 域内 Pending+Running Pod 数（含未绑定）
  reportedReplicaTarget = max(domainDesiredReplicas, observedPods)

指标（观测器侧，label 名与方案文档 10.2 统一为 namespace）：
  service_spread_replica_target{namespace,service,domain}          # reportedReplicaTarget（保护下限后的域级值）
  service_spread_replica_target_detail{namespace,service,domain,deployment,source}  # 调试开关，默认关闭
  service_spread_bound_pods{namespace,service,domain}        # 已绑定节点计数总和（effCount 域内求和）
  service_spread_observed_pods{namespace,service,domain}     # Pending+Running（含未绑定），保护下限输入
  service_spread_node_pods{namespace,service,domain,node}    # 默认关闭（高基数），--暴露开关
  service_spread_reservations{namespace,service,domain}
  service_spread_fallback_active{namespace,service,domain}   # reason=ReplicaTargetFallback
  service_spread_capacity_deficit{namespace,service}         # maxReplicas − 可用节点并集×maxPodsPerNode
指标（插件侧，PreFilter/Filter 埋点，见 §5.2/5.3，reason 规范名见方案文档 10.1）：
  service_spread_filter_rejections_total{reason}
  service_spread_policy_resolution_failures_total{reason}
```

容量校验与告警（方案文档 10.3、12.2，随观测器周期执行，数据同源）：

- `capacity_deficit > 0`：`HPA.spec.maxReplicas > serviceEligibleNodeCount × maxPodsPerNode`，节点并集按各 Deployment 稳定约束求出。
- 存在 Pending Pod 且全部节点被 `maxPodsPerNode` 拒绝（由 `filter_rejections_total{reason=ServiceSpreadConstraint}` 驱动告警规则）。
- 域内节点计数差已超 `maxSkew` 且源于历史遗留或节点状态变化：仅告警，不驱逐。
- 策略/HPA/KEDA/informer 访问连续失败，或 fallback 缓存超 `FallbackCacheTTL` 仍 stale。

KEDA CRD 存在性用 discovery 判定：`keda.sh` group 未注册 → “明确不存在”走 deployment 回退；已注册但 list/watch 失败 → degraded（健康检查 + 指标），不静默当不存在。

## 7. 准入与部署

### 7.1 ServiceSpreadPolicy webhook（cmd/webhook）

`VALIDATING`，处理 CREATE/UPDATE：

**配置来源**：webhook 需要集群级 `serviceLabelKey` 做 selector 校验，与 scheduler pluginConfig 必须一致（不一致时策略会通过 webhook 却被插件拒绝）。落地方式：共享 ConfigMap `service-spread-scheduler-config`（scheduler 与 webhook Deployment 都挂载），webhook 启动时加载并 watch 变更；部署清单用 kustomize 变量保证两处引用同一份。

1. **确定性命名**：期望名 = `"ssp-" + sha256(ns|schedulerName|labelKey|labelValue)[:12]`。`metadata.name` 不匹配 → 拒绝并给出期望名（12 hex ≈ 48 bit，namespace 内冲突概率可忽略；方案文档 4.3 的 API 对象名唯一性兜底）。
2. `serviceSelector.MatchLabels` 有且仅有 `args.ServiceLabelKey` 一个键，且无 `MatchExpressions`。
3. `maxSkew >= 1`、`maxPodsPerNode >= 1`、`schedulerName` 非空且等于共享 ConfigMap 中的受管 schedulerName（方案文档 4.2 完全匹配）。
4. 与既有策略服务重叠检测仅做提示性校验（真正唯一性靠命名规则）。

### 7.2 nodeName 绕过防护（CEL，无需自建服务）

`ValidatingAdmissionPolicy`（清单随附）：

```cel
match: (CREATE || UPDATE) && input.spec.schedulerName == "service-spread-scheduler"
validation: has(input.spec.nodeName) ?
  "presetting spec.nodeName bypasses service-spread-scheduler" : true
exception: requestor ServiceAccount ∈ 允许列表（由 binding 配置）
```

必须覆盖 UPDATE：`spec.nodeName` 可以在 Pod 创建后通过 PATCH 写入（等效自我绑定、绕过全部约束），CREATE-only 会留下该通道。Update 匹配时需容忍系统组件对已调度 Pod 的常规更新（`nodeName` 已存在且不变的 UPDATE 会再次被拒，属可接受的提示噪音；若需精细区分可用 `oldObject` 比较 nodeName 变更）。

### 7.3 RBAC / 部署清单（config/）

- **scheduler Deployment**：自定义镜像；`--config=/etc/kube-scheduler/config.yaml`（KubeSchedulerConfiguration 示例含 `multiPoint.enable: ServiceSpread`、`postFilter.disabled: DefaultPreemption`）；`--leader-elect` + 固定 `--leader-elect-resource-namespace/-name`（全部副本共用，方案文档 3.1）；副本数 2；resources/liveness（`/healthz`、`/readyz`，readiness 关联 informer HasSynced）。
- **webhook Deployment**：controller-runtime manager，cert-manager 注入，`ValidatingWebhookConfiguration` + `ValidatingAdmissionPolicy(Binding)`。
- **RBAC**：Pod/Node/Deployment/ReplicaSet/HPA/ServiceSpreadPolicy 的 `get,list,watch`；`keda.sh ScaledObjects` 的 `get,list,watch`（dynamic client）；LeaseLock Lease 的 `get,list,watch,create,update,patch`；标准 kube-scheduler 事件/绑定权限（system:kube-scheduler 聚合角色复用）。
- **KEDA 不存在集群**：清单参数化，可裁剪 ScaledObject RBAC 与 informer。
- **冲突 scheduler 前置检查**（方案文档 3.1）：CI/runbook 脚本扫描集群中其他 scheduler 工作负载的启动参数，发现使用相同 `schedulerName` 但 leader-elect Lease 名称/namespace 不同的实例即失败；本调度器自身 Lease 由清单固定。

## 8. 测试设计

### 8.1 单元测试

| 模块 | 必测 |
| --- | --- |
| keys.go | 哈希规范化（顺序无关、空/nil、preferred 不入哈希）、quotaKey/schedKey 构造 |
| domain.go | Ready/cordon/selector/taint 匹配矩阵；addedAffinity 生效 |
| state.go | COW 快照一致性；预占→Bind→观察释放全链路；TTL 各分支（含强制 reconcile 后释放、验证不可完成时保留）；reconcile 修复漂移；“绑定观察前双重计数”方向性断言 |
| plugin.go | PreFilter 各拒绝原因（含 CacheNotSynced 前置与可重试语义）；Filter 三条约束数值边界；Reserve 复核失败不写入；PreScore 归一化（极差映射、全等时 skip）与 Score 单调性 |
| enqueue.go | hint 的 Queue/QueueSkip 判定 |
| replicatarget.go | 优先级 1–5 全分支；reportedReplicaTarget 保护下限；capacity_deficit 计算与告警条件 |

### 8.2 集成测试

~~`k8s.io/kubernetes/pkg/scheduler/framework/testing` TestFramework 注册插件~~（该 helper 在 1.28 不存在，见 §9 偏差 14）：调度周期级别场景——并发 Reserve 竞争最后一个名额（验收 10）、策略后补触发重排（验收 11）、NotReady 节点被拒（验收 6 的域外拒绝）——M3 已用插件级单测覆盖决策逻辑（`internal/spread/plugin_test.go`），端到端行为验证并入 §8.3 kind e2e 矩阵。

### 8.3 e2e 矩阵（kind 1.31 + KEDA）

直接映射方案文档第 14 节验收标准 1–11，每条一个用例；另加：滚动升级 maxSurge 场景（11.5）、节点缩容不驱逐（11.2）、新节点回填（11.3）、双 Deployment 域隔离 + 配额共享（7.3 示例）。

### 8.4 混沌/故障演练（M4）

scheduler 重启（预占丢失后靠真实计数收敛）、informer 未同步窗口、KEDA API 不可用（degraded 不误判不存在）、webhook 宕机（策略创建失败但不影响已调度 Pod）。

## 9. 与方案文档的偏差记录

| # | 偏差 | 理由 |
| --- | --- | --- |
| 1 | `PreScore` 仅用于 Score 归一化，不做过滤、不承担“可行集重算 minCount”职责 | 逐节点 Score 看不到其他节点，无法自行归一化；未归一化的原始计数分差（1–2 分）会被 `NodeResourcesFit` 压过，均摊倾向失效。`maxSkew` 硬约束的 `minCount` 始终来自稳定均摊域（方案文档 7.2/8.2 边界不变） |
| 2 | 确定性名使用 12 hex | 降低 namespace 内碰撞概率，仍在 63 字符限制内（方案文档 4.1 示例已同步为 12 hex） |
| 3 | 版本暂定 1.31.x | 用户未确认；接口触点已隔离（§0、§5.1），升级成本受控 |
| 4 | 副本目标观测器与容量校验告警随 scheduler 进程运行（同 informer） | 方案文档未指定进程边界；同进程复用 informer 与 leader election，避免双份 cache 与 RBAC |
| 5 | `Filter` 对不在稳定均摊域的候选节点直接拒绝（`NodeOutsideSpreadDomain`） | 方案文档 7.2 未定义此场景；典型触发是节点 NotReady 后默认注入的 300s toleration 使其仍能通过原生 Filter，但已不满足均摊域条件。直接拒绝是保守且自洽的选择；已回写方案文档 7.2 |
| 6 | kedaFallback 取 `max(minReplicaCount, replicas)` 而非仅 `minReplicaCount` | 方案文档 6.2 第 2 条字面为 minReplicaCount（未配置再回退 replicas）；观测语义上 HPA 未就绪窗口内实际副本可能已高于 min，取 max 更接近现状。仅影响指标，不影响放置 |
| 7 | 缺服务标签一律拒绝，删除 `requireServiceLabel` 开关 | quotaKey 构造依赖 labelValue，缺失时无默认值语义可定义，开关永远应为 true，保留只会误导 |
| 8 | v1 显式禁止 profile 配置 `addedAffinity`，而非将其纳入哈希与域计算 | out-of-tree 插件无法读取其他插件的 pluginConfig；采用方案文档 5.2 允许的“显式禁止”分支，部署清单模板 + CI 保证 |
| 9（M1） | `ServiceSpreadArgs` 不经 `framework.DecodeInto` 解码，改用自带的 `DecodeServiceSpreadArgs`（std JSON + `DisallowUnknownFields`） | 1.28 的 `framework.DecodeInto` 虽接受 out-of-tree 类型（`*runtime.Unknown` 直接 `json.Unmarshal`），但**非严格**：未知字段被静默忽略，违反 §3.2 fail-closed 要求。自带解码器语义相同（含默认 GVK、校验、默认值），仅增加严格性（M1 已实现并单测） |
| 10（M1） | 确定性名规范化编码用 JSON 数组 `[ns, schedulerName, labelKey, labelValue]` 而非 `ns\|schedulerName\|labelKey\|labelValue` 竖线连接 | JSON 字符串转义对任意输入无歧义；竖线拼接理论上存在字段边界歧义（单测已构造出碰撞对）。K8s 输入校验虽排除竖线，但编码本身无歧义更稳，且 golden vector 单测锁定该编码 |
| 11（M1） | CEL VAP（1.28 → `admissionregistration.k8s.io/v1beta1` + feature gate，beta 默认关闭）使用 `object`/`oldObject`/`request.operation` 变量实现「设置或变更 nodeName 拒绝、保持不变放行」 | §7.2 伪代码用的 `input` 并非 VAP CEL 变量名（实为 `object`）。**事实修正**：k8s 1.28 核心 API 已禁止通过 UPDATE 修改 `spec.nodeName`（updatablePodSpecFields 白名单），§7.2 所述「创建后 PATCH nodeName 绕过」通道在主资源上不存在；VAP 覆盖 UPDATE 保留为纵深防御（envtest 已验证存量 Pod 常规更新不受影响）。1.30+ 升级时 apiVersion 改 v1 并去掉 runtime-config |
| 12（M1） | controller-gen 锁定 v0.16.5（而非 1.28 时代配对的 v0.13.0）；setup-envtest 用独立模块 @latest | v0.13.0 依赖的旧 x/tools 在 Go ≥1.23 下编译失败，其产物在 macOS ≥26 无法运行（LC_UUID）；v0.16.5 为可用的最早版本。产物是标准 apiextensions v1，且经 envtest 1.28 apiserver 实际安装验证。release-0.16 的 setup-envtest 仍指向已废弃 GCS（401），独立模块已迁移到 GitHub Releases |
| 13（M1） | §7.1 第 4 条「与既有策略服务重叠检测」实现为**拒绝**（live API list，失败即 fail closed），而非提示性 warning | 确定性命名下正常策略不可能重叠；能被检出的只有 webhook 缺位期间创建的野策略，放行会让插件随后以 AmbiguousServiceSpreadPolicy 拒绝调度，不如在准入时给出可修复的错误（附删除指引） |
| 14（M3） | §8.2 原计划的 `framework/testing.TestFramework` 集成测试不存在于 1.28（该 API 是 1.30+ 概念，§8.2 行文以 1.31 为准）；调度周期场景改由插件级单测（伪 deps 注入）覆盖，端到端并入 kind e2e | 1.28 无可复用的进程内 TestFramework；自建 scheduler 实例 + envtest 的装配成本高且与 e2e 矩阵重复。插件与 framework 的装配点收敛在 `plugins_api.go`（工厂签名 + deps 装配），版本升级或补做进程内集成时只改这一处 |
