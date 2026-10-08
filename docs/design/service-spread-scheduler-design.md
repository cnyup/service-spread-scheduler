# K8s 服务级均摊自定义调度器需求与设计方案

## 1. 背景与目标

集群需要为指定工作负载提供服务级别的 Pod 分布约束：在满足 Kubernetes 原生调度条件的前提下，同一服务的 Pod 应尽量均匀分布在其稳定可调度节点池中，并且不能超过单节点的绝对数量上限。

本方案实现一个基于 Kubernetes Scheduler Framework 的自定义调度器。它只处理显式指定 `schedulerName` 的 Pod，不影响默认调度器及其他调度器管理的工作负载。

### 1.1 目标

- 以用户配置的服务标签标识服务，不硬编码标签键。
- 同一 namespace、同一服务、同一调度域内的多个 Deployment 共同统计和均摊。
- 识别 KEDA `ScaledObject` 管理的 Deployment；若其对应 HPA 存在，优先使用 HPA 的实时期望副本数。
- 对无 HPA 管理的 Deployment，使用 `Deployment.spec.replicas` 回退计算。
- 使用 `maxSkew` 实现严格的均摊偏差限制。
- 使用 `maxPodsPerNode` 实现每节点同服务 Pod 数量的绝对硬上限；该上限按服务维度统计，跨调度域共享。
- 保留 Kubernetes 原生资源、亲和性、污点、卷绑定等调度判断。
- 节点资源、节点数量、HPA 目标变化时，只约束后续调度，不驱逐已运行 Pod。

### 1.2 非目标

- 不接管未指定自定义 `schedulerName` 的 Pod。
- 不将默认调度器或其他 scheduler 调度的 Pod 纳入计数、配额或均摊。
- 不主动驱逐、迁移或重调度已运行 Pod。
- 首版不支持 `StatefulSet`、Argo Rollout、自定义工作负载等非 Deployment 类型的扩缩容关联。
- 不以本次调度瞬时资源充足与否动态改变均摊域；资源可用性仍由原生 Filter 链决策。

## 2. 关键术语

| 名称 | 定义 |
| --- | --- |
| 自定义调度器 | 运行 `schedulerName: service-spread-scheduler` profile 的 kube-scheduler 实例。名称可部署时配置。 |
| 服务标识 | `namespace + serviceLabelKey + serviceLabelValue`。标签键由 scheduler 全局配置决定。 |
| 调度域 | 由 Pod 的稳定节点约束确定的节点集合；不同调度域分别均摊。 |
| 服务配额键 | `namespace + serviceLabelKey + serviceLabelValue + schedulerName`。`maxPodsPerNode` 的统计维度；同一服务的所有调度域共用该配额。 |
| 服务调度键 | `namespace + serviceLabelKey + serviceLabelValue + schedulerName + schedulingDomainHash`。`maxSkew` 均摊与副本目标观测按此键聚合。 |
| 均摊域节点 | 满足稳定节点约束的节点。它们参与 `maxSkew` 的最小计数计算。 |
| 当前可放置节点 | 通过本轮所有原生 Filter 的节点，包括资源、卷等瞬时条件。该集合用于最终选择节点和打分，但不改变 `maxSkew` 的均摊基线。 |

## 3. 接入方式

### 3.1 Scheduler Profile

部署一个包含自定义插件的 kube-scheduler 镜像，并为其配置独立 profile：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: service-spread-scheduler
    plugins:
      multiPoint:
        enabled:
          - name: ServiceSpread
      postFilter:
        disabled:
          - name: DefaultPreemption
    pluginConfig:
      - name: ServiceSpread
        args:
          serviceLabelKey: app.kubernetes.io/name
          requirePolicy: true
          managedSchedulerName: service-spread-scheduler
          fallbackCacheTTL: 10m
```

完整插件参数和语义见 12.1。

生产部署可使用多个 scheduler 副本和 leader election；服务同一 `schedulerName` 的所有副本必须使用同一个 Lease 名称和 namespace，确保同一时刻只能有一个活跃 scheduler 负责该 profile 的调度决策。部署与准入检查必须禁止另一套 scheduler 使用相同 `schedulerName` 但不同 Lease，否则两个 active scheduler 的进程内预占会相互不可见。

两点说明：

- 通过 `multiPoint.enable` 注册的自定义插件在每个扩展点默认排在原生插件之后执行，因此 `ServiceSpread` 的 Filter 天然位于原生 Filter 链之后，无需手工排序（见 8.2）。
- 该 profile 显式禁用了 `DefaultPreemption`，原因见 8.4：默认抢占可能与"不驱逐同服务 Pod"的承诺冲突。

### 3.2 工作负载接入

只有显式声明该 scheduler 的、且未预先绑定节点的 Pod 才进入策略范围：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: image-processing
  namespace: production
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: image-processing
    spec:
      schedulerName: service-spread-scheduler
```

若 Pod 使用 `service-spread-scheduler`，但缺失全局配置指定的服务标签，调度器必须拒绝调度并创建 `Warning` Event。默认调度器 Pod 不受影响，也不参与本调度器的统计。

`spec.nodeName` 会绕过 scheduler，因此接入 namespace 中必须部署 ValidatingAdmissionPolicy 或 validating webhook：当 Pod 的 `schedulerName` 等于受管 scheduler 时，拒绝用户预设 `spec.nodeName`。仅授予受信任系统组件例外权限（例如使用 namespace label 或专用 ServiceAccount 匹配），避免绕过 `maxSkew` 与 `maxPodsPerNode` 硬约束。

## 4. 策略 API

服务级差异化规则通过 CRD 管理，而非允许 Deployment annotation 任意覆盖。这样策略可校验、可审计、可通过 GitOps 管理，并由 RBAC 控制修改权限。

### 4.1 CRD：ServiceSpreadPolicy

建议 API 组为 `scheduling.example.io/v1alpha1`，正式落地前应替换为组织实际域名。

```yaml
apiVersion: scheduling.example.io/v1alpha1
kind: ServiceSpreadPolicy
metadata:
  # 由 admission 从 namespace、schedulerName、服务标签键和值确定性生成。
  name: ssp-7f4f83c8d21a
  namespace: production
spec:
  schedulerName: service-spread-scheduler
  serviceSelector:
    matchLabels:
      app.kubernetes.io/name: image-processing

  # 任意两个参与均摊节点之间允许的最大同服务 Pod 数差。
  maxSkew: 1

  # 单节点、同服务（跨调度域共享）的绝对数量上限。
  maxPodsPerNode: 9
```

`maxSkew` 与 `maxPodsPerNode` 在 CRD schema 中为必填字段（integer，无默认值），避免"未配置即不限制"的隐式行为。

### 4.2 约束与校验

- `spec.schedulerName` 必须与 Pod 的 `spec.schedulerName` 完全匹配。
- `serviceSelector` 首版只支持且必须只包含全局 `serviceLabelKey` 的精确匹配，避免多标签选择器与服务标识产生歧义。
- `maxSkew` 为不小于 `1` 的整数。按本方案的硬不等式，节点计数均为 `0` 的均摊域中放置第一个 Pod 即会产生 `1 - 0` 的偏差，因此 `maxSkew: 0` 连第一个 Pod 也无法调度；首版不定义特殊 bootstrap 语义。
- `maxPodsPerNode` 为正整数。该值是严格硬上限，统计维度为服务配额键（跨调度域共享）。

### 4.3 CRD 作用域与策略匹配的唯一性

`ServiceSpreadPolicy` 必须定义为 **Namespaced CRD**，因为服务标识和策略匹配均以 namespace 为边界。

"每个 `namespace + 服务标签 + schedulerName` 最多命中一条策略"不能只依赖插件运行时拒绝或仅依赖 validating webhook 的列表查询，因为并发创建可能同时通过校验。落地方式分三层：

- **确定性对象名**：Admission 根据 `namespace + schedulerName + serviceLabelKey + serviceLabelValue` 生成确定性名称（必要时使用安全截断的哈希），并要求 `metadata.name` 精确匹配。Kubernetes API 对同 namespace 对象名的唯一性提供最终保证。
- **Admission**：validation webhook 做 schema 静态校验（`serviceSelector` 只含 `serviceLabelKey`、`maxSkew` 与 `maxPodsPerNode` 必填且合法、`maxSkew >= 1`、`schedulerName` 非空且等于受管 scheduler 名，见 4.2 的完全匹配要求）。
- **运行时兜底**：插件若仍检测到多条命中（例如 webhook 缺位期间创建的存量策略），拒绝调度并产生 `AmbiguousServiceSpreadPolicy` Warning Event。
- 未命中策略的处理由全局配置 `requirePolicy` 决定；建议首版默认拒绝，避免无意绕过强约束。

## 5. 服务调度键与调度域

### 5.1 服务聚合范围

服务的基础范围是：

```text
namespace + serviceLabelKey + serviceLabelValue
```

但相同服务标签的 Deployment 不一定能调度到相同节点。例如一个 Deployment 只能使用 GPU 节点池，另一个只能使用 CPU 节点池。两者不能共同使用同一个均摊分母。

因此实际聚合范围使用服务调度键：

```text
namespace
+ serviceLabelKey
+ serviceLabelValue
+ schedulerName
+ schedulingDomainHash
```

### 5.2 schedulingDomainHash

首版对以下稳定调度约束做规范化 JSON 序列化，并计算 SHA-256：

```text
spec.nodeSelector
spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution
spec.tolerations
NodeAffinity 插件为该 profile 配置的 required addedAffinity（若启用）
```

规范化要求：

- Map 按键排序。
- 列表中的表达式、值集合和 toleration 按确定性顺序排序。
- 空字段和默认值按统一形式表示。
- 只处理影响候选节点集合的字段，不把优选 affinity、资源请求、卷绑定结果等瞬时或偏好条件放入哈希。
- 若不希望将 `addedAffinity` 纳入哈希与均摊域计算，则 profile 配置校验必须显式禁止该字段；不能忽略它。

两个 Deployment 只有在上述约束规范化后完全相同，才进入同一个调度域共同计算。首版不做选择器逻辑等价推导。

### 5.3 均摊域节点

均摊域节点由待调度 Pod 的稳定约束计算：

- 节点 Ready。
- 节点未设置 `spec.unschedulable: true`。
- 匹配 `nodeSelector`。
- 满足 required Node Affinity。
- 污点均被 Pod toleration 覆盖。
- 满足该 profile 的 required `addedAffinity`（如配置）。

以下条件不改变均摊域：

- 本轮调度的瞬时资源是否足够。
- PVC / PV 当前是否可绑定。
- 节点镜像缓存、优选 affinity、其他打分因素。

原生 `NodeResourcesFit`、`VolumeBinding` 等 Filter 仍会从本轮可放置节点中排除不合格节点。这样可以在纯 Scheduler Framework 中保持严格、稳定、可解释的 `maxSkew` 硬约束。

## 6. 副本目标计算（观测与容量告警数据源）

副本目标**不参与放置决策**：Filter 中的两条硬约束只依赖节点实际计数和策略值。它用于描述服务当前期望规模、观测均摊是否收敛（`desiredReplicas` 对比各节点计数），以及容量告警与容量预检（见 10.3、12.2）。明确这一点可避免实现者误将其接入调度路径，也让回退语义更简单。

### 6.1 支持的工作负载边界

对于待调度 Pod：

```text
Pod -> ReplicaSet -> Deployment
```

插件通过 ownerReferences 解析其所属 Deployment。无法解析到 Deployment 的 Pod（StatefulSet、裸 Pod、Job、Argo Rollout 等）不在首版支持范围内：拒绝调度并产生 `UnsupportedWorkloadOwner` Warning Event。这是独立的工作负载边界规则，与副本目标是否可解析无关。

### 6.2 KEDA 与 HPA 识别

在 Pod 所在 namespace 内查询：

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
```

当 `ScaledObject.spec.scaleTargetRef` 指向当前 `apps/v1 Deployment` 时，表示该 Deployment 由 KEDA 管理。HPA 同样仅通过 `spec.scaleTargetRef` 指向该 Deployment 来关联；不得依赖 KEDA 自动创建 HPA 的名称。之后查询该关联 HPA 的状态。

目标副本数的优先级如下：

1. 有关联 HPA 且 `status.desiredReplicas` 有效：使用该值。
2. 有关联 `ScaledObject`，但 HPA 尚未创建或状态未就绪：使用 `ScaledObject.spec.minReplicaCount`；若该字段未配置，再使用 `Deployment.spec.replicas`。
3. 无关联 KEDA / HPA：使用 `Deployment.spec.replicas`。
4. 多个 HPA 的 `scaleTargetRef` 指向同一 Deployment：视为配置歧义，取其中最大值并触发 `ReplicaTargetFallback` 告警，提示用户修正。
5. API 查询、缓存同步或权限出现异常：使用最近一次成功读取的目标值并触发告警；`fallbackCacheTTL` 到期仍失败则将指标标记为失效（stale），但不影响调度——放置决策不依赖该值。

同一服务调度键中，所有 Deployment 的目标副本数相加：

```text
domainDesiredReplicas = sum(deploymentDesiredReplicas)
```

### 6.3 回退保护

为避免观测数据在控制面暂时异常时产生误导（例如把一个实际运行 250 个 Pod 的服务上报为 20），导出副本目标指标时执行保护下限：

```text
observedPodCount = 同一服务调度键下，schedulerName 相同且 phase 为 Pending 或 Running 的 Pod 数

reportedReplicaTarget = max(domainDesiredReplicas, observedPodCount)
```

由于副本目标不驱动调度，该保护只影响指标与告警语义，不会造成错误收紧或调度停滞。

> 说明：HPA 的 `spec.maxReplicas` 是扩容上界，不能作为当前每节点上限。HPA 从 250 扩至 300 时，新 Pod 仍应从当前数量较少的节点开始补齐，直到达到策略的 `maxPodsPerNode` 或自然均摊状态。

## 7. 调度规则

### 7.1 统计范围

统计分两个维度，均要求 Pod 满足以下公共条件：

```text
namespace 相同
服务标签键和值相同
schedulerName 相同，且等于目标自定义 scheduler
phase 为 Pending 或 Running
```

不统计默认调度器、其他 scheduler 管理的 Pod，也不统计 `Succeeded`、`Failed` Pod。

在此基础上：

```text
服务配额计数 quotaCount(node)：
  节点上、服务配额键（不含 schedulingDomainHash）匹配的全部 Pod 数。
  该服务的所有调度域共用，用于 maxPodsPerNode。

均摊计数 count(node)：
  节点上、服务调度键（含 schedulingDomainHash，即当前 Pod 所在域）的 Pod 数。
  用于 maxSkew。
```

两个维度分离的原因：`maxPodsPerNode` 表达的是"每个节点最多多少个同类型 Pod"这一服务级约束。若按调度键统计，同一服务的两个 Deployment 因 `nodeSelector` 差异（例如 `{pool: gpu}` 与 `{pool: gpu, size: large}`）落入不同域但节点集重叠时，可在同一节点各占一份 `maxPodsPerNode`，突破上限；滚动升级修改 `nodeSelector` / `toleration` 切换调度域时同样如此。因此绝对上限必须跨域共享，只有均摊（`maxSkew`）在域内计算。

按节点统计时只计入已绑定到该节点的 Pod（`spec.nodeName` 非空）；尚未绑定节点的 Pending Pod 不影响 per-node 计数。

### 7.2 强约束

设：

```text
count(n)         = 均摊域节点 n 上，当前服务调度键的 Pod 数（含 Reserve 预占）
quotaCount(n)    = 节点 n 上，服务配额键的 Pod 数（含 Reserve 预占）
baselineNodes    = 稳定均摊域节点
minCount         = min(count(n))，n 属于 baselineNodes
candidate        = 当前被 Filter 的候选节点
```

将待调度 Pod 放置到 `candidate` 前，`candidate` 必须属于 `baselineNodes`；否则自定义插件返回 `Unschedulable`。这避免诸如 NotReady 节点在临时 toleration 下通过原生 Filter 时绕过 `maxSkew` 强约束。

满足上述前提后，放置必须同时满足：

```text
quotaCount(candidate) + 1 <= policy.maxPodsPerNode
count(candidate) + 1 - minCount <= policy.maxSkew
```

任一条件不满足时，自定义插件返回 `Unschedulable`，该节点不能用于本 Pod。

这两条规则职责不同：

- `maxPodsPerNode` 是服务级绝对上限，跨调度域共享，防止任何形式的节点堆积。
- `maxSkew` 确保域内节点间的同服务 Pod 数差不超过允许偏差。

### 7.3 示例

假设：

```text
均摊域节点数：30
当前同一服务调度键 Pod 数：250
maxSkew：1
maxPodsPerNode：9
```

符合规则的均衡状态为：

```text
20 个节点各 8 个 Pod
10 个节点各 9 个 Pod
```

此时：

- 当前为 8 个的节点可以接收一个 Pod，变为 9 个。
- 当前为 9 个的节点不能接收 Pod，因为会触发 `maxPodsPerNode`，且会使偏差超过 1。
- 若业务将 `maxPodsPerNode` 配为 8，则总容量最多为 240；第 241 个及之后的 Pod 保持 Pending。这是预期的强约束结果。

若 HPA 当前期望为 250、未来最大可扩至 300，策略上限仍为 9，则该服务最多只能承载 270 个 Pod（跨调度域共享该上限）。应通过容量告警提示 `maxPodsPerNode`、节点池规模与 HPA `maxReplicas` 不匹配，而不是突破单节点上限。

同服务多域叠加的示例：Deployment A（`nodeSelector: {pool: gpu}`，10 个 GPU 节点）与 Deployment B（`nodeSelector: {pool: gpu, size: large}`，其中 4 个节点同时满足两个约束）。A、B 哈希不同、各自均摊，但 quota 计数合并：在 4 个重叠节点上，A 的 Pod 数 + B 的 Pod 数 + 1 不能超过 `maxPodsPerNode`。

## 8. Scheduler Framework 插件设计

插件名称：`ServiceSpread`。

### 8.1 扩展点

| 扩展点 | 职责 |
| --- | --- |
| `PreFilter` | 验证 `schedulerName`、服务标签、策略、Deployment owner；解析服务调度键，读取或从 informer cache 获取策略、Pod 和节点信息（owner 解析经 ReplicaSet/Deployment），并构建 CycleState。HPA 与 KEDA 仅由副本目标观测器消费，不进入放置路径。标签缺失、策略缺失或歧义、owner 不支持时返回 `UnschedulableAndUnresolvable`，避免这些 Pod 在调度队列中热重试。 |
| `Filter` | 对每个已通过此前 Filter 的节点，按服务配额键执行 `maxPodsPerNode`、按服务调度键执行 `maxSkew` 硬约束。插件必须排在原生资源、亲和性、污点、卷绑定等 Filter 之后。 |
| `PreScore` | 获取原生 Filter 链最终留下的节点集合，计算可行集内同服务计数的 min/max，写入 CycleState 供 Score 归一化使用。该阶段不做硬过滤，也不改变以稳定均摊域计算的 `minCount`。 |
| `Score` | 计数越少得分越高，且分数按可行集内计数极差归一化：最小计数节点接近满分、最大计数节点接近 0 分。未归一化的原始计数分差过小，会被资源打分稳定压过，导致均摊倾向失效。计数相同返回相同分数，让其他原生 Score 插件继续基于资源利用、亲和性等因素区分。 |
| `Reserve` | 在单个互斥临界区内重新读取真实计数与全部临时预占，重新计算均摊域 `minCount`，并原子校验候选节点仍在稳定均摊域、`maxPodsPerNode` 和 `maxSkew` 后，才按 Pod UID 写入 `serviceSchedulingKey + nodeName`、`serviceQuotaKey + nodeName` 两类预占。若复核失败，返回不可预占状态，不写入任何预占。 |
| `Unreserve` | 调度失败、Permit 拒绝或 Bind 失败时，按 Pod UID 释放临时占位。 |
| `PostBind` | Bind 成功后不立即释放预占。预占必须保留到 Pod informer 观察到同一 Pod UID 已绑定至目标节点后，才在同一计数锁中完成“真实计数生效、预占移除”的切换，防止 watch 延迟期间低估节点计数。 |

### 8.2 插件顺序

`ServiceSpread` 作为 Filter 的最后一项执行。这样只有通过原生硬约束的节点才会进入其逐节点校验；但其 `maxSkew` 的最小计数始终来自稳定均摊域，而非本次资源瞬时可用节点集合。

该设计是纯 Scheduler Framework 的关键边界：Framework 不提供“全部 Filter 完成后，再做一轮可继续过滤的全量节点集合”扩展点。若必须把资源充足节点作为动态均摊分母，则需要 Scheduler Extender 或维护 kube-scheduler fork，本方案不采用这两种方式。

### 8.3 并发与一致性

- 单 active leader 处理调度决策；部署多副本时依赖 kube-scheduler leader election，所有副本复用同一个 Lease。
- `Filter` 与 `Score` 读取真实 Pod 计数加临时预占；`Reserve` 必须在同一个锁保护的临界区中原子重新校验并写入预占，不能只在 Filter 通过后直接递增。若 Reserve 复核失败，scheduler 按正常失败路径释放状态并在后续队列事件中重试，从而避免多个并发 cycle 争抢同一最后配额时突破上限。
- Bind 成功的预占以 Pod UID 保留，直到 informer 观察到该 UID 绑定到预期节点；随后在同一锁内先纳入真实计数再删除预占。TTL 仅用于异常兜底：到期前必须确认 Pod 已删除、未绑定到预期节点或 Bind 已失败，不能对已成功绑定但 informer 延迟的 Pod 直接释放预占。
- 插件重启后临时占位可丢失，但已绑定 Pod 会由 informer 重新建立真实计数；这是可接受的短暂最终一致状态。
- 所有 informer cache 未同步完成前，插件应返回 `UnschedulableAndUnresolvable` 或阻止 scheduler readiness，不能使用空数据继续调度。
- 对 `ServiceSpreadPolicy`、Pod、Node、ReplicaSet、Deployment 的 Add/Update/Delete 注册 scheduler queue 事件（Pod 的 Delete 事件必须包含：HPA 缩容、驱逐或正常退出释放配额后，同服务 Pending Pod 依赖该事件重排）。Policy 创建/修复、owner 链补齐、节点约束变化或 Pod 计数变化时，必须将相关不可调度 Pod 重新入队；支持 QueueingHint 的版本应做定向重排，否则使用有界的相关 Pod 重排。HPA 与 ScaledObject 不注册重排：副本目标不驱动放置决策（第 6 节），仅由观测器消费。

### 8.4 禁用默认抢占

该 profile 显式禁用 `DefaultPreemption`（PostFilter）。原因：Pod 因 `maxSkew` / `maxPodsPerNode` 不可调度时，默认抢占会在候选节点上挑选低优先级 victim 并模拟重跑 Filter；若 victim 恰是同服务、同键的 Pod，移除它会让均摊校验通过，结果是**同服务 Pod 被驱逐**——直接违背 11.2"不驱逐已运行 Pod"的承诺，且与 HPA 扩缩容互相干扰（调度器刚按均摊放置，抢占又移除了 victim）。

对本方案的目标场景（弹性伸缩服务 + 严格均摊），可预测性优先于抢占带来的调度成功率：约束不可满足时让 Pod 保持 Pending，由容量告警驱动扩容节点或调整策略。代价是该 profile 下的 Pod 也不参与对其他工作负载的抢占，需在接入文档中向用户说明。

## 9. 数据访问与 RBAC

插件使用 SharedInformer / lister，避免在每次调度周期直接访问 API Server。需要监听：

- `core/v1 Pods`
- `core/v1 Nodes`
- `apps/v1 Deployments`
- `apps/v1 ReplicaSets`
- `autoscaling/v2 HorizontalPodAutoscalers`
- `keda.sh/v1alpha1 ScaledObjects`
- `scheduling.example.io/v1alpha1 ServiceSpreadPolicies`

对应 ServiceAccount 至少需要上述资源的 `get`、`list`、`watch` 权限。启用 LeaseLock leader election 时，还必须对 leader election namespace 中指定的 `coordination.k8s.io/v1 Lease` 授予 `get`、`list`、`watch`、`create`、`update`、`patch` 权限。CRD、KEDA API 和 HPA API 不可访问时，应暴露健康检查失败或明确降级指标，不能静默当作“对象不存在”。

## 10. 异常处理与可观测性

### 10.1 Pod Event

插件返回的 `Unschedulable` 状态 message 会通过 scheduler 标准的 `FailedScheduling` Event 暴露给用户，无需额外 Event recorder。因此 Event 侧只要求：状态 message 包含被违反的约束（`maxSkew` / `maxPodsPerNode`）和当前节点计数，便于 `kubectl describe pod` 直接诊断。表中 reason 名是规范契约：同时用作 metric label 取值与 Filter/PreFilter message 的前缀；对 Pod 的 Event 呈现统一经标准 `FailedScheduling`（Event 自身的 reason 字段为 `FailedScheduling`，自定义 reason 不作为独立 Event reason）。

高频或非 Pod 维度的情况不逐 Pod 发 Event，只走 metric：

| 原因 | 暴露方式 | 说明 |
| --- | --- | --- |
| `MissingRequiredServiceLabel` | Event + metric | Pod 指定自定义 scheduler，但缺少服务标签。 |
| `ServiceSpreadPolicyNotFound` | Event + metric | 未命中服务策略，且全局配置要求策略存在。 |
| `AmbiguousServiceSpreadPolicy` | Event + metric | 命中多条策略。 |
| `UnsupportedWorkloadOwner` | Event + metric | 无法通过 Pod -> ReplicaSet -> Deployment 解析工作负载。 |
| `ServiceSpreadConstraint` | metric（节点明细在 message 中） | 所有节点均因 maxSkew 或 maxPodsPerNode 被拒绝。 |
| `ReplicaTargetFallback` | 仅 metric | HPA / KEDA 数据未就绪、读取失败或多 HPA 歧义。按 Pod 发 Event 噪音过大。 |

### 10.2 Metrics

建议暴露 Prometheus 指标：

```text
service_spread_filter_rejections_total{reason}
service_spread_policy_resolution_failures_total{reason}
service_spread_replica_target{namespace,service,domain}
service_spread_observed_pods{namespace,service,domain}
service_spread_node_pods{namespace,service,domain,node}
service_spread_reservations{namespace,service,domain}
service_spread_fallback_active{namespace,service,domain}
service_spread_capacity_deficit{namespace,service}
```

高基数标签需要谨慎：生产 Prometheus 中可选择不在全局指标上暴露 `node`，或仅通过调试 endpoint 查询节点级详情。

### 10.3 告警建议

告警由控制器周期性计算，而非 admission 时同步计算（容量公式依赖各 Deployment 的调度域节点数，策略创建时不可得）：

- HPA 的 `maxReplicas` 大于 `服务可用节点数 * maxPodsPerNode`。其中服务可用节点数按各 Deployment 的稳定约束分别求出后取并集。
- 某服务有 Pending Pod，且所有节点均被 `maxPodsPerNode` 拒绝。
- 策略、KEDA、HPA 或 informer 数据访问连续失败；副本目标缓存超过 `fallbackCacheTTL` 仍处于 stale 状态。
- 节点计数差已超过 `maxSkew`，且原因是历史遗留分布或节点状态变更；此时仅告警，不驱逐。

## 11. 行为边界与示例

### 11.1 资源不足节点

某节点在稳定均摊域内，但本轮资源不足：

- 它仍参与 `minCount` 计算。
- 原生 `NodeResourcesFit` 会将它从实际候选节点中排除。
- 如果其计数较低，其他节点可能因 `maxSkew` 被限制，新的 Pod 可能 Pending。

这是为保证绝对均摊约束不因瞬时资源变化而失效所做的明确取舍。

### 11.2 节点减少或变为不可调度

- 本 scheduler profile 与插件不会主动驱逐、抢占或重调度该节点已有 Pod。
- 节点长期 NotReady 时，kube-controller-manager 基于污点的 Pod 驱逐属于 Kubernetes 控制器自身行为，不受本方案控制；本方案的“不驱逐”承诺不覆盖该类故障处置。
- 该节点不再属于后续调度的稳定均摊域。
- 后续 Pod 仅在新的均摊域中满足约束。
- 若已有节点分布已超过新策略，不主动修复；只保证新 Pod 不进一步违反规则。

### 11.3 节点新增

新节点加入均摊域后，其同服务 Pod 数为 0，`minCount` 降为 0：

- 存量节点会被 `maxSkew` 暂时拦住，新 Pod 优先回填新节点，直到各节点计数追平。这是预期的自愈行为。
- 若新节点数量多、或新节点资源紧张（见 11.1），回填期间可能出现一段 Pending 期；应通过扩充资源、调整节点池或临时提高 `maxSkew` 缓解。提高 `maxSkew` 会放宽整个调度域内所有节点的偏差限制，而非仅绕过某个低计数故障节点，因此应作为短期运维措施并在故障恢复后回调。
- 该行为不驱逐任何存量 Pod，只影响新 Pod 放置。

### 11.4 纯 Framework 的均摊基线边界

本设计固定以稳定均摊域作为 `minCount` 基线。标准 Scheduler Framework 中，`Filter` 按节点逐个执行，无法取得所有原生 Filter 完成后的全量可行节点；`PreScore` 虽可读取最终可行节点，但已不能继续 Filter。因此不能将瞬时资源充足节点作为 `maxSkew` 的硬约束分母。

若低计数节点长期资源不足导致服务整体 Pending，应通过扩容该节点池资源、修复故障节点、减少稳定均摊域，或临时提高 `maxSkew` 处理。后者会放宽整个调度域，不能只针对故障节点生效，应纳入变更审批并在恢复后回调。若业务必须使用最终 feasible nodes 作为硬分母，应改用 Scheduler Extender 或维护 kube-scheduler fork，不属于本方案范围。

### 11.5 滚动升级交互

Deployment 滚动升级时，maxSurge 引入的新 Pod 仍占用同服务配额与均摊计数：

- 域内节点已到达 `maxPodsPerNode` 或 `maxSkew` 上限时，surge Pod 会 Pending，滚动升级被放慢（旧 Pod 未删除、新 Pod 未就绪）。
- 升级期间修改 `nodeSelector` / `toleration` 会切换 `schedulingDomainHash`：新旧 Pod 属于不同均摊域，但 `maxPodsPerNode` 按服务配额键跨域共享，不会因域切换突破节点上限。
- 运维提示：对配额接近上限的服务做大版本升级前，可临时调高 `maxPodsPerNode`，或采用 `maxSurge: 0` 且 `maxUnavailable >= 1` 的滚动更新模式，先删除部分旧 Pod 再创建新 Pod，避免 surge 同时占用额外配额；升级完成后恢复策略。

### 11.6 默认调度器 Pod

同 namespace、同服务标签、但 `schedulerName: default-scheduler` 的 Pod：

- 不计入 `observedPodCount`。
- 不计入节点 `count(n)` 与 `quotaCount(n)`。
- 不占用 `maxPodsPerNode` 配额。
- 不受本插件均摊规则约束。

但要注意：这类 Pod 仍实际占用节点资源，会通过原生 `NodeResourcesFit` 间接影响本调度器 Pod 的可放置节点集合。

## 12. 配置建议

### 12.1 集群级插件参数

```yaml
args:
  serviceLabelKey: app.kubernetes.io/name
  requirePolicy: true
  managedSchedulerName: service-spread-scheduler
  fallbackCacheTTL: 10m
  reservationTTL: 5m
  reconcilePeriod: 10m
```

缺服务标签的 Pod 一律拒绝（quotaKey 的构造依赖标签值），不提供 `requireServiceLabel` 开关。

`fallbackCacheTTL` 到期后若 API 仍异常，将副本目标指标标记为 stale 并报警；由于副本目标不驱动调度决策（见第 6 节），该超时不会阻断调度。放置决策依赖的 Pod / Node / 策略 informer 未同步时，才按 8.3 拒绝调度。

### 12.2 容量校验

容量公式依赖各 Deployment 的调度域节点数，admission 时不可计算，因此不在 admission 中做容量校验（admission 只做 4.3 的静态校验）。容量校验由控制器周期性执行：

```text
HPA.spec.maxReplicas <= serviceEligibleNodeCount * maxPodsPerNode
```

其中 `serviceEligibleNodeCount` 为各 Deployment 稳定约束求出的可用节点数的并集大小。不满足时产生 10.3 的容量告警：该服务扩容到 HPA 上界时必然出现 Pending Pod。

### 12.3 Kubernetes 版本兼容性

实现前必须锁定目标 Kubernetes 小版本，并以该版本的 `k8s.io/kubernetes` 作为插件编译依赖；Scheduler Framework 没有稳定的跨小版本插件 ABI。本文中的 queueing hint / 事件重排设计以目标版本提供的 Framework 接口为准：若目标版本不支持 `QueueingHint`，必须实现等价的有界重排策略并在兼容性测试中验证。自定义 scheduler 镜像应与控制面 Kubernetes 小版本保持一致。

## 13. 实施阶段

### Phase 1：API 与只读观测

- 定义 `ServiceSpreadPolicy` CRD、schema、client 和 informer，以及 4.3 的 validation webhook。
- 实现服务调度键、服务配额键与 `schedulingDomainHash`。
- 实现 KEDA `ScaledObject`、HPA、Deployment 副本目标解析与回退指标。
- 仅输出事件、日志与指标，不改变调度决策，用于验证数据正确性。

### Phase 2：硬约束与打分

- 实现 `PreFilter`、`Filter`、`PreScore`、`Score`。
- 引入 `maxSkew` 与 `maxPodsPerNode` 双维度硬约束；确认 profile 已禁用 `DefaultPreemption`。
- 通过 e2e 测试验证标签缺失、策略缺失、多个 Deployment 汇总、不同调度域隔离与配额共享、资源不足节点、节点增减和扩缩容场景。

### Phase 3：并发保护与生产化

- 实现 `Reserve` / `Unreserve` 双维度预占。
- 接入 leader election、readiness、Prometheus、容量告警控制器和 watched-resource queue requeue 机制。
- 演练 scheduler 重启、informer 未同步、KEDA/HPA 延迟、API 访问失败和滚动升级等故障场景。

## 14. 验收标准

1. 仅 `schedulerName: service-spread-scheduler`、且未预设 `spec.nodeName` 的 Pod 被本插件处理；Admission 阻止受管 Pod 通过预设 `nodeName` 绕过约束。
2. 缺失服务标签的目标 Pod 不被调度，并具有可诊断 Event。
3. 同 namespace、同服务标签、同调度域的多个 Deployment 共同计数并聚合副本目标。
4. 不同 `schedulingDomainHash` 的工作负载独立均摊，但共享同一 `maxPodsPerNode` 服务配额。
5. 默认调度器和其他 scheduler 的 Pod 不参与任何本插件计数。
6. 新放置 Pod 后，节点上该服务（跨调度域合计）Pod 数不超过 `maxPodsPerNode`；候选节点必须属于稳定均摊域，且相对该域最小值的差不超过 `maxSkew`。
7. 副本目标（HPA 优先、Deployment 回退）仅用于观测与容量告警，不改变放置决策；读取异常时标记 stale 并告警。
8. 资源不足节点由原生调度 Filter 排除；其瞬时状态不改变稳定均摊域定义。
9. 节点、资源、HPA 或策略变化后不驱逐既有 Pod；该 profile 下不发生默认抢占。
10. 多个 Pending Pod 并发调度时，Reserve 原子复核使其不因竞争突破 `maxPodsPerNode` 或 `maxSkew`。
11. Policy、owner 链、Pod 或 Node 变化后，之前因依赖未就绪或策略约束不可调度的相关 Pod 无需重建即可被重新入队并重试。（HPA / KEDA 变化不触发重排：副本目标不驱动放置决策。）
