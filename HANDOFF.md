# HANDOFF — Service Spread Scheduler 项目交接文档

> 给新会话/新协作者的入口。先读「零、当前状态快照」与「一、下一步」，需要背景再往下读。
> 最后更新：2026-10-08（v0.2.1 发版成功——修复后管线首次完整自动发版，三地产物全部验证）

## 零、当前状态快照（2026-10-08）

**一句话**：功能 100% 完成且全链路验证闭环；v0.2.1 已发版——修复后的 release 管线（ghcr 双架构 → Docker Hub 公开转推 → gh-pages chart）首次完整自动跑通，三地产物逐项验证落定。

| 维度 | 状态 |
| --- | --- |
| 代码 | main @ 0e35379，工作树干净，CI 绿（build/vet/test/race/helm-lint 全过，run 37742964804） |
| 功能 | 设计文档验收标准 11/11、偏差表 14/14、观测指标 7/7 全部实现并有验证证据 |
| 发版 | **v0.2.1 全自动发版成功**（run 37743284488：release 5m55s ✓ + push-dockerhub 30s ✓）；v0.2.0 为部分发版（仅 ghcr 镜像，见 §六）；chart 0.1.2–0.2.1 在 gh-pages |
| 分发 | **Docker Hub `bcyup/service-spread-scheduler` + `bcyup/service-spread-webhook`（公开匿名）**，v0.2.1 镜像 digest 与 CI push 日志逐字节一致（86ed2c49…/e99f1476…）；ACR 管线已退役（存量 v0.1.x 可拉） |
| 文档 | README（中文，三段布局+完整 values 表+大陆 mirror 专节）/ RUNBOOK / deploy-checklist / release-checklist 全部对齐现状 |
| 集群现场 | ACK 测试集群已清零验证残留；yup-dev kind 集群 ssp-e2e2 已删除（2026-10-08，原 v0.1.1 常驻与 KEDA 随之清场），e2e 环境待定新场地（不在 yup-dev） |

## 一、下一步（按优先级）

1. **大陆节点配 mirror 后实测 v0.2.1**（部署前唯一未验项）：ACK 每台节点 `/etc/docker/daemon.json` 加 `registry-mirrors`（README 安装节有示例）→ `systemctl restart docker` → helm install 应全绿。未配 mirror 时 kubelet 拉 docker.io 会 DeadlineExceeded（0.1.4 实测复现）。
2. 用户侧可选清理：GitHub Settings 删 4 个 ACR secrets + ENABLE_ACR variable；ACR 控制台三个仓库（scheduler/webhook/service-spread-chart）可删可留。
3. 非阻塞增强（按需）：`newKEDAIndexer` 周期重探（KEDA 后装目前需重启 scheduler，keda.sh 自动兜底）；GaugeVec 序列基数治理（已删服务的指标序列永生）。

## 二、整体链路（现役）

```
开发循环:
  本地 /Users/yup/qw/scheduling ⇄ mutagen 会话 scheduling ⇄ yup-dev:/root/code/scheduling
  （.git 只在远程；编辑本地做，git/build/test 在远程做：ssh yup-dev "cd /root/code/scheduling/service-spread-scheduler && <cmd>"）
  → push main → GitHub CI（go build/vet/gofmt/test-race + YAML 校验 + helm lint/三渲染 smoke）

发版（一个 tag 触发全部）:
  git tag vX.Y.Z → release job: ghcr 多架构构建（构建源）
                 → push-dockerhub job: 转推 docker.io/bcyup/{service-spread-scheduler,service-spread-webhook}（公开匿名）
                 → chart 发布: helm package → gh-pages 分支（index.yaml + tgz）
  （前置 secrets/variables: DOCKERHUB_TOKEN + ENABLE_DOCKERHUB=true 已配；ACR 相关已无用可删）

部署（标准，任何网络可达 docker.io 的集群）:
  helm repo add ssp https://cnyup.github.io/service-spread-scheduler && helm repo update
  kubectl create ns service-spread-system
  helm install my-spread ssp/service-spread-scheduler -n service-spread-system --set global.namespace=service-spread-system
  （零 secret：镜像公开匿名；证书进程内自签；webhook 冷启动 1-2 次 CrashLoop 自愈属预期）

部署（大陆，kubelet 直连 docker.io 超时——ACK 实测）:
  节点配 registry mirror（docker: daemon.json registry-mirrors；containerd: hosts.toml）后同上
  或 ACR 回退（README 折叠附录有完整 --set 命令 + secret 创建；ACR 个人版"公有"也必须认证）

运维:
  升级: helm upgrade（RBAC 自动重 apply——历史三次漂移事故因此类绝；args 变更 checksum 自动滚 Pod）
  回滚: helm rollback；健康: kubectl get lease service-spread-scheduler -n service-spread-system 看 renewTime（不是 /healthz）
  卸载: helm uninstall + 手动删 CRD（crds/ 语义不随卸载删）
  监控: prometheusrule 可选（4 告警；标签须匹配 ruleSelector，ACK 实测 prometheus:k8s + monitoring ns）
```

## 三、环境约定（新会话必读）

- **开发模式**：本地编辑 + mutagen 同步 + yup-dev 执行（AGENTS.md 有通用纪律）。mutagen 会话名 `scheduling`，忽略 .rivet/bin/.DS_Store；提交前 `mutagen sync flush scheduling`。
- **git 仓库只在远程**（`/root/code/scheduling/service-spread-scheduler/.git`，main，origin=github.com:cnyup/service-spread-scheduler，SSH key `~/.ssh/id_ed25519_github` 认证）。
- **yup-dev 是 4c4g 小机器**：禁止其上编译大型源码树（曾因全量编译 prometheus 触发负载风暴）；重操作单实例、超 2 分钟挂后台轮询；Go 重编译挪本地做（本机 go1.26 可用）。
- **网络实测定性**（为什么分发长这样）：本机/yup-dev → docker.io/hub.docker.com 直连不通（yup-dev 的 daemon 有国内 mirror，pull 通 push 不通）；ACK kubelet → ghcr 无限挂起、docker.io 超时、ACR 干净；GitHub Actions runner → 全通（一切外部推送经 Actions）。
- **kubeconfig**：ACK 测试集群 `~/.kube/config/k8s-ali-bj-xp-test.kubeconfig`（注意带 .kubeconfig 后缀；同目录还有 prod 等多套别混用）。kind e2e 集群在 yup-dev（context kind-ssp-e2e2）。
- 凭证位置：ACR 登录态在本机 `~/.docker/config.json`（用户 bc小喻yup）；Docker Hub 用户 bcyup，PAT 在 GitHub secrets（DOCKERHUB_TOKEN）。

## 四、关键事实速查（踩过的坑，防重蹈）

1. **Docker Hub PAT 只能 CLI/registry 认证**，Hub REST API（建仓/设描述）不认——建仓靠 push 自动创建，仓库描述需 Hub 网页手动（≤100 字符放简述，链接放 Overview）。
2. **helm 预建 ns 路径**：chart 不渲染 Namespace；`--create-namespace` 在 OpenYurt ACK 上与 ns 管控有兼容问题（11 资源 ns not found）——统一 `kubectl create ns` 预建。
3. **gh-pages CDN 约 1-2 分钟延迟**：刚推的 chart 版本 helm 拉不到不是失败，等一会儿 + `helm repo update`。
4. **certwatcher 会漏 kubelet 的原子 symlink 切换**（文件已同步但不重载）——自签轮转后滚动 Deployment 兜底（已实现）；自签冷启动鸡生蛋已解（卷 optional:true + 同步 EnsureBootstrap + exit 重启）。
5. **RBAC 三次同族漂移事故**（清单改了集群没 apply）——helm upgrade 自动重 apply 已根治；裸清单路径记得手动。
6. **prometheusRule 双坑**：标签须匹配 ruleSelector（ACK: `prometheus: k8s`）且放 operator 监听的 ns（ruleNamespaceSelector nil = 仅自己 ns）。
7. KEDA 后装对 scheduler 不可见（启动期一次性 discovery 探测）——keda.sh 会自动检测并重启兜底。
8. e2e 脚本（matrix/chaos/keda）入口自动 re-apply RBAC；DEMO_IMAGE 是 ghcr 地址，大陆跑需改环境变量。
9. ACR 个人版无 OCI chart 支持（push chart manifest 403）、无 -vpc 端点、"公有"仓库不匿名。
10. kind load 多架构镜像 digest not found——`--platform linux/amd64` 重拉 + `docker save|ctr import`（无 --all-platforms）。
11. **GitHub Actions job 级 `if:` 不允许 `secrets` 上下文**（只认 vars/github 等公共上下文）——775efb2 起在 job 级引用 `secrets.DOCKERHUB_TOKEN`，整份 release.yaml 被 GitHub 判为无效文件：tag 不会触发任何 job、push main 每次产生 0s 失败 run（无日志可查，需从 run 页面 HTML 抓 "Invalid workflow file" 报错）。后果：chart 0.1.4–0.1.6 与 Docker Hub 镜像全靠手动补发，HANDOFF 09-30 版误记为「CI 绿/打 tag 即自动」。教训：**改 workflow 后别只看 ci.yaml 绿——push main 后 release.yaml 若出现 0s run 即文件无效**；token 判空放 step 级 fail-loud（已修，7198e2d）。
12. **chart 发布步首跑三连坑**（b89d4fe 新增该步时整份文件就是无效的，从未真正执行过，v0.2.0 首跑暴露，run 37741347699）：① `git config user.name` 在 workspace 目录执行而 commit 在 /tmp/repo——身份不生效，`Author identity unknown`；② 匿名 HTTPS push → `could not read Username`（exit 128）；③ 顶层 `contents: read` 无权推 gh-pages。修法（0e35379）：job 级 `permissions: contents: write`；clone URL 嵌 `x-access-token:${GITHUB_TOKEN}`；身份在 /tmp/repo 内配置；no-change 与失败显式分流，不再 `|| echo` 吞错。

## 五、仓库导航

```
service-spread-scheduler/
├── README.md            中文主文档：是什么/安装（含大陆 mirror）/values 表/进度
├── RUNBOOK.md           运维：Helm 安装、升级回滚、证书三路径、监控、已知问题
├── hack/deploy-checklist.md    部署验收清单（ACK 实测版）
├── hack/release-checklist.md   发版清单
├── charts/service-spread-scheduler/   Helm chart（crds/ + verbatim RBAC 副本 + 自签分支）
├── .github/workflows/   ci.yaml / release.yaml（ghcr+dockerhub+chart）/ dockerhub-sync.yml（手动同步用）
├── internal/spread/     核心：keys/state(COW+预占)/domain/plugin/lifecycle/replicatarget
├── internal/webhook/    准入 + selfcert.go（KEDA 式自签轮转）
├── api/v1alpha1 + apis/config/v1alpha1
├── config/              裸清单（manager/rbac/webhook/monitoring）
├── docs/design/         设计文档（design §14 验收标准、dev-design §9 偏差表 14 条）
└── hack/e2e/            kind e2e（bootstrap/matrix/chaos/keda/verify-observer）
```

## 六、里程碑全史（时间序，供追溯）

- **M1-M3**（2026-09-02）：CRD/webhook/args、状态机与调度域哈希、插件全扩展点——`go test -race` 全绿，kind e2e 矩阵 9 PASS + 混沌 4/4。
- **M4**（2026-09-24，多波）：观测器+告警+janitor/reconciler 装配；capacity_deficit 死信修复；告警聚合语义（max by）；events.k8s.io RBAC 缺口；生产清单（资源/探针/PDB/拓扑）；GitHub 推送；RBAC 契约测试（首跑抓 KEDA 规则缺口）+ CI。
- **e2e 基建**（2026-09-22）：kind ssp-e2e2、镜像链路、bootstrap；验收矩阵 9 PASS/3 SKIP。
- **v0.1.0-alpha.1 → P1 收尾**（09-24）：matrix case 12/13 补全、release.yaml、tag 发版全绿。
- **观测三层验证 + promtool**（09-28）：kind 真实例 Prometheus 4/4 规则加载求值。
- **ACK Step2 三项**（09-28）：cert-manager 证书链首验、ghcr 不可达定性、ruleSelector 双坑实证——大陆 ACR 分发链路打通（acr.5 全绿，排障链含 namespace 斜杠 fail-fast 等）。
- **观测补全**（09-28/29）：reservations 指标（quotaKey 首斜杠解析 bug 被 RED 抓住）+ node_pods/detail 调试开关全链 + README 中文重写。
- **自签证书**（09-29）：KEDA 式进程内轮转，四真 bug 全由真集群验收抓出修复（Secret type immutable / create 不可 name-scope / certwatcher symlink 漏事件 / 冷启动鸡生蛋死锁）。
- **Helm chart**（09-29/30）：全生命周期 ACK 实测；CRD 三方案演化终定 crds/ 目录；chart 安装缺陷根治（ns ownership / SA 竞态 / --create-namespace 兼容）；GitHub Pages chart 仓库 + CI 自动发布。
- **v0.1.0 → v0.1.1**（09-29）：正式发版，双仓镜像验证。
- **Docker Hub 分发演化**（09-30）：PAT 限制实证 → 双仓上线 → 0.1.4 实测大陆 kubelet 拉超时 → 0.1.5 回 ACR → **用户定 Plan A**：0.1.6 Docker Hub 唯一主管线 + 大陆节点 mirror 路线 + ACR 退役（b89d4fe/fc9472f，CI 绿）。
- **release.yaml 无效文件事故修复**（10-08）：09-30 08:47 起（775efb2）job 级 if 引用 secrets 使整份 workflow 失效，tag 发版静默瘫痪 8 天未察觉；chart 0.1.4–0.1.6/Docker Hub 镜像实为手动补发。修复 = job 级只留 vars 判断 + step 级 fail-loud token guard（7198e2d），push main 验证无 0s run、ci 绿（37718490860）。
- **v0.2.0 部分发版 + chart 步三坑修复 → v0.2.1 完整发版**（10-08）：v0.2.0（1edbec1）镜像双架构推 ghcr 成功，但 chart 步首跑即挂（身份作用域/匿名 push/权限不足三连坑，§四.12，修复 0e35379）。v0.2.1 tag 后管线首次完整自动跑通：release 5m55s ✓ + push-dockerhub 30s ✓（run 37743284488）；Docker Hub v0.2.1 digest 与 CI push 日志逐字节一致，chart 0.2.1 落 gh-pages。大陆 mirror 实测为下一未验项。

（各阶段完整细节、证据与提交号见 git log 与本文件历史版本；设计取舍看两份设计文档。）
