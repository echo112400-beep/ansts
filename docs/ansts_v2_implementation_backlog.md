# ANSTS v2.2 工程研发全景任务规划表 (Master Implementation Backlog & Next-Phase Roadmap)

| 文档属性 (Document Metadata) | 详细内容 (Details) |
| :--- | :--- |
| **项目名称 (Project)** | **Agent-Native Sandbox Task Scheduler (ANSTS v2.2 — `celld` HA & Scale-Out Edition)** |
| **当前代码仓基线 (Baseline)** | `main @ d2a2aa1`（45 个 TypeScript 模块编译通过，**M1 ~ M10 共 52 项任务已实现**，**37/37** 项集成/混沌/高可用/横向扩容测试 100% 通过） |
| **待实现规划范围 (Pending Scope)** | **M10 ~ M13（`TASK-49` ~ `TASK-64`，共 4 大阶段、16 项待实现工程任务）**：聚焦热路径全链路装配、真实协议栈驱动、Linux 内核/容器原语落地、K8s/RustFS 生产编排及 MCP/FinOps 治理 |
| **核心模型池 (Model Pools)** | `DeepSeek`（旗舰深度推理池）、`GLM-5.3`（多步工具编排与轻量高速分流池）、`Qwen-3.8`（高频语法修复与高可用热备容灾池） |

---

## 1. 研发阶段与里程碑全景总览 (Milestones Overview: M1 ~ M13)

| 阶段里程碑 (Milestone) | 阶段核心目标 | 涉及核心模块 / 目录 | 任务编号范围 | 优先级 | 当前状态 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **M1：契约驱动、接入网关与在线贝叶斯定容** | HTTP/SSE 网关、`TaskSpec v1beta1` 校验、KV 幂等、L3 429 背压与共轭贝叶斯画像 | `src/gateway/*`<br>`src/profiler.ts` | `TASK-01` ~ `TASK-05` | `P0` | ✅ **已完成** |
| **M2：多租分级队列、5D W-DRF 调度与 CAS 租约** | 三级队列树、Aging+EDF 零饥饿、DAG 配额支票、5D W-DRF 调度与 `PreemptToHibernate` | `src/queue_manager.ts`<br>`src/scheduler.ts`<br>`src/dag/escrow.ts` | `TASK-06` ~ `TASK-11` | `P0` | ✅ **已完成** |
| **M3：Turn 级动态认知路由与四级工具 Saga 网关** | `DeepSeek`/`GLM-5.3`/`Qwen-3.8` 路由、令牌桶、三态熔断、四级 Saga 补偿与审批门禁 | `src/cognitive_router.ts`<br>`src/saga/*` | `TASK-12` ~ `TASK-17` | `P0` | ✅ **已完成** |
| **M4：相变弹性沙箱守护与沙箱内 Harness** | Warm Pool、`0.1C <-> 8.0C` 双模脉冲、20 并发 Semaphore、PSI 守护、VSOCK 与速度断路器 | `src/runtime/*`<br>`src/harness/*` | `TASK-18` ~ `TASK-23` | `P0` | ✅ **已完成** |
| **M5：TurnWAL 增量快照、时光机重放与事件通知** | `TurnWAL` 引擎、L0->L1 合并、按需分页恢复、零 Token 时光机重放与 CloudEvents 发件箱 | `src/checkpoint/*`<br>`src/telemetry/*` | `TASK-24` ~ `TASK-28` | `P1` | ✅ **已完成** |
| **M6：四级双向闭环背压、实时看板与混沌基准** | 四级闭环背压、`anstsctl` CLI、实时仿真看板服务端与 5 大混沌工程基准测试 | `src/cluster/*`<br>`src/cli/*`<br>`src/dashboard/*` | `TASK-29` ~ `TASK-32` | `P1` | ✅ **已完成** |
| **M7：TypeScript 全量迁移、`celld`/RustFS 集成与加固** | 全仓 TypeScript 化、`wrangler.json`、`RustFS` 启动标本及 5 项设计完整性/代码走查加固 | `src/cloudflare/*`<br>`deploy/rustfs-celld/*` | `TASK-33` ~ `TASK-40` | `P0` | ✅ **已完成** |
| **M8：基于 `celld.dev/docs` 的持久化可恢复高可用引擎** | `RPO = 0` 双模式证明、写游标保护、`DurableWorkflowContext`、分片队列、安全 Epoch GC 与自愈 | `src/cloudflare/celld_ha_engine.ts` | `TASK-41` ~ `TASK-45` | `P0` | ✅ **已完成** |
| **M9：高并发横向扩容引擎 (Scale-Out Engine)** | 双层分片调度器（P2C + 热点租户分裂 + Gossip 配额）、动态队列自动扩分片与脑体分离扩容 | `src/cloudflare/horizontal_scaler.ts` | `TASK-46` ~ `TASK-48` | `P0` | ✅ **已完成** |
| **M10：热路径全链路装配与真实存储/模型协议栈对接** | 打通边缘 Worker -> 分片调度/队列 -> 真实 `RustFS` S3 LTX 流驱动 -> 真实 LLM SSE 客户端 | `src/cloudflare/*`<br>`src/checkpoint/s3_ltx_driver.ts`<br>`src/router/live_llm_adapter.ts` | **`TASK-49` ~ `TASK-52`** | **`P0`** | ✅ **已完成** |
| **M11：Linux 宿主机内核原语与容器/MicroVM 运行时落地** | 对接真实 `/sys/fs/cgroup`、`/proc/pressure`、Containerd/Firecracker API、VSOCK 与 `<50ms` 热迁移 | `src/runtime/linux_cgroup_psi.ts`<br>`src/runtime/container_driver.ts` | **`TASK-53` ~ `TASK-56`** | **`P0 / P1`** | 📋 **待实现** |
| **M12：Kubernetes 编排器、`RustFS` 纠删码与真机混沌演练** | K8s Operator & HPA Adapter、`RustFS` 多机 EC 集群、OTLP/Prometheus 导出器与 Toxiproxy 混沌测试 | `deploy/k8s-operator/*`<br>`src/telemetry/otel_prometheus.ts` | **`TASK-57` ~ `TASK-60`** | **`P1`** | 📋 **待实现** |
| **M13：MCP 生态协议治理、FinOps 预算硬顶与多活联邦** | 标准 MCP 工具动态注册与事务绑定、租户预算硬顶熔断、分片热力图控制台与多 Region 双活路由 | `src/saga/mcp_registry.ts`<br>`src/finops/*`<br>`src/cloudflare/federation_router.ts` | **`TASK-61` ~ `TASK-64`** | **`P1 / P2`** | 📋 **待实现** |

---

## 2. 已完成里程碑基线速览 (M1 ~ M9, `TASK-01` ~ `TASK-48` — 33/33 CI Tests Passing)

- [x] **M1 ~ M6 (`TASK-01` ~ `TASK-32`)**：完成网关校验、幂等去重、SSE 信用流控、贝叶斯画像、三级队列树、Aging+EDF、DAG 配额支票、5D W-DRF、`PreemptToHibernate`、认知路由（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）、四级 Saga 网关、Cgroups 相变控制、PSI 守卫、VSOCK 网桥、三层死循环/速度/滥用熔断器、`TurnWAL`、时光机重放、CloudEvents Outbox、四级闭环背压与 `anstsctl` CLI。
- [x] **M7 (`TASK-33` ~ `TASK-40`)**：完成全量 TypeScript 迁移、`celld` / Cloudflare Workers (`worker.ts`, `scheduler_cell.ts`, `harness_cell.ts`)、`wrangler.json`、D1 迁移、`RustFS` + `celld --bucket` 启动标本及 10 项审计与规范完整性加固。
- [x] **M8 (`TASK-41` ~ `TASK-45`)**：基于 `celld.dev/docs` 完成 `RpoZeroDurabilityEngine`（Fleet `fsync` 封存 + Bucket 二次复核 `ownership.json` + 写游标完整性保护）、`DurableWorkflowContext`（`step.do` 缓存、提前事件缓冲、`>1h` 休眠释放 Isolate）、`ShardedQueueBrokerPool`（突破 256 并发上限 + 显式指数退避 + DLQ）、`SafeEpochGarbageCollector`（`retired.json` + 三级保留护栏）与 `FleetHaAndAutoscalerController`（`<=32` 休眠 Cell 限速重平衡 + `handoff=preserve`）。
- [x] **M9 (`TASK-46` ~ `TASK-48`)**：完成 `PartitionedSchedulerRouter`（一致性哈希 + 热点租户自适应分裂 + P2C 路由 + 无锁配额 Gossip 同步）、`DynamicQueueAutoSharder`（水位超 75% 动态倍增分片）与 `BrainBodyContainerPoolScaler`（~30:1 脑体复用与 PSI 感知宿主机扩容）。

---

## 3. 待实现研发任务详细规划 (Pending Implementation Roadmap: M10 ~ M13)

### 阶段十（M10）：热路径全链路装配与真实存储/模型协议栈对接 (`P0` — ✅ 已完成，37/37 CI 测试通过)

> **核心目标**：将 M8（高可用持久化）与 M9（横向扩容分片引擎）接入 `worker.ts`、`scheduler_cell.ts` 和 `harness_cell.ts` 主干热路径，并补齐面向真实 `RustFS` S3 与真实大模型 API 的网络协议驱动。

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-49`** | **边缘 Worker 与分片调度/动态队列/高可用引擎主干热路径全链路装配** | `src/cloudflare/worker.ts`<br>`src/cloudflare/scheduler_cell.ts`<br>`src/cloudflare/harness_cell.ts` | `P0` | `TASK-41`~`48` | - 改造 `worker.ts`：移除硬编码单点 `idFromName("global-cluster-01")`，接入 `PartitionedSchedulerRouter`（按租户亲和或热点 P2C 路由至 `scheduler-cell-shard-00..K`）与 `DynamicQueueAutoSharder`<br>- 改造 `harness_cell.ts`：在事务提交路径挂载 `RpoZeroDurabilityEngine`，在长周期多步任务入口挂载 `DurableWorkflowContext`<br>- **DoD**：端到端集成测试验证从 Edge Worker 提交 500 并发任务自动分流至多调度分片与多队列分片，且单分片故障 `RPO = 0` 无损恢复 |
| **`TASK-50`** | **真实 `RustFS` S3 协议客户端与 LiteFS LTX 增量帧流式同步驱动** | `src/checkpoint/s3_ltx_driver.ts` | `P0` | `TASK-24`<br>`TASK-41` | - 实现零外部重依赖的轻量级 S3 HTTP 客户端（支持 AWS Signature V4 签名、`PUT` 条件头 `If-Match` / `If-None-Match` 与 `GET Range` 按字节区间拉取）<br>- 实现 `ownership.json` CAS 抢占续期、`000001..N.ltx` 增量事务流上传、`retired.json` 原子更新及冷启动热页优先分页拉取（Paged Restore）<br>- **DoD**：对接本地 Mock S3 / `RustFS` `:9000` 端点，验证并发 CAS 抢占、`RPO = 0` 二次复核与 Epoch GC 完整闭环 |
| **`TASK-51`** | **`celld` 内部管理端口 (`:9001`) 控制面客户端与滚动发布编排器** | `src/cloudflare/celld_admin_client.ts`<br>`src/cli/anstsctl.ts` | `P0` | `TASK-45` | - 对接 `celld` 内部管理 API：周期拉取 `GET /state` 与 Bucket 中的 `fleet/capacity-v1.json`，触发 `POST /shutdown?handoff=preserve` 原地保留租约关机<br>- 在 `anstsctl` CLI 中新增 `anstsctl fleet status`、`anstsctl fleet rebalance` 与 `anstsctl fleet rolling-restart`（自动轮询 `restoring == 0` 门禁）子命令<br>- **DoD**：模拟 3 节点滚动升级，验证 CLI 自动等待每台节点 `restoring == 0` 后再重启下一节点，全程 0 任务中断 |
| **`TASK-52`** | **`DeepSeek` / `GLM-5.3` / `Qwen-3.8` 真实 OpenAI-Compatible SSE 流式适配器** | `src/router/live_llm_adapter.ts` | `P0` | `TASK-12`<br>`TASK-39` | - 实现支持连接池复用、超时中断（`AbortSignal`）、首字延迟（TTFT）精确计时及 `reasoning_content`（思维链 CoT）与 `content` 分離解析的真实 SSE 客户端<br>- 支持配置环境变量切换 `LIVE_API_MODE` 与 `DETERMINISTIC_MOCK_MODE`，并将真实 Token 消耗实时写入双时钟账本<br>- **DoD**：通过本地 HTTP SSE 流服务器联调测试，验证流式 Chunk 解析、429 瞬时切换 `Qwen-3.8` 及中断清理零连接泄漏 |

---

### 阶段十一（M11）：Linux 宿主机内核原语与容器/MicroVM 运行时落地 (`P0 / P1` — 第二顺位冲刺)

> **核心目标**：将宿主机守护进程 (`node_daemon.ts`) 中的 Cgroups v2、Linux PSI、容器生命周期与 VSOCK 通信从内存状态机推进至可操作真实 Linux 内核接口与容器引擎的生产驱动。

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-53`** | **真实 Linux Cgroups v2 (`/sys/fs/cgroup`) 与 PSI (`/proc/pressure/*`) 驱动器** | `src/runtime/linux_cgroup_psi.ts` | `P0` | `TASK-19`<br>`TASK-20` | - 实现 `/sys/fs/cgroup/ansts/<sandbox_id>/` 目录树的自动创建与写入：控制 `cpu.max`（`10000 100000` 思考期 $\leftrightarrow$ `800000 100000` 脉冲期）、`memory.high`、`memory.max`、`pids.max` 与 `cgroup.freeze`<br>- 实现 `/proc/pressure/cpu` 与 `/proc/pressure/memory` 实时读取解析器（在非 Linux 或非特权环境自动平滑降级为虚拟文件系统适配器）<br>- **DoD**：在临时挂载的虚拟 CgroupFS / procfs 目录树下验证相变脉冲写入值、PIDs 限制与 Freeze 冻结指令 100% 精确 |
| **`TASK-54`** | **Companion Container / Firecracker 真实沙箱生命周期与 OverlayFS 快照驱动** | `src/runtime/container_driver.ts` | `P0` | `TASK-18`<br>`TASK-34` | - 实现统一 `ContainerRuntimeDriver`：支持调用 `celld` 原生 `ctx.container` 接口、Docker Engine Unix Socket (`/var/run/docker.sock`) 或 Firecracker API Socket<br>- 实现沙箱工作目录 OverlayFS `upperdir` 差异打包与恢复，配合 `TurnWAL` 实现文件级精确回滚<br>- **DoD**：验证容器冷启动、Warm Pool 预热池认领、命令超时强杀及工作区增量快照回滚 |
| **`TASK-55`** | **宿主机 `AF_VSOCK` / Unix Socket 零信任双向代理守护进程与出网白名单** | `src/runtime/vsock_daemon.ts` | `P1` | `TASK-21` | - 基于真实 Unix Domain Socket / `AF_VSOCK` 建立宿主机 `ansts-node-daemon` 与沙箱内 `agent-harnessd` 的全双工二进制帧信道（`Length-Prefixed Frame + CRC32`）<br>- 沙箱内完全无外网网卡与 API Key，所有 LLM 请求与 MCP 工具调用经由 Socket 代理并在宿主机侧完成 `X-Agent-Fence` 注入与域名白名单校验<br>- **DoD**：真实双进程 Socket 通信压测（1,000 帧吞吐延迟 `< 2ms`），非法域名与过期 Epoch 100% 拦截 |
| **`TASK-56`** | **重型脉冲任务跨机实时热迁移 (`<50ms` Ephemeral Heavy Executor Handover)** | `src/runtime/live_migration.ts` | `P1` | `TASK-54`<br>`TASK-55` | - 实现当本地宿主机 `node_burst_semaphore (20)` 满载或 PSI 告警时，将待执行的重型编译/测试脉冲轮次打包（增量 `TurnWAL` + OverlayFS 差异层）通过内部高速通道流式传输至空闲 `Heavy Executor` 节点执行<br>- 执行完成后将产物差异块与 stdout/stderr 流式回传原 `AgentTaskHarnessCell`<br>- **DoD**：双节点模拟压测下，本地满载时自动触发跨节点卸载，切换开销 `< 50ms` 且文件状态完全一致 |

---

### 阶段十二（M12）：Kubernetes 生产编排器、`RustFS` 纠删码与真机混沌演练 (`P1` — 第三顺位冲刺)

> **核心目标**：提供云原生生产环境的一键声明式部署、基于 `celld` 容量指标的自动扩缩容控制器、全链路可观测性大盘以及自动化混沌验收流水线。

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-57`** | **Kubernetes Operator (`CelldFleet` CRD) 与自定义 HPA 弹性伸缩适配器** | `deploy/k8s-operator/*`<br>`src/cluster/k8s_autoscaler.ts` | `P1` | `TASK-45`<br>`TASK-48` | - 编写 Kubernetes StatefulSet / Deployment 清单、PodDisruptionBudget (`PDB`) 以及 `PreStop` Hook（自动调用 `POST /shutdown?handoff=preserve`）<br>- 实现消费 `fleet/capacity-v1.json` 与 `BrainBodyContainerPoolScaler` 计划的 K8s Custom Metrics HPA 适配器，按活跃脉冲数与内存余量自动扩缩 `celld` 脑节点与沙箱躯体节点<br>- **DoD**：校验 K8s YAML 清单结构完整性与扩缩容控制器在突发流量下的副本数收敛单测 |
| **`TASK-58`** | **`RustFS` 分布式多节点纠删码（Erasure Coding）部署清单与跨区容灾同步** | `deploy/rustfs-celld/docker-compose.rustfs-ha.yml`<br>`deploy/rustfs-celld/rustfs-ec-setup.sh` | `P1` | `TASK-36`<br>`TASK-50` | - 升级单机版 `RustFS` 启动标本为 **4 节点 $\times$ 4 NVMe 盘（共 16 驱动器）分布式纠删码（EC:4）集群**，容忍任意 2 台存储节点同时宕机而不中断 S3 CAS 与 LTX 读写<br>- 配置 Bucket 生命周期规则与跨可用区异步镜像复制策略<br>- **DoD**：自动化脚本校验多节点 EC 挂载参数、健康检查探针与权限隔离配置 |
| **`TASK-59`** | **OpenTelemetry (OTLP) 链路追踪、Prometheus `/metrics` 导出器与 Grafana 大盘** | `src/telemetry/otel_prometheus.ts`<br>`deploy/observability/grafana-dashboard.json` | `P1` | `TASK-27` | - 实现标准 Prometheus 文本格式指标导出器（暴露 5D W-DRF 主导份额、分片队列深度、模型池 TTFT/429 率、Cgroups 相变耗时、`RPO = 0` 同步延迟与 Epoch GC 计数）<br>- 提供开箱即用的 Grafana 生产监控大盘 JSON 定义与告警规则（Alertmanager Rules）<br>- **DoD**：验证 `GET /metrics` 输出符合 OpenMetrics 标准且覆盖全部核心 SLA 指标 |
| **`TASK-60`** | **多节点网络分区与脑裂注入端到端混沌测试套件 (`E2E Chaos harness`)** | `tests/e2e_chaos_cluster.test.ts` | `P1` | `TASK-50`<br>`TASK-51` | - 构建多节点 `celld` + Mock `RustFS` 混沌仿真器：注入对称/非对称网络分区、GC 停顿（Stale Leader Pause）、S3 间歇性 `503 SlowDown` 与双节点同时抢占同一 Cell<br>- **DoD**：100% 验证在任意故障组合下 `RPO = 0` 已确认事务零丢失、僵尸节点写请求 100% 被拦截、集群在 `< 10s` 内自愈收敛 |

---

### 阶段十三（M13）：MCP 生态协议治理、FinOps 预算硬顶与多区域双活联邦 (`P1 / P2` — 第四顺位冲刺)

> **核心目标**：完善面向企业级多租户商业化运营的工具生态接入、实时成本风控、可视化分片拓扑管控以及跨地域容灾能力。

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-61`** | **Model Context Protocol (MCP) 工具动态注册中心与四级事务契约绑定** | `src/saga/mcp_registry.ts` | `P1` | `TASK-15`~`17` | - 支持通过 JSON-RPC 2.0 动态连接外部 MCP Server，拉取工具 JSON Schema 并绑定 ANSTS 四级事务属性（`PURE_READ`、`IDEMPOTENT_WRITE`、`SAGA_COMPENSATABLE`、`STAGED_COMMIT`）<br>- 对未声明逆向补偿函数的外部写工具强制降级为 `STAGED_COMMIT`（需人工或策略审批后提交）<br>- **DoD**：验证动态注册 MCP 工具、Schema 参数校验、自动生成逆向补偿计划与审批拦截测试 |
| **`TASK-62`** | **多租户 FinOps 实时成本归因账本与日/月预算硬顶熔断器** | `src/finops/budget_controller.ts` | `P1` | `TASK-40`<br>`TASK-52` | - 实时汇总租户在三大模型池（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）的输入/CoT/输出 Token 费用与沙箱活跃 CPU/RAM 秒级计费<br>- 支持配置租户级日/月预算软告警水位（`80%`）与硬熔断水位（`100%`）：触达硬顶时自动将运行中任务转入 `GRACEFUL_DRAINING` 保存快照并暂停新任务准入<br>- **DoD**：验证多任务并发消费下预算扣减精度、80% 告警事件触发与 100% 硬顶平滑排水零超支 |
| **`TASK-63`** | **实时管控台 (`dashboard`) 升级：分片拓扑热力图、Workflow 追踪与在线审批** | `src/dashboard/server.ts` | `P2` | `TASK-31`<br>`TASK-49` | - 升级后端看板 API 与前端视图：新增 `SchedulerCell` 分片负载热力图、动态队列分片水位监控、`DurableWorkflowContext` 步骤时间轴以及 `STAGED_COMMIT` 在线批准/驳回按钮<br>- **DoD**：验证通过控制台 API 完成高危工具在线审批唤醒沙箱，并实时获取分片扩容状态 |
| **`TASK-64`** | **多区域双活联邦调度器（Multi-Region Active-Active Federation）** | `src/cloudflare/federation_router.ts` | `P2` | `TASK-49` | - 支持跨多个地理区域（如 `us-west`、`eu-central`、`ap-east`）部署独立的 `celld` + `RustFS` 集群，边缘层按数据驻留合规策略（Data Residency）、区域队列水位与跨区 RTT 动态路由<br>- 当单一 Region 发生整体级故障或大模型池区域性中断时，自动将无状态/可迁移任务切流至备选 Region<br>- **DoD**：模拟单一 Region 整体不可用，验证边缘联邦路由器在 `< 1s` 内完成跨区切流与配额重平衡 |

---

## 4. 推荐研发推进路线与执行建议

1. **第一批立即启动（M10：`TASK-49` ~ `TASK-52`）**：优先把已实现的横向扩容路由器 (`PartitionedSchedulerRouter`、`DynamicQueueAutoSharder`) 和 `RPO = 0` 持久化引擎 (`RpoZeroDurabilityEngine`) 正式装配进 `worker.ts` / `scheduler_cell.ts` / `harness_cell.ts` 主干链路，并完成 `s3_ltx_driver.ts` 与 `live_llm_adapter.ts`。
2. **第二批推进（M11：`TASK-53` ~ `TASK-56`）**：落地 Linux Cgroups v2 / PSI 真实内核接口驱动、Companion Container 生命周期驱动与 `AF_VSOCK` 双向代理。
3. **第三批与第四批推进（M12 ~ M13：`TASK-57` ~ `TASK-64`）**：补齐 K8s Operator、`RustFS` 16 盘纠删码集群、OpenTelemetry/Prometheus 监控导出、MCP 工具注册中心与 FinOps 成本硬顶治理。
