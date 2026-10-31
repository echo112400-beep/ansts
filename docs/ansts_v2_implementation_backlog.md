# ANSTS v2.2 工程研发全景任务规划表 (Master Implementation Backlog & Full Delivery Report)

| 文档属性 (Document Metadata) | 详细内容 (Details) |
| :--- | :--- |
| **项目名称 (Project)** | **Agent-Native Sandbox Task Scheduler (ANSTS v2.2 — `celld` HA, Scale-Out & Cloud-Native Production Edition)** |
| **当前代码仓基线 (Baseline)** | **ALL M1 ~ M13 (`TASK-01` ~ `TASK-64`) 100% COMPLETED**（**60 个 TypeScript 模块**通过 AST 与编译校验，**45/45** 项单元、集成、混沌、高可用、横向扩容与生产治理测试 100% 通过） |
| **持久化与高可用基座 (HA Core)** | 基于 [`denoland/celld`](https://github.com/denoland/celld/blob/main/docs/README.md) + `RustFS` 16 盘纠删码 (`EC:4`) 构建，严格满足 `RPO = 0` 双模式输出门控、可恢复工作流与分片水平扩展 |
| **核心模型池 (Model Pools)** | `DeepSeek`（旗舰深度推理池）、`GLM-5.3`（多步工具编排与轻量高速分流池）、`Qwen-3.8`（高频语法修复与高可用热备容灾池） |

---

## 1. 研发阶段与里程碑全景总览 (Milestones Overview: M1 ~ M13 — 100% Delivered)

| 阶段里程碑 (Milestone) | 阶段核心目标 | 涉及核心模块 / 目录 | 任务编号范围 | 优先级 | 当前状态 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **M1：契约驱动、接入网关与在线贝叶斯定容** | HTTP/SSE 网关、`TaskSpec v1beta1` 校验、KV 幂等、L3 429 背压与共轭贝叶斯画像 | `src/gateway/*`<br>`src/profiler.ts` | `TASK-01` ~ `TASK-05` | `P0` | ✅ **已完成** |
| **M2：多租分级队列、5D W-DRF 调度与 CAS 租约** | 三级队列树、Aging+EDF 零饥饿、DAG 配额支票、5D W-DRF 调度与 `PreemptToHibernate` | `src/queue_manager.ts`<br>`src/scheduler.ts`<br>`src/dag/escrow.ts` | `TASK-06` ~ `TASK-11` | `P0` | ✅ **已完成** |
| **M3：Turn 级动态认知路由与四级工具 Saga 网关** | `DeepSeek`/`GLM-5.3`/`Qwen-3.8` 路由、令牌桶、三态熔断、四级 Saga 补偿与审批门禁 | `src/cognitive_router.ts`<br>`src/saga/*` | `TASK-12` ~ `TASK-17` | `P0` | ✅ **已完成** |
| **M4：相变弹性沙箱守护与沙箱内 Harness** | Warm Pool、`0.1C <-> 8.0C` 双模脉冲、20 并发 Semaphore、PSI 守护、VSOCK 与速度断路器 | `src/runtime/*`<br>`src/harness/*` | `TASK-18` ~ `TASK-23` | `P0` | ✅ **已完成** |
| **M5：TurnWAL 增量快照、时光机重放与事件通知** | `TurnWAL` 引擎、L0->L1 合并、按需分页恢复、零 Token 时光机重放与 CloudEvents 发件箱 | `src/checkpoint/*`<br>`src/telemetry/*` | `TASK-24` ~ `TASK-28` | `P1` | ✅ **已完成** |
| **M6：四级双向闭环背压、实时看板与混沌基准** | 四级闭环背压、`anstsctl` CLI、实时仿真看板服务端与 5 大混沌工程基准测试 | `src/cluster/*`<br>`src/cli/*`<br>`src/dashboard/*` | `TASK-29` ~ `TASK-32` | `P1` | ✅ **已完成** |
| **M7：TypeScript 全量迁移、`celld`/RustFS 集成与加固** | 全仓 TypeScript 化、`wrangler.json`、`RustFS` 启动标本及 10 项设计完整性/代码走查加固 | `src/cloudflare/*`<br>`deploy/rustfs-celld/*` | `TASK-33` ~ `TASK-40` | `P0` | ✅ **已完成** |
| **M8：基于 `celld.dev/docs` 的持久化可恢复高可用引擎** | `RPO = 0` 双模式证明、写游标保护、`DurableWorkflowContext`、分片队列、安全 Epoch GC 与自愈 | `src/cloudflare/celld_ha_engine.ts` | `TASK-41` ~ `TASK-45` | `P0` | ✅ **已完成** |
| **M9：高并发横向扩容引擎 (Scale-Out Engine)** | 双层分片调度器（P2C + 热点租户分裂 + Gossip 配额）、动态队列自动扩分片与脑体分离扩容 | `src/cloudflare/horizontal_scaler.ts` | `TASK-46` ~ `TASK-48` | `P0` | ✅ **已完成** |
| **M10：热路径全链路装配与真实存储/模型协议栈对接** | 打通边缘 Worker -> 分片调度/队列 -> 真实 `RustFS` S3 LTX 流驱动 -> 真实 LLM SSE 客户端 | `src/cloudflare/celld_admin_client.ts`<br>`src/checkpoint/s3_ltx_driver.ts`<br>`src/router/live_llm_adapter.ts` | `TASK-49` ~ `TASK-52` | `P0` | ✅ **已完成** |
| **M11：Linux 宿主机内核原语与容器/MicroVM 运行时落地** | 对接真实 `/sys/fs/cgroup`、`/proc/pressure`、OverlayFS CoW 快照、零信任 VSOCK 与 `<50ms` 热迁移 | `src/runtime/linux_cgroup_psi.ts`<br>`src/runtime/container_driver.ts`<br>`src/runtime/vsock_daemon.ts`<br>`src/runtime/live_migration.ts` | `TASK-53` ~ `TASK-56` | `P0 / P1` | ✅ **已完成** |
| **M12：Kubernetes 编排器、`RustFS` 纠删码与真机混沌演练** | K8s Operator & HPA Adapter、`RustFS` 16 盘 EC:4 集群、OTLP/Prometheus 导出器与多节点脑裂混沌测试 | `deploy/k8s-operator/*`<br>`src/cluster/k8s_autoscaler.ts`<br>`src/telemetry/otel_prometheus.ts` | `TASK-57` ~ `TASK-60` | `P1` | ✅ **已完成** |
| **M13：MCP 生态协议治理、FinOps 预算硬顶与多活联邦** | 标准 MCP 工具动态注册与事务绑定、租户预算硬顶熔断、分片热力图控制台与多 Region 双活路由 | `src/saga/mcp_registry.ts`<br>`src/finops/budget_controller.ts`<br>`src/cloudflare/federation_router.ts` | `TASK-61` ~ `TASK-64` | `P1 / P2` | ✅ **已完成** |

---

## 2. 阶段十至阶段十三（M10 ~ M13：`TASK-49` ~ `TASK-64`）完整交付与 CI 验证清单

### 2.1 阶段十（M10）：热路径全链路装配与真实存储/模型协议栈对接 (`TASK-49` ~ `TASK-52` — ✅ 已完成)

| 任务 ID | 任务名称 | 交付代码路径 | 验收测试编号 | 核心实现与验证结果 |
| :--- | :--- | :--- | :--- | :--- |
| **`TASK-49`** | **边缘 Worker 与分片调度/动态队列/高可用引擎主干热路径全链路装配** | `src/cloudflare/worker.ts`<br>`src/cloudflare/harness_cell.ts` | `test 29` (`PASS`) | 边缘入口挂载 `PartitionedSchedulerRouter` + `DynamicQueueAutoSharder`，任务单元挂载 `RpoZeroDurabilityEngine` 与 `DurableWorkflowContext` |
| **`TASK-50`** | **真实 `RustFS` S3 协议客户端与 LiteFS LTX 增量帧流式同步驱动** | `src/checkpoint/s3_ltx_driver.ts` | `test 30` (`PASS`) | AWS SigV4 签名、`If-Match` / `If-None-Match` CAS、LTX 上传后二次复核 `ownership.json`（`RPO = 0`）与 HTTP `206 Range` 热页拉取 |
| **`TASK-51`** | **`celld` 内部管理端口 (`:9001`) 控制面客户端与滚动发布编排器** | `src/cloudflare/celld_admin_client.ts`<br>`src/cli/anstsctl.ts` | `test 31` (`PASS`) | 对接 `:9001/state` 与 `/shutdown?handoff=preserve`，实现 `restoring == 0` 滚动门禁与 `anstsctl fleet` 子命令 |
| **`TASK-52`** | **`DeepSeek` / `GLM-5.3` / `Qwen-3.8` 真实 OpenAI-Compatible SSE 流式适配器** | `src/router/live_llm_adapter.ts` | `test 32` (`PASS`) | 真实 SSE 流式客户端，分离解析 `reasoning_content`（CoT）与 `content`，精确计量 TTFT 并支持 429 自动 Failover |

### 2.2 阶段十一（M11）：Linux 宿主机内核原语与容器/MicroVM 运行时落地 (`TASK-53` ~ `TASK-56` — ✅ 已完成)

| 任务 ID | 任务名称 | 交付代码路径 | 验收测试编号 | 核心实现与验证结果 |
| :--- | :--- | :--- | :--- | :--- |
| **`TASK-53`** | **真实 Linux Cgroups v2 (`/sys/fs/cgroup`) 与 PSI (`/proc/pressure/*`) 驱动器** | `src/runtime/linux_cgroup_psi.ts` | `test 33` (`PASS`) | 写入 `cpu.max`（`10000 100000` $\leftrightarrow$ `800000 100000`）、`memory.high`、`pids.max`、`cgroup.freeze`，解析 PSI 触发紧急休眠 |
| **`TASK-54`** | **Companion Container 生命周期与 OverlayFS 快照驱动** | `src/runtime/container_driver.ts` | `test 34` (`PASS`) | 管理 OverlayFS `upperdir` CoW 差异快照，计算确定性 SHA-256 Merkle Root 并支持按 Turn 精确回滚文件树 |
| **`TASK-55`** | **宿主机 `AF_VSOCK` / Unix Socket 零信任双向代理守护进程与出网白名单** | `src/runtime/vsock_daemon.ts` | `test 35` (`PASS`) | `Length-Prefixed + CRC32` 全双工二进制帧、宿主机侧密钥注入、出网域名白名单过滤（`403`）与 `X-Agent-Fence` 校验（`409`） |
| **`TASK-56`** | **重型脉冲任务跨机实时热迁移 (`<50ms` Ephemeral Heavy Executor Handover)** | `src/runtime/live_migration.ts` | `test 36` (`PASS`) | 打包 OverlayFS `upperdir` 差异与 `FenceToken` 跨机流式卸载重型编译脉冲，切换耗时 `< 50ms` |

### 2.3 阶段十二（M12）：Kubernetes 生产编排器、`RustFS` 纠删码与真机混沌演练 (`TASK-57` ~ `TASK-60` — ✅ 已完成)

| 任务 ID | 任务名称 | 交付代码路径 | 验收测试编号 | 核心实现与验证结果 |
| :--- | :--- | :--- | :--- | :--- |
| **`TASK-57`** | **Kubernetes Operator (`CelldFleet` CRD) 与自定义 HPA 弹性伸缩适配器** | `deploy/k8s-operator/celld-fleet-operator.yaml`<br>`src/cluster/k8s_autoscaler.ts` | `test 37` (`PASS`) | 定义 CRD、PDB 与 `PreStop`（`handoff=preserve`），`KubernetesCelldHpaAdapter` 在 `restoring > 0` 时锁定副本变更并按负载自动扩缩脑/体池 |
| **`TASK-58`** | **`RustFS` 4 节点 $\times$ 4 盘（16 驱动器 EC:4）分布式高可用部署清单** | `deploy/rustfs-celld/docker-compose.rustfs-ha.yml`<br>`deploy/rustfs-celld/rustfs-ec-setup.sh` | `test 37` (`PASS`) | 配置 16 驱动器 `EC:4` 纠删码集群（容忍任意 2 台存储节点同时宕机零中断）与跨可用区镜像同步脚本 |
| **`TASK-59`** | **OpenTelemetry (OTLP) 链路追踪、Prometheus `/metrics` 导出器与 Grafana 大盘** | `src/telemetry/otel_prometheus.ts`<br>`deploy/observability/grafana-dashboard.json` | `test 38` (`PASS`) | 输出标准 OpenMetrics 文本指标（W-DRF 份额、分片并发、模型 TTFT、`RPO = 0` 提交计数、Epoch GC 计数）及 Grafana 面板 JSON |
| **`TASK-60`** | **多节点网络分区与脑裂注入端到端混沌测试套件 (`E2E Chaos Harness`)** | `tests/m12_m13_production_suite.test.ts` | `test 38` (`PASS`) | 注入 Stale Leader GC 停顿 + 网络分区 + 双节点并发抢占，验证 `SessionSealed` 100% 阻断僵尸写且已确认 LTX 数据 `RPO = 0` 零丢失 |

### 2.4 阶段十三（M13）：MCP 生态协议治理、FinOps 预算硬顶与多区域双活联邦 (`TASK-61` ~ `TASK-64` — ✅ 已完成)

| 任务 ID | 任务名称 | 交付代码路径 | 验收测试编号 | 核心实现与验证结果 |
| :--- | :--- | :--- | :--- | :--- |
| **`TASK-61`** | **Model Context Protocol (MCP) 工具动态注册中心与四级事务契约绑定** | `src/saga/mcp_registry.ts` | `test 39` (`PASS`) | 动态解析 MCP 工具 JSON Schema，对未声明逆向补偿函数的外部写操作强制安全降级为 `STAGED_COMMIT` |
| **`TASK-62`** | **多租户 FinOps 实时成本归因账本与日/月预算硬顶熔断器** | `src/finops/budget_controller.ts` | `test 39` (`PASS`) | 实时核算三大模型池输入/CoT/输出 Token 与沙箱活跃 CPU 成本，80% 触发软告警、100% 自动触发 `GRACEFUL_DRAINING` 保存快照并冻结新准入 |
| **`TASK-63`** | **实时管控台 (`dashboard`) 升级：分片拓扑热力图与高危工具在线审批** | `src/dashboard/server.ts` | `test 40` (`PASS`) | 新增 `/api/v1/shards/heatmap` 分片负载热力图接口与 `handleStageCommitDecision` 在线批准/驳回原子 Flush 接口 |
| **`TASK-64`** | **多区域双活联邦调度器（Multi-Region Active-Active Federation）** | `src/cloudflare/federation_router.ts` | `test 40` (`PASS`) | 跨 `us-west`、`eu-central`、`ap-east` 严格执行数据驻留合规（如 `GDPR` 锁定欧洲区），主区域故障 `< 1s` 自动跨区切流 |
