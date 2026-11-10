# ANSTS v2.5 (`feat/celld-v25`) 平台管理面与运行态融合架构设计规范 (M1–M50)

## 1. 架构设计总览 (Architecture Design Blueprint)

ANSTS v2.5 采用 **控制面 (`denoland/celld` v0.6.3 Durable Object Actor) + 执行面 (Bare-Metal Linux MicroVM / `kata` Container + `LayerFS` 2.0 CoW) + 统一治理控制台 (TanStack Management & Runtime Console)** 三层解耦架构：

| 维度 | 核心子系统与源码模块 | 关键工程契约与不变式 |
| :--- | :--- | :--- |
| **架构设计 (Design)** | `PlatformManagementPlaneEngine` (`src/dashboard/server.ts`)<br>`AnstsLocalAndComposeServer` (`src/server_main.ts`) | 6 大导航域（集群拓扑、租户配额、任务契约、数据面存储、模型路由与预算、审批与审计）100% 对接真实状态机与 SQLite 持久化，消除一切前端打桩。 |
| **动态配置中心 (Configuration)** | `v25_mgmt_config_revisions` + `TaskSpecValidator` (`src/gateway/validator.ts`)<br>`wrangler.json` + `specs/taskspec_v1beta1_example.yaml` | 所有集群配置、运行时参数（`CELLD_MAX_CELL_REQUESTS`、`CELLD_DURABILITY`、`THINK/ACT` 配额、PSI 阈值）均通过 Schema 校验、CAS 版本号递增与 `SHA-256` 摘要落盘，支持一键回滚历史版本与热加载。 |
| **运行时引擎 (Runtime)** | `AnstsSchedulerCell` + `AgentTaskHarnessCell` + `CelldContainerHandle` (`kata`/`runsc` + `celld-fence`)<br>`LayerFsWorkspaceDriver` + `LinuxCgroupV2AndPsiDriver` + `ZeroTrustVsockHostDaemon` | 双元组单调递增 Fencing Token (`StaleFenceError [409]`)、`CELLD_DURABILITY=fleet` 对等节点 `fsync` + 对冲复制 (`HEDGED_PEER_COPY`)、64 并发请求过载保护 (`503 Retry-After: 1`)、L0→L1 LTX 增量合并与 `userfaultfd` 4KiB 缺页恢复。 |
| **监控与自动化运维 (Monitoring & Ops)** | `OtelAndPrometheusExporter` (`src/telemetry/otel_prometheus.ts`)<br>`TransactionalOutboxDispatcher` + `CloudEventsBus` + `SseEventStreamGateway` | W3C `traceparent` 分布式调用链瀑布流、Prometheus 直方图分位数（`p50`/`p95`/`p99`）、多窗口 SLO 错误预算燃烧率告警（`PAGE_P0` / `TICKET_P1`）与自动化自愈 Runbook（`RB-SLO-BURN-AND-DLQ-HEAL`）。 |

## 2. 管理面核心 REST API 矩阵 (`/v1beta1/management/*`)

- **设计规范与架构一致性自检**：`GET /v1beta1/management/design/blueprint`
- **版本化配置中心与热加载/回滚**：`GET /v1beta1/management/config/center` · `POST /v1beta1/management/config/center`
- **`denoland/celld` & `LayerFS` 运行时引擎联合调优**：`POST /v1beta1/management/runtime/celld-engine`
- **全链路 SRE 监控、SLO 燃烧率与自动化自愈 Runbook**：`POST /v1beta1/management/ops/observability`
- **全集群 8 表原子快照与冷启动恢复**：`POST /v1beta1/management/persistence/snapshot`
- **原生 `ctx.container` (`kata`) + `celld-fence` 与幂等工作流**：`POST /v1beta1/management/celld/container-workflow`
- **确定性投机多分支 CoW 推演与三路合并**：`POST /v1beta1/management/layerfs/speculative`
- **闭环 Linux PSI 内存守卫与队列动态分片**：`POST /v1beta1/management/cluster/autoscale-psi`
- **模型路由 SSE 推理流水线、三态熔断与双令牌桶**：`POST /v1beta1/management/router/inference-pipeline`
- **Unix Socket Vsock 守护进程、`eBPF` LSM 与 Ed25519 `SLSA` 证明**：`POST /v1beta1/management/security/kernel-attestation`
- **网络分区脑裂演练、宿主机 Preflight 与 `Topology CAS`**：`POST /v1beta1/management/chaos/drill`
- **双归属反向隧道切换、多地域联邦路由与 `Harness` 迁移自愈**：`POST /v1beta1/management/federation/tunnel-harness`
