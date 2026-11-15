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

## M93: celld 开发服务真实启动与 Console 后端对接工作台 (Live celld Dev Cluster & BFF Wiring)
- **后端与代理层优化**:
  - `AnstsLocalAndComposeServer` (`src/server_main.ts`) 与 `DashboardBackendServer` (`src/dashboard/server.ts`) 全量开启 CORS (`Access-Control-Allow-Origin: *`) 与 `OPTIONS` (`204 No Content`) 预检支持，`dashboardPort` (`:8090`) 完整委托代理 Console 静态页面、`/v1beta1/*` BFF 与 `/.well-known/celld/health`、`/state`、`/metrics`，且当默认 `8080` 端口被宿主机占用时自动无感回退至 `:8089`。
  - `scripts/dev_cluster_local_supervisor.mjs` (`:8088` TanStack Console 反向代理) 与 `deploy/dev-cluster/nginx-tanstack-console.conf` 完整透传 `Authorization: Bearer <jwt>`、`Accept`、`Last-Event-ID` 请求头，支持 `/v1/events/stream` 与 `/v1beta1/management/events/stream?live=1` 无缓冲实时流式转发 (`http.request` pipe)，并代理 `/.well-known/celld/health`、`/state`、`/metrics`。
  - 新增 `GET /v1beta1/console/celld-dev-status`、`POST /v1beta1/management/celld/probe` 及 CLI `anstsctl mgmt-celld-status`、`anstsctl mgmt-celld-probe`，执行 6 项核心子系统探活核验 (`CELLD_WELL_KNOWN_HEALTH`、`CELLD_NATIVE_LOAD_STATE`、`CELLD_DURABLE_SQLITE_RPO0`、`CELLD_DO_CELL_PLACEMENT`、`CONSOLE_BFF_CORS_AND_JWT_GUARD`、`LIVE_SSE_AND_LAYERFS_ENGINE`)。
- **Console 前端对接优化**:
  - `getBackendOrigin()` 与 `callBackendApi()` 支持 `?backend=` 参数、顶栏「🔌 后端对接与 celld 探活」对话框动态切换及本地开发集群候选端点 (`:8088`、`:18080`、`:18081`、`:8089`、`:8090`) 自动探活绑定（严格校验 HTTP 2xx 响应，离线时保持 503 零伪造状态）。

## M94: Git 分支历史合并 (`main` a9ccc0e + `feat/celld-v25` M16-M93) 与全量契约统一
- **Git 历史无损合并**:
  - 将 `.git.rar` 中的 `main` 分支 (`582401d..a9ccc0e`, M1–M15) 与 `feat/celld-v25` 分支 (`93507bd..a9235d8`, M16–M93) 执行双父节点 Git Merge，使 `main` 与 `feat/celld-v25` 同时指向统一的合并提交。
  - 完整保留 `main` 分支的 6 份核心架构设计文档 (`docs/architecture_design_doc.md`、`docs/microservices_architecture.md`、`docs/celld_persistent_recoverable_ha_scheduler_design.md`、`docs/ansts_production_deployment_architecture_design.md`、`docs/ansts_v2_implementation_backlog.md`、`docs/agent_sandbox_scheduler_dashboard.html`) 以及 `feat/celld-v25` 的全部 Console 与后端工程实现。
- **跨阶段符号与接口向后兼容**:
  - 统一导出 `AgentTask` (`TaskSpec`)、`ResourceVector5D` (`ResourceProfile`)、`ToolEffectClass` (`ToolCallEffectClass`)、`BayesianTaskProfiler` (`TaskResourceProfiler`)、`MultiTenantQueueManager` (`HierarchicalQueueManager`) 及 `computeCrc32`、`encodeLengthPrefixedCrcFrame`、`decodeLengthPrefixedCrcFrames`，并通过 `tests/m94_git_branch_merge_and_contract_unification.test.ts` 验证（83 个测试套件共 223 项测试 100% 通过）。

## M95: 全角色动态拓扑发现、节点自注册与 256-Slot 自动伸缩 (Dynamic Cluster Topology, Self-Registration & Multi-Role Elastic Scaling)
- **后端动态拓扑注册表与自注册/心跳 API (`src/dashboard/server.ts` & `src/server_main.ts`)**:
  - 新增持久化集群拓扑注册表 `v25_cluster_registry`，统一管理 `brain`、`body`、`gateway` 三类节点的 `baseUrl`、`registeredVia` (`SEED` / `SELF_REG` / `TUNNEL` / `SCALE_API`)、`lastHeartbeatMs`、`ownedSlots` 与 `leaseEpoch`。
  - 新增 `POST /internal/v1beta1/cluster/register`、`POST /internal/v1beta1/cluster/heartbeat`、`POST /internal/v1beta1/cluster/deregister` 与 `POST /v1beta1/management/topology/evict-expired`，任意新启动的 `brain` / `body` / `gateway` 进程配置 `seedGatewayUrls` 或 `primaryGatewayUrl` 后自动完成注册、加入 256-Slot 哈希环、同步更新 `peerBrainUrls` / `peerBodyUrls` / `peerGatewayUrls`，并在心跳超时后自动摘除失联节点并重平衡哈希槽。
  - 修复 `readJsonBody` 在 Bearer JWT 鉴权时覆盖请求体节点 `role` (`brain`/`body`/`gateway`) 的缺陷，扩展 `POST /v1beta1/management/nodes/scale` 支持 `gateway` 角色。
- **反代与 Console 前端全动态拓扑渲染 (`scripts/dev_cluster_local_supervisor.mjs` & `docs/console-management/prototype.js`)**:
  - `:8088` 反代周期同步 `/v1beta1/console/topology` 的 `activeGatewayUrls`，动态发现新上线 `gateway` 节点。
  - 前端移除 3 节点硬编码，基于 `slotOwners` (256 项) 与 `slotDistribution` 动态渲染任意数量 `brain` 节点的 256 槽一致性哈希环与节点选择表单，并在「节点与分片单元编排」页面新增「全角色动态拓扑注册表与自动伸缩工作台」。
