# ANSTS v2.0 工程研发全景任务规划表 (Master Implementation Backlog & Roadmap)

| 文档属性 (Document Metadata) | 详细内容 (Details) |
| :--- | :--- |
| **项目名称 (Project)** | **Agent-Native Sandbox Task Scheduler (ANSTS v2.0)** |
| **当前代码仓基线 (Baseline)** | `main @ 0bb837f`（已完成项目脚手架、Protobuf 契约、核心算法内存原型与 6 项基础集成测试） |
| **规划范围 (Scope)** | 覆盖 5 大领域限界上下文、11 个核心微服务组件，从内存原型演进至生产级分布式高可用系统的全部研发任务 |
| **核心模型池 (Model Pools)** | `DeepSeek`（旗舰深度推理池）、`GLM-5.3`（轻量高速/只读轮次分流池）、`Qwen-3.8`（高可用热备容灾池） |
| **审核状态 (Review Status)** | **ALL M1 ~ M6 COMPLETED & VERIFIED IN CI (main @ aa5f03f)** |

---

## 1. 研发阶段与里程碑总览 (Milestones Overview)

本规划将 ANSTS v2.0 的后续研发拆解为 **6 个演进阶段（Epic M1 ~ M6）、共 32 个可独立交付与验证的工程任务（`TASK-01` ~ `TASK-32`）**。每个任务均绑定明确的源码模块路径、前置依赖与自动化 CI 验收标准（Definition of Done, DoD）：

| 阶段里程碑 (Milestone) | 阶段核心目标 | 涉及微服务组件 | 任务编号范围 | 优先级 | 预计交付周期 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **M0：工程基线与核心算法原型** | 完成代码仓初始化、GitHub Actions CI、5D W-DRF/Saga/Fencing 内存参考实现 | 全模块骨架 (`src/ansts/*`) | `BASE-01` ~ `BASE-06` | `P0` | **已完成 (Done)** |
| **M1：契约驱动、接入网关与在线贝叶斯定容** | 实现 HTTP/gRPC/SSE 接入网关、`TaskSpec v1beta1` 严格校验器、L3 背压与持久化贝叶斯画像 | `ansts-api-gateway`<br>`ansts-bayesian-profiler` | `TASK-01` ~ `TASK-05` | `P0` | ✅ 已完成 (Done) |
| **M2：多租分级队列、5D W-DRF 调度与 CAS 租约** | 实现 H-WFQ 多租分片队列、DAG 父子配额支票托管、5D W-DRF 并行选主与分布式 CAS 租约协调器 | `ansts-queue-manager`<br>`ansts-core-scheduler`<br>`ansts-lease-coordinator` | `TASK-06` ~ `TASK-11` | `P0` | ✅ 已完成 (Done) |
| **M3：Turn 级动态认知路由与四级工具 Saga 网关** | 实现跨 `DeepSeek`/`GLM-5.3`/`Qwen-3.8` 的流式代理、分布式令牌桶、三态熔断器与 MCP 四级事务网关 | `ansts-cognitive-router`<br>`ansts-saga-tool-gateway` | `TASK-12` ~ `TASK-17` | `P0` | ✅ 已完成 (Done) |
| **M4：相变弹性沙箱宿主机守护与 PID 1 Harness** | 实现 Cgroups v2 `0.1C <-> 8C` 控制器、预热池克隆、PSI 紧急休眠、VSOCK 网桥与进展率 $\mathcal{V}_k$ 守护 | `ansts-node-daemon`<br>`agent-harnessd` | `TASK-18` ~ `TASK-23` | `P0 / P1` | ✅ 已完成 (Done) |
| **M5：Turn-WAL 增量快照、时光机重放与事件通知** | 实现二进制 `TurnWAL` 引擎、后台 L0$\to$L1 合并、按需分页恢复（Paged Restore）与事务性发件箱 | `ansts-checkpoint-manager`<br>`ansts-event-notifier` | `TASK-24` ~ `TASK-28` | `P1` | ✅ 已完成 (Done) |
| **M6：四级双向闭环背压、实时看板联调与混沌压测** | 打通全链路四级双向背压控制环、对接极简可视化仿真看板 SSE 接口、完成生产级混沌基准测试 | 全集群联调 & Chaos Suite | `TASK-29` ~ `TASK-32` | `P1 / P2` | ✅ 已完成 (Done) |

---

## 2. 已完成基线清单 (M0 — Completed Baseline in `0bb837f`)

- [x] **`BASE-01` 项目脚手架与 GitHub Actions CI 流水线**：完成 `pyproject.toml`、`Makefile`、`scripts/ci_check.py` 与 `.github/workflows/ci.yml`（支持 Lint、Build、Python 3.11/3.12 矩阵集成测试）。
- [x] **`BASE-02` 核心 Protobuf 接口契约与 `TaskSpec v1beta1` 样例**：完成 `proto/ansts_rpc.proto` 与 `specs/taskspec_v1beta1_example.yaml`。
- [x] **`BASE-03` 五维资源向量与双元组栅栏模型**：完成 `src/ansts/models.py`（`FenceToken(epoch, turn_seq)` 字典序偏序比较与 `ResourceVector5D` 折算）。
- [x] **`BASE-04` 5D W-DRF 调度器与 DAG 支票内存原型**：完成 `src/ansts/scheduler.py`、`src/ansts/queue_manager.py` 与 `src/ansts/profiler.py` 基础逻辑。
- [x] **`BASE-05` 认知路由与四级 Saga 网关内存原型**：完成 `src/ansts/cognitive_router.py`（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）与 `src/ansts/saga_gateway.py`。
- [x] **`BASE-06` 6 大核心场景端到端集成测试**：完成 `tests/test_integration_suite.py` 并全部跑通。

---

## 3. 待实现研发任务详细拆解 (Pending Implementation Backlog)

### 阶段一（M1）：契约驱动、接入网关与在线贝叶斯定容 (`ansts-api-gateway` & `ansts-bayesian-profiler`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-01`** | **`TaskSpec v1beta1` 严格校验器与默认值注入器** | `src/ansts/gateway/validator.py` | `P0` | `BASE-02` | - 校验全部 7 大配置块（优先级、模型池、TPS/Token 配额、双相资源、工具事务等级、Harness 与 Webhook）<br>- 注入合理默认值，对非法参数返回结构化字段级错误码<br>- **DoD**：覆盖 15+ 正反例 YAML 校验单测 |
| **`TASK-02`** | **幂等去重器与任务提交 HTTP/gRPC 接入服务端** | `src/ansts/gateway/server.py` | `P0` | `TASK-01` | - 基于 `SHA256(tenant_id + idempotency_key)` 实现 24h 幂等去重表<br>- 提供 `POST /v1beta1/tasks`、`GET /v1beta1/tasks/{id}`、`DELETE /v1beta1/tasks/{id}` 接口<br>- **DoD**：并发重复提交相同幂等键 100 次仅创建 1 个任务实例 |
| **`TASK-03`** | **L3 客户端准入背压与排队水位响应头注入** | `src/ansts/gateway/backpressure.py` | `P0` | `TASK-02` | - 当租户排队深度超限或集群处于 Stage 3 红线时，立即拒绝入队并返回 `HTTP 429 Too Many Requests`<br>- 自动计算并注入 `Retry-After`、`X-Queue-Position` 与 `X-Backpressure-Stage` 响应头<br>- **DoD**：模拟队列满载触发标准 429 响应头集成测试 |
| **`TASK-04`** | **SSE 流式事件网关与客户端信用窗口（Credit Window）流控** | `src/ansts/gateway/sse_stream.py` | `P1` | `TASK-02` | - 支持客户端订阅单任务或租户级实时 CloudEvents SSE 流（支持 `Last-Event-ID` 断点续传）<br>- 实现基于 ACK 信用帧的消费端窗口跟踪，当信用 `< 25%` 时触发 Stage 4 正向背压（合并 `thinking.delta`）<br>- **DoD**：模拟慢速消费者验证自动切换按句聚合推送 |
| **`TASK-05`** | **在线共轭贝叶斯画像器与渐进式配额续签引擎** | `src/ansts/profiler.py` | `P1` | `BASE-04` | - 实现对数正态分布 $(\mu_h, \sigma_h^2)$ 与思考期占比 $\text{Beta}(\alpha_h, \beta_h)$ 的增量持久化更新<br>- 实现冷启动回退策略（样本数 `< 5` 时采用 `0.5 * max_total_tokens`）与 85% 水位自动续签 API<br>- **DoD**：验证 100 个历史样本下 P50/P85 预测收敛误差 `< 5%` |

---

### 阶段二（M2）：多租分级队列、5D W-DRF 调度与分布式 CAS 租约 (`queue-manager` / `scheduler` / `lease-coordinator`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-06`** | **三级多租队列树（L1 商业组 / L2 租户 / L3 `P0~P4` 泳道）** | `src/ansts/queue_manager.py` | `P0` | `BASE-04` | - 实现租户保底配额向量 $\vec{G}_t$、突发上限向量 $\vec{M}_t$ 与权重 $w_t$ 的强约束检查<br>- 实现排队超时扫描器（超过 `max_queue_wait_sec` 自动置为 `EXPIRED` 并通知）<br>- **DoD**：验证高负载下租户无法突破 $\vec{M}_t$ 抢占其他租户保底配额 $\vec{G}_t$ |
| **`TASK-07`** | **对数老化（Aging）+ EDF 截止时间动态紧迫度最小堆** | `src/ansts/queue_manager.py` | `P0` | `TASK-06` | - 优化租户内 `P0~P4` 候选任务评分计算 $S(j, \tau)$，支持实时更新等待时长补偿与截止时间加速项<br>- **DoD**：模拟持续涌入 `P1` 任务场景，证明排队等待的 `P4` 任务在有限 Tick 内必被调度（零饿死证明测试） |
| **`TASK-08`** | **多智能体 DAG 状态机、加密配额支票与防死锁快速通道** | `src/ansts/dag/escrow.py` | `P0` | `TASK-06` | - 实现 `EscrowVoucher` 的 HMAC 签名与校验、父任务 `HIBERNATED_YIELD` 状态转换与槽位即时释放<br>- 实现 Join 屏障慢分支闲置 TPS 自动汇聚（Straggler Boost）与 Race 投机模式级联取消指令<br>- **DoD**：在集群槽位 100% 满载下并发派生 20 个子任务，验证 0 死锁全部完成 |
| **`TASK-09`** | **五维相变与推理强度感知调度器（5D Phase & Effort W-DRF）** | `src/ansts/scheduler.py` | `P0` | `TASK-07` | - 完整实现稳态 CPU、突发 CPU、稳态内存、并发子会话、有效 TPS 五维主导份额 $D_t$ 实时账本<br>- 实现工作节点（预热池亲和、突发余量、本地快照缓存）$\times$ 模型池（配额余量、TTFT）联合匹配打分器<br>- **DoD**：混合提交 CPU 密集型与 Token 密集型任务，验证各租户主导份额严格按权重 $w_t$ 收敛 |
| **`TASK-10`** | **高优任务优雅抢占休眠（`PreemptToHibernate`）控制器** | `src/ansts/scheduler.py` | `P1` | `TASK-09` | - 当 `P0/P1` 任务准入但节点算力不足时，自动挑选代价最低的 `P4/Spot` 运行中沙箱下发休眠指令<br>- 被抢占沙箱刷出当前 Turn 快照后转入 `HIBERNATED` 并回退至高优重入队通道<br>- **DoD**：验证 `P0` 任务在 `< 150ms` 内完成抢占准入且被抢占任务 0 已完成轮次丢失 |
| **`TASK-11`** | **分布式 CAS 租约存储后端与心跳超时故障转移协调器** | `src/ansts/lease_coordinator.py` | `P0` | `BASE-03` | - 抽象 `LeaseStorageBackend` 接口（提供内存、SQLite WAL 及 Etcd/TiKV CAS 驱动实现）<br>- 实现工作节点心跳监控协程（3s 心跳，12s 判定失联），自动递增 `epoch` 触发跨节点重调度<br>- **DoD**：并发 10 个工作协程争抢同一任务租约，确保严格仅 1 个协程赢下 CAS |

---

### 阶段三（M3）：Turn 级动态认知路由与四级工具 Saga 网关 (`cognitive-router` & `saga-tool-gateway`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-12`** | **`DeepSeek` / `GLM-5.3` / `Qwen-3.8` 多模型池适配器与流式代理** | `src/ansts/router/pool_client.py` | `P0` | `BASE-05` | - 实现面向 `DeepSeek`、`GLM-5.3`、`Qwen-3.8` 三大模型池的统一流式调用接口与 SSE Chunk 解析器<br>- 实时统计每轮输入 Token、思维链（CoT）Token、输出 Token 及首字延迟（TTFT）<br>- **DoD**：验证流式 Chunk 转发与 Token 精确计量单测 |
| **`TASK-13`** | **Turn 级动态认知路由决策引擎（只读分流与连续报错提权）** | `src/ansts/cognitive_router.py` | `P0` | `TASK-12` | - 实现轮次意图分类器：识别纯文件读取/日志解析轮次自动路由至 `GLM-5.3`（`minimal/low` 强度）<br>- 当监测到连续 $\ge 2$ 次工具报错或回滚重试时，自动切回 `DeepSeek` 并将强度提至 `max (5.0x)`<br>- **DoD**：回放 20 轮典型研发轨迹，验证自动分流节省 `> 35%` 等效 TPS 消耗 |
| **`TASK-14`** | **模型池分布式滑动窗口令牌桶（Token Pacing）与三态熔断器** | `src/ansts/router/breaker_limiter.py` | `P0` | `TASK-12` | - 按 `(tenant_id, model_pool)` 维护 `reserved_tps` 与 `max_burst_tps` 滑动窗口令牌桶，超速时注入 Pacing 延迟<br>- 实现 30s 窗口错误率（HTTP 429/5xx `> 25%`）与 TTFT（`> 15s`）自动熔断状态机（`CLOSED -> OPEN -> HALF_OPEN`），秒级无感切换至 `Qwen-3.8`<br>- **DoD**：注入 429 故障验证 `CLOSED -> OPEN -> Qwen-3.8 接管 -> HALF_OPEN 探测恢复` 全周期 |
| **`TASK-15`** | **四级工具事务拦截器与确定性幂等键派生引擎** | `src/ansts/saga_gateway.py` | `P0` | `BASE-05` | - 强制前置校验 `X-Agent-Fence: <epoch>:<turn_seq>`，过期栅栏直接返回 `409 StaleFenceRejected`<br>- 实现 `PURE_READ` 结果缓存、`IDEMPOTENT_WRITE` 自动注入 `SHA256(task:epoch:turn:call_id)` 幂等头<br>- **DoD**：验证重放相同 `(epoch, turn, call_id)` 请求时 100% 命中缓存或幂等键一致 |
| **`TASK-16`** | **Saga 持久事务日志与 LIFO 逆向补偿执行器** | `src/ansts/saga/compensator.py` | `P0` | `TASK-15` | - 对 `SAGA_COMPENSATABLE` 工具在正向执行前持久化登记逆向工具名与参数映射规则<br>- 当收到树搜索回滚或任务熔断指令时，按 LIFO 逆序自动调用补偿工具并支持失败重试<br>- **DoD**：模拟跨越 4 轮的 Git 分支创建与 K8s Job 提交，回滚时验证逆序清理成功 |
| **`TASK-17`** | **`STAGED_COMMIT` 暂存缓冲区与人工审批门禁（Human Approval Gate）** | `src/ansts/saga/staging.py` | `P1` | `TASK-15` | - 拦截高危生产写操作（如 `settlement_ledger_post`）存入宿主机 `Staging Buffer` 并返回模拟回执<br>- 若配置 `require_human_approval: true`，自动触发 `agent.tool.approval.requested` 事件并通知宿主机将沙箱转入 `HIBERNATED`；审批通过后唤醒并原子 Flush<br>- **DoD**：验证未获批准或中途回滚时外部生产系统 0 实际写入 |

---

### 阶段四（M4）：相变弹性沙箱宿主机守护与沙箱内 Harness (`node-daemon` & `agent-harnessd`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-18`** | **预热沙箱池管理器（Warm MicroVM Pool）与隔离驱动抽象层** | `src/ansts/runtime/warm_pool.py` | `P0` | `TASK-11` | - 抽象 `SandboxDriver` 接口（支持 `FirecrackerDriver`、`KataDriver`、`GVisorDriver` 及本地进程沙箱模拟器）<br>- 维护按 `warm_pool_id` 预热的休眠沙箱池，支持 `< 65ms` CoW 克隆分配与销毁后块擦除<br>- **DoD**：测试预热池水位自动补充与并发分配延迟 |
| **`TASK-19`** | **Cgroups v2 单轮双相脉冲控制器（`0.1C <-> 8.0C`）与节点突发信号量** | `src/ansts/runtime/phase_controller.py` | `P0` | `TASK-18` | - 实现思考期（`cpu.max = 0.1C`, `memory.high = 256MB`, `zram` 回收）与工具执行期（`2C~8C`, `4096MB`）的 `< 5ms` 切换接口<br>- 实现宿主机级 `Burst Semaphore` 令牌桶（超并发上限时自动微排队 `100~500ms` 削峰）<br>- **DoD**：20 个沙箱同时请求 8C 突发时验证主机总分配核数严格不超物理上限 |
| **`TASK-20`** | **Linux PSI 内存压力监测与低优沙箱紧急休眠防 OOM 守卫** | `src/ansts/runtime/psi_guard.py` | `P1` | `TASK-19` | - 实时采样宿主机 `/proc/pressure/memory`（及模拟压力注入接口）<br>- 当 `some avg10 > 40%` 时，自动挑选处于 `COGNITIVE_PHASE` 的最低优先级沙箱转储至 NVMe 休眠<br>- **DoD**：注入内存高压信号，验证低优思考态沙箱在 `< 50ms` 内休眠释放内存，执行态沙箱 0 中断 |
| **`TASK-21`** | **宿主机与沙箱内部 `AF_VSOCK` 零信任透明代理网桥** | `src/ansts/runtime/vsock_bridge.py` | `P0` | `TASK-18` | - 实现 `agent-harnessd` 与 `ansts-node-daemon` 之间的双向帧通信协议<br>- 沙箱内零 API Key 存储，由宿主机网桥统一注入凭证、强制附加 `X-Agent-Fence` 并实施域名白名单过滤<br>- **DoD**：验证沙箱内尝试访问非白名单外网域名或伪造旧 Epoch 时 100% 被宿主机网桥阻断 |
| **`TASK-22`** | **沙箱内 `agent-harnessd` 主循环、上下文滑动压缩与子会话信号量** | `src/ansts/harness/runner.py` | `P0` | `TASK-21` | - 实现 `Thought -> Tool Call -> Observation` 状态机与 `max_concurrent_sessions` 本地信号量管控<br>- 当累计 Token 达 `80%` 水位时，自动触发 `sliding-summary` 上下文压缩（保留 System Prompt 与最近 $N$ 轮）<br>- **DoD**：验证达到 80% 水位时自动生成压缩摘要并发出 `agent.quota.watermark.warning` 事件 |
| **`TASK-23`** | **三层任务级熔断器：AST 语法死循环 + 进展率（$\mathcal{V}_k$）树搜索回滚** | `src/ansts/harness/velocity_breaker.py` | `P0` | `TASK-22` | - 实现工具参数 AST 归一化哈希比对（连续 3 次相同调用注入强制反思干预）<br>- 实现每轮进展率 $\mathcal{V}_k$ 评估器（综合测试通过率、报错消减、修改信息熵与 Merkle 树振荡惩罚），当 4 轮均值 `< 0.10` 时自动触发回滚 + 负反馈防重记忆注入<br>- **DoD**：注入 `Modify -> Revert -> Modify` 振荡轨迹，验证第 4 轮精准回滚并提权至 `DeepSeek (max)` |

---

### 阶段五（M5）：`TurnWAL` 增量快照、时光机重放与事件通知 (`checkpoint-manager` & `event-notifier`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-24`** | **`TurnWAL` 二进制增量快照引擎与对象存储条件写（`If-None-Match`）** | `src/ansts/checkpoint/wal_engine.py` | `P0` | `TASK-11` | - 序列化每轮对话树增量、环境变量、虚拟时钟偏移与 OverlayFS 4KB CoW 差异块为 `.wal` 文件<br>- 支持对等节点快速复制与对象存储 `ep_<epoch>/turn_<seq>.wal` 条件写防覆盖<br>- **DoD**：验证旧 Epoch 节点尝试覆盖写快照时被存储层原子拒绝 |
| **`TASK-25`** | **后台 L0 $\to$ L1 快照合并器（Compactor）与跨节点按需稀疏分页恢复（Paged Restore）** | `src/ansts/checkpoint/compactor.py` | `P1` | `TASK-24` | - 当单任务累计达到 16 个 L0 `.wal` 增量块时，后台自动合并为单个 L1 基础快照<br>- 实现跨节点故障恢复时的按需分页拉取：优先恢复对话上下文与最近修改热页，后台异步填充冷页<br>- **DoD**：验证 20 轮快照合并前后状态哈希 100% 一致，跨节点恢复耗时 `< 100ms` |
| **`TASK-26`** | **零 Token 确定性时光机重放引擎（Deterministic Time-Travel Replay）** | `src/ansts/checkpoint/time_travel.py` | `P1` | `TASK-24` | - 支持指定任意历史任务 `task_id` 与目标轮次 `target_turn`（如停在第 25 轮故障前夕）<br>- 从 `TurnWAL` 直接回放录制好的 LLM SSE 流与 `PURE_READ` 工具响应，0 模型调用、0 Token 消耗复现现场<br>- **DoD**：对已完成的 10 轮任务执行时光机重放至第 6 轮，验证耗时 `< 50ms` 且 Token 消耗为 0 |
| **`TASK-27`** | **CloudEvents 1.0 标准事件总线与 OpenTelemetry 全链路追踪注入** | `src/ansts/telemetry/cloudevents.py` | `P1` | `BASE-03` | - 统一封装生命周期、Turn 认知路由、工具调用、配额水位与背压熔断 4 大类 CloudEvents 1.0 报文<br>- 全链路透传 `trace_id` / `span_id` 与用户自定义 `custom_metadata`<br>- **DoD**：验证所有核心状态流转均生成符合 CloudEvents 1.0 规范的 JSON 事件 |
| **`TASK-28`** | **事务性发件箱（Transactional Outbox）与 HMAC 签名 Webhook 投递器** | `src/ansts/telemetry/outbox_webhook.py` | `P1` | `TASK-27` | - 状态变更与 `notification_outbox` 记录同事务原子提交，保证通知 100% 不漏发<br>- 实现 `X-Agent-Signature-256`（HMAC-SHA256）签名、5 次指数退避重试与租户死信队列（DLQ）重放接口<br>- **DoD**：模拟下游 Webhook 接收端前 3 次返回 503、第 4 次返回 200，验证退避重试成功投递 |

---

### 阶段六（M6）：四级双向闭环背压、实时看板联调与混沌工程验收 (`cluster` & `dashboard`)

| 任务 ID | 任务名称 | 目标代码路径 | 优先级 | 依赖 | 核心实现内容与验收标准 (DoD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`TASK-29`** | **四级双向闭环背压控制器（Stage 1~3 下游反压 + Stage 4 上游正压）** | `src/ansts/cluster/backpressure_loop.py` | `P0` | `TASK-14`<br>`TASK-20` | - 联动模型池 TTFT/429 率、节点 PSI 水位与客户端 SSE 信用窗口，自动在 `NORMAL`、`STAGE1_SOFT_PACING`、`STAGE2_POOL_FAILOVER_HIBERNATE`、`STAGE3_QUEUE_FREEZE`、`STAGE4_UPSTREAM_THROTTLE` 间平滑切换<br>- **DoD**：端到端验证四级背压触发与恢复滞回曲线（防抖动） |
| **`TASK-30`** | **统一集群编排门面（`AnstsClusterRuntime`）与 CLI 运维工具 (`anstsctl`)** | `src/ansts/cli/anstsctl.py` | `P1` | `TASK-29` | - 提供命令行工具 `anstsctl`：支持 `submit`、`top`（查看租户 5D W-DRF 份额与节点水位）、`inspect`、`replay`（时光机重放）、`chaos`（故障注入）<br>- **DoD**：通过 CLI 跑通完整任务提交、状态查看与重放闭环 |
| **`TASK-31`** | **极简可视化仿真看板与后端实时 API / SSE 联调桥接** | `src/ansts/dashboard/server.py` | `P2` | `TASK-04`<br>`TASK-30` | - 提供轻量 HTTP + SSE 服务，使前端极简仿真看板既可运行在纯前端独立仿真模式，也可一键连接本地 Python `AnstsClusterRuntime` 后端实时观测真实调度状态<br>- **DoD**：启动后端服务后看板实时渲染真实的 5D W-DRF 份额与 CloudEvents 事件流 |
| **`TASK-32`** | **生产级五大混沌工程基准测试套件（Chaos Benchmark Suite）** | `tests/test_chaos_benchmarks.py` | `P0` | `TASK-29` | - 完整实现设计文档第 11.2 节定义的 5 项混沌验收测试：`DeepSeek` 429 风暴切 `Qwen-3.8`、宿主机 `SIGKILL` 跨节点 `<800ms` 恢复、僵尸节点 `409 StaleFenceRejected` 拦截、20 沙箱同时编译 `Burst Semaphore` 削峰、语义死循环回滚与 Saga 零脏写<br>- **DoD**：纳入 GitHub Actions CI 必过门禁，输出量化 SLO 验收报告 |

---

## 4. 审核确认事项 (Review & Next-Step Confirmation)

请你审核以上 **M1 ~ M6 共 32 项研发任务规划**：

1. **优先级与范围调整**：是否有需要提前、裁剪或新增的业务特性（例如特定的 MCP 工具协议适配或特定存储后端）？
2. **启动执行方式**：审核通过后，你可以选择：
   * **方式 A（按阶段逐一交付）**：从 **M1（`TASK-01` ~ `TASK-05`：接入网关、严格校验器、SSE 流控与贝叶斯画像持久化）** 开始，每完成一个阶段运行 CI 并由你检视；
   * **方式 B（核心链路优先冲刺）**：优先实现你最关注的核心模块（例如直接推进 **M2 + M3 的核心调度与认知路由/Saga 网关完整实现**）。
