# Google Engineering Design Doc: 通用 Agent 沙箱任务调度系统 (ANSTS v2.0) 微服务架构设计规范

| 文档属性 (Document Metadata) | 详细内容 (Specification Details) |
| :--- | :--- |
| **文档标题 (Title)** | **ANSTS v2.0 Microservices Architecture Specification: 面向大规模 Agent 沙箱调度的微服务拆分、通信契约与分布式数据架构** |
| **文档状态 (Status)** | **APPROVED / SYNCHRONIZED PRODUCTION ARCHITECTURE SPECIFICATION (v2.2 — TypeScript + celld HA + Horizontal Scale-Out + Multi-Region Federation Edition)** |
| **作者 (Authors)** | 分布式 Agent 平台架构组 (Distributed Agent Platform Architecture Team) |
| **关联主文档 (Parent Doc)** | `ANSTS v2.0 全量架构规范` (5D Phase & Effort-Aware W-DRF + 相变弹性沙箱 + 双元组栅栏 + Saga 工具网关) |
| **最后更新 (Last Updated)** | 2026-10-29 |
| **核心模型池 (Model Pools)** | `DeepSeek` (旗舰深度推理池)、`GLM-5.3` (高速轻量/只读轮次分流池)、`Qwen-3.8` (高可用热备与弹性容灾池) |
| **技术栈选型 (Tech Stack)** | TypeScript (Node.js / V8 Isolate / Cloudflare Workers / denoland/celld 全栈统一实现)、RustFS (S3 EC:4 对象存储)、SQLite WAL / LTX、gRPC + Protobuf、gRPC + Protobuf、Kafka、S3、TiKV/Etcd |

---

## Executive Summary (执行摘要)

为支撑单集群 **100,000+ 并发活跃/休眠沙箱**、每秒 **50,000+ Turn 状态跃迁** 以及跨 **`DeepSeek`、`GLM-5.3`、`Qwen-3.8`** 三大模型池的毫秒级动态认知路由，通用 Agent 沙箱任务调度系统（ANSTS v2.0）不能采用单体控制面架构，必须按照 **领域驱动设计（DDD 限界上下文）** 与 **读写/冷热路径分离原则** 拆分为高内聚、低耦合的微服务集群。

本设计文档遵循 **Google Engineering Design Document** 规范，定义 ANSTS v2.0 的 **五大业务域、11 个核心微服务组件、gRPC/Protobuf 服务间接口契约、分布式存储分片模型、关键跨服务协作时序（Sequence Diagrams）以及生产级混合部署拓扑**。核心架构设计要点如下：

1. **控制面、数据面与认知路由面三面解耦**：将高频流式 Token 转发（`ansts-cognitive-router`）与重 I/O 快照复制（`ansts-checkpoint-manager`）完全剥离出核心调度器（`ansts-core-scheduler`），确保五维主导资源公平调度（5D W-DRF）单次 Tick 耗时稳定在 `< 10ms`。
2. **全链路双元组栅栏透传（End-to-End `Fence(epoch, turn_seq)` Propagation）**：通过统一的 gRPC Metadata 拦截器与 HTTP Header（`X-Agent-Fence`），在 API 网关、调度器、认知路由网关、MCP Saga 工具网关及对象存储层实施无锁单活校验，100% 阻断网络分区下的僵尸沙箱脑裂写。
3. **控制面全无状态化与分片水平扩展（Stateless Sharded Microservices）**：除底层持久化引擎外，所有控制面微服务均采用无状态设计或按 `tenant_id` / `task_id` 一致性哈希分片（Consistent Hashing），支持任意节点秒级水平扩缩容与滚动热升级。

---

## 1. Microservices Decomposition Principles & Bounded Contexts (微服务拆分原则与领域边界)

### 1.1 四大架构拆分准则

1. **按变更频率与吞吐量级拆分（Rate-of-Change & Throughput Isolation）**：
   * **Task 级低频控制流**（任务提交、排队准入、节点分配，约 $10^2 \sim 10^3\ \text{QPS}$）由 `ansts-queue-manager` 与 `ansts-core-scheduler` 处理；
   * **Turn 级中频认知与事务流**（轮次模型路由、工具 Saga 拦截、进展率校验、增量快照，约 $10^4 \sim 5 \times 10^4\ \text{QPS}$）由 `ansts-cognitive-router`、`ansts-saga-tool-gateway` 与 `ansts-checkpoint-manager` 处理；
   * **Token 级高频数据流**（SSE Token 流式推送、沙箱 PTY 标准输出、Cgroups 相变切换，约 $10^6\ \text{Events/s}$）由宿主机 `ansts-node-daemon` 与边缘流网关直接点对点承载，绝不经过中心调度器。
2. **故障半径严格隔离（Blast Radius Containment）**：外部 MCP 工具超时或第三方 Webhook 阻塞仅局限在 `ansts-saga-tool-gateway` 与 `ansts-event-notifier` 独立协程池内，绝不阻塞沙箱生命周期管理或 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 推理流。
3. **单向依赖与异步事件解耦（Acyclic Dependency & Event-Driven）**：核心调度链路采用低延迟 gRPC 同步调用，而计费统计、审计归档、轨迹分析与外部通知全部通过 CloudEvents 1.0 消息总线（Kafka）异步解耦。
4. **安全零信任边界（Zero-Trust Sandbox Boundary）**：运行在客户沙箱内的 `agent-harnessd` 处于非授信区（Untrusted Zone），仅能通过宿主机 `AF_VSOCK` 硬件虚拟通道与授信区的 `ansts-node-daemon` 通信。

### 1.2 五大领域限界上下文（DDD Bounded Contexts）

```
+-------------------------------------------------------------------------------------------------------------+
|                                  1. 接入与智能定容域 (Ingress & Profiling Context)                           |
|      [ansts-api-gateway] (Go/Envoy)                  [ansts-bayesian-profiler] (Rust)                       |
|      - 统一鉴权 / 幂等去重 / L3 准入背压 (429)         - 任务指纹提取 / P50 乐观软预留 / 渐进式配额续签计算      |
+-------------------------------------------------------------------------------------------------------------+
                       | (gRPC: SubmitTask / RenewQuota)
                       v
+-------------------------------------------------------------------------------------------------------------+
|                              2. 多租排队与五维联合调度域 (Scheduling & Lease Context)                         |
|      [ansts-queue-manager] (Rust)      [ansts-core-scheduler] (Rust)     [ansts-lease-coordinator] (Rust)   |
|      - H-WFQ 多租队列树 (P0~P4)         - 5D Phase/Effort W-DRF 选主      - CAS 租约颁发与 Fence(epoch,turn) |
|      - Aging 老化 + EDF 截止提权        - 节点预热池 × 模型池健康度联合打分 - 节点心跳探测与 Epoch+1 故障转移   |
|      - DAG 配额支票托管与慢分支提权      - 抢占式休眠 (PreemptToHibernate) - 闲置沙箱主动休眠指令下发          |
+-------------------------------------------------------------------------------------------------------------+
          | (gRPC: DispatchSandbox)                      | (gRPC: RouteTurn / ExecuteTool)
          v                                              v
+--------------------------------------------+  +-------------------------------------------------------------+
|  3. 沙箱节点执行数据域 (Node Execution)     |  |     4. 认知路由与工具事务控制域 (Cognitive & Saga Context)    |
|  [ansts-node-daemon] (Rust, 每物理机单实例) |  |  [ansts-cognitive-router] (Rust)                            |
|  - Firecracker/Kata/gVisor 预热池 (~65ms)  |  |  - DeepSeek / GLM-5.3 / Qwen-3.8 动态路由与 429 自动切换     |
|  - 思考期(0.1C) <-> 工具期(8C) 5ms相变伸缩  |  |  - 模型池级 TPS 令牌桶 Pacing 与推理强度动态降级              |
|  - 节点 Burst Semaphore & Linux PSI 休眠   |  |  [ansts-saga-tool-gateway] (Go)                             |
|  - AF_VSOCK 代理桥接与本地 TurnWAL 录制     |  |  - 外部 MCP 工具 Fence 强校验与四级副作用拦截                 |
|  +--------------------------------------+  |  |  - Staging Buffer 暂存提交、人工审批门禁与 LIFO Saga 补偿    |
|  | [agent-harnessd] (沙箱内 PID 1, Rust) |  |  +-------------------------------------------------------------+
|  | - Turn 循环与并发子会话管控           |  |                                  |
|  | - 进展率 (V_k) 计算与树回滚请求       |  |                                  v
|  +--------------------------------------+  |  +-------------------------------------------------------------+
+--------------------------------------------+  |     5. 快照持久化与可观测通知域 (Persistence & Telemetry)     |
                      |                         |  [ansts-checkpoint-manager] (Rust)                          |
                      +-----------------------> |  - TurnWAL 增量块上传、L0->L1 合并压缩与跨节点 Paged Restore |
                                                |  - 零 Token 确定性时光机重放 (Deterministic Replay Engine)   |
                                                |  [ansts-event-notifier] (Go)                                |
                                                |  - CloudEvents 1.0 总线、Transactional Outbox 与 Webhook    |
                                                +-------------------------------------------------------------+
```

---

## 2. Microservices Catalog & Detailed Responsibilities (核心微服务目录与职责详解)

系统共由 **11 个核心组件**（9 个控制面/平台微服务 + 1 个宿主机守护进程 + 1 个沙箱内 PID 1 守护进程）构成：

| 服务编号与名称 | 所属域 | 编程语言 | 状态属性与扩展方式 | 核心职责与关键算法 |
| :--- | :--- | :--- | :--- | :--- |
| **1. `ansts-api-gateway`** | 接入与定容域 | Go + Envoy | 无状态水平扩展 | 统一暴露 REST / gRPC / SSE 接口；校验 `TaskSpec v1beta1`；基于 `idempotency_key` 执行幂等去重；实施 L3 准入背压（返回 `HTTP 429` + `Retry-After` + `X-Queue-Position`）；管理客户端 SSE 流式信用窗口（Credit Window）。 |
| **2. `ansts-bayesian-profiler`** | 接入与定容域 | Rust | 无状态 + Redis 缓存 | 提取任务结构化指纹 $h(j)$；在线更新累计 Token 消耗的对数正态分布与双相占比 Beta 分布；在任务准入时输出 **P50 乐观软预留配额**，并在任务运行达 85% 水位时计算渐进式续签增量。 |
| **3. `ansts-queue-manager`** | 排队与调度域 | Rust | 按租户分片（Consistent Hash） | 维护 L1/L2/L3（`P0~P4`）多租分级队列树；实时计算队列头部任务的 **对数老化 + EDF 紧迫度得分 $S(j, \tau)$**；维护多智能体 DAG 拓扑图，签发 **父子配额支票（Quota Escrow Voucher）** 免排队直通令牌，执行 Join 屏障慢分支 TPS 提权与 Race 级联剪枝。 |
| **4. `ansts-core-scheduler`** | 排队与调度域 | Rust | 主备选主 + 分片并行调度 | 执行 **5D Phase & Effort-Aware W-DRF** 算法计算各租户加权主导份额 $D_t = \frac{1}{w_t}\max_k(\sum \widetilde{R}_{j,k}/C_k)$；综合节点预热池命中、突发算力余量及 `DeepSeek`/`GLM-5.3`/`Qwen-3.8` 配额水位执行联合匹配打分；触发高优任务抢占休眠（`PreemptToHibernate`）。 |
| **5. `ansts-lease-coordinator`** | 排队与调度域 | Rust | 无状态 + TiKV/Etcd CAS | 负责任务所有权租约（Ownership Lease）的原子条件写（CAS）；维护单调递增双元组栅栏 `Fence(epoch, turn_seq)`；监听宿主机心跳（3s 周期，12s 超时），一旦节点失联立即递增 `epoch` 触发跨节点断点恢复；下发长等待主动休眠指令。 |
| **6. `ansts-cognitive-router`** | 认知与事务域 | Rust | 无状态水平扩展 | 统一代理沙箱发往 **`DeepSeek`**（旗舰主池）、**`GLM-5.3`**（只读/轻量高速池）与 **`Qwen-3.8`**（热备容灾池）的推理请求；前置校验 `X-Agent-Fence` 拦截僵尸流；执行 Turn 级动态模型分流与推理强度（`minimal`~`max`）折算；维护模型池级自适应熔断器与分布式令牌桶 Pacing。 |
| **7. `ansts-saga-tool-gateway`** | 认知与事务域 | Go | 无状态水平扩展 + 持久事务日志 | 统一代理沙箱对外部 MCP Server 与受控 API 的调用；强制校验双元组栅栏；按 `PURE_READ`、`IDEMPOTENT_WRITE`、`SAGA_COMPENSATABLE`、`STAGED_COMMIT` 四级策略拦截请求；维护 `Staging Buffer` 暂存区、人工审批门禁回调及回滚时的 LIFO 逆向补偿执行。 |
| **8. `ansts-node-daemon`** | 节点执行数据域 | Rust | 每物理机单实例（DaemonSet） | 管理本地 Firecracker / Kata / gVisor 预热沙箱池（`~65ms` 克隆启动）；通过 `AF_VSOCK` 与沙箱通信；执行 **单轮双相脉冲 Cgroups v2 切换（思考期 `0.1C/256MB` $\leftrightarrow$ 工具期 `2C~8C`）**；维护本地 `Burst Semaphore` 令牌桶与 Linux PSI 内存超水位紧急休眠；捕获 OverlayFS 脏页生成 `TurnWAL`。 |
| **9. `agent-harnessd`** | 沙箱内部运行时 | Rust | 沙箱内 PID 1 静态二进制 (`<15MB`) | 驱动单任务 `Thought -> Tool -> Observation` 闭环；管控沙箱内并发子会话（`max_concurrent_sessions`）与进程树；在每轮 Turn 结束时计算 **语义进展率 $\mathcal{V}_k$**，当连续 4 轮均值 `< 0.10` 时向宿主机发起树搜索回滚（Backtrack）请求。 |
| **10. `ansts-checkpoint-manager`** | 持久化与观测域 | Rust | 无状态水平扩展 | 接收各节点流式上传的 `TurnWAL` 增量段与 OverlayFS CoW 差异块；执行后台 L0 $\to$ L1 合并压缩（Compaction）；在跨节点故障恢复或回滚时提供 **Paged Restore（按需稀疏分页加载）**；提供零 Token 确定性时光机重放流。 |
| **11. `ansts-event-notifier`** | 持久化与观测域 | Go | 无状态水平扩展（Kafka 消费组） | 消费全链路 CloudEvents 1.0 事件流；实现 **Transactional Outbox（事务性发件箱）** 高可靠通知投递；向外部业务系统发送带 `X-Agent-Signature-256` 签名的 Webhook、配额 80% 预警及人工审批交互卡片；管理租户死信队列（DLQ）。 |

---

## 3. Inter-Service Communication Protocols & API Contracts (微服务间通信协议与接口契约)

### 3.1 通信协议矩阵

| 通信路径 | 源服务 $\to$ 目标服务 | 传输协议 | 序列化与安全机制 | P99 延迟预算 |
| :--- | :--- | :--- | :--- | :--- |
| **控制面同步指令** | `api-gateway` $\to$ `queue-manager` $\to$ `core-scheduler` $\to$ `node-daemon` | gRPC over HTTP/2 | Protobuf v3 + 双向 mTLS + `x-agent-fence` Header | `< 15ms` |
| **沙箱宿主机内通信** | `agent-harnessd` (Guest) $\leftrightarrow$ `ansts-node-daemon` (Host) | `AF_VSOCK` (CID=2) | 零拷贝二进制帧 + Protobuf Stream | `< 0.5ms` |
| **认知推理流式通道** | `ansts-node-daemon` $\to$ `ansts-cognitive-router` $\to$ `DeepSeek` / `GLM-5.3` / `Qwen-3.8` | gRPC Bidirectional Stream / HTTP/2 SSE | Protobuf Chunk / SSE + 分布式令牌桶 Credit 帧 | 首字外代理开销 `< 2ms` |
| **外部工具事务通道** | `ansts-node-daemon` $\to$ `ansts-saga-tool-gateway` $\to$ External MCP | gRPC / MCP-over-SSE | Protobuf + 幂等键注入 + Saga WAL 登记 | 网关拦截开销 `< 4ms` |
| **快照复制与分页拉取** | `ansts-node-daemon` $\leftrightarrow$ Peer `node-daemon` / `checkpoint-manager` | gRPC Stream / S3 Conditional PUT | LZ4 压缩 4KB 脏页块 + SHA-256 校验 | 对等落盘 `~14ms` |
| **可观测事件与通知** | 各微服务 $\to$ Kafka $\to$ `ansts-event-notifier` | Kafka Protocol / CloudEvents 1.0 | JSON / Avro + Transactional Outbox | 异步端到端 `< 100ms` |

### 3.2 核心微服务 Protobuf 接口契约 (`ansts_rpc.proto`)

```protobuf
syntax = "proto3";
package ansts.v2.rpc;

// 双元组防脑裂栅栏令牌（所有状态写、LLM 请求、工具调用必填）
message FenceToken {
  string task_id = 1;
  uint64 epoch = 2;
  uint32 turn_seq = 3;
}

// 五维归一化资源向量 (5D Phase & Effort-Aware Resource Vector)
message ResourceVector5D {
  double expected_steady_cpu_cores = 1;  // 折算思考期(0.1C)与执行期后的稳态期望核数
  double burst_cpu_limit_cores = 2;      // 工具执行期峰值突发核数
  uint64 expected_memory_mb = 3;         // 稳态期望内存 (MB)
  uint32 concurrent_sessions = 4;        // 并发子会话槽位数
  uint32 effective_tps = 5;              // 乘以推理强度系数 alpha(e) 后的有效吞吐 (TPS)
}

// ==================== 1. 联合调度器与节点守护服务契约 ====================
service SandboxNodeService {
  // 派发任务并在预热池克隆启动沙箱 (~65ms)
  rpc DispatchSandbox(DispatchSandboxRequest) returns (DispatchSandboxResponse);
  // 主动休眠沙箱（用于高优抢占 PreemptToHibernate、审批等待、父等子让位或 PSI 内存压力）
  rpc HibernateSandbox(HibernateSandboxRequest) returns (HibernateSandboxResponse);
  // 从指定 TurnWAL 快照唤醒或跨节点断点恢复沙箱
  rpc ResumeSandboxFromWal(ResumeSandboxRequest) returns (ResumeSandboxResponse);
  // 执行树搜索回滚（当进展率 V_k < 0.10 时回滚至 turn_k - 4）
  rpc BacktrackSandbox(BacktrackSandboxRequest) returns (BacktrackSandboxResponse);
}

message DispatchSandboxRequest {
  FenceToken fence = 1;
  string tenant_id = 2;
  string warm_pool_id = 3;
  string isolation_driver = 4;           // "firecracker" | "kata" | "gvisor"
  ResourceVector5D allocated_quota = 5;
  string assigned_model_pool = 6;        // "DeepSeek" | "GLM-5.3" | "Qwen-3.8"
  map<string, string> custom_metadata = 7;
}

// ==================== 2. Turn 级动态认知路由服务契约 ====================
service CognitiveRouterService {
  // 按轮次特征动态路由至 DeepSeek / GLM-5.3 / Qwen-3.8 并返回流式 Token
  rpc StreamTurnCompletion(TurnCompletionRequest) returns (stream TurnCompletionChunk);
}

message TurnCompletionRequest {
  FenceToken fence = 1;
  string tenant_id = 2;
  string primary_model = 3;              // 默认 "DeepSeek"
  string fast_utility_model = 4;         // 默认 "GLM-5.3"
  repeated string fallback_models = 5;   // 默认 ["Qwen-3.8", "GLM-5.3"]
  string requested_effort = 6;           // "minimal" | "low" | "medium" | "high" | "max"
  bool is_read_only_utility_turn = 7;    // 若为 true，路由器自动分流至 GLM-5.3
  uint32 consecutive_tool_errors = 8;    // 若 >= 2，路由器自动将 DeepSeek 推理强度提至 max
  bytes serialized_messages = 9;
}

// ==================== 3. 四级副作用工具事务与 Saga 网关契约 ====================
service SagaToolGatewayService {
  // 拦截执行工具调用（校验 Fence，按 PURE_READ / IDEMPOTENT / SAGA / STAGED 分类处理）
  rpc ExecuteTransactionalTool(ToolCallRequest) returns (ToolCallResponse);
  // 当发生 Turn 回滚或任务熔断时，按 LIFO 逆序执行指定 Turn 之后的所有 Saga 补偿操作
  rpc CompensateTurns(CompensateTurnsRequest) returns (CompensateTurnsResponse);
  // 当任务最终成功或人工审批通过时，原子刷出所有 STAGED_COMMIT 暂存写操作
  rpc FlushStagedCommits(FlushStagedRequest) returns (FlushStagedResponse);
}

enum ToolEffectClass {
  PURE_READ = 0;
  IDEMPOTENT_WRITE = 1;
  SAGA_COMPENSATABLE = 2;
  STAGED_COMMIT = 3;
}

message ToolCallRequest {
  FenceToken fence = 1;
  string tool_call_id = 2;
  string tool_name = 3;
  ToolEffectClass effect_class = 4;
  string compensation_tool_name = 5;
  bytes arguments_json = 6;
}
```

---

## 4. Distributed Data Architecture & Storage Partitioning (分布式数据存储架构与分片设计)

为了在保证强一致性的前提下支撑超高并发，ANSTS 将存储层划分为四类专用存储引擎：

| 存储层次与引擎选型 | 承载的核心实体与数据结构 | 分片键（Partition Key）与一致性模型 | 关键性能与防脑裂机制 |
| :--- | :--- | :--- | :--- |
| **1. 核心租约与状态存储**<br>（TiKV / Etcd / Spanner） | 任务状态机记录、当前合法栅栏 `Fence(epoch, turn_seq)`、节点活跃心跳表、DAG 父子支票账本 | 按 `task_id` 哈希分片；**线性一致（Linearizable CAS）** | 所有租约变更强制通过 `CompareAndSwap(expected_epoch)` 原子提交，单次 CAS 延迟 `< 2ms`。 |
| **2. 分布式配额与队列热存**<br>（Redis Cluster + Lua） | 租户 5D 实时资源占用向量 $\vec{U}_t$、`DeepSeek`/`GLM-5.3`/`Qwen-3.8` 滑动窗口令牌桶、贝叶斯指纹统计参数 $(\mu_h, \sigma_h)$ | 按 `tenant_id` 与 `model_pool` 分片；原子 Lua 脚本 | 支持每秒 `200,000+` 次令牌桶扣减与配额续签，节点故障时可在 `< 1s` 内从活跃任务表重建热状态。 |
| **3. 增量快照与产物对象存储**<br>（S3 / GCS / MinIO） | `TurnWAL` 增量块（`ep_<epoch>/turn_<seq>.wal`）、L1 合并快照、工作区产出物 | 按 `s3://bucket/tenants/<tenant_id>/tasks/<task_id>/` 前缀分区；**写后读强一致** | 上传快照强制携带 `If-None-Match: *` 条件头，从物理存储层拒绝旧 Epoch 覆盖写。 |
| **4. 事件总线与可观测性存储**<br>（Kafka + VictoriaMetrics + Tempo） | CloudEvents 1.0 实时事件流、`notification_outbox` 表、Prometheus 指标、OTel 分布式调用链 | Kafka 按 `task_id` 分区（保证单任务事件严格保序） | 保留 7 天热流供客户端断点续订 SSE，归档至列式存储供账单审计与回溯分析。 |

---

## 5. Core Distributed Workflows & Sequence Diagrams (核心跨服务分布式协作时序设计)

### 5.1 时序一：任务准入、5D W-DRF 选主、预热沙箱启动与首轮动态认知路由

```
Client        api-gateway    bayesian-profiler   queue-manager   core-scheduler   lease-coord    node-daemon   cognitive-router
  |                |                 |                 |               |               |              |               |
  |--1.SubmitTask->|                 |                 |               |               |              |               |
  | (TaskSpec YAML)|--2.GetP50Quota->|                 |               |               |              |               |
  |                |<-P50 SoftQuota--|                 |               |               |              |               |
  |                |--3.EnqueueTask(P50Quota)--------->|               |               |              |               |
  |<--202 Accepted-|                                   |               |               |              |               |
  |                                                    |<-4.PopMin5D---| (5D W-DRF     |              |               |
  |                                                    |--Candidate--->|  + Aging/EDF) |              |               |
  |                                                    |               |--5.CAS Lease->|              |               |
  |                                                    |               |<-Fence(1, 1)--|              |               |
  |                                                    |               |--6.DispatchSandbox(Fence 1,1)|               |
  |                                                    |               |               |              |--7.CloneWarmVM|
  |                                                    |               |               |              |  (~65ms Ready)|
  |                                                    |               |               |              |--8.Set 0.1C-->|
  |                                                    |               |               |              |--9.StreamTurn(Fence 1,1)
  |                                                    |               |               |              |  [Route -> DeepSeek / GLM-5.3]
```

### 5.2 时序二：单轮双相脉冲伸缩、进展率 $\mathcal{V}_k$ 树搜索回滚与 Saga LIFO 逆向补偿

当沙箱内 `agent-harnessd` 在第 8 轮结束时检测到连续 4 轮平均进展率 $\overline{\mathcal{V}}_{5:8} = 0.04 < 0.10$ 时，跨微服务自动触发 **树搜索回滚与外部副作用补偿**：

```
agent-harnessd       ansts-node-daemon       ansts-saga-tool-gateway       ansts-checkpoint-manager       ansts-cognitive-router
      |                      |                          |                             |                             |
      |--1.ToolCall(git_push)|                          |                             |                             |
      |   (SAGA_COMPENSATABLE|--2.Unlock 8.0C (5ms)---->|                             |                             |
      |                      |--3.ExecuteTool(Fence 1,8)|                             |                             |
      |                      |   (登记逆向工具:         |                             |                             |
      |                      |    git_branch_delete)    |                             |                             |
      |<--Tool Observation---|<--200 OK-----------------|                             |                             |
      |                      |                          |                             |                             |
      |--4.Turn #8 完成: 计算进展率 V_8 = 0.04 < 0.10    |                             |                             |
      |--5.RequestBacktrack->|                          |                             |                             |
      |   (target_turn = 4)  |--6.CompensateTurns(5..8)>|                             |                             |
      |                      |   (按 LIFO 逆序自动调用   |                             |                             |
      |                      |    git_branch_delete 等) |                             |                             |
      |                      |<--Compensation Done------|                             |                             |
      |                      |--7.PullTurnWalSnapshot(turn=4)------------------------>|                             |
      |                      |<--OverlayFS CoW Diff + Context Tree (Turn #4)----------|                             |
      |<--8.Restore & Inject-|                          |                             |                             |
      |   Negative Memory    |                          |                             |                             |
      |--9.Turn #5 (Retry)---|--10.StreamTurn(Fence 1,9, upgrade_effort="max")------------------------------------->|
      |                      |                                                                       [Route -> DeepSeek (max 5x)]
```

### 5.3 时序三：宿主机突发宕机、`Epoch + 1` 租约抢占、僵尸节点拦截与跨节点断点续跑

```
Zombie Node-01         ansts-lease-coordinator         Healthy Node-02         ansts-checkpoint-manager        ansts-saga-tool-gateway
      |                           |                           |                           |                               |
      | (网络分区/内核假死 12s)    |                           |                           |                               |
      |                           |--1.心跳超时: CAS 递增租约  |                           |                               |
      |                           |   Fence(ep=1,t=6)->(2,6)  |                           |                               |
      |                           |--2.ResumeSandbox(Fence 2,6)-------------------------->|                               |
      |                           |                           |--3.PagedRestore(ep=1,t=6)>|                               |
      |                           |                           |<--Turn #6 WAL Snapshot----|                               |
      |                           |                           |--4.从 Turn #7 无损续跑    |                               |
      |                           |                           |                           |                               |
      |--5.假死恢复! 携带旧栅栏 Fence(ep=1, t=6) 尝试调用外部写工具-----------------------|------------------------------>|
      |<--6.HTTP 409 StaleFenceRejected (当前合法栅栏为 [2, 7] > [1, 6])------------------|-------------------------------|
      |                                                                                                                   |
 [收到 409 立即触发本地 cgroup.kill = 1 自毁僵尸沙箱，实现 0 脑裂、0 脏写]
```

---

## 6. Service-Level High Availability, Scaling & Deployment Topology (微服务高可用与混合部署拓扑)

### 6.1 控制面与数据面混合部署架构

在生产环境中，ANSTS 采用 **“Kubernetes 控制面集群 + 裸金属/支持嵌套虚拟化的 KVM 沙箱工作节点池”** 的混合部署拓扑：

1. **控制面微服务集群（Kubernetes Control Plane Cluster）**：
   * `ansts-api-gateway`、`ansts-cognitive-router`、`ansts-saga-tool-gateway`、`ansts-checkpoint-manager`、`ansts-event-notifier` 均以无状态 `Deployment` 部署，配置 HPA（基于 CPU 与 gRPC 活跃流数自动在 `3 ~ 64` 副本间弹性扩缩），跨 3 个可用区（Multi-AZ）反亲和分布。
   * `ansts-queue-manager` 与 `ansts-core-scheduler` 采用 `StatefulSet` + 一致性哈希分片环（Consistent Hash Ring）部署：按 `tenant_id` 分片并行出队，任何分片实例故障在 `< 3s` 内由相邻分片接管。
2. **沙箱数据面节点池（Bare-Metal / Metal-as-a-Service Worker Fleet）**：
   * 每台物理工作节点直接运行 Linux 6.6+ 内核（开启 Cgroups v2、PSI、KVM 与 OverlayFS），以 Systemd 服务或特权 DaemonSet 运行单个 `ansts-node-daemon`。
   * 本地 NVMe SSD 划分为两个高速区：`Warm Pool Snapshot Cache`（存放预热 MicroVM 内存镜像）与 `Local TurnWAL RingBuffer`（存放最近 10 轮增量块供对等节点毫秒级拉取）。

### 6.2 各微服务生产级 SLA 与容灾降级策略表

| 微服务名称 | 部署副本形态 | 单实例故障 RTO | 极端故障下的优雅降级策略（Graceful Degradation） |
| :--- | :--- | :--- | :--- |
| **`ansts-api-gateway`** | 无状态多活（$\ge 6$ 副本，Anycast L7 LB） | `0ms`（LB 自动摘除） | 当下游队列满载时直接基于本地缓存水位返回 `HTTP 429` + `Retry-After` |
| **`ansts-bayesian-profiler`** | 无状态多活（$\ge 3$ 副本） | `< 50ms` | 若画像服务不可用，调度器自动回退至 `0.5 * max_total_tokens` 静态保守预留，不阻塞任务准入 |
| **`ansts-queue-manager`** | 一致性哈希分片（每分片主备热同步） | `< 2s` | 分片切换期间网关短暂缓冲新提交任务（最长 5s），已在跑沙箱不受任何影响 |
| **`ansts-core-scheduler`** | 分片并行 + Etcd/TiKV Lease 选主 | `< 3s` | 调度器重启期间，所有运行中沙箱继续在宿主机完成当前 Turn 循环 |
| **`ansts-cognitive-router`** | 无状态多活（$\ge 8$ 副本） | `< 100ms`（下一轮自动重连） | 主池 `DeepSeek` 异常时自动降级至 `Qwen-3.8` 或 `GLM-5.3`；令牌桶中心失联时退化为单实例本地限流 |
| **`ansts-saga-tool-gateway`** | 无状态多活（$\ge 6$ 副本） | `< 200ms` | 依靠 `Idempotency-Key` 与持久层 Saga 日志安全重试 |
| **`ansts-node-daemon`** | 每台宿主机单进程（状态持久化于本地磁盘） | 进程重启 `< 1s`（不杀沙箱） | Daemon 采用 **Live-Upgrade 无损热重启** 设计：重启期间已有 Firecracker 进程保持运行，重连 `AF_VSOCK` 即可恢复接管 |
---

## 7. TypeScript 代码仓模块映射与 `celld` / Cloudflare / `RustFS` 部署同步 (v2.1)

本章节同步展示当前已完成全量编码闭环并通过 20 项 CI 测试的 **TypeScript 源码模块映射关系**：

| 领域层 / 子系统 | TypeScript 源码文件 (`src/**/*.ts`) | 核心类 / 接口名称 | 关联测试文件 (`tests/*.test.ts`) |
| :--- | :--- | :--- | :--- |
| **核心领域模型与双时钟预算** | `src/models.ts` | `FenceToken`, `ResourceVector5D`, `AgentTask` (`advanceClocks`, `GRACEFUL_DRAINING`), `EscrowVoucher`, `TurnWalFrame` | `integration_suite.test.ts`<br>`code_audit_improvements.test.ts` |
| **接入网关与贝叶斯画像** | `src/gateway/validator.ts`<br>`src/gateway/server.ts`<br>`src/gateway/backpressure.ts`<br>`src/gateway/sse_stream.ts`<br>`src/profiler.ts` | `TaskSpecValidator`, `ApiGatewayServer`, `IngressBackpressureController`, `SseEventStreamGateway`, `BayesianTaskProfiler` (`toJSONString`) | `chaos_benchmarks.test.ts`<br>`code_audit_improvements.test.ts` |
| **多租队列、DAG 支票与 5D 调度** | `src/queue_manager.ts`<br>`src/dag/escrow.ts`<br>`src/scheduler.ts`<br>`src/lease_coordinator.ts` | `MultiTenantQueueManager`, `DagEscrowCoordinator`, `CoreScheduler5D` (`hibernatedResumeQueue`), `LeaseCoordinator` | `integration_suite.test.ts`<br>`code_audit_improvements.test.ts` |
| **认知动态路由与四级 Saga 网关** | `src/cognitive_router.ts`<br>`src/router/pool_client.ts`<br>`src/router/breaker_limiter.ts`<br>`src/saga_gateway.ts`<br>`src/saga/compensator.ts`<br>`src/saga/staging.ts` | `CognitiveRouter` (`DeepSeek`/`GLM-5.3`/`Qwen-3.8`), `ModelPoolClientAdapter`, `ThreeStateCircuitBreaker`, `SlidingWindowTokenBucket`, `SagaToolGateway`, `SagaCompensatorEngine`, `StagingCommitAndApprovalGate` | `chaos_benchmarks.test.ts`<br>`code_audit_improvements.test.ts` |
| **相变沙箱守护与 PID 1 Harness** | `src/node_daemon.ts`<br>`src/runtime/warm_pool.ts`<br>`src/runtime/phase_controller.ts`<br>`src/runtime/psi_guard.ts`<br>`src/runtime/vsock_bridge.ts`<br>`src/harness/runner.ts`<br>`src/harness/velocity_breaker.ts` | `NodeDaemon`, `WarmSandboxPoolManager`, `CgroupPhaseController`, `LinuxPsiMemoryGuard`, `ZeroTrustVsockBridge`, `AgentHarnessRunner`, `SyntacticAndVelocityBreaker` | `integration_suite.test.ts`<br>`chaos_benchmarks.test.ts` |
| **增量快照、时光机重放与通知** | `src/checkpoint/wal_engine.ts`<br>`src/checkpoint/compactor.ts`<br>`src/checkpoint/time_travel.ts`<br>`src/telemetry/cloudevents.ts`<br>`src/telemetry/outbox_webhook.ts` | `TurnWalEngine`, `WalCompactorAndPagedRestorer`, `DeterministicTimeTravelReplayer`, `CloudEventsBus`, `TransactionalOutboxDispatcher` | `chaos_benchmarks.test.ts` |
| **`celld` & Cloudflare 边缘协同层** | `src/cloudflare/worker.ts`<br>`src/cloudflare/scheduler_cell.ts`<br>`src/cloudflare/harness_cell.ts`<br>`src/cloudflare/env.ts`<br>`wrangler.json` / `migrations/0001_init_ansts.sql` | `cloudflareCelldWorker`, `AnstsSchedulerCell`, `AgentTaskHarnessCell` (`rehydrateFromSql`), `CloudflareCelldEnv` | `celld_cloudflare.test.ts`<br>`code_audit_improvements.test.ts` |
| **`RustFS` + `celld` 生产部署标本** | `deploy/rustfs-celld/start-rustfs-celld.sh`<br>`deploy/rustfs-celld/docker-compose.rustfs-celld.yml`<br>`deploy/rustfs-celld/systemd/celld.service` | RustFS (`:9000` S3 API / `:9001` Console, UID `10001`) + 3 节点 `celld` (`--bucket s3://ansts-fleet --endpoint http://rustfs:9000`) | `celld_cloudflare.test.ts` |
