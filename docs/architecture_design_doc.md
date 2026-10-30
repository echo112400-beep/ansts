# Google Engineering Design Doc: 通用 Agent 沙箱任务调度系统 (ANSTS v2.0 全量架构规范)

| 文档属性 (Document Metadata) | 详细内容 (Specification Details) |
| :--- | :--- |
| **文档标题 (Title)** | **Agent-Native Sandbox Task Scheduler (ANSTS): 多租户、双相脉冲与认知强度感知的分布式沙箱任务调度系统** |
| **文档状态 (Status)** | **APPROVED / SYNCHRONIZED PRODUCTION SPECIFICATION (v2.1 — TypeScript + celld/Cloudflare + RustFS Edition)** |
| **作者 (Authors)** | 分布式 Agent 基础设施与沙箱运行时架构组 |
| **评审人 (Reviewers)** | 计算调度组 (Compute Scheduling)、模型网关组 (LLM Gateway)、安全隔离组 (Sandbox Security)、SRE 韧性工程组 |
| **最后更新 (Last Updated)** | 2026-10-30 (全量同步 TypeScript 实现、celld/Cloudflare 脑体分离架构、RustFS 启动标本与 5 项代码走查加固) |
| **核心模型池 (Target Model Pools)** | `DeepSeek` (旗舰深度推理池)、`GLM-5.3` (高速轻量/只读轮次分流池)、`Qwen-3.8` (高可用热备与弹性容灾池) |
| **隔离运行时 (Isolation Drivers)** | Firecracker MicroVM (默认生产级隔离)、Kata Containers (重型/GPU 负载)、gVisor (`runsc`)、瞬态重型执行器池 |

---

## Executive Summary (执行摘要)

**通用 Agent 沙箱任务调度系统（Agent-Native Sandbox Task Scheduler, ANSTS）** 是一套专为自主智能体（Autonomous AI Agents）工作负载设计的分布式多租户调度与运行时编排系统。传统的容器或批处理调度器（如 Kubernetes Kube-Scheduler、Google Borg、Volcano、YARN）假设工作负载仅消耗静态的本地物理计算资源（vCPU、Memory），且进程在生命周期内的资源占用相对平稳。然而，Agent 沙箱负载本质上是 **“本地物理沙箱算力”与“远端大模型推理配额”高度耦合的双域非稳态系统**。

本设计文档遵循 **Google Engineering Design Document (Design Doc)** 规范，面向生产级 Linux MicroVM 沙箱环境输出最终全量架构方案。核心工程创新包括：

1. **融合双相脉冲与推理强度的五维主导资源公平调度算法（5D Phase & Effort-Aware W-DRF）**：将稳态期望算力（vCPU）、突发算力（Burst vCPU）、稳态期望内存（Memory）、并发子会话槽位（Sessions）以及经推理强度（Reasoning Effort）折算的有效令牌吞吐率（Effective TPS & Token Rate）统一纳入多租帕累托公平分配模型，配合对数老化（Aging）与最早截止时间优先（EDF）彻底消除低优任务饿死。
2. **单轮内部双相脉冲弹性沙箱（Bi-Modal Phase-Elastic Sandbox）**：针对 Agent 在单轮 Turn 内 **60%~85% 时间处于等待大模型流式输出的“认知思考期（CPU < 2%）”、仅 15%~40% 时间处于“工具执行突发期”** 的物理特征，通过 Cgroups v2 与 `virtio-balloon`/`zram` 实现思考期 `0.1 vCPU / 256MB` 与工具期 `2.0~8.0 vCPU / 4GB` 的 `< 5ms` 原地垂直相变伸缩。
3. **无死锁多智能体 DAG 协同调度（Quota Escrow & Straggler Boost）**：通过 **父子配额支票托管（Quota Escrow Voucher）** 与父任务阻塞微休眠让渡机制，从数学上根除“父占槽位等子、子在队列等槽位”的环路死锁；同时支持 Join 屏障慢分支动态提权（Straggler Acceleration）与 Race 投机分支级联剪枝。
4. **Turn 级动态认知路由与三层异常熔断体系**：支持在 `DeepSeek`（复杂规划与推理）、`GLM-5.3`（只读查看与轻量解析分流）和 `Qwen-3.8`（429 限流无感热备接管）之间按轮次特征动态路由；引入滑动窗口进展率指标（Progress Velocity $\mathcal{V}_k$），对陷入语义原地踏步的 Agent 自动执行 **树搜索回滚（Backtrack-then-Break）**。
5. **四级工具事务网关（Saga Gateway）、双元组防脑裂栅栏与零 Token 时光机重放**：宿主机 VSOCK 网关对工具调用实施四级副作用管控（`PURE_READ`、`IDEMPOTENT_WRITE`、`SAGA_COMPENSATABLE`、`STAGED_COMMIT`），确保异常恢复与回滚时对外部生产系统 **零脏写**；通过 `Fence(epoch, turn_seq)` 双元组栅栏杜绝网络分区脑裂，并依托增量预写日志（Turn-WAL）实现 `RPO = 0 已确认轮次` 的秒级跨节点断点续跑与确定性重放。

---

## 1. Context and Scope (背景与问题范围)

### 1.1 Background & Problem Statement (背景与传统调度器四大结构失配)

现代 Agent 任务（如软件工程研发 Agent、金融流水审计 Agent、深度研究多智能体集群）运行在隔离的 Linux 沙箱中，由 **Agent Harness** 驱动执行多轮 `Thought -> Tool Call -> Sandbox Execution -> Observation` 闭环。直接在传统容器调度器上运行此类任务会遭遇以下四大根本性结构失配：

| 失配维度 | 传统容器调度器假设 (Borg / K8s) | Agent 沙箱工作负载真实物理特征 | 缺乏 ANSTS 时的线上生产故障表现 |
| :--- | :--- | :--- | :--- |
| **1. 双域耦合资源消耗** | 任务仅消耗宿主机本地 CPU、内存与磁盘带宽。 | 沙箱同时消耗 **本地物理计算资源**（vCPU、RAM、PIDs、子会话槽位）与 **远端大模型推理配额**（`DeepSeek` / `GLM-5.3` / `Qwen-3.8` 的 TPS、RPM、总 Token 预算与推理强度非线性放大）。 | 当远端 `DeepSeek` 模型池发生限流时，盲目拉起本地沙箱导致数千台 MicroVM 空转占满内存；高推理强度（`max=5.0x`）任务瞬间打满共享 TPS 导致同租户任务饿死。 |
| **2. 单轮双相脉冲特征** | 容器在生命周期内的 CPU/内存使用率近似稳态分布。 | 单轮 Turn 呈现显著双相交替：**认知思考期**（10~60s 等待 LLM 首字与流式生成，沙箱 CPU `< 2%`）与 **工具执行期**（1~15s 编译代码/跑测试/无头浏览器，CPU 瞬间飙至 `800%`）。 | 若按峰值（8C/8G）静态预留，集群实际平均 CPU 利用率 **不足 12%**；若简单静态超卖，多沙箱同时进入编译期将直接触发宿主机内核 OOM Killer。 |
| **3. 动态多智能体 DAG 死锁** | 作业 DAG 拓扑在提交前静态已知（如 Spark Stage）。 | 主 Agent 在运行中途动态派生 $M$ 个子 Agent 并阻塞等待汇聚（`Join`）或竞争探索（`Race`）。 | 高负载下，所有活跃槽位被处于 `WAITING_CHILDREN` 的父沙箱占满，而子沙箱在队列中等待父沙箱释放槽位，引发 **经典持有并等待（Hold-and-Wait）集群死锁**。 |
| **4. 语义死循环与外部副作用** | 进程失败判定简单（退出码 $\neq 0$），无状态重试安全。 | Agent 常陷入 **语义级停滞**（每轮微调参数绕过语法去重，但反复失败无实质进展），且工具调用具有外部写副作用（写库、发邮件、推分支）。 | 失控任务单次烧穿数百美元 Token 预算；节点宕机恢复或回滚重试时重复执行非幂等工具，造成生产数据污染。 |

### 1.2 Core Design Philosophy (核心设计哲学)

为解决上述痛点，ANSTS 确立了以下四项底层设计准则：

1. **无中心协调器的条件写租约（Coordinator-Free CAS Leasing）**：避免在高频 Turn 路径上引入重型多副本锁服务，直接利用状态存储/对象存储的原子条件写（Compare-And-Swap，`If-Match` / `If-None-Match`）与单调递增二元组栅栏 `Fence(epoch, turn_seq)` 保障全局唯一写入者。
2. **基于 Turn 边界的增量快照与按需分页恢复（Turn-Boundary CoW Snapshot & Paged Restore）**：在每轮 Turn 结束时仅刷出对话 DAG 增量与 OverlayFS 4KB 脏页差异块（`TurnWAL`），支持对等节点快速复制与跨节点按需稀疏分页加载，消除全量虚拟机镜像传输开销。
3. **长等待零成本休眠（Wait-State Hibernation）**：在外部人工审批、模型池限流退避或父等子屏障等待期间，立即卸载活跃计算上下文并保留 CoW 磁盘快照，使等待期算力持有成本降低 `> 95%`。
4. **认知与算力解耦的细粒度路由（Turn-Granular Cognitive Routing）**：以单轮 Turn 为最小路由粒度，根据当前轮次的任务性质与模型池健康度，在 `DeepSeek`、`GLM-5.3`、`Qwen-3.8` 之间动态切换模型与推理强度。

---

## 2. Goals and Non-Goals (设计目标与非目标边界)

### 2.1 Goals & Quantitative SLOs (核心设计目标与量化指标)

1. **多租五维帕累托公平与零饿死保证**：通过 5D W-DRF 算法确保计算密集型与 Token 密集型租户间的严格公平共享；通过 Aging 对数老化与 EDF（最早截止时间优先）动态提权，确保 `P3/P4` 低优任务 100% 不会发生无限期饿死。
2. **亚百毫秒级预热沙箱派发（`P50 < 65ms`, `P99 < 180ms`）**：结合节点预热池（Warm MicroVM Pool）与基础镜像层亲和度打分，实现任务准入后的极速沙箱就绪。
3. **高密相变弹性超卖（物理算力打包密度提升 `3.5x ~ 5.0x`）**：利用认知思考期与工具执行期的双相脉冲错峰复用物理核与内存，配合节点级突发信号量（Burst Semaphore）与 Linux PSI 内存压力主动休眠，保证宿主机级非预期 OOM 发生率 `< 0.01%`。
4. **低成本自适应认知分流（综合推理成本降低 `> 35%`）**：通过 Turn 级认知路由器自动识别只读/解析轮次并分流至轻量池 `GLM-5.3`，仅在复杂规划与错误诊断轮次使用旗舰池 `DeepSeek`。
5. **绝对无脑裂与零已确认轮次丢失（`Split-Brain = 0`, `RPO = 0 Confirmed Turns`, `RTO < 800ms`）**：通过 `Fence(epoch, turn_seq)` 双元组栅栏拦截 100% 僵尸节点写请求与工具调用；节点崩溃后在存活节点无损恢复至最新已完成 Turn。
6. **有界故障半径与四级双向闭环背压**：在 4 轮内识别语义停滞并完成树回滚与 Saga 逆向补偿；当主模型池 `DeepSeek` 出现 HTTP 429 风暴时在下一轮 Turn 无感切换至热备池 `Qwen-3.8`。

### 2.2 Non-Goals (非目标边界)

1. **不负责底层大模型训练与微调**：ANSTS 严格定位于 Agent 推理期与沙箱工具执行期的运行时调度器，不涉及分布式模型权重训练。
2. **不替代模型推理框架内部的算子调度**：ANSTS 通过标准网关协议与模型服务集群（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）交互，不侵入推理引擎内部的 GPU 算子与显存页管理。
3. **不维护定制化 Linux 内核分支**：所有沙箱隔离与资源相变均基于标准 Linux Kernel Cgroups v2、Namespaces、`virtio-vsock`、`virtio-balloon`、OverlayFS 及主线 Firecracker / Kata / gVisor 实现。

---

## 3. System Architecture Overview (系统总体架构)

### 3.1 Five-Layer Decoupled Architecture (五层解耦逻辑架构图)

```
+----------------------------------------------------------------------------------------------------------+
|                     第一层：智能准入与贝叶斯画像网关层 (Ingress & Bayesian Admission Layer)                 |
|  - TaskSpec v1beta1 静态校验与默认值注入     - 幂等键去重 (Idempotency-Key SHA-256)                       |
|  - 任务指纹在线贝叶斯画像 (P50 乐观软预留)   - L3 客户端准入背压 (HTTP 429 / Retry-After / QueuePosition) |
+----------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v
+----------------------------------------------------------------------------------------------------------+
|             第二层：多租队列、DAG 拓扑与多维联合调度层 (Unified Multi-Dimensional Scheduler)                |
|  +----------------------------------+  +-------------------------------+  +---------------------------+  |
|  | 分级多租队列树 (H-WFQ & 5D W-DRF) |  | DAG 拓扑与配额托管引擎         |  | 节点与模型池联合匹配器      |  |
|  | - L1 商业等级 -> L2 租户 -> P0~P4|  | - 父子配额支票托管 (防死锁)    |  | - 预热沙箱池 Warmth 亲和   |  |
|  | - 双相期望算力 + 强度折算有效TPS  |  | - Join 屏障慢分支动态提权     |  | - 突发算力余量与装箱均衡   |  |
|  | - Aging 对数老化 + EDF 截止提权   |  | - Race 投机模式级联剪枝       |  | - DeepSeek/GLM/Qwen 水位  |  |
|  +----------------------------------+  +-------------------------------+  +---------------------------+  |
+----------------------------------------------------------------------------------------------------------+
                                                     |
                       CAS 租约获取 + 双元组全链路栅栏派发: Fence(epoch, turn_seq)
                                                     |
                                                     v
+----------------------------------------------------------------------------------------------------------+
|              第三层：自适应认知路由、三层熔断与双向背压控制中枢 (Resilience & Cognitive Control)            |
|  - Turn 级动态认知路由器: 在 DeepSeek (旗舰主池)、GLM-5.3 (轻量快池)、Qwen-3.8 (热备容灾池) 间按轮次切换    |
|  - 三层异常熔断网: L1 AST语法/Cgroup越界熔断 + L2 进展率(Progress Velocity)树回滚 + L3 模型池429自动切换   |
|  - 四级双向背压环: 下游 429/节点PSI过载向上反压 <===> 上游慢消费SSE窗口/Token预算逼近红线向下节流           |
+----------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v
+----------------------------------------------------------------------------------------------------------+
|            第四层：相变弹性沙箱运行时与事务化工具网关层 (Phase-Elastic Runtime & Saga Gateway)             |
|  +----------------------------------------------------------------------------------------------------+  |
|  | Host Worker Daemon (Rust 编写，每台物理机/宿主机部署单实例)                                         |  |
|  |  - 相变控制器: 思考期收缩至 0.1C/256MB/zram <--5ms--> 工具期解锁 2C~8C (受节点 Burst Semaphore 保护)|  |
|  |  - VSOCK 透明代理网关: Fence(epoch,turn) 强校验 | TPS 令牌桶 Pacing | 四级工具 Staging 与 Saga 补偿 |  |
|  |  - Turn-WAL 确定性录制器: 实时捕获 SSE Token 流 + 工具 I/O + 虚拟时钟种子 + OverlayFS 4KB CoW 差异块|  |
|  |  +------------------------------------------------------+  +------------------------------------+  |  |
|  |  | 隔离沙箱实例 (Firecracker MicroVM / Kata / gVisor)    |  | 瞬态重型执行器池 (Ephemeral Pods)  |  |  |
|  |  |  [agent-harnessd (PID 1)] <---> [子会话并发状态树]    |  |  按需挂载无头浏览器/GPU/重型编译容器  |  |  |
|  |  |  [内置 Bash/REPL 执行器]   <---> [/workspace 工作区]   |  |  执行完毕毫秒级卸载释放               |  |  |
|  |  +------------------------------------------------------+  +------------------------------------+  |  |
|  +----------------------------------------------------------------------------------------------------+  |
+----------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v
+----------------------------------------------------------------------------------------------------------+
|               第五层：运行时观测、CloudEvents 事件总线与持久化层 (Observability & Persistence)             |
|  - CloudEvents 1.0 实时事件流 (SSE / gRPC / Kafka) - 事务性发件箱通知器 (Transactional Outbox + HMAC)    |
|  - OpenTelemetry 全链路分布式追踪                  - 共享对象存储桶 (S3/GCS: CAS 租约与 Turn-WAL 快照链) |
+----------------------------------------------------------------------------------------------------------+
```

### 3.2 End-to-End Execution Lifecycle (端到端调度与运行数据流)

1. **声明式提交与贝叶斯画像定容**：客户端提交 `TaskSpec v1beta1`。接入网关校验幂等键后，提取任务指纹（租户、Harness 类型、主模型 `DeepSeek`、工具集签名），查询在线贝叶斯画像器获取历史 P50 Token 与执行时长分布，执行 **P50 乐观软预留**。
2. **多租 5D W-DRF 出队**：任务进入对应租户的 `P0~P4` 优先级泳道。调度器在每个 Tick 挑选五维加权主导份额 $D_t$ 最小的租户，并结合 Aging 老化补偿与 EDF 截止时间选出紧迫度最高的首部任务。
3. **节点×模型池联合匹配与双元组栅栏**：调度器联合评估候选工作节点（预热池 `warm_pool_id` 命中、突发算力余量、本地快照亲和度）与目标模型池（`DeepSeek` / `GLM-5.3` / `Qwen-3.8` 的可用 TPS 配额余量与首字延迟），通过状态存储 CAS 原子写入租约并颁发 `Fence(epoch=1, turn_seq=1)`。
4. **单轮内部双相弹性循环**：
   * **认知思考期（Cognitive Phase）**：`agent-harnessd` 发起推理请求，宿主机将沙箱 Cgroup 收缩至 `0.1 vCPU / 256MB`；若识别当前轮次为纯文件读取或格式解析，认知路由器自动将请求分流至 **`GLM-5.3`**（节省 75% 成本），复杂推理轮次则路由至 **`DeepSeek`**。
   * **工具执行期（Execution Phase）**：收到工具调用后，宿主机在 `< 5ms` 内从本地 `Burst Semaphore` 获取突发令牌，将沙箱 Cgroup 解锁至 `2.0~8.0 vCPU`；外部 MCP 调用经由宿主机 **Saga 事务网关** 拦截审计。
5. **Turn 边界增量快照与进展率审计**：每轮 Turn 结束时刷出 `TurnWAL` 增量块并计算进展率 $\mathcal{V}_k$。若连续 4 轮平均进展率 `< 0.10`，触发树搜索回滚至 4 轮前健康快照并执行 Saga 逆向补偿。

---

## 4. Detailed Design: Declarative Contract (`TaskSpec v1beta1`) & Normalization (详细设计一：任务规范与归一化)

### 4.1 Complete `TaskSpec v1beta1` Schema (YAML 完整规范定义)

```yaml
apiVersion: scheduler.agent.io/v1beta1
kind: AgentTask
metadata:
  task_id: "tsk_01j9z8k7m6n5p4q3r2s1t0u9v8"
  idempotency_key: "fin-audit-20261028-v2-001"
  tenant_id: "tnt_enterprise_fin_01"
  namespace: "prod-risk-analysis"
  priority_class: "high-interactive"            # 枚举: P0-critical | P1-high | P2-normal | P3-low | P4-spot
  priority_score: 860                           # 同优先级泳道内基础分 [0, 1000]
  created_at: "2026-10-28T12:00:00Z"
  custom_metadata:                              # 用户自定义键值元数据，全链路透传至调度过滤、计费账单与事件通知
    business_unit: "global-settlement"
    cost_center: "CC-9042"
    trigger_user: "usr_88412"
    workflow_run_id: "wf_99812"
    compliance_tier: "pci-dss-strict"

spec:
  # 1. 排队、抢占与贝叶斯画像策略
  scheduling_policy:
    queue_name: "fin-audit-dedicated"
    max_queue_wait_sec: 300
    sla_deadline: "2026-10-28T13:00:00Z"
    preemption_policy: "PreemptToHibernate"     # PreemptToHibernate (抢占时被抢占沙箱转入CoW休眠) | Never | YieldOnPressure
    affinity:
      warm_image_preferred: true
      locality_region: "us-west-2"
    auto_sizing:
      enabled: true                             # 启用基于历史指纹的 P50 乐观软预留与渐进式配额续签
      fingerprint_key: "fin-csv-audit-v3"

  # 2. 认知模型策略、推理强度与 Turn 级动态路由
  cognitive_policy:
    primary_model: "DeepSeek"                   # 主力旗舰深度推理模型池
    fast_utility_model: "GLM-5.3"               # 轻量高速模型池（用于只读查看、日志解析与简单工具轮次）
    fallback_models:
      - "Qwen-3.8"                              # 当主模型池触发 429/5xx 熔断时的首选无感热备模型池
      - "GLM-5.3"
    default_reasoning_effort: "high"            # minimal (1.0x) | low (1.2x) | medium (1.8x) | high (3.0x) | max (5.0x)
    thinking_budget_tokens: 16384               # 单轮思维链 (CoT) 最大推理 Token 预算
    temperature: 0.2
    max_context_window: 128000
    adaptive_turn_routing:
      enabled: true
      downgrade_to_fast_on_read_only_turns: true
      upgrade_to_max_on_tool_error_streak: 2    # 连续 2 次工具报错自动将下一轮 DeepSeek 推理强度提至 max

  # 3. 令牌流控配额与多智能体 DAG 协同策略
  quota_and_dag_policy:
    max_total_tokens: 600000
    max_input_tokens: 480000
    max_output_tokens: 120000
    reserved_tps: 150
    max_burst_tps: 500
    max_rpm: 60
    max_concurrent_sessions: 6
    max_total_sessions: 24
    dag_coordination:
      quota_escrow_delegation: true             # 允许父会话直接切分已获批配额签发支票给子会话，0ms 免排队防死锁
      parent_yield_on_blocking_join: true       # 父会话在 Join 屏障等待期间自动转入 HIBERNATED_YIELD 释放物理算力
      straggler_acceleration:
        enabled: true
        completion_ratio_trigger: 0.75          # 当同组 >=75% 兄弟分支完成时，慢分支自动继承闲置 TPS 并提权
        boost_tps_multiplier: 2.0
      speculative_race_mode: "cancel-losers"    # Race 模式任一分支达成目标断言后立即级联终止其余分支

  # 4. 执行用时预算与相变弹性沙箱环境 (Phase-Elastic Sandbox Environment)
  execution_and_sandbox:
    max_wall_clock_sec: 1800
    max_active_compute_sec: 600
    idle_hibernate_after_sec: 20
    max_turns: 45
    single_turn_timeout_sec: 120
    isolation_driver: "firecracker"             # firecracker | kata | gvisor | runc
    image:
      repository: "registry.internal/agent-runtime/python-data-science"
      tag: "3.12-cuda12-v4"
      digest: "sha256:4a8b9c2e1f0d3e5a7b6c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a"
      warm_pool_id: "pool-py312-ds"
    phase_elastic_resources:
      cognitive_phase:                          # 认知思考期（等待 DeepSeek/GLM-5.3/Qwen-3.8 响应时）
        cpu_cores: 0.1
        memory_active_mb: 256
        memory_balloon_reclaim: true
      execution_phase:                          # 工具执行期（5ms 内毫秒级解锁突发算力）
        cpu_request_cores: 2.0
        cpu_burst_limit_cores: 8.0
        memory_limit_mb: 4096
        ephemeral_disk_mb: 10240
        max_pids: 256
      ephemeral_heavy_executors:                # 重型工具外部瞬态容器卸载
        - trigger_tool: "playwright_browser"
          executor_pool: "pool-headless-chromium-gpu"
          max_attach_sec: 120
    network_policy:
      mode: "allowlist"
      allowed_domains: ["api.github.com", "internal-warehouse.corp.local"]
      max_bandwidth_mbps: 50
    storage_mounts:
      - mount_path: "/workspace/input"
        source_uri: "s3://tenant-fin-01/inputs/2026-10-28/"
        read_only: true
      - mount_path: "/workspace/output"
        source_uri: "s3://tenant-fin-01/outputs/tsk_01j9z8/"
        read_only: false

  # 5. 工具集权限与外部副作用四级事务契约 (Transactional Tools & Saga Spec)
  tools_and_saga_config:
    builtin_tools:
      - name: "bash_executor"
        enabled: true
        timeout_sec: 60
        deny_commands: ["rm -rf /", "mkfs", "dd", "shutdown"]
      - name: "file_editor"
        enabled: true
    mcp_servers:
      - name: "settlement-core-mcp"
        transport: "sse"
        endpoint: "http://mcp-gateway.internal/settlement"
        max_calls_per_minute: 30
        secret_env_refs: ["SETTLEMENT_API_TOKEN"]
    transactional_policies:
      - tool_name: "postgres_readonly_query"
        effect_class: "PURE_READ"               # 纯只读：结果写入 TurnWAL 日志，重放时直接从缓存返回
      - tool_name: "s3_report_upload"
        effect_class: "IDEMPOTENT_WRITE"        # 幂等写：网关自动注入确定性 Idempotency-Key
      - tool_name: "git_branch_push"
        effect_class: "SAGA_COMPENSATABLE"      # 可补偿写：回滚或熔断时按 LIFO 逆序自动调用补偿工具
        compensation_tool: "git_branch_delete"
      - tool_name: "settlement_ledger_post"
        effect_class: "STAGED_COMMIT"           # 暂存提交：写入宿主机 Staging Buffer，任务成功或审批通过后原子刷出
        require_human_approval: true
        approval_timeout_sec: 3600
        on_approval_wait: "hibernate_sandbox"

  # 6. Agent Harness 与多级语义韧性配置
  harness_and_resilience:
    harness_type: "react-plan-execute-v2"
    entrypoint_command: ["/opt/harness/bin/agent-runner", "--mode=autonomous"]
    system_prompt_template_ref: "tpl_fin_auditor_v3"
    task_instruction: "审计 /workspace/input 下的 Q3 交易流水 CSV，识别异常拆单行为并输出结构化报告至 /workspace/output/report.json"
    context_management:
      compaction_strategy: "sliding-summary"
      compaction_trigger_ratio: 0.80
      preserve_system_and_last_n_turns: 6
    checkpoint_policy:
      deterministic_wal: true
      checkpoint_every_turn: true
      snapshot_storage_backend: "s3://agent-checkpoints-prod/"
    semantic_circuit_breaker:
      ast_identical_call_limit: 3
      max_consecutive_tool_errors: 5
      progress_velocity_window_turns: 4
      min_progress_velocity_threshold: 0.10
      max_backtrack_attempts: 2

  # 7. 运行时事件流观测与高可靠通知
  observability_and_notify:
    event_stream:
      enabled: true
      granularity: "turn_and_tool"
      sink_topic: "kafka://agent-events-prod/tenant-fin-01"
    notifications:
      webhook_endpoints:
        - url: "https://orchestrator.corp.local/hooks/agent-callback"
          secret_header_ref: "WEBHOOK_HMAC_SECRET"
          events:
            - "agent.task.started"
            - "agent.tool.approval.requested"
            - "agent.quota.watermark.warning"
            - "agent.circuit_breaker.tripped"
            - "agent.task.completed"
          retry_policy:
            max_retries: 5
            initial_backoff_ms: 500
            max_backoff_ms: 30000
```

### 4.2 Five-Dimensional Resource Normalization Matrix (五维资源向量归一化矩阵)

调度器将每个任务 $j$ 的声明式配置实时映射为五维标准化资源需求向量 $\vec{R}_j = \langle \widetilde{\text{CPU}}_j, \text{CPU}_{\text{burst}, j}, \widetilde{\text{Mem}}_j, \text{Sess}_j, \widetilde{\text{TPS}}_j \rangle$：

| 资源维度 ($k$) | 关联 `TaskSpec` 字段 | 归一化数学折算公式 | 运行期硬件/网关强约束机制 | 超限处置策略 |
| :--- | :--- | :--- | :--- | :--- |
| **1. 稳态期望算力 ($\widetilde{\text{CPU}}_j$)** | `cognitive_phase.cpu_cores`, `execution_phase.cpu_request_cores` | $\rho_{\text{cog}} \cdot \text{CPU}_{\text{cog}} + (1 - \rho_{\text{cog}}) \cdot \text{CPU}_{\text{exec}}$ | Cgroups v2 `cpu.weight` & `cpu.max` | 工具执行期触发 CFS 带宽限流 |
| **2. 突发算力余量 ($\text{CPU}_{\text{burst}, j}$)** | `execution_phase.cpu_burst_limit_cores` | 工具调用瞬间申请的峰值物理核数 | 宿主机级 `Burst Semaphore` 令牌桶 | 宿主机 VSOCK 层微排队 100~500ms 削峰 |
| **3. 稳态期望内存 ($\widetilde{\text{Mem}}_j$)** | `cognitive_phase.memory_active_mb`, `execution_phase.memory_limit_mb` | $\rho_{\text{cog}} \cdot \text{Mem}_{\text{cog}} + (1 - \rho_{\text{cog}}) \cdot \text{Mem}_{\text{exec}}$ | Cgroups v2 `memory.high` / `memory.max` + `virtio-balloon` | 捕获沙箱内进程 OOM 并回滚至上一轮 Turn |
| **4. 并发子会话槽位 ($\text{Sess}_j$)** | `max_concurrent_sessions` | 当前同时激活的主/子 Agent 会话数 | 宿主机 VSOCK 会话信号量 | 拒绝派生新子会话（返回 `SessionQuotaExceeded`） |
| **5. 有效认知吞吐 ($\widetilde{\text{TPS}}_j$)** | `reserved_tps`, `default_reasoning_effort`, `max_total_tokens` | $\text{TPS}_{\text{reserved}, j} \cdot \alpha(e_j)$ | 分布式滑动窗口令牌桶（按 `DeepSeek`/`GLM-5.3`/`Qwen-3.8` 隔离） | 流式 Token Pacing 延迟注入或下调推理强度 |

---

## 5. Detailed Design: Core Scheduling & Routing Algorithms (详细设计二：核心调度与路由算法)

### 5.1 Online Bayesian Task Profiling & Optimistic P50 Reservation (在线贝叶斯画像与乐观软预留)

用户在配置 `max_total_tokens`（如 `600,000`）和 `max_wall_clock_sec`（如 `1800s`）时通常按最坏安全边界填写，而实际任务 P50 消耗往往仅为最大值的 `15%~25%`。若按静态上限锁死配额，会导致集群准入率极低。

1. **任务指纹提取**：网关计算结构化任务指纹 $h(j) = \text{SHA256}(\text{tenant\_id}, \text{harness\_type}, \text{primary\_model}, \text{tool\_set\_signature})$。
2. **在线共轭贝叶斯更新**：画像器为每个指纹 $h$ 维护累计 Token 消耗的对数正态分布 $X \sim \text{LogNormal}(\mu_h, \sigma_h^2)$ 以及思考期时间占比的 Beta 分布 $\rho_{\text{cog}} \sim \text{Beta}(\alpha_h, \beta_h)$。
3. **P50 乐观预留与渐进式续签（Progressive Lease Renewal）**：
   * 任务准入时仅冻结历史中位数 **P50 配额 $\widehat{Q}_{50}(h)$**。
   * 当运行中任务消耗达 $0.85 \times \widehat{Q}_{50}(h)$ 时，宿主机自动向调度器发起免排队渐进式续签，每次追加 $(\widehat{Q}_{85}(h) - \widehat{Q}_{50}(h))$ 直至用户声明的硬上限 `max_total_tokens`。该机制将集群并发准入吞吐提升 **`2.8x`**。

### 5.2 Hierarchical Multi-Tenant Queueing (H-WFQ) & 5D Phase/Effort-Aware W-DRF

#### (1) 双相脉冲期望算力与推理强度有效吞吐折算

设任务 $j$ 在单轮 Turn 内处于认知思考期（等待 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 流式输出）的时间占比为 $\rho_{\text{cog}, j} \in [0, 1]$，处于工具执行期的时间占比为 $\rho_{\text{exec}, j} = 1 - \rho_{\text{cog}, j}$。其稳态物理算力需求为：

$$\widetilde{\text{CPU}}_j = \rho_{\text{cog}, j} \cdot \text{CPU}_{\text{cog}, j} + \rho_{\text{exec}, j} \cdot \text{CPU}_{\text{exec}, j}, \qquad \widetilde{\text{Mem}}_j = \rho_{\text{cog}, j} \cdot \text{Mem}_{\text{cog}, j} + \rho_{\text{exec}, j} \cdot \text{Mem}_{\text{exec}, j}$$

定义推理强度（`reasoning_effort`）非线性算力放大系数 $\alpha(e_j)$：

$$\alpha(e_j) = \begin{cases} 1.0, & e_j = \text{minimal} \\ 1.2, & e_j = \text{low} \\ 1.8, & e_j = \text{medium} \\ 3.0, & e_j = \text{high} \\ 5.0, & e_j = \text{max} \end{cases}$$

由于高推理强度会产生大量内部思维链（CoT）推理 Token 并长时间占用推理并发流，任务 $j$ 的有效认知吞吐负载定义为：

$$\widetilde{\text{TPS}}_j = \text{TPS}_{\text{reserved}, j} \cdot \alpha(e_j)$$

#### (2) 五维租户加权主导份额（5D Weighted Dominant Share）

设集群五维总容量向量为 $\vec{C} = \langle C_{\text{cpu}}, C_{\text{burst}}, C_{\text{mem}}, C_{\text{sess}}, C_{\text{tps}} \rangle$，租户 $t$（权重 $w_t$）当前运行中任务集合为 $\mathcal{A}_t$，则租户 $t$ 的加权主导份额 $D_t$ 为：

$$D_t = \frac{1}{w_t} \max_{k \in \{\text{cpu}, \text{burst}, \text{mem}, \text{sess}, \text{tps}\}} \left( \frac{\sum_{j \in \mathcal{A}_t} \widetilde{R}_{j, k}}{C_k} \right)$$

* **跨租户调度准则**：每次调度循环总是优先选择 **未超过租户突发上限 $\vec{M}_t$ 且加权主导份额 $D_t$ 最小** 的活跃租户出队任务，实现多维异构资源下的帕累托最优与防吵闹邻居（Noisy Neighbor）隔离。

#### (3) 租户内对数老化与最早截止时间综合评分（Aging + EDF Score）

在被选中的租户内部，各优先级泳道（`P0~P4`）头部的候选任务 $j$ 按动态紧迫度得分 $S(j, \tau)$ 排序（$\tau$ 为当前时间戳）：

$$S(j, \tau) = P_{\text{base}}(j) + \beta \cdot \ln\left(1 + \frac{\tau - \tau_{\text{enqueue}}(j)}{T_{\text{aging}}}\right) + \gamma \cdot \mathbb{I}(\text{has\_deadline}) \cdot \max\left(0,\ \frac{T_{\text{horizon}}}{\max\big(1,\ \tau_{\text{deadline}}(j) - \tau\big)}\right)$$

* $P_{\text{base}}(j) \in [0, 1000]$：任务基础优先级分；
* $\beta \cdot \ln(1 + \Delta \tau / T_{\text{aging}})$：随排队等待时长平滑增长的对数老化补偿分（默认 $\beta = 65, T_{\text{aging}} = 30\text{s}$），确保低优任务等待足够长时间后必能跨过优先级阈值；
* 第三项为 **EDF 截止时间加速项**（默认 $\gamma = 180, T_{\text{horizon}} = 60\text{s}$）：当任务临近 `sla_deadline` 时快速抬升紧迫度。

### 5.3 Multi-Agent DAG Topology Scheduling: Escrow, Straggler Boost & Race Pruning

#### (1) 父子配额托管继承协议与防死锁数学证明（Quota Escrow Delegation）

当运行中的父任务 $P$ 调用 `spawn_sub_agents({c_1, \dots, c_M})` 并进入阻塞式 `Join Barrier` 时：

1. **父沙箱状态让渡**：父沙箱自动转入 `HIBERNATED_YIELD` 微休眠态，立即释放其占用的物理 $\text{CPU}_P$、$\text{Mem}_P$ 与活跃会话槽位；
2. **签发加密配额支票（Escrow Voucher）**：父任务将其已获准入的剩余 Token 预算与预留吞吐 $\text{TPS}_P$ 切分为带有父栅栏签名的配额支票 $\mathcal{E}(P \to c_i) = \langle \text{task\_id}_P, \text{epoch}_P, \text{tps}_i, \text{tokens}_i \rangle$（满足 $\sum_i \text{tps}_i \le \text{TPS}_P$）；
3. **免排队直通准入（Fast-Path Admission）**：持有合法 Escrow Voucher 的子任务直接进入 `escrow_fast_queue` 免排队启动；
4. **无死锁定理证明**：由于处于 `WAITING_CHILDREN` 的父任务不持有活跃计算槽位，且子任务消费的是父任务已预留的内部配额（对集群净新增配额需求为 0），资源等待图（Wait-For Graph）中不存在“活跃槽位持有者等待队列准入”的环路，从而 **从数学上严格根除父子死锁**。

#### (2) 关键路径慢分支动态加速（Straggler Acceleration）

设同一个 `Join Barrier` 下有 $M$ 个并行子任务，当前已完成 $M_{\text{done}}$ 个。当完成比例 $\frac{M_{\text{done}}}{M} \ge 0.75$ 时，尚未完成的慢分支 $c_s$ 自动继承所有已完成兄弟分支释放的闲置 TPS 配额并获得优先级提权：

$$S_{\text{dag}}(c_s, \tau) = S(c_s, \tau) + \Delta_{\text{crit}} \cdot \left( \frac{M_{\text{done}}}{M - M_{\text{done}}} \right), \qquad \text{TPS}_{\text{limit}}(c_s) = \min\left( \text{TPS}_{\text{burst\_max}},\ \text{TPS}_{\text{reserved}}(c_s) + \sum_{c_d \in \text{Done}} \text{TPS}_{\text{unused}}(c_d) \right)$$

#### (3) Race 投机模式级联剪枝（Speculative Race Cancellation）

对于 `speculative_race_mode: "cancel-losers"` 的多分支探索任务，一旦任一子分支 $c_w$ 率先通过验证断言，调度器在 `< 20ms` 内向同组其余分支下发级联中断指令（断开 VSOCK LLM 流 + `cgroup.kill = 1`），立即回收未消耗的 Token 预算。

### 5.4 Compute-Model Joint Placement & Turn-Level Cognitive Routing

#### (1) 算力节点 × 模型推理池联合匹配打分函数

每次调度任务 $j$ 时，调度器在可行工作节点集合 $\mathcal{N}_{\text{feas}}$ 与目标模型池集合 $\mathcal{M}_{\text{feas}}$（涵盖 `DeepSeek`、`GLM-5.3`、`Qwen-3.8`）上求取联合最优解 $(n^*, m^*)$：

$$\text{Score}(n, m, j) = \underbrace{\omega_1 \cdot \text{WarmSandbox}(n, j) + \omega_2 \cdot \text{BurstHeadroom}(n, j) + \omega_3 \cdot \text{DataLocality}(n, j)}_{\text{物理沙箱工作节点得分}} + \underbrace{\omega_4 \cdot \text{PoolQuotaHeadroom}(m, j) - \omega_5 \cdot \text{TTFT}_{\text{norm}}(m)}_{\text{大模型推理池健康度得分}}$$

1. **预热池与镜像亲和度（$\text{WarmSandbox}$，权重 $\omega_1 = 0.35$）**：优先选择已持有 `warm_pool_id` 休眠 MicroVM 或已缓存基础镜像 OverlayFS 层的节点，将启动耗时从数秒压缩至 `~65ms`。
2. **节点突发算力余量与装箱均衡度（$\text{BurstHeadroom}$，权重 $\omega_2 = 0.20$）**：评估节点 `Burst Semaphore` 空闲令牌数以及 CPU/内存余量比。
3. **本地快照亲和度（$\text{DataLocality}$，权重 $\omega_3 = 0.15$）**：对于休眠唤醒或断点恢复任务，优先调度至持有本地 NVMe L0 快照缓存的节点。
4. **模型池配额余量与首字延迟（$\text{PoolQuotaHeadroom} - \text{TTFT}_{\text{norm}}$，权重 $\omega_4 = 0.15, \omega_5 = 0.15$）**：实时评估目标模型池的可用 TPS 水位与 P95 首字延迟。

#### (2) Turn 级动态认知路由矩阵（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）

| 轮次特征与实时信号 | 路由目标模型池 | 生效推理强度 (`reasoning_effort`) | 路由决策收益与架构说明 |
| :--- | :--- | :--- | :--- |
| **初始架构规划 / 核心代码编写 / 复杂推理轮次** | **`DeepSeek`** (`primary_model`) | `high` ($3.0\times$) | 发挥旗舰深度推理与复杂规划能力，保障核心任务收敛质量。 |
| **纯只读文件查看 / 目录枚举 / 日志格式解析轮次** | **`GLM-5.3`** (`fast_utility_model`) | `minimal` ($1.0\times$) 或 `low` ($1.2\times$) | 自动将简单机械轮次分流至 `GLM-5.3` 高速池，单轮成本降低 **75%**，首字延迟降至 `0.18s`。 |
| **连续工具报错恢复（连续 $\ge 2$ 轮工具返回非零）** | **`DeepSeek`** (`primary_model`) | 动态提权至 `max` ($5.0\times$) | 临时解锁最大思维链预算（`16k CoT Tokens`）深度诊断复杂错误根因。 |
| **主模型池 `DeepSeek` 拥塞或触发 HTTP 429 熔断** | **`Qwen-3.8`** (`fallback_models[0]`) | `medium` ($1.8\times$) 或 `high` ($3.0\times$) | 在下一轮 Turn 边界毫秒级无感切换至热备池 `Qwen-3.8`，沙箱内存与磁盘状态零中断。 |

### 5.5 Core Scheduler Pseudocode (核心联合调度算法参考实现)

```python
class UnifiedAgentSchedulerV2:
    def run_scheduling_tick(self, now: float) -> None:
        # 1. 扫描所有活跃多智能体 DAG 屏障：执行 Race 级联剪枝与慢分支加速 (Straggler Boost)
        for dag in self.dag_registry.active_dags():
            if dag.speculative_race_mode == "cancel-losers" and dag.has_winning_branch():
                for loser in dag.running_branches():
                    self.terminate_branch_immediately(loser, reason="SPECULATIVE_RACE_WON_BY_SIBLING")
            elif dag.completion_ratio() >= dag.straggler_trigger_ratio:
                for straggler in dag.running_branches():
                    dag.transfer_unused_sibling_tps(straggler)
                    straggler.dynamic_priority_boost = 150

        # 2. 优先排空持有父任务配额支票 (Escrow Voucher) 的免排队子任务队列 (彻底防死锁)
        while self.escrow_fast_queue:
            sub_task, voucher = self.escrow_fast_queue.popleft()
            if self.verify_voucher_fence(voucher):
                n_star, m_star = self.select_optimal_placement(sub_task, voucher.quota)
                if n_star and m_star:
                    self.dispatch_with_fenced_lease(sub_task, n_star, m_star, voucher.quota)

        # 3. 构建候选租户最小堆：按五维加权主导份额 D_t 升序排列
        eligible_tenants = [
            t for t in self.tenants.values()
            if t.has_queued_tasks() and not t.is_backpressured() and not t.exceeds_burst_ceiling()
        ]
        for t in eligible_tenants:
            t.dominant_share = self.compute_5d_dominant_share(t)
        heapq.heapify(eligible_tenants)

        # 4. 跨租户 5D W-DRF 出队 + 贝叶斯 P50 预留 + (节点, 模型池) 联合匹配
        while eligible_tenants:
            tenant = heapq.heappop(eligible_tenants)
            candidate = tenant.peek_highest_aging_edf_task(now)

            # 基于任务指纹获取 P50 乐观软预留配额
            quota_slice = self.bayesian_profiler.get_p50_reservation(candidate)

            # 联合求解最佳沙箱节点 n* 与最佳模型池 m* (DeepSeek / GLM-5.3 / Qwen-3.8)
            n_star, m_star = self.select_optimal_placement(candidate, quota_slice)
            if not n_star or not m_star:
                if candidate.preemption_policy == "PreemptToHibernate":
                    victim = self.find_hibernatable_victim(candidate, quota_slice)
                    if victim:
                        self.graceful_hibernate_sandbox(victim, reason="PREEMPTED_BY_HIGH_PRIORITY")
                continue

            # 原子获取 CAS 租约并颁发双元组栅栏 Fence(epoch + 1, turn_seq = 1)
            tenant.dequeue(candidate)
            lease = self.state_store.acquire_cas_lease(
                task_id=candidate.id,
                node_id=n_star.id,
                model_pool=m_star.pool_name,
            )
            self.dispatch_with_fenced_lease(candidate, n_star, m_star, quota_slice, lease)

            if tenant.has_queued_tasks() and not tenant.exceeds_burst_ceiling():
                tenant.dominant_share = self.compute_5d_dominant_share(tenant)
                heapq.heappush(eligible_tenants, tenant)
```

---

## 6. Detailed Design: Task-Driven Phase-Elastic Sandbox Runtime (详细设计三：相变弹性沙箱运行时)

### 6.1 Decoupled Task & Sandbox State Machine (任务与沙箱解耦状态机)

```
                      +--------------------+
                      |     SUBMITTED      | (Schema 校验 + 幂等键登记 + 贝叶斯指纹提取)
                      +---------+----------+
                                |
                                v
                      +--------------------+      排队超时 (max_queue_wait_sec)
                      |       QUEUED       |-----------------------------------------+
                      +---------+----------+                                         |
                                | (5D W-DRF 准入 + CAS 租约 Fence(ep=1, turn=1))     |
                                v                                                    |
                      +--------------------+                                         |
                      |    PROVISIONING    | (预热池 MicroVM 克隆 ~65ms + 挂载卷)     |
                      +---------+----------+                                         |
                                |                                                    v
+------------------+  +---------+----------+  L2 进展率 < 0.10 (回滚次数 < 2)   +----+--------------+
|   HIBERNATED /   |<>|      RUNNING       |------------------------------------>|   BACKTRACKING    |
| HIBERNATED_YIELD |  | [思考期 0.1C/256M] |<------------------------------------| (恢复至 Turn k-4   |
| (释放CPU/内存,    |  |       <--5ms-->    |  注入负反馈防重记忆 + 提至 max 强度  |  + Saga 逆向补偿) |
|  保留CoW磁盘快照) |  | [工具期 2C~8C突发] |                                     +-------------------+
+------------------+  +----+----+-----+----+
         ^                 |    |     |
         | 审批/抢占/父等子  |    |     | 回滚 >= 2 次 / AST死循环 / Cgroup越界
         +-----------------+    |     +------------------------------------------+
                                | 宿主机宕机 / 心跳失联                           v
                      +---------v----------+                           +---------+---------+
                      |     RECOVERING     |                           |  CIRCUIT_BROKEN   |
                      | (Epoch+1 抢占租约   |                           | (执行 Saga 回滚    |
                      |  + Paged WAL 恢复) |                           |  并发送告警通知)   |
                      +---------+----------+                           +-------------------+
                                |
                                +--------------------+
                                                     | 完成所有 Turn 且 Staged 提交刷出成功
                                                     v
                                           +---------+---------+
                                           |     SUCCEEDED     |
                                           +-------------------+
```

### 6.2 Bi-Modal Phase-Elastic Resource Control & Host Anti-Avalanche Guardrails (双相伸缩与防雪崩三道防线)

在每台物理工作节点上，Rust 编写的 `Host Worker Daemon` 实时监听沙箱 `virtio-vsock` 通道上的阶段信令，在两个物理资源工作点之间执行 `< 5ms` 切换：

1. **认知思考期（`COGNITIVE_PHASE`）**：当 `agent-harnessd` 发出 LLM 推理请求并等待 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 返回流式 Token 时，宿主机立即将该沙箱 Cgroup v2 的 `cpu.max` 设为 `"10000 100000"`（`0.1 vCPU`），并通过 `virtio-balloon` 与 `memory.reclaim` 将冷匿名页压缩换出至宿主机 `zram`。
2. **工具执行期（`EXECUTION_PHASE`）**：当 LLM 输出工具调用指令（如执行 `cargo test` 或数据处理脚本）时，宿主机向本地突发池申请令牌，在 `< 5ms` 内将 `cpu.max` 提升至 `"800000 100000"`（`8.0 vCPU`），恢复完整内存水位。

#### 节点级防雪崩三道防线（Host Burst Guardrails）
为防止同一台物理机上的数十个沙箱在同一毫秒从“思考期”同时进入“工具编译期”引发瞬时 CPU 争抢或宿主机 OOM，设计三道硬防线：
* **防线一：节点突发令牌桶（Node Burst Semaphore）**：例如 32 核物理机最多允许 $K=4$ 个沙箱同时处于 8 核满载突发态。当第 5 个沙箱发起重型工具调用时，在宿主机 VSOCK 层微排队 `100~500ms`（对数秒级的 Agent Turn 几乎无感），平滑削峰。
* **防线二：基于 Linux PSI 的内存压力紧急休眠（PSI Pressure-Driven Hibernation）**：宿主机实时监测 `/proc/pressure/memory`。一旦 `some avg10 > 40%`，立即挑选处于 `COGNITIVE_PHASE` 的低优沙箱，将其内存零拷贝转储至本地 NVMe 并转入 `HIBERNATED`，确保正在执行工具的沙箱绝不触发宿主机 OOM Killer。
* **防线三：Turn 边界跨节点无缝热迁移（Live Turn Migration）**：若某节点连续 3 轮处于高水位，刚完成当前 Turn 快照的沙箱在下一轮 Turn 启动前直接通过 `< 50ms` 租约移交迁移至空闲节点唤醒。

---

## 7. Detailed Design: High Reliability, Fault Recovery, Circuit Breaking & Bidirectional Backpressure (详细设计四：高可靠、异常恢复、三层熔断与双向背压 — DFX)

### 7.1 Split-Brain Prevention via Dual-Tuple Fencing `(epoch, turn_seq)` (双元组全链路防脑裂栅栏)

在分布式环境下，网络分区或宿主机内核卡死极易导致旧节点上的“僵尸沙箱”恢复运行并与新节点同时写状态或调外部工具。本系统采用单调递增二元组栅栏 $\mathcal{F} = (\text{epoch}, \text{turn\_seq})$ 实现 **100% 零脑裂**：

```
+---------------------------+                           +-----------------------------------------+
| 假死恢复旧节点 Worker-01   |  携带过期栅栏发起写/工具请求 | 共享对象存储 / MCP 工具网关 /             |
| (网络分区恢复后的僵尸沙箱) |  Fence(epoch=1, turn=6)   | LLM 推理代理控制面                       |
+-------------+-------------+ ------------------------> +--------------------+--------------------+
              |                                                              |
              |  <--- HTTP 409 StaleFenceRejected ---------------------------+
              |       (当前合法栅栏已推进至 Fence(epoch=2, turn=6))           |
              v                                                              |
 [宿主机收到 409 立即写入 cgroup.kill = 1 毫秒级自毁本地僵尸沙箱]               |
                                                                             |
+---------------------------+                           +--------------------+--------------------+
| 新接管节点 Worker-02       |  携带合法栅栏发起写/工具请求 | 字典序偏序校验通过:                       |
| (CAS 赢下租约合法接管)     |  Fence(epoch=2, turn=6)   | (2, 6) >= (2, 6) -> 原子落盘并推进至(2,7)|
+---------------------------+ ------------------------> +-----------------------------------------+
```

1. **严格字典序偏序法则**：对于任意两个栅栏 $\mathcal{F}_1 = (e_1, s_1)$ 与 $\mathcal{F}_2 = (e_2, s_2)$，当且仅当 $(e_1 > e_2) \lor (e_1 = e_2 \land s_1 \ge s_2)$ 时允许通过。
2. **三大基础设施强拦截点**：
   * **对象存储层**：快照对象 Key 编码为 `s3://checkpoints/<task_id>/ep_<epoch>/turn_<turn_seq>.wal` 并启用 `If-None-Match: *` 条件写，物理杜绝旧节点覆盖新节点数据；
   * **LLM 代理网关层**：每次推理请求必带 `X-Agent-Fence: <epoch>:<turn_seq>`，过期请求立即中断 SSE 流，防止僵尸沙箱偷烧租户 Token；
   * **MCP 工具网关层**：所有外部工具调用前置校验栅栏，确保僵尸沙箱无法对外产生任何网络副作用。

### 7.2 Turn-Level Incremental WAL Checkpointing & Deterministic Time-Travel Replay (断点恢复与零 Token 时光机重放)

1. **Turn 级增量快照（`RPO = 0 Confirmed Turns`）**：每完成一轮 Turn，宿主机将对话树、环境变量与 OverlayFS 4KB 脏页差异块打包为紧凑的 `TurnWAL` 增量段（`ep_02/00001-0000k.wal`），同步复制到对等节点磁盘（`~14ms`）并上传对象存储，后台自动执行 L0 $\to$ L1 合并压缩。
2. **跨节点秒级断点续跑（`RTO < 800ms`）**：节点宕机后，新节点通过 CAS 将 `epoch` 加 1，从预热池克隆 MicroVM（`~65ms`），通过 **Paged Restore（按需稀疏分页恢复）** 挂载最新 Turn 快照并注入恢复提示，直接从中断的那一轮 Turn 继续推理，此前所有已完成轮次的 Token 与计算成果 **100% 保留**。
3. **零 Token 确定性时光机重放（Time-Travel Replay）**：由于 `TurnWAL` 完整录制了每轮 LLM 原始流、工具 I/O 字节、虚拟时钟偏移与随机数种子，当线上任务在第 25 轮异常失败时，开发者可在 **不调用 `DeepSeek`、不消耗任何 Token** 的前提下，于 `< 200ms` 内重放前 24 轮状态并精准停在第 25 轮现场进行调试或切换模型续跑。

### 7.3 Three-Tier Circuit Breaking Architecture (三层异常熔断体系)

| 熔断层级 | 熔断器名称 | 实时检测信号与数学判定规则 | 熔断触发后的自动化处置动作 |
| :--- | :--- | :--- | :--- |
| **第一层：单任务语法与资源守卫** | **AST 工具死循环与连续报错熔断器** | 连续 $\ge 3$ 次调用同一工具且参数 AST 归一化哈希完全相同、$A \to B \to A \to B$ 周期振荡，或连续 $\ge 5$ 次工具返回非零退出码。 | 第 1 次触发：注入系统级强制反思提示并禁用该工具签名 1 轮；<br>第 2 次触发：直接熔断为 `CIRCUIT_BROKEN`。 |
| **第一层：单任务语法与资源守卫** | **Token 激增与 Cgroup 越界熔断器** | 单轮消耗超过总预算 35%、模型退化输出重复字符，或 eBPF/Cgroups 监测到触及 `max_pids` / 扫描非授权端口。 | 立即切断当前 SSE 生成流，冻结沙箱 Cgroup，回滚至上一轮快照并切换至 `Qwen-3.8` 重试一次。 |
| **第二层：语义轨迹进展率守卫** | **进展率（Progress Velocity $\mathcal{V}_k$）语义熔断器** | 连续 4 轮滑动窗口平均进展率 $\overline{\mathcal{V}}_{k-3:k} < 0.10$（计算公式见下方）。 | **树搜索回滚（Backtrack-then-Break）**：若回滚次数 `< 2`，将磁盘与上下文一键回滚至 $k-4$ 轮快照，执行 Saga 逆向补偿，注入负反馈防重记忆并将 `DeepSeek` 推理强度提至 `max`；若连续 2 次回滚仍停滞则置为 `CIRCUIT_BROKEN`。 |
| **第三层：全局模型池基础设施守卫** | **自适应模型池熔断器（`DeepSeek` / `GLM-5.3` / `Qwen-3.8`）** | 30s 滑动窗口内模型池 HTTP `429/5xx` 错误率 $> 25\%$ 或首字延迟 TTFT P95 $> 15\text{s}$。 | 熔断器由 `CLOSED` 转入 `OPEN`；所有运行中任务在下一轮 Turn 自动无感切换至热备池 `Qwen-3.8`；每 10s 放行 5% 探测流量（`HALF-OPEN`）直至恢复。 |

#### 语义进展率（Progress Velocity $\mathcal{V}_k$）数学定义
在每轮 Turn $k$ 结束时，系统综合测试通过率增量、错误签名消减率、有效代码修改信息熵增与文件树振荡惩罚计算 $\mathcal{V}_k$：

$$\mathcal{V}_k = w_{\text{test}} \cdot \Delta \text{PassRate}_k + w_{\text{err}} \cdot \text{ErrorSetReduction}_k + w_{\text{diff}} \cdot \text{NovelDiffEntropy}_k - w_{\text{osc}} \cdot \text{OscillationPenalty}_k$$

若当前工作区 Merkle 树哈希与历史 $k-2$ 或 $k-3$ 轮快照重合（即出现 `修改 -> 撤销 -> 再修改` 原地打转），则 $\text{OscillationPenalty}_k = 1.0$，迅速拉低 $\mathcal{V}_k$ 触发回滚。

### 7.4 External Side-Effect Governance & Saga Compensation Gateway (外部副作用四级事务与 Saga 网关)

为确保在执行“树搜索回滚”或“跨节点故障恢复”时外部世界不出现重复写或脏数据，宿主机 VSOCK 工具网关对所有工具实施四级事务分类拦截：

| 副作用事务等级 (`effect_class`) | 典型工具示例 | 运行期宿主机网关拦截行为 | 发生 Turn 回滚或任务熔断时的恢复动作 |
| :--- | :--- | :--- | :--- |
| **1. `PURE_READ`** | `read_file`, `postgres_readonly_query`, `web_search` | 将响应内容写入当前 Turn 的 `TurnWAL` 事件日志。 | 重放时直接从 `TurnWAL` 缓存返回，不向外部发起重复读取。 |
| **2. `IDEMPOTENT_WRITE`** | `s3_report_upload`, `object_put` | 自动注入确定性幂等头 `Idempotency-Key: SHA256(task_id:epoch:turn:call_id)`。 | 安全重放，外部存储根据幂等键自动去重覆盖。 |
| **3. `SAGA_COMPENSATABLE`** | `git_branch_push`, `k8s_job_create`, `reserve_inventory` | 正向执行前在持久层登记逆向补偿工具与参数 $(T_{\text{comp}}, \text{args}_{\text{inv}})$。 | 当回滚跨越该 Turn 或任务异常终止时，网关按 **LIFO 逆序** 自动调用补偿工具（如自动删除临时分支）。 |
| **4. `STAGED_COMMIT`** | `settlement_ledger_post`, `send_customer_email`, `merge_pr` | **写请求不直接发往外部生产系统**，而是写入宿主机 `Staging Buffer` 并向 Agent 返回模拟成功回执。 | 若中途发生回滚或失败，直接丢弃暂存区（外部系统 **零影响**）；仅当整个任务最终成功或人工审批通过后，由网关原子刷出（Flush & Commit）。 |

### 7.5 Four-Stage Bidirectional Backpressure Control Loop (四级双向闭环背压控制矩阵)

| 背压传导方向 | 触发等级与实时信号 | 执行面（Sandbox & Harness）调控动作 | 调度与接入网关面（Scheduler & Ingress）调控动作 |
| :--- | :--- | :--- | :--- |
| **自下而上反压**<br>（下游过载 $\to$ 向上传导） | **Stage 1：推理池轻度拥塞**<br>（`DeepSeek` TTFT P95 $> 5\text{s}$ 或 429 率 $> 5\%$） | Turn 级认知路由器自动将非核心轮次切至 `GLM-5.3`，并将 `high/max` 推理强度下调一级。 | 联合打分器降低拥塞池权重，优先将新任务路由至 `Qwen-3.8`。 |
| **自下而上反压**<br>（下游过载 $\to$ 向上传导） | **Stage 2：节点/模型池中度过载**<br>（宿主机 PSI $> 40\%$ 或 429 率 $> 25\%$） | 宿主机主动将所有处于 I/O 等待或审批等待的沙箱转入 `HIBERNATED`；节点 `Burst Semaphore` 突发上限减半。 | 触发主模型池 `OPEN` 熔断并全量切至热备池 `Qwen-3.8`；暂停 `P3/P4` 低优泳道出队。 |
| **自下而上反压**<br>（下游过载 $\to$ 向上传导） | **Stage 3：集群级饱和红线**<br>（集群可用算力 $< 5\%$ 或全模型池限流） | 仅保留 `P0/P1` 任务主会话运行，挂起所有投机探索子分支。 | 冻结队列出队，API 网关返回 `HTTP 429 Too Many Requests` + `Retry-After` + `X-Queue-Position`。 |
| **自上而下正压**<br>（上游受限 $\to$ 向下节流） | **Stage 4：客户端慢消费或预算逼近红线**<br>（SSE 信用窗口 $< 25\%$ 或剩余 Token $< 15\%$） | 暂停高频 `thinking.delta` 推送转为按句聚合；对沙箱高频日志子进程实施 PTY 写阻塞；强制收缩 `max_concurrent_sessions = 1` 并触发上下文摘要压缩。 | 停止为该任务签发新的子任务 Escrow 配额支票，集中剩余配额保障主会话收敛。 |

---

## 8. Observability, Notification & DFX Operations (可观测性、事件通知与运维体系)

### 8.1 CloudEvents 1.0 Standard Event Stream & OpenTelemetry Tracing

系统所有生命周期流转、Turn 认知路由、工具调用、配额水位告警与熔断动作均以标准 **CloudEvents 1.0** 格式通过 SSE / gRPC / Kafka 实时输出，并携带统一的 OpenTelemetry `traceparent`：

```json
{
  "specversion": "1.0",
  "id": "evt_01j9z9a2b3c4d5e6f7g8h9j0k1",
  "source": "/ansts/us-west-2/worker-us-west-01/tsk_01j9z8k7m6n5p4q3r2s1t0u9v8",
  "type": "agent.circuit_breaker.fallback_routed",
  "time": "2026-10-28T12:18:04.112Z",
  "subject": "session/main/turn/7",
  "datacontenttype": "application/json",
  "data": {
    "task_id": "tsk_01j9z8k7m6n5p4q3r2s1t0u9v8",
    "tenant_id": "tnt_enterprise_fin_01",
    "fence": { "epoch": 2, "turn_seq": 7 },
    "primary_model_pool": "DeepSeek",
    "routed_fallback_pool": "Qwen-3.8",
    "effective_reasoning_effort": "medium",
    "sandbox_phase": "COGNITIVE_PHASE_0.1C",
    "custom_metadata": {
      "business_unit": "global-settlement",
      "cost_center": "CC-9042"
    }
  }
}
```

### 8.2 Transactional Outbox Notification Engine (事务性发件箱高可靠通知)

关键状态变更（如 `waiting_approval`、`circuit_broken`、`completed`）在更新租约状态库的同时，在同一事务内写入 `notification_outbox` 表，确保状态变更与外部 Webhook 通知 **100% 原子一致，绝不漏发**。通知投递器附带 `X-Agent-Signature-256`（HMAC-SHA256 签名）与 `X-Event-Id` 幂等头，支持 5 次指数退避重试与租户死信队列（DLQ）一键重放。

### 8.3 Core Production Metrics & Alerting Thresholds (核心监控指标与告警阈值)

| Prometheus 指标名称 | 类型 | 指标含义描述 | 生产告警触发阈值 |
| :--- | :--- | :--- | :--- |
| `ansts_scheduler_dispatch_latency_ms` | Histogram | 任务准入到 MicroVM 进入 `RUNNING` 态端到端耗时 | 连续 5m `P99 > 250ms` |
| `ansts_tenant_dominant_share_ratio` | Gauge | 租户五维 W-DRF 加权主导份额 $D_t$ | 满载下偏离权重基准 `> 25%` |
| `ansts_model_pool_ttft_seconds` | Histogram | `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 首字延迟 | `P95 > 10s` 触发 Stage 1 背压 |
| `ansts_fencing_stale_rejections_total` | Counter | 被双元组栅栏 `Fence(epoch, turn_seq)` 拦截的僵尸请求数 | `> 10 次/min`（排查网络分区） |
| `ansts_host_psi_memory_some_avg10` | Gauge | 宿主机 Linux PSI 内存压力阻塞百分比 | 主动休眠后仍 `> 45%` |
| `ansts_semantic_backtrack_total` | Counter | 因进展率 $\mathcal{V}_k < 0.10$ 触发的树搜索回滚次数 | 同指纹任务触发率 `> 15%` |

---

## 9. Alternatives Considered (备选方案对比与架构权衡 Trade-offs)

| 核心设计决策 | 曾评估的备选方案 (Alternative) | 放弃原因与工程权衡分析 (Trade-off Analysis) |
| :--- | :--- | :--- |
| **1. 双相脉冲垂直伸缩 MicroVM (`0.1C <-> 8C`)** | 传统静态容器配额（Kubernetes Requests = Limits 静态 Pod） | 静态预留要求在长达 30 分钟的会话期内全程锁死 8 核 CPU 与 4GB 内存，但沙箱 80% 时间在等待 `DeepSeek` 流式响应，导致物理集群超过 70% 的算力闲置浪费。 |
| **2. 对象存储 CAS 租约 + `(epoch, turn_seq)` 双元组栅栏** | 为每个 Turn 与工具调用引入中心化 Raft / etcd 分布式锁服务 | 在数万并发沙箱高频 Turn 切换下，中心化 etcd 会成为严重的写放大与延迟瓶颈。采用对象存储条件写 + 请求头携带二元组栅栏，以无锁方式实现线性扩展与严格单活安全。 |
| **3. 父子配额支票托管（Quota Escrow Voucher）** | 将派生的子 Agent 作为普通独立任务重新投入租户排队池 | 当集群满载时，等待子任务结果的父任务占满全部运行槽位，而队列中的子任务因无槽位无法启动，必然触发全局死锁。配额支票移交结合父沙箱休眠让位彻底消除了死锁条件。 |
| **4. 进展率（$\mathcal{V}_k$）树搜索回滚 + Saga 补偿网关** | 遇到错误仅简单重试当前轮，或达到轮次上限直接报错终止 | 盲目原地重试会让大模型在已被错误污染的上下文中继续幻觉打转；直接终止则浪费已消耗的全部 Token。回滚 4 轮至干净快照并注入负反馈防重记忆，可使 `> 68%` 陷入停滞的任务在第二分支成功收敛。 |
| **5. 贝叶斯指纹 P50 软预留 + 渐进式续签** | 按用户填写的 `max_total_tokens` 实施 100% 硬预留准入 | 用户普遍将 `max_total_tokens` 设为安全上限（实际中位数仅占 20%），硬预留会导致 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 的预留吞吐池长期处于假性满载。 |

---

## 10. Security, Privacy, and Multi-Tenant Isolation (安全、隐私与多租隔离)

1. **虚拟化级强内核隔离**：默认采用 **Firecracker MicroVM**（每个任务拥有独立 Guest Linux Kernel 与最小化设备模型）或 **Kata Containers**，从硬件虚拟化层阻断容器逃逸（`runc` 内核提权漏洞）跨租户横向渗透。
2. **零信任凭证代理（Zero-Trust VSOCK Proxy）**：沙箱内部环境完全不保存 `DeepSeek`、`GLM-5.3`、`Qwen-3.8` 的 API Key 或外部数据库密码。所有外部流量强制经过宿主机 `AF_VSOCK` 代理，由宿主机按 `TaskSpec.network_policy.allowed_domains` 实施白名单过滤、带宽限速与凭证动态注入。
3. **跨租户快照加密与物理隔离**：对象存储中的 `TurnWAL` 增量快照块与工作区产出物均按租户独立 KMS 密钥加密（`SSE-KMS`），预热池微虚拟机克隆遵循单向写时复制（Copy-on-Write），销毁后立即擦除块设备脏页。

---

## 11. Rollout Plan & Chaos Validation Benchmarks (演进路线图与混沌验收标准)

### 11.1 分阶段上线里程碑 (Phased Rollout)

* **Phase 1 (M1 — 基础调度底座与预热池)**：上线分级多租队列（H-WFQ）、5D W-DRF 调度器、Firecracker 预热池（`pool-py312-ds`、`pool-rust-toolchains`）以及双元组栅栏 `Fence(epoch, turn_seq)`。
* **Phase 2 (M2 — 认知动态路由与双相弹性)**：打通 `DeepSeek`、`GLM-5.3`、`Qwen-3.8` 三大模型池的 Turn 级动态路由与健康度联合打分，上线 Cgroups v2 `0.1C <-> 8C` 毫秒级相变伸缩。
* **Phase 3 (M3 — DAG 支票托管、语义回滚与 Saga 网关)**：全量启用父子配额支票免排队通道、进展率 $\mathcal{V}_k$ 树回滚熔断器以及四级工具副作用暂存与逆向补偿网关。

### 11.2 混沌工程验收标准 (Chaos Engineering Acceptance Criteria)

| 混沌演练场景 | 故障注入方式 | 生产级验收通过标准 (SLO Pass Criteria) |
| :--- | :--- | :--- |
| **1. 主模型池 `DeepSeek` HTTP 429 限流风暴** | 在 `DeepSeek` 网关注入 50% HTTP 429 响应 | 模型池熔断器在 `< 5s` 内切换为 `OPEN`；100% 配置了备选池的在跑任务在下一轮 Turn 自动无感路由至 **`Qwen-3.8`**，沙箱零重启。 |
| **2. 宿主机突然掉电宕机 (`SIGKILL`)** | 强制杀死承载 50 个活跃沙箱的 `worker-us-west-01` | 存活节点在 `< 800ms (P95)` 内通过 CAS 抢占租约（`epoch + 1`）并利用 Paged Turn-WAL Restore 完成跨节点断点续跑，**已确认轮次丢失数 = 0**。 |
| **3. 网络分区恢复后的僵尸节点非法写请求** | 隔离旧节点网络 30s 后重放过期 Turn 快照与外部工具写请求 | 对象存储与 MCP 工具网关 **100% 拒绝** 并返回 `HTTP 409 StaleFenceRejected`；旧节点收到 409 后在 `< 50ms` 内自毁本地僵尸 MicroVM。 |
| **4. 同节点 20 个沙箱同时触发编译突发脉冲** | 在单台 32 核物理机上同时触发 20 个沙箱执行 `cargo build --release` | 节点 `Burst Semaphore` 自动对超出并发上限的突发请求实施 `150~400ms` 微排队削峰；宿主机 PSI 内存压力 `< 45%`，**宿主机 OOM 次数 = 0**。 |
| **5. 恶意/失控 Agent 语义死循环与外部写操作** | 注入模拟工具持续返回循环报错并尝试写入 `settlement_ledger_post` | 暂存提交（`STAGED_COMMIT`）被宿主机网关拦截未发往生产库；第 4 轮触发进展率树回滚并执行 Saga 逆向补偿；连续 2 次回滚无进展后精准置为 `CIRCUIT_BROKEN`。 |
---

## 12. Production Implementation & Deployment Synchronization (v2.1 工程实现与生产部署同步规范)

本章节同步了 **ANSTS v2.1 全量 TypeScript 代码仓（40 个模块 / 20 项自动化集成与审计测试）**、**`denoland/celld` 与 Cloudflare Workers 脑体分离协同架构**、**`RustFS` S3 存储底座启动标本** 以及 **5 项关键代码走查加固机制**。

### 12.1 “脑体分离（Brain-Body Disaggregation）”协同架构

在生产部署中，ANSTS 将智能体任务的 **有状态认知控制脑（Brain）** 与 **重算力相变沙箱躯干（Body）** 进行物理与运行时解耦：

1. **轻量有状态控制脑（`AgentTaskHarnessCell` & `AnstsSchedulerCell` — `~0.47 MB / Cell`）**：
   * 基于 `denoland/celld`（或 Cloudflare Durable Objects）的单线程 V8 Isolate + 私有嵌入式 SQLite 构建。
   * 在占任务总时长 60%~85% 的 **认知思考期（Cognitive Phase，等待 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 流式 Token）** 与 **人工审批等待期（Human Approval Gate）**，任务完全驻留在 `0.47 MB` 的 V8 Cell 中（WebSocket 休眠态仅 `~0.03 MB`，唤醒耗时 `~4 ms`），**无需占用任何 Linux MicroVM 内存与 vCPU**。
2. **按需突发算力躯干（Companion Container / Phase-Elastic MicroVM — `0.1C <-> 8.0C`）**：
   * 仅当大模型输出需要执行 Bash、代码编译、测试或无头浏览器的工具调用时，`AgentTaskHarnessCell` 才通过 `this.ctx.container` 或 `ansts-node-daemon` gRPC 接口按需唤醒预热池中的 Firecracker / Kata / gVisor 沙箱（`~62 ms` CoW 克隆，受宿主机 `Burst Semaphore` 与 Linux PSI 内存守卫保护），执行完毕后立即停止或休眠算力容器。

### 12.2 代码走查加固的五大核心生产机制 (Production Hardening Mechanisms)

| 加固编号与机制名称 | 对应源码实现 (`src/**/*.ts`) | 解决的分布式边界问题 | 核心状态流转与数学不变量 |
| :--- | :--- | :--- | :--- |
| **1. Cell 冷启动与休眠唤醒 SQLite 状态重水化 (`rehydrateFromSql`)** | `src/cloudflare/harness_cell.ts` | `celld` / Durable Object 从 `Hibernated` 或 `Inactive` 唤醒时会重新执行构造函数，纯内存变量会丢失 | 构造函数自动扫描私有 SQLite 的 `turn_wal_frames` 与 `saga_journal` 表，原子还原最新 `FenceToken(epoch, turnSeq + 1)`、累计 `usedTokens`、历史快照链及待补偿 Saga 栈，保障休眠唤醒 **零状态丢失** |
| **2. 抢占/PSI 休眠任务自动唤醒队列 (`hibernatedResumeQueue`)** | `src/scheduler.ts`<br>`src/models.ts` | 被 `P0/P1` 抢占或因宿主机 PSI `>40%` 紧急休眠的 `P3/P4` 任务若不重新纳入调度环会导致永久挂起 | 引入 `HibernationReason`（区分 `PREEMPTED` / `PSI_MEMORY_PRESSURE` 与 `HUMAN_APPROVAL_WAIT`）；在 `scheduleTick()` 中一旦节点释放出 CPU 余量，按 Aging+EDF 紧迫度自动唤醒被抢占沙箱，以 `(epoch + 1, prevTurnSeq)` 原地续跑 |
| **3. 全链路组件组合装配 (Unified Component Composition)** | `src/cognitive_router.ts`<br>`src/saga_gateway.ts`<br>`src/node_daemon.ts` | 避免顶层路由/网关与底层限流器、熔断器、补偿器状态割裂 | `CognitiveRouter` 内聚驱动三态熔断器（含 `10s` 冷却期转 `HALF_OPEN` 探测）、滑动窗口令牌桶 Pacing 与流式计量；`SagaToolGateway` 组合 `SagaCompensatorEngine`（3 次重试 LIFO 补偿）与 `StagingCommitAndApprovalGate` |
| **4. 双时钟独立计量与优雅排水 (`advanceClocks` & `GRACEFUL_DRAINING`)** | `src/models.ts` | 任务在休眠等待人工审批期间不应消耗活跃 CPU 计算时限（`maxActiveComputeSec`） | 严格分离挂钟用时（`wallClockSec`）与活跃计算用时（`activeComputeSec`，仅在 `RUNNING` 态累加）；当触及时间或 Token 硬上限时进入 `GRACEFUL_DRAINING` 状态输出阶段性总结报告 |
| **5. V8 Isolate 纯边缘序列化兼容 (`toJSONString` / Lazy FS)** | `src/profiler.ts` | 纯 V8 边缘运行时无本地 POSIX 文件系统 | `BayesianTaskProfiler` 原生支持无文件系统的 JSON / KV / D1 状态导入导出，同时保留按需懒加载的本地文件持久化接口 |

### 12.3 基于 `RustFS` + `denoland/celld` 的生产部署架构标本

在自托管生产环境中，推荐使用 **RustFS**（Apache-2.0 开源、无 GC 停顿的高性能 Rust S3 兼容对象存储）作为 `celld` 集群的共享存储桶底座（`One Fleet, One Bucket`）：

```text
+---------------------------------------------------------------------------------------------------+
| 1. 接入负载均衡层 (Envoy / Caddy / Cloudflare Anycast L7 LB)                                       |
+-------------------------------------------------+-------------------------------------------------+
                                                  | (HTTP / WebSocket / SSE -> :8080)
                                                  v
+---------------------------------------------------------------------------------------------------+
| 2. denoland/celld 有状态控制与认知路由集群 (3~N 节点, CELLD_DURABILITY=fleet, ~14ms 对等落盘)       |
|  - celld-node-01 (:8080 public / :8081 internal, --advertise celld-node-01:8081)                  |
|  - celld-node-02 (:8080 public / :8081 internal, --advertise celld-node-02:8081)                  |
|  - celld-node-03 (:8080 public / :8081 internal, --advertise celld-node-03:8081)                  |
|  * 运行 AnstsSchedulerCell + AgentTaskHarnessCell + __D1 (ansts-audit-ledger) + __KV + __Queue    |
+------------------------+-------------------------------------------------+------------------------+
                         | (S3 Conditional CAS & LTX Stream -> :9000)      | (Companion Container / gRPC)
                         v                                                 v
+------------------------------------------------+   +----------------------------------------------+
| 3. RustFS 高性能 S3 对象存储集群 (UID 10001)    |   | 4. ANSTS 裸金属相变沙箱池 (Firecracker/Kata)  |
|  - Port :9000 (S3 API, /health, Peer RPC)      |   |  - 思考期 0.1C/256MB <-> 工具期 8.0C/4GB      |
|  - Port :9001 (RustFS Web Console)             |   |  - 节点 Burst Semaphore + Linux PSI 内存守卫  |
|  - Bucket: s3://ansts-fleet (RPO=0 LTX & Lease)|   |  - CELLD_CONTAINER_RUNTIME=runsc / kata      |
+------------------------------------------------+   +----------------------------------------------+
```

#### 标准启动命令标本 (`deploy/rustfs-celld/start-rustfs-celld.sh`)

```bash
# 1. 一键拉起 RustFS (:9000 S3 API / :9001 Console) + 3 节点 celld 对等复制集群
docker compose -f deploy/rustfs-celld/docker-compose.rustfs-celld.yml up -d

# 2. 部署 ANSTS v2.1 TypeScript Worker & Durable Objects 并执行 D1 SQL 迁移
export CELLD_BUCKET="s3://ansts-fleet"
export S3_ENDPOINT="http://127.0.0.1:9000"
celld deploy . --bucket "${CELLD_BUCKET}" --endpoint "${S3_ENDPOINT}" --region us-east-1
celld d1 migrations apply ansts-audit-ledger --bucket "${CELLD_BUCKET}" --endpoint "${S3_ENDPOINT}"

# 3. 执行无侵入集群健康与租约体检 (确认 restoring=0 且 CAS/Read-after-write 正常)
celld diagnose --bucket "${CELLD_BUCKET}" --endpoint "${S3_ENDPOINT}"
```
