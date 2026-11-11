# ANSTS v2.5 全生命周期深度改进与全量问题修复清单 (Milestones M51 ~ M63 DFX)

> **文档版本**: v2.5.0-production-hardened  
> **更新时间**: 2026-11-10  
> **测试状态**: **56 / 56** 套件全部通过，**143 / 143** 项测试 100% PASS，0 失败，0 告警  
> **目标架构**: Cloudflare Workers / `denoland/celld` v0.6.3 + Linux Cgroup v2 / OverlayFS / MicroVM / UFFD + LayerFS + S3/R2 LTX

---

## 目录
1. [改进与修复概览](#1-改进与修复概览)
2. [M51：调度内核 Anti-Stale、无死锁与防饥饿加固](#2-m51调度内核-anti-stale无死锁与防饥饿加固)
3. [M52：系统级陈旧租约、级联死锁与状态一致性整改](#3-m52系统级陈旧租约级联死锁与状态一致性整改)
4. [M53：跨层深层风险消除（Saga、FinOps、WarmPool、WAL、Drain-Token）](#4-m53跨层深层风险消除saga-finops-warmpool-wal-drain-token)
5. [M54：运行时弹性、Harness 状态恢复、审批超时与 A2A 防饥饿](#5-m54运行时弹性harness-状态恢复审批超时与-a2a-防饥饿)
6. [M55：存储高可用、Last-Brain 存活、LayerFS 同步与 RPO=0 事务回滚](#6-m55存储高可用last-brain-存活layerfs-同步与-rpo0-事务回滚)
7. [M56：冷启动重水合、3-Way 冲突裁决、应急准入与内存泄漏消除](#7-m56冷启动重水合3-way-冲突裁决应急准入与内存泄漏消除)
8. [M57：Half-Open 探针保护、叶子优先 GC、节点单调性与反序列化防护](#8-m57half-open-探针保护叶子优先-gc节点单调性与反序列化防护)
9. [M58：容器网络隔离、eBPF 路径防绕过、安全证明绑定与大包防护](#9-m58容器网络隔离ebpf-路径防绕过安全证明绑定与大包防护)
10. [M59：SQLite 复合主键与落盘、KV TTL 淘汰、SSE 容错与 UFFD 完整性](#10-m59sqlite-复合主键与落盘kv-ttl-淘汰sse-容错与-uffd-完整性)
11. [M60：全链路持久化闭环、深度幂等、确定性断点恢复与重复合流提权](#11-m60全链路持久化闭环深度幂等确定性断点恢复与重复合流提权)
12. [M61：全子系统持久化、CAS/Push 幂等重试、断点状态回滚与重复仲裁合流](#12-m61全子系统持久化caspush-幂等重试断点状态回滚与重复仲裁合流)
13. [测试套件与自动化验证矩阵](#13-测试套件与自动化验证矩阵)

---

## 1. 改进与修复概览

本工程在完成 ANSTS v2.5 基础架构融合的基础上，针对高并发、多租户、长时间运行 Agent 以及网络不可靠分布式边缘环境中的**Stale（陈旧状态/脏读）**、**Deadlock（死锁/悬挂阻塞）**、**Starvation（饥饿/雪崩）**、**Persistence Gap（持久化断链）**、**Idempotency Loss（非幂等重试副作用）**、**Breakpoint Resumption（断点恢复裂化）** 与 **Duplicate Concurrency（并发重复开销）** 展开了持续 13 轮（M51 ~ M63 DFX）的全面深挖与加固。

| 里程碑 | 关注核心领域 | 测试套件与通过数 | 代表性 Git 提交 |
| :--- | :--- | :--- | :--- |
| **M51** | 调度器内核 5D 评分、心跳超时驱逐、DAG 级联 Dead-Letter、HoL 非阻塞调度 | 44 Suites / 131 Tests | `9951ea2` |
| **M52** | Cell 状态恢复、DO 警报续订、路由半开冷却、子 Agent 失败解封、隧道指令重排 | 45 Suites / 132 Tests | `15fb20f` |
| **M53** | Saga 回滚幂等失效、FinOps 每日 UTC 重置、沙箱池泄漏防御、WAL 双元单调性 | 46 Suites / 133 Tests | `10d8334` |
| **M54** | Harness 冷启动 Fencing、熔断器踩踏防护、审批工单 TTL 淘汰、SSE 丢帧回放 | 47 Suites / 134 Tests | `5ff647b` |
| **M55** | 租户级跨任务幂等、Last-Brain 存活、LayerFS Rule D9 同步、RPO=0 事务原子回滚 | 48 Suites / 135 Tests | `04d4b4c` |
| **M56** | 8 表全量水准线重水合、LayerFS 3-Way 删除冲突检测、P0 应急容量隔离、Facet 内存泄漏消除 | 49 Suites / 136 Tests | `374d255` |
| **M57** | 模型池 HALF_OPEN 单飞保护、叶子优先 CAS GC、NodeDaemon 轮次单调性、输入 NaN 防投毒 | 50 Suites / 137 Tests | `ae18f38` |
| **M58** | 容器云元数据/私网 RFC1918 隔离、eBPF LSM 根路径校验、SPIFFE/SLSA 防篡改、HTTP 413 大包保护 | 51 Suites / 138 Tests | `38c4346` |
| **M59** | SQLite 复合主键覆盖修复、DELETE 磁盘刷盘、KV TTL 物理过期、SSE 畸变帧跳过、UFFD 缓存失效 | 52 Suites / 139 Tests | `8fe14b7` |
| **M60** | 全链路 SQLite 持久化闭环、递归深度规范化 JSON 幂等、WAL 确定性断点恢复、在途合流提权 | 53 Suites / 140 Tests | `eff8f2a` |
| **M61** | A2A 黑板/拓扑 CAS 幂等、暂存审批/DAG/FinOps 持久化与防重、速度熔断器同轮去重、LayerFS 断点回滚 | 54 Suites / 141 Tests | `c937aa7` |
| **M62** | DFX 全维度可靠性（CFS硬下限/目录清理）、可诊断性（基数上限/指标规范）、安全性（PSI P0/P1豁免）、可服务性（SLA预留落盘） | 55 Suites / 142 Tests | `e281d45` |
| **M63** | DFX 深度加固 II（混沌演练TTL自愈、背压/削峰/分片NaN保护、主权路由SQLite落盘、WFG多环死锁解环、TEE/IPv6-IMDS防护与成本钳制） | 56 Suites / 143 Tests | `HEAD` |

---

## 2. M51：调度内核 Anti-Stale、无死锁与防饥饿加固

- **[Stale-1] 节点心跳超时驱逐与排他隔离** (`src/scheduler.ts`):
  - **问题**: 节点心跳静默但健康标记仍为 `healthy: true` 时，调度器持续将任务派发至已失联节点，造成任务调度即刻挂死。
  - **整改**: 在 `CoreScheduler5D.evictStaleNodes` 中设定 `nodeHeartbeatTtlSeconds`（默认 30s）检测阈值；在静态节点评分 `scoreNodeForTask` 中增加硬性时间戳校验，超时节点直接返回 `-1.0` 拒绝派发并标记离线。
- **[Stale-2] 孤儿租约回收与节点资源归还** (`src/scheduler.ts`):
  - **问题**: 租约过期被扫描出时未恢复原节点的 `free_cpu_cores`、`free_memory_mb` 与 `running_tasks`，导致失联节点上的虚拟资源额度永久蒸发。
  - **整改**: 在 `reconcileOrphanedLeases` 中增加归属节点容量反向加和与饱和度截断，超 3 次重试的任务转入 `DEAD_LETTER`。
- **[Deadlock-1] 租户级队头阻塞（Head-of-Line Blocking）消除** (`src/scheduler.ts`):
  - **问题**: 单个大任务无法在当前节点池放置时一直堵在租户优先队列顶端，导致同租户后续可立即执行的小任务永久饥饿。
  - **整改**: 引入单调度周期 `deferredTasks` 暂存环，无法放置的任务暂存至周期结束再放回，保证后续就绪任务能立即调度执行。
- **[Deadlock-2] 级联 DAG 坏死传递** (`src/scheduler.ts`):
  - **问题**: 上游依赖任务因超时或超次重试进入 `DEAD_LETTER` 后，下游依赖它的子任务永久停留在 `SUBMITTED` 状态无尽等待。
  - **整改**: 实现 `cascadeDeadLetterToDependents`，上游失败时递归级联将所有传递依赖子任务推进至 `DEAD_LETTER` 并注明触发因由。
- **[Starvation-1] 公平抢占补偿与受害者轮转** (`src/scheduler.ts`, `src/queue_manager.ts`):
  - **问题**: `P0_EMERGENCY` 紧急抢占总是挑中同一个 `P3_BATCH` 任务反复剥夺，导致该批处理任务永远无法完成。
  - **整改**: 为任务新增 `preempted_count` 与 `last_preempted_at` 属性，设置抢占冷却期与最大抢占次数，并在二叉最大堆中为被抢占任务提供反向动态权重补偿。

---

## 3. M52：系统级陈旧租约、级联死锁与状态一致性整改

- **[State-1] Cloudflare DO SchedulerCell 水准线恢复与定时警报续订** (`src/cloudflare/scheduler_cell.ts`):
  - **问题**: Durable Object 发生冷启动重启后，已持久化的任务无法恢复调度器内存态，且缺少定期的闹钟续期触发，调度器处于假死状态。
  - **整改**: 在 `AnstsSchedulerCell` 构造器中加载 `rehydrateFromSql()` 恢复节点心跳、任务队列与 Fencing 水准线，并在存在排队任务或活跃租约时自动续订 `alarm(Date.now() + 1000)`。
- **[Router-Stale-2] 认知路由器滑动窗口衰减与半开恢复** (`src/cognitive_router.ts`):
  - **问题**: LLM 模型端点偶发 429 报错后计数器永不衰减，且熔断器开启后缺乏冷却期后试探半开恢复机制。
  - **整改**: 引入 60 秒时间窗口衰减清除过期限流计数，并在熔断开启超过冷却时间后允许单个请求进入 `HALF_OPEN` 状态试探恢复。
- **[Escrow-Deadlock-3] DAG 配额托管子任务失败解封与槽位回收** (`src/dag/escrow.ts`):
  - **问题**: 子 Agent 失败或被取消时，父任务停留在 `WAITING_SUB_AGENT` 无法被唤醒，且预扣配额槽位永久泄漏。
  - **整改**: 实现 `failOrCancelChildSubAgent`，支持子任务失败时自动唤醒或终结父任务，并释放占用的配额槽位。
- **[Tunnel-Stale-4] 反向隧道心跳陈旧自降级与在途指令重新排队** (`src/cloudflare/tunnel_gateway_cell.ts`):
  - **问题**: 双宿主反向隧道在主网关失联时无法感知，正在长轮询中的未确认指令随之丢失。
  - **整改**: 在 `TunnelGatewayCell` 中实现 `evictStaleTunnelsAndRequeueInFlight`，自动判定超时网关为非健康，并将未应答指令无缝重新排队至备用网关。

---

## 4. M53：跨层深层风险消除（Saga、FinOps、WarmPool、WAL、Drain-Token）

- **[Saga-Stale-1] Saga 回滚后幂等执行日志自动作废** (`src/saga_gateway.ts`, `src/saga/compensator.ts`):
  - **问题**: 任务因失败执行 LIFO 逆向补偿后，`SagaToolGateway.executionJournal` 仍保留旧的成功记录，重试同轮次工具时误命中缓存而跳过实际重跑。
  - **整改**: 联动补偿引擎与网关，回滚步骤时同步删除受影响轮次的幂等执行日志；并在补偿调用中使用独立 `try/catch` 隔离异常，避免单个步骤异常阻塞后续补偿。
- **[FinOps-Stale-2] 租户每日预算 UTC 窗口自动重置与退款补偿** (`src/finops/budget_controller.ts`):
  - **问题**: 租户预算跨天后无法重置，达到日上限后所有请求永久被拒；且回滚的任务无法退还已扣减的预算。
  - **整改**: 增加按 UTC 日期键格式化的滑动窗口重置检测，跨天请求自动清零已用额度，并增加 `refundSpend` 原语支持安全回滚退款。
- **[WarmPool-3] 预热容器分配幂等与孤儿容器回收** (`src/runtime/warm_pool.ts`):
  - **问题**: 并发申请预热容器时可能重复绑定同一工作目录；容器归还失败时资源长期挂死在泄漏状态。
  - **整改**: 引入幂等分配校验与带超时的扫描清理机制，确保容器分配与归还生命周期闭环。
- **[WAL-Stale-4] TurnWalEngine 双元组 `(fencingToken, turnIndex)` 单调性校验** (`src/checkpoint/wal_engine.ts`):
  - **问题**: 仅依据单一递增序号判断，无法防御跨节点重派发时不同租约下的乱序写入与陈旧写入。
  - **整改**: 引入 `(fencingToken, turnIndex)` 双元偏序约束，在写日志前严格校验，并提供全链路 CRC32 完整性验证。
- **[Scaler-Deadlock-5] 优雅缩容排空令牌生命周期与超时兜底** (`src/cluster/manual_scale_controller.ts`):
  - **问题**: 节点下线排空时若某长任务阻塞，排空过程永久卡住，导致编排流程死锁。
  - **整改**: 为节点下线排空设定租约超时（Drain Token TTL），到期后强制回收，保障集群弹性操作确定性结束。

---

## 5. M54：运行时弹性、Harness 状态恢复、审批超时与 A2A 防饥饿

- **[Harness-1] AgentTaskHarnessCell 冷启动 Fencing 水准线恢复** (`src/cloudflare/harness_cell.ts`):
  - **问题**: Harness Cell 重启后内部最大 Fencing Token 丢失，持有旧 Token 的僵尸 Worker 仍能覆盖最新轮次输出。
  - **整改**: 在构造器中通过 `rehydrateWatermarksFromSql()` 扫描持久化表恢复 `highestFencingToken` 和 `highestTurnIndex`，阻断陈旧写入。
- **[Breaker-2] 三态熔断器半开并发踩踏防线** (`src/router/breaker_limiter.ts`):
  - **问题**: 熔断器进入 `HALF_OPEN` 后，并发请求同时穿透向后端下发请求，造成后端服务二次雪崩。
  - **整改**: 强制限制 `HALF_OPEN` 状态下仅允许 1 个试探请求进入，试探成功后才转为 `CLOSED`。
- **[Staging-3] 人工审批工单 TTL 淘汰与拒绝自动中止** (`src/saga/staging.ts`):
  - **问题**: 处于 `SUSPENDED_APPROVAL` 的任务若审批人长期不处理会永久占用系统资源；且审批拒绝后未暂存副作用未被妥善清理。
  - **整改**: 审批拒绝时立即标记并中止未提交副作用；增加后台看门狗扫描超时工单并自动流转至 `DEAD_LETTER`。
- **[Outbox-4] 事务性发件箱单异常隔离与 SSE 超出窗口兜底回放** (`src/telemetry/outbox_webhook.ts`, `src/gateway/sse_stream.ts`):
  - **问题**: Outbox 循环中单个 Webhook 发送抛出异常导致整个批次中止；SSE 客户端携带已淘汰的 `Last-Event-ID` 重连时所有事件被静默丢弃。
  - **整改**: Outbox 逐条捕获并记录错误；SSE 在事件已出环形队列时降级回放保留窗口中的最新事件并标记 `replayGapDetected`。
- **[A2A-5] 多 Agent Git 补丁仲裁冲突老化加速防饥饿** (`src/a2a/blackboard_and_git_arbiter.ts`):
  - **问题**: 低优先级 Agent 的补丁若与高优先级 Agent 持续冲突，将永远无法被合并。
  - **整改**: 记录 Agent 连续冲突轮次，仲裁时动态累加冲突补偿分（每次冲突 +150 分），确保冲突 Agent 逐步提升优先级完成合并。

---

## 6. M55：存储高可用、Last-Brain 存活、LayerFS 同步与 RPO=0 事务回滚

- **[Persistence-1] 租户隔离的跨任务幂等查找与完整 Spec 保真** (`src/cloudflare/scheduler_v25_persistence.ts`):
  - **问题**: 任务以相同幂等键重试时，未能完整恢复原任务所有 Spec 字段，且跨租户可能发生重合。
  - **整改**: 幂等键施加租户作用域（`tenantId::idemKey`），冷启动恢复保留完整 `spec_json`，并防止陈旧 S3 快照覆盖更高 Fencing Token 的本地任务。
- **[HA-2] 分片协调器 Last-Brain 存活保底守卫** (`src/cloudflare/ha_scheduler_shard_coordinator.ts`):
  - **问题**: 当集群中只剩最后一个 Brain 节点发生抖动被驱逐时，整个 256 分片哈希环变为空环，导致全部任务无法路由。
  - **整改**: 引入 Last-Brain 保护守卫，检测到最后节点时保留环映射，只递增其调度 Epoch 隔离陈旧租约，杜绝空环死锁。
- **[LayerFS-3] LayerFS 远端 Store 同步 Pull-to-Push 状态一致性** (`src/checkpoint/layerfs_remote_sync.ts`):
  - **问题**: 执行 `pullStoreSnapshot()` 拉取最新远端快照后，本地未更新内部 `remoteGeneration` 与 `lastKnownEtag`，导致紧随其后的推送因触发 Rule D9 被错误拒绝。
  - **整改**: 在拉取逻辑中同步更新基线世代与 ETag 缓存，消除合法拉取后立即推送报 409 的问题。
- **[Harness-4] 时间旅行调试 CRC32 验签与最高纪元去重** (`src/checkpoint/time_travel.ts`):
  - **问题**: 历史分叉恢复时可能读取到重试产生的同轮次多个快照，甚至包含已损坏的脏数据。
  - **整改**: 逐帧验证 WAL CRC32 校验码，同一轮次仅保留最高 `(fencingToken, walSeq)` 的有效帧。
- **[Storage-5] RPO=0 LTX 同步刷盘 CAS 冲突时原子回滚 SQLite 本地事务** (`src/cloudflare/celld_ha_engine.ts`):
  - **问题**: 本地 SQLite 已执行写操作，但向 R2 刷写 LTX 分段因脑裂 CAS 失败时，本地 SQLite 状态未回滚导致状态与对象存储分叉。
  - **整改**: 将 SQLite 事务与 R2 CAS 写入纳入原子边界，R2 CAS 失败时强制执行 `ROLLBACK TRANSACTION`。

---

## 7. M56：冷启动重水合、3-Way 冲突裁决、应急准入与内存泄漏消除

- **[Kernel-1] 统一组件持久化水准线恢复与中断飞行任务重排** (`src/checkpoint/unified_state_store.ts`):
  - **问题**: 系统重启后，中断在 `LEASED` 或 `RUNNING_*` 状态的任务因节点消失无法继续运行，也不会重新排队。
  - **整改**: 重水合时不仅恢复持久化租约的最高 Fencing Token 水准线，还将未终结的中断任务安全重置为 `QUEUED` 重新入队派发。
- **[Storage-2] LayerFS 3-Way 协同合并彻底支持文件删除与冲突拦截** (`src/dag/layerfs_branch_coordinator.ts`):
  - **问题**: 三方分支合并算法仅处理增量修改，忽略了文件被显式删除的场景，且删除与修改并发时发生静默覆盖。
  - **整改**: 完善合并算法：双方皆删除正常移除；单方删除另一方未修改正常传播删除；单方删除而另一方修改报 `LayerFsMergeConflictError` 拒绝合并。
- **[Storage-3] LayerFsContentStoreEngine 环路安全重构与引用图置顶保护** (`src/checkpoint/layerfs_store_engine.ts`):
  - **问题**: Delta 压缩链存在坏数据时可能产生死循环；删除基底对象时未检查派生对象依赖。
  - **整改**: 增加最大解压深度保护防止循环依赖，并在删除时进行反向依赖扫描，被派生对象引用的基底块禁止删除。
- **[Admission-4] 网关背压控制器 P0 应急预留容量隔离** (`src/gateway/backpressure.ts`):
  - **问题**: 在高并发集群饱和时，系统由于队列排满对所有新请求直接返回 429，甚至包括故障自愈和应急诊断请求。
  - **整改**: 划分 10% 应急容量池，即使普通请求已触发限流，`P0_EMERGENCY` 级别任务仍能获得准入。
- **[Celld-5] CelldAdminClient 调度器注册非倒退保护与 Facet/Isolate 内存泄漏消除** (`src/cloudflare/celld_admin_client.ts`, `src/cloudflare/celld_native_optimizations.ts`):
  - **问题**: 重平衡节点注册若收到乱序心跳可能回退节点 Fencing 纪元；V8 隔离沙箱与 Facet 子 Actor 在销毁后残留全局引用导致内存泄漏。
  - **整改**: 注册时强制校验纪元非递减；明确执行 `contextify` 解绑与 Facet 显式 `delete` 清理映射。

---

## 8. M57：Half-Open 探针保护、叶子优先 GC、节点单调性与反序列化防护

- **[Router-1] 模型连接池 HALF_OPEN 单探针保护与 OPEN 冷却期保真** (`src/router/pool_client.ts`):
  - **问题**: 当模型端点熔断开启后，若并发请求在冷却期未满时到达，部分请求错误越过限制发送给后端。
  - **整改**: 严格校验当前时间与故障发生时间差，冷却期内硬性快速失败（返回 503）；进入 `HALF_OPEN` 后锁定单并发探测。
- **[GC-2] 分布式 CAS GC 垃圾回收多趟叶子优先扫描与测试分支清理** (`src/storage/distributed_gc_sweeper.ts`):
  - **问题**: 垃圾回收一次性扫描可能误删被其它提交引用的上层目录树；测试临时分支（如 CoW 二分分支）在完成后未清理。
  - **整改**: 采用自底向上的多趟标记清除机制，并在二分排查流程后确保显式注销临时 CoW 分支。
- **[Daemon-3] NodeDaemon 跨节点迁移轮次单调性防线** (`src/node_daemon.ts`):
  - **问题**: 任务从节点 A 迁移至节点 B 时，节点 B 本地无该任务历史检查点，重新以 `current_turn = 0` 计算下一个轮次，导致轮次倒退。
  - **整改**: 计算下一轮次基准时，统一取 `Math.max(task.current_turn, checkpoint.turn_index)`，跨节点迁移后依然严格递增。
- **[Validator-4] TaskSpec 验证器防御 NaN / 非法浮点数并限制网关请求体体积** (`src/gateway/validator.ts`, `src/gateway/server.ts`):
  - **问题**: 外部请求若传入 `priority: NaN` 或负数资源需求，会导致调度器 DRF 动态分值计算为 `NaN` 彻底瘫痪队列；大体积请求体可能引发 Node.js 内存耗尽。
  - **整改**: 验证器中严格校验所有数值字段为有限非负整数或浮点数；网关服务强制限制最大请求体（1MB），超限直接返回 HTTP 413。
- **[Vsock-5] Vsock 帧流分包边界与格式安全性规范** (`src/runtime/vsock_bridge.ts`):
  - **问题**: 通过 Unix 域套接字传输的二进制 RPC 报文若缺少统一的帧长头部，在流式粘包时会引发反序列化紊乱。
  - **整改**: 严格规范 `[length: 4 bytes] + [payload]` 二进制帧封装与校验机制。

---

## 9. M58：容器网络隔离、eBPF 路径防绕过、安全证明绑定与大包防护

- **[Container-1] 容器边界 RFC1918、CGNAT、IPv6 严密隔离与并发启动上限** (`src/cloudflare/celld_container_bridge.ts`):
  - **问题**: 早期容器沙箱出站网络仅封禁了 `10.0.0.0/8` 等常规私网，遗漏了 `100.64.0.0/10`（运营商 CGNAT）及 IPv6 局部链路地址；瞬时并发拉起未设防导致主机进程耗尽。
  - **整改**: 全面拦截 RFC1918、`100.64.0.0/10`、`fc00::/7`、`fe80::/10` 及云平台内部元数据网段；增加 `max_instances` 启动限额保护。
- **[LSM-2] 递归 OverlayFS 上层目录差异解析与 eBPF LSM 根路径锚定** (`src/runtime/container_driver.ts`, `src/runtime/ebpf_lsm_bridge.ts`):
  - **问题**: OverlayFS 差异分析时未做深度递归；eBPF LSM 策略未考虑相对路径与软链接逃逸。
  - **整改**: 递归扫描 `upperdir` 中所有新增/修改/删除文件；路径校验前强制进行绝对路径规范化（`path.resolve`）并校验根路径锚点。
- **[Attest-3] SPIFFE/SLSA 凭据有效性与空签名拒绝** (`src/runtime/slsa_provenance_attestor.ts`, `src/runtime/confidential_tee_bridge.ts`):
  - **问题**: 传入空签名或缺少域名的单段 SPIFFE ID 时未拒绝验证。
  - **整改**: 强化数字签名与证书链合法性检查，禁止空签名与格式不规范的身份标识。
- **[Canary-4] 金丝雀回滚后安全锁定与 PSI 采集过滤** (`src/cluster/canary_rollout_controller.ts`):
  - **问题**: 金丝雀发生严重异常触发回滚后，未进入安全锁定状态，可能在下一周期被自动重新触发；PSI 压力计算纳入了已终结的僵尸进程。
  - **整改**: 回滚后打上 `rollback_locked = true` 阻止自动再推进；计算负载压力指标时排除已退出进程。
- **[Telemetry-5] CloudEvents 环形缓冲、EWMA NaN 防护与主网关 413 保护** (`src/telemetry/cloudevents.ts`, `src/profiler.ts`, `src/server_main.ts`):
  - **问题**: 长期运行的遥测事件总线持续累加事件导致内存无上限膨胀；EWMA 权重输入异常导致衰减指标变为 `NaN`；主服务器缺少请求体体积硬上限。
  - **整改**: 为 `CloudEventsBus` 施加固定容量（默认 1000 条）的先进先出环形队列；清洗所有计费与资源指标；`server_main.ts` 接入 1MB 体积守卫。

---

## 10. M59：SQLite 复合主键与落盘、KV TTL 淘汰、SSE 容错与 UFFD 完整性

- **[M59-1] SQLite 复合主键支持、`DELETE FROM` 磁盘刷盘与 `LIMIT` 截断** (`src/cloudflare/env.ts`):
  - **问题**: `InMemoryCellSqlStorage` 在 `INSERT` 时仅以第一列值作为主键，导致 `harness_turns`（`PRIMARY KEY (task_id, turn_index)`）同任务不同轮次记录相互覆盖；`DELETE FROM` 遗漏了 `this.flushToDisk()` 导致冷启动后已删数据复活；`SELECT` 语句未支持 `LIMIT`。
  - **整改**: 解析 DDL 中的复合主键列名，使用 `col1::col2` 组合主键存储；在 `DELETE FROM` 完成后调用 `flushToDisk()` 同步磁盘文件；支持 `SELECT ... LIMIT N / ?`。
- **[M59-2] KV `expirationTtl` 物理过期与分片参数合法性校验** (`src/cloudflare/env.ts`, `src/cloudflare/celld_native_optimizations.ts`):
  - **问题**: `InMemoryKvNamespace.put` 忽略了 `options?.expirationTtl`，导致本该超时的锁和临时缓存永远存活；分片数量允许非法值 `<= 0`。
  - **整改**: 在 KV 内部记录绝对毫秒时间戳 `expiresAtMs`，并在 `get()` 时惰性校验与清理过期键；校验分片数必须为正整数。
- **[M59-3] LLM SSE 畸变帧跳过、时钟回拨负延迟钳制与空 Token 用量保真** (`src/router/live_llm_adapter.ts`):
  - **问题**: 单个被网络截断的非合法 JSON `data:` 帧直接导致解析器抛错中断流；时钟微调导致 `lastChunkMs < firstChunkMs` 时产生负数 TPOT 耗时；空回答输出丢弃计费统计。
  - **整改**: 容错捕获损坏帧并递增 `malformedFramesSkipped` 计数；对耗时施加 `Math.max(0, ...)` 非负钳制；空 Token 输出依然发出包含 `prompt_tokens` 的结算事件。
- **[M59-4] UFFD 远端快照页面重新暂存缓存失效与缺页校验** (`src/runtime/uffd_lazy_restorer.ts`):
  - **问题**: `stageRemoteSnapshotPage` 更新相同页面索引时未清理 `this.faultedPages`，导致新任务缺页时命中上一任务留下的陈旧页面摘要。
  - **整改**: 在暂存新页面时显式调用 `this.faultedPages.delete(pageIndex)` 废止旧缓存，并在缺页处理中校验 SHA-256 数据完整性。
- **[M59-5] Vsock 桥接强制 HTTP/HTTPS 协议与逃逸命令拦截** (`src/runtime/vsock_bridge.ts`):
  - **问题**: 出站网络仅校验目标域名，放行了 `file://` 等非网络协议；命令安全检查仅做字面量匹配，漏掉了 `rm -rf --no-preserve-root /`、读取 `/proc/self/environ` 及云端 IMDS `169.254.169.254`。
  - **整改**: 限制仅允许 `http:` 与 `https:` 协议；基于正则表达式拦截变体危险命令、环境变量外泄与云元数据探测。
- **[M59-6] Facet 终态生命周期历史自动清理与 S3 LTX 路径校验** (`src/cloudflare/celld_native_optimizations.ts`, `src/checkpoint/s3_ltx_driver.ts`):
  - **问题**: 频繁创建和销毁 Facet 后，内部无限期保留终态历史；S3 LTX 分段写入未拒绝含有 `..` 的非法分片名或负数事务号。
  - **整改**: 增加最大保留记录上限（默认 512）并实现定期清理；严格校验分片 ID 与事务号单调自增非负整数约束。

---

## 11. M60：全链路持久化闭环、深度幂等、确定性断点恢复与重复合流提权

- **[M60-1] 端到端全链路 SQLite 持久化与重水合** (`src/checkpoint/unified_state_store.ts`, `src/cloudflare/scheduler_cell.ts`, `src/cloudflare/env.ts`):
  - **问题**: 重启后任务的 `idempotency_key` 索引、`current_turn` 轮次进度丢失；`TurnWalEngine`、`SagaToolGateway`、`TransactionalOutboxDispatcher` 缺少直接 SQLite 落盘；`DurableDiskCellSqlStorage` 传入目录 + `cellName` 时未解析出正确的 SQLite 文件路径。
  - **整改**:
    - 为 `TurnWalEngine`（`turn_wal_entries`）、`SagaToolGateway`（`saga_execution_journal`）、`TransactionalOutboxDispatcher`（`transactional_outbox_messages`）接入底层 SQLite 表与启动自动重水合。
    - 在 `UnifiedComponentPersistenceStore` 中实现 `snapshotFullLinkState` 与 `rehydrateFullLinkState`，一次性恢复调度器、WAL 链、Saga 日志与 Outbox 队列。
    - 修复 `DurableDiskCellSqlStorage`，支持目录加 `cellName` 自动补全 `.sqlite.json` 路径。
- **[M60-2] 租户隔离的任务幂等与深度规范化嵌套 JSON 工具哈希** (`src/scheduler.ts`, `src/saga_gateway.ts`):
  - **问题**: `CoreScheduler5D` 幂等索引缺少租户前缀，导致多租户相同业务 ID 发生碰撞；`SagaToolGateway.generateIdempotencyKey` 仅对顶层对象键排序，嵌套对象内部键序不同时哈希不一致，导致副作用工具被重复执行。
  - **整改**:
    - 幂等键强制升级为 `${tenant_id}::${idempotency_key}`。
    - 实现 `SagaToolGateway.canonicalizeValue`，深度递归规范化任意层级对象（数组保持顺序，对象递归排序键），保证语义相同的嵌套参数生成 100% 相同的 SHA-256 幂等键。
- **[M60-3] SSE 事件 ID 幂等去重与 CloudEvents/FinOps 计费防重** (`src/gateway/sse_stream.ts`, `src/telemetry/cloudevents.ts`):
  - **问题**: 重试发布具有相同 `envelope.id` 的事件时，历史列表重复追加且下游客户端重复接收；`CloudEventsBus.emitEvent` 重试时重复计算计费开销。
  - **整改**:
    - `SseEventStreamGateway.publish` 记录已发布的 `id`，遇到相同事件直接返回 0 跳过派发与追加。
    - `CloudEventsBus.emitEvent` 增加 `idempotencyKey` / `eventId` 校验，重试事件直接返回已有实例，不增加租户开销。
- **[M60-4] 确定性 WAL 断点恢复与半轮次未提交脏状态回滚** (`src/checkpoint/wal_engine.ts`, `src/node_daemon.ts`):
  - **问题**: 节点崩溃发生在一轮中部分工具已执行、但尚未写入 WAL 检查点之时，重启后残留未提交副作用；跨节点迁移后新节点无检查点，产生执行裂化。
  - **整改**:
    - `TurnWalEngine.appendTurnDelta` 支持相同 `(turnIndex, fencingToken, checksumSha256)` 幂等重试，`recoverTaskBreakpoint` 剔除损坏帧并选出每轮最高纪元有效检查点。
    - `NodeDaemon.recoverTaskFromBreakpoint` 自动重建节点检查点链，恢复 `task.current_turn`，并通过 `SagaToolGateway.rollbackAndInvalidateTask` 自动回滚并作废大于恢复轮次的半轮次未提交副作用，确保从下一轮重新干净执行。
- **[M60-5] 在途重复任务合流检查与队列优先级实时提升** (`src/models.ts`, `src/queue_manager.ts`, `src/scheduler.ts`, `src/gateway/server.ts`, `src/cloudflare/scheduler_v25_persistence.ts`):
  - **问题**: 并发提交内容完全相同（相同租户、`prompt`、依赖）的任务时，系统重复排队执行；当高优重复请求到来时，排队中的任务无法得到提权。
  - **整改**:
    - 增加内容指纹 `computeTaskContentFingerprint` 与在途合流机制（`inspectAndMergeDuplicateTask`，配置 `mergeDuplicateInFlight: true`），重复请求自动合流到已有在途任务，记录合并次数与关联 ID。
    - 队列管理器新增 `reprioritizeQueuedTask`，当合流的重复请求具有更高优先级（如 `P0_EMERGENCY`）时，立即提升原排队任务优先级并在二叉堆中重新堆化调整。

---

## 12. M61：全子系统持久化、CAS/Push 幂等重试、断点状态回滚与重复仲裁合流

- **[M61-1] A2A 黑板与拓扑 CAS 幂等重试、补丁自冲突与持久化** (`src/a2a/blackboard_and_git_arbiter.ts`, `src/cluster/topology_cas_coordinator.ts`):
  - **问题**: 网络超时重试已写入的同一 CAS 变更时误报冲突；同一 Agent 在同一仲裁周期内重试提交多条补丁记录时与自身产生假冲突，并被误扣除仲裁惩罚；黑板记录未落盘 SQLite。
  - **整改**:
    - `SharedBlackboardAndGitArbiter` 支持绑定 SQLite（`a2a_blackboard_entries` / `a2a_agent_conflicts`），并在 `putCas` 中当版本号落后 1 且规范化值相同时以 `deduplicated: true` 幂等返回。
    - `arbitrateGitPatchTouches` 在仲裁前按 `agentId` 合流合并涉及文件与最高分，避免同 Agent 自冲突。
    - `TopologyCasCoordinator` 支持成员集合完全一致时的幂等重试。
- **[M61-2] 审批门禁、DAG 配额托管与 FinOps 账本持久化与防重** (`src/saga/staging.ts`, `src/dag/escrow.ts`, `src/finops/budget_controller.ts`):
  - **问题**: 重试 `stageEffect` 产生重复副作用并在提交时执行两次；重试 `requestHumanApproval` 产生重复工单；重试父子配额托管导致可用槽位被重置；重试扣费/退费导致双计。
  - **整改**:
    - 全部模块支持绑定 SQLite 实时落盘与冷启动恢复。
    - `stageEffect` 与 `requestHumanApproval` 按规范化参数去重合流；`resolveApproval` 增加终态防篡改与同状态幂等返回。
    - `DagEscrowCoordinator` 对已预订/已注册的任务实现幂等，防配额二次占用。
    - `FinOpsBudgetController.recordSpend` 与 `refundSpend` 均支持 `idempotencyKey`，避免网络超时重试导致双重记账。
- **[M61-3] 速度与语义死循环熔断器单轮重试就地合并** (`src/harness/velocity_breaker.ts`):
  - **问题**: 单个轮次（如 `turnIndex = 1`）因网络波动重试时，`SyntacticAndVelocityBreaker` 连续记录多条相同轮次，导致重试 2 次即被误判为 `SEMANTIC_DEAD_LOOP`，且 Token 统计被翻倍。
  - **整改**: 在 `recordAndCheckTurn` 中识别未清理周期内的相同 `turnIndex`，执行就地合并更新（`coalescedSameTurnRetry: true`），只有在跨不同轮次连续调用同一工具与参数时才触发死循环熔断，并增加 `persisted_velocity_breaker` SQLite 落盘。
- **[M61-4] 反向隧道指令幂等合流与 HarnessCell 崩溃对账** (`src/cloudflare/tunnel_gateway_cell.ts`, `src/cloudflare/harness_cell.ts`):
  - **问题**: `TunnelGatewayCell` 重复下发指令会导致 Body 节点重复拉起容器；`AgentTaskHarnessCell` 在向 R2 写入 LTX 成功后、向 SQLite 写入检查点前如果崩溃，重启后本地断点滞后于 R2。
  - **整改**:
    - `TunnelGatewayCell.enqueueCommandForHost` 支持 `idempotencyKey`，在途指令合流共享同一个 `completionPromise`，已完成指令直接返回缓存结果。
    - `AgentTaskHarnessCell` 实现 `reconcileBreakpointFromR2AndSql()`，冷启动时扫描 R2 快照自动补齐 SQLite 检查点，并在 `executeAndCheckpointTurn` 中支持相同轮次与参数的幂等返回。
- **[M61-5] LayerFS 工作区分支断点回滚与远端 Store 幂等推送重试** (`src/runtime/layerfs_workspace_driver.ts`, `src/checkpoint/layerfs_remote_sync.ts`):
  - **问题**: 任务回滚到历史轮次断点时，LayerFS 分支头指针无法精确定位回退；`LayerFsRemoteStoreSynchronizer.pushStoreSnapshot` 携带同一快照重试时误报 Rule D9 409 冲突。
  - **整改**:
    - 在 `LayerFsWorkspaceDriver` 中新增 `rollbackBranchToCommitOrStep(branchName, targetCommitOrStepId)`，支持分支向历史断点干净回退。
    - 在 `LayerFsRemoteStoreSynchronizer.pushStoreSnapshot` 中支持显式 `idempotencyKey`，在快照数据 SHA-256 摘要相同且属于刚提交的上一世代时安全幂等返回（无键陈旧推送依然严格执行 Rule D9 拦截）。

---

## 13. M62：DFX 全维度可靠性、可诊断性、可服务性与可观测性加固

从 **DFX (Design for Excellence)** 维度展开深度排查与加固，涵盖可靠性（Reliability）、可维护性（Maintainability）、可诊断性（Diagnosability）、可服务性（Serviceability）、安全性（Safety）与可部署性（Deployability）：

- **[M62-1] DFX-Reliability & Recoverability: `CgroupPhaseController` & `LinuxCgroupV2AndPsiDriver` CFS 最小硬下限与目录释放回收** (`src/runtime/phase_controller.ts`, `src/runtime/linux_cgroup_psi.ts`):
  - **问题**: 在任务分配极低计算资源（如 `think_cpu_cores: 0.001`）时，计算所得 CFS 额度过小甚至为 0，导致容器进程被内核完全挂起冻死（CFS Starvation）；任务退出后，主机遗留数以万计的空 cgroup 目录，耗尽 Linux 内核 inode 与 cgroup ID 上限。
  - **整改**: 在控制器中设置 `MIN_CPU_QUOTA_US = 5000`（即 0.05 核下限保护），确保任意任务具有最低限度的探针心跳与恢复能力；在驱动中实现 `removeTaskCgroup(taskId)`，任务终结时自动、幂等清理对应 cgroup 目录。
- **[M62-2] DFX-Observability & Diagnosability: `OtelAndPrometheusExporter` 指标名规范化与基数上限内存保护** (`src/telemetry/otel_prometheus.ts`):
  - **问题**: 动态生成的指标名称包含非法字符破坏 Prometheus 抓取格式；长期运行中不同租户与任务 ID 作为标签传入时导致指标基数爆炸（Metric Cardinality Explosion），引发 Node.js 进程内存无限制泄漏。
  - **整改**: 增加 `sanitizeMetricName` 自动规范化字符；设置 `MAX_METRIC_ENTRIES = 500` 硬性指标条目上限，超出容量拒绝动态扩充并记录告警，保护长期运行可观测性子系统的内存安全。
- **[M62-3] DFX-Safety & Fault Tolerance: `LinuxPsiMemoryGuard` 压力指标有限值清洗与 P0/P1 驱逐豁免** (`src/runtime/psi_guard.ts`):
  - **问题**: `/proc/pressure/memory` 偶发坏数据或负数/NaN 解析值导致防线误触发；在发生紧急内存压力时，原本处于自愈与控制平面的核心高优任务（`P0_EMERGENCY` 与 `P1_HIGH`）可能被误选为受害者被强行挂起与驱逐。
  - **整改**: 彻底清洗 PSI 指标为有限非负数值；赋予 `P0_EMERGENCY` 与 `P1_HIGH` 紧急驱逐豁免权，仅将 `P2_STANDARD` 与 `P3_BATCH` 任务纳入驱逐候选集，杜绝自愈控制流被误杀引发的集群连环雪崩。
- **[M62-4] DFX-Operability & Serviceability: `CapacitySlaEnforcer` 应急预留状态持久化与动态调优** (`src/cluster/capacity_sla_enforcer.ts`):
  - **问题**: 容量 SLA 违规判定遗漏了零除保护；应急预留（Emergency Headroom）标记仅存放在内存中，重启后预留状态丢失导致新流量涌入再次挤垮集群；策略阈值缺乏热更新原语。
  - **整改**: 增加输入数值的有限区间校验；绑定 SQLite 表 `cluster_capacity_sla_state` 实时持久化与冷启动恢复应急预留激活状态；提供 `updatePolicy` 支持运行时动态调控容量水位。
- **[M62-5] DFX-Maintainability & Deployability: `EdgeTunnelHaRouter` 幂等透传与 `CrossRegionAsyncReplicator` 异常隔离** (`src/runtime/edge_tunnel_ha_router.ts`, `src/cluster/cross_region_replicator.ts`):
  - **问题**: 反向隧道路由器在向 Body 宿主下发异步长轮询指令时未透传幂等键，导致重试指令多次下发拉起重复容器；跨地域异步复制 S3 失败时未做隔离，网络异常直接向上抛出中断主业务事务。
  - **整改**: 支持 `options.idempotencyKey` 全链路透传至 `TunnelGatewayCell` 合流等待；异步跨地域 S3 复制增加全面的非阻塞隔离与 URL/路径清洗，保障灾备复制故障不干扰生产运行。

---

## 14. M63：DFX 全模块深度加固 II（混沌自愈、Fail-Safe 削峰分片、主权路由落盘、多环解死锁与安全计费钳制）

- **[M63-1] DFX-Reliability & Fault Tolerance: `AutomatedChaosDrillRunner` `try/finally` 自动愈合与 `NetworkChaosFaultInjectionProxy` TTL 自过期** (`src/cluster/chaos_drill_runner.ts`, `src/runtime/network_chaos_proxy.ts`):
  - **问题**: 混沌演练在注入网络分区后若发生异常未执行 `healPartition`，或未配置过期时间，会导致生产主机永久处于断网分区状态；且 `runSplitBrainAndRecoveryDrill` 未校验 `NaN`/负数令牌。
  - **整改**: 为 `NetworkChaosFaultInjectionProxy.injectPartition` 增加 `ttlMs` 自动到期愈合与 `healAllPartitions()`；在 `executeLivePartitionAndFencingDrill` 中使用 `try ... finally` 无条件愈合网络分区，并对 Fencing 令牌执行有限非负校验。
- **[M63-2] DFX-Availability & Fail-Safe Admission: `BidirectionalBackpressureController`、`PriorityLoadShedder`、`DynamicQueueAutoSharder` 与 `BrainBodyContainerPoolScaler` 零除/NaN 防护** (`src/cluster/backpressure_loop.ts`, `src/cluster/priority_load_shedder.ts`, `src/cloudflare/horizontal_scaler.ts`):
  - **问题**: 当所有 LLM 端点离线（`totalEndpoints = 0` 且存在限流）或 PSI 传入 `NaN` 时，背压控制器误判为 `GREEN`；`PriorityLoadShedder.shouldAdmitUnderLoad` 在 `utilizationRatio` 为 `NaN` 时发生 Fail-Open 放行 `P3_BATCH`；`DynamicQueueAutoSharder.adjustShards` 在 `maxWriteRpsPerShard <= 0` 或 `NaN` 时将分片数污染为 `NaN`/`Infinity`。
  - **整改**: 背压控制器对零端点故障与 `NaN` 指标自动进入 `RED_SHEDDING` 保守保护；`PriorityLoadShedder` 在 `NaN` 负载率下执行 Fail-Safe（仅允许 `P0_EMERGENCY`）；自动分片器与容器池扩缩容全面清洗非负有限输入。
- **[M63-3] DFX-Compliance & Determinism: `GeoSovereigntyAndLatencyRouter` 负延迟清洗、同延迟确定性裁决与 SQLite 持久化** (`src/cluster/geo_routing.ts`):
  - **问题**: 注册区域若传入负数或 `NaN` 延迟会破坏排序并错误吸纳流量；相同延迟的区域缺乏确定性二级排序；网关重启后区域可用性状态丢失。
  - **整改**: 将非法/负数延迟惩罚性清洗为 `999,999ms`；在延迟相同时按 `regionId` 字典序确定性裁决；新增 `geo_sovereignty_regions` SQLite 持久化与冷启动重水合。
- **[M63-4] DFX-Serviceability: `A2aWaitForGraphDeadlockDetector` 多环并发死锁迭代式彻底解环** (`src/a2a/deadlock_detector.ts`):
  - **问题**: 当等待图中同时存在多个不相交或嵌套死锁环（如 `A <-> B` 与 `C <-> D`）时，`resolveCycleWithLowestPriorityVictim` 仅剔除首个环的受害者即返回，导致其余死锁环仍处于未解开状态（`resolved: false`）。
  - **整改**: 升级为有界迭代式解环循环，逐轮定位当前环中优先级最低的受害者并剥离其等待边，直至全图死锁环 100% 消除，同时返回完整的 `evictedVictims` 列表。
- **[M63-5] DFX-Security & Cost Accuracy: `ConfidentialTeeAttestationBridge` 外部验签防篡改、`ZeroTrustSandboxSecurityGuard` IPv6-IMDS/块设备防护与 `ModelPoolClientAdapter` 成本钳制** (`src/runtime/confidential_tee_bridge.ts`, `src/runtime/zero_trust_security_guard.ts`, `src/router/pool_client.ts`):
  - **问题**: TEE 证明生成未严格校验十六进制度量摘要格式且缺少外部 Quote 防篡改校验函数；零信任命令守卫未拦截 IPv4 映射 IPv6 云元数据地址 `::ffff:169.254.169.254`、ECS 元数据 `169.254.170.2`、`mkfs.*`、`dd of=/dev/*` 及空字节 ` ` 注入；`ModelPoolClientAdapter` 在 `prefixCacheMatchedRatio > 1.0` 时算出负数 `uncachedTokens` 导致计费失真。
  - **整改**: 增加严格十六进制格式检查与 `verifyExternalAttestationQuote`；在零信任守卫中拦截 IPv6/ECS IMDS、块设备格式化覆盖与空字节注入；在模型池客户端中将 `prefixCacheMatchedRatio` 严格钳制在 `[0, 1]` 并保证 `uncachedTokens >= 0` 与 `costUsd >= 0`。

---

## 15. 测试套件与自动化验证矩阵

代码库包含 54 个端到端单元测试与混沌集成测试套件，全面覆盖以上改进：

| 序号 | 测试文件 | 验证范围与断言项 | 状态 |
| :---: | :--- | :--- | :---: |
| 1 | `tests/celld_cloudflare.test.ts` | Cloudflare DO / Workers 原生绑定与持久化 | PASS |
| 2 | `tests/celld_ha_durability.test.ts` | RPO=0 LTX 分段刷盘、CAS 与分片队列 | PASS |
| 3 | `tests/chaos_benchmarks.test.ts` | 500 任务高并发调度性能（<100ms）与 PSI 熔断 | PASS |
| 4 | `tests/code_audit_improvements.test.ts` | 验证器、背压、DAG 托管与速度熔断器极限测试 | PASS |
| 5 | `tests/design_deliverable_consistency.test.ts` | 双元组 FenceToken、CRC32 校验与 58 核心模块一致性 | PASS |
| 6 | `tests/full_component_persistence.test.ts` | 8 表状态全量快照与调度器重水合 | PASS |
| 7 | `tests/google_engineering_all_tasks_audit.test.ts` | 全模块类型规范与端到端行为审计 | PASS |
| 8 | `tests/horizontal_scaleout.test.ts` | 64 分片路由与动态队列自动扩缩容 | PASS |
| 9 | `tests/integration_suite.test.ts` | 调度、Fencing、多租户公平性与 Saga 幂等 | PASS |
| 10 | `tests/m10_hotpath_protocols.test.ts` | SigV4 S3 LTX 驱动、管理客户端与 SSE 延迟计算 | PASS |
| 11 | `tests/m11_linux_runtime_primitives.test.ts` | Linux Cgroup v2、OverlayFS、Vsock 域套接字与热迁移 | PASS |
| 12 | `tests/m12_m13_production_suite.test.ts` | Otel/Prometheus 指标、MCP 注册、FinOps 预算与 CLI | PASS |
| 13 | `tests/m15_celld_facets_dynamic_workers.test.ts` | Facet 子 Actor、V8 沙箱隔离与 32 分片 KV 优化 | PASS |
| 14 | `tests/m16_celld_v063_optimizations.test.ts` | ctx.container 网桥、二叉堆队列与 LTX 压缩 | PASS |
| 15 | `tests/m16_production_hardening_suite.test.ts` | 金丝雀滚动发布、优先级载荷脱落与网络混沌代理 | PASS |
| 16 | `tests/m17_layerfs_integration.test.ts` | LayerFS 前缀/增量压缩、工作区挂载与三方合并 | PASS |
| 17 | `tests/m17_reliability_capacity_console.test.ts` | 容量 SLA 守护、拓扑 CAS 与多节点开发服务器 | PASS |
| 18 | `tests/m18_local_and_compose_runtime.test.ts` | 本地/Docker 运行时服务与 TanStack 管理 BFF 交互 | PASS |
| 19 | `tests/m22_m23_cow_zerotrust_a2a.test.ts` | A2A 黑板、死锁检测、零信任防御与 TEE 证明 | PASS |
| 20 | `tests/m24_m25_affinity_compactor_slsa_profiler.test.ts` | SLSA Provenance 证明、UFFD 缺页恢复与 CAS GC | PASS |
| 21 | `tests/m32_p0_mcp_hub_predictive_autoscaler.test.ts` | 预测性沙箱预热与双宿主边缘反向隧道高可用 | PASS |
| 22 | `tests/m33_dev_cluster_live_functional_e2e.test.ts` | 3 Brain + 2 Body + 2 Gateway 全链路集成调度 | PASS |
| 23 | `tests/m34_p0_p2_hardening_e2e.test.ts` | Brain 崩溃重启恢复、跨节点分支同步与 eBPF 安全桥 | PASS |
| 24 | `tests/m35_nat_tunnel_failover_rebalance_a2a_gc.test.ts` | NAT 隔离反向隧道长轮询、256 分片再平衡与 CAS GC | PASS |
| 25 | `tests/m36_attestation_sla_canary_layerfs_merge.test.ts` | 零信任命令安全拦截、金丝雀回滚与跨节点 S3 分支合并 | PASS |
| 26 | `tests/m37_geo_replication_chaos_deterministic_fork.test.ts` | GDPR 地理主权隔离、S3 异步复制与 CoW 分支分叉 | PASS |
| 27 | `tests/m38_cognitive_finops_saga_mcp_timetravel.test.ts` | 前缀缓存亲和路由、LIFO Saga 回滚与时间旅行调试 | PASS |
| 28 | `tests/m39_harness_dag_cloudevents_sse_cli.test.ts` | Harness 阶段控制器、Velocity 熔断与 HMAC-SHA256 Outbox | PASS |
| 29 | `tests/m40_tanstack_console_ui_bff.test.ts` | 原型控制台前端静态界面与 BFF 聚合端点 | PASS |
| 30 | `tests/m41_management_plane_console.test.ts` | 租户 CAS、P0 租约排空与 4-Eyes 人工审批门禁 | PASS |
| 31 | `tests/m42_cross_gateway_mgmt_wal_cli.test.ts` | 跨网关状态同步、WAL CRC32 检查器与管理 CLI | PASS |
| 32 | `tests/m43_celld_native_rule_d9_quota_sse.test.ts` | V8 沙箱隔离、Rule D9 409 拦截与审计 SSE 流 | PASS |
| 33 | `tests/m44_mgmt_scale_canary_finops_audit.test.ts` | 节点动态扩缩容、闭环审批自执行与审计搜索 | PASS |
| 34 | `tests/m45_a2a_migration_gc_diagnostics.test.ts` | A2A 黑板仲裁、任务跨节点热迁移与版本诊断探针 | PASS |
| 35 | `tests/m46_real_execution_harness_migration_mcp.test.ts` | 真实子进程 CoW 二分排查与 HTTP 迁移压缩包校验 | PASS |
| 36 | `tests/m47_full_runtime_cgroup_overlay_vsock_ed25519.test.ts` | 物理 cgroup v2/OverlayFS、Vsock 真实调用与 Ed25519 签名 | PASS |
| 37 | `tests/m48_unified_persistence_container_speculative_autoscale.test.ts` | 8 表全量持久化、安全凭据校验与推测性分支执行 | PASS |
| 38 | `tests/m49_router_pipeline_kernel_security_chaos_federation.test.ts` | 令牌桶限流、套接字守护进程与多地域联邦路由 | PASS |
| 39 | `tests/m50_design_config_runtime_observability_ops.test.ts` | 配置中心 CAS 热重载、版本回滚与 SLO 燃尽率告警 | PASS |
| 40 | `tests/m51_scheduler_anti_stale_deadlock_starvation.test.ts` | 调度内核防陈旧心跳、防死锁与防饥饿抢占补偿 | PASS |
| 41 | `tests/m52_system_wide_stale_deadlock_starvation_rectification.test.ts` | Cell 状态恢复、DO 闹钟续期与隧道在途重试 | PASS |
| 42 | `tests/m53_deep_cross_layer_risk_elimination.test.ts` | Saga 幂等作废、FinOps UTC 自动跨天与双元 WAL 单调性 | PASS |
| 43 | `tests/m54_deep_runtime_resilience_anti_starvation.test.ts` | Harness Fencing 恢复、审批 TTL 与 A2A 冲突老化加速 | PASS |
| 44 | `tests/m55_storage_shard_sync_transaction_audit.test.ts` | 租户跨任务幂等查找、Last-Brain 存活与 RPO=0 原子回滚 | PASS |
| 45 | `tests/m56_kernel_storage_merge_backpressure_audit.test.ts` | 统一水准线重水合、3-Way 删除合并与 P0 应急预留池 | PASS |
| 46 | `tests/m57_router_gc_scale_daemon_validator_vsock_audit.test.ts` | 半开单探针、叶子优先 CAS GC、轮次单调性与 413 大包保护 | PASS |
| 47 | `tests/m58_container_lsm_spiffe_canary_telemetry_audit.test.ts` | 私网 RFC1918 隔离、eBPF 根路径锚定与遥测环形队列 | PASS |
| 48 | `tests/m59_sql_kv_sse_uffd_vsock_ltx_audit.test.ts` | SQLite 复合主键/DELETE 刷盘、KV TTL 淘汰与 UFFD 缓存失效 | PASS |
| 49 | `tests/m60_full_link_persistence_idempotency_breakpoint_coalesce_audit.test.ts` | 全链路 SQLite 持久化、深度递归规范化幂等与断点恢复 | PASS |
| 50 | `tests/m61_subsystem_persistence_idempotency_breakpoint_coalesce_audit.test.ts` | 全子系统持久化、CAS/Push 幂等重试与同轮次熔断器合流 | PASS |
| 51 | `tests/m62_dfx_hardening_audit.test.ts` | DFX 可靠性（CFS下限/清理）、可诊断性（基数上限）、安全性（P0/P1豁免）与服务性（SLA预留落盘） | PASS |
| 52 | `tests/m63_dfx_deep_module_hardening_audit.test.ts` | DFX 深度加固 II（混沌TTL自愈、NaN防御、主权路由SQLite、多环解死锁、TEE/IMDSv6与计费钳制） | PASS |
| 53 | `tests/scheduler_authority.test.ts` | 256 虚拟槽位权威归属与跨纪元租约验证 | PASS |
| 54 | `tests/scheduler_persistence.test.ts` | 单写路径持久化与幂等键去重提交 | PASS |
| 55 | `tests/spec_completeness_audit.test.ts` | 规范完整性与全套组件端到端互通闭环 | PASS |
| 56 | `tests/v23_geo_routing.test.ts` | GDPR 地理主权硬锁定与合规路由 | PASS |

---

## 16. 核心管理面端点与 CLI 命令参考

本系统所有改进与修复均接入了管理面控制总线，并提供了即席审计接口与控制台命令：

- **M59 数据库与网络加固审计**:
  - HTTP 端点: `POST /v1beta1/management/system/sql-kv-sse-uffd-audit`
  - CLI 命令: `node dist/src/cli/anstsctl.js mgmt-sql-kv-uffd-audit`
- **M60 全链路持久化与幂等断点审计**:
  - HTTP 端点: `POST /v1beta1/management/system/persistence-idempotency-recovery-audit`
  - CLI 命令: `node dist/src/cli/anstsctl.js mgmt-persistence-idempotency-audit`
- **M61 跨子系统持久化与幂等合流审计**:
  - HTTP 端点: `POST /v1beta1/management/system/subsystem-persistence-idempotency-audit`
  - CLI 命令: `node dist/src/cli/anstsctl.js mgmt-subsystem-idempotency-audit`
- **M62 DFX 全维度可靠性与可服务性加固审计**:
  - HTTP 端点: `POST /v1beta1/management/system/dfx-hardening-audit`
  - CLI 命令: `node dist/src/cli/anstsctl.js mgmt-dfx-audit`
- **M63 DFX 第二阶段全模块深度加固审计**:
  - HTTP 端点: `POST /v1beta1/management/system/dfx-deep-audit`
  - CLI 命令: `node dist/src/cli/anstsctl.js mgmt-dfx-deep-audit`
- **全系统 CI 静态检查与集成测试**:
  - 执行命令: `env -u NODE_PATH npm run ci` (依次执行 TypeScript AST 语法检查、Wrangler 配置校验、代码构建与 56 个测试套件执行)


## 十四、M64 落地功能如实实现一致性全链路核查与闭环修复（End-to-End Implementation Consistency & Authentic Feature Wiring Suite）

| 编号 | 模块与涉及文件 | 发现的实现不一致/断链隐患 | 修复方案与测试验证 |
| :--- | :--- | :--- | :--- |
| **M64-1** | **Cloudflare Worker 与 Cell 路由/队列投递一致性**<br>`src/cloudflare/worker.ts`<br>`src/cloudflare/scheduler_cell.ts`<br>`src/cloudflare/harness_cell.ts` | `worker.fetch` 将 `/v1/scheduler/*` 原样转发至 `AnstsSchedulerCell.fetch`，但 Cell 内部仅匹配无前缀 `/submit` 与 `/schedule-tick`，导致经由 Worker 入口的调度请求静默落空；`worker.queue` 仅调用 `msg.ack()` 而未向 `ANSTS_SCHEDULER_CELL` 投递任务；未路由 `/v1/harness/*`。 | `AnstsSchedulerCell.fetch` 兼容带 `/v1/scheduler` 前缀与无前缀路径（含 `/submit`、`/register-node`、`/schedule-tick`、`/tasks`）；为 `AgentTaskHarnessCell` 补齐 `fetch` 接口并在 `worker.ts` 接入 `/v1/harness/*`；`worker.queue` 真正向 `ANSTS_SCHEDULER_CELL` 提交任务并在失败时触发 `msg.retry()`。 |
| **M64-2** | **三套持久化引擎 `TaskSpec` 100% 字段保真一致性**<br>`src/cloudflare/scheduler_cell.ts`<br>`src/cloudflare/scheduler_v25_persistence.ts`<br>`src/checkpoint/unified_state_store.ts` | 三套 SQLite 存储引擎在序列化/反序列化 `TaskSpec` 时遗漏了 `resource_profile`（5D 算力需求）、`parent_task_id`、`expected_SubAgent_count`、`prefix_Cache_hash`、`retry_count`、`preempted_count`、`last_preempted_at` 等关键字段，导致重启后退化为默认值。 | 统一补全 `AnstsSchedulerCell`、`SchedulerV25SingleWritePathStore` 与 `UnifiedComponentPersistenceStore` 对 `TaskSpec` 全量字段的落盘与冷启动恢复保真。 |
| **M64-3** | **Saga 补偿/暂存审批 Turn 精度与 MCP/Router/Escrow 持久化闭环**<br>`src/saga/compensator.ts`<br>`src/saga/staging.ts`<br>`src/saga_gateway.ts`<br>`src/saga/mcp_registry.ts`<br>`src/cognitive_router.ts`<br>`src/dag/escrow.ts` | `SagaCompensatorEngine`、`McpDynamicToolRegistry`、`CognitiveRouter` 缺少 `bindSqlStorage` 落盘能力；`StagingCommitAndApprovalGate.commitStagedEffects` 未向回调传递真实 `turnIndex`，且 `server_main.ts` 硬编码 `turnIndex = 1`，导致 Turn 3 暂存副作用在按 Turn 3 回滚时漏回滚；`DagEscrowCoordinator` 重启后丢失 `dependencyGraph` 边导致跨重启环路漏检。 | 为 `SagaCompensatorEngine`（含重启后动态补偿函数解析器与幂等记录）、`McpDynamicToolRegistry`（自动重绑 `SagaToolGateway`）、`CognitiveRouter`（含非有限值防护）及 `DagEscrowCoordinator`（持久化 `dependencyGraph` 边）全面接入 SQLite；修正 `commitStagedEffects` 传递真实 `item.turnIndex` 并由 `POST /v1beta1/saga/rollback` 调用 `rollbackAndInvalidateTask`。 |
| **M64-4** | **主服务 `AnstsLocalAndComposeServer` 全子系统 SQLite 接线与请求合并一致性**<br>`src/server_main.ts`<br>`src/scheduler.ts`<br>`src/cloudflare/ha_scheduler_shard_coordinator.ts` | `AnstsLocalAndComposeServer` 初始化时仅将 `DurableDiskCellSqlStorage` 绑定至 `v25Store`，其余 15 个有状态子系统仍使用纯内存实例；冷启动仅恢复 `scheduler.allTasks` 而未恢复 `idempotencyIndex`/`contentFingerprintIndex`/`leaseCoordinator`/`queueManager`；`POST /v1/tasks` 缺少与 `ApiGatewayServer` 一致的跨 `task_id` 幂等与指纹合并及在途任务优先级提升。 | 在 `AnstsLocalAndComposeServer` 中将 `durableSql` 统一绑定至全部 15 个有状态子系统；补全冷启动对调度器全量索引与队列的恢复；对齐 `POST /v1/tasks` 合并去重与 `CoreScheduler5D` 在途（`LEASED`/`RUNNING`）任务的优先级提升。 |
| **M64-5** | **`AgentHarnessRunner` 路由 Token 计量闭环与分片调度路由一致性**<br>`src/harness/runner.ts`<br>`src/cloudflare/horizontal_scaler.ts` | `AgentHarnessRunner.executeSingleTurn` 在执行完单轮工具后未调用 `router.reportInvocationMetrics()`，导致经由 Harness 执行的轮次从不更新 `CognitiveRouter` 的 `tpm_used` 与 `ttft_ewma_ms`；`PartitionedSchedulerRouter` 缺少跨分片节点注册与跨分片 `scheduleTickAcrossShards()`。 | `AgentHarnessRunner.executeSingleTurn` 在每轮完成后真实调用 `router.reportInvocationMetrics()`（幂等命中时计 0 Token）；为 `PartitionedSchedulerRouter` 增加 `stateFactory`、`registerNodeAcrossShards()` 与 `scheduleTickAcrossShards()`。 |

- **验证命令**：
  - CLI：`node dist/src/cli/anstsctl.js mgmt-consistency-audit`
  - HTTP API：`POST /v1beta1/management/system/implementation-consistency-audit`
  - E2E 测试套件：`tests/m64_implementation_consistency_audit.test.ts`（全仓 **57/57 测试套件，147/147 测试用例全部通过**）。


## 十五、M65 落地功能如实实现一致性深度核查第二阶段（Deep Implementation Consistency Phase II）

| 编号 | 模块与涉及文件 | 发现的实现不一致/断链隐患 | 修复方案与测试验证 |
| :--- | :--- | :--- | :--- |
| **M65-1** | **`WalCompactorAndPagedRestorer` 与 `LayerFsRemoteStoreSynchronizer` 构造函数契约与 SQLite 持久化对齐**<br>`src/checkpoint/compactor.ts`<br>`src/checkpoint/layerfs_remote_sync.ts` | `server_main.ts` 向 `WalCompactorAndPagedRestorer(4096, durableSql)` 与 `LayerFsRemoteStoreSynchronizer(bucket, projectId, durableSql)` 传入了 `durableSql`，但两个类构造函数未声明 `sql` 参数且未持久化压缩快照与 Rule D9 代际状态，导致重启后 `restoreViaUserfaultfd` 退化为默认 100 页、`remoteGeneration` 归零。 | 为 `WalCompactorAndPagedRestorer` 新增 `constructor(pageSizeBytes = 4096, sql?: SqlStorageLike)` 与 `persisted_compacted_snapshots` 表持久化；为 `LayerFsRemoteStoreSynchronizer` 新增第三参数 `sql?: SqlStorageLike` 与 `layerfs_remote_sync_state` 表持久化。 |
| **M65-2** | **灰度控制器回滚锁、预热器 EWMA 与资源画像器 SQLite 持久化恢复**<br>`src/cluster/canary_rollout_controller.ts`<br>`src/runtime/predictive_prewarmer.ts`<br>`src/profiler.ts` | `CanaryRolloutController` 触发 `ROLLED_BACK` 后若进程重启，状态重置为 `CANARY_ACTIVE`，绕过了 M58-4 回滚锁守卫；`PredictiveSandboxPrewarmer` 与 `TaskResourceProfiler` 学习到的 EWMA 到达率与租户资源画像在重启后丢失。 | 为 `CanaryRolloutController`（`persisted_canary_state`）、`PredictiveSandboxPrewarmer`（`persisted_prewarmer_state`）与 `TaskResourceProfiler`（`persisted_profiler_history`）全面补齐 SQLite 持久化与冷启动自动恢复，并在 `server_main.ts` 绑定 `durableSql`。 |
| **M65-3** | **主服务在线路由状态变更与 `v25Store` / `TurnWalEngine` 实时落盘同步**<br>`src/server_main.ts` | `/v1beta1/harness/run-turn`、`/v1beta1/saga/execute`、`/v1beta1/saga/approve`、`/v1beta1/dag/escrow` 及后台 `tickTimer` 修改内存中 `task.current_turn` 或 `task.state` 后未同步回写 `v25_scheduler_tasks`，且 `/v1beta1/dag/escrow` 缺少 `failChild` 分支。 | 新增 `syncTaskToV25Store` 并在上述所有路由与后台 `tickTimer` 状态变更后实时同步落盘；在 `/v1beta1/harness/run-turn` 完成后自动追加 `TurnWalEngine` 帧；在 `/v1beta1/dag/escrow` 中补齐 `failChild` 动作。 |
| **M65-4** | **`TunnelGatewayCell` 标准 DO `fetch` 接口与 Worker `/v1/tunnel/*` 路由闭环**<br>`src/cloudflare/tunnel_gateway_cell.ts`<br>`src/cloudflare/env.ts`<br>`src/cloudflare/worker.ts` | `worker.ts` 导出了 `TunnelGatewayCell`，但 `TunnelGatewayCell` 缺少 Durable Object `fetch(request)` 方法，`worker.ts` 也未将 `/v1/tunnel/*` 转发至 `ANSTS_TUNNEL_CELL`。 | 在 `CloudflareCelldEnv` 声明 `ANSTS_TUNNEL_CELL`，为 `TunnelGatewayCell` 实现支持 `/register`、`/poll`、`/ack`、`/health`、`/evict-stale` 的 `fetch` 方法，并在 `worker.ts` 完成 `/v1/tunnel/*` 路由挂载。 |
| **M65-5** | **CI 流水线跨文件 AST 导出与构造函数参数契约静态守卫**<br>`scripts/ci_check.mjs` | `scripts/ci_check.mjs` 原本仅逐文件编译，无法在编译期拦截跨文件导入缺失或 `new ClassName(...)` 参数数量与构造函数签名不一致的问题。 | 在 `scripts/ci_check.mjs` Stage 1 中基于 `@babel/parser` 增加全仓 144 个模块的跨文件 `import/export` 符号核验与 `new ClassName(...)` 构造函数参数数量边界静态守卫。 |

- **验证命令**：
  - CLI：`node dist/src/cli/anstsctl.js mgmt-consistency-phase2-audit`
  - HTTP API：`POST /v1beta1/management/system/deep-consistency-phase2-audit`
  - E2E 测试套件：`tests/m65_deep_implementation_consistency_phase2_audit.test.ts`（全仓 **58/58 测试套件，151/151 测试用例全部通过**）。
