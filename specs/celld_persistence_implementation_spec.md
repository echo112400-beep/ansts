# 基于 Celld 实现微服务可恢复持久化与预算安全

## Problem Statement

租户提交 AgentTask 并收到成功响应后，需要确信任务不会因进程重启、Cell 驱逐或执行节点故障而消失。任务恢复时，还必须知道哪些模型请求、工具操作、审批和费用已经发生，避免重复执行、重复扣费和配额超发。

现有实现只有部分租约、Turn 和 Saga 元数据进入 Cell SQLite。任务队列、预算、部分租约、工作流等待和通知重试仍依赖进程内状态；部分持久化证明也是内存模拟。多个调度实例还可能分别认为自己拥有同一组节点容量。因此，接单成功、预算硬上限、执行接管和完整恢复尚未形成统一的业务保证。

旧架构文档同时存在多种存储方案与未经实测的恢复时限。开发者缺少一份可以直接实施、以外部行为验收的统一规格。

## Solution

以 Celld 为主要生产运行时，按业务状态所有权划分持久化 Cell。接单、执行、资源、预算和外部副作用各有明确的权威入口；跨 Cell 通过稳定操作身份、预留、幂等消息和可恢复阶段推进。

生产环境使用 bucket 持久化确认，对象存储独立于 Celld 计算节点故障域。在对象存储完整可用的前提下，全部 Celld 计算节点永久丢盘后，关键已确认记录仍须恢复。对象存储自身灾难与误删除由独立备份和恢复演练覆盖。

租户可查询任务、操作、预算和补偿进度。系统对结果未知的外部操作显式阻塞，对未知费用保留预留，对尚未停止的执行保留资源占用。看板与搜索读取异步视图，业务命令始终由权威 Cell 校验。

本规格落地已确认的持久化设计，不承诺现有代码已经具备这些能力。生产容量、延迟和恢复时限由真实存储上的分档压测和故障演练签收。

## User Stories

1. As a tenant API caller, I want an accepted task to survive loss of every Celld compute disk, so that successful submission remains trustworthy.
2. As a tenant API caller, I want acceptance to preserve the complete TaskSpec, so that recovery retains my scheduling, execution and model policies.
3. As a tenant API caller, I want repeated submissions with the same idempotency key to identify one task, so that retries do not duplicate work.
4. As a tenant API caller, I want conflicting payloads under the same idempotency key to be rejected, so that an existing request cannot silently change meaning.
5. As a tenant API caller, I want a timed-out request to remain queryable under its original identity, so that I can resolve uncertainty without creating another task.
6. As a tenant administrator, I want idempotency records retained for at least 24 hours and longer for unresolved work, so that late retries remain safe.
7. As a tenant API caller, I want durable acceptance to include initial queue membership and pending events, so that my task can progress after a crash.
8. As a tenant API caller, I want backpressure to be explicit, so that I can distinguish a rejected submission from accepted work.
9. As a tenant administrator, I want task and operation queries scoped to my tenant, so that I can manage my workloads without accessing another tenant's state.
10. As a tenant API caller, I want acceptance and runnable eligibility reported separately, so that I understand when budget or resources still block execution.
11. As a tenant administrator, I want weighted fairness within each region and resource pool, so that competing workloads receive their configured shares.
12. As a platform operator, I want cross-pool allocations adjusted without a global scheduling bottleneck, so that the system can expand by resource pool.
13. As a platform operator, I want one authoritative capacity ledger per execution node, so that concurrent schedulers cannot allocate the same capacity twice.
14. As a tenant administrator, I want provisioning and reserved work included in usage accounting, so that pending starts cannot bypass fairness or capacity limits.
15. As a tenant administrator, I want recovered tasks and DAG children checked against the same capacity rules as ordinary tasks, so that fast paths cannot overcommit resources.
16. As a platform operator, I want dispatch to resume after failure at any stage, so that partial reservations do not permanently strand tasks.
17. As a platform operator, I want repeated delivery of one dispatch command to start at most one execution for that dispatch and node incarnation, so that message retries remain safe.
18. As a tenant administrator, I want preempted capacity reused only after the old execution stops or is isolated, so that replacement work receives real resources.
19. As a platform operator, I want phase changes to consume previously granted burst capacity, so that fast local switching does not require a remote durable write for every transition.
20. As a tenant administrator, I want hot-tenant backpressure to preserve one budget authority, so that overload cannot silently weaken accounting guarantees.
21. As a tenant administrator, I want one TaskCell to issue execution epochs, so that competing components cannot independently claim task ownership.
22. As a tenant administrator, I want stale executors denied new operation authorization and result submission, so that failover does not admit conflicting writes.
23. As a platform operator, I want a restarted node recognized by a new incarnation, so that old commands and grants cannot be reused accidentally.
24. As a tenant administrator, I want heartbeat loss to trigger investigation and fencing, so that an unreachable executor is not immediately assumed stopped.
25. As a tenant administrator, I want suspend and resume to retain execution policies and progress, so that hibernation changes resource usage without losing task meaning.
26. As a tenant administrator, I want task execution epochs separated from Celld ownership epochs, so that application authorization survives runtime placement changes correctly.
27. As a tenant administrator, I want conservative cost reserved before a model call, so that concurrent calls cannot collectively exceed a strict hard budget.
28. As a tenant administrator, I want actual provider usage reconciled against its reservation, so that unused budget becomes available without double settlement.
29. As a finance operator, I want each reservation and settlement tied to a currency, billing period and price version, so that charges remain explainable after policy changes.
30. As a finance operator, I want monetary values represented in integer units, so that repeated aggregation does not introduce floating-point drift.
31. As a tenant administrator, I want unknown provider consumption to keep its budget frozen, so that uncertain charges cannot be spent again.
32. As a tenant administrator, I want fallback calls to obtain separate reservations, so that a timed-out primary request cannot hide overlapping costs.
33. As a tenant administrator, I want strict hard-budget calls rejected when no credible cost bound exists, so that the advertised cap has enforceable meaning.
34. As a platform operator, I want shared provider quotas granted from one authority per quota domain, so that routers cannot each spend the full shared allowance.
35. As a platform operator, I want uncertain grants preserved across router restarts, so that failure cannot refill already allocated provider capacity.
36. As a finance operator, I want unresolved holds visible for reconciliation, so that operators can resolve frozen budget using recorded evidence.
37. As a tenant administrator, I want external operations assigned stable identities before execution, so that retries and failover refer to the same logical action.
38. As a tenant administrator, I want supported provider idempotency and lookup mechanisms used during recovery, so that completed effects are not repeated.
39. As a tenant administrator, I want unresolvable external outcomes marked OUTCOME_UNKNOWN, so that the system does not blindly repeat irreversible actions.
40. As an approver, I want approval bound to the operation and argument digest, so that approval cannot authorize changed parameters.
41. As an approver, I want changed arguments to require a new approval, so that authorization reflects the action actually executed.
42. As a tenant administrator, I want Saga compensation attempts and failures retained, so that incomplete rollback remains visible and recoverable.
43. As a tenant administrator, I want partial-Turn tool results preserved independently of final Turn submission, so that recovery cannot forget an external effect.
44. As a tenant administrator, I want computational backtracking to retain costs and operation history, so that rollback does not erase real-world consequences.
45. As a tenant administrator, I want irreversible Race branch submissions gated until a winner is selected, so that losing branches cannot commit conflicting effects.
46. As a tenant administrator, I want DAG progress exposed through recoverable intermediate states, so that I can distinguish waiting, reservation and activation.
47. As a tenant administrator, I want parent execution safely yielded before child allowances become spendable, so that parent and children cannot consume the same allowance.
48. As a tenant administrator, I want child voucher redemption and allowance return to be idempotent, so that repeated messages cannot create quota.
49. As a tenant administrator, I want each DAG join and Race winner decided once, so that duplicate completion events cannot resume the parent repeatedly.
50. As a tenant administrator, I want an empty child set to complete immediately, so that a parent does not wait forever for nonexistent work.
51. As a tenant administrator, I want straggler boosts backed by real transferable allowance, so that repeated evaluation cannot allocate the same released capacity twice.
52. As a tenant administrator, I want losing children stopped and reconciled before their allowances return, so that reclaimed quota is genuinely available.
53. As a tenant administrator, I want a completed Turn tied to a complete recoverable workspace, so that confirmed progress is useful after a node failure.
54. As a tenant administrator, I want recovery to use the latest fully committed Turn and its snapshot, so that incomplete local state cannot replace durable progress.
55. As a platform operator, I want interrupted snapshot uploads resumed by application-managed chunks, so that node loss does not require trusting a lost multipart session.
56. As a tenant administrator, I want snapshot content checked against independent digests, so that corrupted or missing data cannot silently become a successful restore.
57. As a tenant administrator, I want missing committed snapshot blocks to block recovery with a visible error, so that the system cannot fabricate a recovered workspace.
58. As a platform operator, I want unreferenced upload fragments reclaimed safely, so that failed uploads do not grow storage indefinitely.
59. As a tenant administrator, I want live recovery and rollback references protected from garbage collection, so that storage cleanup cannot break retained tasks.
60. As a platform operator, I want Cell reactivation to reconstruct state from durable records, so that eviction does not depend on preserved JavaScript memory.
61. As an integration consumer, I want stable event identities and aggregate versions, so that duplicate or out-of-order delivery does not corrupt my view.
62. As a platform operator, I want delivery progress preserved beyond Queue retention and alarm retry limits, so that transport expiration cannot erase unfinished business.
63. As an integration operator, I want exhausted deliveries visible and replayable under their original identities, so that manual recovery does not create duplicate operations.
64. As a platform operator, I want periodic reconciliation to rediscover unfinished work, so that missed wakeups do not strand accepted tasks.
65. As a tenant administrator, I want task lists and dashboards rebuilt from retained authoritative state, so that losing a query view does not lose my tasks.
66. As a tenant API caller, I want cancellation, approval and budget commands checked against authoritative state, so that a stale dashboard cannot authorize an invalid transition.
67. As a streaming client, I want committed business events replayed from a durable cursor within seven days, so that reconnecting does not silently skip retained events.
68. As a streaming client, I want an expired cursor to require explicit resynchronization, so that I can recover a complete current task view.
69. As a streaming client, I want committed Turn results recoverable even when provisional token deltas are lost, so that connection loss does not lose confirmed output.
70. As a tenant administrator, I want completed task content and snapshots retained for 30 days by default, so that recent work remains inspectable and recoverable.
71. As a finance operator, I want task summaries, settlements and operation audits retained for 365 days by default, so that historical outcomes remain explainable.
72. As a tenant administrator, I want configurable longer retention and unresolved-record protection, so that active tasks, unknown outcomes and pending compensation are not automatically forgotten.
73. As a platform operator, I want the object store validated for Celld's required semantics and failure domain, so that the configured durability mode has a sound storage foundation.
74. As a platform operator, I want independent backups restored into an isolated fleet, so that backup availability is demonstrated by recovery rather than object presence alone.
75. As a platform operator, I want migration to preserve verifiable historical state and list unresolved records explicitly, so that a maintenance window does not invent successful outcomes.
76. As a platform operator, I want stable Cell namespaces and versioned business schemas, so that deployment changes do not accidentally open empty identities.
77. As a platform operator, I want adjacent deployment versions to accept compatible commands and events, so that rollout does not corrupt in-flight work.
78. As a platform owner, I want correctness and disaster recovery verified before capacity claims are published, so that production commitments match evidence.
79. As a platform owner, I want throughput and recovery measured with tenant skew, task state and snapshot size recorded, so that results describe the workload they actually support.
80. As a developer, I want one primary public API test boundary reused across scenarios, so that implementation refactoring does not invalidate behavior-focused acceptance tests.

## Implementation Decisions

1. **运行时与耐久性边界**：以 Celld 为主要生产运行时，不保留必须同时部署到原生 Cloudflare 的硬约束。生产 fleet 使用 bucket 确认模式；计算集群全部丢盘的恢复承诺以对象存储完整可用为前提。误删除及存储自身灾难使用独立备份覆盖。
2. **单一状态所有者**：同一业务字段只存在一个权威写入者。服务和工作进程可以缓存、投影或执行命令，不能独立宣布修改已经成为业务事实。
3. **TenantLedgerCell**：按租户管理接单记录、完整初始 TaskSpec、初始队列、幂等结果、政策、计费周期、预算预留、结算与额度转移。接单四件套必须在同一 SQLite 事务中提交。
4. **TaskCell**：按任务管理执行状态、租约、业务 epoch、Turn、外部操作、审批、补偿、等待、快照引用与执行版本。接单状态与执行状态分别归属租户账本和任务 Cell，避免两个主写者修改同一状态。
5. **SchedulerPoolCell**：按地域和资源池管理竞争候选、租户份额及派发意图。池内实施公平调度，池间通过额度分配和周期调整接受短期偏差。已预留、启动中和运行中的占用都参与计算。
6. **NodeCapacityCell**：按物理执行节点管理 incarnation、容量、资源预留、分配、burst 授权与回收。普通、恢复和 DAG 派发共享排他容量入口；节点代理负责落实并核对真实执行状态。
7. **DagCell**：按 DAG 管理拓扑、子任务观察、屏障、唯一 winner 和转移进度。租户账本是额度数值的权威；DagCell 不独立产生或归还预算。
8. **ProviderQuotaCell 与 RoutingConfigCell**：共享供应商 RPM/TPS 由对应配额域统一授予；路由、资源池成员、政策和价格使用持久版本。熔断统计和健康观察可以重建，不能替代权威授权。
9. **共用持久协议**：各 Cell 使用业务表、操作记录、inbox、outbox 与 schema_version。本地事务同时修改业务状态、操作结果和待发送事件；消息包含稳定 eventId、operationId、聚合版本、租户身份和内容摘要。
10. **本地事务规则**：本地 SQL 原子修改不跨网络等待。等待外部 I/O 后重新校验状态版本、epoch 与预留状态；不能将 actor 的同步执行语义当成跨 await 的锁。写游标须消费完整，输出遵守 Celld 的持久化确认边界。
11. **命令与版本约束**：重复 operationId 必须匹配同一操作类型、资源和参数摘要；冲突时拒绝。聚合修改使用 expectedVersion 条件。执行 epoch 只能由 TaskCell 签发，调用者不得自行提高 epoch 获得权限。
12. **接单接口契约**：202 表示完整规格、初始队列、幂等结果及 outbox 已获得 bucket 持久化确认。预算和资源不足仍可阻塞后续运行资格。TaskCell 可以在接单后幂等初始化，其失败由持久记录驱动重试。
13. **接单幂等契约**：去重窗口至少为 24 小时，任务未终结或结果未知时延长。同键不同内容返回冲突。窗口结束并清理记录后，重用键可以创建新任务，客户端契约需明确这一点。
14. **超时语义**：缺失确认不证明提交未发生。客户端沿用原 operationId 查询或重试；查询不可用或恢复未完成时继续表示 unknown，不生成新身份重做同一动作。
15. **派发协议**：持久推进 PREPARING、RESERVED、AUTHORIZED、STARTED 阶段。资源预留绑定 dispatchId 与节点 incarnation；TaskCell 校验状态和预留后签发执行授权，启动通过持久命令完成。同一派发重投不得重复启动；授权结果未知时先核对，再决定释放。
16. **停止与故障接管**：心跳超时触发怀疑和核对。TaskCell、节点代理、工具入口与结果入口共同实施业务执行栅栏。旧沙箱未停止或隔离前不复用其容量；节点重启后以新 incarnation 对账旧授权。已发出的外部请求不因换 epoch 自动撤销。
17. **快速本地路径**：节点在预先授予的 burst 容量内切换阶段。Router 在预先授予的速率额度内处理请求。重启、超时和分区后的未知授予不能被当作未使用额度重复发放。
18. **预算与结算**：调用前冻结可信的保守费用上界，记录币种、计费周期和价格版本；调用后凭可核验 usage 或凭据幂等结算，并释放未用余额。金额使用整数最小单位，字符数估算不能充当最终计费凭据。
19. **预算未知与 fallback**：未知消费继续冻结；备用模型的新请求单独预留。没有可信费用上界的调用拒绝进入严格硬预算模式。恢复和回滚不能撤销已发生费用。
20. **工具操作协议**：先持久登记稳定逻辑 operationId、参数摘要和意图，再发送外部操作。attemptId 和 fence 与逻辑身份分离；接管不能通过新 epoch 改变同一外部操作的身份。
21. **工具恢复与审批**：支持幂等或查询的操作使用供应商协议核对；两者都缺失时进入 OUTCOME_UNKNOWN。审批绑定具体逻辑操作、参数摘要和授权范围；参数变化需要重新审批。补偿记录进度和失败，不承诺外部效果可以原子回滚。
22. **持久等待**：TaskCell 保存 deadline、待办和事件 inbox，alarm 唤醒到期工作，周期 reconciler 修复漏唤醒。原生 Workflows 可辅助编排，但不另建可独立修改的任务业务真相；不继续把内存工作流模拟器当作生产持久引擎。
23. **Turn 与快照提交**：先登记上传意图，上传租户隔离的不可变分块，并校验独立摘要；再在 TaskCell 的单个事务提交完整 Turn、manifest 引用、状态版本及结算和事件 outbox。确认 Turn 前全部引用必须可恢复。
24. **部分执行恢复**：自动恢复以最近完整 Turn 和快照为计算基线，同时恢复该基线之后已持久记录的工具结果、费用和补偿进度。计算回滚不等同于遗忘外部效果。
25. **对象协议与 GC**：大对象采用应用层分块与可恢复上传进度；不依赖可跨节点存活的 multipart 会话，不把 ETag 充当 SHA-256。引用先撤销并记 tombstone，再经宽限期清理无引用块；未知引用保守保留。缺块或校验失败显式阻塞恢复并告警。
26. **DAG 守恒协议**：父任务安全挂起后，租户账本才允许子额度生效。voucherId 和兑换、归还、boost 的操作身份唯一。重复、乱序和接管不得超发；空子集立即完成。winner 和 join 只决定一次，父任务恢复核对屏障及未结算额度。
27. **Race 提交门禁**：分支默认只读或写入隔离环境；不可逆外部提交在选出 winner 后授权。loser 停止并结算后才回收额度，避免把取消请求当作已停止证明。
28. **事件与长期重试**：采用至少一次消息交付，通过消费者 inbox 保证重复业务修改不生效。处理聚合版本缺口，避免旧事件覆盖新状态。outbox 保存尝试、下次重试及最后错误；耗尽后转可查询、可告警、可人工重放的阻塞状态。
29. **唤醒和发现**：Queue 与 alarm 仅为推进机制。持久任务和租户注册信息驱动周期对账，消息过期或内建重试耗尽后仍可发现未完成操作；不能仅依赖进程内 Cell 列表。
30. **查询与 SSE**：D1 按负载分库，保存可重建投影、消费身份、来源版本和持久 feed 游标。SSE 只保证指定 feed 的游标顺序，不宣称跨任务全局发生顺序；临时 token delta 可丢失，完整提交结果可补齐。取消、审批和预算检查回权威 Cell 校验。
31. **保留期**：业务事件默认 7 天；终态完整内容和快照默认 30 天；摘要、结算与操作审计默认 365 天。租户可配置更长期限。活跃任务、unknown、预算 hold、未完成 outbox 和待补偿记录解决前不自动清除。过期 SSE 游标要求显式重新同步。
32. **服务映射**：Gateway、Dashboard 与 CLI 使用权威接口；Profiler 从已提交样本更新可重建统计；队列、调度、租约、Router、Saga、NodeDaemon、Harness、Checkpoint 与通知组件分别服从对应 Cell 的状态所有权。模型预测和软预留不能替代硬预算授权。
33. **升级与迁移**：固定脚本及 Cell class 身份，业务 SQL 使用显式 schema 版本；短迁移使用本地事务，长回填分批可恢复，相邻版本兼容命令和事件。允许维护窗口停写迁移，导出并核对可验证历史，未知记录建立人工处理清单，不补造已经丢失的内存历史。
34. **备份与发布**：首版采用停写、可验证切点的独立备份，并恢复到隔离 fleet 演练。热租户先背压，不自动拆分预算账本；账本再分片需另行验证守恒。发布前先完成真实故障验收，再签收规模、延迟、RTO 与存储成本。

## Testing Decisions

1. **已确认的主要测试边界**：复用公开 Worker HTTP API，通过接单、任务及操作查询、审批、Turn 提交和 SSE 检查外部行为。需要补充的查询能力仍属于同一业务 API 边界，不增加读取内部 Map、表或仓储的专用测试接口。
2. **好测试的标准**：断言调用者可见结果、身份稳定性、资源及金额守恒、恢复后的工作区和操作状态。测试应允许内部模块、表结构与 Cell 调用方式重构；不以内部方法调用次数、Map 大小或 SQL 文本证明正确性。
3. **现有先例**：现有 Celld Worker 集成套件已经用 Request/Response 覆盖接单、去重、Turn、重放和事件消费，可复用请求与断言组织方式。它使用内存 KV、R2 和模拟 Durable Object State，不能作为真实持久化的验收证据。
4. **场景先例**：现有整体集成、混沌场景和 Celld HA 模拟套件覆盖 DAG、栅栏、故障接管、工作流、队列及 GC 的概念场景；复用这些行为场景，重新以真实进程和存储验证相关保证，不继承模拟 proof 作为通过标准。
5. **实际受测模块**：通过同一外部入口覆盖 Gateway、七类权威 Cell、Scheduler、Lease、Router、预算结算、Saga、NodeDaemon、Harness、Checkpoint、DAG、通知、查询视图和恢复协调器。只有必要的算法边界测试保留在更低层。
6. **真实持久化验收环境**：启动真实 Celld 进程与隔离对象存储，配置 bucket 确认；测试控制器负责终止进程、丢弃测试计算节点磁盘、阻断网络和重建节点。对象存储在计算集群丢盘场景中必须保留，存储自身损失另用备份场景验证。
7. **外部效果可观察性**：使用独立的可控供应商、工具和 Webhook 服务，记录其实际收到的逻辑操作身份、效果和计量。可配置响应丢失、已生效后断线、重复返回和延迟，以外部记录与业务查询共同验证结果。
8. **故障阶段覆盖**：在接单提交、资源预留、执行授权、沙箱启动、外部效果、Turn 提交、快照上传、消息发送和确认边界分别中断。恢复测试使用原客户端身份重试，验证可见结果，而非依赖固定睡眠时间猜测内部进度。
9. **时间相关测试**：在隔离环境使用受控时间或缩短的测试配置验证超时、重试、幂等窗口、保留期和 GC。配置必须覆盖同一生产状态转换，不新增绕过授权和持久化的业务 API。
10. **性能与正确性分开验收**：正确性场景通过后，再按活跃/休眠规模、租户倾斜、资源池数、快照大小和对象存储负载压测。分别报告接单、派发、Turn 提交与故障恢复，不沿用未经验证的旧 RTO。

必须包含以下外部验收场景：

| 场景 | 可观察的通过条件 |
|---|---|
| 202 后丢弃全部计算节点磁盘 | 重建 fleet 后仍查到原任务、完整规格及原幂等结果 |
| 同租户同键并发提交 | 全部成功响应指向同一任务；不同内容收到冲突 |
| 提交成功但响应丢失 | 原 operationId 可找回原结果；不因重试产生第二次业务效果 |
| 派发任一阶段失败或重投 | 同一派发不重复启动；节点分配和租户份额不重复增加 |
| 恢复任务或 DAG fast path 竞争容量 | 仍遵守相同容量与授权规则，不能绕过资源预留 |
| 旧节点失联后继续提交 | 旧 epoch 的新授权与结果被拒绝；未知外部效果仍可核对 |
| 并发消费、fallback 和未知计费 | 已结算与未决预留符合额度守恒，不借未知余额继续消费 |
| 外部效果成功后响应丢失 | 查询或稳定幂等重试找回结果；无能力核对时进入 OUTCOME_UNKNOWN |
| 审批后修改参数 | 原批准不能授权新内容，必须重新审批 |
| 部分 Turn 已发生工具效果 | 重启后仍显示该效果，恢复计算不重复执行或遗忘费用 |
| DAG 完成消息重复或乱序 | winner、join 和父恢复只生效一次；额度无超发 |
| Race loser 尚未停止 | 不回收其未知占用，不授予其不可逆提交 |
| 上传中断或发现损坏分块 | 已确认 Turn 的快照完整；不完整内容不能被宣布已恢复 |
| Queue 过期或 alarm 重试耗尽 | 持久待办仍可见，reconciler 或人工重放继续原操作 |
| D1 投影丢失 | 权威查询与命令保持正确，视图可从保留数据重建 |
| SSE 断线与游标过期 | 窗口内补齐业务事件；窗口外明确要求重新同步 |
| GC 与未决引用同时存在 | 活跃、unknown、补偿和有效快照引用未被自动删除 |
| 独立备份恢复到隔离 fleet | 可核对任务、预算、操作记录与全部有效快照引用 |
| 维护窗口迁移 | 可验证历史完整导入，无法确定的在途状态列入显式处理清单 |

## Out of Scope

- 在此规格编写阶段直接实施业务代码、迁移生产数据或部署服务。
- 原生 Cloudflare 与 Celld 双平台部署兼容的硬性要求。
- 所有地域、资源池之间的严格全局公平或全局事务。
- 自动拆分单个热租户预算账本；需要另行验证额度转移与账本迁移。
- 对任意外部服务承诺端到端 exactly-once；缺少幂等和查询能力时使用显式 unknown。
- 恢复任意时刻的进程内存或保证逐 token 临时输出无损续接。
- 未经验证的全 fleet 在线一致性备份；首版使用维护窗口备份。
- 在所有快照、租户倾斜和存储配置下直接承诺十万沙箱、五万 Turn/s 或固定秒级 RTO。
- 承诺对象存储自身损毁、业务误删除后仍能恢复备份切点之后的全部写入。
- 将已有内存模拟器升级为另一套底层复制、ownership、LTX 回收或通用工作流引擎。
- 重建已经丢失且没有可验证来源的历史内存状态。

## Further Notes

- 运行时键约束：接单前校验租户与任务的逻辑 Cell 名称为非空字符串、不含控制字符，且不超过 1024 UTF-8 字节。Celld 0.6.3 对更长名称不保留可恢复的 `id.name`；超长输入须显式拒绝，不能在收到 202 后才发现无法路由到原逻辑身份。

- 设计来源为已确认的《Celld 微服务持久化设计规范》。其中七类权威 Cell、跨 Cell 协议、保留期、迁移和验收已完成共同确认；本规格将这些决策整理为可分派的实施要求。
- 测试边界已由用户确认：公开 Worker HTTP API 为主要入口，真实 Celld 与隔离对象存储承担灾备验收。
- 建议实施顺序为：持久协议基础；接单与任务恢复；资源及执行授权；预算与供应商配额；外部操作及快照；DAG、查询和通知；迁移、备份与容量验收。
- 官方依据包括 [Durable Objects](https://celld.dev/docs/services/durable-objects/)、[确认与恢复保证](https://celld.dev/docs/guarantees/)、[Workflows](https://celld.dev/docs/services/workflows/)、[Queues](https://celld.dev/docs/services/queues/)、[R2](https://celld.dev/docs/services/r2/) 和 [D1](https://celld.dev/docs/services/d1/)。架构核验日期为 2026-10-31；运行时升级时重新核对具体版本契约。
- 本规格发布到 `echo112400-beep/ansts` 的 GitHub Issues，并应用 `ready-for-agent`。这是已确认的实施规格，无需额外 triage 访谈。
- 跟踪 issue：[echo112400-beep/ansts#1](https://github.com/echo112400-beep/ansts/issues/1)。本地规格是实施参考；需要同步编辑时，同时更新已发布 issue。
- issue tracker、triage 标签和单一上下文领域文档的 agent 配置记录在 `docs/agents/`。后续工程技能按对应说明读取。
