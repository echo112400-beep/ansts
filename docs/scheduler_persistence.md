# 调度状态的分布式持久化与故障恢复

## 目标与一致性边界

每个命名的 `AnstsSchedulerCell` 是一个资源分配的一致性域。所有提交、调度、DAG 和租约变更都经过该 Cell 的 SQLite；内存只是可重建的缓存。celld 负责 Cell 的唯一所有权、复制、租约接管和数据库恢复，应用负责恢复调度对象及业务状态。应用不再另建一个可能与 celld 所有权冲突的选主系统。

默认名称仍是 `global-5d-scheduler`。可通过 Worker 环境变量 `SCHEDULER_CLUSTER_ID` 选择稳定的集群名称，同一部署的 HTTP 和 Cron 路由使用同一名称。该名称是部署配置，不能由不可信请求任意选择。

多个集群可由不同 Cell 承载，但资源必须隔离：不得把同一节点的全部容量同时注册到多个集群。本次没有实现跨集群容量协调或全局 W-DRF。现有默认节点配置来自 `AnstsClusterRuntime`；实际分片前须为各部署配置不重叠的节点/容量。更改 Cell 名称相当于选择另一份数据库，不是已有任务的迁移。

## 持久化的数据

调度状态保存在 Cell 自己的 SQLite 里，分表增量维护（`src/cloudflare/scheduler_cell.ts`）。**没有单独的整机快照文件**；`src/cloudflare/scheduler_state.ts` 里的整机 JSON checkpoint 方案已在 v2.5 被本方案取代（bounded 全量快照每次提交的复杂度随任务数增长，且与事务性 outbox 互相冲突），文件已删除。

| 表 | 内容 | 恢复用途 |
| --- | --- | --- |
| `scheduler_checkpoint` | 单行 `{schema: 2, revision, tasks[], leases[]}` + FNV-1a 校验 | 版本判定、单调 `revision`、引用一致性 |
| `cell_tasks` | 任务投影（`state`/`epoch`/`turn_seq`/`enqueue_ts`/`deadline_ts`）+ **完整 TaskSpec JSON** | 重建 `AgentTask` |
| `cell_leases` | `node_id`/`model_pool`/`epoch`/`turn_seq`/`renewed_ts` | 重建 CAS 租约与续约水位 |
| `cell_dags`、`cell_dag_vouchers` | DAG 父子关系、race 模式、配额 voucher | 重建 DAG 与快速队列 |
| `cell_idempotency` | `${tenant_id}:${idempotency_key}` → `task_id` | 跨重启幂等 |
| `cell_outbox` | 待发布事件（`seq`/`event_id`/payload） | 至少一次投递 |
| `cell_profiler_snapshots` | Bayesian 共轭先验 | 冷启动 token 预留 |

任务恢复先 `Object.assign(new AgentTask({taskId, tenantId}), <persisted>)`，再用投影列覆盖 `state`/`fence`/时间戳。因此恢复后的 5D 需求向量（`reservedTps`/`cogCpu`/`execCpu`）、`softReservedTokens`、`localityRegion`、`isolationDriver`、`warmPoolId`、`effort` 是提交时接受的值，而不是构造函数默认值——否则重启后的 W-DRF 份额与配额准入会按默认 1c/4G 计算。

对象引用只存 task ID，恢复时每个任务只构造一份实例，`scheduler.tasks`、`queueMgr.tenantQueues`、`queueMgr.activeDags[].children`、`queueMgr.escrowFastQueue` 共享同一引用；`FenceToken`、`FingerprintStats` 恢复为实际类实例。恢复不会重新调用 `submitTask` 或 `scheduleTick`，避免重复入队、重置预算或增加 epoch。

增量写入：每次变更只 `DELETE WHERE task_id = ?` + 重插受影响的任务行、租约行，并重写单行 checkpoint。成本随变更量增长，不随集群任务数增长。终态任务与幂等记录本次不自动清理（避免缩短幂等保证），长期运行需归档。

## 命令提交与失败

1. 读取并校验请求体。提交成功后设置 alarm（已有更早的 alarm 不会被持续流量推迟）。
2. 同步执行命令，并把需要发布的事件写入事务内的 outbox 候选行。
3. 用 `storage.transactionSync()`（宿主提供时）在一个事务里提交任务投影、租约投影、幂等键、outbox 行与单行 checkpoint。`tick`/`recover`/`complete`/`dag/spawn-escrow` 走同一事务边界。
4. 事务成功后等 `storage.sync()`，再返回成功。

事务内没有外部 I/O 或 await。SQL 失败回滚所有表；宿主不提供 `transactionSync` 时（内存模拟器），cell 先用快照记下任务状态、队列长度、租约与续约水位，失败时用补偿回滚内存缓存，因此调用方拿到 503 时集群状态与失败前一致。若 sync 失败，返回 503；SQL 可能已经提交，客户端应使用相同幂等键重试，而不是假设失败必然意味着未提交。

幂等判断位于 Scheduler Cell 中，持久化在 `cell_idempotency`；Worker KV 不作为任务是否存在的依据。相同租户和幂等键返回已有任务（200 + `deduplicated: true`，含重启后）；已有 task ID 搭配不同键返回 409。幂等键目前保留至状态归档。

## outbox 与消息语义

提交任务时，事件与状态同一事务持久化：任务被接纳后，`agent.task.queued` 写入 `cell_outbox`，只有队列 `send()` 返回后才在下一个事务里删除该行并重写 checkpoint。alarm 逐条发送待发布事件，队列故障或 Cell 崩溃时下一次 alarm 重试；行不会被一次性批量确认，因此新提交不会被旧 outbox 的确认覆盖。

如果队列已接收事件，但 Cell 未提交删除就崩溃，同一个 `event_id` 会再次发送，所以语义是 **至少一次**。`event_id` 在提交事务里就固定为 `evt_<task_id>_queued`，不是发送时生成。D1 审计消费者使用 payload 中的稳定 event_id，通过 `ON CONFLICT(event_id) DO NOTHING` 去重。
调度接口的 HTTP 响应和 dispatch 事件可能同时被调用方看到。执行消费者必须以 task ID + epoch 去重，并在执行前确认当前租约；旧 dispatch 事件可能晚于 recovery 事件到达。当前队列消费者只写审计记录，没有新增自动启动执行器的消费者。

## 心跳、恢复与完成协议

以下接口均通过 Worker 的 `/v1beta1` 前缀暴露，属于受信任的控制面，应沿用部署的访问控制：

| POST 接口 | 请求字段 | 行为 |
| --- | --- | --- |
| `/nodes/heartbeat` | `node_id` | 刷新已注册节点心跳，恢复节点可用状态 |
| `/scheduler/tick` | `{}` 或空 body | 调度排队任务，持久化 lease 与 dispatch 事件 |
| `/scheduler/recover` | `{}` 或空 body | 回收失联节点上的 RUNNING 任务；可重复调用 |
| `/leases/validate` | `task_id, node_id, epoch, turn_seq` | 查询 assignment 是否仍有效 |
| `/tasks/complete` | `task_id, node_id, epoch, turn_seq` | 校验当前 lease 后持久化 SUCCEEDED，重复确认返回原结果 |

节点应每 3 秒发送心跳，超时阈值沿用 LeaseCoordinator 的 12 秒。首次派发时初始化节点的心跳起点；之后派发更多任务不会延长已有心跳。节点未接入心跳协议时，不应把它当作可靠运行的执行节点。

租约活性用**续约水位**（`cell_leases.renewed_ts`）判定，而不是只靠进程内心跳表：`/nodes/heartbeat` 会顺带续约该节点持有的所有租约，`/scheduler/tick` 派发时写入当前水位。因此节点失联后即使整个进程重启、内存心跳表清空，`/scheduler/recover` 仍能识别过期租约并把该节点标记为不可调度。

Cron（Worker `scheduled()`）每次先调 `/scheduler/recover` 再调 `/scheduler/tick`，两个接口都幂等，可与手动调用重叠。alarm 只负责 outbox 投递，不做回收——回收与重派统一由 Cron 或显式接口驱动，避免同一 Cell 里出现两个隐式调度源。

回收动作：节点失联后标记不可调度，将其 RUNNING 任务的 epoch 提升、所有者设为 unassigned、清除执行资源占用并重新入队。新派发再次增加 epoch，允许有间隙，禁止回退。已完成、已回收的任务不会被重复回收（第二次调用返回空列表）。所有节点不可用时，普通队列和 DAG 快速队列继续保留，等待节点心跳和后续 tick。

`/scheduler/recover` 本身只回收，操作者随后可调用 tick。定时 alarm 同时进行回收和重新派发。正常排队任务仍由已有 Cron 或显式 tick 推进，重启不会隐式重派已有 RUNNING 任务。

完成请求必须匹配当前节点和 epoch，turnSeq 不得回退。完成状态被持久化后，节点失联不会让该任务再次入队。任务完成也会重新评估 DAG barrier。

## 执行可靠性的边界

本方案持久化并恢复的是调度状态，不包含 Harness 的工作区、LLM 上下文或工具进程内存。现有 `AgentTaskHarnessCell` 仍独立维护自己的 epoch，不能把它的 `/failover` 与调度租约当作同一个权威来源。

执行器接入时必须使用调度分配的 epoch；外部资源写入必须由目标存储/工具原子校验 fencing token 或使用幂等/Saga 协议。单独在执行前调用 `/leases/validate` 有检查与写入之间的时间窗口，不能阻止已经开始的旧执行者完成外部副作用。此接口只证明调度端当前 assignment，有效性不是外部副作用 exactly-once 的承诺。新执行节点恢复上下文仍需 Harness 的持久化/恢复协议，本次没有改写 Harness。

## 升级、灾备与运行要求

- 正常重启、迁移或 celld 所有者接管：使用同一命名空间和 Cell ID，构造函数重建内存调度对象后再服务。
- 校验失败（checkpoint 校验和不符、`schema < 2`）或 legacy 表（`cell_leases` 行缺少 `node_id`/`epoch`/`turn_seq`）：构造失败并抛错，禁止静默创建空调度器。先修复/恢复存储，再接收流量。
- 旧版本只有 lease/voucher 表：旧表缺少 TaskSpec 和队列，不能安全自动转换。升级前停止新提交、完成/导出旧运行任务并对账，然后在隔离的稳定新 Cell 上切流；不要删表假装恢复成功。没有可导出状态时需人工按原始 TaskSpec 对账重建。
- celld 的 fleet/bucket durability、S3 条件写支持及故障域由运行环境保证。fleet 模式允许以 peer 落盘确认，bucket 模式等到对象存储确认；不能把它们都解释为所有故障下都不会丢数据。Celld 版本升级需遵循相应版本的升级规则。
- 不把异步 D1 审计、KV 缓存或 R2 应用日志当作可立即接管的调度权威副本。灾备切换必须停止旧写入者并恢复同一权威存储，避免启动两个独立调度数据库分配同一资源。
- 监控 `/cluster/top` 的 `scheduler_revision`、`pending_outbox_events`、任务数、lease 数；同时监控 503、队列失败、心跳年龄和恢复次数。outbox 持续增长需要先处理消息系统故障或分片容量。

## 验证

`npm run build && npm test` 覆盖真实 SQLite 文件关闭/重开、并发重复提交、事务中途失败、事件发送与确认之间的崩溃、DAG 引用恢复、节点失联及 epoch 递增、损坏/旧格式拒绝恢复。这些测试加载 Node.js 内置 `node:sqlite` 的 `DatabaseSync`。该模块从 Node.js 22.13.0 起默认可用，不再需要 `--experimental-sqlite`；Node.js 18 与 20 没有这个内置模块。CI 与 `Dockerfile.dev` 使用 Node.js 22。

`node scripts/verify_scheduler_recovery.mjs` 使用独立临时目录启动本机 celld，两次 SIGKILL 后验证任务、幂等记录和 lease。它不会修改实际部署数据。这项本机验证不等价于多节点 fleet、对象存储中断或网络分区测试；上线前应在部署使用的 bucket 和隔离故障域中演练所有权接管及 peer/bucket 丢失。

参考：[Cloudflare SQLite storage/transactionSync](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)、[celld ownership 与 durability](https://github.com/denoland/celld/blob/main/docs/README.md)、[celld guarantees](https://github.com/denoland/celld/blob/main/docs/guarantees.md)。
