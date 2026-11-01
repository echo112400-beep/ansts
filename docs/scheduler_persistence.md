# 调度状态的分布式持久化与故障恢复

## 目标与一致性边界

每个命名的 `AnstsSchedulerCell` 是一个资源分配的一致性域。所有提交、调度、DAG 和租约变更都经过该 Cell 的 SQLite；内存只是可重建的缓存。celld 负责 Cell 的唯一所有权、复制、租约接管和数据库恢复，应用负责恢复调度对象及业务状态。应用不再另建一个可能与 celld 所有权冲突的选主系统。

默认名称仍是 `global-5d-scheduler`。可通过 Worker 环境变量 `SCHEDULER_CLUSTER_ID` 选择稳定的集群名称，同一部署的 HTTP 和 Cron 路由使用同一名称。该名称是部署配置，不能由不可信请求任意选择。

多个集群可由不同 Cell 承载，但资源必须隔离：不得把同一节点的全部容量同时注册到多个集群。本次没有实现跨集群容量协调或全局 W-DRF。现有默认节点配置来自 `AnstsClusterRuntime`；实际分片前须为各部署配置不重叠的节点/容量。更改 Cell 名称相当于选择另一份数据库，不是已有任务的迁移。

## 持久化的数据

`src/cloudflare/scheduler_state.ts` 定义版本为 1 的 checkpoint，包含：

- 完整任务字段及已注册任务集合、租户、节点和容量。
- 租户配额、队列顺序、过期任务、DAG 父子关系、快速队列及 voucher。
- 当前 lease 的节点、模型池、epoch、turnSeq 和节点心跳时间。
- 幂等键映射、Bayesian profile、模型池断路器、背压状态。
- 尚未被消息队列接收并确认清除的 outbox 事件。

checkpoint 带单调 revision 和 SHA-256 校验；它不是防篡改签名。对象引用保存为 task ID，恢复时只构造一份任务对象，让队列、DAG 和调度器共享引用。FenceToken、ResourceVector5D、FingerprintStats 恢复为实际类实例。

`cell_leases` 和 `cell_dag_vouchers` 继续存在，作为事务内重建的诊断投影，不能用于恢复完整调度器。恢复不会重新调用 submitTask 或 scheduleTick，避免重复入队、重置预算或增加 epoch。

当前采用有界完整快照，每次提交复杂度随集群任务数增加；checkpoint UTF-8 大小超过 1 MiB 时拒绝提交并返回 503，保持旧状态。这是可审计的初始实现，并非无限规模的事件日志。长期运行需要归档终态任务和幂等记录；本次不自动清理，避免缩短幂等保证。大规模升级应迁移至按任务/队列/租约分表增量更新，并保持同一事务边界。

## 命令提交与失败

1. 读取并校验请求体，提前安排最迟 5 秒后的恢复 alarm；已有更早的 alarm 不会被持续流量推迟。
2. 在所有异步等待之后，从最新内存状态深复制并恢复出候选运行时。
3. 同步执行命令，将需要发布的事件加入候选 outbox。
4. 用 `storage.transactionSync()` 原子提交 checkpoint 与两个投影表。
5. 事务成功后替换内存缓存，等待 `storage.sync()`，再返回成功。

步骤 2–5 的事务与缓存替换部分没有外部 I/O 或 await。SQL 失败会回滚所有表，候选运行时不会发布。若 sync 失败，返回 503；SQL 可能已经提交，客户端应使用相同幂等键重试，而不是假设失败必然意味着未提交。

幂等判断位于 Scheduler Cell 中；Worker KV 不再作为任务是否存在的依据。相同租户和幂等键返回已有任务；已有 task ID 搭配不同键返回 409。幂等键目前保留至状态归档，恢复后仍有效。

## outbox 与消息语义

提交任务、派发任务、回收租约、完成任务时，事件与状态同一事务持久化。alarm 每次最多发送 100 个事件，发送成功后才提交删除。队列故障或 Cell 崩溃后，下一次 alarm 重试。

如果队列已接收事件，但 Cell 未提交删除就崩溃，同一个 `event_id` 会再次发送，所以语义是 **至少一次**。D1 审计消费者使用 payload 中的稳定 event_id，通过 `ON CONFLICT(event_id) DO NOTHING` 去重。事件发送期间的新提交不会被旧 outbox 的确认覆盖。

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

alarm 每 5 秒执行恢复检查：节点失联后标记不可调度，将其 RUNNING 任务的 epoch 提升、所有者设为 unassigned、清除执行资源占用并重新入队；若有回收任务，再运行一次调度尝试。新派发再次增加 epoch，允许有间隙，禁止回退。已完成、已回收的任务不会被重复回收。所有节点不可用时，普通队列和 DAG 快速队列继续保留，等待节点心跳和后续 tick。

`/scheduler/recover` 本身只回收，操作者随后可调用 tick。定时 alarm 同时进行回收和重新派发。正常排队任务仍由已有 Cron 或显式 tick 推进，重启不会隐式重派已有 RUNNING 任务。

完成请求必须匹配当前节点和 epoch，turnSeq 不得回退。完成状态被持久化后，节点失联不会让该任务再次入队。任务完成也会重新评估 DAG barrier。

## 执行可靠性的边界

本方案持久化并恢复的是调度状态，不包含 Harness 的工作区、LLM 上下文或工具进程内存。现有 `AgentTaskHarnessCell` 仍独立维护自己的 epoch，不能把它的 `/failover` 与调度租约当作同一个权威来源。

执行器接入时必须使用调度分配的 epoch；外部资源写入必须由目标存储/工具原子校验 fencing token 或使用幂等/Saga 协议。单独在执行前调用 `/leases/validate` 有检查与写入之间的时间窗口，不能阻止已经开始的旧执行者完成外部副作用。此接口只证明调度端当前 assignment，有效性不是外部副作用 exactly-once 的承诺。新执行节点恢复上下文仍需 Harness 的持久化/恢复协议，本次没有改写 Harness。

## 升级、灾备与运行要求

- 正常重启、迁移或 celld 所有者接管：使用同一命名空间和 Cell ID，构造函数加载 checkpoint 后再服务。
- 校验失败、未知版本、坏引用：构造失败，禁止静默创建空调度器。先修复/恢复存储，再接收流量。
- 旧版本只有 lease/voucher 表且无 checkpoint：拒绝启动。旧表缺少 TaskSpec 和队列，不能安全自动转换。升级前停止新提交、完成/导出旧运行任务并对账，然后在隔离的稳定新 Cell 上切流；不要删表假装恢复成功。没有可导出状态时需人工按原始 TaskSpec 对账重建。
- celld 的 fleet/bucket durability、S3 条件写支持及故障域由运行环境保证。fleet 模式允许以 peer 落盘确认，bucket 模式等到对象存储确认；不能把它们都解释为所有故障下都不会丢数据。Celld 版本升级需遵循相应版本的升级规则。
- 不把异步 D1 审计、KV 缓存或 R2 应用日志当作可立即接管的调度权威副本。灾备切换必须停止旧写入者并恢复同一权威存储，避免启动两个独立调度数据库分配同一资源。
- 监控 `/cluster/top` 的 `scheduler_revision`、`pending_outbox_events`、任务数、lease 数；同时监控 503、队列失败、心跳年龄和恢复次数。outbox 持续增长需要先处理消息系统故障或分片容量。

## 验证

`npm run build && npm test` 覆盖真实 SQLite 文件关闭/重开、并发重复提交、事务中途失败、事件发送与确认之间的崩溃、DAG 引用恢复、节点失联及 epoch 递增、损坏/旧格式拒绝恢复。Node.js 的 `node:sqlite` 测试需要支持 DatabaseSync 的 Node 版本（已在 Node 24 验证）。

`node scripts/verify_scheduler_recovery.mjs` 使用独立临时目录启动本机 celld，两次 SIGKILL 后验证任务、幂等记录和 lease。它不会修改实际部署数据。这项本机验证不等价于多节点 fleet、对象存储中断或网络分区测试；上线前应在部署使用的 bucket 和隔离故障域中演练所有权接管及 peer/bucket 丢失。

参考：[Cloudflare SQLite storage/transactionSync](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)、[celld ownership 与 durability](https://github.com/denoland/celld/blob/main/docs/README.md)、[celld guarantees](https://github.com/denoland/celld/blob/main/docs/guarantees.md)。
