# Body 执行核对与停止语义

当前接口为开发环境管理协议。管理员凭据由 `scripts/prepare_body_dev_credentials.py` 生成，保存在忽略的 `.dev-secrets/body-operator.token`；Body 容器只挂载各自的宿主机凭据。当前管理员身份为 `development-operator`，不是生产多人 RBAC/审批系统。

`POST /v1/body/reconcile?taskId=<id>` 使用 `Authorization: Operator <token>`。宿主机凭据不可调用此接口。请求必须包含 `tenant_id`、`task_id`、`expected_version`、`action`、`evidence`、`reason`；修改执行结论还必须包含原 `node_id` 与 `fencing_token`。

| action | 附加字段 | 行为 |
| --- | --- | --- |
| `retain_block` | 无 | 保持 UNCERTAIN，增加版本并记录审计 |
| `confirm_completed` | `result` | 证据确认完成，保留不可变结果；协调 Scheduler 完成任务 |
| `confirm_not_started` | `replacement` | 证据确认此前从未执行，创建新的尝试；协调 Scheduler 重新排队。replacement 为完整执行身份，使用新 attemptId 和更高 fence，输入摘要和租户/任务保持相同 |

禁止把“没有收到结果”当成“从未执行”。若不能证明副作用是否发生，应继续阻塞。旧记录缺少租户/输入证据时，不可直接通过此接口重建执行身份。

状态与哈希链审计保存在同一执行记录中。版本冲突返回 409；同一个已处理请求可安全重试；改变参数后重用旧版本会被拒绝。跨 Cell 更新未完成返回 `SCHEDULER_RECONCILIATION_PENDING`（503），此时重试相同请求，不得删除执行记录。

原生执行器使用独立进程组。超时、客户端断开、宿主进程正常停止及失租会终止进程组，取消后的结果不导入父进程分支。强制杀死宿主进程、已经发生的外部副作用仍需核对；不存在通用的副作用撤销保证。

回报结果在 `ANSTS_STORAGE_DIR/result-outbox/` 持久保存，确认完成后删除。回报失败不重新执行任务；退避时间与结果一起保留。该 outbox 尚没有生产容量上限/归档策略。
