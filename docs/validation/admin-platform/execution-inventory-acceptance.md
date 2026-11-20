# QF-05 执行清单与覆盖诊断验收

日期：2026-11-18。QF-05 仍为 in_progress。

Scheduler/Body 状态与精简执行历史在同一原生 SQLite 事务内保存，清理 Scheduler 任务不删除历史。每租户序号、摘要链、计数和水位用于检出缺行及来源变化；不复制提示词或执行结果。首次启用的当前记录标为 baseline，不能还原已丢失的历史。

私有接口使用服务认证和目录租户授权；公开入口拒绝伪造 verified header。覆盖适配器固定来源、检查执行身份和历史阶段、核对前后 Scheduler 水位。来源失败、历史不足或扫描限额均返回不完整。诊断不创建最终账单、不释放预算。

验证：

- `npm run test:control`：195 项通过，含事务回滚、重建、任务清理、摘要损坏、旧基线、来源变化、截断和权限边界。
- `npm run build`：329 模块通过。
- `python3 scripts/verify_body_evidence_native.py`：实际 celld、Daemon、Docker、用量、归档及 Fleet 重启通过；新增执行历史原文与水位恢复断言通过，无付费模型调用。
- 开发部署第一次安全重启保留119条原任务与执行规格；`verify_execution_inventory_deployment.mjs capture` 验证三Fleet入口及Body代理一致、旧数据完整性拒绝和凭据边界。
- 全仓库 `npm test`：283通过、51失败、4跳过。失败包括旧入口缺失导出、Worker绑定列表及旧鉴权断言；此记录不代表全仓库通过。

后续：关账服务消费覆盖证据、历史未计量任务处置、实际预算结算、晚到钩子及人工调整。财务独立归档不包含 Scheduler/Body 新历史，仍保留原单权威备份边界。

第二次安全重启完成，119条原任务及规格、原Drain保留。`verify-recovery` 核对三Fleet和Body代理的清单原文、水位、覆盖起点与独立捕获完全一致；既有财务归档、真实用量恢复及监控部署回归均通过。
