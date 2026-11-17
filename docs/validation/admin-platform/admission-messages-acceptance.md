# QF-02 持久消息与影子跨对象投递验收

日期：2026-11-16。本批完成持久投递、接收去重及影子对象接线；QF-02整体仍在进行中。

## 实现与约束

预留变更、审计、幂等回执和outbox意图在同一原生事务提交；outbox写入失败会回滚预留阶段及操作回执。事件ID固定由tenant/reservation/version/kind生成，规范化内容另有SHA256摘要。同键重放返回原提交结果，不重复生成事件。

准备阶段生成RESERVE_CAPACITY，启动授权生成START_EXECUTION，不确定状态生成QUERY_CAPACITY或QUERY_EXECUTION，撤销/结算完成生成RELEASE_CAPACITY。消息只包含受控协议字段，不保存服务密钥或签名凭证。旧版本待投递意图标为SUPERSEDED，记录仍保留。

10秒持久投递租约用于竞争领取，3秒网络期限限制等待，失败按1～60秒退避重投，并按下次可投递时间排序，避免反复失败的旧消息挡住其他已就绪消息。接收端提交后丢失回执，重投仍使用原事件ID；接收端返回原始不可变回执，不重复更改接收状态。旧租约不能确认新租约，较新预留状态也会阻止旧投递回执生效。错误只记录固定枚举，不持久化可能含密钥的适配器异常文本。

独立AdmissionShadowMailboxCell持久化inbox及代际/版本撤销墓碑，按租户和逻辑接收者确定路由。撤销先到达时，晚到旧预留被标为STALE；同事件ID不同内容保留冲突摘要并隔离，不覆盖原回执。已确认的重复事件仍返回原回执，但不会重复应用效果。所有消息与回执明确SHADOW，不能把原回执当作当前执行授权。

领取时可由当前持久状态恢复旧版本未建立outbox的事件，使用相同稳定ID，不推断“已投递”或“已终止”。过期的新增容量/启动意图、停用租户的新增意图被隔离，资源和预算不自动释放。查询和撤销投递继续可用。没有对pending、去重回执或撤销墓碑进行TTL清理；容量上限拒绝新增并回滚，不删除旧记录绕过上限。

## 验证

| 检查 | 结果 |
| --- | --- |
| node scripts/check_control.mjs | 严格类型检查及75项测试通过，本批新增10项消息故障测试 |
| scripts/verify_directory_gateways.py | 隔离三个原生celld进程：独立发送/接收对象投递、原生恢复、pending查询消息保留及回执不推进执行阶段通过 |
| restart_dev_cluster.py --check-only | 完整Compose检查通过 |
| restart_dev_cluster.py --skip-build | v6-admission-shadow-mailbox迁移已部署；118条任务与执行规格保留，原始Drain恢复 |
| ANSTS_VERIFY_ADMISSION=1 verify_directory_deployment.mjs（Playwright） | 实际发送/收件箱对象投递通过，容量记录后仍PREPARING，启动记录后仍START_AUTHORIZED；目录/配额浏览器回归通过 |
| verify_monitoring_deployment.mjs | 重启后4/4目标在线，监控身份仍不能执行业务操作 |
| verify_admission_recovery.mjs | 实际Fleet重启前后，三个入口均读回完全一致的UNCERTAIN及待投递QUERY_EXECUTION |

故障测试覆盖：outbox写失败整体回滚、旧状态补建稳定事件、领取后数据库重开恢复、接收提交后回执丢失、发送端确认写失败、旧租约拒绝、乱序撤销、同ID内容冲突、过期/停用隔离和有限错误记录。跨重启单元测试使用独立原生SQLite文件；真实celld验证另外覆盖对象命名空间和RPC路径。

实际验收只在m0-verification-*租户生成影子task/attempt标识，没有把它们提交给Scheduler、Body或Engine，没有调用付费模型。验收租户和成本中心已停用。私有恢复记录同时保存预留和pending消息快照，以便三个Fleet入口重启前后精确比较。

## 尚未完成

目前由内部admission-dispatch请求触发一次投递；尚无自动扫描/告警/重投调度。收件箱仅记录影子观察和撤销墓碑，不提供真实物理容量，不执行Engine，不生成有效终止证据，也不改变租户资源/预算占用。真实容量/Body适配、认证终止证据、自动补偿调度、队列额度、费率和用量账本仍需实现。

新增outbox/inbox也尚未纳入独立归档恢复；目录备份不包含它们。服务保持SHADOW/NOT_CONNECTED，不能宣称真实执行准入或QF-02原始验收已经完成。

后续进展：本页记录最初的手动投递切片；当前已接入原生alarm自动投递、持久暂停恢复和看门狗，见 [自动投递验收](admission-automatic-acceptance.md)。真实执行/容量补偿和独立归档仍未完成。
