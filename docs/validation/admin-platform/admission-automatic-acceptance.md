# QF-02 自动影子投递与原生唤醒验收

日期：2026-11-16。本批接入自动投递/重试及暂停恢复；QF-02整体仍在进行中。

## 运行行为

首次准入变更在验证权限及目录身份后，先持久注册原生alarm，再提交预留/outbox变更。注册失败返回503，不提交新预留或消耗其幂等键。控制对象记录UNINITIALIZED → RUNNING初始化状态，失败后可安全重试。已暂停的对象不会因后续准入变更被自动启用；历史未初始化对象可由SRE显式启用。

RUNNING按原生alarm唤醒，持久控制身份限定到单个租户。处理器先安排60秒看门狗，再进行目录和收件箱调用，每批最多四条。目录读取和每次消息传输各有3秒期限；消息沿用原有租约和有界退避，目录/控制故障另有30秒重试下限。故障不会释放预留，也不把回执当作真实执行或终止证据。

RUNNING即使空队列也保留每分钟一次持久心跳，避免旧alarm恰好被消耗、随后新消息提交而丢失唤醒。空队列不调用目录/收件箱。PAUSED停止续约，已经安排的一次回调可能仍发生，已领取的一次投递可能完成；显式授权的admission-dispatch仍可用于诊断。这不是Engine停止开关或真实准入启用开关。

暂停/恢复仅SRE_ADMIN可修改，要求expectedVersion、原因和幂等键；相同键返回原不可变配置回执，并同时返回当前配置。旧恢复回执重放不能覆盖更新的暂停。配置、历史、幂等回执和有限运行计数持久保存；指标按消息确认/重试/回执隔离命名，不把它们冒充执行数。状态接口包含真实getAlarm返回值。

## 验证证据

| 检查 | 结果 |
| --- | --- |
| node scripts/check_control.mjs | 严格类型检查及81项测试通过，本批新增6项自动调度测试 |
| 单元故障注入 | 注册失败不提交、暂停/CAS/重放、重建控制对象、接收端重试、8轮目录故障仍继续安排alarm、空闲心跳及暂停停止续约通过 |
| python3 scripts/verify_directory_gateways.py | 三个原生celld进程：无手动dispatch自动投递、暂停/恢复、强制终止后到期alarm恢复通过 |
| 原生强制终止证据 | 使用独立测试容器；确认消息处理时间晚于停止完成时间，且早于重启后的首次状态查询，排除停止前已完成及查询触发的误判 |
| restart_dev_cluster.py --skip-build | 已部署真实开发环境，118条原任务与执行规格保留，原始Drain恢复 |
| verify_admission_automatic_deployment.mjs prepare | 真实native alarm自动投递，无手动dispatch；暂停保留待释放消息，验收元数据已停用 |
| verify_directory_deployment.mjs（Playwright） | 原有影子/目录/配额及浏览器回归通过；此脚本明确使用PAUSED手动模式，以固定其故障快照 |
| verify_monitoring_deployment.mjs | 重启后4/4目标在线，监控身份仍不能执行业务操作 |
| verify_admission_automatic_deployment.mjs verify-recovery | 实际Fleet重启后暂停配置/队列保留，恢复后原生alarm自动投递成功，原预留状态不变，验收控制器已再次暂停 |

原生兼容性先通过独立AlarmProbe实测，再在实际TenantControlCell验证。Cloudflare的[alarm文档](https://developers.cloudflare.com/durable-objects/api/alarms/)用于设计参考；celld兼容性不靠假定，以上述实际进程和时间证据为准。

真实开发验证使用m0-verification-*命名的独立租户与成本中心。只生成影子task/attempt字符串，未提交Scheduler/Body/Engine、未调用付费模型。验收元数据停用后仍可完成其待释放消息；这证明停用不阻止清理，不代表允许停用租户获取新容量。恢复验证结束后将自动控制器再次暂停。

## 边界与后续

原有影子收件箱仍只记录消息；真实容量、Scheduler/Body、认证终止证据及补偿状态推进尚未接入。队列额度、费率/计量/账本、跨故障域归档恢复及真实执行崩溃验收仍待完成。新控制表不在目录归档范围。服务继续SHADOW/NOT_CONNECTED，不能宣称硬配额已启用或QF-02完成。
