# QF-02 影子服务与认证凭证验收

日期：2026-11-16。该切片接入真实开发Fleet，但尚未接入任务执行准入。整体QF-02保持in_progress。

## 实现范围

TenantControlCell在同一原生数据库内持久化策略及准入预留，通过Fleet的五个内部服务接口读写、签发和验证影子凭证。接口目录见 [admission.schema.json](../../admin-platform-plan/admission.schema.json)。没有新增公开Gateway管理代理。

目录服务、准入协调服务和凭证签名使用独立身份。本机密钥为忽略目录.dev-secrets中的admission-service.key、admission-signing.key，权限0600；只给Worker部署服务注入文件路径，Gateway不配置这两个路径。部署暂存使用umask077，密钥值不打印。重启前校验绑定服务、文件存在/权限/格式及两把新密钥不同；部署脚本额外拒绝复用目录密钥。没有更换既有目录、Body或用户密钥。新开发环境可先运行 `python3 scripts/init_admission_secrets.py`，只生成缺失的两个文件，重复运行保留已有身份，不输出值。

凭证固定为ansts-admission-shadow-v1用途的HMAC-SHA256，绑定目标主机、tenant/task/attempt、fencing/generation、目录/策略/预留版本、请求摘要、资源和到期时间。使用权威当前预留重新构造规范化字段后核验，离线签名不能替代撤销查询。UNCERTAIN、已撤销、过期、旧代际及错误目标均拒绝。密钥缺失拒绝签发，无默认密钥。

所有服务响应明确protocolMode=SHADOW及enforcement=NOT_CONNECTED。容量回执只是影子输入。RUNNING、SETTLING、SETTLED及带执行证据的请求返回EXECUTION_ADAPTER_REQUIRED；未经认证的服务请求不能直接确认执行、扣费或释放已经发出启动授权的占用。

## 验证证据

| 检查 | 结果 |
| --- | --- |
| node scripts/check_control.mjs | 严格类型检查及65项测试通过 |
| python3 scripts/verify_directory_gateways.py | 隔离三个真实celld进程：影子并发不超发、重放、独立服务鉴权、凭证目标验证、原生重启和UNCERTAIN恢复通过 |
| python3 scripts/restart_dev_cluster.py --check-only | 完整Compose、密钥绑定和文件检查通过 |
| python3 scripts/restart_dev_cluster.py --skip-build | 实际Worker更新并安全恢复，118条任务及执行规格保留，原始Drain状态恢复 |
| ANSTS_VERIFY_ADMISSION=1 node scripts/verify_directory_deployment.mjs（启用Playwright） | 真实影子准备/重放/超额拒绝/容量绑定/授权记录/签发/目标验证/UNCERTAIN撤销通过；目录和配额浏览器回归通过 |
| node scripts/verify_monitoring_deployment.mjs | 4/4目标在线，监控身份不能执行业务操作 |
| node scripts/verify_admission_recovery.mjs | 实际Fleet重启前后，三个入口均读取完全一致的UNCERTAIN预留 |

实际验证仅为m0-verification-*验收租户创建影子记录，任务和attempt字符串没有提交给Scheduler或Body，没有执行任务或调用付费模型。验收后租户和成本中心已停用。保留影子UNCERTAIN记录以验证恢复，未通过TTL或删除数据绕过占用。

目录导出和主机文件备份只覆盖目录。策略/预留表尚无独立归档恢复流程，此次重启保留不等同于跨故障域灾备。

## 尚未完成的原验收项

Scheduler与Body的真实执行接线、认证容量与终止证据适配、持久outbox/inbox补偿、队列额度、费率和用量账本，以及既有运行占用迁移仍需实现。影子凭证不能作为现有Body grant；切换enforce前还需完整执行链崩溃演练。不能将这次影子服务验收当作QF-02或整体37项任务完成。
