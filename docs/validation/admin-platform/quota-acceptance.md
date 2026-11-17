# 配额策略管理验收

日期：2026-11-16。入口：http://127.0.0.1:8088/manage/tenants 。在租户行点击“查看配额策略”。

本次交付配额策略持久化、历史分页及管理界面。资源额度使用非负安全整数；内部预算使用最多78位十进制字符串，避免大整数精度丢失。每个版本保留目录元数据、操作者和原因；SRE可编辑，Viewer/Operator只读。空策略不填充虚假额度。

历史查询支持limit 1～100和不透明游标，绑定身份、授权范围、租户目录版本及历史快照。中途状态变化返回409，界面提示刷新首页。当前版本独立于历史页返回。管理界面提供每页数量、下一页、首页刷新、CAS冲突输入保留及稳定幂等重试。

| 验证 | 结果 |
| --- | --- |
| node scripts/check_control.mjs | 47项严格检查与测试通过，覆盖事务回滚、CAS、权限、恢复、游标失效 |
| scripts/verify_directory_gateways.py | 隔离三个原生进程验证双Gateway并发、分页及权威重启恢复通过 |
| scripts/verify_tenant_directory_ui.mjs | 模拟API浏览器验证大整数、冲突保留、历史翻页及只读角色通过 |
| UI typecheck / Next生产构建 | 通过；不代表全仓历史类型问题已解决 |
| scripts/restart_dev_cluster.py --skip-build | 实际部署完成，118条原任务及执行规格保留，原始Drain状态恢复 |
| scripts/verify_directory_deployment.mjs | 真实双Gateway策略创建、重放、409、只读权限、分页及浏览器编辑通过 |
| node scripts/verify_monitoring_deployment.mjs | 4/4目标在线，专用监控身份不能执行业务操作 |

真实验收创建明确命名的m0-verification-*租户和成本中心：API创建策略v1，以较低并发创建v2，跨网关逐页读取；浏览器再创建v3并翻页验证。大整数credit精确往返。验收后租户和成本中心均停用，审计及策略历史保留；未创建任务或调用付费模型。

[真实界面截图](live-directory.png)。实际环境验证了读写与分页；权威重启后的策略恢复由隔离原生三进程验收证明，本次未另行声称生产策略跨故障域恢复已通过。

响应仍为enforcement=NOT_CONNECTED。配额没有限制正在执行或新增任务，没有产生用量账单或实际扣费。执行准入、旧任务策略/费率快照及激活协议继续由QF-02/QF-08接入。目录归档只覆盖目录，不包含独立租户策略Cell；策略备份恢复仍需扩展。因此QF-01保持in_progress，整体37项任务未完成。
