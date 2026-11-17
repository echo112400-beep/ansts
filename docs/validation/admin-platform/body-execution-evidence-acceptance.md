# QF-02 Body 执行观察证据验收

日期：2026-11-16。本批接入真实执行观察与租户状态更新；不代表执行前硬配额门禁已启用，QF-01/QF-02仍在进行中。

## 实现范围

容器执行可附带tenantAdmissionObservation，绑定已有租户预留、策略/目录版本、代际、请求绑定摘要及executionInputDigest。Daemon按实际请求重新计算输入摘要，资源从规范化执行规格提取，不接受观察上下文自报CPU/内存。支持范围目前为container；未附加上下文的旧请求沿用原流程，agent观察尚待接入。

固定输入的新预留生成包含服务器随机挑战的绑定摘要，防止先执行后预先拼造匹配事实。旧的未固定输入记录不能直接使用证据接口。观察上下文本身不是启动授权，也不能替代已有Scheduler租约和Body执行grant。

Body在本机持久化UNCERTAIN、RUNNING、NOT_STARTED、TERMINATED观察及递增序号，独立于HTTP结果。RUNNING来自Engine确认容器启动，此时并不保证用户argv已开始；TERMINATED来自确认删除/停止。NOT_STARTED仅由Daemon明确在执行前取消的分支生成。创建响应丢失且删除返回404时，改为BODY_STOP_UNCONFIRMED，不能把“查询不到”当成确定未创建；本机恢复或缺少记录也不自动产生未启动证明。

Host事实按独立用途HMAC签名，读取只允许对应Body身份。控制端从已固定目标Host导出验签键，校验tenant/task/attempt、fencing/generation、输入、资源及策略快照；未知Host、错误签名或绑定不符拒绝。规范化事实、摘要、序号和原提交回执同事务保存，重放不重复修改状态，晚到旧RUNNING不能复活已终止预留。

验证RUNNING后可推进到RUNNING；过期或不符合启动授权阶段的运行观察保留UNCERTAIN占用。验证TERMINATED后进入SETTLING并释放并发/CPU/内存额度，未定价预算继续占用，actualCreditUnits保持null；不伪造零费用。明确NOT_STARTED可撤销并递增代际、只退款一次。消息意图与上述状态同事务提交。

## 验证证据

| 检查 | 结果 |
| --- | --- |
| node scripts/check_control.mjs | 严格类型检查及91项测试通过，本批新增10项 |
| 证据故障测试 | Host重建、签名/绑定/重放/乱序、未启动正证、outbox写失败回滚、资源释放而预算保留、Engine创建响应丢失与404不确定性通过 |
| 旧执行回归 | 按生产CommonJS文件布局编译运行4项测试通过，覆盖旧进程/容器执行、持久结果、取消与Engine故障；不代表全仓历史类型问题已解决 |
| python3 scripts/verify_body_evidence_native.py | 隔离的真实celld Scheduler/Body控制对象、原生Daemon与实际Docker Engine联合验收通过，无模拟Scheduler或Engine |
| 原生联合路径 | 实际租约/执行grant → Engine启动事实 → 租户RUNNING → 确认终止 → SETTLING；资源额度可复用，但未定价预算仍阻止超额新增 |
| 原生恢复 | Daemon重建仍可读取同一终止证明；控制权威重启后重放返回原提交，不重复结算或释放 |
| restart_dev_cluster.py --build-only / --skip-build | 新运行镜像构建部署完成；118条原任务及执行规格保留，原始Drain恢复 |
| verify_body_evidence_deployment.mjs | 两个实际Body证据端点鉴权、Host密钥配置及UNKNOWN语义通过，监控身份拒绝；无任务变更 |
| verify_directory_deployment.mjs（Playwright） | 既有影子/目录/配额与浏览器回归通过 |
| verify_admission_recovery.mjs / verify_monitoring_deployment.mjs | 原UNCERTAIN/pending快照3入口一致，4/4监控目标在线 |

原生联合验收使用独立celld数据库和唯一Engine owner，执行一个无网络的Node容器命令，随后完成并确认该测试任务；没有触碰开发环境任务或调用付费模型。联合验收中的预留估算是显式测试值，不是供应商价格或真实内部费率。

开发部署只验证了接口、身份及旧状态保留；实际Docker执行及正证回传由上述隔离联合验收证明。生产BodyAgent目前没有自动传递观察上下文或上传证据，不能用接口存在冒充自动接线完成。

## 仍待完成

BodyAgent观察绑定及持久证据outbox、真实容量授权及执行前租户门禁、agent观察、历史长ID兼容、队列额度、费率/用量账本、独立归档及真实执行全链故障演练仍需继续。当前启动授权仍由既有执行grant控制，配额控制服务仍标记SHADOW/NOT_CONNECTED。目录归档不包含新Body事实或租户证据回执表。
