# QF-02 租户侧持久预留内核

日期：2026-11-16。范围是可测试的内部协议库，不是已经上线的准入API。首批验收时尚未接线；后续TenantControlCell影子服务与持久消息已接入，见服务和消息验收记录。Scheduler/Body真实执行仍未接线，8088继续显示执行准入未启用。

实现：[AdmissionReservationStore](../../../src/control/admission_reservations.ts)。复用原生transactionSync存储接口，与TenantPolicyStore使用同一数据库；策略读取、资源与预算检查、预留写入、审计和幂等回执在一个事务内提交。拒绝内存降级。

支持PREPARING → RESERVED → START_AUTHORIZED → RUNNING → SETTLING → SETTLED，以及UNCERTAIN和CANCELLED分支。每个操作带CAS、fencing、授权generation及幂等键。同键同内容返回原提交快照，同键不同内容409；原始幂等键不入库。终态attempt不能换reservationId重新占用。

准备阶段校验租户激活状态、策略版本及目录版本/地域/成本中心一致，固定策略快照与规范化请求摘要。资源累计使用BigInt，预算使用整数字符串；降低策略不改写原预留快照。待确认预算跨UTC周期继续占用，已结算消耗在策略窗口改变后仍被纳入相应窗口检查。此处预算是协议测试占用，不是已验证provider成本或正式账本。

准备凭证有效期最多5分钟。START_AUTHORIZED必须先持久提交；过期不能继续预留/授权/启动。撤销在事务内递增generation。UNCERTAIN没有TTL清理或自动释放；已授权时释放必须有绑定目标、attempt、fencing及generation的执行证据。已记录RUNNING后，失联也不能再用NOT_STARTED证据释放。缺少实际成本时保留SETTLING和预算占用，不默认成本为零。

## 验证

运行 `node scripts/check_control.mjs`：严格类型检查及58项测试通过，其中本批新增11项。

覆盖每个正常阶段关闭/重开原生SQLite、幂等回执恢复、UNCERTAIN跨日恢复、写入和启动授权失败回滚、降低额度、跨日预算、周期变更、晚到成本、角色/地域/租户/元数据边界、过期和旧代际，以及假未启动证据拒绝。并发测试使用四个Worker线程及四个独立SQLite连接，争抢最后一个槽位：一个提交，三个额度拒绝；这是同主机数据库并发测试，不是分布式部署证明。

测试使用临时SQLite文件并清理，不接触现有118条开发任务；未重启开发容器，因为本批没有更改运行入口或部署接线。没有创建真实预留、执行任务或调用付费模型。

## 后续接入条件

- TenantControlCell服务路由、Gateway/Scheduler适配和持久outbox/inbox补偿；入队额度还需独立处理，本库只支持单attempt执行占用。
- 容量池回执的权威校验、认证启动凭证和Body在线撤销/代际验证。当前target/capacityReceipt只是内部适配器输入，不能据此宣称容量真实可用。
- 执行证据必须由可信适配器先验证签名、来源和终止事实；本库只验证绑定字段，不认证字符串证据。不得直接暴露transition给浏览器。
- 版本费率、可验证成本上限、用量分段和账本集成。rateSnapshot显式为null；人工测试成本不构成真实计费验收。
- 独立备份恢复、真实多进程Cell部署与崩溃演练、既有占用迁移后才允许enforce。

QF-01/QF-02仍为in_progress，整体任务没有标记完成。

## 后续切片：认证凭证及影子服务接入

原有“尚未接入任何路由”的描述是首批内核验收边界；后续已增加TenantControlCell影子服务接口及Fleet内部转发。入口见 [协议目录](../../admin-platform-plan/admission.schema.json)，未增加公开Gateway后台写接口。

协调服务使用独立ANSTS_ADMISSION_SERVICE_TOKEN，不能用目录服务凭据调用。签名另用ANSTS_ADMISSION_SIGNING_KEY，没有缺省密钥。签名使用带用途前缀的HMAC-SHA256，固定purpose=ansts-admission-shadow-v1，绑定租户/任务/attempt、目标、fencing/generation、资源、目录/策略/预留版本、请求摘要与有效期。验证还读取最新权威状态；UNCERTAIN、撤销、过期、目标或代际变化均拒绝。此凭证不是Body执行grant。

服务只开放准备、容量绑定、启动授权记录、不确定标记及未启动撤销。RUNNING、SETTLING、SETTLED和带执行证据的请求被拒绝为EXECUTION_ADAPTER_REQUIRED，防止尚未认证的回执直接确认执行或释放预算。容量回执目前仍是影子输入，不代表真实容量。

严格检查与65项测试通过。隔离三个真实celld进程验证影子并发不超发、幂等、凭证目标限制、服务身份分离、原生权威重启后状态与凭证一致，以及UNCERTAIN恢复和旧凭证拒绝。后续真实开发影子流程及重启恢复已通过，见 [服务验收](admission-service-acceptance.md)；真实执行链仍未接入。

事务与游标使用约束对照了[Cloudflare SQLite存储文档](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)；当前实际后端是celld，兼容性证据来自上述原生进程测试，而不是假定两者运行时完全等价。
