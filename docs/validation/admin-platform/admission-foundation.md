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

QF-02 凭证持久化补证：控制面 SQLite 用例在 START_AUTHORIZED 后重开，验证原凭证在有效期内可读，到期边界拒绝凭证及 RUNNING 转移。凭从未启动的证据撤销后再次重开，验证代际提升、旧凭证与旧代际转移均被拒绝，重复撤销返回原结果。该凭证属于影子协议；此用例未验证 Body/Engine 的实际启动拒绝。

QF-02 Host 独立恢复：在终止证据已提交但响应丢失时停止原 BodyDaemon，将其状态目录复制到独立临时目录，由另一 BodyDaemon 读取。恢复的签名终止证明与原始字节一致，待确认 outbox 在退避窗口结束后重新可见且仅含该证明。原目录上的后续重放仍返回同一租户预留结果。隔离原生演练退出 0；此证据只覆盖停机副本，不代表在线 Host 备份或 Body Cell 独立恢复已验收。

QF-02 Body Cell 独立恢复：隔离 celld 通过现有迁移协议导出真实已完成执行的 Body 状态，导入未初始化的独立对象，重新导出与源完全一致（含执行及已消费令牌记录）；再次导入不会覆盖。组合复跑验证 Host 停机副本及原生 Body 到期拒绝，退出 0。未恢复在线路由或历史执行清单，本项仅覆盖 Body 状态行。

QF-02 独立恢复复验：导入后的 Body Cell 在真实 celld 重启后重新导出，执行及已消费令牌状态仍与原档一致。独立恢复及到期启动模式退出 0。默认完整原生回归另在月结预览遇到额外 BODY_ATTEMPT_SEQUENCE_UNPROVEN；禁用独立对象导入后仍复现，不能归因于导入夹具，也未放宽断言。该差异待核实。

完整原生回归复验：连续两次默认模式退出 0，均通过非空 Scheduler/Body 覆盖、预览授权与水位校验、真实 Agent 和账本恢复。保留原序列断言；新增仅在 BODY_ATTEMPT_SEQUENCE_UNPROVEN 出现时输出隔离任务的状态、版本、来源时间与 baseline 标记。此前额外诊断未再复现，根因仍未证实。

QF-02 恢复对象在线读回：隔离 celld 重启后，经生产 /v1/body/result 处理器读取恢复对象，按原任务及租户返回与源对象一致的 COMPLETED 结果及 fencing；无认证返回 401，错误租户返回 404。目标对象由夹具映射，无共享环境路由变更，也不表示历史覆盖已恢复。组合原生演练退出 0。

恢复结果路由补验：隔离演练改用生产 ANSTS_BODY_NAMESPACE_PREFIX 配置定向到独立恢复对象，无自定义对象 ID 重写。celld 重启后，恢复结果与源一致，无认证 401、错误租户 404 的断言保持通过，整轮退出 0。共享集群未切换路由。

恢复覆盖边界补验：恢复结果经真实路由可读，重启后的恢复对象清单仍只有一条 COMPLETED 导入基线，其 coverageSince 晚于原始执行时间，不会冒充完整执行历史。隔离原生演练退出 0。

QF-02 启动撤销门禁：Scheduler 的试点 start_check 现重新读取租户权威并核对 START_AUTHORIZED、代际、fencing、目标、执行摘要、容量回执及到期时间，再读取容量授权。租户已撤销或代际变化时，即使容量对象仍显示 grant，启动仍被拒绝。统一 Gateway 定向用例 5/5，默认 Body/celld 原生演练退出 0。尚未发布共享 Fleet。

QF-02 撤销联跑：ANSTS_EVIDENCE_REVOKE_BEFORE_START=true 下，隔离夹具在 Body 权威签发执行令牌后，以夹具 Host 密钥向真实租户权威送入未启动证明。租户在原有效期内取消预留并提升代际；携带旧令牌的真实 BodyDaemon HTTP 启动请求返回 409 STALE_EXECUTION_LEASE，模型调用为零且执行证据查询 404。整轮退出 0，未发布共享 Fleet。

QF-02 撤销门禁已发布到共享开发 Fleet，配置保持 observe。维护退出 0，121 条任务及执行规格保留、恢复原 Drain，双 Gateway 与三 Scheduler 均健康。双 Gateway 结算读回与既有 observe 基线逐字一致。撤销拒绝行为来自真实 Scheduler/BodyDaemon 隔离联跑，未在共享环境撤销用户任务。

QF-02 运行中控制面恢复：启用 ANSTS_EVIDENCE_RESTART_WHILE_RUNNING=true，在真实 BodyAgent 等待首次模型响应且租户预留为 RUNNING 时硬停隔离 celld。固定夹具宿主端口后，原 Agent 保持控制地址不变；重启读回 RUNNING 预留与停机前逐字段相同，放行模型后继续原执行、投递终止证明并完成费用结算。完整原生复跑退出 0。此前一次重跑在到达硬停前遇到夹具会话 401，未复现，未修改鉴权。
