# 后台能力技术方案

状态：**实施中：M0 已按记录验收，QF-01/QF-02 分批实现，其余能力按任务清单推进**。范围见 [PRODUCT.md](PRODUCT.md)，任务依赖见 [TASKS.md](TASKS.md)。

## 1. 架构选择与数据归属

优先复用当前 celld / Durable Object / SQLite 持久模式，避免为首期新增一个独立数据库运维面。新增控制对象必须先验证当前部署的事务、对象唯一权威、迁移和恢复能力，不能把进程内 Map 当作持久化。若验证不能满足原子准入要求，停止 enforce 上线并重新决策存储架构。

| 拟新增模块 | 权威范围 | 持久数据 |
| --- | --- | --- |
| TenantDirectoryCell | 全局目录与核算元数据写权威 | 租户索引、成本中心目录、归属版本、不可变费率模板 |
| TenantControlCell | 每个租户唯一额度与成本权威 | 租户/成本中心归属版本、配额、预算、准入预留、用量、账本、关账、去重记录、outbox |
| ConfigReleaseCell | 每个配置作用域唯一发布权威 | 不可变配置版本、审批、发布计划、目标 ACK、验证证据、审计 |
| CapacityInventoryCell | 全局物理 Host 归属与可信报告权威（MK-01 已实现） | Host/Daemon 唯一绑定、池与管理员上限、报告 epoch/sequence、新鲜度与隔离 |
| CapacityPoolCell | 每个物理容量池唯一分配权威（后续 MK-02/03） | 唯一宿主机、有效容量、队列、容量授权、隔离占用 |
| RegionCatalogCell | 单一目录写权威，读取可缓存 | 地域/故障域、能力、连接信任、驻留策略版本、探测证据 |
| Management API / Console | 认证、授权和查询投影 | 不自行维护可写额度或成本副本 |

物理池首期不允许同一宿主机跨多个可写权威重复登记。对象路由标识使用规范化租户/作用域 ID，不接受客户端选择任意 cell key。控制对象和内部 API 采用服务身份，外部调用只能经过 Gateway 授权。跨地域目录可先中央管理；不承诺跨地域多写一致性。

目录负责发现租户和分发版本化元数据，不承担每次执行的热路径计数。租户控制对象保存已激活的不可变费率/归属快照；目录发布新版本不自动改写在途记录，激活采用持久消息与显式生效版本。汇总按授权目录有界并发查询租户投影，部分不可达时标注不完整，不能返回看似完整的零值。

`TenantControlCell` 内额度、预算与预留在同一事务更新；物理资源、执行与租户控制对象之间采用持久消息和状态补偿，不声称存在跨对象事务。结果查询与汇总可以是最终一致，必须携带 `asOf`、版本、完整性和延迟；准入不能依据最终一致的报表计数。

## 2. 数据约束

以下为逻辑实体，具体 SQLite schema 在 CP-02 实施时定义并测试，不在本次创建数据库：

- `Tenant`：稳定 ID、状态、成本中心归属版本；停用阻止新准入，保留历史。
- `QuotaPolicy`：作用域、schemaVersion、revision、CPU milli-core/内存 MiB/并发/队列上限、预算周期、阈值、reason、actor。
- `AdmissionReservation`：reservationId、tenant/task/attempt、requestDigest、策略和费率版本、预估成本、资源量、UTC 周期、状态、fencing 与授权代际。
- `UsageEvent`：producerId、attemptId、fencingToken、单调 sequence、事件时间与接收时间、计量窗口、原始数量、证据摘要、schemaVersion。
- `LedgerEntry`：不可变 entryId、eventId、rateCardVersion、costCenterVersion、credit 数量、periodId、调整关联、pending 原因。
- `RateCard`：不可变版本、生效时间、计量维度、整数分子/分母、舍入规则、适用池/模型；历史版本不覆盖。
- `PeriodClose`：OPEN / CLOSING / CLOSED、输入水位、计数/汇总摘要、结账人、时间；缺失事件不允许 CLOSED。
- `ConfigBundle / Release / Approval / TargetAck`：内容摘要、scope、revision、冻结目标、审批身份、CAS 版本与证据。
- `HostCapacity / CapacityGrant`：唯一 hostId、poolId、报告 epoch、硬容量、授权、心跳时间、执行占用与回收证据。
- `Region / PlacementPolicy / ProbeEvidence`：国家/司法辖区、故障域、适配器、可信端点引用、能力、证据有效期、允许驻留范围。

额度与原始计量统一为整数基本单位，成本运算采用整数 credit 子单位和显式舍入；对外大整数使用十进制字符串，避免 JS Number 溢出或浮点累计漂移。总和必须能从原始事件与费率版本重算。

所有变更带 `expectedVersion`；同一 `(tenantId, Idempotency-Key)` 重放同一内容返回同一操作，不同内容返回 409。内部消息另有稳定 eventId 和收件去重，去重窗口不按零点清空。去重压缩需有归档和不可重放水位证据。未结预留、待核算、未投递 outbox 不参与普通 TTL 清理。

通用审计记录 actor 的已验证数字身份、tenant/scope、requestId、动作、原因、前后版本、脱敏差异和结果。摘要链仅用于完整性检查，不等同于独立防篡改存储。审计索引与业务写入同事务或持久 outbox，禁止“写成功但无审计”。

## 3. 准入与执行协议

目标路径：Gateway 授权 → 租户准入预留 → 容量池预留 → Scheduler 绑定 attempt → Body 验证凭证并预留本机资源 → Engine 执行 → 用量/结果持久投递 → 确认结算与释放。

1. 入队前检查租户状态与队列上限；出队执行前原子检查并发、CPU、内存、预算和驻留策略。长时间排队需重新取得准入，不能沿用过期授权。
2. 租户权威生成签名/认证的预留凭证，绑定 task、attempt、资源、策略、授权代际、到期时间。容量池与 Body 验证目标、资源和 fencing，拒绝篡改或旧代际。Gateway 双副本不得分别产生独立额度。
3. 跨对象操作按 `PREPARING → RESERVED → START_AUTHORIZED → RUNNING → SETTLING → SETTLED` 持久推进。必须先记录 START_AUTHORIZED 再交给执行器。任一阶段可进入 `UNCERTAIN` 并阻止重复分配。
4. 只有尚未发出启动授权、且协调器已持久撤销旧代际的准备态可超时释放；Body 必须拒绝撤销/过期凭证。已发出启动授权后，需 Engine 未启动/已终止的有效证据才能释放。网络超时本身不是未执行证据。
5. CPU/内存租户并发预留在确认结束后释放；预算预留结算为实际成本或待核算项。未结预算保留在原周期并在当前可用预算中显示跨期占用，避免跨零点绕过限制。用量按实际 UTC 分段归属，原策略与费率按任务启动快照固定。
6. 用量分段可持续增加预留；达到执行前约定的硬资源/时长上限，由现有取消链发起停止并等待退出确认。供应商调用必须设置可验证的 token/请求上限；无法限制成本的执行在硬预算模式下拒绝准入。晚到用量造成的差额保留并限制后续准入，不伪造“绝不超出估算”的保证。

迁移为 `observe → shadow → enforce`。shadow 同时计算新决策但不影响原链路；enforce 试点需迁移既有运行与不确定 attempt 的占用，无法证明的占用隔离。enforce 下控制权威不可达时拒绝新准入，不能回退到旧无配额路径。紧急停止可暂停新增，不能删除账本或释放未知占用。

## 4. 真实计量与成本归集

复用 attempt/fencing 标识，扩展独立 usage outbox；不能直接拿任务完成数或 Prometheus 图表作为账单。Body 在持久事件中记录计量起止和已分配 CPU、内存、slot；provider 适配器记录响应返回的 token。CPU 实际使用率只有容器计数器可信采集时才作为单独维度，首期不将资源预留量冒充利用率。

事件以 producer + attempt + fencing + sequence 唯一化；相同键不同 payload 进入冲突隔离，重放不重复结算。按固定顺序分段，累计值取相邻差量，拒绝倒退或异常跳变。执行重试新建 attempt，费用保留在各自 attempt 并汇总任务。

成本算法按费率版本对原始整数数量计算，周期聚合后按明示规则舍入，不能逐 UI 单元随意舍入再累计。缺失费率或 provider 用量标为 pending。结账先冻结输入水位并验证全部 attempt/事件序列；已关闭周期的晚到事件写下一开放周期的调整项，保留原归属周期与原始事件关联。

不导入旧默认模型名称、示例 USD 金额或内存累计值为真实历史账单。历史缺失保留缺失标记。计量事件留存/归档政策需在首个结账前确定；删除仅在归档验证和去重水位安全后执行。

## 5. 配置状态机与发布安全

配置版本：`DRAFT → VALIDATED → SUBMITTED → APPROVED / REJECTED`。发布：`PLANNED → CANARY → VERIFYING → ROLLING_OUT → SUCCEEDED`，异常进入 `PAUSED / FAILED`。回退另建新的 release 和 revision，不能把旧 release 的失败记录覆盖掉。

发布对象记录 schemaVersion、baseRevision、contentDigest、冻结的 targetIds 和每目标 desired/applied/observed 版本。目标以服务身份返回 ACK，至少包含实际读取的内容摘要、执行代际与时间。验证窗口/错误阈值随计划冻结，超时、离线、缺失指标都不能计为成功。

审批由服务端认证 actor，禁止 request body 伪造 approvedBy，申请人与批准人必须不同。内容或目标变化使审批失效。每 scope 只允许一个活动发布，CAS 和 fencing 防止并发发布/旧 worker 覆盖新版本。协调器重启从日志续跑，重复 ACK 幂等，未知目标状态先暂停。

配置接收者适配器只暴露白名单字段，先 dry-run 再 apply，再读取实际状态确认。支持生效于新任务的参数与可安全热更新的参数，界面分别说明。需要重启/基础设施变更的字段首期拒绝发布。secretRef 只接受可信秘密存储中的引用，不回传秘密值。

## 6. 容量分配与调度集成

池容量来自经过身份校验的 hostId 报告、管理员设置的可分配上限与现有占用，取可信最小约束。同一宿主机多个 Daemon 的容量不能相加。CPU、内存、slot 为不同维度，所有维度同时满足才发授权。

容量池内部使用带权主导资源公平排队，权重有版本，队列防饥饿；验证方式为固定工作负载下的分配轨迹，而不是声明最优。首期不支持未来容量承诺和运行中抢占。

一个容量授权关联一个租户 reservation，单一有效绑定到 attempt；消费授权需要 CAS/epoch。租户额度与池容量分别扣一次，但不生成两份 usage。授权到期只回收未使用部分；已启动、断联、UNCERTAIN 进入保留或隔离。只有确认释放事件才能恢复可借容量。停止市场功能只停止新借用，已有执行继续完成和结算。

## 7. 地域目录与适配器

先定义适配器接口：`describeCapabilities / verifyIdentity / probe / estimatePlacement / submitNewTask / drain`。首期实现目录、模拟适配器和策略评估；真实 adapter 必须显式注册并通过部署验收。

地域放置先筛选驻留、数据类型、模型/运行时能力与身份有效性，再评估容量和健康，最后比较延迟。地域元数据与探测证据必须有来源、时间和过期策略；未知/过期驻留信息对受约束任务拒绝放置。

端点不接受任意用户 URL，管理员登记也需受控 allowlist、DNS/IP 校验和出口限制，避免探测 API 成为 SSRF 通道。远端使用可轮换服务身份、mTLS 或等价认证；secret 只存引用。dev loopback 例外仅限显式开发配置，生产拒绝沿用。

接入真实第二地域前，必须确定供应商/地点、控制权威故障策略、镜像和运行时、对象存储/LayerFS 数据可达性、复制与备份、密钥边界、网络费用计量及业务 RPO/RTO。凭证与数据驻留约束同样约束模型服务。仅迁移新准入，不自动转移执行中的任务；自动灾备需要另行设计全局 fencing 和实测恢复流程。

## 8. 拟议 API

下表描述目标 API；已实现范围以 RUNTIME-CONTRACT.md、contracts.schema.json 和 PROGRESS.md 的验收记录为准。管理资源走 `/v1/admin`，内部执行协议走认证的内部路由，不能通过增加普通浏览器参数访问。

| 方法与路径 | 用途 / 关键约束 |
| --- | --- |
| GET/POST `/v1/admin/cost-centers` | 范围内成本中心目录 / 全局 SRE 登记 |
| PUT `/v1/admin/cost-centers/:id` | CAS 更新名称/状态，不删除历史归属 |
| GET/POST `/v1/admin/tenants` | 范围内租户目录 / SRE 创建 |
| GET/PUT `/v1/admin/tenants/:id/quota` | 配额快照 / expectedVersion + reason |
| GET `/v1/admin/tenants/:id/usage` | 游标、时间窗、attempt 粒度与完整性 |
| GET `/v1/admin/finops/summary` | 授权范围成本中心汇总，明确 asOf |
| GET/POST `/v1/admin/finops/rate-cards` | 历史费率 / 创建不可变费率版本 |
| POST `/v1/admin/finops/periods/:id/close` | 校验水位后关账，CAS、幂等 |
| POST `/v1/admin/finops/adjustments` | 追加调整，不覆盖原记录 |
| POST `/v1/admin/finops/exports` | 授权异步导出，短期访问凭证与审计 |
| GET/POST `/v1/admin/config-bundles` | 查询 / 新版本 |
| POST `/v1/admin/config-bundles/:id/validate` | schema、语义、影响范围预检 |
| POST `/v1/admin/config-bundles/:id/submit` | 冻结审批内容 |
| POST `/v1/admin/config-bundles/:id/approvals` | 独立操作者批准或拒绝 |
| POST `/v1/admin/config-bundles/:id/releases` | 创建分批发布计划 |
| GET `/v1/admin/releases/:id` | 每目标 desired/applied/observed 与证据 |
| POST `/v1/admin/releases/:id/pause`、`.../rollback` | 暂停 / 新建回退版本与审批流程 |
| GET/POST `/v1/admin/capacity/pools` | 真实池目录 / SRE 登记 |
| GET/POST `/v1/admin/capacity/requests` | 授权范围需求 / OPERATOR 申请 |
| POST `/v1/admin/capacity/requests/:id/cancel` | 取消未消费需求，确认已用部分保留 |
| GET `/v1/admin/capacity/grants` | 授权、执行绑定、到期与隔离 |
| GET/POST `/v1/admin/regions` | 目录 / SRE 登记草案 |
| PUT `/v1/admin/regions/:id` | CAS 更新元数据与信任引用 |
| GET/POST `/v1/admin/placement-policies` | 策略版本 / 创建版本 |
| POST `/v1/admin/placements/simulate` | 仅模拟；不产生执行与资源预留 |
| POST `/v1/admin/regions/:id/drain` | 停止新放置；不重跑已有任务 |
| GET/POST `/v1/admin/archives` | 全局管理员查询/创建校验快照，保留全部回执 |
| GET `/v1/admin/archives/:id` | 校验内容与清单后导出；不等同于独立备份 |
| GET `/v1/admin/operations/:id`、`/v1/admin/audit` | 异步操作与脱敏审计查询 |

写入统一要求服务端权限、可信 Origin（cookie 会话）、Idempotency-Key、reason 和 CAS 版本。创建资源的 baseVersion 可为空，更新必须显式提供。错误采用 `{code,message,requestId,currentVersion?,details?}`；401 未认证、403 越权、409 版本/幂等冲突、422 策略不合法、429 额度/速率限制、503 权威不可达。拒绝原因不得泄漏其他租户数据。

分页使用稳定排序、cursor 与快照水位；导出不以现有 management 最多 1000 条任务的页面切片作为全量数据源。大型导出为异步作业，下载再次验证授权，CSV 防公式注入，避免导出租户 prompt/密钥。

## 9. 接入位置与可观测性

拟新增 `src/control/` 放通用模型、授权/审计与持久协议，`src/cloudflare/*_control_cell.ts` 放权威对象适配；配额与用量业务落 `src/finops/`，池分配落 `src/cluster/`，用量 producer 落 `src/runtime/`，发布逻辑放 `src/config_release/`。这些是任务的目标目录，并非现有模块声明。

接入修改涉及 `src/cloudflare/env.ts`、`worker.ts`、`gateway_cell.ts`、`scheduler_cell.ts` 以及 Body 的实际执行入口。CP-01 必须确认并记录运行链路，再建立契约，不能只把新服务挂到旧 dashboard BFF。前端复用 `ui/console/components/admin-workspace.tsx` 与 `ui/console/lib/admin.ts`，新增领域详情组件和 API client。

指标包括准入拒绝原因、控制对象事务失败、未结预留年龄、用量投递延迟/冲突、待核算金额、发布目标滞后、池隔离容量、地域证据新鲜度。指标标签避免 tenant/task 高基数，详细对象信息进入受控审计查询。计量事件与监控指标分别保存，Prometheus 不作为计费权威。

既有观测验收记录中 Gateway/Daemon 指标存在不可用项；CP-06 必须重新探测和修复实际路径/鉴权，再设发布健康门槛。不能直接假定现有 dashboard 支持完整错误率或 SLO 验证。

## 10. 验证与回退

单元测试覆盖整数核算、跨日分段、策略验证与状态机；契约测试覆盖权限、CAS、幂等和分页。集成测试必须覆盖双 Gateway 并发、一槽多租户争抢、跨对象准备/授权各边界崩溃、Engine 启动后断联、outbox 重放、跨零点、价格变更、关账晚到事件、发布协调器重启和旧 ACK。

浏览器覆盖只读/操作员/管理员、同人审批拒绝、409 输入保留、部分发布失败、容量失联与地域空态；桌面、窄屏和键盘流程沿用 hrui 基线。模拟 provider 可验证 token 和错误情形，真实容器验证资源生命周期；不需要为首期测试发起付费模型调用。

隔离 Compose 项目使用独立端口/卷/测试租户，执行前确认环境标识，避免修改当前开发任务或删除现有数据。升级保留迁移与备份证据，恢复演练验证账本、预留、执行状态与审计，而不仅检查容器存活。

回退：配额停止新增 enforce 流量但保留持久账本；配置发布暂停并新建回退版本；容量池停止新借用而保留在途授权；地域停新放置。任何回退不得清空去重记录、释放 UNCERTAIN、回滚 schema 导致数据丢失，或把当前任务自动迁移重跑。

## 2026-11-16 QF-02 实施切片：租户侧预留内核

新增 control/admission_reservations.ts，策略读取、配额累计和预留/回执在同一原生数据库事务提交。原子状态推进、授权前持久化、撤销代际、UNCERTAIN保留及绑定终止证据的释放约束已实现；旧策略快照及未结预算跨周期保留。协议库尚未接入任何运行路由，不能当作已部署准入。

prepare只处理一个执行attempt（concurrentAttempts=1、queuedTasks=0），队列额度后续另接入。人工传入的容量回执/执行证据必须在未来适配器认证后使用；当前库不验证外部签名，也不签发Body执行凭证。rateSnapshot=null，合成测试成本只是占用协议测试，实际费率/用量归QF-03/QF-04。QF-01的运行集成依赖仍未满足，不提前启用enforce。验收与限制见 ../validation/admin-platform/admission-foundation.md。

## 2026-11-16 QF-02 后续切片：影子服务与认证凭证

TenantControlCell及Fleet增加五个内部admission-*操作，使用独立协调服务身份；目录查询仍只使用目录服务凭据。独立签名密钥签发绑定权威预留的影子凭证，目的固定为ansts-admission-shadow-v1，验证要求最新预留、未到期、正确目标及START_AUTHORIZED状态。没有公开Gateway代理，没有给Body发送真实执行grant。

未接可信执行证据适配前，服务关闭RUNNING/SETTLING/SETTLED及带evidence的请求；准备、容量绑定、授权持久化、UNCERTAIN和未启动撤销可进行影子验收。服务不会接受浏览器自报终止或成本。元数据激活、持久跨对象消息和真实执行接线仍未完成。部署用独立0600文件注入，启动前检查配置不能把准入密钥路径放到公开Gateway。详见 ../validation/admin-platform/admission-service-acceptance.md。

## 2026-11-16 QF-02 持久消息切片

AdmissionReservationStore把协议意图与预留/审计/幂等回执同事务提交。稳定事件ID、10秒投递租约、3秒传输期限及有界重试退避支持至少一次投递；接收端用持久inbox去重，保留冲突摘要和代际/版本撤销墓碑。回执确认只改变消息投递状态，不能改变资源、预算或执行阶段。

新增v6-admission-shadow-mailbox及独立AdmissionShadowMailboxCell，原有命名空间与卷保留。内部admission-outbox读取最多100条授权范围内待处理/隔离消息，admission-dispatch单次投递。旧状态在领取时按稳定ID恢复消息；过期或停用的新获取意图隔离，UNCERTAIN占用保留。当前没有后台自动投递循环，也没有真实容量/执行效果。目录归档不包括新outbox/inbox。详细证据与限制见 ../validation/admin-platform/admission-messages-acceptance.md。

## 2026-11-16 QF-02 自动影子投递

TenantControlCell引入持久投递配置及原生alarm。新准入变更先注册唤醒再提交，RUNNING保持60秒持久心跳，处理前预置看门狗，每批最多4条；目录/传输各限3秒，目录故障至少30秒后重试。保持空闲心跳以关闭旧唤醒消费与新提交之间的竞态；PAUSED停止续约，不清理占用或pending。已有旧状态通过SRE显式启用恢复，不因为只读查询或后续变更绕过暂停。

新增admission-delivery-status和admission-delivery-control，仅内部服务可访问；控制修改为SRE、CAS和幂等，返回原配置回执及当前配置，防止旧恢复重放覆盖新暂停。自动服务身份scopeTenant固定为当前对象租户；不持久化调用者密钥或临时凭证。真实celld强制终止验收通过，时间证据确保消息在重启后且首次查询前已处理。自动投递仍只连接影子收件箱，真实执行/容量补偿尚未实现。详见 ../validation/admin-platform/admission-automatic-acceptance.md。

## 2026-11-16 QF-02 Body 正证与资源/预算分离

新增Host持久观察账本及HMAC事实接口。容器启动/确认终止由私有Engine生命周期回调记录；执行前取消可生成明确NOT_STARTED，未知记录、创建响应丢失后的404或恢复时的缺失不生成负证。新预留可固定executionInputDigest并携带服务器随机绑定挑战；事实验签再校验Host、task/attempt、资源、快照及代际，规范化原事实/回执与状态/消息同事务保存。

TERMINATED进入SETTLING，资源额度释放但预算保持待核算，不默认费用为0。RUNNING已过期或未满足授权阶段时保留UNCERTAIN。旧未固定输入的预留、agent观察及BodyAgent自动绑定/证据outbox仍未接线；本接口不会替代现有Scheduler/Body执行grant或启用硬配额。实际原生Scheduler/Body+Daemon+Engine联合验收及部署边界见 ../validation/admin-platform/body-execution-evidence-acceptance.md。

## 2026-11-17 QF-02 Host 观察投递实施

Host 最新观察事实自身构成持久 outbox：与签名事实同文件原子提交确认摘要和有界重试信息。允许合并中间状态，不能作为有序用量事件流。确认和失败延期都必须匹配当前摘要；新事实不能被旧网络响应删除或延迟。旧事实文件无确认信息时重新投递，接收端原有不可变回执保证幂等。

BodyAgent 每2秒尝试一次独立投递，单批最多4条；每次远端调用10秒超时，失败持久退避至最多60秒。Fleet 新增 Host 专用 `/internal/v1/body-execution-evidence`，认证 Host 后验证同 Host 签名，以服务端生成的租户限定 OPERATOR actor 查询目录并调用 TenantControlCell。不会把协调器密钥交给 Host，也不会让 Host 调用通用 prepare/transition。

task/attempt 在新准入、消息和观察中采用与既有 Body attempt 相同的512字符上限；其余控制标识限制保留。自动上下文绑定、agent观察及真实容量授权尚待完成；观察和投递仍不构成执行授权。详见 ../validation/admin-platform/body-observation-outbox-acceptance.md。

## 2026-11-17 QF-02 自动绑定与 agent 生命周期

新增只读 Host 路由 `/internal/v1/body-observation-binding` 和协调器内部 `admission-observation-binding`。查找由 Host 身份确定目标，从租户唯一权威返回已有启动授权的观察上下文；必须同时匹配执行身份、fencing、输入与资源。查询不改变预留，不能被当作容量或执行 grant；过期未结 attempt 可查询以投递终止事实。BodyAgent 自动查找并传入上下文，SHADOW 下明确记录无绑定/不可达，不宣称 enforce 已启用。

agent 在恢复检查和缓存命中判断后、进入实际循环前产生 RUNNING；正常返回且工具停止已确认后产生 TERMINATED。BODY_STOP_UNCONFIRMED 继续传播并保留占用。等待模型时也属于本机 agent 执行生命周期，原始供应商用量仍需独立 producer，不从观察状态推导 token 或零费用。缺可信价格/成本上限时，自动预留创建仍未开放。

本批及Host观察outbox代码已在实际开发集群部署，具体安全重启及隔离/真实验收见 ../validation/admin-platform/body-agent-binding-acceptance.md。

## 2026-11-17 MK-01 物理目录与可信报告

增加全局 `CapacityInventoryCell`，在一个原生事务权威中约束 Host→Pool、Daemon→Host 唯一归属；后续每池分配/队列权威不得另建可写物理归属副本。新增 v7 迁移。池、Host、审计、幂等回执、最后报告与冲突隔离同 SQLite 状态保存，现阶段不允许静默迁移或删除绑定。

Daemon 从显式只读挂载的宿主机 machine-id 派生 Host ID；本地持久 epoch/sequence 在签名前提交。报告使用独立目的 HMAC 和 Host 凭据，Fleet 验证认证 Daemon、签名及注册归属后写目录。BodyAgent 独立每5秒报告，不影响执行循环。源时间及 receivedAt 共同约束15秒新鲜度，重放不刷新，未知/过期/恢复/排空/冲突不提供可借容量。已知占用保留，不作TTL返还。

有效 Host 上限取管理员上限与成员报告约束的最小值，不叠加各Daemon总量；占用则累加各本机持久预留。池进一步受自身上限约束。该保守容量不是硬件利用率或资源计量；`allocation: NOT_CONNECTED` 明示尚未接入grant。首期全局SRE管理物理目录，API为 `/v1/admin/capacity/pools`、`/hosts`（GET/POST/PUT）和 `/audit`（GET）；Host使用私有 `/internal/v1/capacity/report`，普通管理身份不能冒用报告通道。

真实开发两Daemon从共同machine-id归为一个本机Host，已部署并完成双端重启/epoch连续性验证；不将本地池称为第二地域。验收和边界见 ../validation/admin-platform/capacity-inventory-acceptance.md。

## 2026-11-17 QF-03 源端协议与接收内核

用量使用独立 `UsageEvent`，键为 producer/tenant/task/attempt/fencing/sequence；输入摘要绑定内容而不参与去重键，防止变更输入生成同一执行的另一份费用。事件签名有独立用途，不复用观察/容量回执。原生每attempt原子文件同时保存不可变事件和投递状态，先保存序号再公开；每流仅投递最早未确认事件，确认不删原事件、隔离阻断推进。

计量CPU原始单位保留Engine NanoCpus，数量为nano-core×ms，后续费率按明确有理数换算；配额CPU仍为整数milli-core，不以向上取整的配额粒度提前核算。分配内存/slot亦使用整数时长，按UTC日界切分。窗口覆盖本机已分配资源，不能标作CPU实际利用率。历史重放不重新打开用量流；未知停止保留开窗，只有Engine正向清理后关闭本机分配，未知provider响应继续pending。

Agent循环在每次模型HTTP调用前写请求意图，适配器在解析action前同步提交响应数量。requestedModel与reportedModel分开，缓存拆分仅保留返回证据；原运行时保守token预算不进入用量事件。Source事件不存成本或原始prompt/模型文本。SQLite UsageInbox在可信适配器认证执行绑定后接收，原始事件、水位同事务，冲突保留原数据并隔离、记录requiresAttention；源时间与接收时间分开。

以上是源端阶段记录；后续接线已完成，当前实现见下节。默认关闭计量，启用 shadow 时必须显式配置租户白名单。

## 2026-11-17 QF-03 生产接线

Fleet Host 专用 binding/usage 入口从已认证 Host 和真实 Body RUNNING 记录核验执行，执行前由租户权威持久绑定身份和实际资源。绑定后允许历史流在旧 Body 证明退役后补投。TenantControl 使用独立准入服务凭据接收签名事件；查询使用目录服务凭据，不将写入凭据交给 Gateway。

生产 BodyAgent 独立循环自动投递，源端 60 秒 checkpoint 并按 UTC 日界拆分；无结构执行 CPU/内存为 null，只记录已知 slot。未知停止不假设释放，结构化分配仅在正向 Engine 清理后恢复关闭。接收端同事务保存事件、receivedAt、水位和稳定分页顺序；上下文固定租户版本/地域/成本中心，费率为空。授权查询采用 actor/scope/tenant 版本绑定的 HMAC 游标，冻结上界避免分页期间追加漂移。

四条低基数 Prometheus 规则覆盖冲突、投递积压、provider 未知和资源缺失。原生生产脚本验收覆盖远端提交后丢回执、旧 Body 不可读、租户接收重建及实际 fork 旧执行；没有真实模型请求。验收证据见 ../validation/admin-platform/usage-service-acceptance.md。计量接收不代替 QF-02 执行授权，也未接入价格、账本或硬配额。

## 2026-11-17 QF-04 费率与派生账目内核

`RateCatalog` 是未来 TenantDirectoryCell 唯一写入适配器使用的内核，当前尚未连接服务路由。费率仅接受版本化 credit-subunit、整数分子/分母、FLOOR/CEIL/HALF_EVEN 和显式资源/模型字段；缺失费率是 null，零费率必须显式填写。全局管理员 CAS 创建版本，内容摘要、幂等回执和审计同事务写入，不允许覆盖旧版本。分子/分母最大 2^127−1，精确计算约分后最大 2^255−1，舍入 credit 及汇总最大 2^127−1；溢出拒绝，不截断。

`TenantRateStore` 只接受可信目录适配器解析的费率，不得从浏览器直接接收快照。显式激活保存租户元数据版本、完整费率和原因；模板创建不会自动激活。执行前 pin 固定租户/地域/成本中心版本/池和激活版本，包括未激活的 null；已有用量绑定后禁止首次补 pin。租户元数据版本变化要求重新激活。已有流保持原快照，旧缺失价格不会因新模板或账目重建而自动补价。

`UsageCostLedger` 仅从同权威已接收 UsageInbox 事件创建不可变派生记录，并核对执行时归属；记录原始事件、来源摘要、接收时间、精确数量/费率/分数与 pending 原因。资源分配按源 UTC 分段，provider 数量归属响应事件所在 UTC 日。舍入桶固定为 UTC 日、成本中心版本、费率版本/激活版本和维度；先累加精确分数再舍入，不逐事件累加舍入值。缓存分拆价格与全部输入价格互斥，不重复累计 input/cache token；模型须精确匹配 provider/requested/reported 三项。

原始数量、费率、归属版本、池选择或 provider 响应缺失均保持 pending。派生汇总最多处理 10000 个事件和流，截断、未投影、open、隔离或缺响应均明确不完整；完整总额为 null，已知子总额单列。跨租户聚合缺少响应也不返回完整总额。`final=false` 固定，本内核不实施 QF-05 关账或晚到调整，不能作为已结算账单。

本阶段没有实例化生产费率/账目服务、执行 pin 或自动投影，没有修改现有用量或启用硬配额。API、实际权威接线与部署仍属 QF-04 未完成项。证据见 ../validation/admin-platform/rate-ledger-foundation.md。

## 2026-11-17 QF-04 服务与原生执行接线

TenantDirectoryCell 已实例化 RateCatalog，负责唯一模板写入和独立事务审计；TenantControlCell 的激活适配器从目录私有 `rate-resolve` 读取真实模板，浏览器仅提供版本号。真实 Host ingress 根据首次 RUNNING 绑定给出私有 priceEligible，目录 `accounting-context` 提供固定归属与成本中心版本。存在旧预留或旧用量绑定、迟到/完成流均不能补价；未知池保留 null。准入自身的自动价格快照仍待后续接线。

价格 pin、用量绑定，以及事件接收、账目投影分别同一原生同步事务；joinedTransactions 仅在同步调用栈合并嵌套内核事务，不跨 await。新事件自动投影，历史读时每批最多恢复100条未投影事件，原始无价格数据保持 pending。账目/费率/激活历史使用作用域 HMAC 游标和冻结水位。跨租户汇总仅查询授权目录，最多1000租户、并发4，时间预算用尽或响应不可达的租户显式列缺失，不形成完整总额。

接口与验证详见 ../validation/admin-platform/rate-ledger-service-acceptance.md。统一审计合并、独立备份恢复、准入价格/可信成本上限及更多跨对象故障仍未验收，不提前标记QF-04完成。

## 2026-11-17 QF-04 统一审计与独立恢复

管理审计目录范围合入 rate-card；rate-activation 通过同一 API 的显式 tenantId 分派到租户权威，不复制成第二份可写审计。目录页 coverage 说明激活需选择租户，UI 按原 hrui 控件提供两个筛选，后续激活记录 before 值对应实际旧快照。

新增 AuthorityArchiveStore，同一事务捕获固定 SQL 表/列清单的单权威快照，原始业务 JSON 字节不重写；源摘要 CAS、归档与幂等回执同时保存。目录含元数据/费率/审计/业务回执，租户含策略/预留/outbox/投递控制/用量/价格/账目；归档目录本身及外部 Host/Engine/mailbox/容量权威不包含。多个单权威备份不是全系统同时冻结。

归档摘要/表计数及独立 HMAC 校验；还核对事件身份/连续序号/水位、价格绑定/历史激活/审计和账目重算。限100000行/32MiB，每权威100份，超限不截断。恢复仅支持专用 CLI 创建全新离线 SQLite 文件，拒绝已有目标，失败整体回滚；逐表摘要、既有账目重算一致后返回 operationalResume=DISABLED。不开放在线导入、不启动 alarm，不推断 UNCERTAIN 停止，也不释放预算或重发消息。

当前准入 prepare 仍接收调用方 estimatedCreditUnits，rateSnapshot=null；缺少可信成本上限及真实容量授权。后续由 QF-02/QF-08 完成，不将运行时计量价格回填旧预留；不再把该跨任务门禁误写成 QF-04 自身费率内核未完成。成本汇总现在将任何未结预留列为不完整，防止空用量事件被当成完整零成本。

接口、离线命令及独立原生/部署验收见 ../validation/admin-platform/rate-ledger-recovery-acceptance.md。

## 2026-11-18 QF-05 月结与调整内核

新增 PeriodSettlement，首期关账为UTC_MONTH（YYYY-MM），日分段和按日/归属/费率激活/维度的舍入桶不变。预览在同步原生事务内核对已接收事件序号/窗口、完整 provider 响应、固定价格、相关 attempt 覆盖与跨期未结预留，缺失不最终关账。关账固定水位、原账目摘要、覆盖声明、校验时的流状态和计价桶；basis摘要可重算。CAS、不可变关账单与幂等回执同事务，原数据和准入占用不改写。

晚到源事件按接收ordinal与原闭账水位识别，追加关联原账单/事件/价格的确定性调整，落到不早于原周期下一月的当前开放周期。差额为 round(原精确量+已结晚到量+本期晚到量)-round(原精确量+已结晚到量)，不独立逐条舍入；更早调整期未结阻止依赖其基数的后续关闭。来源冲突形成独立attention，遗漏投递钩子也会被关账预览拒绝。单据和读取有行数/历史长度保护，超限不截断。

SettlementCoverage 当前仅是未来可信适配器参数；生产清单认证/全量覆盖和历史未计量任务门禁尚未接入，不得直接将浏览器 complete=true 作为证据。内核尚未实例化到TenantControlCell，未开放在线关账 API；实际成本分摊/准入结算/人工调整及备份v2兼容需要后续接线。现有备份v1不覆盖period表，启用服务前必须补齐。

另修复 BodyUsageJournal 月界顺序：provider回调落盘前先原子保存已经跨过的UTC日界资源片段，避免后来checkpoint追加更早时间戳。当天普通回调不额外切段，已关闭资源不因晚到响应重新分配；旧事件前缀不改写。此修复已实际安全部署，关账本身仍未启用。验证和边界见 ../validation/admin-platform/period-settlement-foundation.md。

## 2026-11-18 period 备份 v2

财务归档根据实际 period 数据选择 schemaVersion=2，补入关账身份、单据、幂等回执和调整四表；没有 period 数据时仍生成 v1。签名信封目的保持兼容，schemaVersion/版本化白名单受签名和内容校验约束。离线验证复核冻结 basis、关账摘要、回执引用和调整来源，恢复不触发结算。旧v1表清单与签名验证保留。

原生SQLite的关账/调整恢复和三份既有实际v1文件兼容通过；新格式尚未部署，实际celld与生产关账适配器仍待验收。见 ../validation/admin-platform/period-backup-v2-acceptance.md。

## 2026-11-18 QF-05 执行清单来源

Scheduler 和 Body 新增原生事务执行历史：状态和精简观察记录同事务保存，任务清理不删清单；保留每租户连续序号、摘要链与来源水位。没有原生事务的旧模拟存储不提供可信清单。首次启用仅记录当前状态为 baseline，coverageSince 之前的周期不能证明完整；迁移导入提升覆盖起点。

私有 execution-inventory 与 settlement-coverage 接口验证服务凭据和目录租户权限，固定 Scheduler 分片、Body 名称和代理来源。覆盖适配器核对身份、历史阶段、源水位及前后 Scheduler 版本；缺来源、截断或历史不足保持 complete=false。每来源最多10000条/8MiB，最多发现1000个Body，发现时间受限。`GET /v1/admin/tenants/:tenantId/settlements/:periodId/preview` 在租户权威内收集该覆盖并调用月结预览，响应不含执行流标识、事件摘要或冻结 basis；当前源码已接线，尚未运行时/部署验收。最终关账仍未接为在线 API，不释放预算。

整表诊断与 Body 快照继续使用 10000 条/8 MiB 上限。月结覆盖通过私有 Scheduler 接口按序号分页：每页 512 条，最多 128 页。收集器校验每页的租户、序号、跨页摘要与固定水位，读完 Body 后复查 Scheduler 水位；读取中断时覆盖保持不完整。
