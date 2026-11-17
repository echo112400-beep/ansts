# M0 实施进度与验证

日期：2026-11-15。CP-02 已完成；CP-01、CP-03～CP-05 进行中，其余未开始；不能将这批代码视为 M0 全部验收完成。

## 类型检查问题

根因之一是 tsconfig 的自定义 typeRoots 优先选中 `types/node/index.d.ts`，该离线声明文件是外部模块，其 `node:*` 声明不能替代完整官方 Node 类型，并且遮蔽了已安装的 @types/node。已改为使用已安装的官方类型，并从正式编译输入中排除离线 stub。

新增 `npm run typecheck` 如实执行全仓 tsc；原 ci_check 的 AST/转译不等同于完整类型检查。完整 tsc 仍失败：旧 dashboard/console_bff_api、dashboard/server 与历史测试存在缺失导出和接口不一致，例如 UffdLazyPageRestorer、ConsolePermission、ProductionPreflightGate 等。没有创建伪造兼容类、关闭 strict 或排除旧源代码来隐藏这些错误。全仓遗留错误尚未修复完成。

新增 `tsconfig.control.json` 仅用于独立验证 M0 工作范围，不替代全仓检查。当前涵盖的依赖（含真实 Gateway 认证入口）严格类型检查通过。

## 已实现

- `src/control/contracts.ts`：授权、独立审批、整数配额、CAS、大整数 credit。
- `src/control/tenant_directory.ts`：原生事务内维护目录、版本、幂等回执和审计；失败整体回滚，未知 schema 拒绝加载。
- `src/control/admin_directory_api.ts`：租户列表/创建/更新、审计代理，服务端身份透传与字段白名单；未绑定返回503。
- `src/cloudflare/tenant_directory_cell.ts`：内部专用服务凭据，缺少真实 transactionSync 时拒绝启用，不能使用旧 JSON SQL 模拟器冒充持久事务。
- Gateway 已接入管理路径的认证与 cookie Origin 检查；worker 导出新的 Cell。两个 Gateway 必须绑定同一 `tenant-directory-v1` 权威对象，不允许各自维护目录。

配置需求：`ANSTS_TENANT_DIRECTORY_CELL` 原生事务绑定、至少32字符的 `ANSTS_DIRECTORY_SERVICE_TOKEN`。未创建凭据、未启用开发部署绑定、未重启现有容器。

## 验证结果

`npm run test:control`：严格 TypeScript 检查 + 18 项测试通过。测试 runner 使用临时目录，不修改 dist 或现有数据；SQLite 集成测试需要支持 node:sqlite 的 Node 22+，本次为 Node 24.21.0。

覆盖真实 SQLite 关闭/重开、CAS、幂等重放/冲突、事务写失败、旧/新地域双向权限、只读拒写、内部服务认证、无事务拒绝、忽略客户端伪造 actor，以及真实 Gateway 未认证401/未配置503。

额外 Gateway 回归：`gateway_tenant_authorization` 两项、`celld_unified_gateway` 三项，共5项通过。此前 host proxy 失败已修复：运行时选项遗漏 celldWorkerUrl，导致调用落入本地调度。现恢复显式上游代理、请求体上限、身份/Origin/幂等头透传、响应状态/Set-Cookie 透传与上游失败503；失败不会回落到本地调度。

## 下一步与剩余门槛

1. 继续清理全仓历史缺失导出/接口不一致；host proxy 回归已恢复通过。
2. 验证实际 celld storage 的原生 transactionSync、双网关同权威对象绑定与持久恢复；当前 SQLite 测试不能代替环境验收。
3. 目录已补成本中心归属历史、字段审计差异、按身份与快照版本绑定的游标分页。快照变化返回409，避免跨页混读；每页最多100条。已设置10000条审计/8 MiB快照上限，容量达到时拒绝新增写入但仍允许幂等重放。后续仍需独立成本中心实体、行级存储/归档与扩容方案。
4. 通用审计补齐差异、操作查询、归档和安全去重水位，完成 CP-04 剩余验收。
5. CP-05 已新增 ui/console/lib/control.ts，共享传输支持游标、取消、错误分类、写入幂等键且不自动重试；冲突不修改调用方表单。尚需真实页面接入和浏览器验证。CP-06 指标鉴权/路径修复未开始。

## 隔离 celld 验证记录

使用与开发环境一致的 ghcr.io/denoland/celld:0.6.3 镜像，独立 /tmp/ansts-directory-probe 项目、独立本地端口与本地存储，未连接既有 Fleet 或读取其秘密。构建成功、SQLite binding 被识别，但最小 /ping 和目录调用均返回502；即使增加入口异常响应，仍未进入可验证业务响应。此结果不能用于认定 transactionSync 支持或不支持。临时 ansts-m0-directory-probe 容器已删除，现有开发服务未修改。后续需先解决该隔离运行时的入口/路由问题，再执行重启与双网关权威验证。

当前目录 Cell 已从 scheduler worker 与实际 gateway_worker 两个入口导出；未添加运行部署绑定。历史记录缺少归属时间时保留 null，不用当前时间冒充历史发生时间。

## 原生 celld 验证更新

此前502已定位到测试 HTTP 客户端继承的代理路径；显式使用 urllib ProxyHandler({}) 直连后，最小 worker 与真实目录 Cell 均返回200。

新增可重复执行命令：`python3 scripts/verify_directory_celld.py`。该脚本创建独立随机名称的容器、临时项目与本地随机端口，使用当前用户运行以避免 root-owned 临时文件，不挂载现有运行环境或秘密，结束时自动清理。

实测通过：原生 transactionSync、错误服务凭据401、目录首次写入、容器重启后恢复、同幂等键返回原结果且审计仅一条、不同键过期版本409。脚本最后一次执行退出码0。故不再将原生事务支持列为未知。CP-02仍需验证双 Gateway 绑定及升级恢复流程；未启用当前开发部署绑定。

## Gateway 并发权威验证

隔离脚本已扩展为两个独立的 DirectoryTestIngress Durable Object，均调用实际 handleUnifiedGatewayRequest（JWT 认证、管理 API），并绑定同一个 tenant-directory-v1。此处验证的是两个独立对象入口，不是两个物理 Gateway 部署进程或跨故障域 HA。

验证结果：两个入口使用同一 Idempotency-Key 并发创建，返回同一结果且只增加一条审计；使用不同键并发更新同一个 expectedVersion，状态码为200和409，仅一次更新提交。原生事务、重启恢复、身份拒绝和历史重放继续通过。最后一次脚本执行退出码0。

测试发现 celld 不接受同一 class 的重复 namespace 别名登记；测试配置已改为唯一 ANSTS_TENANT_DIRECTORY_CELL 绑定。readiness 检查增加容器存活验证，失败时输出隔离容器日志并清理，避免对退出容器反复等待。剩余门槛仍为实际双进程部署接入、升级恢复和各领域任务。

## 远程目录接入路径

新增 ANSTS_DIRECTORY_URL：Gateway 可通过单一受信服务 origin 调用 `/internal/v1/directory/{list,audit,mutate}`，携带独立 ANSTS_DIRECTORY_SERVICE_TOKEN。显式配置远程地址时不会退回本地 namespace；HTTP 错误原样返回，网络错误503，写操作只发送一次，禁止重定向和包含凭据/路径/查询参数的 origin。

Fleet worker 已增加上述内部路由，先验证专用凭据，再转交唯一 tenant-directory-v1。主 Fleet Wrangler 配置新增 v4-tenant-directory SQLite class 迁移；Gateway 启动脚本支持从独立 secret 文件读取目录凭据及远端地址。未生成或复用现有秘密，也未更改运行中容器配置。Fleet 侧仍需由部署流程注入相同专用凭据。

`npm run test:control` 严格类型检查与20项测试通过；Gateway 统一入口回归3项通过。新增真实 HTTP 测试验证503时只调用一次且不回退本地，以及不安全 origin 在发送前被拒绝。实际双进程部署及内部路由端到端验收尚待完成。

## 三个真实进程的远程目录验收

新增命令：`python3 scripts/verify_directory_gateways.py`。使用实际 src/cloudflare/worker.ts 与 gateway_worker.ts，构建并启动一个目录权威 celld 进程和两个独立 Gateway celld 进程。三个进程使用独立本地 SQLite 状态、随机独立 Docker 网络及端口，无现有环境/凭据挂载，结束自动清理。

最后执行退出码0，验证通过：匿名401；双入口同键并发创建仅一次提交；同版本竞争更新一成功一409；审计恰好两条；其他租户 Viewer 得到空目录；权威进程重启后审计不变；权威停止时读写均503；恢复后没有离线写入或额外审计。

该证据已覆盖真实多进程远程接入，取代此前仅对象入口级的验证边界。它不等于现有开发 Fleet 已完成部署，也不等于跨地域/故障域容灾。仍需部署注入专用凭据与启动配置、既有 Fleet 升级验证及 CP-02 其余验收。

## 租户目录页面与认证传输

新增 ui/console/components/tenant-directory.tsx，嵌入现有租户管理页。复用 hrui 表格、表单与按钮，支持登记/编辑、下一页/首页、归属历史、最近读取时间、服务不可用、保留旧数据与版本冲突保留输入。只有可写 SRE_ADMIN 显示管理操作；服务端继续负责真实授权。停用目录记录不冒充已实现执行准入限制。

Console 新增管理 fetch 适配，沿用当前 Bearer 凭据及会话取消信号；Cookie 登录仍使用 same-origin。创建与更新显式传递幂等键，相同未确定请求重试保留键，输入变化时生成新键。

验证：ui/console TypeScript检查与 Next生产构建通过；npm run test:control 严格检查及21项测试通过。scripts/verify_tenant_directory_ui.mjs 浏览器验收通过：Bearer转发、未接入禁写、409保留输入、登记成功、390px窄屏及无页面异常。所有/v1请求均拦截模拟，未修改实际任务/租户；截图 docs/validation/admin-platform/tenant-directory.png。实际后端链路由隔离三进程脚本另行验证。

CP-05尚未全部完成：需真实部署绑定后的浏览器联调、只读/操作员矩阵及更多错误恢复场景。

## 成本中心实体与归属校验

目录新增持久 CostCenterRecord，包含稳定ID、名称、ACTIVE/DISABLED和版本。全局范围 SRE_ADMIN 可通过 POST /v1/admin/cost-centers 与 PUT /v1/admin/cost-centers/:id 管理；GET 采用同身份/快照游标分页。租户范围身份只能看到其可访问租户引用的成本中心。

成本中心事务保存版本、幂等回执和独立审计（原因、身份、请求、时间、前后版本与差异）。与租户审计分别保存，后续需统一查询/归档接口。新租户或归属变更必须引用已登记且 ACTIVE 的中心，未知/停用返回 COST_CENTER_NOT_ACTIVE。停用不改写历史，也不阻止已有归属租户修改非归属元数据。旧快照缺少中心实体时保留原始归属，不伪造注册记录；管理员需显式登记才能用于新归属。

验证：npm run test:control 严格类型检查及23项测试通过；原生 celld 单进程/多对象验证和三进程远程链路验证通过（两套脚本已加入中心登记步骤）。覆盖未知/停用拒绝、已有归属保留、全局写权限、范围内读取、CAS及同键重放。成本中心管理界面与统一审计查询仍待完成，当前租户表单的成本中心输入须使用已登记ID。

## 成本中心页面与归属选择

新增 CostCenterDirectory 组件，复用现有 hrui 表格和表单。全局范围 SRE_ADMIN 可登记、编辑、停用成本中心，显示版本与变更原因；其他角色仅查看授权目录。User 展示类型补充可选 scopeRegion，用于判断全局管理入口；权限依旧由后端验证。

租户表单改为已登记中心选择器，新增归属只提供 ACTIVE 项；历史未登记或已停用归属保留明确标记，不静默替换。选择列表按游标读取全部授权中心，最多100页且遵守快照冲突拒绝，不把首批数据冒充完整目录。没有可用中心时禁用登记租户，先登记成本中心。

验证：Next生产构建与前端类型检查通过；控制面严格检查及23项测试通过。真实浏览器（模拟API）通过中心登记/停用、停用中心不出现在新归属选项、租户创建、409保留输入、Bearer转发、窄屏及 VIEWER/OPERATOR/SRE_ADMIN 入口矩阵。截图已更新，未修改真实数据。部署后的浏览器联调仍待执行。

## 统一管理审计查询

GET /v1/admin/audit 默认合并租户与成本中心事件，支持 domain=all/tenant/cost-center、limit和cursor。记录提供稳定ID、目标、操作者、时间、原因、请求ID、前后版本与差异；旧记录没有差异时界面明确说明。游标绑定身份/作用域、对象筛选与数据快照；跨筛选或快照变化返回409。

服务端仅允许SRE_ADMIN查询。租户范围身份仅返回其可访问租户事件；成本中心审计只对全局范围管理员可见。租户和成本中心写入仍各自与其审计及幂等回执同事务提交；查询统一不意味着跨对象分布式事务。

新增 ManagementAuditPanel，嵌入审批/审计页面，提供筛选、分页、保留旧数据、差异与请求详情。浏览器发现旧CSS在窄屏隐藏2/4/5/6列，已对三个新管理面板使用完整表格+容器横向滚动，保留版本、原因和管理按钮。

验证：前后端类型检查和Next生产构建通过；24项控制面测试通过；原生celld与三进程远程脚本通过。三进程统一审计计数现包括一条成本中心登记及两条租户变更，共3条，重放/失败/离线写入不增加。mock浏览器通过审计分页/筛选/详情、三角色目录流程与手机编辑入口；截图management-audit.png。尚需操作状态查询、归档与真实开发部署验收。

## 持久操作回执查询

租户与成本中心成功写入返回 x-ansts-operation-id，由领域、目标ID和幂等键确定，可在重放后保持稳定。GET /v1/admin/operations/:id 从持久回执查询 SUCCEEDED、目标、提交版本、审计请求及时间；不会把对象后来版本当作原操作结果。Gateway、远程权威和旧host代理均透传回执头，客户端保存该元数据并在成功提示中显示。

查询仅对SRE开放；租户操作验证回执时地域与当前目标地域的权限，成本中心操作要求全局范围。不在授权范围与不存在均返回404，避免通过操作ID发现其他租户数据。404仅表示当前查询未找到/不可见，不构成未执行证明；超时重试仍须使用同一幂等键与内容。

审计页新增操作ID查询表单。验证：前后端严格检查、生产构建与26项控制面测试通过；三进程脚本验证旧提交版本、越权404、重启后回执一致；mock浏览器验证查询结果和现有目录/审计流程。CP-04还剩归档、安全去重水位与真实开发部署验收。

## 事务归档、完整性与去重保留

新增原生SQL归档表和请求回执表：POST /v1/admin/archives 带expectedVersion/reason/Idempotency-Key，事务保存完整目录快照、SHA256摘要、源版本、两类审计水位、回执数量、操作者/原因/请求和时间。创建前CAS，返回前读回校验；同键同内容重放返回原清单，不因后续目录变更重新归档。GET /v1/admin/archives 查询清单与当前源版本，GET /v1/admin/archives/:id 导出校验后的manifest+snapshot。所有归档操作仅全局范围SRE_ADMIN允许。

保留策略明确为 ALL_RETAINED，safeReplayWatermark=null；不会清理任何幂等回执、去重键或历史审计，不会用零点或TTL让旧操作再次执行。最多100个归档，达到上限拒绝新增。快照仍保留原8MiB/审计容量限制，不以删除记录绕过限制。此归档位于同一个Cell数据库，是可导出的检查点，不等同于独立存储备份或跨故障域容灾；外部归档存储、清理和恢复运维仍需后续工作。

验证：严格类型检查及29项测试通过，覆盖摘要损坏、幂等冲突/CAS、旧目录自动加归档表且旧回执不丢、归档/请求回执写入失败整体回滚。三进程脚本通过全局归档创建/导出、受限SRE拒绝、权威重启后归档一致、既有并发和故障恢复。目录控制台归档操作入口及真实开发部署验收尚待完成。

## 归档管理界面与下载前校验

审计页面新增 DirectoryArchives 面板，仅全局SRE可见，支持查看源版本/检查点数量/原因、创建检查点、校验后下载。只读会话不能创建；409保留原因并允许刷新源版本后重新提交；未配置或服务错误时禁用创建，保留旧清单。界面明确同库检查点需另存独立存储，不提供删除或清理回执按钮。

客户端 verifyDirectoryArchive 在生成Blob前检查归档ID、schema版本、快照版本、字节数（最大8MiB）及SHA256；失败不触发下载。摘要对JSON快照计算，导出文件同时包含manifest与snapshot。

验证：UI类型检查和Next生产构建通过；30项严格控制面测试通过，新增UTF8/中文/emoji快照与同长度内容篡改拒绝测试。mock浏览器验证归档409输入保留、刷新重试、成功下载文件名/JSON内容，以及篡改归档不触发额外下载；原有三角色目录、审计、操作查询与窄屏回归继续通过。真实开发部署、独立归档存储与恢复演练仍未完成。

## 真实开发部署验收通过

完整记录见 ../validation/admin-platform/deployment.md。安全重启完成，118条原任务与规格摘要保留。真实双Gateway目录API、浏览器目录/操作查询/归档下载、主机文件备份校验均通过。验收元数据均停用，未创建任务或调用付费模型。CP-02按原验收条件完成，其余任务继续；独立故障域恢复属于后续演练，不能用本机文件备份代替。

## CP-06 指标端点与真实采集恢复

新增独立监控凭据，与用户/Body/目录凭据分离。Gateway /metrics 提供按固定route/method/status_class分类的请求计数与耗时；Daemon /metrics 提供执行槽位、配置资源预算与预留、活跃执行、Drain/恢复状态、结果outbox文件数及最旧持久资源预留年龄。指标没有tenant/task/prompt标签；资源预算不是CPU利用率，多个Daemon预算不能直接相加为物理宿主机容量。

Prometheus两个job使用Bearer credentials_file。主监控key为私有文件0600；给非root Prometheus的只读副本位于0700私有目录下，文件0444以便容器指定用户读取；仅该文件只读挂载，不暴露其他秘密。Gateway与Daemon仍执行原业务授权。

已构建新镜像并安全重启，118条原任务保留。`node scripts/verify_monitoring_deployment.mjs` 实测通过：4/4目标up、匿名指标401、专用凭据200、监控身份不能创建任务/切换Drain且状态未改变。控制面32项测试通过；Daemon接口及LayerFS取消2项回归通过。Daemon及其依赖目标严格类型检查通过。

类型检查另发现Body调用LayerFS push/pull时原有第三个signal参数不匹配且未被使用，已修复可选AbortSignal透传并测试取消不继续导入。这项源码修复发生在本次Body镜像构建后，尚未部署；下次构建必须纳入。CP-06仍需补齐准入结果指标并完成最终验收。

## CP-06 验收完成

Daemon新增固定枚举准入计数，持久journal准入时计accepted、缓存结果返回计replayed，各拒绝分支在首次响应时按固定原因计数；执行完成不再重复计准入。真实Daemon协议测试覆盖一次准入/一次重放/一次grant拒绝各计1，非法请求与容量类别测试通过。统一 test:control 已纳入Daemon及LayerFS的严格类型检查和测试，共37项通过。

新增 publication_observability 服务端采集/校验：要求部署配置指定的全部目标唯一、up且15秒内新鲜，缺失/重复/过期/异常证据均拒绝。使用真实Prometheus数据收集并验证，删除任一目标时拒绝。CF-02发布流程后续必须调用此门禁；不能将门禁基础模块完成等同于配置发布已实现。

已重建并安全更新实际镜像，LayerFS signal签名及取消透传已纳入。实际4个目标up，PromQL准入计数存在，监控身份不能执行业务操作；118条任务与规格保留。验证命令 node scripts/verify_monitoring_deployment.mjs 已包含真实观测证据门禁检查。CP-06按原验收完成，其他任务继续。

## M0 六项按原要求验收完成

对照见 ../validation/admin-platform/m0-acceptance.md。新增运行契约与11接口schema目录；三角色×8范围矩阵、跨UTC日/月幂等与敏感键不落审计测试通过。统一严格检查40项通过；UI typecheck/生产构建及键盘/401恢复浏览器通过。归档独立故障域恢复与容量扩展归VA后续验收；全仓历史类型问题仍在整体未完成范围，不因M0完成而忽略。

## M1 / QF-01 开始

新增 src/finops/tenant_policy.ts，定义版本化租户配额策略、CPU milli-core/MiB/并发/队列整数上限、内部credit字符串预算和0～10000基点软阈值、UTC日/月窗口。拒绝不安全整数、浮点credit、错误版本与非UTC策略时间，明确此模型尚未接入执行准入。

统一严格检查42项通过（原M0验收40项，加QF-01两项）；覆盖整数边界与UTC跨日/月/年。持久策略版本/CAS、API、界面和准入快照为后续实现内容。M0完成不代表后续31项完成，也不代表全仓历史类型错误已修复。

## QF-01 持久策略与管理API实现（尚未部署）

新增 TenantPolicyStore / TenantControlCell，每个租户使用 tenant-control-v1:{id} 的独立原生事务权威。保存不可变策略历史、目录元数据版本/地域/成本中心快照、审计和哈希幂等回执；CAS更新、不同内容同键409，停用租户拒绝新版本，既有版本不被降低额度覆盖。只读身份可读授权范围，历史地域也经过校验；错误绑定到另一租户的状态拒绝加载。

源码新增 GET/PUT /v1/admin/tenants/:id/quota。Gateway白名单提取策略字段，actor来自已验证身份；Fleet在创建租户控制对象之前验证目录中该租户存在且可见，控制对象再次获取目录元数据，不接受浏览器伪造region/metadata。新增v5-tenant-control绑定迁移。所有响应明确 enforcement=NOT_CONNECTED，没有把存储策略宣称为已强制执行。请求定义见 quota.schema.json。

严格检查46项通过，覆盖真实SQLite重启、幂等/CAS、降低限额保留旧快照、角色/地域/租户边界、错误绑定、写失败不消耗幂等键及恶意路径/身份字段拒绝。三进程原生脚本新增真实Quota API并发同键只创建一次、过期版本409、越权404/Operator拒写和权威重启后策略一致，最后执行退出0。完整Compose静态校验通过，尚未重启/部署本批Worker。

QF-01仍需历史分页、管理界面和实际部署；QF-02/QF-08负责元数据激活/准入生命周期与执行侧启用。当前配额只作为管理策略保存，未接入运行任务。目录归档目前只覆盖目录数据，不包含新的租户策略对象；策略备份/恢复必须随后扩展，不能用M0目录归档替代全系统备份。

## 2026-11-16 QF-01 历史分页、管理界面与真实部署验收

历史接口新增1～100条分页及绑定身份/范围/租户目录版本/历史快照的游标；身份或快照变化拒绝继续翻页。管理界面沿用现有后台样式，支持资源额度、UTC预算窗口、大整数credit字符串、阈值及原因编辑，版本冲突保留输入；Viewer/Operator只读，停用租户不可写。

严格检查47项、隔离三进程原生并发/重启/分页、mock浏览器权限与冲突回归、UI类型检查及生产构建通过。真实环境安全重启保留118条原任务及执行规格；双Gateway策略创建/重放/CAS/翻页及真实浏览器编辑通过，4/4监控目标在线。验收元数据已停用，没有创建任务或调用付费模型。记录见 ../validation/admin-platform/quota-acceptance.md。

本次补齐管理切片；QF-01保持in_progress，因为旧任务的执行策略/费率快照与新增准入限制尚未接入。响应仍明确NOT_CONNECTED；独立策略Cell尚不在目录归档范围，不把目录备份当作策略备份。

## 2026-11-16 QF-02 租户侧持久预留内核

实现原生事务预留、规范请求摘要、不可变策略快照、CAS/幂等、fencing/generation、正常阶段与UNCERTAIN/撤销状态。所有未终结占用跨UTC保留，启动授权后不能因TTL释放；已经确认运行的任务不能用未启动证据退款。缺少成本保持SETTLING，不伪造零成本。

严格检查58项通过；新增11项覆盖逐阶段SQLite恢复、失败回滚、跨期/降额/晚到成本与四线程独立连接并发不超发。尚未接入TenantControlCell路由、Scheduler/Body、认证证据及持久消息补偿；没有更改真实执行路径或重启容器。QF-01/QF-02均保持in_progress。详细证据与后续条件见 ../validation/admin-platform/admission-foundation.md。

## 2026-11-16 QF-02 影子服务、认证凭证与真实恢复

TenantControlCell/Fleet已接入五个内部影子预留接口；协调服务身份和签名密钥独立，目录凭据不能调用准入操作。固定用途HMAC凭证绑定目标、task/attempt、策略/目录/预留版本、资源及代际；校验要求最新权威状态。执行确认/结算/带证据请求在可信适配器接入前拒绝，不接受浏览器自报终止。

严格检查65项和隔离三进程原生验收通过。修复真实部署发现的密钥路径服务绑定错误，并把绑定/文件/权限校验加入重启前置检查。真实影子创建、重放、超额拒绝、签发/目标验证及UNCERTAIN撤销通过；实际Fleet重启后3个入口完全恢复同一UNCERTAIN预留，118条任务规格保留，4/4监控目标在线。验收租户/成本中心已停用，未执行任务或调用付费模型。

QF-02仍in_progress：Scheduler/Body、认证容量/终止证据、持久outbox/inbox补偿、队列额度、费率/计量/账本和执行崩溃演练尚待实现。服务保持SHADOW/NOT_CONNECTED。完整记录 ../validation/admin-platform/admission-service-acceptance.md。

## 2026-11-16 QF-02 持久投递与独立影子收件箱

预留/审计/幂等与outbox意图同事务提交，事件ID稳定。新增持久领取租约、有限退避、发送超时、不可变接收回执、同ID内容冲突隔离，以及代际/版本撤销墓碑。旧待投递意图被更高版本取代；网络超时、消息回执和过期不会释放UNCERTAIN占用。停用/过期的新获取意图隔离，查询与撤销可继续。

新增AdmissionShadowMailboxCell及v6迁移，内部admission-dispatch单次向独立对象投递，admission-outbox读取授权范围内最多100条待处理/隔离消息。接收回执只确认记录，不推进RUNNING或结算。旧状态在领取时按稳定ID补建消息，不清理历史回执或pending。

严格检查75项（新增10项故障场景）及隔离原生三进程验证通过。真实发送/接收对象投递、目录/配额浏览器回归通过；实际Fleet重启后3个入口精确恢复UNCERTAIN及pending查询消息，118条原任务与规格保留，4/4监控目标在线。验收元数据停用，未提交执行任务或调用付费模型。

QF-02保持in_progress：自动重投/补偿调度、真实容量/Scheduler/Body与认证终止证据、队列额度、费率/计量/账本及独立归档尚待完成。当前只交付影子投递层，没有将回执伪装成真实执行或容量证明。记录 ../validation/admin-platform/admission-messages-acceptance.md。

## 2026-11-16 QF-02 自动投递、暂停恢复与原生冷恢复

引入持久DeliveryControl及原生alarm：新变更先注册alarm再提交预留；RUNNING保留60秒心跳以关闭空队列/新提交唤醒竞态，外部I/O前注册看门狗，每批最多4条。目录/传输各限3秒，故障按有界策略继续安排唤醒，不依赖平台有限异常重试次数，不释放不确定占用。暂停/恢复为SRE+CAS/幂等，返回原回执及当前配置，旧恢复重放不能覆盖新暂停。自动服务身份限定当前租户，消息确认计数不冒充执行计数。

严格检查81项，新增6项自动调度测试覆盖注册失败、暂停/CAS/重放、8轮目录故障、接收重试及空闲心跳。原生三进程强制终止证明到期alarm的处理时间在停止后、首次查询前。真实开发自动投递及浏览器回归通过；暂停配置及队列跨实际Fleet重启保留，恢复后原生alarm自行完成释放消息，验收控制器再暂停。118条原任务与规格保留，4/4监控目标在线。

QF-02仍in_progress：自动消息投递已完成该切片，真实容量/Scheduler/Body、认证终止证据与补偿状态推进、队列额度、费率/计量/账本及独立归档仍未实现。全部服务保持SHADOW/NOT_CONNECTED，没有执行真实验收任务或调用付费模型。记录 ../validation/admin-platform/admission-automatic-acceptance.md。

## 2026-11-16 QF-02 实际 Body/Engine 观察事实

实现持久Host执行观察、独立用途签名与租户事实验签接口。绑定不可预测请求挑战、输入摘要、资源/策略快照及代际；保留规范化事实和幂等回执，拒绝乱序复活。Engine正证进入RUNNING/SETTLING，确认终止释放资源额度，未定价预算继续占用。修复创建响应丢失且删除404时的错误清理确认，保留STOP_UNCONFIRMED；缺失事实不视为未启动。

严格检查91项及4项生产布局旧执行回归通过。隔离的真实celld Scheduler/Body、原生Daemon及实际Docker Engine联合证明租约/grant、启动/终止验签、预算保留及Host/权威恢复，不使用模拟Scheduler/Engine。新镜像构建部署完成，118条原任务规格保留；双Body证据端点鉴权/UNKNOWN、浏览器与原UNCERTAIN/pending恢复及4/4监控通过。

QF-02仍in_progress：BodyAgent自动绑定/证据outbox、真实容量授权与执行前门禁、agent及长ID兼容、队列/费率/用量账本和独立归档尚待实现。观察不是启动授权，不把接口存在冒充自动执行接线。详见 ../validation/admin-platform/body-execution-evidence-acceptance.md。

## 2026-11-17 QF-02 Host 观察 outbox 与历史长身份

修复 task/attempt 超过 128 字符时无法进入新协议的问题，准入、消息和观察统一接受既有 Body attempt 的 512 字符边界。Host 最新事实与待投递意图同文件原子落盘，摘要确认、持久有限退避、新事实替代及旧确认冲突保护已接入；BodyAgent 独立循环在长执行期间向 Fleet Host 专用入口投递，入口从已认证 Host 和签名事实生成租户限定 actor，再由租户权威验签和核对绑定。

严格检查与97项测试、300模块构建、隔离真实celld/Daemon/Engine验收通过。终止回执丢失后跨Host/控制对象重启重放成功，资源释放、未定价预算保留；本批尚未重新部署开发集群。全仓库lint受旧无扩展名导入与检查器解析不兼容阻塞，未宣称全绿。

QF-02仍in_progress：自动创建/绑定观察上下文、agent观察、容量授权/执行门禁、队列与费率/用量账本、独立归档及真实开发部署仍待完成。隔离脚本仍建立观察预留，不把自动relay误认为自动准入。详见 ../validation/admin-platform/body-observation-outbox-acceptance.md。

## 2026-11-17 QF-02 自动观察绑定、agent事实与实际部署

Host专用绑定查询从已认证身份与租户权威解析既有启动授权，校验task/attempt/fencing、Host、输入及资源，不创建预留或返回执行授权。BodyAgent自动传入已有绑定，影子无绑定/服务失败记录明确事件；不默认创建零成本预留。agent观察覆盖等待模型与工具运行，正常结束记TERMINATED，Engine停止不确定保留UNCERTAIN；观察不替代token/费用证据。

严格102项、301模块构建、隔离实际生产BodyAgent脚本+celld+Daemon+Docker Engine通过：长ID自动查找、执行、终止事实自动投递和确认，无人工relay或真实付费模型。上批Host outbox与本批代码已构建部署到开发集群；118条原任务及规格保留，原始Drain恢复，3入口历史UNCERTAIN/pending精确恢复、双Host新接口、真实浏览器及4/4监控通过。

QF-02继续in_progress：自动创建有可信成本上限的预留、真实容量授权/执行前硬门禁、队列/费率/用量账本、独立归档及全过程故障验收仍未完成。服务保持SHADOW/NOT_CONNECTED。详见 ../validation/admin-platform/body-agent-binding-acceptance.md。

## 2026-11-17 MK-01 物理 Host/容量池权威完成

新增全局CapacityInventoryCell持久维护物理Host唯一归属、Pool/Daemon绑定、管理员上限与报告；未来每池授权权威以该目录为前提。宿主机machine-id只读挂载，派生稳定摘要身份；本机签名报告使用先落盘epoch/sequence，重复包不刷新时效、旧代际拒绝、同标识冲突持久隔离。同Host多个Daemon不叠加总量，失联/未报告/排空/恢复未知均归零可借容量而保留占用。管理写入RBAC/CAS/幂等/审计同事务。

严格111项、308模块构建、隔离真实celld双Gateway并发/重启/报告过期通过。v7迁移及新镜像已实际部署；真实双Daemon自动报告共同物理身份，登记为一个本机开发Host。登记后再次安全重启保留118条原任务规格，Host/Pool版本和归属保留，两路epoch递增且自动恢复新鲜报告；原UNCERTAIN/pending、浏览器与4/4监控通过。未发放grant、执行任务或调用模型。

MK-01按原条款标记completed，MK-02～MK-06仍待实现。配额执行仍未启用，目录明确allocation=NOT_CONNECTED，不把硬件目录当作容量授权/计费。完整记录 ../validation/admin-platform/capacity-inventory-acceptance.md。

## 2026-11-17 QF-03 用量源、不可变投递与接收内核

开始QF-03。新增独立签名UsageEvent与原生BodyUsageJournal：首次执行前记录分配窗口，按UTC切分nano-core×ms、MiB×ms、slot×ms，避免将配额milli-core舍入量当成Engine精确用量。provider请求意图先持久提交，DeepSeek响应token在模型action解析前记录；响应丢失/字段缺失保留UNKNOWN/null，不导入预算估算。事件及outbox同文件原子保存，序号跨重启不断序，有序确认保留原事件、持久退避与隔离。

新增SQLite UsageInbox接收内核，执行绑定、连续序号/窗口及provider请求关联校验，事件与水位事务提交；冲突保留原数据、隔离并产生持久requiresAttention记录。严格123项、312模块构建、真实celld/Daemon/Engine与本地HTTP供应商夹具联合用量验收、5项旧agent实际工具/中断恢复回归通过。

QF-03保持in_progress：仅隔离选项开启计量；Fleet鉴权接收/Host自动用量投递、租户查询分页、实际告警、无结构旧执行缺失标记及部署仍待实现。没有部署本批或启用真实用量计费/硬配额。详见 ../validation/admin-platform/usage-producer-acceptance.md。

## 2026-11-17 QF-03 自动用量服务与真实部署完成

完成执行前持久绑定、Host 鉴权签名入口、TenantControl 不可变接收及生产 BodyAgent 自动投递。已提交但丢回执可重试，旧 Body 证明退役不阻塞已绑定历史流。租户查询支持权限、冻结水位和签名游标；无结构旧执行只记录 slot，未知 CPU/内存不补零。四条低基数告警及正负规则测试通过。

严格128项、315模块构建、5项实际Agent回归与原生实际celld/Daemon/Docker/本地HTTP模型响应验收通过。镜像已部署并两次安全重启：118条原任务及规格保留，新增1条专属真实无网络容器验收，用量自动入账后重启原文/接收时间/上下文/分页顺序一致。双Gateway、3入口历史UNCERTAIN/pending、容量目录/epoch、真实管理浏览器和4/4监控通过；未调用付费模型。

QF-03按自身用量条款completed，计划共8项完成、2项进行中、27项未开始，剩余29项。QF-01/QF-02继续in_progress；计量绑定不替代执行授权，定价/账本/硬配额未开放。计量默认关闭，开发仅显式试验租户shadow白名单，试验元数据已停用。详见 ../validation/admin-platform/usage-service-acceptance.md。
