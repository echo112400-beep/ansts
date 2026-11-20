# M0 目录部署验收

日期：2026-11-15。真实开发入口 http://127.0.0.1:8088 。本记录仅证明 M0 已验项目，不能代表37项任务全部完成。

## 部署与保留

使用完整4份Compose配置及 scripts/restart_dev_cluster.py --skip-build。先Drain执行器、保存任务快照，再按存储/Fleet/Worker/执行器/Gateway/Next/Nginx顺序恢复。未删除卷；118条原任务的ID、租户与执行规格摘要一致，原始Drain状态恢复，验收后当前任务数仍118。

目录服务凭据独立生成并保存在忽略的本机secret文件，未复用用户签名或Body凭据。Fleet部署脚本只注入目录服务凭据，两个Gateway从同一secret文件读取并访问Fleet目录路由。两类启动脚本使用umask077，不公开凭据内容。实际Worker部署包含v4-tenant-directory binding迁移。

## 实际执行证据

| 命令/检查 | 结果 |
| --- | --- |
| restart_dev_cluster.py --check-only | 完整配置通过 |
| restart_dev_cluster.py --skip-build | 完成，118条任务保留 |
| Gateway 01/02 /readyz | 均200 |
| verify_directory_deployment.mjs（启用Playwright） | API与真实浏览器验收通过 |
| 短期测试身份 | 15分钟；凭据只留内存，不写公开日志 |
| 原生/三进程验证脚本 | 此前通过事务、重启、并发、离线拒写与权限 |

实际API覆盖成本中心创建、租户创建、第二Gateway同键重放、版本更新、过期CAS409、Viewer隔离与拒写、旧操作回执版本、租户审计、归档创建/导出与摘要验证。真实浏览器读取新租户，随后查询旧操作，并下载真实归档；下载JSON与API返回内容一致。

使用 m0-verification-* 命名的验收元数据，没有创建任务或调用付费模型。验收后租户和成本中心均设置DISABLED，审计和回执按设计保留。备份导出到 .dev-secrets/backups/，文件权限0600，摘要已校验；这是主机文件备份，不是跨主机/跨地域容灾证明。

截图：[租户目录](live-directory.png)、[审计与真实归档](live-audit-archive.png)。

## 仍未完成

独立故障域备份恢复演练、目录容量扩展、全仓历史类型错误以及后续配额计费/配置发布/内部容量/地域治理任务仍需继续。未启用任何幂等回执清理，也没有把当前工作标为整体完成。

## 2026-11-16 配额管理更新

新增v5-tenant-control迁移后再次安全重启，118条原任务及执行规格保留，原始Drain状态恢复。真实双Gateway配额API、浏览器编辑与历史分页通过；4/4 Prometheus目标在线，专用监控身份不能执行业务操作。详细范围见 [配额验收](quota-acceptance.md)。策略执行准入尚未启用，目录归档不包含独立租户策略Cell。

## 2026-11-16 影子准入服务与恢复

实际Worker已部署影子预留内部服务和独立认证凭证，真实流程验收通过。安全重启后，三个Fleet入口均读回相同UNCERTAIN影子预留，118条原任务及执行规格保留，原始Drain状态恢复，4/4监控目标在线。新增部署前检查防止把准入密钥路径配置到公开Gateway。记录见 [影子准入服务验收](admission-service-acceptance.md)。没有创建实际执行任务或启用硬配额；持久消息补偿和真实执行证据适配仍未实现。

## 2026-11-16 持久消息与影子邮箱迁移

v6-admission-shadow-mailbox已部署，真实独立对象间的影子投递与原管理浏览器回归通过。实际Fleet重启后，3个入口精确读回UNCERTAIN预留及未投递查询消息；118条原任务与规格保留，原始Drain恢复，4/4监控目标在线。详见 [持久消息验收](admission-messages-acceptance.md)。没有实际任务执行，自动重投/真实容量和执行补偿仍待接入。

## 2026-11-16 自动投递与持久控制恢复

原生alarm自动影子投递已部署。真实自动投递、暂停配置与待释放消息跨Fleet重启、恢复后自动续投通过；控制器已再次暂停，验收元数据停用。118条原任务与规格保留、原始Drain恢复，原UNCERTAIN/pending快照仍由3个入口精确读回，4/4监控目标在线。实际进程强制终止仅发生在独立测试项目，未强制终止开发任务。见 [自动投递验收](admission-automatic-acceptance.md)。真实Engine/容量补偿仍未接线。

## 2026-11-16 Body 事实接口与新运行镜像

新Daemon/Engine观察代码及租户验签接口已构建部署，118条原任务规格保留，原始Drain恢复。两个实际Body端点严格要求Host身份，UNKNOWN不生成未启动证明，监控身份拒绝；浏览器与既有UNCERTAIN/pending恢复、4/4监控均通过。实际正证与Docker运行的联合证明来自隔离原生项目，生产BodyAgent自动证据回传仍未接线。见 [Body观察验收](body-execution-evidence-acceptance.md)。

## 2026-11-17 Host outbox、自动观察绑定与 agent 生命周期

两批代码已构建部署：Host持久观察outbox与自动relay、Host只读绑定查询、生产BodyAgent自动传入已有上下文、agent生命周期事实。安全重启保留118条原任务和执行规格，原始Drain恢复。双Host新接口、3个Fleet入口鉴权及旧无快照预留UNBOUND、原UNCERTAIN/pending精确恢复、真实配额/目录/审计/归档浏览器回归及4/4监控通过。

真实Docker工具执行和自动签名事实回传由隔离原生项目中的实际生产BodyAgent脚本验证，使用确定性模型适配器；实际开发验收没有执行任务或调用付费模型。缺可信成本的自动预留创建、真实容量授权和执行硬配额仍未开放。见 [自动绑定与agent验收](body-agent-binding-acceptance.md)。

## 2026-11-17 MK-01 容量目录部署及恢复

v7-capacity-inventory迁移与新报告器已部署。两台实际Daemon从共同只读宿主机身份自动报告，本机开发池只计一个物理Host。登记后再次安全重启保留118条原任务规格、池/Host配置版本和成员归属；双报告epoch递增并自动恢复新鲜度，双Gateway读同一权威。旧准入UNCERTAIN/pending、观察接口、真实管理浏览器及4/4监控通过。

容量申请/授权及回收未接入，allocation仍为NOT_CONNECTED；未执行真实任务或调用付费模型。见 [物理目录验收](capacity-inventory-acceptance.md)。

## 2026-11-17 QF-03 用量服务部署

已构建部署用量绑定/鉴权接收、生产BodyAgent自动投递、租户分页及四条用量告警。两次安全排空重启保留原118条任务及规格；专属租户增加一条真实无网络容器任务，自动产生可验证资源数量，随后停用试验元数据。第二次重启保留119条任务，用量原文/receivedAt/上下文/分页顺序一致，双Gateway查询、历史UNCERTAIN/pending、容量恢复、管理浏览器和4/4监控通过。没有调用付费模型，未接入价格或执行硬配额。见 [用量服务验收](usage-service-acceptance.md)。

## 2026-11-17 QF-04 服务部署与派生账目恢复

目录模板/事务审计、租户显式激活、首次执行快照、自动投影及授权分页/汇总已部署。两次安全排空重启保留全部119条任务和执行规格；现有专属租户的派生账目、原接收时间、null历史价格及汇总逐项恢复一致，双Gateway共用权威。模板、激活和新任务均未在开发环境创建。原用量/历史UNCERTAIN/pending、Host观察、容量目录、真实管理浏览器及4/4监控回归通过。

真实定价容器和实际价格pin的正向证据来自独立celld/Daemon/Engine项目，不将开发空费率查询冒充定价执行验收。统一审计合并、独立备份及准入成本快照仍待完成。见 [服务验收](rate-ledger-service-acceptance.md)。

## 2026-11-18 QF-04 审计与独立备份恢复部署

统一模板/租户激活审计、单权威财务快照和离线恢复校验已部署。两次安全排空重启保留119条原任务及规格、原Drain。三份真实快照（目录、既有用量租户、历史UNCERTAIN租户）已下载到忽略的私有宿主机目录，并恢复到新离线SQLite；源摘要、旧空价格、账目、预留/outbox一致。重启后双Gateway读取与私有副本一致；真实审计筛选、原管理流程及用量/观察/预留/容量/4目标监控回归通过。

没有创建开发价格策略、激活或新任务，没有付费模型请求；没有覆盖在线卷或启动离线恢复对象。按QF-04原条款完成，准入硬门禁继续未启用。见 [收尾验收](rate-ledger-recovery-acceptance.md)。

## 2026-11-18 源端 provider 月界修复部署

BodyUsageJournal 在provider时间戳落盘前保存跨过的UTC日界资源片段，修复后续checkpoint时间倒退阻塞。已构建部署并安全重启，119条原任务/规格和Drain保留，未创建新执行或调用模型。原用量、空价格、账目、独立备份、UNCERTAIN/pending、Host观察、容量/epoch和4/4监控回归通过，源usage open/pending/quarantined均0。

QF-05新月结内核仅作隔离原生SQLite验收，未接入Worker/租户关账服务，生产没有创建period表或最终账单；不得将本次源端修复部署误作关账服务上线。见 [月结内核验收](period-settlement-foundation.md)。

## 2026-11-18 财务备份 v2 兼容部署

v2归档能力已构建部署，两次安全排空重启保留119条任务及规格。开发环境没有period数据，新实际快照仍为v1，独立宿主机恢复与重启后双Gateway原文比对通过；原账目、用量、预留/outbox、容量与4/4监控回归通过。真实v2关账/调整正证来自独立celld测试入口，覆盖与月末时钟为夹具，不代表可信执行清单或生产关账上线。见 [验收](period-backup-v2-deployment.md)。
