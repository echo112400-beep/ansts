# QF-04：统一审计与独立备份恢复验收

日期：2026-11-18。继[内核](rate-ledger-foundation.md)和[服务接线](rate-ledger-service-acceptance.md)后，本批完成审计与独立恢复验证。计价服务用于内部 showback，不执行真实扣费。

## 审计

`GET /v1/admin/audit` 的目录查询包含租户、成本中心和不可变费率模板，`domain=rate-card` 可单独筛选。费率激活来自每个租户的权威记录，使用 `domain=rate-activation&tenantId=…`；没有租户选择时拒绝查询，不将不可达租户静默算作“全量审计”。目录查询返回明确 coverage，控制台“目录内全部对象”和“租户费率激活”分别表达范围。

激活记录保留操作者、请求、原因、版本、费率摘要及元数据版本；后续激活的 before 值来自原激活快照。租户授权、服务凭据和作用域签名游标沿用原有边界。真实浏览器已验证新筛选与旧布局，截图为 `live-finops-audit.png`。

## 独立快照与离线恢复

- 目录：`GET/POST /v1/admin/finops/archives`，`GET /v1/admin/finops/archives/:id`。
- 租户：`GET/POST /v1/admin/tenants/:id/finops-archives`，`GET /v1/admin/tenants/:id/finops-archives/:id`。
- POST 需要 `expectedRevision`、reason 和幂等键；快照、清单及幂等回执同原生事务提交。SRE 必须覆盖完整目标权威；区域受限身份不能导出整租户。

目录快照包含目录元数据、不可变模板、模板审计及业务回执。租户快照包含激活/价格绑定、原始用量/水位/冲突、账目、额度策略、预留/回执、outbox 和投递控制。每份快照是单权威一致读取，不是多个对象同时冻结的全系统备份。历史归档目录、Host/Engine、全局 mailbox 和容量池不包含在本格式中；既有 M0 目录归档继续单独保留。

使用固定表/列白名单、表计数、完整源摘要及独立目的 HMAC，拒绝任意 SQL、错误租户、篡改、缺失事件和不一致价格/审计关系。上限为 100000 行、32 MiB、每权威 100 份归档，超限拒绝而不截断。密钥不写入快照，验证时通过独立密钥文件提供；历史签名须使用对应签名时的验证密钥。

恢复命令：

```sh
node scripts/restore_finops_archive.mjs archive.json verification-key-file new-offline.sqlite TENANT tenant-id
```

目标必须是不存在的新文件，权限 0600；原生事务校验后逐表读回摘要并重算已有账目。失败回滚并删除本次创建的目标，已有目标拒绝覆盖。CLI 不启动 Worker、alarm、Host 或执行服务，没有在线 restore API，输出 `operationalResume=DISABLED`。复制的 UNCERTAIN、预算占用及投递状态不自动释放或重发；在线恢复/身份代际切换仍需其他任务验收。

## 验证证据

严格控制面检查及 **159 项测试**、**323 模块构建**、Console 类型检查通过。覆盖归档权限、版本冲突、幂等、发布失败回滚、非白名单 schema、签名与内容损坏、错误作用域、非空目标拒绝，以及源用量/价格/账目/UNCERTAIN/outbox 精确恢复。

隔离真实 celld、CJS Daemon/生产 BodyAgent、Docker Engine、HTTP 模型夹具联合验收通过。实际定价容器自动投影；目录/租户快照下载到宿主机后，独立 CLI 恢复到新 SQLite 文件，源摘要与临时汇总一致。实际 Fleet 重启后模板/激活/归档字节保持一致；原有未知价格继续 pending，没有付费模型请求。

开发镜像已部署，三份实际快照（目录、原用量验收租户、历史 UNCERTAIN 租户）下载到忽略的 `.dev-secrets/finops-backups/`，独立恢复验证通过。原有 119 条任务和执行规格保留，没有创建开发费率、激活或新任务。

第二次开发重启后，三份归档通过双 Gateway 读取均与独立宿主机副本逐项一致；原用量/空价格/派生账目、三个入口历史 UNCERTAIN/pending、Host 观察、容量目录/epoch、真实管理浏览器及 4/4 Prometheus 全部回归通过。QF-04 按原条款标记 completed。

## QF-04 与后续任务边界

QF-04 的原条款为整数/明确舍入、原始用量重算、未知数据 pending、目录唯一费率写入、显式租户快照和部分汇总完整性；原条款及本批额外审计/备份收尾已完成，QF-04 标记 completed。

准入仍由调用方提供预估额，缺少可信成本上限和真实容量授权，相关自动预留与预算门禁留在 QF-02/QF-08。不把执行时价格补入旧预留，不声称硬配额启用。QF-05 继续负责关账、晚到调整；QF-06/07 负责正式导出与费用界面。跨模块全过程故障与在线恢复属于后续集成验收。

另外修复：即使尚无用量事件，只要存在未结准入预留，成本汇总即标 `UNSETTLED_ADMISSION_RESERVATIONS`，总额为 null；不会将空事件集误称为完整零成本。全仓库 lint 的既有扩展名检查问题未在本批处理。
