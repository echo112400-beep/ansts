# VA-01 跨模块回归记录

本轮在隔离环境运行，未改动共享开发集群。

- `npm run test:control`：230/230。UTC 零点精确分段和序号连续；晚到用量原子生成一次调整，未知价格保持待核算；旧 ACK 不隐藏较新事实；重复 outbox 保持同一结果。
- `npm run build` 和 `node --test dist/tests/celld_unified_gateway.test.js`：构建成功，5/5。真实地域未接通时拒绝地域约束提交，不进入本地队列。
- `ANSTS_VERIFY_SEPARATE_GATEWAYS=1 ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY=1 python3 scripts/verify_capacity_inventory_native.py`：退出 0。独立 Gateway 下验证旧占用、远端断联时不准入，以及启动授权响应丢失、硬停重启与无人重试回收。未取得 Scheduler 未接收证明前保留占用。
- `ANSTS_EVIDENCE_RESTART_WHILE_RUNNING=true python3 scripts/verify_body_evidence_native.py`：退出 0。硬停保留 RUNNING 预留；原任务继续执行，可信成本原子替换预估，重启与独立归档恢复后结算结果一致，旧缺价仍待核算。

契约依据：[PRODUCT](../../admin-platform-plan/PRODUCT.md) 与 [TECH](../../admin-platform-plan/TECH.md)。上述结果尚不证明所有跨对象提交窗口均经独立故障注入；需补齐阶段对照和可复验的账本重算、无超发证据后关闭 VA-01。

独立账本重算：原生 Body 演练按页读取试点 attempt 的费用明细，断言 event ID 不重复；以 BigInt 独立累加 UTC 日期和费用维度的分数，再按本夹具 CEIL 费率规则取整，与实际入账相等。并发重放返回同一结算结果；结算后用精确可用额度提交成功，多 1 子单位的请求被拒绝。默认原生演练整轮退出 0。

Scheduler 接收后丢应答：ANSTS_VERIFY_LOST_ACCEPTANCE_ACK=1 在真实 Scheduler 提交成功后替换响应为 503。硬停前后均只保留一条任务，重放取得同一执行 ID；后续同一槽竞争、资格回切以及已接收任务的启动到期与清理证明依旧通过。整轮退出 0。对应 TECH 的先持久化再确认以及幂等补偿要求。

VA-03 部分证据：ANSTS_VERIFY_COMPOSE_ACCEPTANCE=1 python3 scripts/verify_body_evidence_native.py 退出 0。独立 Compose 项目、动态 loopback 端口、独立临时状态目录与夹具租户验证真实 Body/Engine、账本重算、重启和 v1/v2 归档恢复。没有挂载共享开发数据。此轮是单控制容器夹具，尚不覆盖完整独立 Fleet、全部新控制对象或生产迁移。

VA-03 配置升级复验：隔离 Compose 先以旧绑定启动并产生租户预留，再追加 RegionCatalogCell 绑定与迁移标签重启。通过真实 Gateway 读到新目录为空、写入开发地域后读回 DEVELOPMENT_ONLY；旧租户预留重启后逐字段一致。Worker /healthz 作为 Compose 启动门禁，完整夹具退出 0。此次只验证新增地域对象的配置升级，不代表生产多节点滚动升级完成。

VA-03 升级后持久性：新增地域目录通过 Gateway 写入后，在原生账本恢复步骤再次重启同一隔离 Compose 控制容器；费用汇总与地域目录分别对照重启前快照，均逐字段一致。整轮退出 0。

预留创建丢应答：ANSTS_VERIFY_LOST_PREPARE_ACK=1 与 ANSTS_VERIFY_LOST_ACCEPTANCE_ACK=1 联跑退出 0。租户已保存 PREPARING 后注入 503，Scheduler 清单为空，原预留估算为 5。随后覆盖绑定/启动授权应答丢失、硬停、接收应答丢失及重放，最终仅有一条任务，竞争者仍无法占第二槽。

## 故障窗口对照

- TECH §4/§5，准备到接收：双入口单槽竞争；PREPARING、容量绑定、启动授权、Scheduler 接收的丢应答可重放。硬停后对已接收任务保持原 ID，对未接收任务仅凭权威证明释放。
- TECH §5/§6，执行到结算：真实运行中硬停保留 RUNNING；终止证据丢响应后重投，旧 ACK 不吞新事实。结算并发重放及重启均一致；明细独立重算 7 个事件、5 桶、5 子单位；超过剩余额度 1 单位被拒。
- TECH §6，跨零点和晚到：控制面验证 UTC 分段精确、迟到调整事务回滚及归档重放；缺价保持待核算。
- 尚需把发布协调器重启及旧目标 ACK 的用例与本批运行记录关联；各阶段注入方式有丢应答、硬停和事务失败三种，不能统称逐阶段进程硬停。

TECH §7 发布协调器：tests/control/config_rollout.test.ts 改用文件 SQLite 实际关闭并重开。缺观测暂停后的同一批次可继续；再次重开后，旧代际目标 ACK 返回 STALE_CONFIG_ROLLOUT，新发布记录逐字段不变。控制面 233/233。此用例覆盖持久状态恢复，不模拟远端目标进程硬停。

VA-03 / QF-06 导出恢复：隔离 Compose 的真实 Gateway 自动生成非空 CSV，重启后同一授权范围下载的字节完全一致，其他租户身份下载返回 404。原生组合演练退出 0，未接触共享开发任务或账本。

VA-03 配置发布对象补证：隔离 Compose 在原租户状态上新增 ConfigReleaseCell 绑定及迁移，使用服务鉴权创建草稿；再次重启后，按原作用域读回草稿逐字段一致。定向运行通过该断言，但后续已过期 Body 启动返回 503 BODY_EXECUTION_UNCERTAIN，而非期待的 409；整轮退出 1，不能作为完整 VA-03 通过证据。

VA-03 完整夹具重跑：固定项目独占的本地端口后，ANSTS_VERIFY_COMPOSE_ACCEPTANCE=1（未启用提前退出的过期模式）整轮退出 0。配置草稿、地域记录、旧租户预留和非空 CSV 跨重启一致，真实 Body/Engine 执行、独立账本重算、费率固定与 v1/v2 归档通过。部署范围仍为单控制容器，不把此结果外推到多节点恢复。

VA-03 审计读回：完整隔离 Compose 演练在重启前保存费率激活审计页，重启后逐条对照审计项及后续游标，两者一致。整轮退出 0；和同轮的账本、预留、授权及目录读回一起作为隔离部署证据。

VA-03 控制对象绑定复验：隔离 Compose 从启动起声明生产 Worker 使用的既有控制对象，再升级加入 RegionCatalogCell 和 ConfigReleaseCell。通过 Gateway 对比升级前后的状态响应，逐字段一致。完整演练通过并清理隔离容器和网络，覆盖旧预留、配置草稿、地域记录、账本、导出、鉴权和审计。实际多节点 Fleet 的故障切换与恢复仍待验收。

VA-03 三节点恢复补证：运行 python3 scripts/verify_va03_fleet_recovery.py，退出 0。独立 Compose 项目使用真实生产 Worker、三个 Fleet 节点、独立 RustFS bucket 和节点数据卷。停止写入入口，超过节点租约 TTL 后，由两个存活节点分别验证成本中心幂等回执与审计页；原节点重新加入后再次读到相同审计页。实验清理独占容器、网络和卷。故障注入针对写入入口，未独立确认对象所有权所在节点；多节点下的账本、预留和租户授权仍待补证。

VA-03 三节点逐节点恢复：同一隔离 Fleet 依次正常停止每个节点，等待超过租约 TTL 后在两个存活节点核对目录审计、成本中心幂等回执、租户策略、非空预留及财务归档内容。每轮核对跨租户费用查询返回 404，已停止节点重新加入后读回相同状态。运行时确认 TenantDirectoryCell 和 TenantControlCell 均发生所有权交接。整轮通过并清理独占资源。多节点下的已计价用量账目和非正常退出恢复仍待补证。

VA-03 已计价账本与强制退出：隔离三节点 Fleet 使用生产 Body Daemon 执行真实本地容器，自动计量并生成非空已计价账本。逐节点正常停机和重加入后，两台存活节点及重加入节点读回相同成本明细、正值费用小计、预留、归档与授权结果。随后以 SIGKILL 终止 Fleet 日志中记录的租户控制对象所有者，存活节点以更高 epoch 恢复同一状态，原节点重加入后再次通过核对。验证脚本退出 0 并清理隔离资源。VA-03 技术演练通过，正式关闭仍依赖 VA-02。
