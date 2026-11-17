# MK-01：物理宿主机与容量池权威验收

日期：2026-11-17。MK-01 已完成本项验收；容量申请/公平队列、grant、回收与管理界面分别属于 MK-02～MK-05，仍未完成。

## 验收要求与证据

| 原要求 | 实现及验证 |
| --- | --- |
| 稳定 hostId、poolId | Daemon 从只读挂载的宿主机 `machine-id` 派生不可逆摘要 ID；拒绝空/无效来源。管理员绑定 pool 与 Daemon，单一全局 CapacityInventoryCell 禁止同 Host 改池、同 Daemon 跨 Host 登记、静默移除旧成员。实际开发两个 Daemon 解析为同一 Host |
| 报告 epoch | epoch/sequence 先原子落盘并 fsync 后签名；重启 epoch 递增。旧 epoch/sequence 不覆盖新占用；重复回执不刷新 receivedAt。真实开发 Fleet/Daemon 重启后，两路 epoch 均递增，Host/Pool 归属及配置版本保持 |
| 管理员上限 | Pool/Host 上限按 CPU milli-core、内存 MiB、slots 分别校验；SRE 全局权限、reason、CAS、幂等、操作 ID 与审计同事务。降低上限保留已有占用并显示超配，不强杀任务 |
| 身份与新鲜度 | Host 服务凭据、独立用途 HMAC、report.daemonId 与认证身份、管理员成员关系四层检查。15秒时效同时约束源时间和服务端接收时间；未来时间、旧代际拒绝。同标识不同内容持久隔离，正确重放或自增 epoch 不能解除隔离 |
| 多 Daemon 不重复统计 | 物理 Host 只计一次，报告是约束而非可相加的物理机器。有效上限取管理员上限与各报告总量的保守最小值；各 Daemon 已知占用相加。原生与实际开发验收均证明双 Daemon 归为一个 Host |
| 失联隔离 | 未报告、过期、恢复待确认、排空或冲突的 Host 不提供新可借容量；最后已知占用保留。原生 celld 实际等待报告过期后可用归零、占用仍在；重放旧包不延长有效期 |

## 已执行检查

- `npm run test:control`：严格类型检查及111项测试通过，新容量测试覆盖原子回滚、双绑定拒绝、上限、排空/恢复、过期/重放、冲突重启保留、签名、源身份和 Daemon 实际本地资源报告。
- `npm run build`：308个模块构建通过。未宣称全仓库类型检查或 lint 全绿，既有导入兼容问题仍另行处理。
- `python3 scripts/verify_capacity_inventory_native.py`：实际 celld、两个独立 Gateway 对象、唯一 SQLite 权威；并发重放只提交一次，CAS 一胜一冲突；双签名报告去重、重启保留和真实过期占用隔离通过。
- `node --check scripts/body_cell_agent.mjs`、`scripts/start_body_daemon.mjs`：通过。
- Compose 前置校验、镜像构建与实际部署通过，新增 `v7-capacity-inventory` 迁移保留原命名空间及状态卷。
- `node scripts/verify_capacity_deployment.mjs prepare`：使用真实 Daemon 资源签名与共同物理身份登记本机开发池；只在目录不存在时创建，不覆盖已有管理员配置。实际 BodyAgent 每5秒自动报告，新目录成为 HEALTHY；未发放授权或执行任务。
- 登记后再次执行安全重启与 `verify-recovery`：118条原任务及规格保留；Host/Pool 配置版本、成员归属保留；双方持久 epoch 递增且自动恢复新鲜报告；双 Gateway 查询同一权威，Viewer 不能访问物理管理目录。
- 历史 admission UNCERTAIN/pending、Host 观察接口、真实目录/配额/审计/归档浏览器回归与4/4监控目标通过。浏览器验收元数据已停用；真实本机容量目录保留，供后续授权任务使用。

## 能力边界

该目录是物理归属和报告权威，不是分配器。接口持续返回 `allocation: NOT_CONNECTED`，不改变原 Scheduler 放置，不创建 grant、不增加租户硬额度、不生成用量或费用。报告中的资源是本机可用预算约束和持久资源预留，不能显示成真实 CPU 利用率或计费数据。

没有报告时不凭管理员上限生成“健康硬件”；初期采用保守约束，可能低于机器真实总量。首期物理目录仅允许全局 SRE 管理，租户容量申请和分摊视图待后续任务。Host/Daemon 迁移及隔离解除必须有独立排空/恢复证据，当前没有自动解除或 TTL 回收。

目录上限为100个池、1000个 Host、每 Host 16个 Daemon，超过容量或缺失事务/绑定时返回不可用。管理审计和回执持续保留，不进行普通 TTL 清理。机器标识摘要依赖宿主机唯一 machine-id；复制宿主机镜像时必须正确初始化独立机器身份。
