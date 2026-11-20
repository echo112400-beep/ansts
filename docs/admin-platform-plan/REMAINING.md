# 未完成任务清单

状态来源：[tasks.json](tasks.json)。P0 工作树基线已记录，P1 只读月结预览已通过[隔离验收](../validation/admin-platform/period-preview-native-acceptance.md)，P2 的预算入账、自动晚到调整与鉴权关账入口已通过隔离验证。

## 总体状态

任务台账共 37 项，**已完成 37 项、进行中 0 项、尚未开始 0 项**。历史账务核算和真实地域接入另有跟踪记录。

| 阶段 | 已完成 | 进行中 | 未开始 |
| --- | ---: | ---: | ---: |
| M0 | 6 | 0 | 0 |
| M1 | 8 | 0 | 0 |
| M2 | 7 | 0 | 0 |
| M3 | 6 | 0 | 0 |
| M4 | 6 | 0 | 0 |
| M5 | 4 | 0 | 0 |

## 已完成的验证批次

1. **P0 · 已记录当前工作树的运行基线。** 严格控制面测试 195 项、重点回归 45 项均通过，构建、类型检查和 lint 通过。全量测试 322 通过、12 失败、4 跳过，共 338 项。
2. **P1 · 只读月结预览已通过隔离验收。** 实测 Gateway、租户权威、Scheduler 与 Body 接线，覆盖权限、水位变化、稳定版本和扫描上限，并核对预留与关账记录。见[隔离验收](../validation/admin-platform/period-preview-native-acceptance.md)。
3. **P2 · 完成关账依赖。** 已隔离验证可信实际成本、UTC 日月分摊、预留入账及释放，覆盖版本冲突、回滚、重放、重启与归档。见[预留成本验收](../validation/admin-platform/reservation-cost-preview-acceptance.md)。自动晚到调整与鉴权关账已通过[关账与晚到调整验收](../validation/admin-platform/period-close-native-acceptance.md)。独立 Gateway/Fleet 路径已在三个隔离进程中验收。已验证 UNCERTAIN 单独阻断关账。带价格的真实 Docker 取消和失败执行已在隔离环境完成用量入账、非空月关账及重启重放。历史未计量任务仍待处置，人工正负调整已通过隔离验收。

全量基线中的 12 项失败待归因。QF-05 规划任务按用户决定结项；历史关账证据缺口继续记录为待核算账务事项。

## 关键任务结项记录

### QF-02、QF-08 与 M3 已验收

QF-02 的正式验收条款已在准入试点覆盖：准备与启动授权重放、RUNNING 硬停恢复、结算重放、过期及撤销拒绝、UNCERTAIN 保留。QF-08 已验证双入口、单槽、失联拒绝和回切保账。共享开发集群保持 observe。普通租户的强制配额推广、历史清单恢复及真实历史关账不在此次 M3 验收范围。

按用户决定，M3 验收暂视 QF-05 的历史关账前置已满足；QF-05 规划结项已按用户决定完成，历史账务仍待核算，既有账务状态保持不变。见[准入验收](../validation/admin-platform/admission-foundation.md)与[M3 验收](../validation/admin-platform/m3-capacity-allocation-acceptance.md)。

### QF-05：结算、跨期、关账与调整（已完成）

用户决定按2026-10基线加10%完成规划结项，m3-pilot估算月额度341 credit-subunit。该估算不代表历史实际关账。旧月份权威清单和原始计价证据不足，历史账务继续待核算。估算过程见[关账验收](../validation/admin-platform/period-close-native-acceptance.md)。

已实现 UTC 月结内核：冻结输入/水位、完整性校验、CAS/幂等关闭、晚到精确舍入差额和冲突 attention；原生 SQLite 重开一致。修复 provider 跨零点引发资源分段时间倒退，已安全部署，119条任务保留。见[内核验收](../validation/admin-platform/period-settlement-foundation.md)。剩余：

- Scheduler/Body 事务历史、私有覆盖诊断及只读预览已通过[隔离验收](../validation/admin-platform/period-preview-native-acceptance.md)。Body 候选超限时总额保持未知，结算表超限返回 503。大租户的归档与扫描策略仍待确定。
- 内部预算入账及预留释放已通过[预留成本验收](../validation/admin-platform/reservation-cost-preview-acceptance.md)。自动晚到钩子与鉴权关账 API 已通过[关账与晚到调整验收](../validation/admin-platform/period-close-native-acceptance.md)。人工正负调整已通过鉴权、重放和归档验收，历史未计量任务处置仍待完成。
- 财务备份 v2 的 SQLite、真实 celld、旧 v1 兼容及开发部署恢复已通过；真实 Docker 取消与失败的可信计价、重启恢复及非空月关账重放均已通过隔离演练；并发关账返回同一凭据。历史未计量数据处置及实际开发 Fleet 关账部署仍待核验。

QF-03/QF-04 已完成：用量源、目录费率、固定执行价格、自动账目、授权分页汇总、统一审计及单权威离线备份恢复均已验收。见[用量服务](../validation/admin-platform/usage-service-acceptance.md)和[费率/账目收尾](../validation/admin-platform/rate-ledger-recovery-acceptance.md)。这些能力不替代 QF-02 的执行启动授权。

## 后续任务

依赖列保留计划中的直接依赖；验收列摘录原任务要求，完整条件见 [TASKS.md](TASKS.md) 和 [tasks.json](tasks.json)。

### M1：配额计量与内部成本

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| QF-06 | 成本中心汇总与安全导出 | QF-05, CP-03 | 汇总包含价格/归属版本、单位、水位和待核算标记。 游标读取完整授权数据，异步导出再验权，CSV 公式注入防护通过。 |
| QF-07 | 租户额度与成本分摊界面 | CP-05, QF-01, QF-06 | 实现租户详情与 finops 汇总，额度/预留/用量/成本分别展示。 编辑影响预览、CAS 冲突、pending、费用钻取与导出全流程可用。 |
| QF-08 | 影子计量与试点强制配额 | QF-02, QF-05, CP-06 | observe/shadow/enforce 有明确开关及试点租户；旧占用导入或隔离有证据。 双网关争抢、一槽竞争、控制面断联均不绕过硬额度；停止试点不清账。 |

### M2：配置发布

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |

### M4：已按供应商中立范围验收

真实接入按[单独部署任务](REGION-LIVE-DEPLOYMENT.md)推进，当前未验收。

### M5：集成与交付验收（已结项）

VA-01 至 VA-04 均已完成。交付证据见 [VA-04 记录](../validation/admin-platform/va04-delivery.md)。

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| VA-02 | 后台浏览器与可访问性验收 | QF-07, CF-07, MK-05, RG-05 | 三角色、跨租户、冲突保留、审批分离、空态与部分失败回归。 桌面/窄屏截图和键盘流程通过，旧登录/Profile/任务/拓扑回归通过。 |
| VA-03 | 隔离 Compose 部署与恢复演练 | VA-01, VA-02 | 独立项目、端口、卷与测试租户；新 control cell 绑定、迁移、健康检查完整。 重启/升级/恢复验证账本、预留、授权与审计；现有开发任务和数据不被重置。 |
| VA-04 | 试点、交付证据与能力解锁 | VA-03, QF-06, RG-06 | 按模块记录 API/持久化/执行链/浏览器四层证据，未过项继续标未接入。 确认成本中心/费率维护人、试点额度、审批备份人、留存策略；真实地域验收单独保持待配置。 |

## 结项后的跟踪事项

- 2026 年 10 月实际费用仍待核算。预估额度、证据缺口和后续关账条件见[关账验收](../validation/admin-platform/period-close-native-acceptance.md)。
- 真实地域接入继续待配置，验收条件见[真实地域接入任务](REGION-LIVE-DEPLOYMENT.md)。
- 已记录的全量基线仍有 12 项失败待归因，详见[基线记录](../validation/admin-platform/worktree-runtime-baseline.md)。

## 验收边界与待配置事项

- 首期是内部成本分摊和容量分配；费率由管理员版本化维护。缺用量或价格证据时保持待核算，不能填零或补造模型用量。
- 配置发布需要实际指标与目标 ACK，双人审批需要两个不同的数字用户身份。
- 多个 Daemon 共用物理宿主机不能重复计算容量；离线或 UNCERTAIN 占用不能仅凭 TTL 释放。
- 地域、供应商、驻留要求与 RPO/RTO 尚待确定。可先做目录和模拟，真实第二地域部署与灾备验收待实际环境落实。开发多 Cell 不能作为多地域证据。
- 策略、准入、证据及未来账本的独立备份恢复需分别验收，M0 目录备份不能替代。
- 全仓库类型检查已通过，待补齐当前工作树的 lint、测试与运行时证据。

## GitHub 未关闭 Issue：另列待核验

下方保留 GitHub 仓库 [echo112400-beep/ansts](https://github.com/echo112400-beep/ansts) 的既有快照（当时有 **34 个未关闭 Issue**）。旧工程清单尚未与后台任务逐项映射，不能直接合计。后续按实现和验收证据重新核验后再关联计划任务。本次未刷新快照或变更远端状态。

| Issue | 标题 | 核验状态 |
| --- | --- | --- |
| [#1](https://github.com/echo112400-beep/ansts/issues/1) | Implement Celld microservice persistence and recovery | 待核验及映射 |
| [#10](https://github.com/echo112400-beep/ansts/issues/10) | T9: Celld bucket acceptance, migration, independent backup, and release gate | 待核验及映射 |
| [#12](https://github.com/echo112400-beep/ansts/issues/12) | Bayesian 工作画像驱动资源定容 | 待核验及映射 |
| [#13](https://github.com/echo112400-beep/ansts/issues/13) | 入口背压与 SSE 信用窗口 | 待核验及映射 |
| [#14](https://github.com/echo112400-beep/ansts/issues/14) | Aging、EDF 与五维 W-DRF 公平调度 | 待核验及映射 |
| [#15](https://github.com/echo112400-beep/ansts/issues/15) | 按画像和供应商健康度动态路由模型 | 待核验及映射 |
| [#16](https://github.com/echo112400-beep/ansts/issues/16) | 共享供应商限流与三态熔断 | 待核验及映射 |
| [#17](https://github.com/echo112400-beep/ansts/issues/17) | Warm Pool 与沙箱相变控制 | 待核验及映射 |
| [#18](https://github.com/echo112400-beep/ansts/issues/18) | PSI 压力触发休眠与安全恢复 | 待核验及映射 |
| [#19](https://github.com/echo112400-beep/ansts/issues/19) | 执行速度与滥用熔断 | 待核验及映射 |
| [#20](https://github.com/echo112400-beep/ansts/issues/20) | 零信任 VSOCK 沙箱出网桥 | 待核验及映射 |
| [#21](https://github.com/echo112400-beep/ansts/issues/21) | Turn WAL 压实与确定性时光机 | 待核验及映射 |
| [#22](https://github.com/echo112400-beep/ansts/issues/22) | 从节点压力到入口准入的闭环背压 | 待核验及映射 |
| [#23](https://github.com/echo112400-beep/ansts/issues/23) | 分片队列代理与 HA 自动扩缩 | 待核验及映射 |
| [#24](https://github.com/echo112400-beep/ansts/issues/24) | Celld 与 RustFS 部署加固 | 待核验及映射 |
| [#25](https://github.com/echo112400-beep/ansts/issues/25) | 集群 CLI 与实时运维看板 | 待核验及映射 |
| [#26](https://github.com/echo112400-beep/ansts/issues/26) | 多节点故障与网络分区混沌验证 | 待核验及映射 |
| [#27](https://github.com/echo112400-beep/ansts/issues/27) | 热租户分区与队列自动分片 | 待核验及映射 |
| [#28](https://github.com/echo112400-beep/ansts/issues/28) | Brain/Body 容器复用扩容 | 待核验及映射 |
| [#29](https://github.com/echo112400-beep/ansts/issues/29) | Celld 管理 API 滚动重启编排 | 待核验及映射 |
| [#30](https://github.com/echo112400-beep/ansts/issues/30) | OpenAI 兼容实时 SSE 推理适配器 | 待核验及映射 |
| [#31](https://github.com/echo112400-beep/ansts/issues/31) | Linux cgroup、PSI 与 OverlayFS 沙箱原语 | 待核验及映射 |
| [#32](https://github.com/echo112400-beep/ansts/issues/32) | VSOCK 守护进程双向帧校验 | 待核验及映射 |
| [#33](https://github.com/echo112400-beep/ansts/issues/33) | 带执行栅栏的跨主机热迁移 | 待核验及映射 |
| [#34](https://github.com/echo112400-beep/ansts/issues/34) | 裸金属手动扩缩容控制器 | 待核验及映射 |
| [#35](https://github.com/echo112400-beep/ansts/issues/35) | RustFS 16 盘 EC:4 高可用集群 | 待核验及映射 |
| [#36](https://github.com/echo112400-beep/ansts/issues/36) | OTLP/Prometheus 指标与集群脑裂验证 | 待核验及映射 |
| [#37](https://github.com/echo112400-beep/ansts/issues/37) | MCP 工具动态注册与 staged commit | 待核验及映射 |
| [#38](https://github.com/echo112400-beep/ansts/issues/38) | FinOps 预算告警、硬顶与优雅排空 | 待核验及映射 |
| [#39](https://github.com/echo112400-beep/ansts/issues/39) | GDPR 感知的跨区域联邦路由 | 待核验及映射 |
| [#40](https://github.com/echo112400-beep/ansts/issues/40) | Durable Object Facets 生命周期监督 | 待核验及映射 |
| [#41](https://github.com/echo112400-beep/ansts/issues/41) | 动态 Worker CPU 与子请求硬限制 | 待核验及映射 |
| [#42](https://github.com/echo112400-beep/ansts/issues/42) | Celld KV 分片写入与请求级缓存 | 待核验及映射 |
| [#43](https://github.com/echo112400-beep/ansts/issues/43) | V8 堆、SQL 时限与原生 RPC 守卫 | 待核验及映射 |
