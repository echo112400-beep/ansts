# 后台能力建设方案

状态：**M0 六项、MK-01、QF-03 与 QF-04 已验收完成，剩余28项继续推进**。更新：2026-11-18。

用户已确认：首期面向**内部成本分摊与容量分配**；多地域**先规划，地域和供应商待定**。目前已开始基础契约实现，目录控制面已部署到开发Fleet与双Gateway，配额策略管理已部署；QF-02租户侧持久预留及认证凭证已接入影子内部服务，支持原生alarm自动投递与暂停恢复，Host观察持久outbox、自动relay、已有预留自动绑定与agent观察已通过隔离真实验收并部署；MK-01物理容量目录及自动报告已部署并验证共同Host去重/重启恢复；执行配额/价格尚未启用。

| 文档 | 内容 |
| --- | --- |
| [RUNTIME-CONTRACT.md](RUNTIME-CONTRACT.md) | 已部署执行链路、身份/单位边界与验证映射 |
| [contracts.schema.json](contracts.schema.json) | 当前M0请求Schema及11个已实现接口 |
| [PRODUCT.md](PRODUCT.md) | 用户流程、后台页面、权限、边界与验收 |
| [TECH.md](TECH.md) | 数据归属、执行协议、API、故障恢复与接入位置 |
| [REMAINING.md](REMAINING.md) | 未完成任务、优先级、验收门禁与 GitHub 待核验清单 |
| [TASKS.md](TASKS.md) | 37 项实现任务、依赖、负责模块与验收条件 |
| [tasks.json](tasks.json) | 可供后续执行工具读取的任务清单，包含当前实施状态 |
| [overview.html](overview.html) | 可浏览的方案总览；源稿为 overview.md |

QF-03 已完成用量源、Fleet 自动投递、租户分页及告警，真实开发任务和重启恢复已验收；默认关闭，仅显式试验租户 shadow，不启用计费或硬配额。见 [用量服务验收](../validation/admin-platform/usage-service-acceptance.md)。

QF-04 已验收：目录费率、显式激活、固定执行快照、自动账目、授权分页汇总、统一审计及单权威独立备份恢复已部署；历史未知价格继续 pending。准入自动成本预留和硬门禁继续由 QF-02/QF-08 推进，见 [收尾验收](../validation/admin-platform/rate-ledger-recovery-acceptance.md)。

QF-05 已开始月结/晚到调整内核，尚未启用生产关账；本批发现的 provider 跨零点分段问题已修复并安全部署。见 [月结内核验收](../validation/admin-platform/period-settlement-foundation.md)。

## 范围与推进顺序

1. M0：租户目录、权限、审计、持久化与观测基础。
2. M1：资源配额、用量计量、内部成本账本与分摊。先影子计量，再试点强制配额。
3. M2：配置版本、差异预检、双人审批、分批发布与回退。
4. M3：内部容量池、配额内借用、公平排队与分配回收。
5. M4：地域目录、驻留约束、放置模拟与治理页面。真实地域接入另设环境门槛。
6. M5：跨模块故障恢复、浏览器回归、隔离 Compose 演练与试点验收。

M2、M4 的模型和只读接口可在 M0 后并行推进；M3 的实际分配依赖 M1 的准入与资源生命周期。这里描述工作依赖，不代表已启动并行实现。

首期不接真实支付、开票、外部供需撮合、自动抢占运行任务、自动跨地域重跑。内部费率由管理员制定并版本化，不填充供应商价格。没有用量或价格证据时显示“待核算”。

## 当前能力基线

现有 [后台产品范围](../admin-console/PRODUCT.md) 与 [验收记录](../validation/admin-console/acceptance.md) 已说明：导航覆盖不代表高级后台能力上线。8088 的现有管理页、GitHub 身份、任务执行和拓扑继续作为接入底座。

本方案以工作区源码为依据；基线 HEAD 为 `ff4ad9a`，工作区包含未提交修改，不能用该提交号代表全部运行状态。结构核验使用 codebase-memory Tier 2、索引代际 `2026-11-15T10:07:06Z`，并直接读取关键源码。相关路径覆盖未报告问题；这不构成全仓库完备性证明。

| 已检查文件 | 可复用基础 | 建设时仍需补齐 |
| --- | --- | --- |
| `src/finops/budget_controller.ts` | 日预算、策略和可选 SQLite 恢复逻辑 | 不可变用量事件、跨日去重、版本费率、结账与新准入链路 |
| `src/runtime/body_resource_ledger.ts` | 本机 CPU/内存持久预留，确认 Engine 退出后释放 | 租户额度、物理宿主机去重、统一预留凭证 |
| `src/cloudflare/execution_attempt.ts` | attempt、fencing、版本与核对审计 | 用量事件关联、账本结算和跨控制对象补偿 |
| `src/runtime/body_execution_journal.ts`、`body_result_outbox.ts` | 本地防重执行及结果持久投递 | 用量 outbox、分段计量、结算重放协议 |
| `src/saga/staging.ts` | 审批票据与可选持久化机制 | 配置发布专用状态机、真实操作者分离、执行 ACK |
| `src/cloudflare/federation_router.ts` | 内存地域登记及健康/RTT/驻留区筛选 | 地域目录持久化、真实探测、身份、放置与灾备证据 |
| `src/cloudflare/gateway_cell.ts` | 当前认证与 console 管理接口接入点 | 四个管理域的真实 API 与持久控制对象绑定 |

## 尚待决定的事项

这些事项不阻塞本次规划；实施对应阶段前落实：内部成本中心归属与费率维护人、试点租户及额度、审批备份人、历史账本归档期限、地域/供应商/驻留要求，以及各业务的 RPO/RTO 目标。外部账单和正式财务核算不在首期范围。

任务估算采用 S/M/L 表示相对复杂度，不承诺交付日期。M0 完成持久控制对象验证、确定投入人数后，再给出日历排期。
