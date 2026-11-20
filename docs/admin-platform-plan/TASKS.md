# 实现任务清单

状态：实施中；M0 的 CP-01～CP-06、MK-01、QF-03、QF-04 与 CF-01、CF-02、CF-03、CF-04、CF-05、CF-06、CF-07 已 `completed`，QF-01、QF-02、QF-05 为 `in_progress`，其余18项为 `not_started`。验收对照见 [M0 验收](../validation/admin-platform/m0-acceptance.md)，详细过程见 [PROGRESS.md](PROGRESS.md)。

S/M/L 为相对复杂度：S 局部变更，M 单模块或常规跨层，L 跨权威状态/执行链/故障恢复。不能直接相加推算工期；先完成 M0 技术验证，再依据团队投入排期。负责项表示建议职能和代码责任范围，实际负责人待分配。目录为拟议接入目标，不保证已存在。

## 里程碑与依赖

| 阶段 | 可评审结果 | 主要解锁条件 |
| --- | --- | --- |
| M0 基础 | 持久控制对象、租户、权限、审计、观测 | 当前部署链路与存储验证 |
| M1 配额成本 | 影子计量、分摊、试点准入 | M0；真实用量与资源释放证据 |
| M2 配置发布 | 预检、双人审批、灰度及回退 | M0；至少两位独立审批身份 |
| M3 容量分配 | 公平队列、借用授权、保守回收 | M1 准入与物理容量验证 |
| M4 地域规划落地 | 目录、策略、模拟与适配器门禁 | M0；真实接入另待供应商/地域 |
| M5 综合验收 | 隔离部署、恢复演练、试点证据 | 各领域链路验收 |

主依赖链：CP-01 → CP-02 → CP-03 → CP-04 → QF-01 → QF-02 → QF-03 → QF-04 → QF-05 → QF-08 → MK-03 → MK-04 → MK-06 → VA-01 → VA-03 → VA-04。CF 与 RG 在基础成熟后可分线推进；日历关键路径还需结合人数和每项实际耗时。

M4 的契约/模拟开发不等待供应商选择。RG-06 的清单和模板可以完成，但真实第二地域的部署及灾备验收仍须保持待配置，不能用模拟结果关闭环境验收。

## M0 基础控制面

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| CP-01 | 核验当前运行链路与管理契约 | 后端/架构 / M | 无 |
| CP-02 | 持久控制对象底座与迁移恢复 | 后端 / L | CP-01 |
| CP-03 | 租户与成本中心目录及 RBAC | 后端 / M | CP-02 |
| CP-04 | 审计、CAS、幂等与操作查询 | 后端 / M | CP-02, CP-03 |
| CP-05 | 管理 API client 与 hrui 交互底座 | 前端 / M | CP-01, CP-03, CP-04 |
| CP-06 | 修复观测路径并建立控制面指标 | 运维/后端 / M | CP-01 |

### CP-01 验收

目标：`src/cloudflare/gateway_cell.ts`、`src/cloudflare/scheduler_cell.ts`、`docs/admin-platform-plan`。

- 记录 Gateway→Scheduler→Body→Engine 的实际入口和控制对象绑定；区分旧 dashboard 原型。
- 冻结 tenant/attempt/fencing、单位、错误码与 API schema，并形成权限契约测试清单。

### CP-02 验收

目标：`src/control/`、`src/cloudflare/env.ts`、`src/cloudflare/worker.ts`。

- 验证单对象事务及唯一写权威；迁移有版本、可重复执行、重启恢复与数据备份。
- 两个 Gateway 并发更新同一租户不产生独立额度；无持久化时禁止启用 enforce。

### CP-03 验收

目标：`src/control/`、`src/dashboard/console_rbac_guard.ts`、`src/cloudflare/tenant_directory_cell.ts`。

- 目录持久化租户稳定 ID、状态、成本中心及归属版本；不可变元数据显式激活，展示名称不参与授权。
- VIEWER/OPERATOR/SRE_ADMIN 和跨租户读写矩阵全部覆盖；停用不删除历史。

### CP-04 验收

目标：`src/control/`、`src/cloudflare/gateway_cell.ts`。

- 同 key 同 payload 返回原结果、不同 payload 返回409；跨日重放不重复变更。
- 业务变更与审计原子或通过持久 outbox 保证；权限、原因、版本与脱敏均可核查。

### CP-05 验收

目标：`ui/console/lib/`、`ui/console/components/`。

- 统一授权读取、游标、操作状态、409 保留输入和过期数据提示。
- 所有写入口基于真实能力状态显示；键盘、窄屏、空态和无权限态可用。

### CP-06 验收

目标：`deploy/dev-cluster/prometheus.yml`、`src/runtime/`、`src/cloudflare/`。

- 复测并修复 Gateway/Daemon 的 metrics 路径与鉴权，不放宽业务接口权限。
- 准入、outbox、预留年龄等指标有明确来源；缺失监控不能通过发布验证。

## M1 配额与成本分摊

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| QF-01 | 租户额度与预算策略版本 | 后端 / M | CP-03, CP-04 |
| QF-02 | 统一准入预留与补偿协议 | 调度/后端 / L | QF-01 |
| QF-03 | 资源与 provider 用量 producer/outbox | 运行时 / L | CP-02, QF-02 |
| QF-04 | 不可变账本与版本费率 | 后端/FinOps / L | QF-03, CP-04 |
| QF-05 | 结算、跨期、关账与调整 | 后端/FinOps / L | QF-04 |
| QF-06 | 成本中心汇总与安全导出 | 后端 / M | QF-05, CP-03 |
| QF-07 | 租户额度与成本分摊界面 | 前端 / L | CP-05, QF-01, QF-06 |
| QF-08 | 影子计量与试点强制配额 | 调度/运维 / L | QF-02, QF-05, CP-06 |

### QF-01 验收

目标：`src/finops/`、`src/cloudflare/tenant_control_cell.ts`。

- 并发/CPU/内存/队列及 UTC 日月预算的范围、单位和 CAS 校验有效。
- 降低限额只限制新增；旧运行任务保留策略与费率快照。

### QF-02 验收

目标：`src/control/`、`src/cloudflare/scheduler_cell.ts`、`src/runtime/`。

- 准备、启动授权、运行、结算持久状态串联；每阶段崩溃后可恢复。
- 旧代际、撤销/过期凭证拒绝执行；UNCERTAIN 不因 TTL 自动释放。

### QF-03 验收

目标：`src/runtime/`、`src/cloudflare/execution_attempt.ts`。

- CPU/内存分配时间、slot 与 token 来源清晰；分配量不伪装 CPU 利用率。
- attempt/fencing/sequence 防重放，重启不断序；同键不同内容隔离并告警。

### QF-04 验收

目标：`src/finops/`。

- 整数 credit 与费率有显式舍入；原始事件可重算，溢出受到拒绝。
- 价格未知与用量缺失保持 pending；模型示例值及历史缺失不能自动补账。
- 费率模板由目录唯一写入，租户快照显式激活；汇总部分不可达必须标注不完整。

### QF-05 验收

目标：`src/finops/`、`src/cloudflare/tenant_control_cell.ts`。

- 跨零点预留保留、事件按 UTC 分段、取消和失败用量不丢。
- 缺事件不能最终关账；重复关闭幂等，晚到事件以关联调整记录处理。

### QF-06 验收

目标：`src/finops/`、`src/cloudflare/gateway_cell.ts`。

- 汇总包含价格/归属版本、单位、水位和待核算标记。
- 游标读取完整授权数据，异步导出再验权，CSV 公式注入防护通过。

### QF-07 验收

目标：`ui/console/components/`、`ui/console/app/manage/`。

- 实现租户详情与 finops 汇总，额度/预留/用量/成本分别展示。
- 编辑影响预览、CAS 冲突、pending、费用钻取与导出全流程可用。

### QF-08 验收

目标：`src/cloudflare/scheduler_cell.ts`、`deploy/dev-cluster/`。

- observe/shadow/enforce 有明确开关及试点租户；旧占用导入或隔离有证据。
- 双网关争抢、一槽竞争、控制面断联均不绕过硬额度；停止试点不清账。

## M2 配置发布

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| CF-01 | 配置 schema 与不可变版本 | 后端 / M | CP-04 |
| CF-02 | 差异与影响范围预检 | 后端 / M | CF-01, CP-06 |
| CF-03 | 配置专用双人审批 | 后端 / M | CF-02, CP-03 |
| CF-04 | 目标应用适配器与实际 ACK | 运行时/后端 / L | CF-01, CP-02 |
| CF-05 | 灰度协调器、暂停及恢复 | 后端 / L | CF-03, CF-04, CP-06 |
| CF-06 | 前进版本回退与审计证据 | 后端 / M | CF-05 |
| CF-07 | 配置发布与审批界面 | 前端 / L | CP-05, CF-06 |

### CF-01 验收

目标：`src/config_release/`、`src/cloudflare/config_release_cell.ts`。

- 白名单字段校验，secretRef 不返回密钥；禁止任意 JSON 修改身份和基础设施。
- 每 scope 内容摘要、baseRevision、schemaVersion 和 CAS 可恢复。

### CF-02 验收

目标：`src/config_release/`。

- 结构化差异列出影响目标、新任务/热更新生效范围及风险。
- 缺少真实观测、未知字段和不支持的重启动作明确阻止发布。

### CF-03 验收

目标：`src/config_release/`、`src/saga/staging.ts`。

- 验证不同数字身份；自己审批、伪造 approvedBy 均失败。
- 内容或目标变化重新审批；仅一名管理员时明确阻塞。

### CF-04 验收

目标：`src/config_release/`、`src/cloudflare/`、`src/runtime/`。

- 目标执行 dry-run/apply/readback，返回实际摘要与代际，拒绝过期发布。
- 目标重启与重复命令幂等；200 或 desiredVersion 不能冒充生效。

### CF-05 验收

目标：`src/config_release/`、`src/cloudflare/config_release_cell.ts`。

- 冻结目标与健康窗口，单 scope 单活动发布，逐批取得 ACK 后验证。
- 部分失败/缺指标进入暂停；协调器重启继续既有操作，旧 ACK 不覆盖新版本。

### CF-06 验收

目标：`src/config_release/`。

- 回退创建新版本和审批，不修改旧发布证据或回滚数据库。
- 目标失联时显示未恢复项，不能将部分回退标成全部成功。

### CF-07 验收

目标：`ui/console/components/`、`ui/console/app/manage/`。

- 差异、预检、独立审批、每目标进度、暂停与回退流程闭环。
- 覆盖同人拒绝、409 输入保留、离线目标和部分成功；保持 hrui 样式。

## M3 内部容量分配

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| MK-01 | 物理宿主机与容量池权威 | 调度/运行时 / L | CP-02, CP-03 |
| MK-02 | 容量申请与公平队列 | 调度 / M | MK-01, QF-01 |
| MK-03 | 容量授权与准入绑定 | 调度/后端 / L | MK-02, QF-02, QF-08 |
| MK-04 | 到期、取消与不确定占用回收 | 运行时/调度 / L | MK-03, QF-03 |
| MK-05 | 内部算力分配界面 | 前端 / M | CP-05, MK-04 |
| MK-06 | 容量池负载与恢复演练 | 测试/运维 / L | MK-04, MK-05, CP-06 |

### MK-01 验收

目标：`src/cluster/`、`src/runtime/body_resource_ledger.ts`。

- 稳定 hostId、poolId、报告 epoch 与管理员上限，身份和新鲜度可验证。
- 同宿主机多 Daemon 不重复统计；失联容量隔离而非回归空闲。

### MK-02 验收

目标：`src/cluster/`。

- 即时借用受租户额度、池容量和优先级策略约束；未支持未来预订。
- 固定多资源负载验证权重与防饥饿轨迹；租户看不到他人任务内容。

### MK-03 验收

目标：`src/cluster/`、`src/cloudflare/scheduler_cell.ts`。

- grant 与 reservation/attempt 单一绑定，跨对象失败可恢复且无重复扣减。
- 借用不能提高硬配额，授权不生成第二份费用，旧 epoch 不能重复消费。

### MK-04 验收

目标：`src/cluster/`、`src/runtime/`。

- 仅未消费授权可到期回收；RUNNING/UNCERTAIN 必须有退出证据。
- 取消请求、主机断联和 outbox 重放不造成超卖或丢失占用。

### MK-05 验收

目标：`ui/console/components/`、`ui/console/app/manage/`。

- 展示真实池容量、保障/借用、需求排队原因、到期与隔离。
- OPERATOR 申请/取消及 SRE 管池权限正确，无支付或外部交易 UI。

### MK-06 验收

目标：`tests/`、`deploy/dev-cluster/`。

- 多租户 CPU/内存错配、单机多 Daemon、重复授权与断联轨迹通过。
- 市场暂停后在途执行与成本结算继续，恢复不重发已消费授权。

## M4 多地域治理

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| RG-01 | 地域、故障域与可信目录 | 后端 / M | CP-02, CP-04 |
| RG-02 | 驻留策略与放置模拟 | 后端/调度 / L | RG-01, QF-01 |
| RG-03 | 适配器契约与安全探测 | 后端/安全 / L | RG-01, CP-06 |
| RG-04 | 新任务放置与排空接入开关 | 调度 / L | RG-02, RG-03, QF-02 |
| RG-05 | 地域治理与证据页面 | 前端 / M | CP-05, RG-02, RG-03 |
| RG-06 | 真实地域接入与灾备演练门禁 | 架构/运维 / M | RG-04, RG-05 |

### RG-01 验收

目标：`src/cloudflare/region_catalog_cell.ts`、`src/cluster/`。

- 持久目录包含司法辖区、能力、身份引用和证据有效期。
- 未知/未配置状态真实，单开发机多 Cell 不标记成多地域。

### RG-02 验收

目标：`src/cluster/`、`src/cloudflare/federation_router.ts`。

- 驻留/模型/数据类型先过滤，健康与 RTT 后排序，输出逐项排除原因。
- 模拟不调用 Engine、不扣额度；未知/过期驻留信息拒绝受约束任务。

### RG-03 验收

目标：`src/cluster/`、`src/control/`。

- 模拟适配器覆盖能力/身份/探测/提交/排空契约，任意 URL 与 SSRF 被阻止。
- 服务身份可轮换；开发 loopback 例外不会进入生产配置。

### RG-04 验收

目标：`src/cloudflare/scheduler_cell.ts`、`src/cluster/`。

- 先通过模拟端到端与 feature gate；真实适配器未验收时不能启用。
- 排空仅停止新放置，不迁移或重跑 RUNNING/UNCERTAIN 任务。

### RG-05 验收

目标：`ui/console/components/`、`ui/console/app/manage/`。

- 目录空态、策略差异、放置原因和证据时效可读。
- RPO/RTO 目标与实测分开，未知供应商/灾备状态不显示为就绪。

### RG-06 验收

目标：`docs/admin-platform-plan/`、`deploy/`。

- 交付供应商参数清单、身份/存储/复制/网络核验步骤和恢复演练模板。
- 真实接入另建部署任务：设施与 RPO/RTO 目标未确定时保持未验收，不宣称跨域容灾。
- 环境门槛：具体供应商、地域、远端凭证、复制设施与业务恢复目标待用户确定；仅阻塞真实环境接入。

## M5 综合验证与交付

| ID | 任务 | 建议负责 / 规模 | 前置 |
| --- | --- | --- | --- |
| VA-01 | 跨模块契约与故障注入回归 | 测试/后端 / L | QF-08, CF-06, MK-06, RG-04 |
| VA-02 | 后台浏览器与可访问性验收 | 前端/测试 / M | QF-07, CF-07, MK-05, RG-05 |
| VA-03 | 隔离 Compose 部署与恢复演练 | 运维 / L | VA-01, VA-02 |
| VA-04 | 试点、交付证据与能力解锁 | 产品/运维 / M | VA-03, QF-06, RG-06 |

### VA-01 验收

目标：`tests/`。

- 覆盖双 Gateway、跨对象每阶段崩溃、零点、晚到事件、旧 ACK 和重复 outbox。
- 所有负例对照 PRODUCT/TECH，保存账本重算与无超发证据。

### VA-02 验收

目标：`ui/console/`、`docs/validation/`。

- 三角色、跨租户、冲突保留、审批分离、空态与部分失败回归。
- 桌面/窄屏截图和键盘流程通过，旧登录/Profile/任务/拓扑回归通过。

### VA-03 验收

目标：`deploy/dev-cluster/`、`docs/validation/`。

- 独立项目、端口、卷与测试租户；新 control cell 绑定、迁移、健康检查完整。
- 重启/升级/恢复验证账本、预留、授权与审计；现有开发任务和数据不被重置。

### VA-04 验收

目标：`docs/validation/`、`ui/console/lib/admin.ts`。

- 按模块记录 API/持久化/执行链/浏览器四层证据，未过项继续标未接入。
- 确认成本中心/费率维护人、试点额度、审批备份人、留存策略；真实地域验收单独保持待配置。

## 完成定义与状态维护

实现者完成一项任务时更新 tasks.json 的状态，并补充实际修改路径、验证命令与证据位置；若修改依赖或范围，同时更新本页和 PRODUCT/TECH。没有执行过的测试不得写成通过。

部署命令、测试脚本名在任务实现后以实际仓库为准；本规划不提供伪造的现成命令。每个模块有独立上线开关和验收记录，最终交付不会把地域待配置项视为已通过真实容灾验收。
