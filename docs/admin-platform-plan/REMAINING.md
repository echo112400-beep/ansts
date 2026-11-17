# 未完成任务清单

整理日期：2026-11-17。状态来源：[tasks.json](tasks.json)；进展依据：[PROGRESS.md](PROGRESS.md)及其验收记录。本清单按本批用量服务及开发部署验收更新；未重新执行全仓库检查。

## 总体状态

共 37 项：**已完成 8 项、进行中 2 项、尚未开始 27 项，剩余 29 项**。M0 已按现有记录验收；M1～M5 仍有工作。`not_started` 表示该计划任务尚未开展验收，不代表没有可复用的旧代码。

| 阶段 | 已完成 | 进行中 | 未开始 |
| --- | ---: | ---: | ---: |
| M0 | 6 | 0 | 0 |
| M1 | 1 | 2 | 5 |
| M2 | 0 | 0 | 7 |
| M3 | 1 | 0 | 5 |
| M4 | 0 | 0 | 6 |
| M5 | 0 | 0 | 4 |

## 优先收尾：进行中的任务

### QF-01：租户额度与预算策略版本

策略持久化、CAS、幂等、历史分页、管理界面与双 Gateway 部署验收已有记录。剩余：

- 完成执行准入的策略快照、费率快照及元数据激活接入，联动 QF-02、QF-04、QF-08。
- 扩展策略独立备份与恢复验证。
- 验证降额只影响新增请求，已有任务保持原策略与费率快照。

### QF-02：统一准入预留与补偿协议

预留持久化、认证凭证、影子 outbox/inbox、自动投递及暂停恢复、实际 Body/Engine 观察事实、Host 持久观察 outbox、自动 relay、已有预留自动绑定与 agent 观察已有隔离和部署验收记录。剩余：

- 基于可信费率/成本上限自动创建预留；已有预留自动绑定、agent 观察和 Host 持久投递已部署，见[最新验收](../validation/admin-platform/body-agent-binding-acceptance.md)。
- 接入真实容量授权、执行前租户门禁与补偿状态推进；观察回执不能代替启动授权。
- 补齐队列额度，与后续费率、用量、账本和结算接线。
- 完成准入和证据的独立归档、恢复及执行全过程故障验收。

当前仍为 `SHADOW / NOT_CONNECTED`，未启用执行硬配额。详细边界见[自动绑定与 agent 验收](../validation/admin-platform/body-agent-binding-acceptance.md)。

QF-03 已完成：真实用量源、Fleet 自动投递、租户分页、告警及开发重启恢复已验收，见[用量服务验收](../validation/admin-platform/usage-service-acceptance.md)。数量验收不解除 QF-02 的执行授权门禁。

## 尚未开始的 27 项

依赖列保留计划中的直接依赖；验收列摘录原任务要求，完整条件见 [TASKS.md](TASKS.md) 和 [tasks.json](tasks.json)。

### M1：配额计量与内部成本

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| QF-04 | 不可变账本与版本费率 | QF-03, CP-04 | 整数 credit 与费率有显式舍入；原始事件可重算，溢出受到拒绝。 价格未知与用量缺失保持 pending；模型示例值及历史缺失不能自动补账。 费率模板由目录唯一写入，租户快照显式激活；汇总部分不可达必须标注不完整。 |
| QF-05 | 结算、跨期、关账与调整 | QF-04 | 跨零点预留保留、事件按 UTC 分段、取消和失败用量不丢。 缺事件不能最终关账；重复关闭幂等，晚到事件以关联调整记录处理。 |
| QF-06 | 成本中心汇总与安全导出 | QF-05, CP-03 | 汇总包含价格/归属版本、单位、水位和待核算标记。 游标读取完整授权数据，异步导出再验权，CSV 公式注入防护通过。 |
| QF-07 | 租户额度与成本分摊界面 | CP-05, QF-01, QF-06 | 实现租户详情与 finops 汇总，额度/预留/用量/成本分别展示。 编辑影响预览、CAS 冲突、pending、费用钻取与导出全流程可用。 |
| QF-08 | 影子计量与试点强制配额 | QF-02, QF-05, CP-06 | observe/shadow/enforce 有明确开关及试点租户；旧占用导入或隔离有证据。 双网关争抢、一槽竞争、控制面断联均不绕过硬额度；停止试点不清账。 |

### M2：配置发布

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| CF-01 | 配置 schema 与不可变版本 | CP-04 | 白名单字段校验，secretRef 不返回密钥；禁止任意 JSON 修改身份和基础设施。 每 scope 内容摘要、baseRevision、schemaVersion 和 CAS 可恢复。 |
| CF-02 | 差异与影响范围预检 | CF-01, CP-06 | 结构化差异列出影响目标、新任务/热更新生效范围及风险。 缺少真实观测、未知字段和不支持的重启动作明确阻止发布。 |
| CF-03 | 配置专用双人审批 | CF-02, CP-03 | 验证不同数字身份；自己审批、伪造 approvedBy 均失败。 内容或目标变化重新审批；仅一名管理员时明确阻塞。 |
| CF-04 | 目标应用适配器与实际 ACK | CF-01, CP-02 | 目标执行 dry-run/apply/readback，返回实际摘要与代际，拒绝过期发布。 目标重启与重复命令幂等；200 或 desiredVersion 不能冒充生效。 |
| CF-05 | 灰度协调器、暂停及恢复 | CF-03, CF-04, CP-06 | 冻结目标与健康窗口，单 scope 单活动发布，逐批取得 ACK 后验证。 部分失败/缺指标进入暂停；协调器重启继续既有操作，旧 ACK 不覆盖新版本。 |
| CF-06 | 前进版本回退与审计证据 | CF-05 | 回退创建新版本和审批，不修改旧发布证据或回滚数据库。 目标失联时显示未恢复项，不能将部分回退标成全部成功。 |
| CF-07 | 配置发布与审批界面 | CP-05, CF-06 | 差异、预检、独立审批、每目标进度、暂停与回退流程闭环。 覆盖同人拒绝、409 输入保留、离线目标和部分成功；保持 hrui 样式。 |

### M3：内部容量市场

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| MK-02 | 容量申请与公平队列 | MK-01, QF-01 | 即时借用受租户额度、池容量和优先级策略约束；未支持未来预订。 固定多资源负载验证权重与防饥饿轨迹；租户看不到他人任务内容。 |
| MK-03 | 容量授权与准入绑定 | MK-02, QF-02, QF-08 | grant 与 reservation/attempt 单一绑定，跨对象失败可恢复且无重复扣减。 借用不能提高硬配额，授权不生成第二份费用，旧 epoch 不能重复消费。 |
| MK-04 | 到期、取消与不确定占用回收 | MK-03, QF-03 | 仅未消费授权可到期回收；RUNNING/UNCERTAIN 必须有退出证据。 取消请求、主机断联和 outbox 重放不造成超卖或丢失占用。 |
| MK-05 | 内部算力分配界面 | CP-05, MK-04 | 展示真实池容量、保障/借用、需求排队原因、到期与隔离。 OPERATOR 申请/取消及 SRE 管池权限正确，无支付或外部交易 UI。 |
| MK-06 | 容量池负载与恢复演练 | MK-04, MK-05, CP-06 | 多租户 CPU/内存错配、单机多 Daemon、重复授权与断联轨迹通过。 市场暂停后在途执行与成本结算继续，恢复不重发已消费授权。 |

### M4：多地域治理

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| RG-01 | 地域、故障域与可信目录 | CP-02, CP-04 | 持久目录包含司法辖区、能力、身份引用和证据有效期。 未知/未配置状态真实，单开发机多 Cell 不标记成多地域。 |
| RG-02 | 驻留策略与放置模拟 | RG-01, QF-01 | 驻留/模型/数据类型先过滤，健康与 RTT 后排序，输出逐项排除原因。 模拟不调用 Engine、不扣额度；未知/过期驻留信息拒绝受约束任务。 |
| RG-03 | 适配器契约与安全探测 | RG-01, CP-06 | 模拟适配器覆盖能力/身份/探测/提交/排空契约，任意 URL 与 SSRF 被阻止。 服务身份可轮换；开发 loopback 例外不会进入生产配置。 |
| RG-04 | 新任务放置与排空接入开关 | RG-02, RG-03, QF-02 | 先通过模拟端到端与 feature gate；真实适配器未验收时不能启用。 排空仅停止新放置，不迁移或重跑 RUNNING/UNCERTAIN 任务。 |
| RG-05 | 地域治理与证据页面 | CP-05, RG-02, RG-03 | 目录空态、策略差异、放置原因和证据时效可读。 RPO/RTO 目标与实测分开，未知供应商/灾备状态不显示为就绪。 |
| RG-06 | 真实地域接入与灾备演练门禁 | RG-04, RG-05 | 交付供应商参数清单、身份/存储/复制/网络核验步骤和恢复演练模板。 真实接入另建部署任务：设施与 RPO/RTO 目标未确定时保持未验收，不宣称跨域容灾。 |

### M5：集成与交付验收

| 任务 | 名称 | 直接依赖 | 验收要求 |
| --- | --- | --- | --- |
| VA-01 | 跨模块契约与故障注入回归 | QF-08, CF-06, MK-06, RG-04 | 覆盖双 Gateway、跨对象每阶段崩溃、零点、晚到事件、旧 ACK 和重复 outbox。 所有负例对照 PRODUCT/TECH，保存账本重算与无超发证据。 |
| VA-02 | 后台浏览器与可访问性验收 | QF-07, CF-07, MK-05, RG-05 | 三角色、跨租户、冲突保留、审批分离、空态与部分失败回归。 桌面/窄屏截图和键盘流程通过，旧登录/Profile/任务/拓扑回归通过。 |
| VA-03 | 隔离 Compose 部署与恢复演练 | VA-01, VA-02 | 独立项目、端口、卷与测试租户；新 control cell 绑定、迁移、健康检查完整。 重启/升级/恢复验证账本、预留、授权与审计；现有开发任务和数据不被重置。 |
| VA-04 | 试点、交付证据与能力解锁 | VA-03, QF-06, RG-06 | 按模块记录 API/持久化/执行链/浏览器四层证据，未过项继续标未接入。 确认成本中心/费率维护人、试点额度、审批备份人、留存策略；真实地域验收单独保持待配置。 |

## 推荐执行顺序

1. 优先收尾 QF-01/QF-02 的真实执行接线与恢复证据，再推进 QF-03 → QF-04 → QF-05 → QF-08；QF-06 → QF-07 补齐成本汇总和完整界面。
2. 按现有直接依赖，**CF-01、RG-01 已具备启动条件**，可分别开展配置版本和地域可信目录。MK-01 已完成，见[物理目录验收](../validation/admin-platform/capacity-inventory-acceptance.md)。后续任务仍按依赖推进。
3. 配置链路推进到 CF-07；容量授权须等待 QF-08，再推进回收和负载演练；地域先完成目录、模拟、探测及接入开关。
4. 各模块达标后执行 VA-01/VA-02 → VA-03 → VA-04。真实地域门禁由 RG-06 单独落实。

## 验收边界与待配置事项

- 首期是内部成本分摊和容量分配；费率由管理员版本化维护。缺用量或价格证据时保持待核算，不能填零或补造模型用量。
- 配置发布需要实际指标与目标 ACK，双人审批需要两个不同的数字用户身份。
- 多个 Daemon 共用物理宿主机不能重复计算容量；离线或 UNCERTAIN 占用不能仅凭 TTL 释放。
- 地域、供应商、驻留要求与 RPO/RTO 尚待确定。可先做目录和模拟，真实第二地域部署与灾备验收待实际环境落实。开发多 Cell 不能作为多地域证据。
- 策略、准入、证据及未来账本的独立备份恢复需分别验收，M0 目录备份不能替代。
- 既有全仓库类型问题仍需单独清理；局部严格检查通过不代表全仓库类型检查通过。

## GitHub 未关闭 Issue：另列待核验

GitHub 仓库 [echo112400-beep/ansts](https://github.com/echo112400-beep/ansts) 当前有 **34 个未关闭 Issue**。这些是旧工程清单，尚未与上面的后台任务建立逐项对应关系；未关闭不等于未执行，不能与 30 项直接相加。下一步按实现与验收证据标记已完成、部分完成或待实现，再关联计划任务。本次仅整理，不变更远端状态。

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
