# QF-03：真实用量源与持久 outbox（实施中）

> 本文保留用量源阶段验收记录；后续 Fleet 自动投递、租户查询、告警和部署进展见 [用量服务验收](usage-service-acceptance.md)。

日期：2026-11-17。本批完成源端与接收内核的隔离验收，QF-03 尚未完成整体验收，也未更新开发部署。

## 已实现

`UsageEvent` 独立于执行观察、容量报告和预算估算。事件绑定 producer、tenant、task、attempt、fencing、输入摘要、单调 sequence 和稳定 eventId；摘要和签名覆盖规范化内容。CPU、内存、slot 数量与 provider token 分开保存，不包含价格、USD、credit、prompt、模型输出或凭据字段。

- 资源窗口从本机成功资源预留时间计起，在首次执行 journal 准入后、模型/Engine 执行前持久提交。历史结果重放不重新打开旧事件流；旧缺失记录不补造成历史用量。
- CPU 保存与 Engine `NanoCpus` 一致的整数 nano-core 配置，以 `nano-core × ms` 计量；配额参照仍是向上取整的 milli-core，计量不提前按配额粒度取整。内存为 `MiB × ms`、slot 为 `slot × ms`。所有乘积是 BigInt 十进制字符串，按 UTC 日界切分无重叠窗口。它们是分配时长，不是 CPU/RSS 利用率。
- 模型 HTTP 请求前持久记录请求意图；供应商响应返回后、解析模型 action 前记录 token，因此无效模型输出仍保留真实响应数量。失败重试分别记录，不把运行时 token 保守预算当成实际消耗。
- DeepSeek 响应读取输入、输出、总 token 和存在的缓存拆分，同时保留返回 model、response ID 和响应摘要。字段语义依据 [DeepSeek 官方 Chat Completions 文档](https://api-docs.deepseek.com/api/create-chat-completion/)。缺失或不合法数量保持 null/MISSING；响应丢失保持 UNKNOWN，不填零，不推断缓存命中。
- 每 attempt 文件原子保存不可变事件与投递意图，序号先持久化后公开；确认仅修改投递元数据，已确认事件保留。每流只出队最早未确认事件，持久有限退避；冲突隔离后旧确认不能继续推进该流。
- Engine 停止未确认时不产生资源关闭事件，保留资源占用；Host 启动仅在 Engine 正向清理后关闭旧资源窗口。尚未取得响应的供应商请求继续缺失，不能随本机恢复当作零消耗。
- SQLite `UsageInbox` 接收内核验证已认证执行绑定、事件序号、窗口连续性和请求关联。事件与水位同事务写入，重放幂等；同 ID 不同内容保留原事件、隔离流并持久记录 `USAGE_EVENT_CONFLICT / requiresAttention`。记录源事件时间及独立接收时间。

`UsageInbox.inspect().pending` 只表示当前数量事件流完整性；它不代表价格已知或费用已结算。本批没有实现费用账本。

## 已执行验证

| 检查 | 结果和范围 |
| --- | --- |
| `npm run test:control` | 严格类型检查及123项测试通过，新增12项覆盖跨 UTC、CPU 细粒度精度、不可变重放、重启序号、真实 token 与未知值、模型失败重试、冲突隔离/注意记录、SQLite 回滚、源端文件失败与未确认停止 |
| `npm run build` | 312个模块构建通过；不是全仓库类型检查通过 |
| `python3 scripts/verify_body_evidence_native.py` | 原生 celld Scheduler/Body、实际 Daemon/Docker Engine、生产 BodyAgent 脚本与本地 HTTP 供应商夹具通过；DeepSeek 适配器处理夹具的真实 HTTP 响应，未请求真实模型服务 |
| 原生用量验证 | 容器和 agent 分配窗口、整数数量、输入/输出 token、签名、有序待投递与确认、真实 SQLite 收件箱重建重放通过。受信绑定来自实际 Scheduler 租约，未把事件自身当作身份权威 |
| 旧 agent 执行回归 | `ANSTS_SANDBOX_TEST_IMAGE=... node --test dist/tests/agent_tool_loop.test.js`：5项通过，含真实容器工具与中断恢复 |
| 脚本语法 | `node --check scripts/body_cell_agent.mjs` 通过，心跳遵守 Daemon 明确的 unhealthy 状态 |

原生夹具只操作独立容器、临时状态与随机端口；不修改开发任务、容量目录或真实身份配置，不调用付费模型。用量数据未导入开发账本。

## 继续实施的边界

计量由原生 `BodyDaemonOptions.usageMetering` 显式开启，默认关闭；本批仅在隔离验证开启。私有 `/v1/daemon/usage/pending`、`ack`、`defer` 需要 Host 身份，不构成租户可访问的用量 API。生产启动器尚未启用，Fleet 路由/Host 自动用量投递与 TenantControlCell 接收未接线。原生验收使用隔离程序显式连接 SQLite 接收内核，不能当作 Fleet 用量服务已上线。

后续需完成：接收前的真实执行绑定查找、跨对象投递/回执和实际告警通道、租户授权查询/分页、旧/无结构执行的缺失覆盖标记、服务重启故障验收与开发部署。QF-02 全链路仍待完成，QF-03 利用已验收的执行身份先开发源端，不解除原计划验收依赖。

源端每流保留最多5000事件、8MiB。容量或持久化故障不能清理未知占用或伪造零费用；待投递、未知 provider 与隔离记录不作普通 TTL 清理。独立归档和安全去重压缩仍属后续交付门禁。
