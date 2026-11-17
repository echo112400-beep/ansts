# QF-03 用量服务接线与部署验收

日期：2026-11-17。范围是不可变资源与 provider 原始用量，不包含价格、费用账本或执行硬配额。

## 来源与执行绑定

执行前持久化 Host、tenant、task、attempt、fencing、输入摘要与实际分配；Fleet 从已认证 Host 及真实 Body 记录核验，租户权威保存绑定。旧 Body 证明退役后，已经绑定的历史流仍可投递。Host 不持有目录或准入服务凭据。

CPU 使用 Engine nano-core × 毫秒，内存为 MiB × 毫秒，slot 为槽 × 毫秒；分配时长不代表 CPU 利用率。UTC 零点切段。无结构旧执行只保留已知 slot，CPU/内存为 null，并计入缺失覆盖告警。

DeepSeek 请求意图先持久保存；响应中的 token 在 action 解析前采集，缺失、无响应或不可验证均为 UNKNOWN/null。预算估值不进入原始用量。字段依据 [DeepSeek 官方接口](https://api-docs.deepseek.com/api/create-chat-completion/)。隔离验收使用实际适配器解析本地 HTTP 响应，没有调用付费模型。

## 持久投递、查询和告警

事件及发送意图原子落盘，稳定 stream/sequence 与摘要确认；生产 BodyAgent 独立循环自动签名投递、持久退避，不依赖人工 relay。TenantControl SQLite 同事务保存事件、接收时间和水位；重复投递不修改原记录，同键不同内容隔离并保留 attention 证据。

租户查询采用授权检查、签名游标和冻结水位，归属上下文保持执行时版本。rateCardVersion=null、pricing=NOT_CONNECTED，不能将数量完整误解为已定价。四条低基数 Prometheus 规则覆盖冲突、积压、未知供应商用量和资源覆盖缺失；规则夹具正负例均通过。

## 已完成验证

- 严格控制面检查与 128 项测试通过，315 模块构建通过，5 项实际 Agent 工具及中断恢复回归通过。
- 原生 celld、生产 BodyAgent、Daemon、实际 Docker Engine 联合验收通过；远端提交后丢回执能自动重试，退役 Body 证明不阻塞已绑定事件，接收对象重建后重放幂等。
- 实际 fork 的旧运行时仅记录 slot，未知 CPU/内存不补零；供应商响应、失败重试、游标冻结、租户隔离及文件/事务失败均有隔离验证。

## 真实开发部署与重启恢复

新运行镜像已构建部署到三节点 Fleet、两台 Daemon 和双 Gateway；安全排空重启保留 118 条原任务及规格与原 Drain 状态。仅专属试验租户加入 shadow 白名单，执行一条真实无网络容器任务，实际自动产生并投递 OPEN/SEGMENT/CLOSE，整数 CPU/内存/slot 数量与 Engine 分配一致。Viewer 本租户查询通过，跨租户拒绝。试验租户及成本中心随后停用，不删除数量证据。

第二次安全重启保留全部 119 条任务（118 条原任务 + 1 条专属验收任务）。事件原文、receivedAt、不可变上下文及分页顺序逐项一致，双 Gateway 共用权威，四条用量告警规则实际加载。历史 UNCERTAIN/pending 在三个 Fleet 入口精确恢复，Host outbox 鉴权、容量目录归属与新 epoch、4/4 Prometheus 及真实管理浏览器回归均通过。

复验命令：`node scripts/verify_usage_deployment.mjs verify-recovery`。configure/execute 是创建专属试验数据的步骤，不用于普通只读检查。运行检查点位于忽略的 `.dev-secrets/redeployment/usage-acceptance.json`，不提交凭据或运行状态。

按原始用量来源、防重放、重启连续性及冲突隔离告警条款，QF-03 标记 completed；未解除 QF-02 的启动授权与其他模块依赖门禁。

## 能力边界

计量默认关闭；shadow 必须显式指定租户白名单。单流最多 5000 事件或 8 MiB，达到上限拒绝继续追加；独立归档和费率/关账属于后续任务。QF-02 的启动授权、硬门禁和可信成本预留仍未完成，本次用量绑定不代替执行授权。全仓库 lint 仍受既有无扩展名导入检查问题阻塞，本次未宣称全仓库检查通过。
