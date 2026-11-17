# QF-02：自动绑定与 agent 观察验收

日期：2026-11-17。范围：BodyAgent 自动查找已有权威观察绑定、agent 生命周期证据，以及本批和上一批 Host outbox 的开发部署。

## 实现边界

`/internal/v1/body-observation-binding` 使用 Host 服务身份，从服务端生成限定租户的 OPERATOR actor；查询租户权威中已持久授权的 task、attempt、fencing、目标 Host、输入摘要与资源快照。响应只包含观察绑定，不返回准入通用写能力或协调器密钥。请求不能指定 actor 或其他目标 Host。

查找不创建或变更预留，不产生容量授权。无匹配绑定返回 `binding: null`；未登记租户返回目录错误。过期但尚未结算的既有 attempt 可读回绑定，用于投递终止证据，不代表允许再次启动。RESERVED 尚未持久启动授权时不提供绑定，已取消/已结算不提供新绑定。

生产 BodyAgent 自动查询并传入绑定，调用原有 Scheduler/Body 执行 grant 后执行。影子模式下，缺绑定或查询失败明确记录 UNBOUND/DEFERRED 并保留原执行路径；这不是 enforce 模式下的降级行为。缺费率或可信成本上限时不自动创建零成本预留。

agent 在检查恢复状态及缓存结果后、进入实际工具循环前记录 RUNNING，覆盖等待模型响应和工具执行的整个本机生命周期。工具确认退出且循环正常返回后记录 TERMINATED；Engine 停止未确认时传播失败并保留 UNCERTAIN/资源占用。模型 token 与价格仍须 QF-03/QF-04 的独立可信证据，生命周期事实不构成用量或成本。

## 已执行验收

| 检查 | 结果 |
| --- | --- |
| `npm run test:control` | 严格类型检查与102项测试通过。新增自动绑定 Host/租户/输入/资源隔离、只读查询、过期与未授权状态、响应模式校验；agent 模型等待期间 RUNNING、完成 TERMINATED、停止未确认 UNCERTAIN |
| `npm run build` | 301个模块构建通过；不构成全仓库类型检查通过 |
| `node --check scripts/body_cell_agent.mjs` | 通过 |
| `python3 scripts/verify_body_evidence_native.py` | 独立 celld + 原生 Daemon + 实际 Docker Engine 通过。实际生产 `body_cell_agent.mjs` 子进程自动查询长 ID agent 绑定、执行真实容器工具、上传签名终止事实并确认 outbox；模型适配器为确定性测试实现，未调用付费模型 |
| 原生故障回归 | 容器 RUNNING/SETTLING、远端成功但回执丢失、Host/权威重启重放、摘要确认、资源释放与未定价预算保留通过 |
| `python3 scripts/restart_dev_cluster.py --check-only` | 完整 Compose 配置、秘密文件权限/服务隔离、挂载校验通过 |
| `--build-only` 后 `--skip-build` | 实际新镜像构建与开发部署完成；执行器排空后重启，保留全部状态卷；118条原任务与执行规格一致，原始 Drain 恢复 |
| `node scripts/verify_body_evidence_deployment.mjs` | 双 Host outbox 鉴权和有界查询通过；三个 Fleet 入口认证、方法、字段白名单、租户预检通过；旧缺输入快照预留只返回 UNBOUND，无状态修改 |
| `node scripts/verify_admission_recovery.mjs` | 三个入口精确读回原 UNCERTAIN 预留及 pending outbox |
| `node scripts/verify_monitoring_deployment.mjs` | 4/4监控目标在线，专用监控身份不能执行业务变更，发布观测证据完整 |
| `scripts/verify_directory_deployment.mjs`，启用 Playwright | 真实双 Gateway 配额 CRUD/CAS/重放/历史分页、浏览器策略编辑、目录/只读权限、审计操作查询与归档下载通过；验收元数据已停用，未创建实际执行任务 |

原生验收预留仍由隔离 fixture 使用明确的测试预算和观察容量标记建立；BodyAgent 无需 fixture 手动传入绑定或手动 relay。不能将这项结果表述为真实容量授权或自动成本准入。开发部署验收不运行用户任务或调用真实模型。

## 后续条件

QF-01/QF-02 均保持 `in_progress`。自动预留创建和估算需可信费率及成本上限；实际容量授权、执行前租户硬门禁、队列额度、用量 producer/账本/结算、独立归档及全过程故障门禁待后续批次。当前服务继续 `SHADOW / NOT_CONNECTED`。

全仓库 lint 仍受旧无扩展名导入与自定义解析器不兼容阻塞；本批没有更改该部分，也没有声称全仓库检查通过。
