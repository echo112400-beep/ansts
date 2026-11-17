# QF-02：Host 观察事实持久投递验收

日期：2026-11-17。范围：历史长执行身份、Host 状态观察 outbox、BodyAgent 自动投递、Fleet 身份隔离与回执确认。本批未更新开发集群镜像，也未启用执行硬配额。

后续状态：本批 outbox 已随下一批构建部署，自动绑定和 agent 观察也已接入；见[最新验收](body-agent-binding-acceptance.md)。下文保留本批最初验收时的范围。

## 实现

- 准入、影子消息与签名观察接受最长 512 字符的 task/attempt，匹配既有 Body execution attempt 边界；租户、宿主机、预留与幂等键的原有限制保留。凭证往返覆盖 300 字符任务身份。
- Host 同一个原子落盘事实同时充当待投递意图，不存在“事实已更新、另一个 outbox 文件尚未写入”的窗口。旧格式事实默认待投递。
- 状态观察允许合并中间版本，只投递最新事实；它不是计量事件流，不能代替 QF-03 的有序用量 producer。
- 确认按规范化事实摘要匹配，重复确认幂等；旧事实的确认和失败回执不能清除或延迟新事实。确认保留原事实，不删除历史身份绑定。
- 失败重试次数和下次投递时间与事实一起持久化，退避上限 60 秒；新事实立即变为待投递。单批最多 4 条，BodyAgent 独立循环在长执行期间继续投递。
- 新 Fleet 路由 `/internal/v1/body-execution-evidence` 仅接收 Host 身份认证的签名事实。服务端生成限定租户的 OPERATOR actor，先解析目录，再调用租户唯一权威；不接受浏览器 actor，不暴露通用准入写接口。
- 租户权威继续校验预留目标、输入、代际和事实签名。只有权威明确返回 `sourceVerified` 后，发送方才能根据摘要确认本地投递。

Host 私有接口：POST `/v1/daemon/execution-evidence/pending`、`.../ack`、`.../defer`。均要求 Body 服务身份；未知事实不构成“未启动”证据。

## 已执行验证

| 验证 | 结果与范围 |
| --- | --- |
| `npm run test:control` | 严格 TypeScript 检查及 97 项测试通过；新增长身份全链契约、持久确认与退避、Host 隔离、丢回执重放、新事实与旧 ACK 竞态；守护进程 HTTP 鉴权/分页边界/确认/重启回归通过 |
| `python3 scripts/verify_body_evidence_native.py` | 隔离 celld Scheduler/Body/租户权威、原生 Daemon 与实际 Docker Engine 通过；运行事实经 Host ingress 自动 relay 进入 RUNNING；终止事实进入 SETTLING；模拟远端成功但回执丢失，重启 Host 和 celld 后重放并确认 |
| 真实执行资源/预算 | 终止后资源额度释放；未定价预算保留，超额新准备仍被拒绝；重复终止回执保持相同结果 |
| `node --check scripts/body_cell_agent.mjs` | 通过 |
| `npm run build` | 300 个 TypeScript 模块构建通过；不等于全仓库类型检查通过 |
| `npm run lint` | 未通过：首个阻塞为 `src/checkpoint/verifiable_turn_prover.ts` 的 `../models`。实际 `src/models.ts` 存在，自定义检查器按显式 `.js` → `.ts` 解析，旧代码的无扩展名导入无法解析。本批未修改这些旧模块 |

原生验证脚本使用独立容器、随机端口和临时存储，结束清理隔离资源；未读取真实服务密钥，未修改现有开发任务，未调用付费模型。

## 尚未完成

QF-02 保持 `in_progress`。生产 BodyAgent 尚未自动创建或绑定预留上下文；本批真实验收仍由隔离脚本建立观察预留和绑定，不能声称真实任务自动启用配额。agent 观察、真实容量授权、执行前门禁、队列额度、费率/计量/账本及独立归档仍待后续批次。

本批新增代码已通过隔离真实运行，但开发集群尚未重新部署这些改动。后续部署须保留现有卷和任务，先执行部署检查，再验收自动 relay 的真实运行接线。
