# 当前代码架构与开发部署

核对日期：2026-11-14。最新交付和验收见 [剩余任务交付记录](remaining-work-delivery.md)。早期设计中的容量、延迟、RPO/RTO 不作为生产承诺。

## 进程与状态

| 组件 | 职责与权威 |
| --- | --- |
| 两个 Gateway Worker | 用户鉴权、任务 API；经受鉴权的内部接口使用同一个 fleet Gateway Cell，容器重建不丢失共享状态 |
| 三节点 celld fleet | Scheduler、Gateway、Body、Harness、Tunnel 的持久对象；Scheduler 对象为 global-shard-0 |
| Body 控制状态 | 迁移后使用 body-ha-v1: 命名空间；按任务保存准入、凭证消费、结果、运行日志和核对审计 |
| 原 Body celld 监听器 | 18085 保留兼容代理，停止它不会中断 Daemon 到 fleet 的路径；原持久卷保留作迁移证据 |
| 两个原生 Daemon | 管理 Engine、执行 journal、结果 outbox、drain、资源及清理账本；默认每宿主单槽 |
| 沙箱 | 固定摘要 Node 镜像；非 root、禁网、只读根、限额 tmpfs、CPU/内存/PID 硬限制，无宿主目录或服务凭据 |
| 控制台 | 8088；真实任务列表、提交、短期凭据或配置后的 GitHub 登录 |

## 执行与恢复

Gateway 验证结构化 execution 并将其绑定到输入摘要。Scheduler 保存原始容器预算，Daemon 在远程准入前预留槽位和 CPU/内存。容器启动前写入 fsync cleanup-ledger。无法确认清理时保留资源，readiness 返回 503。

容器以独立 exec 运行 argv。命令退出后在沙箱内冻结遗留子进程，采集有界工作区产物，再强制移除容器。Docker archive 不包含 tmpfs，因此产物通过固定的沙箱内采集器读取。不会把归档解包到宿主。

结果包含 SUCCEEDED / FAILED / TIMED_OUT / CANCELLED。执行记录的 COMPLETED 表示结果交付完成；命令成败看 executionOutcome 和 exitCode。旧 TaskState 的 CANCELLED 枚举仍映射 DEAD_LETTER，用户应结合 execution_outcome 判断取消。结果不可覆盖，重复完成按同一 owner、fence 和 outcome 确认。

运行日志每约三秒保存有界快照；授权用户通过 includeResult=1 轮询。最终日志最多保留前 8KiB；产物最多 16 个、单文件 8KiB、合计 12KiB，并返回截断标记。链接、路径越界和非普通文件不导出。

Daemon drain 持久化，停止新准入并允许已有任务完成；重启后需显式 resume。取消先持久记录具名用户请求；已租赁任务在确认退出前不释放资源。无法证明结果的尝试进入 UNCERTAIN，禁止自动重跑。

本机执行请求使用原生 HTTP 客户端，避免 fetch 默认的五分钟响应头等待期限。续租在已知有效期内容忍短暂网络错误，超过安全期限停止执行。实际十分钟任务经历四秒断网后成功完成。

## 身份与 Agent

GitHub OAuth 使用 state、PKCE、服务端数字用户 ID 权限表及 HttpOnly 会话。生产 HTTPS 使用 Secure / __Host- Cookie；仅 development 配置下的回环 HTTP 地址使用独立开发 Cookie。Cookie 写请求校验 Origin。另有固定 issuer/audience/JWKS 的 RS256 API 身份验证。

DeepSeek 密钥只在 Daemon 中使用。Agent 仅可读写有界文本工作区、在禁网容器中执行测试、结束任务；最后一次写入后必须有通过的测试。Turn、工作区、调用次数与 token 预算落盘；已提交 Turn 可恢复，测试执行证据不完整时保留核对。跨宿主机自动迁移 Agent 工作区尚未开放。

详见 [GitHub 与 DeepSeek 配置](provider-configuration.md)。

## 更新与迁移

已完成迁移的集群必须同时保留四个 Compose 文件：

```bash
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml \
  -f deploy/dev-cluster/docker-compose.sandbox.yml \
  -f deploy/dev-cluster/docker-compose.providers.yml \
  -f deploy/dev-cluster/docker-compose.control-ha.yml up -d --build
```

先 drain 再更新 Daemon。不要对有在途任务的集群直接运行旧重启脚本。dev-cluster-up.sh 会依据忽略目录中的 provider 配置和已验证迁移标记选择 override；存在未验证迁移时拒绝回到旧权威。

迁移工具 scripts/migrate_body_to_fleet.py 要求两台 Daemon drained，先保存私有快照，导入后逐项比较；冲突目标不覆盖。恢复演练 scripts/verify_body_restore.py 将 fleet 快照还原到隔离的 celld 实例，保留线上权威。若要回退流量，须再次 drain、向全新命名空间反向复制并核对，再切换客户端；不能直接恢复已经落后的旧卷。

## 验证边界

已验证真实容器生命周期、租户隔离、DeepSeek 修改与测试、drain 跨重启、Daemon 崩溃回收、入口切换与真实任务执行，以及快照恢复。容量测量为固定小工作负载，不构成 SLO。容器 Engine 权限仍属于受信任开发宿主边界，不宣称敌对多租户隔离。
