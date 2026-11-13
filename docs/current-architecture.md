# 当前代码架构与开发部署

核对日期：2026-11-13。代码基线：`8eef81e`。本文描述已接入的运行路径；早期设计中的容量、延迟、RPO/RTO 和生产安全指标不构成当前验证结论。

## 进程与状态边界

| 组件 | 当前实现 | 状态与边界 |
| --- | --- | --- |
| Gateway | `src/cloudflare/gateway_worker.ts`、`gateway_cell.ts` | 独立 celld Worker，Gateway Cell 保存提交、租约和会话；经 HTTP 调用 Scheduler |
| Scheduler | `src/cloudflare/worker.ts`、`scheduler_cell.ts` | 三节点 celld fleet 承载 Scheduler、Harness、Tunnel、Body Cell；默认调度对象为 `global-shard-0` |
| Body Cell | `src/cloudflare/body_cell.ts` | 按 host 保存心跳与回执；按 `execution:<taskId>` 保存共享执行准入与完成结果 |
| Body 执行代理 | `scripts/body_cell_agent.mjs` | 在原生 Node 进程中向 Scheduler 注册容量、轮询任务、续租、调用本地执行接口并回报完成 |
| 本地执行器 | `src/server_main.ts`、`runtime/body_execution_journal.ts` | LayerFS 工作区执行与宿主机文件操作；执行日志位于 Body 持久卷 |
| 控制台 | Nginx、`docs/` | 静态页面和 API 代理；页面中存在的功能不代表独立 Gateway Worker 已实现对应后端路由 |

当前 Compose 有 12 个 service，其中 `celld-deploy` 为一次性部署作业。两个 Gateway 使用 `celld dev`，各自持有容器临时目录中的独立状态；它们不组成共享 Gateway 状态集群。Brain 才是使用对象存储发布的三节点 fleet。

## 提交、执行与恢复

1. Gateway 校验任务，经 `/v1/scheduler/submit` 提交并触发调度 tick，保存 Gateway 提交记录。
2. Body 代理向同一调度对象注册真实容量并轮询分配给自己的 `LEASED` 任务；Gateway 不再替 Body 驱动执行。
3. 代理验证租约并向任务共享 Body Cell 申请执行准入。未完成记录仅允许同宿主机、同 fence 重试；其他宿主机或 fence 被阻止。已有完成结果可用于完成新分配，无需再次执行。
4. Body 执行入口再次向 Scheduler 校验租约，使用本地独占、fsync 日志防止重启后的自动重放。当前执行契约是在 LayerFS 工作区写入任务 trace，并提交快照；不是完整的长周期 Agent 工具循环。
5. 执行期间代理每 3 秒续租并更新心跳；执行完成后依次保存共享完成结果、host 回执，再完成 Scheduler 租约。
6. Gateway 查询租约时结合 Scheduler 的终态计算 `RELEASED`；这是读取视图的协调，不会改写原始 Gateway 租约行。

## 故障语义与已知限制

- host 心跳超过 15 秒时 Body Cell 查询显示离线；调度容量与任务恢复仍由 Scheduler 自己的节点、租约策略决定。
- 本地执行日志存在但没有完成结果时返回 `EXECUTION_OUTCOME_UNCERTAIN`。共享准入没有完成结果时禁止跨宿主机自动重跑；当前没有人工核对/解除准入的管理 API。
- HTTP 请求取消或租约续租失败不能撤销已经发生的宿主机副作用。外部工具需要自己的幂等键与 fence 校验；不承诺严格 exactly-once。
- 续租覆盖执行 HTTP 请求等待期，结果回传失败依靠后续轮询重试。原生同步操作不能由该机制强制终止。
- Body Cell 路由和本地执行入口已接入开发用宿主机凭据。Scheduler 服务写接口、完整租户授权和凭据轮换仍待接入；当前不是生产执行协议。
- Body 默认固定访问 `celld-brain-01:8081`；celld 对象可以跨节点路由，但客户端入口故障自动切换尚未实现。
- Gateway Worker 只实现代码中显式路由的任务、健康、Gateway 状态和租约管理接口。旧 Node host 中的控制台、隧道等接口不能据此视为已经迁入 Gateway Worker。
- 根目录 `docker-compose.yml` 仍使用 Node host 入口；开发集群必须显式选择 `deploy/dev-cluster/docker-compose.dev-cluster.yml`。

## 开发部署与验证

```bash
python3 scripts/prepare_body_dev_credentials.py
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --build
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --force-recreate celld-deploy
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml ps -a
curl -fsS http://localhost:18080/health
curl -fsS http://localhost:18081/health
# Body 状态现在需要宿主机服务凭据；匿名访问返回 401。
```

Gateway 源码更新后需重建其容器以重新暂存 Worker；Brain 源码更新后需重跑一次性部署服务。Body 镜像需重新构建；开发配置单独挂载代理脚本，修改后重启 Body 进程生效。

入口：控制台 `8088`；Gateway `18080/18081`；Brain `18082–18084`；Body `19501/19502`；RustFS `9000/9001`；Prometheus `9090`；Grafana `3000`。

已有验证记录：TypeScript 构建通过；Body Cell/执行日志/Gateway 相关 6 项测试通过；实际提交任务达到 `COMPLETED`，Gateway 查询租约为 `RELEASED`。这些记录不等同于全套测试、长任务故障注入、生产容量或安全验证。

第一阶段已开始实施，新增执行尝试状态、版本与不可变结果；旧记录缺少身份和输入证据时转为待核对。最新验证与剩余范围见 [推进记录](phase-one-progress.md)。
