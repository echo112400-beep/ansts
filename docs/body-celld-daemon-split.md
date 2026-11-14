# Body celld 与原生 Daemon 拆分

更新：2026-11-14。

Body 控制状态由独立 `src/cloudflare/body_worker.ts` 承载，绑定 `AnstsBodyCell`。原生 `src/runtime/body_daemon_server.ts` 管理执行进程、停止、工作区快照和执行日志，不再实例化通用 Node 主机中的 Scheduler、Gateway 或控制台。

请求路径：Gateway → Scheduler；Body agent → Body celld → Scheduler；Body agent → 本机 Daemon → Body celld 校验准入 → 独立执行进程。

## 接口和持久性

- Body celld 保持 `/v1/body/*`，验证 host 身份；对 Scheduler 仅代理执行所需的允许列表。控制模块访问 Scheduler 使用单独派生的 BodyControl 身份。
- Daemon 提供认证的 `/v1/sandboxes`、`/v1/sandboxes/execute`、`/v1/sandboxes/stop`。停止等待进程退出，未确认停止返回 503。
- `/health` 表示原生进程存活；`/readyz` 同时检查远程控制模块。控制模块失联时，执行准入失败，正在执行的工作在续租失败后取消。
- Body celld 数据使用 `body_control_data` 持久卷；Daemon 各自保留执行日志、结果 outbox 和按租户任务隔离的工作区快照。
- Brain 的 `/v1/body/*` 配置独立地址后仅代理，远端故障不退回旧状态。旧 Body 绑定作为兼容与迁移来源保留。

## 开发部署和迁移

统一入口：`bash deploy/dev-cluster/dev-cluster-up.sh`。入口先停止旧 Daemon，再启动控制模块、迁移作业和两个原生 Daemon。Compose 共 14 个服务，其中 `celld-deploy` 和 `body-state-migrate` 为一次性作业。

Body celld 端口 `18085`，原生 Daemon 为 `19501/19502`。独立启动入口为 `scripts/celld_body_up.sh` 和 `scripts/start_body_daemon.mjs`。Body celld 需要根认证密钥及 Scheduler 地址；Daemon 需要本机派生 token 和 Body celld 地址。

迁移作业枚举 Scheduler 任务和配置的 host，从 Brain 的操作员专用旧状态导出接口读取记录，将执行状态、审计及已消费准入 nonce 导入新存储。非空目标不覆盖；持久待完成标记支持中断后继续。自定义 host 需同步配置迁移清单。迁移时必须停止所有旧执行代理。

旧存储保留用于核对。新模块开始写入后，不能直接切回旧存储，否则会丢失新的准入记录；回滚必须先停止执行并迁移最新状态，或继续保留新的控制模块。

## 验证与限制

`python3 scripts/verify_body_split.py` 验证任务完成、控制模块重启后的状态、Daemon 独立重启，以及控制模块缺失时拒绝准入。回归入口还有 `verify_body_attempt_protocol.mjs`、`verify_tenant_task_isolation.mjs` 和 `verify_body_restart_reconciliation.py`。

当前 Body celld 是带持久卷的单实例 `celld dev`，尚未形成高可用 fleet。执行隔离仍使用进程组与 LayerFS 工作区，尚未实现容器或 MicroVM 的安全沙箱；外部副作用仍需要幂等与 fence 校验。

本轮部署迁移了 18 个非空旧状态对象，跳过 24 个空对象。构建通过；相关回归 25 项通过、1 项因缺少本机 celld 配置跳过；实际容器验证覆盖任务完成、租户同名任务隔离、控制台认证、控制模块重启、Daemon 中途重启和核对审计。
