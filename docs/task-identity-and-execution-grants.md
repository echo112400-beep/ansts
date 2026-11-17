# 租户任务标识与执行凭证

新任务内部 ID 为 `ansts:v1:` 加 `[tenantId, taskId]` JSON 的 base64url 编码。使用结构编码避免分隔符碰撞；Scheduler、Gateway 持久行、Body Cell 寻址、执行日志与结果队列沿用这个内部 ID。

公开提交仍接受原 `taskId`/`task_id`，响应同时返回 `executionTaskId`。读取使用 `GET /v1/tasks/<公开ID>?tenantId=<租户>`；同名记录超过一条而未指定租户时返回 `TENANT_REQUIRED`。Body 管理和核对接口使用 executionTaskId。已有未编码记录按同一租户的原始 ID 保守复用，避免重新提交时重复创建。

这些规则隔离存储命名空间。用户权限另外由 [Gateway 用户授权](gateway-user-authorization.md) 校验；生产身份提供方与控制台登录尚未接入，不能把开发部署视为生产身份系统。

执行开始后控制面签发 30 秒凭证，绑定租户、任务、尝试、宿主机、fence 和输入摘要。Body 的固定服务凭据只用于身份认证，不能替代这张执行凭证。执行入口调用控制面验证签名、时间和共享 RUNNING 状态，并在 Durable Object 中消费单次 nonce；过期、篡改、身份不符或重放均被拒绝。

恢复重试可以申请新凭证，但仍受共享执行状态和本地独占日志约束，不会因新凭证而放行不确定副作用。旧尝试不能使用凭证启动另一尝试。已消费 nonce 当前随执行记录保留，自动归档与密钥轮换尚未实现。

Scheduler 的 register-node、lease-renew、complete 和 schedule-tick 服务路径要求 Body 或 Gateway 身份；Body 更新任务前校验宿主机归属。旧 Harness/Tunnel/管理接口的统一权限接入仍在后续范围。

独立启动器可以通过 `ANSTS_BODY_AUTH_SECRET`（Scheduler）和 `ANSTS_SCHEDULER_SERVICE_TOKEN`（Gateway）向暂存 `.dev.vars` 注入服务配置；不得把凭据提交到仓库。Compose 启动前运行凭据准备脚本，新增 Gateway 凭据与现有 Body 凭据存放于忽略的 `.dev-secrets/`。

验证命令：`node scripts/verify_tenant_task_isolation.mjs`，要求在能访问开发集群端口且有 Node.js 22 的环境中运行。
