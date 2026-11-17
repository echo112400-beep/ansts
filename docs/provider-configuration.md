# GitHub 与 DeepSeek 配置

## 已实现的接口

- GET /v1/auth/providers：查询 GitHub 登录是否配置。
- GET /v1/auth/github：进入 GitHub OAuth，使用 state 与 S256 PKCE。
- GET /auth/callback/github：兑换临时代码，验证 GitHub 数字用户 ID，签发 15 分钟应用会话。
- POST /v1/auth/logout：清除 HttpOnly 应用会话，检查 Origin。
- POST /v1/tasks/{id}/reconcile?tenantId=...：SRE_ADMIN 的具名、带版本与证据核对。

GitHub token 不返回浏览器、不进入任务容器、不持久保存。权限来自 ANSTS_GITHUB_IDENTITIES_FILE 中以数字 GitHub 用户 ID 为键的显式 role / scopeTenant 映射；登录名变化不会改变映射。

## 开发配置

导入明确指定的 env 文件，只复制 GitHub 配对凭据与 DEEPSEEK_API_KEY。默认优先选择用于网页登录的 TINYAGENT_VUE_GITHUB_ID / SECRET；没有该配对时使用 TINYAGENT_GITHUB_ID / SECRET。可用 --github-client-env-prefix 显式选择，Client ID 与 Secret 必须来自同一组：

```bash
python3 scripts/import_provider_credentials.py --env-file /path/to/existing.env \
  --github-user-id YOUR_NUMERIC_GITHUB_ID --origin http://127.0.0.1:8088
```

副本位于忽略的 .dev-secrets，文件权限 0600。初始用户为 default 租户的 SRE_ADMIN，不授予其他租户。需要调整时修改私有 github-identities.json 并更新 Gateway。

当前开发地址为 http://127.0.0.1:8088，OAuth 应用应登记完整回调：

```text
http://127.0.0.1:8088/auth/callback/github
```

用户需要在 GitHub 完成应用授权。既有 OAuth 应用若服务于另一项目，不应删除原项目的回调。生产部署使用独立 HTTPS origin；只有显式 development 且 hostname 是 127.0.0.1 / ::1 才允许 HTTP Cookie。

2026-11-15 修正：当前网页登录使用用户提供的新 OAuth App `Ov23li4C8jnqGmeeSo0h`，回调须登记在这个应用下。当前凭据由本项目 .dev-secrets 下的私有文件维护；外部 env 文件中的旧配对不代表当前配置。原先误用了另一组 `Iv1…` GitHub App 凭据。GitHub App 授权参数不发送 `read:user`，OAuth App 才请求这一 scope。

更新时保留 docker-compose.providers.yml 和已经采用的 control-ha override。DeepSeek API key 从 ANSTS_DEEPSEEK_API_KEY_FILE 读取；模型默认 deepseek-flash，可用 ANSTS_DEEPSEEK_MODEL 指定。Gateway 只有显式 ANSTS_AGENT_ENABLED=true 才接受 Agent 任务。两台 Daemon 都需配置模型。

## Agent 请求

execution.kind 为 agent，包含 image、timeoutMs、cpu、memoryMb、maxTurns（1–16）、maxTokens（1024–65536）和有界初始 files 文本映射。其余任务、租户、资源和超时字段沿用容器任务契约。服务端模型地址固定，任务不能指定请求地址或凭据。

模型每轮返回一个 JSON 动作：read(path)、write(path, content)、test(argv)、finish。工具名和路径由执行器验证，禁用外部副作用工具。test 在新的隔离容器中物化工作区，完成后采集快照；finish 必须在最后一次写入后有通过的测试。调用、预算及完整工作区写入 fsync Turn 检查点。未知测试结果不得自动重放。

UNCERTAIN 的 Agent 可以由具名管理员在有证据时使用 resume_from_checkpoint 及新 attempt/fence 申请恢复；仍有 pendingTest 的检查点拒绝自动恢复。检查点当前保留在原宿主持久卷，不承诺跨宿主迁移。

真实服务验收命令：python3 scripts/verify_deepseek_agent.py。该脚本会调用付费模型并提交一个受限代码修改任务。

## 服务密钥轮换

python3 scripts/rotate_body_service_key.py 准备新根密钥、派生 host/Gateway/operator 凭据及 15 分钟旧密钥宽限。工具不重启服务、不打印值。先发布接受双密钥的控制服务，再在宽限内重建客户端；已有短期执行凭证在相同窗口验证旧签名。过期后旧凭据被拒绝。轮换中途不要再次执行轮换。

生产 secret 分发应使用部署平台的 secret 管理，不能把开发文件复制当作生产 secret 管理验收。

## 接口核对来源

GitHub OAuth 参数与 PKCE 依据 https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps 。DeepSeek Chat Completions 依据 https://api-docs.deepseek.com/api/create-chat-completion/ 。可选 RS256 API 身份校验依据固定 issuer/audience/JWKS；它不实现另一套浏览器 OIDC 授权码流程。
