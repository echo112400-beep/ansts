# Gateway 用户授权与开发密钥轮换

除健康接口外，Gateway 已实现的数据与任务接口要求 `Authorization: Bearer <JWT>`。未配置 `ANSTS_USER_AUTH_SECRET` 或缺少有效凭据时返回 401，没有默认用户或默认签名密钥。

JWT 使用独立用户签名密钥验证 HS256，要求有效的 sub、role、scopeTenant、iat、exp。支持 VIEWER、OPERATOR 和 SRE_ADMIN。VIEWER 禁止所有写操作；scopeTenant 为具体租户时仅能提交、读取和管理该租户的数据，all 表示由签发方明确授予跨租户范围。

任务查询根据已认证租户过滤；越权显式 tenantId 返回 403。Gateway 状态与总览只返回授权租户的任务、提交和租约；租户用户不能读取其他用户会话。租约写操作仍需现有确认字段与操作权限。后台 Brain 的 Scheduler/Harness/Tunnel/旧 Brain 数据路径要求服务身份，用户 JWT 不能用于绕过 Gateway。

开发凭据准备脚本生成 `.dev-secrets/user-signing.key`，Gateway 启动时注入独立签名配置。`scripts/gateway_dev_identity.mjs` 为本地测试生成 15 分钟有效凭据，不提供公共登录或发证端点，也不把凭据输出到日志。控制台的用户登录/安全会话交互尚未接入；旧页面不能依赖匿名 API 或固定演示 token。

## 开发用户密钥轮换

```bash
python3 scripts/rotate_gateway_user_key.py
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --force-recreate ansts-gateway-01 ansts-gateway-02 tanstack-console
```

脚本原子写入新密钥，保存前一密钥和 15 分钟宽限截止时间。旧密钥仅可验证轮换前签发的读取凭据；所有写操作立即要求新密钥签发的凭据。宽限结束后旧凭据拒绝。宽限期内再次轮换会被脚本拒绝，避免静默撤销已有读者。

此命令只轮换开发用户签名密钥，不轮换 Body 根密钥、管理员核对凭据或 Gateway 服务凭据。生产 IdP/OIDC、多人管理、撤销列表和服务根密钥滚动轮换仍需独立实现。

实际部署验证：匿名查询拒绝、跨租户访问拒绝、VIEWER 写入拒绝、同名租户任务分别完成，以及轮换前凭据只读/新凭据写入均已通过。
