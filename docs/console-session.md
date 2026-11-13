# 控制台登录与真实任务工作区

入口：http://localhost:8088/ 。默认页面只连接已实现的身份、任务查询和任务提交接口，不展示尚未接入的旧管理指标。原管理页面代码保留在 `legacy-management-console.html`，旧演示身份与固定 JWT 已移除，历史渲染器默认停用。

## 开发环境登录

凭据准备、构建与集群启动后，使用 Node.js 22 签发明确角色和租户的短期凭据：

```bash
node scripts/issue_console_dev_token.mjs --tenant tenant-alpha --role VIEWER --subject operator-name
```

凭据写入忽略的 `.dev-secrets/console-login.jwt`，有效期 15 分钟。复制文件内容到登录框；工具不打印凭据值。OPERATOR 可提交授权租户任务；VIEWER 仅查看。需要跨租户权限时必须由签发方明确指定 `--tenant all`。

登录调用 `GET /v1/auth/me` 验证签名并获取用户、角色、租户、到期时间和写入能力；身份信息不从浏览器自行解码的 JWT 或 URL 参数获取。列表调用 `GET /v1/console/tasks`，仅返回授权任务，最多显示最近 50 条；提交使用现有 dispatch API。

凭据仅保存在页面内存，不写 localStorage、sessionStorage、URL 或 Cookie。刷新需要重新登录。退出和过期清除身份、任务和输入，终止等待中的请求，并阻止晚到响应重新填回旧租户内容。退出仅清除当前页面会话，不代表服务端吊销其他页面或已签发凭据。

轮换宽限中的旧凭据被服务端标记为只读，页面隐藏提交入口。生产 OIDC 尚需具体身份提供方的 issuer 和客户端配置；此入口是开发凭据流程，不是密码登录或公共发证服务。

## 验证记录

- 后端相关测试：32 项通过、1 项独立 celld 二进制测试跳过。
- `scripts/verify_console_auth_dom.mjs`：无效凭据、服务端身份、租户过滤、VIEWER 隐藏写入、OPERATOR 提交、退出清理、晚到响应丢弃、凭据到期和不持久保存均通过。该脚本需要 jsdom，可在临时 Node 容器安装后运行，不修改项目依赖。
- `scripts/verify_console_deployment.mjs`：部署静态资源、身份接口、只读能力和租户边界通过。
- 新增 UI 源码检查：0 FAIL、0 WARN。浏览器访问宿主不可用，尚未完成真实浏览器截图、尺寸与键盘交互核验。

样式采用 Framed sheet（seed 1302272691），保留 ANSTS 字体与中性色盘，没有重抽方向。页面首先回答“当前身份能访问哪些任务”，以用户/角色/租户行和真实任务数构成主要信息；没有样例业务数据或性能数字。
