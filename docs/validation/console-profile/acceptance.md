# GitHub Profile 验收 · 2026-11-15

- 7 项后端测试通过：原 OAuth state/PKCE/租户鉴权、当前身份资料读取、忽略其它用户查询参数、过滤非展示字段、上游身份不匹配/超大响应、签名资料快照无需再次调用 GitHub、UTF-8 Cookie 大小限制。
- Next.js TypeScript 检查及生产构建通过；开发 Compose 镜像已重建，Gateway/Nginx 已更新。
- 浏览器验证：侧栏用户名与头像、个人资料入口、GitHub 主页链接、应用角色、桌面/移动端、Escape 关闭、资料请求失败后重试、退出清空会话；无 pageerror 或横向溢出。
- GitHub API 首次实际返回该账号用户名 echo112400-beep，未设置昵称。后续观察到共享出口 403 限流，因此采用登录时取得的签名资料快照。最终浏览器测试使用私有短期签名凭据和已观察到的公开账号字段；缺省资料字段为测试空值。头像通过真实 GitHub 头像服务加载，已确认 naturalWidth > 0。不代用户执行真实 GitHub OAuth 浏览器授权。
- 旧会话需要在资料面板选择“重新连接 GitHub”，完成一次登录以写入快照。

[桌面](desktop.png) · [移动端](mobile.png)
