# Next.js 控制台验收 · 2026-11-15

入口：http://127.0.0.1:8088 。部署为 Next.js 开发服务器 + Nginx 同源代理。

## 已验证

- 独立应用 TypeScript 检查通过；Next.js 15.5.28 生产构建通过；后端构建通过。
- Gateway 与 GitHub 身份相关测试：4/4 通过，包括租户隔离、只读写入限制、授权统计、分页/搜索、OAuth state/PKCE。
- 真实浏览器自动验收：13 项通过；仅操作隔离租户 console-ui-acceptance。
- login
- container-submit
- scope-search
- completion
- logs
- artifact-download
- refresh-failure-retains-data
- cancel-own-task
- empty-filter-recovery
- mobile-overflow
- navigation-health
- logout-clear
- no-page-errors

- 真实容器输出 console-browser-ok，产物下载内容 console-artifact-ok；取消仅作用于同次运行创建的任务。
- GitHub 授权跳转的当前 Client ID 和回调 URL 校验通过。
- HttpOnly Cookie 自动进入工作区、同源 POST、退出清除 Cookie 均通过。
- 1440×900 / 390×844 截图检查，无横向溢出；浏览器无 pageerror。
- Nginx 配置检查通过；dev-server、Nginx、双 Gateway 健康。
- 独立视觉评审最终 8/10。独立 UX 走查实际创建 Trace 并等到完成；发现的 Trace 表单与无日志提示已修正。

## 边界

没有替用户完成 GitHub 真实账号的浏览器授权；没有通过 UI 再次消费真实 DeepSeek 模型服务。Agent 创建入口使用现有后端执行能力，生产身份映射和密钥保留在私有服务器配置。高级 SRE 核对决策继续使用既有后端接口。静态视觉评审未证明真机触控或动效手感。

## 截图

- [桌面总览](desktop.png)
- [手机总览](mobile.png)
- [创建任务](create.png)
- [任务详情](detail.png)
