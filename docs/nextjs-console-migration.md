# Next.js 控制台迁移

浏览器入口保持 http://127.0.0.1:8088。GitHub 回调保持 http://127.0.0.1:8088/auth/callback/github。

## 布局与功能

采用任务工作台：固定侧栏、授权范围、四个真实统计、可搜索/分页/筛选的任务表、状态分布和任务详情抽屉。移动端改为双列统计和紧凑任务列表。首页不生成趋势、费用、CPU 使用率等没有数据来源的指标。

迁移原控制台的 GitHub 登录、内存短期凭据、身份/租户权限、自动刷新、任务列表与提交；补充容器与 Agent 提交、日志/结果/产物下载、取消请求和控制平面状态。只读身份不显示写入入口。SRE 的高级核对决策仍使用现有后端接口，页面仅提供待核对筛选和原始证据。

`ui/console/` 是独立 Next.js 15 / React 19 应用，沿用 `ui/` 的 Inter、JetBrains Mono、图标与纸白/蓝色基础。原 HarnessRouter 模板保留，未启用其 middleware 或服务端路由。

## 数据与身份

`GET /v1/console/tasks` 新增 `q`、`status`、`page`、`limit`，返回完整授权范围内的 `counts` 和执行能力，默认仍返回最近 50 条，兼容原客户端。统计在租户授权过滤后计算，搜索和分页不改变统计基数。执行中包含已分配和正在执行的任务；取消以 execution_outcome 为准。

Next.js 不接触 GitHub/DeepSeek 密钥。Nginx 将 `/v1/`、`/v1beta1/`、GitHub 回调和 `/healthz` 转发至双 Gateway；其余请求转发至 Next.js dev-server。原 HttpOnly 会话与写入 Origin 校验保持同源。短期凭据只保存在内存，退出或过期时取消未完成请求并清空工作区。开发 Nginx CSP 允许 Next.js HMR 所需的 WebSocket 与 eval；上线时应改用生产构建并收紧 CSP。

## 开发与部署

现有开发集群使用 `npm run dev:cluster:up` 安全重启（会校验私有配置并带上 provider/sandbox/HA overlays，先排空再分阶段恢复）。新机器需先配置私有存储/观测 env 文件与身份密钥。仅更新 Next.js：

```sh
docker compose -f ui/docker-compose.yml up -d --build dev-server
```

此命令要求现有集群网络已存在。局部 Compose 文件与主集群共享项目；不要使用 `--remove-orphans`，其它集群服务应保留。端口 3000 仅容器内部使用，不与宿主 Grafana 冲突。源码 app/components/lib 只读挂载以支持热更新，依赖固定在镜像。修改 package.json/lock 或配置后重建镜像。首次迁移 Nginx 配置后需通过主集群 Compose 重建 tanstack-console。

```sh
npm --prefix ui/console ci
npm --prefix ui/console run typecheck
npm --prefix ui/console run build
```

不要把 `.dev-secrets` 加入构建上下文或前端环境变量。GitHub 的真实用户授权动作由浏览器用户完成；自动验收使用独立租户短期凭据，不替用户授权 GitHub。

## GitHub 个人资料

侧栏使用 GitHub 昵称（没有昵称时使用用户名）、头像和 `@login`；点击用户区域可打开个人资料，查看公开简介、公司、所在地、加入日期、GitHub 主页、应用角色、租户范围和会话有效期，并退出账号。开发凭据用户保留独立的身份展示，不伪装成 GitHub 账号。

OAuth 回调把经过筛选的展示字段放入签名应用会话，`GET /v1/auth/profile` 只返回当前已验证身份的资料；不接受查询参数指定其它用户。展示字段不参与角色或租户授权，不保存 GitHub access token。字段按 UTF-8 字节限长以控制 Cookie 大小；头像由 Next.js 图片优化器加载，仅允许 GitHub 的头像域名与 `/u/` 路径。

旧会话没有资料快照时，按已验证的数字用户 ID 查询 GitHub 的[公开资料接口](https://docs.github.com/en/rest/users/users#get-a-user-using-their-id)，有界缓存 5 分钟；遇到 GitHub 限流仍保留登录和任务功能，并提供重试、重新连接 GitHub。重新登录一次即可获得资料快照，之后打开 Profile 无需重复调用 GitHub 资料 API。


## 系统拓扑详情

系统状态现使用 `/system`，展示完整配置拓扑与有界实时探测。SRE_ADMIN 可选择节点、追踪配置依赖、查看 Fleet/Daemon 容量与采集异常；其它角色保留基础连通性。支持暂停、手动刷新、搜索与失败保留快照。详见 [产品范围](system-topology/PRODUCT.md)、[技术说明](system-topology/TECH.md)、[验收记录](validation/system-topology/acceptance.md)。
