# 最近代码与任务审查、开发环境重启

日期：2026-11-15。审查基线：当前 HEAD ff4ad9a 的最近两次提交与后续活跃工作区改动。重点为已交付的执行链路、Next.js 控制台、GitHub Profile、系统拓扑与开发部署；没有将全仓历史模块记为完整审计或全量测试通过。

## Standards：实现质量

独立审查：/root/recent_code_quality。

- P1，已修复：`ConsoleRbacAuthGuard` 的参数、claims 和 payload 丢失 Profile 字段，导致签名会话不含登录资料、跨副本/重启后重新依赖匿名 GitHub API。恢复 `ConsoleIdentityProfile` 接入及签发；资料与 OAuth 测试、重启后浏览器资料/头像检查均通过。资料仍不参与角色授权。

## Spec：需求符合度

独立审查：/root/recent_spec_review。

- P1，已修复：当前 Scheduler/Worker 源码缺失已交付的 execution、完成、续租、取消及 Body 路由。部分未知路由错误地返回成功，按当前源码重建会破坏已验收执行链路。与运行中 Gateway 保留的完整源码逐文件比较后，恢复 8 个相关模块：models、scheduler、lease_coordinator、cloudflare/worker、scheduler_cell、env、gateway/validator、console_rbac_guard。未整体覆盖其余存在差异的业务模块。
- P2，已修复：任务详情每3秒读取新数据，但状态和取消按钮依赖打开时的旧 category。现在依据最新 detail.state / execution_outcome 派生状态；浏览器验证在抽屉内取消后自动显示已取消并移除取消入口。

## 部署审查与修复

- 基础 Compose 已回到旧模拟进程定义，与健康的真实三节点 celld Fleet 不一致；启动入口也遗漏 provider/sandbox/control-ha overlays。根据现有容器配置恢复实际服务、端口、命令、健康检查与9个卷的原名称。保留迁移命名空间 body-ha-v1:，旧迁移任务置于显式 legacy-migration profile，重启不运行旧迁移。
- Nginx 回到了静态 docs fallback，缺少 Next.js、OAuth callback、HMR 和头像所需规则。恢复同源代理与开发 CSP，移除不需要的通配跨域头。
- `.gitignore` / `.dockerignore` 缺少私有目录保护。补齐后，私有凭据被忽略，已跟踪的私有文件数量为0。现用存储/观测凭据保留在本机0600私有 env 文件，没有写入版本化 Compose，也未轮换任何密钥。
- 原 Dockerfile 缺少 Docker CLI，且用 `|| true` 吞掉构建失败。改为 Node22、Docker CLI、锁文件安装及必须成功的转译构建。
- 原 down 脚本使用 `down -v`，会删除开发持久卷。统一为 `scripts/restart_dev_cluster.py` 的排空/停止/分阶段恢复流程，不删除卷；失败保留原始 Drain 意图与任务快照，重试续接。
- 恢复中发现生成的 Grafana 挂载路径错误，已改回 `deploy/observability/grafana-dashboard.json`。新增所有 bind source 的存在/类型预检；续接恢复时未重复停止已恢复的存储、Fleet 和执行器。

原始文件、运行配置和状态校验保存在本机忽略的私有目录；未提交任何改动或覆盖其它无关工作区内容。

## 重启与数据证明

- 原两台 Daemon 非 Drain、无在途沙箱/恢复阻塞。先持久 Drain，再停止入口/执行器、Fleet、存储与观测，随后分阶段恢复、重新部署 Worker 并恢复原始 Drain=false。
- 13 个常驻服务已恢复运行；Next.js、双 Gateway、Nginx、Fleet、Body 与存储均通过其健康检查，Grafana/Prometheus 由真实 HTTP 接口进一步验证。
- 9 个持久卷名称和现用存储/观测凭据在重启前核对一致。
- 重启前 116 条任务的 ID、租户与执行规格摘要在重启后全部匹配；3份既有完成结果摘要一致。
- 浏览器验收随后只在隔离租户新增并取消自己的验收任务，没有取消用户现存任务，也没有调用真实付费模型。

## 验证结果

- 固定 Node22 相关回归：50项中48通过、2项因需要真实沙箱暂跳过；随后在新执行器镜像中启用真实 Engine，Agent 工具/不可变结果/崩溃恢复文件的5项全部通过，覆盖之前2项跳过。没有将重复执行累加为额外独立用例。
- 后端转译构建、Next.js TypeScript 与生产构建通过；转译构建不等同于全仓严格类型检查。
- 控制台 13 项浏览器检查通过，包含真实容器提交/完成、日志、产物内容、取消及详情状态同步、错误恢复与退出。
- 拓扑 18 项浏览器检查通过，包含16组件/31依赖、暂停/刷新、旧快照保留、移动端依赖跳转、浏览器后退及 Viewer 拒绝全局详情。
- Profile 签名快照、实际头像加载、桌面/移动端、失败重试与退出验证通过。该资料浏览器测试使用私有签名测试凭据，未代替用户进行真实 OAuth 授权；OAuth Client ID、回调路径、PKCE 跳转及无效 callback 的服务端403防护已检查。
- Python/Bash 语法与相关 diff 空白检查通过。

## 仍存在的已知边界

Prometheus 服务可访问，但4个采集目标仍失败：两台 Daemon 的 `/metrics` 为401，两台 Gateway 为404。这是重启前已记录的问题，本次没有把它当作健康或已修复；系统拓扑当前为12正常、1降级、3未独立探测，Fleet 3/3、可用槽位2/2。

未运行完整历史套件、付费 DeepSeek 验收、生产发布或容灾演练。新版部署定义保留本机私有存储/观测 env 文件引用；新机器需要先配置这些文件与现有身份密钥，不能复制真实秘密进仓库。

## 后续开发操作

```sh
# 配置与 bind mount 预检，不修改容器
python3 scripts/restart_dev_cluster.py --check-only
# 重建、排空并安全重启现有开发集群
npm run dev:cluster:up
# 安全停止，保留所有持久卷与原始 Drain 意图
npm run dev:cluster:down
```

访问：http://127.0.0.1:8088 ，系统拓扑：http://127.0.0.1:8088/system 。
