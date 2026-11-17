# 系统拓扑详情页验收 · 2026-11-15

入口：http://127.0.0.1:8088/system 。系统状态导航进入相同页面。当前角色 SRE_ADMIN 可查看全局只读拓扑。

## 已交付

16 个组件、31 条配置依赖：Nginx / Next.js、双 Gateway、三 celld Fleet、双 Daemon、RustFS、Prometheus、Grafana、兼容 Body 代理、GitHub、DeepSeek、Daemon 本地 Engine。Fleet 内状态对象单独解释，避免与物理实例混淆。

点击节点可查看直接依赖、角色、探测地址、时间、HTTP 状态、耗时、Fleet 对象/内存/激活指标、Daemon 槽位与资源预留、Prometheus 各目标的采集证据。支持全景/执行主链、异常定位、实例筛选、暂停/手动刷新、失败保留旧快照。

## 验证

- 后端构建与 12 项相关测试通过；其中5项覆盖新增拓扑：鉴权先于探测、仅使用服务端地址、字段筛选、抓取降级、部分超时、并发合并、容量缺失、配置/响应体边界。
- Next.js TypeScript 检查与生产构建通过，包含 `/system` 深链接。
- 真实浏览器 18 项检查通过：
- authenticated-deep-link
- all-configured-nodes
- desktop-no-overflow
- scrape-evidence-and-directed-dependencies
- executor-and-fleet-telemetry
- topology-view-switch
- inventory-search-and-issue-filter
- pause-stops-polling-manual-refresh-works
- failed-refresh-retains-timestamp-and-data
- retry-recovers
- mobile-overflow
- mobile-exception-drilldown
- dependency-switch-resets-scroll-and-shows-title
- sticky-close-and-Escape
- navigation-and-back-preserve-session
- viewer-has-basic-status-only
- server-rejects-viewer-topology
- no-browser-errors

- 1440×900 与390×844，无横向溢出、无浏览器 pageerror。
- 独立灰度审查：/root/system_layout_review，采用异常优先结构。
- 独立视觉审查：/root/system_visual_review；加强依赖线对比和箭头、语义文字，并让“查看异常”滚动定位。
- 独立只读 UX：/root/system_ux_review；发现移动端依赖跳转保留旧滚动位置，已修复为固定身份/关闭栏、切换回顶，并经浏览器回归。
- 仅更新双 Gateway、Next dev-server 与 Nginx reload；未停止 Daemon、改动任务或调用外部模型。
- Next dev-server、Nginx 与双 Gateway 均 healthy；`/system` HTTP 200。

## 当前现场证据与边界

当前13项进行了探测，12项正常，Prometheus 1项功能降级；3项逻辑/外部依赖未独立探测。Fleet 3/3、执行槽位2/2可准入。

Prometheus API 返回 HTTP 200，但4个采集目标均失败：两台 Daemon `/metrics` 返回401，两台 Gateway `/metrics` 返回404。页面展示这些真实问题，没有修复原有采集链路，也没有画成全绿。

连线是配置调用关系，不是网络抓包、流量或逐跳链路测试。CPU/内存来自预留账本，不是宿主实时利用率。Engine、GitHub、DeepSeek 只展示配置；不推断健康。逻辑对象不逐对象读写探测，不宣称副本数、EC:4 或 RPO/RTO。普通角色仅保留基础连通性，不获取全局拓扑地址与容量。未验证真机触控/读屏或长期运行。

[桌面首屏](desktop.png) · [桌面完整页面](desktop-full.png) · [移动端](mobile.png) · [移动端详情](mobile-detail.png) · [执行器详情](executor.png)
