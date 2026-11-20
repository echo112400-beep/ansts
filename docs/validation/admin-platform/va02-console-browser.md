# VA-02 控制台浏览器验收

`scripts/verify_tenant_finops_browser.js` 在隔离 Next 控制台配合 API 夹具执行。Chromium 验证 Viewer 390 px、Operator 390 px、SRE 管理员 1280 px。三种角色均可键盘展开费用明细；Operator 和 SRE 管理员可下载 CSV，Viewer 不显示导出入口。非管理员无配额改写入口；管理员可预览当前占用，模拟 409 后保留编辑输入。预留服务部分失败不遮蔽用量和费用，费用未知显示待核算。各视口无横向溢出。

截图：[窄屏 Viewer](va02-finops-viewer-390.png)、[桌面 SRE 管理员](va02-finops-admin-1280.png)。本轮浏览器使用模拟响应，API 权限与租户隔离另由控制面集成测试覆盖。后续复验见下文。

审批分离复验：scripts/verify_config_approval_browser.js 通过。提交人看到自审禁止说明，批准按钮不可用；独立审批人用 Enter 批准指定 revision 和 sequence，浏览器仅发送一次写请求。此为 API 夹具下的页面证据。

旧控制台路径复验：scripts/verify_legacy_console_browser.js 通过。未登录时显示 GitHub 登录入口，短期开发凭据经夹具认证后进入工作区，键盘可打开并退出个人资料和任务详情；系统页读取拓扑快照并显示全景控制。未访问真实 GitHub 或对共享集群创建任务。

费用角色复验：按真实 Gateway 的写入角色边界更新 Chromium 夹具和截图，VIEWER:390、OPERATOR:390、SRE_ADMIN:1280 均通过。隔离 celld 已独立验证授权创建后自动后台生成（202 PENDING → READY）及 CSV 下载。

空态与独立失败复验：Chromium 390 px Viewer 在用量 API 返回 503 时看到用量读取失败，成功的空费用响应显示「尚无费用事件」，预留占用继续可读，导出入口无权限时隐藏。原有三角色场景同轮通过。
