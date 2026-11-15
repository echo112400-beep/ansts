# 实现约定

Gateway 新增只读 `/v1/console/system`，先验证身份与 SRE_ADMIN 角色，再调用独立聚合器。服务拓扑来自服务端 `ANSTS_SYSTEM_TOPOLOGY_JSON`，开发启动脚本从版本化 JSON 文件加载；不接受用户传入探测 URL、任务或凭据。不修改 Daemon 或停止在途任务。

聚合器并行、有时限、有响应体大小上限地读取健康端点；Fleet 补读内部 `/state`。仅映射白名单指标和规范化采集错误。5秒缓存与并发合并避免重复扇出；节点异常是快照内容而非全接口失败。未主动探测的逻辑/外部依赖保持“未探测”。

拓扑 JSON 的节点、分层与连接来自当前四份 Compose 及真实代码。连接属于配置证据，健康端点属于现场观测；二者在界面中区分。Next.js 原生组件实现交互与响应式，复用现有 token/icon，不嵌入独立图表 HTML。`app/system/page.tsx` 复用控制台壳；状态导航支持路径与内存身份保持。

部署仅重建 Next dev-server 和双 Gateway，Nginx reload；保留 providers/sandbox/control-ha overlays 和现有执行服务。
