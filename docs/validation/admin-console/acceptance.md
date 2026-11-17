# 后台管理交付

入口 http://127.0.0.1:8088 ，深链接 /manage/queues、/manage/tenants、/manage/approvals、/manage/configs、/manage/runtime、/manage/observability、/manage/swarm、/manage/federation、/manage/security；节点与Cell复用系统拓扑，平台与任务保留既有工作台。

按 docs/index.html 12管理域组织，沿用ui/hrui v2配色、字体、分组侧栏、表格与抽屉。聚合API `/v1/console/management` 在服务端按身份过滤；最多返回1000条任务并标注截断，统计覆盖完整授权范围，不返回密钥。核对使用既有SRE接口，具备版本、原因、证据、确认及冲突保留输入。

15项相关后端测试通过，Next TypeScript与生产构建通过。真实浏览器验证导航、数据表、搜索、深链接与移动端；核对写请求被测试拦截为409，仅验证CAS字段与输入保留，没有修改任何现存任务的核对决定。真实核对接口行为由Body reconciliation测试覆盖。

独立视觉评审已完成并压缩重复统计；独立UX执行遇工具额度限制，未完成，交互为主代理实测。

未实现运行后端的范围：配额编辑/计费、配置版本发布回滚、通用双人审批、LayerFS写操作、历史SLO/自动自愈、算力市场、多地域迁移/纠删码证明、账号授权编辑与生产合规门控。这些页面展示实际可用信息与接入边界，不提供假成功按钮。不能将导航覆盖等同于上述能力全部实现。
