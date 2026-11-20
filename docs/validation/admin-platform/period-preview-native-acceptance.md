# QF-05 只读月结预览隔离验收

2026-11-18，使用 celld 0.6.3 和原生 SQLite Durable Objects，通过真实 Gateway 入口请求只读月结预览。Gateway、租户权威、Scheduler 和 Body 绑定在同一个隔离 celld 实例。Docker Engine 运行本地执行夹具；额外清单行、并发水位变化和扫描超限由隔离探针注入。开发环境、独立进程间部署路径和线上数据均未参与本轮验收。

复现命令为 python3 scripts/verify_body_evidence_native.py。脚本退出码为 0，执行后清理了临时容器。

| 场景 | 实际结果 |
| --- | --- |
| 无凭据、跨租户、仅有单区域权限 | 分别返回 401、404、403。 |
| 管理员 POST 客户端覆盖和读取已关账月份 | 分别返回 404 和 409 PERIOD_ALREADY_CLOSED。GET 注入覆盖参数时仍使用可信清单，版本号和总额与正常请求一致。 |
| Scheduler 清单超过 512 条，存在真实 Body 执行 | 公共预览返回 200；其覆盖水位与服务鉴权诊断一致，诊断命中真实 Body 的清单水位。 |
| 续页前新增观察，或 Body 读取后重查首页时新增观察 | 均返回 SCHEDULER_INVENTORY_CHANGED，总额为 null。 |
| 历史不足或观察水位未到月末 | 分别返回 HISTORICAL_COVERAGE_UNAVAILABLE 或 INVENTORY_WATERMARK_BEFORE_PERIOD_END，总额为 null。 |
| 超过 1000 个 Body 候选 | 返回 BODY_DISCOVERY_LIMIT，总额为 null。 |
| 结算事件顺序表超过 10000 条 | 返回 503 SETTLEMENT_SCAN_LIMIT，清除夹具后恢复同一预览版本。 |
| 读取副作用 | 准入状态、关账及调整记录在请求前后逐行相同。 |

严格控制面 195 项、全仓类型检查和 lint 均通过。QF-05 保持进行中，可信实际成本分摊、预留结算、预算释放和正式关账继续验证。
