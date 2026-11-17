# M0 运行链路与契约基线

本页记录已部署链路；后续QF/CF/MK/RG接口不在M0已实现清单内。机器可读请求定义见 [contracts.schema.json](contracts.schema.json)，通过operations.requestRef选择对应$defs。保留未知字段兼容，但服务端只读取白名单字段，忽略请求体actor。

## 实际运行路径

| 层 | 当前入口 / 权威 | 源码 |
| --- | --- | --- |
| 浏览器入口 | :8088 Nginx，根路径到Next，/v1到双Gateway | deploy/dev-cluster/nginx-tanstack-console.conf |
| Gateway | gateway_worker → handleUnifiedGatewayRequest；验证JWT/会话、Origin和范围 | src/cloudflare/gateway_worker.ts、gateway_cell.ts |
| 调度 | 远程schedulerFetch → Fleet worker → global-shard-0 SchedulerCell | src/cloudflare/worker.ts、scheduler_cell.ts |
| 执行权威 | Body兼容入口代理到Fleet；body-ha-v1:execution:*对象 | src/cloudflare/body_worker_routes.ts、body_cell.ts；control-ha overlay |
| 原生执行 | BodyDaemon验证执行授权/lease/fencing，再写本机journal与资源预留 | src/runtime/body_daemon_server.ts |
| Engine/Agent | ContainerSandbox.execute；AgentToolLoop使用同一Engine能力 | src/runtime/container_sandbox.ts、agent_tool_loop.ts |
| Trace兼容执行 | runBodyExecution启动独立进程组，取消后等待退出 | src/runtime/body_execution_process.ts |
| 结果 | result-outbox持久投递，Body确认状态；UNCERTAIN保留核对流程 | scripts/body_cell_agent.mjs、src/runtime/body_result_outbox.ts |
| M0目录 | 双Gateway → 专用凭据 /internal/v1/directory/* → tenant-directory-v1 | src/control/admin_directory_api.ts、src/cloudflare/tenant_directory_cell.ts |

旧src/dashboard/console_bff_api.ts与server.ts不承载8088的新管理域写接口。它们存在历史类型/导出不一致，不能以原型方法存在证明功能已部署。

## 身份、版本与权限

tenant_id/task_id/attempt_id/node_id标识不同实体，不互相代替。fencing_token是正安全整数；同一个任务重试的新attempt不是旧attempt的重复事件。执行输入摘要由实际生产者规范JSON后SHA256生成；现有底层newAttempt验证非空字符串，不能声称所有入口均强制64hex格式。

目录对象version从1开始；新增请求隐含expectedVersion=0，更新必须显式给出expectedVersion。额度计量使用CPU milli-core与MiB；Daemon资源预算仍为CPU cores/memoryMb，未来接入时显式转换，不能直接混用。credit为内部整数单位，不是钱包或支付金额。

读取遵守已验证tenant和region范围；VIEWER/OPERATOR不得管理目录；租户范围SRE不能写全局成本中心或导出全局归档。管理审计仅SRE可读，成本中心审计/归档要求全局范围。历史操作还验证其原始与当前地域范围。UI隐藏按钮只是交互提示，服务端为授权权威。

## 验证对应关系

- tests/control/contracts.test.ts：权限、配额单位边界、CAS、credit。
- tests/control/tenant_directory.test.ts：完整三角色/租户/地域矩阵，归属与停用，版本/幂等，跨日跨月，审计/操作/归档事务与恢复。
- tests/control/admin_directory_api.test.ts：真实Gateway认证、身份白名单、远程调用与不回退写入。
- scripts/verify_directory_celld.py、verify_directory_gateways.py：原生持久化、多对象/多进程同权威并发与故障恢复。
- scripts/verify_directory_deployment.mjs：实际双Gateway API/浏览器联调与独立主机文件归档。
- scripts/verify_monitoring_deployment.mjs：4目标采集、监控只读权限、准入计数与服务端观测门禁。

结构核验采用codebase-memory Tier2、定向调用链和直接源码读取，已核验依赖路径覆盖。图谱不是跨HTTP真实运行的证明；实际请求证据见部署验收与上述脚本结果。

## QF-02 影子服务补充（2026-11-16）

TenantControlCell现承载持久预留内部接口，Fleet使用独立协调服务身份授权。凭证用途为ansts-admission-shadow-v1，需要实时权威状态验证；不是现有Body执行grant。接口见 admission.schema.json。实际影子流程、三入口一致读取及Fleet重启恢复已验证，真实执行准入仍未接入；不能用这份影子服务契约宣称硬配额或用量计费上线。

QF-02后续增加admission-outbox和admission-dispatch内部入口，以及独立影子邮箱对象。原生租户事务提交预留与outbox；收件箱去重、墓碑和回执在接收对象事务内持久化。投递回执只证明记录，不能证明容量可用、执行启动或终止；自动补偿调度仍未接入。真实重启恢复证据见 ../validation/admin-platform/admission-messages-acceptance.md。

QF-02自动影子投递现由原生alarm驱动，新增delivery-status/control内部接口。启用对象有60秒持久心跳，变更前注册唤醒，暂停/恢复采用SRE+CAS+幂等。真实强制终止（隔离项目）及开发Fleet重启恢复已验收；这是消息投递自动恢复，不代表真实Engine/容量补偿已完成。见 ../validation/admin-platform/admission-automatic-acceptance.md。
