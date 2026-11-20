# 2026-11-18 工作树运行基线

控制面严格检查及 195 项测试通过。构建编译了 334 个模块。全仓 TypeScript 检查和 lint 通过。六个重点回归文件共 45 项通过，覆盖 M17、M22/M23、M32、M94、Fleet 客户端与 HA 持久性。

```sh
npm run test:control
npm run typecheck -- --pretty false
npm run lint
npm run build
node --test \
  dist/tests/m17_reliability_capacity_console_suite.test.js \
  dist/tests/m22_m23_cow_zerotrust_a2a_suite.test.js \
  dist/tests/m32_p0_mcp_hub_predictive_prewarmer.test.js \
  dist/tests/m94_git_branch_merge_and_contract_unification.test.js \
  dist/tests/celld_fleet_client.test.js \
  dist/tests/celld_ha_durability.test.js
npm test
```

M32 最初失败于内存 SQL 夹具对 `WHERE score > ?` 的处理。补齐参数化比较后，重点回归全部通过。lint 修正了参数属性默认值、必需参数位置及相对 TypeScript 导入的识别。

## 全量结果

338 项中 322 项通过、12 项失败、4 项跳过。以下 12 项在本轮修复前后的两次运行均失败，根因和首次引入这些失败的变更仍待确认。

| 数量 | 失败项 |
| --- | --- |
| 2 | celld Worker 项目暂存测试的 SQLite Durable Object 绑定清单断言。Google Engineering Standards 的源模块数量断言。 |
| 3 | M52 冷启动及状态恢复综合测试。M57 Daemon 租约续期失败。M69-2 冷启动中断租约容量退还及活跃任务指纹索引。 |
| 2 | M64-1 Scheduler/Harness Worker 路由及队列转发。M65-4 Tunnel Worker 路由。 |
| 2 | M69-6 final-closure-audit HTTP/CLI。M70 控制台 Day-2 BFF/CLI 联合测试。 |
| 3 | M76 Nginx 路由和 SSE 配置。M77 CSP、实时 SSE 及发布门禁。M93 本地 supervisor、Nginx 和 Console 后端发现接线。 |

本次验证使用工作树构建与本地测试夹具。QF-05 只读月结预览的隔离 celld 端到端请求仍待验证。
