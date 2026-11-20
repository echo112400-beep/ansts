# M3 开发 Fleet 候选发布输入

由当前 esbuild 依赖图与 Git 工作树生成。以下仅列出两个入口实际打包的已修改源文件；路径重叠不表示改动全部属于 M3。未发布。

## Scheduler：42 个路径

- `src/cloudflare/body_cell.ts`
- `src/cloudflare/body_worker_routes.ts`
- `src/cloudflare/capacity_inventory_cell.ts`
- `src/cloudflare/config_preflight_api.ts`
- `src/cloudflare/config_release_cell.ts`
- `src/cloudflare/config_rollout_targets.ts`
- `src/cloudflare/gateway_cell.ts`
- `src/cloudflare/scheduler_cell.ts`
- `src/cloudflare/scheduler_config_target.ts`
- `src/cloudflare/tenant_control_cell.ts`
- `src/cloudflare/tenant_directory_cell.ts`
- `src/cloudflare/worker.ts`
- `src/cluster/capacity_inventory.ts`
- `src/cluster/capacity_queue.ts`
- `src/cluster/capacity_request_store.ts`
- `src/config_release/config_approvals.ts`
- `src/config_release/config_preflight.ts`
- `src/config_release/config_rollout.ts`
- `src/config_release/config_target_application.ts`
- `src/config_release/config_versions.ts`
- `src/control/admin_capacity_api.ts`
- `src/control/admin_directory_api.ts`
- `src/control/admin_finops_api.ts`
- `src/control/admission_reservations.ts`
- `src/control/body_usage_ingress.ts`
- `src/control/capacity_ingress.ts`
- `src/control/execution_inventory.ts`
- `src/control/execution_inventory_ingress.ts`
- `src/control/tenant_directory.ts`
- `src/dashboard/console_rbac_guard.ts`
- `src/finops/authority_archive.ts`
- `src/finops/execution_coverage.ts`
- `src/finops/finops_page.ts`
- `src/finops/period_settlement.ts`
- `src/finops/rate_cards.ts`
- `src/finops/tenant_rate_store.ts`
- `src/finops/usage_cost_ledger.ts`
- `src/finops/usage_inbox.ts`
- `src/models.ts`
- `src/saga/staging.ts`
- `src/saga_gateway.ts`
- `src/scheduler.ts`

## Gateway：19 个路径

- `src/cloudflare/config_preflight_api.ts`
- `src/cloudflare/gateway_cell.ts`
- `src/cloudflare/tenant_directory_cell.ts`
- `src/cluster/capacity_inventory.ts`
- `src/config_release/config_preflight.ts`
- `src/config_release/config_versions.ts`
- `src/control/admin_capacity_api.ts`
- `src/control/admin_directory_api.ts`
- `src/control/admin_finops_api.ts`
- `src/control/tenant_directory.ts`
- `src/dashboard/console_rbac_guard.ts`
- `src/finops/authority_archive.ts`
- `src/finops/finops_page.ts`
- `src/finops/period_settlement.ts`
- `src/finops/rate_cards.ts`
- `src/finops/tenant_rate_store.ts`
- `src/finops/usage_cost_ledger.ts`
- `src/finops/usage_inbox.ts`
- `src/models.ts`

## 开发试点发布范围

- 基线 ff9c23e；工作树 src 下有 74 个新增或修改路径。部署脚本复制当前 src，并构建相关控制台与执行器。此候选涉及 M3、FinOps、配置发布等改动。
- 只对专用验收租户开启 enforce，使用专用容量池和预先估算值 5。排空前生成 Worker 配置并预检持久 Scheduler 清单；正式部署重新读取清单。保留原卷和原任务，用重启前快照核对任务及执行规格后恢复原 Drain 状态。
- 双 Gateway 各提交一笔，核对单槽只接收一笔；通过专用租户和隔离故障夹具确认回放及断联边界。退出试点切回 observe，核对已形成的预留、配额占用与费用记录。
- 试点不能消除 QF-05 的历史覆盖缺口；开发环境仍须通过权威结算门禁，不能以合成历史关账。

### 专用池前置条件

双 Gateway 只读目录均确认：当前只有 dev-local，一个健康的物理宿主机下有两个 Daemon，分配为 NOT_CONNECTED。专用池需要独立且可签名上报的宿主机；两个 Daemon 不能作为两个宿主机重复登记。若改用 dev-local，则需明确变更试点范围，先核对保障份额、旧占用及可用容量。本次未改动目录。

代码复核：宿主机只能绑定一个 poolId，绑定变更会返回 HOST_BINDING_IMMUTABLE。现有池支持按租户配置 guaranteedShares，但它保护池内份额，不会创建独立物理池；启用分配也是按整个池生效。若使用 dev-local，发布窗口需先确认同池其他申请和既有占用，并保留原策略及分配开关用于恢复。专用池路径仍以新增独立宿主机为准。
