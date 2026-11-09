# ANSTS · 平台管理控制台（Management Plane）功能与架构设计文档

## 1. 设计背景与定位

在 ANSTS v2.5（基于 [denoland/celld](https://github.com/denoland/celld/blob/main/docs/README.md) 与 [LayerFS (`layerfs.ai`)](https://layerfs.ai/) 构建的分布式控制面与执行面架构）中，原有的 TanStack 运行面控制台侧重于研发与单集群实时遥测，而**平台管理控制台（Management Console）**面向多区域 SRE 管理员（`SRE_ADMIN`）、区域运维工程师（`OPERATOR`）与合规审计员（`VIEWER`），提供严格的权限边界、零伪造状态呈现、并发版本锁保护及双人复核（4-Eyes Principle）能力。

## 2. 核心设计准则：零伪造状态（Zero-Fabrication Discipline）

1. **数据过期（`stale`）**：当遥测流滞后超过阈值时，显式展示快照采集时间戳（如 `09:12:00 UTC`）并冻结自动缩容，防止基于陈旧指标误杀活跃节点。
2. **接口不可用（`offline`）**：当控制面 BFF 返回 `503 Service Unavailable` 时，绝不展示虚假的绿色健康状态，自动切换为最后一次已验证缓存的只读视图并锁定全部写操作。
3. **费用数据未知（`budget`）**：当 CloudEvents 计费流水延迟时，显式标记为 `UNKNOWN` 而非伪造 `$0.00`，并自动触发 FinOps 保护性软降级（路由至 `Qwen-3.8` 经济模型）。
4. **任务停止未确认（`cancel-unknown`）**：当向私有网络 Body 宿主下发任务停止指令但未收到反向隧道 ACK 时，状态严格保持为 `CANCEL_UNCONFIRMED（停止未确认）`，并提供推进 **Epoch Fencing Token** 强制吊销旧租约写入权限的恢复手段。

## 3. 11 大核心管理场景闭环

| 场景标识 (`#scenario`) | 场景名称 | 触发表现与恢复闭环 |
| :--- | :--- | :--- |
| `normal` | 正常管理流程 | 多区域（`us-west-2` / `eu-central-1` / `ap-northeast-1`）SLO 达标，配额、账单与节点编排正常运作 |
| `stale` | 数据过期 | 顶部告警横幅提示遥测滞后 18 分钟，自动扩缩容挂起，支持一键强制同步最新 BFF 快照 |
| `offline` | 接口不可用 | 控制面 BFF 离线（503），全站写按钮锁定，仅保留本地快照只读查看与重连探测 |
| `capacity` | 平台容量不足 | `us-west-2` 计算水位达 `96.4%`、P0 排队 P99 升至 `340ms`，支持一键开启 P3/P2 优先级削峰与热池扩容 |
| `quota` | 租户额度耗尽 | `Northstar` 并发槽达 `120 / 120 (100%)`，支持在线提额并记录审计日志 |
| `budget` | 费用数据未知 | 日账单显示 `UNKNOWN (计费流延迟)`，支持配置未知账单保护策略 |
| `partial` | 批量执行部分失败 | 批量节点排水 `2 成功 / 1 失败`（`body-host-02` 返回 `409 ERR_ACTIVE_LEASE_PINNED`），支持迁移 P0 租约后单点重试 |
| `cancel-unknown` | 任务停止未确认 | `task-ns-8841` 停止未收到反向隧道 ACK，支持推进 Epoch Fencing Token 强制隔离旧容器 |
| `conflict` | 配置版本并发冲突 | 编辑 `Northstar` 配额时检测到 `周 (SRE_ADMIN)` 已提交 `v15`，弹窗内拦截 `HTTP 409 CONFLICT` 并支持加载 `v15` 合并提交 |
| `history-missing` | 历史回合不可用 | `task-ns-8841` 的 `Turn #4–#5` LTX 段缺失，阻止损坏段检出并引导从 `Turn #3 (Commit C2)` 安全零拷贝分叉 |
| `verification` | 恢复指标再次超标 | 初次扩容后热点分片 `celld-brain-02` 锁争用导致 P99 复测升至 `410ms`，支持二级恢复：256 哈希槽重平衡与二次验证 |

## 4. 四级演示身份与 RBAC 矩阵 (`#identity`)

* **`chen`（陈 · `SRE_ADMIN` · 全平台）**：具备全区域、全租户配额修改、节点排水与高危审批权限（不能自审自己发起的申请单）。
* **`zhou`（周 · `SRE_ADMIN` · 全平台）**：第二平台管理员，用于验证双人异体复核（审批陈或林发起的高危单）及模拟并发配置冲突（`v14 -> v15`）。
* **`lin`（林 · `OPERATOR` · `US West / Northstar`）**：区域运维角色，仅允许操作 `us-west-2` 及 `Northstar` 作用域内的节点与任务，无权审批高危单或跨区域修改。
* **`xu`（许 · `VIEWER` · `US West / Northstar`）**：只读观察员，所有变更操作均按 RBAC 策略置灰禁用。
