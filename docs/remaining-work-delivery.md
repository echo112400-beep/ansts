# 剩余任务交付记录

日期：2026-11-14。范围依据用户要求、next-milestone.md 与 next-phase-roadmap.md。本轮实现已部署到开发集群；GitHub 应用回调登记和用户授权由用户完成。

## 交付与验收

| 工作 | 已交付行为 | 验证 |
| --- | --- | --- |
| drain | 持久停止新准入、在途任务完成、重启保持、显式恢复 | 单元、真实 Daemon 重启和鉴权检查 |
| 用户取消 | 租户与角色授权；从服务端身份记录 actor；已租赁取消保留资源直到退出确认 | 正常、在途取消、越权拒绝、跨重建与重复请求 |
| 结果状态 | SUCCEEDED / FAILED / TIMED_OUT / CANCELLED；journal/outbox 不可变结果和持久重试 | 非零退出、超时、重复完成、结果冲突 |
| 资源预留 | 持久 CPU/内存账本、预先同步保留槽位、清理失败保持阻塞、启动回收后释放 | 分数 CPU、内存、跨重建、真实崩溃与无孤儿容器 |
| 产物与运行日志 | 授权结果查询、约三秒日志快照、沙箱内有界产物采集、不导出链接 | 二进制产物、越权拒绝、日志及截断标记 |
| 长任务 | 原生 HTTP 本机执行传输；租约有效期内容忍短暂网络错误 | 十分钟任务经历四秒断网后成功完成 |
| 故障矩阵 | 可重复命令和证据文件 | 三轮连续通过，涵盖响应丢失、outbox、取消、SIGKILL、回收、旧 fence 和 HA |
| 共享控制状态 | Gateway 与 Body 状态在同一个 celld fleet 权威；Body 使用全新命名空间 | 50 个 Cell 迁移后逐项比对；Gateway 重启、首入口故障及实际任务完成 |
| 快照恢复 | 私有快照、冲突目标不覆盖、隔离恢复工具 | 65 个 Cell 还原到隔离 celld 并比对；线上权威未切换 |
| GitHub / 身份审计 | state + PKCE、数字 ID 权限映射、HttpOnly 会话、Origin 防护、具名核对；另有可选 RS256/JWKS API 校验 | 自动测试；部署入口跳转/回调/PKCE 验证。用户补充 OAuth 回调并完成首次授权 |
| 服务密钥轮换 | 15 分钟双密钥窗口、新签名发行与旧执行凭证验证、私有文件导入工具 | 宽限、过期拒绝及鉴权回归；未在本轮轮换正在使用的根密钥 |
| DeepSeek Agent | 主路径支持 read/write/test/finish、沙箱测试、预算、Turn 持久化与原宿主恢复约束 | 真实 DeepSeek 5 Turn 及最终复验 4 Turn 修改 sum.js、真实容器测试通过、原 test.js 未被修改、产物可查 |
| 容量测量 | 固定双租户工作负载、吞吐和 P95、逐租户时延 | 8 个任务的实测 JSON；不把小样本结果作为生产 SLO |

## 验收命令

```bash
npm run build
python3 scripts/verify_fault_matrix.py --rounds 3
python3 scripts/verify_long_execution.py
python3 scripts/verify_deepseek_agent.py
python3 scripts/verify_body_restore.py
python3 scripts/measure_execution_capacity.py --tasks 8
```

工具使用默认开发端口，包含明确的开发容器停止/重启故障注入，finally 恢复服务。不要在生产环境直接运行。DeepSeek 验收会调用真实付费模型。

Node 22 相关回归：46 项中 45 项通过，1 项因缺少独立 celld 测试二进制跳过。真实 Engine 测试已开启。另有针对恢复亲和性、缺失检查点与回环 HTTP 登录的契约测试。

仓库全量测试：314 项，264 通过、46 失败、4 跳过。干净基线 4ac7022：297 项，249 通过、46 失败、2 跳过。逐项比较没有新增失败。历史失败包含旧接口/导出契约和既有审计。lint 仍被历史跨文件导入/导出不一致阻塞；本轮没有将它记为通过。build 是仓库原有转译检查，不等同于严格类型检查。

## 重要行为与边界

- Docker archive 无法取得 tmpfs 内容；实现改为独立 exec 及固定沙箱内采集器。命令退出后冻结残余任务进程再读取工作区，随后确认 Engine 清理。
- 初次十分钟验收发现 fetch 在五分钟等待响应头时断开，任务按规则进入 UNCERTAIN。修正本机 HTTP 客户端后，同类十分钟任务通过。验收失败及 SIGKILL 的 fixture 保留为有明确原因的 SUSPENDED_RECONCILIATION，不自动重跑；无遗留物理预留或容器。
- TaskState.CANCELLED 的历史枚举值仍为 DEAD_LETTER。公共结果使用 execution_outcome / executionOutcome 区分，不以旧 COMPLETED 状态宣称业务成功。
- Agent 初始和最终工作区受大小限制，仅支持文本 read/write。恢复仅使用原宿主的已提交检查点；缺失或 pendingTest 检查点拒绝自动恢复。尚未开放跨宿主 Agent 检查点迁移。
- GitHub 已配置 http://127.0.0.1:8088，完整回调是 http://127.0.0.1:8088/auth/callback/github。只有显式 development 回环地址允许非 Secure 的开发 Cookie；生产仍需 HTTPS。
- 生产 secret 平台分发、真实 IdP/OAuth 发布验收、实际 CPU/内存使用采样及生产 SLO 未由开发验证替代。不宣称敌对多租户安全或无条件 exactly-once。

补充：末轮发现模型可能在 JSON 模式下仍返回无效 JSON。现已给出明确的格式错误、追加单对象纠正提示，并在持久调用/token 预算内重试；不重放工具。最终真实服务复验以四个工具 Turn 完成。

## 部署与恢复

使用 [当前架构](current-architecture.md) 中的四个 Compose override。已迁移集群不能只用旧基础 Compose 更新。dev-cluster-up.sh 检查迁移验证标记，拒绝在不完整迁移时静默选择旧权威。

Body 原控制卷与私有迁移快照均保留。恢复演练没有回退线上流量；若需要回退，先 drain，再复制至全新目标并核对后切换，不能直接启用已经落后的旧卷。

Provider 只复制用户指定文件中的必要字段，未改动原项目 env。凭据位于 .dev-secrets，权限 0600，并同时被 git 和 Docker build context 忽略。配置及 GitHub 登录说明见 [provider-configuration.md](provider-configuration.md)。

原 root 所有的 dist 已保留到 /tmp/ansts-original-build-20261114，工作区重新构建。宿主 Node 24 超出项目声明范围，因此增加了固定 Node 22 镜像的相关回归。
