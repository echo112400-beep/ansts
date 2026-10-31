# ANSTS v2.2 生产环境整体部署架构设计规范（无 K8s 裸金属 / Systemd / Compose 手动扩容版）

| 文档属性 | 规范详情 |
| :--- | :--- |
| **系统名称** | **Agent-Native Sandbox Task Scheduler (ANSTS v2.2 — Non-K8s Bare-Metal & VM Manual Scale-Out Edition)** |
| **核心技术基座** | TypeScript 全栈运行时 + [`denoland/celld`](https://github.com/denoland/celld/blob/main/docs/README.md) 分布式状态机集群（基于共享 Bucket 自发现，**零 Kubernetes 依赖**） + `RustFS` 16 盘纠删码 (`EC:4`) S3 存储 + Linux Systemd / Docker Compose + Cgroups v2 / Firecracker |
| **模型路由矩阵** | `DeepSeek`（旗舰深度推理池）、`GLM-5.3`（多步工具编排与只读高速分流池）、`Qwen-3.8`（高频语法修复与 429 容灾热备池） |
| **生产级 SLA 指标** | 已确认事务持久化 **`RPO = 0`**；单节点原地热升级 **`RTO < 1.5s`**；跨节点故障接管 **`RTO < 10s`**；跨区域联邦容灾切换 **`< 1s`**；脑体复用比 **`~30 : 1`** |
| **配套部署工程文件** | `deploy/manual-fleet/fleet-topology.yaml`、`deploy/manual-fleet/scale-fleet.sh`、`deploy/manual-fleet/systemd/ansts-node-daemon.service`、`deploy/rustfs-celld/systemd/celld.service`、`deploy/rustfs-celld/docker-compose.rustfs-ha.yml` |

---

## 1. 为什么不采用 Kubernetes？（Why Non-K8s for `celld` + MicroVM Sandboxes）

1. **`celld` 原生内建去中心化集群协同**：根据 [`denoland/celld` 官方架构 (`docs/README.md`)](https://github.com/denoland/celld/blob/main/docs/README.md)，`celld` 节点仅需挂载同一个 S3 存储桶（`--bucket s3://ansts-fleet`）并指定 `--internal-listen :9001` 与 `--advertise <ip:9001>`，即可通过存储桶内的 `fleet/capacity-v1.json` 与 `ownership.json` 自动完成节点发现、单写者租约抢占（`CAS`）与每 `5s` 最多 `32` 个休眠 Cell 的平滑重平衡，完全不需要 Kubernetes StatefulSet 或 Etcd。
2. **避免双层虚拟化与 Cgroups 嵌套冲突**：沙箱宿主机直接操作裸金属或 KVM 宿主机的 `/sys/fs/cgroup/ansts/<id>`、`/proc/pressure/*`、OverlayFS 与 `AF_VSOCK`，采用 `systemd` 守护进程（`ansts-node-daemon.service`）直接管理比在 K8s Pod 内嵌套特权容器更轻量、无 Cgroup v2 委托冲突且冷启动更快。
3. **确定性可控的手动横向扩容（Operator-Driven Manual Scale-Out）**：运维人员可通过 `scale-fleet.sh` 脚本、`anstsctl fleet scale-out` CLI 或 `POST /v1beta1/fleet/scale` 接口，随时按需向集群追加 `celld` 大脑节点、沙箱躯体宿主机或调度/队列分片，系统自动执行 `restoring === 0` 安全门禁检查并平滑再均衡负载。

---

## 2. 无 K8s 生产整体部署架构全景图（Non-K8s Production Master Topology）

```text
=============================================================================================================================
              ANSTS v2.2 无 K8s (Systemd + Docker Compose) 跨区域与多可用区手动扩容整体部署架构图
=============================================================================================================================

                         [ 运维人员 (scale-fleet.sh / anstsctl fleet scale-out) / 研发终端 / Web 管控台 ]
                                                            │
                                                            ▼ (Anycast HTTPS / WSS / CloudEvents SSE)
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L0 & L1: 全球边缘接入与多活联邦路由平面 (Cloudflare Workers / Bare-Metal Envoy + Stateless Edge Worker Fleet)              │
│  ┌────────────────────────────┐  ┌─────────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────┐  │
│  │ MultiRegionFederationRouter│  │ FinOpsBudgetController      │  │ ANSTS_KV 分布式幂等去重  │  │ PartitionedScheduler │  │
│  │ • GDPR -> eu-central 强锁定│  │ • 80% 软告警 / 100% HTTP 402│  │ • SHA256(tenant+idemKey) │  │ • FNV-1a 租户亲和    │  │
│  │ • 区域级故障 <1s 自动切流  │  │ • 实时 Token + CPU 成本归因 │  │ • L3 429 排队背压响应头  │  │ • 热点租户分裂 + P2C │  │
│  └─────────────┬──────────────┘  └──────────────┬──────────────┘  └─────────────┬────────────┘  └──────────┬───────────┘  │
└────────────────┼────────────────────────────────┼───────────────────────────────┼──────────────────────────┼──────────────┘
                 │ (POST /v1beta1/fleet/scale 在线手动扩缩节点与分片)                                        │
                 ▼                                                                                           ▼
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L2: 高可用 celld 状态机“大脑”裸金属/VM 集群 (Systemd: celld.service | Multi-AZ Hosts 10.240.16.11..N)                      │
│     启动命令: celld --bucket s3://ansts-fleet --endpoint http://10.240.64.11:9000 --listen :8080 --internal-listen :9001  │
│     手动扩容: ./deploy/manual-fleet/scale-fleet.sh add-brain-node celld-node-04 10.240.16.14 100                          │
│                                                                                                                           │
│  ┌─────────────────────────────────────────────────────────┐  ┌────────────────────────────────────────────────────────┐  │
│  │ 2A. 分片调度控制平面 (AnstsSchedulerCell Shards 00..K)  │  │ 2B. 动态分片异步队列池 (ShardedQueueBrokerPool 00..63) │  │
│  │  • 支持手动/API 在线加分片 (POST /v1beta1/scheduler/..) │  │  • 突破单 Queue Cell 256 并发写上限                    │  │
│  │  • 5D Phase & Effort W-DRF + Aging/EDF 反饥饿最小堆     │  │  • 支持手动指定分片数或超 75% 水位翻倍扩分片           │  │
│  │  • 多智能体 DAG 支票托管 (Escrow) + 无锁配额 Gossip     │  │  • 基于 message.attempts 显式指数退避 + 持久化 DLQ     │  │
│  └────────────────────────────┬────────────────────────────┘  └───────────────────────────┬────────────────────────────┘  │
│                               └─────────────────────────────┬─────────────────────────────┘                               │
│                                                             ▼ (idFromName(task_id) — 一任务一专属 Cell 状态机)            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 2C. 任务级细粒度大脑池 (AgentTaskHarnessCell + DurableWorkflowContext — 单机驻留 10,000+ 轻量级 V8 Isolate)         │  │
│  │  • 私有嵌入式 SQLite + TurnWAL：冷启动/休眠唤醒自动执行 rehydrateFromSql() 恢复 Fence、WAL 与 Saga 账本             │  │
│  │  • RpoZeroDurabilityEngine：Fleet 模式 1~2 Peer 节点 fsync 复制 + 接管会话封存 (SessionSealed)；Bucket 二次复核     │  │
│  │  • DurableWorkflowContext：step.do 幂等缓存、step.waitForEvent 提前事件缓冲、>1h 休眠自动卸载 V8 Isolate            │  │
│  │  • ManualFleetScaleController：新节点加入后每 5s 自动迁入 <=32 休眠 Cell；下线前强制校验全集群 restoring == 0       │  │
│  └────────────────────────────┬───────────────────────────────────────────────────────────┬────────────────────────────┘  │
└───────────────────────────────┼───────────────────────────────────────────────────────────┼───────────────────────────────┘
                                │                                                           │
        (PHASE_LLM_WAIT: 0.1C + WebSocket Hibernation)              (PHASE_TOOL_BURST: 0.1C -> 8.0C, ~30:1 脑体复用比)
                                ▼                                                           ▼
┌────────────────────────────────────────────────────────────┐ ┌────────────────────────────────────────────────────────────┐
│ L3-A: 认知模型路由平面 (CognitiveRouter & Live SSE Pool)   │ │ L3-B: 脑体分离沙箱“躯体”裸金属宿主机池 (Systemd Daemon)    │
│  • LiveOpenAiCompatibleSseAdapter (HTTP/2 流式连接池)      │ │  • 服务单元: deploy/manual-fleet/systemd/ansts-node-daemon │
│  • 实时拆分 reasoning_content (CoT) 与 content Token       │ │  • 手动扩容: scale-fleet.sh add-body-host body-host-03 ... │
│  • 滑动窗口令牌桶 Pacing + 三态熔断器 (30s 窗口 >25% 429)：│ │  • LinuxCgroupV2AndPsiDriver (/sys/fs/cgroup/ansts/<id>)   │
│    ├─ DeepSeek Pool (复杂架构规划 / 深度代码推理 / 回溯)   │ │  • CompanionContainerAndOverlayDriver (OverlayFS upperdir  │
│    ├─ GLM-5.3 Pool  (纯只读文件检索 / 多步轻量工具编排)    │ │    CoW 快照 + SHA256 Merkle Root 毫秒级文件树回滚)         │
│    └─ Qwen-3.8 Pool (高频语法修复 / DeepSeek 429 秒级接管) │ │  • ZeroTrustVsockHostDaemon (AF_VSOCK 二进制帧 + 域名白名单│
│  • McpDynamicToolRegistry：未声明逆向补偿自动降级 STAGED   │ │  • EphemeralHeavyExecutorMigrator (<50ms 跨机实时热迁移)   │
└────────────────────────────────────────────────────────────┘ └────────────────────────────┬───────────────────────────────┘
                                                                                            │
                                                                                            ▼ (25GbE Storage VLAN / S3 SigV4)
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L4: 分布式持久化存储、数据库归档与全链路可观测平面 (Docker Compose / Systemd Bare-Metal Storage & Observability)          │
│  ┌───────────────────────────────────────────────────────────────────────┐  ┌──────────────────────────────────────────┐  │
│  │ RustFS 4 节点 x 4 NVMe 盘 (16-Drive EC:4 纠删码高可用 S3 存储集群)    │  │ 可观测性与审计归档中心 (Observability)   │  │
│  │  • 部署文件: deploy/rustfs-celld/docker-compose.rustfs-ha.yml         │  │  • OtelAndPrometheusExporter (/metrics)  │  │
│  │  • 容忍任意 2 台存储服务器 (8 块 NVMe 盘) 同时故障零数据丢失、零中断  │  │  • Grafana 生产大盘 + Alertmanager 告警  │  │
│  │  • cells/<ns>/<id>/ownership.json : S3 If-Match / If-None-Match CAS   │  │  • D1 / SQLite 审计表 (0001_init_ansts)  │  │
│  │  • cells/<ns>/<id>/ep_XXXX/*.ltx  : HTTP 206 Range 热页优先分页恢复   │  │  • CloudEvents 1.0 + HMAC 发件箱 Webhook │  │
│  └───────────────────────────────────────────────────────────────────────┘  └──────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 手动横向扩容与缩容操作手册（Manual Scale-Out & Scale-In SOP）

系统提供 **3 种等价的手动扩容入口**（Shell 运维脚本 `deploy/manual-fleet/scale-fleet.sh`、命令行工具 `anstsctl fleet`、以及控制面 HTTP API `POST /v1beta1/fleet/scale`），底层统一由 `ManualFleetScaleController`（`src/cluster/manual_scale_controller.ts`）协调并确保数据零丢失：

### 3.1 场景一：手动横向扩容 `celld` 大脑节点（Brain Node Scale-Out）
当监控面板显示单台 `celld` 节点的 `owned_cells > 2,500` 或 `memory_headroom` 低于 20% 时，在新物理机/VM（如 `10.240.16.14`）上启动 `celld.service` 并注册：

1. **在新主机启动 `celld` Systemd 服务**（指向共享 `RustFS` 存储桶 `s3://ansts-fleet`）：
   ```bash
   sudo systemctl start celld
   # 或使用脚本一键拉起并加入集群：
   ./deploy/manual-fleet/scale-fleet.sh add-brain-node celld-node-04 10.240.16.14 100
   ```
2. **通过 CLI 或 HTTP API 登记并触发即时重平衡**：
   ```bash
   # 方式 A：使用 anstsctl CLI
   node dist/src/cli/anstsctl.js fleet scale-out celld-node-04 body-host-03

   # 方式 B：调用控制面 HTTP API
   curl -X POST http://10.240.0.10:8080/v1beta1/fleet/scale \
     -H "content-type: application/json" \
     -d '{
       "add_brain_nodes": [{"nodeId": "celld-node-04", "advertiseAddress": "10.240.16.14:9001", "placementWeight": 100}]
     }'
   ```
3. **底层自动协同流程**：
   - `ManualFleetScaleController` 首先校验全集群 `totalRestoring === 0`（若正有节点在恢复数据，返回 `409 BLOCKED_BY_RESTORING` 防止并发抖动）；
   - 新节点写入 `fleet/capacity-v1.json` 后，`FleetHaAndAutoscalerController` 每 `5s` 自动从负载最高的旧节点平滑迁移最多 **`32` 个处于休眠状态（Hibernated）的 Cell** 到新节点，正在执行 I/O 的热 Cell 零中断。

### 3.2 场景二：手动横向扩容沙箱“躯体”宿主机（Sandbox Body Host Scale-Out）
当并发工具执行脉冲（`PHASE_TOOL_BURST`）增长、宿主机 `node_burst_semaphore (20)` 趋于饱和或 Linux PSI 告警时：

1. **在新裸金属/KVM 宿主机启动 `ansts-node-daemon.service`**：
   ```bash
   sudo cp deploy/manual-fleet/systemd/ansts-node-daemon.service /etc/systemd/system/
   sudo systemctl enable --now ansts-node-daemon.service
   ```
2. **向集群控制面注册新沙箱宿主机**：
   ```bash
   ./deploy/manual-fleet/scale-fleet.sh add-body-host body-host-03 10.240.32.13 20
   ```
   注册后集群突发并发容量立即增加 `+20` 个 `8.0C` 脉冲槽位，并自动按 `25%` 比例预热 Warm Pool 空闲容器，同时 `capacityAdvisory` 实时返回最新的脑体复用比（`brainToBodyRatio`）与推荐宿主机总数。

### 3.3 场景三：手动在线扩容调度分片与异步队列分片（Shard Scale-Out）
无需重启任何进程，即可在线成倍扩展 `AnstsSchedulerCell` 调度分片数与 `ShardedQueueBrokerPool` 队列分片数：
```bash
# 在线追加 8 个调度分片（如从 8 分片扩至 16 分片）：
./deploy/manual-fleet/scale-fleet.sh scale-shards 8
```
扩容后，`PartitionedSchedulerRouter` 自动将热点大租户（`HOT_TENANT_P2C`）与过载溢出流量（`OVERFLOW_P2C`）分流至新激活的 `scheduler-cell-shard-XX` 分片。

### 3.4 场景四：手动缩容下线或原地保留重启（Manual Scale-In & Preserve Handoff）
- **原地保留租约重启（单机升级内核或更新配置，`RTO < 1.5s`）**：
  ```bash
  ./deploy/manual-fleet/scale-fleet.sh remove-brain-node http://10.240.16.12:9001 preserve
  ```
  调用 `:9001/shutdown?handoff=preserve`，节点取消排队中的冷启动、把已停止的 SQLite 数据库刷入 `RustFS` 并**保留 `ownership.json` 租约不释放**，重启后原地秒级恢复。
- **永久缩容下线节点（跨机平滑迁移排空）**：
  ```bash
  ./deploy/manual-fleet/scale-fleet.sh remove-brain-node http://10.240.16.12:9001 drain
  ```
  `ManualFleetScaleController.executeManualNodeDrain()` 前置确认全集群 `restoring === 0`，随后将目标节点标记为 `draining = true`、刷盘全部活跃 SQLite 状态并释放 `ownership.json` 租约，由集群剩余健康节点通过 S3 CAS（`epoch + 1`）无损接管。

---

## 4. 三档无 K8s 生产集群硬件配置与部署清单（Bare-Metal / VM Sizing BOM）

| 生产档位 | 支撑并发活跃任务数 | L1 边缘网关 (`systemd` / Cloudflare) | L2 `celld` 大脑主机 (`celld.service`) | L3-B 沙箱躯体主机 (`ansts-node-daemon.service`) | L4 `RustFS` (`EC:4`) 存储主机 (`docker-compose.rustfs-ha.yml`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **小型生产集群 (Starter)** | **1,000 ~ 3,000 并发任务**<br>（约 100 并发工具脉冲） | 2 $\times$ `4C 8GB` Linux VM | **3 台物理机/VM**（跨 3 AZ）<br>`8 vCPU / 16GB RAM / 200GB NVMe` | **6 台裸金属/KVM 主机**（每机 20 脉冲）<br>`32 vCPU / 64GB RAM / 1TB NVMe` | **4 台存储主机 $\times$ 4 盘 (16 盘 `EC:4`)**<br>`16 vCPU / 32GB RAM / 4x2TB NVMe` |
| **中型生产集群 (Standard)** | **10,000 ~ 25,000 并发任务**<br>（约 600 并发工具脉冲） | 4 $\times$ `8C 16GB` Linux VM | **8 台物理机/VM**（16 调度分片 + 32 队列分片）<br>`16 vCPU / 32GB RAM / 500GB NVMe` | **36 台裸金属/KVM 主机**（含 25% Warm Pool）<br>`64 vCPU / 128GB RAM / 2TB NVMe` | **4 台存储主机 $\times$ 4 盘 (16 盘 `EC:4`)**<br>`32 vCPU / 64GB RAM / 4x4TB NVMe / 25GbE` |
| **大型跨区联邦集群 (Enterprise)** | **100,000+ 并发任务**<br>（跨 `us-west` / `eu-central` / `ap-east`） | 每区 8 台 `16C 32GB` 边缘路由主机 | **每区 12~16 台主机**（64 调度/队列分片）<br>`32 vCPU / 64GB RAM / 1TB NVMe` | **每区 80~120 台裸金属主机**（CPU + GPU 池）<br>`96 vCPU / 256GB RAM / 4TB NVMe` | **每区 8 台存储主机 $\times$ 4 盘 (32 盘 `EC:4`)**<br>`64 vCPU / 128GB RAM / 4x8TB NVMe / 2x25GbE` |
