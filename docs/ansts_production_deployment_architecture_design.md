# ANSTS v2.2 生产环境整体部署架构设计规范 (Production Overall Deployment Architecture Design)

| 文档属性 | 规范详情 |
| :--- | :--- |
| **系统名称** | **Agent-Native Sandbox Task Scheduler (ANSTS v2.2 — Cloud-Native Production Edition)** |
| **核心技术基座** | TypeScript 全栈运行时 + [`denoland/celld`](https://github.com/denoland/celld/blob/main/docs/README.md) 分布式状态机集群 + `RustFS` 16 盘纠删码 (`EC:4`) S3 存储 + Linux Cgroups v2 / Firecracker / Companion Containers |
| **模型路由矩阵** | `DeepSeek`（旗舰深度推理池）、`GLM-5.3`（多步工具编排与只读高速分流池）、`Qwen-3.8`（高频语法修复与 429 容灾热备池） |
| **生产级 SLA 指标** | 已确认事务持久化 **`RPO = 0`**；单节点原地热升级 **`RTO < 1.5s`**；跨节点故障接管 **`RTO < 10s`**；跨区域联邦容灾切换 **`< 1s`**；脑体复用比 **`~30 : 1`** |
| **配套部署工程文件** | `wrangler.json`、`deploy/k8s-operator/celld-fleet-operator.yaml`、`deploy/rustfs-celld/docker-compose.rustfs-ha.yml`、`deploy/rustfs-celld/start-rustfs-celld.sh`、`deploy/observability/grafana-dashboard.json` |

---

## 1. 生产整体部署架构全景图（Multi-Region & Multi-AZ Production Topology）

生产环境采用 **“全球边缘多活联邦（L0/L1）+ `celld` 持久化状态机脑集群（L2）+ 认知路由与脑体分离沙箱躯体集群（L3）+ `RustFS` 16 盘纠删码存储基座（L4）”** 的四层五平面解耦架构：

```text
=============================================================================================================================
                     ANSTS v2.2 生产环境跨区域双活与多可用区整体部署架构图 (Production Master Topology)
=============================================================================================================================

                               [ 企业研发终端 / CI 流水线 / anstsctl CLI / Web 实时管控台 ]
                                                            │
                                                            ▼ (Anycast HTTPS / WSS / CloudEvents SSE)
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L0 & L1: 全球边缘接入与多活联邦路由平面 (Global Edge Ingress & Active-Active Federation Plane)                            │
│  ┌────────────────────────────┐  ┌─────────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────┐  │
│  │ MultiRegionFederationRouter│  │ FinOpsBudgetController      │  │ ANSTS_KV 分布式幂等去重  │  │ PartitionedScheduler │  │
│  │ • GDPR -> eu-central 强锁定│  │ • 80% 软告警 / 100% HTTP 402│  │ • SHA256(tenant+idemKey) │  │ • FNV-1a 租户亲和    │  │
│  │ • 区域级故障 <1s 自动切流  │  │ • 实时 Token + CPU 成本归因 │  │ • L3 429 排队背压响应头  │  │ • 热点租户分裂 + P2C │  │
│  └─────────────┬──────────────┘  └──────────────┬──────────────┘  └─────────────┬────────────┘  └──────────┬───────────┘  │
└────────────────┼────────────────────────────────┼───────────────────────────────┼──────────────────────────┼──────────────┘
                 │ (us-west / eu-central / ap-east)                                                          │
                 ▼                                                                                           ▼
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L2: 多可用区高可用 celld 状态机“大脑”平面 (K8s StatefulSet: ansts-celld-brain, 3~32 Replicas across AZ-1 / AZ-2 / AZ-3)   │
│     启动参数: celld --bucket s3://ansts-fleet --endpoint http://rustfs-ha:9000 --listen :8080 --internal-listen :9001     │
│     环境变量: CELLD_DURABILITY=fleet | CELLD_TTL_MS=10000 | CELLD_REBALANCE_INTERVAL_MS=5000 | CELLD_LTX_RETENTION_SECS   │
│                                                                                                                           │
│  ┌─────────────────────────────────────────────────────────┐  ┌────────────────────────────────────────────────────────┐  │
│  │ 2A. 分片调度控制集群 (AnstsSchedulerCell Shards 00..K)  │  │ 2B. 动态分片异步队列集群 (DynamicQueueAutoSharder)     │  │
│  │  • 5D Phase & Effort W-DRF 主导份额公平调度             │  │  • ansts-events-q-shard-00 .. 63 (突破单队列 256 上限) │  │
│  │  • 对数老化 (Aging) + EDF 截止时间零饥饿最小堆          │  │  • 聚合并发 >75% 自动在线翻倍扩容队列分片              │  │
│  │  • 多智能体 DAG 支票托管 (Escrow) + 慢分支提速          │  │  • 基于 message.attempts 的显式指数退避 + 持久化 DLQ   │  │
│  │  • 跨分片无锁配额 Gossip 同步 (syncTenantQuotaGossip)   │  │  • 多组无状态消费者 Worker 并行拉取写 D1 审计表        │  │
│  └────────────────────────────┬────────────────────────────┘  └───────────────────────────┬────────────────────────────┘  │
│                               └─────────────────────────────┬─────────────────────────────┘                               │
│                                                             ▼ (idFromName(task_id) — 一任务一专属 Cell 状态机)            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 2C. 任务级细粒度大脑池 (AgentTaskHarnessCell + DurableWorkflowContext — 单节点驻留 10,000+ 轻量级 V8 Isolate)       │  │
│  │  • 私有嵌入式 SQLite + TurnWAL：冷启动/休眠唤醒自动执行 rehydrateFromSql() 恢复 Fence、WAL 与 Saga 账本             │  │
│  │  • RpoZeroDurabilityEngine：Fleet 模式 1~2 Peer 节点 fsync 复制 + 接管会话封存 (SessionSealed)；Bucket 二次复核     │  │
│  │  • DurableWorkflowContext：step.do 幂等缓存、step.waitForEvent 提前事件缓冲、>1h 休眠自动卸载 V8 Isolate            │  │
│  │  • FleetHaAndAutoscalerController：每 5s 平滑迁移 <=32 休眠 Cell；PreStop 调用 POST /shutdown?handoff=preserve      │  │
│  └────────────────────────────┬───────────────────────────────────────────────────────────┬────────────────────────────┘  │
└───────────────────────────────┼───────────────────────────────────────────────────────────┼───────────────────────────────┘
                                │                                                           │
        (PHASE_LLM_WAIT: 0.1C + WebSocket Hibernation)              (PHASE_TOOL_BURST: 0.1C -> 8.0C, ~30:1 脑体复用比)
                                ▼                                                           ▼
┌────────────────────────────────────────────────────────────┐ ┌────────────────────────────────────────────────────────────┐
│ L3-A: 认知模型路由平面 (CognitiveRouter & Live SSE Pool)   │ │ L3-B: 脑体分离重型沙箱“躯体”平面 (Bare-Metal / KVM Hosts) │
│  • LiveOpenAiCompatibleSseAdapter (HTTP/2 流式连接池)      │ │  • LinuxCgroupV2AndPsiDriver (/sys/fs/cgroup/ansts/<id>)   │
│  • 实时拆分 reasoning_content (CoT) 与 content Token       │ │    cpu.max (10000 100000 <-> 800000 100000) & cgroup.freeze│
│  • 滑动窗口令牌桶 Pacing + 三态熔断器 (30s 窗口 >25% 429)：│ │  • CompanionContainerAndOverlayDriver (OverlayFS upperdir  │
│    ├─ DeepSeek Pool (复杂架构规划 / 深度代码推理 / 回溯)   │ │    CoW 差异快照 + SHA256 Merkle Root 毫秒级文件树回滚)     │
│    ├─ GLM-5.3 Pool  (纯只读文件检索 / 多步轻量工具编排)    │ │  • ZeroTrustVsockHostDaemon (AF_VSOCK Length+CRC32 二进制帧│
│    └─ Qwen-3.8 Pool (高频语法修复 / DeepSeek 429 秒级接管) │ │    沙箱内零 API Key、宿主机注入凭证、域名白名单、Fence 校验)│
│  • McpDynamicToolRegistry：未声明逆向补偿自动降级 STAGED   │ │  • EphemeralHeavyExecutorMigrator (<50ms 跨机实时热迁移)   │
└────────────────────────────────────────────────────────────┘ └────────────────────────────┬───────────────────────────────┘
                                                                                            │
                                                                                            ▼ (25GbE Storage VLAN / S3 SigV4)
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ L4: 分布式持久化存储、数据库归档与全链路可观测平面 (Storage, Audit DB & Observability Plane)                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  ┌──────────────────────────────────────────┐  │
│  │ RustFS 4 节点 x 4 NVMe 盘 (16-Drive EC:4 纠删码高可用 S3 存储集群)    │  │ 可观测性与审计归档中心 (Observability)   │  │
│  │  • 容忍任意 2 台存储服务器 (8 块 NVMe 盘) 同时故障零数据丢失、零中断  │  │  • OtelAndPrometheusExporter (/metrics)  │  │
│  │  • cells/<ns>/<id>/ownership.json : S3 If-Match / If-None-Match CAS   │  │  • Grafana 生产大盘 + Alertmanager 告警  │  │
│  │  • cells/<ns>/<id>/ep_XXXX/*.ltx  : HTTP 206 Range 热页优先分页恢复   │  │  • D1 / SQLite 审计表 (0001_init_ansts)  │  │
│  │  • cells/<ns>/<id>/retired.json   : SafeEpochGarbageCollector 回收基座│  │  • CloudEvents 1.0 + HMAC 发件箱 Webhook │  │
│  └───────────────────────────────────────────────────────────────────────┘  └──────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 生产集群分层部署规划与硬件配置清单（Sizing & BOM）

由于系统实现了**脑体分离（Brain-Body Disaggregation，约 `30 : 1` 复用比）**与**单轮双相脉冲 Cgroups（思考期 `0.1C / 256MB` $\leftrightarrow$ 执行脉冲期 `8.0C / 4GB`）**，生产集群的物理硬件成本相比传统“一任务一独占容器”架构降低 **85%~90%**。

### 2.1 三种生产规模的服务器资源配置表

| 集群规模 | 支撑并发活跃任务数 | L1 边缘接入网关 | L2 `celld` 状态机“大脑”节点 | L3-B 沙箱容器“躯体”宿主机 | L4 `RustFS` S3 分布式存储节点 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **小型生产集群 (Starter Prod)** | **1,000 ~ 3,000 并发任务**<br>（约 100 并发工具脉冲） | Cloudflare Workers 或 2 $\times$ `4C 8GB` Envoy/Worker 节点 | **3 节点**（跨 3 AZ）<br>每节点：`8 vCPU / 16GB RAM / 200GB NVMe` | **6 节点**（每机上限 20 脉冲）<br>每节点：`32 vCPU / 64GB RAM / 1TB NVMe` | **4 节点 $\times$ 4 盘 (`EC:4`)**<br>每节点：`16 vCPU / 32GB RAM / 4 x 2TB NVMe` |
| **中型生产集群 (Standard Prod)** | **10,000 ~ 25,000 并发任务**<br>（约 600 并发工具脉冲） | Cloudflare Workers + 4 $\times$ `8C 16GB` 区域接入网关 | **8 节点**（16 调度分片 + 32 队列分片）<br>每节点：`16 vCPU / 32GB RAM / 500GB NVMe` | **36 节点**（含 25% Warm Pool 缓冲）<br>每节点：`64 vCPU / 128GB RAM / 2TB NVMe` | **4 节点 $\times$ 4 盘 (`EC:4`)**<br>每节点：`32 vCPU / 64GB RAM / 4 x 4TB NVMe / 25GbE` |
| **大型联邦集群 (Enterprise Global)** | **100,000+ 并发任务**<br>（跨 `us-west` / `eu-central` / `ap-east` 3 Region） | 全球 Anycast Workers + 每区 8 台边缘路由节点 | **每区 12~16 节点**（64 调度分片 + 64 队列分片）<br>每节点：`32 vCPU / 64GB RAM / 1TB NVMe` | **每区 80~120 节点**（专用 CPU 池 + GPU 重型执行池）<br>每节点：`96 vCPU / 256GB RAM / 4TB NVMe` | **每区 8 节点 $\times$ 4 盘 (32 盘 `EC:4`)**<br>每节点：`64 vCPU / 128GB RAM / 4 x 8TB NVMe / 2x25GbE` |

---

## 3. 网络分区隔离、VPC 子网与端口通信矩阵（Network & Security Zoning）

生产环境强制划分 **4 个安全隔离网段（VLAN / VPC Subnets）**，确保沙箱内部代码与外网及存储网物理隔离：

| 安全区域 (Zone) | 网段规划示例 | 部署组件 | 开放端口与协议 | 零信任安全策略 |
| :--- | :--- | :--- | :--- | :--- |
| **Zone 1：公网/边缘接入区 (Ingress DMZ)** | `10.240.0.0/20` | Edge Worker / Envoy API Gateway | `:443` (HTTPS / WSS / SSE) | 强制 TLS 1.3、KV 幂等校验、FinOps 预算硬顶检查（超支直接返回 `402`）、L3 `429` 令牌桶限流 |
| **Zone 2：`celld` 控制与状态机平面 (Brain Mesh Subnet)** | `10.240.16.0/20` | `ansts-celld-brain` StatefulSet、`CelldAdminClient` | `:8080` (Worker HTTP/RPC)<br>`:9001` (`celld` 内部状态 `/state`、`/shutdown` 与 Peer 复制) | `:9001` 仅对集群内部 Peer 节点与 K8s Operator / Prometheus 开放，严禁公网路由可达 |
| **Zone 3：沙箱宿主机与零信任网桥区 (Sandbox Body Subnet)** | `10.240.32.0/19` | `ansts-node-daemon`、Firecracker / Companion Containers | 宿主机 `:9102` (热迁移与遥测)<br>沙箱内：**仅开放 `AF_VSOCK` / Unix Socket** | **沙箱容器内部禁用默认网卡外网路由 (`0.0.0.0/0` DROP)**、零 API Key 存储；一切 LLM 与 MCP 调用必须经 `ZeroTrustVsockHostDaemon` 校验 `X-Agent-Fence` 与域名白名单后由宿主机代发 |
| **Zone 4：高速持久化存储网 (Storage NVMe Subnet)** | `10.240.64.0/20` | `RustFS` 16 盘 `EC:4` 集群、D1 / SQLite 归档库 | `:9000` (S3 API, AWS SigV4)<br>`:9001` (RustFS Console) | 独立 25GbE 存储内网，仅允许 `celld` 节点与宿主机 Snapshot 组件通过 AWS Signature V4 签名访问 |

---

## 4. 核心生产组件的部署配置与启动规范

### 4.1 存储基座层：`RustFS` 4 节点 $\times$ 4 盘（16 驱动器 `EC:4`）高可用集群
生产环境使用仓库内的 `deploy/rustfs-celld/docker-compose.rustfs-ha.yml` 与 `rustfs-ec-setup.sh` 部署：
- **纠删码容灾（Erasure Coding `EC:4`）**：16 块 NVMe 驱动器组成统一纠删集（12 数据块 + 4 奇偶校验块），可承受**任意 2 台物理存储服务器（8 块 NVMe 盘）同时断电**，而 [`docs/guarantees.md`](https://github.com/denoland/celld/blob/main/docs/guarantees.md) 所需的 `ownership.json` 条件写（`If-Match` / `If-None-Match`）与 `.ltx` 增量事务流读写 **100% 零中断**。
- **权限与目录初始化**：容器以非特权 `10001:10001` 运行，启动前通过 InitContainer 或 `rustfs-ec-setup.sh` 自动挂载 `/data/nvme{1...4}` 并初始化 `s3://ansts-fleet` 存储桶。

### 4.2 状态机控制层：Kubernetes `ansts-celld-brain` StatefulSet
生产环境使用仓库内的 `deploy/k8s-operator/celld-fleet-operator.yaml` 部署：
- **持久化模式（`CELLD_DURABILITY=fleet`）**：主节点每次提交 SQLite 事务时，实时同步给 1~2 个 Peer 节点完成 `fsync` 落盘证明，随后异步刷入 `RustFS` S3 存储桶；若新节点抢占接管（`epoch + 1`），立即封存（`SessionSealed`）旧节点的复制日志会话，从根源阻断脑裂脏写。
- **单写者租约与历史纪元 GC**：配置 `CELLD_TTL_MS=10000`（每 `3.33s` 自动续期）与 `CELLD_LTX_RETENTION_SECS=86400`（自动通过 `SafeEpochGarbageCollector` 写入 `retired.json` 并安全回收 24 小时前的远古 LTX 纪元，同时严格保留 `currentEpoch` 与 `currentEpoch - 1`）。
- **优雅原地升级钩子（`PreStop` Hook）**：
  ```yaml
  lifecycle:
    preStop:
      exec:
        command: ["/bin/sh", "-c", "curl -fsS -X POST http://127.0.0.1:9001/shutdown?handoff=preserve"]
  ```
  当 Pod 重启或原地更新配置时，`handoff=preserve` 立即取消排队中的冷启动请求、优先把已停止的 SQLite 数据库上传至 `RustFS`，并**保留 Bucket 中的 `ownership.json` 租约不释放**，新容器拉起后 `< 1.5s` 即可原地接续服务。

### 4.3 沙箱宿主机层：`ansts-node-daemon` 与脑体分离执行池
每台裸金属或 KVM 宿主机部署 `ansts-node-daemon`（集成 `LinuxCgroupV2AndPsiDriver`、`CompanionContainerAndOverlayDriver`、`ZeroTrustVsockHostDaemon` 与 `EphemeralHeavyExecutorMigrator`）：
1. **Cgroups v2 双相脉冲配额**：挂载 `/sys/fs/cgroup/ansts/<sandbox_id>/`，在等待 `DeepSeek` / `GLM-5.3` / `Qwen-3.8` 响应时将 `cpu.max` 设为 `10000 100000`（`0.1C`）、`memory.high` 设为 `256MB`；进入工具执行脉冲时在 `< 5ms` 内切至 `800000 100000`（`8.0C`）、`4096MB`。
2. **单机 20 并发脉冲信号量与 `<50ms` 跨机实时卸载**：每台宿主机通过 `node_burst_semaphore = 20` 严格限制同时处于 8.0C 脉冲态的沙箱数；当本机信号量满载或 `/proc/pressure/memory` 的 `some avg10 >= 40%` 时，`EphemeralHeavyExecutorMigrator` 自动将当前轮次的 OverlayFS `upperdir` CoW 差异包流式卸载至空闲 `Heavy Executor` 节点执行。

---

## 5. 生产运维标准作业程序（Production Day-2 Operations SOP）

### 5.1 零停机滚动升级流程（Zero-Downtime Rolling Release）
使用内置 CLI `anstsctl fleet rolling-restart` 或 `CelldAdminClient.executeRollingRestart()` 执行生产发布：
1. **前置健康门禁检查**：调用全集群节点的 `:9001/state`，确认所有节点 `restoring == 0`（无正在从 `RustFS` 分页拉取恢复中的 Cell）且 `healthy == true`。
2. **单节点平滑排空/保留重启**：对 `celld-node-i` 触发 `POST /shutdown?handoff=preserve`（原地升级）或 `SIGTERM`（跨节点迁移升级）。
3. **等待恢复收敛门禁（`waitUntilClusterRestoringZero`）**：持续轮询全集群 `GET /state`，严格等待 `totalRestoring === 0` 后，才允许推进重启 `celld-node-(i+1)`，配合 Kubernetes `PodDisruptionBudget (maxUnavailable: 1)` 确保全程零请求掉落。

### 5.2 在线水平扩容流程（Horizontal Scale-Out）
- **调度分片与队列分片在线扩容**：当监控大盘 `ansts_scheduler_shard_inflight_requests` 水位上升时，调用 `POST /v1beta1/scheduler/shards`（如 `{"add_shards": 8}`）在线追加 `AnstsSchedulerCell` 分片；`DynamicQueueAutoSharder` 在队列并发生产者达 `75%`（$N \times 256 \times 0.75$）时自动倍增活跃队列分片。
- **脑节点与沙箱宿主机弹性伸缩**：`KubernetesCelldHpaAdapter` 每 15 秒读取 `fleet/capacity-v1.json` 与宿主机 PSI 指标：
  - 若 `restoring > 0`，自动锁定副本数变更（`BLOCKED_BY_RESTORING`）防止抖动；
  - 若任一脑节点 `pressured == true` 或平均持有 Cell 数超过 `2,500`，自动触发 `ansts-celld-brain` StatefulSet 扩容，并由 `celld` 内置 Rebalancer 每 `5s` 平滑迁移最多 `32` 个休眠 Cell 至新节点；
  - 沙箱宿主机池始终维持 `25%` 的预热空闲微容器（Warm Pool Buffer），确保工具脉冲挂载延迟 `< 10ms`。

### 5.3 多级故障自愈与容灾切换矩阵（Failure Recovery SLA Matrix）

| 故障等级与场景 | 自动检测机制 | 自愈与容灾动作 | 恢复时间目标 (RTO) | 数据恢复点目标 (RPO) |
| :--- | :--- | :--- | :--- | :--- |
| **L1：单模型池限流或超时 (`DeepSeek` HTTP 429 / TTFT > 15s)** | `ThreeStateCircuitBreaker` (30s 窗口错误率 `>25%`) | 熔断器切 `OPEN`，`CognitiveRouter` 与 `LiveOpenAiCompatibleSseAdapter` 当轮毫秒级无感切换至 `Qwen-3.8` / `GLM-5.3` | **`< 50ms`** | **`RPO = 0`** |
| **L2：Agent 陷入语法死循环或原地踏步 ($\mathcal{V}_k < 0.10$)** | `SyntacticAndVelocityBreaker` (连续 3 次同 AST 或 4 轮进展率低) | `SagaCompensatorEngine` 按 LIFO 逆序回滚外部副作用，`CompanionContainerAndOverlayDriver` 恢复 `upperdir` 至历史最优 Turn 快照并提权至 `DeepSeek (max)` | **`< 100ms`** | **`RPO = 0`（零脏写）** |
| **L3：单台 `celld` 脑节点原地重启 / 容器热更新** | `POST /shutdown?handoff=preserve` | 取消排队冷启动、刷盘已停止 SQLite DB、保留 `RustFS` 中的 `ownership.json` 租约，新进程原地接管 | **`< 1.5s`** | **`RPO = 0`** |
| **L4：单台 `celld` 脑节点或沙箱宿主机硬宕机 (`SIGKILL` / 断电)** | `CELLD_TTL_MS=10000` 租约过期 / 12s 心跳超时 | 存活节点通过 S3 `If-Match` CAS 抢占所有权、递增 `epoch + 1`、封存旧节点日志会话（`SessionSealed`），通过 HTTP `206 Range` 热页优先恢复 SQLite 与 `TurnWAL` 并从精确 Step/Turn 续跑 | **`< 10s`** | **`RPO = 0`** |
| **L5：单台或两台 `RustFS` 存储节点硬宕机（最多坏 8 块 NVMe 盘）** | `RustFS` 16 盘 `EC:4` 纠删码健康探针 | 纠删码引擎自动利用剩余 $\ge 8~12$ 块健康盘在线重构数据块，上层 `celld` 无感知继续读写 | **`0s`（无中断）** | **`RPO = 0`** |
| **L6：单一地理区域整体中断（如 `us-west` 全区光纤/电力故障）** | `MultiRegionFederationRouter` 区域健康探针 | 全球边缘层自动将新任务与可迁移任务切流至 `ap-east` 或 `eu-central`（严格遵守 `GDPR` 等合规约束） | **`< 1s`** | **`RPO = 0`** |
