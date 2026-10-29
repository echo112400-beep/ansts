# ANSTS v2.0 — Agent-Native Sandbox Task Scheduler (TypeScript Microservices Edition)

[![ANSTS v2.0 TypeScript CI](https://github.com/org/ansts/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)

**ANSTS v2.0** 是面向大规模自主智能体（Autonomous AI Agents）沙箱工作负载构建的多租户、双相脉冲与认知强度感知的分布式调度系统（全量 **TypeScript** 实现，覆盖 M1 ~ M6 全部 32 项工程任务）。

## 核心架构特性

1. **5D Phase & Effort-Aware W-DRF 公平调度 (`src/scheduler.ts`)**：统一调度稳态期望算力（$\widetilde{\text{CPU}}$）、突发算力余量（$\text{CPU}_{\text{burst}}$）、稳态内存（$\widetilde{\text{Mem}}$）、并发子会话槽位（$\text{Sess}$）与推理强度折算有效吞吐（$\widetilde{\text{TPS}}$），配合对数老化（Aging）与最早截止时间优先（EDF）彻底消除饥饿。
2. **单轮双相脉冲弹性沙箱 (`src/runtime/phase_controller.ts`)**：认知思考期收缩至 `0.1 vCPU / 256MB`，工具执行期 `5ms` 内解锁 `2.0~8.0 vCPU`，结合节点 `Burst Semaphore` 与 Linux PSI 内存压力主动休眠防雪崩。
3. **Turn 级动态认知路由 (`src/cognitive_router.ts`)**：复杂规划路由至旗舰池 `DeepSeek`，只读查看/解析轮次自动分流至轻量池 `GLM-5.3`（节省 75% 成本），主池 429 熔断时毫秒级无感切换至热备池 `Qwen-3.8`。
4. **无死锁多智能体 DAG 协同 (`src/dag/escrow.ts`)**：父任务阻塞时转入 `HIBERNATED_YIELD` 并签发 HMAC-SHA256 加密配额支票让子任务免排队直通，支持慢分支动态加速（Straggler Boost）与 Race 级联剪枝。
5. **进展率（Progress Velocity $\mathcal{V}_k$）树回滚、四级工具 Saga 网关 (`src/saga_gateway.ts`) 与双元组防脑裂栅栏 `Fence(epoch, turnSeq)` (`src/lease_coordinator.ts`)**。

## 目录结构

```text
.
├── .github/workflows/ci.yml           # GitHub Actions Node.js/TypeScript 自动化 CI 流水线
├── docs/                              # Google Engineering Design Doc 全量规范与微服务架构文档
├── proto/ansts_rpc.proto              # 核心微服务 gRPC / Protobuf 接口契约
├── specs/taskspec_v1beta1_example.yaml# TaskSpec v1beta1 声明式任务配置示例
├── src/                               # M1 ~ M6 全量 TypeScript 核心微服务源码 (24 个 .ts 模块)
│   ├── gateway/                       # API 网关、7 块 Schema 校验器、L3 429 背压、SSE 信用窗口
│   ├── dag/                           # 多智能体 DAG 状态机与 HMAC 配额支票托管
│   ├── router/                        # DeepSeek / GLM-5.3 / Qwen-3.8 流式客户端与三态熔断限流器
│   ├── saga/                          # 四级事务 LIFO 逆向补偿引擎与 STAGED_COMMIT 人工审批门禁
│   ├── runtime/                       # 预热沙箱池、Cgroups v2 双相控制器、Linux PSI 守卫、VSOCK 网桥
│   ├── harness/                       # 沙箱内 Harness 循环、80% 滑动上下文压缩、AST 死循环熔断器
│   ├── checkpoint/                    # TurnWAL 条件写引擎、L0->L1 合并与 Paged Restore、零 Token 时光机重放
│   ├── telemetry/                     # CloudEvents 1.0 总线与 Transactional Outbox 签名 Webhook
│   ├── cluster/                       # 四级双向闭环背压状态机
│   ├── cli/                           # anstsctl 集群运维 CLI 与 AnstsClusterRuntime 门面
│   └── dashboard/                     # 实时可视化看板 HTTP/SSE 桥接服务
├── tests/                             # TypeScript 端到端集成测试与五大混沌基准测试套件
├── scripts/ci_check.mjs               # TypeScript AST 检查、编译构建与测试运行器
├── package.json
└── tsconfig.json
```

## 快速运行本地 CI（TypeScript 语法检查 + 编译构建 + 集成测试）

```bash
npm run ci
# 或使用 Makefile：
make ci
```
