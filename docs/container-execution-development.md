# 容器执行开发部署

2026-11-14：开发集群已启用并通过真实任务验证。仅用于受信任开发环境；Daemon 获得 Docker Engine 宿主权限，任务容器不获得 Engine socket。不要将该部署描述为敌对多租户安全环境。

## 启用与更新

先准备现有开发凭据，并确保本机已缓存 override 中指定摘要的 Node 镜像。

```bash
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml -f deploy/dev-cluster/docker-compose.sandbox.yml up -d --build --force-recreate celld-deploy body-celld body-daemon-01 body-daemon-02 ansts-gateway-01 ansts-gateway-02
python3 scripts/verify_container_deployment.py
```

维护本部署时必须保留 sandbox override。仅使用基础 Compose 或旧的集群重启验证脚本会移除容器能力配置；Gateway 将拒绝新的容器任务。更换镜像时同时更新两个 Daemon 和两个 Gateway 的允许摘要。

## 任务与结果

容器任务在原有授权 `/v1/tasks` 接口提交，新增 `execution` 字段：`kind: container`、`image`（允许的摘要）、`argv`、`timeoutMs`、`cpu`、`memoryMb`。必须同时声明覆盖执行需求的任务 `resource_profile` 与 `timeout_seconds`。示例及完整验证见 `scripts/verify_container_deployment.py`。

在原任务查询 URL 上添加 `includeResult=1`，返回 Body 执行阶段和结果；同样要求用户身份及租户授权。结果包含退出码、最多 8KiB 日志、字节截断标记和日志尾部限制标记。目前不是实时日志流。执行尝试完成不等于命令成功，应检查 `exitCode`。

## 重启回收

容器带有 `ansts.managed=sandbox` 与 `ansts.owner=<nodeId>` 标签。Daemon 在监听和上报容量前，强制移除本机所有权下的残留沙箱；Engine 故障或回收失败会阻止启动。只有一个 Daemon 可以使用同一 nodeId；不要并行启动同一身份的两个实例。

该回收只确认停止，不推断此前执行结果。现有执行 journal 和 Body UNCERTAIN 保护仍保留，不自动重跑未知执行。测试 fixture 的创建与回收：

```bash
docker run -d --name ansts-recovery-smoke --label ansts.managed=sandbox --label ansts.owner=recovery-smoke node:22-alpine sleep 600
# 在具有 Node 22 和 Engine socket 的环境中运行：
node scripts/verify_sandbox_recovery.mjs
```

尚未完成：工作区产物导出、实时日志、具名取消与 drain、Engine create 响应丢失等更完整故障矩阵。

容器启动前 fsync 写入 cleanup-ledger 记录。无法确认清理时保留记录与槽位，Daemon readiness 为 503，健康响应包含 recoveryRequired；代理以 unhealthy 心跳停止领取新工作。只有 Engine 启动回收成功后才删除阻塞记录。此机制覆盖容器执行，尚不替代完整的 CPU/内存资源账本。
