# M3 容量分配验收记录

## 已验证

- **MK-01 物理容量目录。** 隔离 celld 验证了双 Gateway 并发重放与版本冲突、同一宿主机上的多个签名 Daemon 去重、目录重启恢复、旧报告重放及失联保留占用。运行入口为 `python3 scripts/verify_capacity_inventory_native.py`。
- **MK-02 申请与队列。** 同一隔离脚本验证两个 Gateway 对同一申请幂等收敛、重启后仍可查询、租户列表隔离、取消重放、真实租户配额和 celld 定时签发。SQLite 定向测试覆盖带权排序、资源错配与防饥饿。
- **MK-03 授权与准入。** `python3 scripts/verify_body_evidence_native.py` 验证同一借用任务经过容量签发、租户预留、Scheduler 放置和生产 BodyAgent 后，在真实 Docker Engine 上执行。定向测试覆盖重复请求、旧 fencing 和旧 Daemon epoch。隔离演练的模型适配器采用固定响应。
- **MK-04 回收。** 两个隔离脚本分别验证未启动取消后的定时释放和真实 Body 签名退出后的释放。SQLite 定向测试覆盖到期、UNCERTAIN 保留、先取消后绑定和释放重放。
- **MK-05 控制台。** 容量页已接入池目录、申请列表、取消和暂停开关。签名 Gateway 会话的接口测试验证跨租户提交被拒绝，控制台类型检查通过。
- **MK-06 故障演练。** 已在隔离环境验证多租户额度筛选、同宿主机 Daemon 去重、失联占用和真实借用任务计费。结算重放在 Fleet 重启后仍返回原凭据。

## 补充验收证据

- **固定负载。** `tests/control/capacity_queue.test.ts` 在相同 CPU 与内存混合负载下，对照了两个权重版本的连续签发轨迹，覆盖跨宿主机错配和防饥饿。
- **保障份额。** `tests/control/capacity_inventory.test.ts` 验证其他租户的闲置保障份额会从借用方余量扣除，且目录和签发使用同一计算。
- **暂停与恢复。** `scripts/verify_body_evidence_native.py` 在同一隔离环境验证暂停期间新申请留在队列、已授权任务继续执行和结算、恢复后新申请获批、原授权保持 RELEASED。
- **浏览器。** `ANSTS_CAPACITY_BROWSER_SESSION=ansts-m3 python3 scripts/verify_capacity_inventory_native.py` 使用 Chromium 和签名隔离 Gateway 会话，验证 VIEWER 只读、OPERATOR 提交与取消、SRE_ADMIN 暂停与恢复。三种角色在 390 px 无横向溢出。浏览器代理转发真实的认证和容量接口，其他控制台接口使用夹具。

## 最终核对

- 版本化租户优先级已接入签发。原生脚本验证两个有配额的租户竞争时，高优先级租户先获批，零额度租户保持排队。
- 本轮后端及控制台的 TypeScript 检查均通过。
- 计划前置状态仍有差距。QF-01、QF-02 为进行中，QF-08 为进行中。隔离借用任务验证了 M3 使用的准入和结算路径，范围没有覆盖这些前置任务的全部验收条款。



- 真实 celld 一槽争用：同一租户并发提交两笔不同预留，恰好一笔成功，另一笔返回 `RESOURCE_QUOTA_EXCEEDED`。成功预留随后完成授权绑定，取消后由定时任务释放。此演练使用隔离租户准入入口。


## QF-08 提交门禁实现进度

- Scheduler 支持 `ANSTS_ADMISSION_MODE`（`observe`、`shadow`、`enforce`）和逗号分隔的 `ANSTS_ADMISSION_PILOT_TENANTS`。部署默认 `observe`。`shadow` 记录试点提交缺少预留的事件；它尚未执行完整的影子计量。`enforce` 拒绝试点租户缺少预留的新提交，Gateway 返回 `428 ADMISSION_RESERVATION_REQUIRED`。提供预留的 HTTP 提交仍走现有签名、预留与容量授权校验。Scheduler 的 RPC 提交同样不能跳过试点门禁。
- 本节记录的是代码实现。试点现有任务及占用仍需隔离或导入，普通 Gateway 提交还需接入自动预留。启用试点前须验证双 Gateway 争抢、控制面失联和退出试点后账目保留。上文的一槽结果不覆盖这些情形。

### 普通 Gateway 试点提交

- 在隔离 celld 中，两个 Gateway 通过正式 `POST /v1/tasks` 竞争一个槽：一个返回 202 并进入 Scheduler，另一个返回 `409 CAPACITY_PENDING`。重复提交获胜任务仍成功；租户侧预留为 `START_AUTHORIZED`，预先估算值为 5。
- 注入租户预留入口 503 后，普通提交得到 503，Scheduler 无任务；恢复入口后，同一提交继续。运行入口：`python3 scripts/verify_capacity_inventory_native.py`。另一次运行在旧的手动绑定用例中出现 `CAPACITY_BIND_REJECTED`，完整重跑通过。
- 部署配置仍默认 observe。试点自动预留还要求配置试点池与预先估算。Scheduler 会拒绝仍有本地旧任务占用的试点租户提交；全 Fleet 的旧占用隔离、退出试点账目留存和预留中断后的回收仍需验收。

### 中断后回收

预估准入与到期记录在同一租户事务提交。租户定时任务只取消尚未获准启动、尚未观察到执行的 PREPARING/RESERVED 预留，并将已有容量凭据或绑定意图交给持久化释放队列。已获准启动的预留仍需执行端证据。隔离 celld 演练验证了一笔被遗弃的 PREPARING 预留在真实 60 秒到期后变为 CANCELLED，且 generation 从 1 变为 2。跨对象绑定成功后租户未提交的崩溃点仍需单独注入验证。

已补充跨对象绑定故障注入：真实容量对象先成功持久化绑定，夹具再将首次成功响应替换为 503。此时 Scheduler 任务列表为空。通过普通 Gateway 入口重试后，同一授权恢复并进入 Scheduler，另一任务仍等候单槽；获胜预留仍为 5。完整原生演练通过。此场景验证响应丢失，未模拟 Fleet 进程崩溃。

### 退出试点

隔离 celld 从 enforce 切回 observe 并清空名单，重启同一容器存储后，逐字段比较试点预留及租户配额余量，与切换前一致。原等待任务随后经正式 Gateway 入口按 observe 提交，原试点占用仍保持。完整原生演练通过。该验证覆盖预留和配额占用，没有对比已结算费用账本。

### 旧分片占用隔离

试点部署须指定全量 Scheduler 分片名单 `ANSTS_ADMISSION_SCHEDULER_SHARDS`，含 `global-shard-0`。正式 Scheduler HTTP 提交由主分片接收，准入前会查询名单中的每个 Scheduler 对象：旧任务仍占用时拒绝请求，任一对象不可用时返回 503。配置名单须由部署清点，代码不能自动发现历史任意命名分片；启用前所有提交入口须完成同版本切换。TypeScript 和部署脚本语法已通过；还需用真实多分片复验。

真实多分片复验已通过：在 observe 下把旧任务写入第二个 Scheduler 对象，保留存储重启进入 enforce 后，通过普通 Gateway 提交得到 409。取消旧任务后，第二个对象注入库存读取 503，主分片仍未收任务。恢复库存后继续执行单槽竞争、失联、响应丢失、到期和退出试点。验证走正式 Worker Scheduler 路由；实际部署名单覆盖仍需独立确认。

开发环境预检：Compose 配置声明三个 Fleet 成员，当前仅 brain-02 健康，brain-01/03 均退出。成员数量不等于逻辑分片名单；不能据此证明历史分片覆盖。部署及重启预检现校验 enforce 所需的分片名单、租户、池和预估额度；`restart_dev_cluster.py --check-only` 已通过，未重启服务。

费用留存补证：`python3 scripts/verify_body_evidence_native.py` 用真实 Docker 执行生成正数费用和结算凭据；租户从 shadow 试点退出到 observe 后重启同一隔离存储，费用汇总、结算凭据和已结算预留逐字段一致。该用例与 enforce 退出的预留及配额留存是两项独立证据；没有在同一用例验证 enforce 退出及已结算费用。

强制试点费用留存复验通过：隔离 celld 先进入 enforce，确认无预留提交被拒；真实 Agent 经绑定授权执行并结算为正数费用。退出到 observe 并使用相同存储重启后，费用汇总、结算凭据和已结算预留逐字段相等。完整 `verify_body_evidence_native.py` 通过。此结果补齐上述同场景证据。

影子模式的已认证 Gateway 提交现在调用租户侧只读配额预演。预演与正式预留共用资源和预算判定，并使用相同的服务端预估值；不创建预留，不改变提交结果。隔离 celld 已通过一槽已占用时返回资源额度不足、空租户重复预演均可用的断言。该预演不模拟物理池排队和旧执行占用。

## 闭环复核

- MK-01 已完成。物理目录身份、报告新鲜度、去重与失联占用有隔离原生证据。
- MK-02 的公平调度轨迹见 `tests/control/capacity_queue.test.ts`；双 Gateway 和租户隔离见原生容量脚本。规划前置 QF-01 尚未关闭。
- MK-03 已验证实际 Body 执行、单槽硬配额、绑定成功后的响应丢失重试及结算重放。还需关闭 QF-02/QF-08，并单独核对跨对象崩溃与旧 epoch 恢复边界。
- MK-04 有未授权到期、取消、真实签名退出及重放证据；已授权但未进 Scheduler 的故障阶段仍需核对退出证明路径。
- MK-05 已有真实接口及移动端浏览器角色验证。仍需确认池管理角色的完整覆盖，并等待 MK-04 前置关闭。
- MK-06 有真实执行和暂停恢复证据；多资源错配需要在同一原生负载演练复验，不能仅以队列单测代替。

上一轮包含影子资源预演的原生容量演练已正常退出。新增影子预算负例用预估 5 与预算 4 对照。

MK-05 池管理权限补证：真实隔离 Gateway 中，签名 VIEWER 和 OPERATOR 可读取所属租户的容量目录，但写池和暂停授权均返回 403；SRE_ADMIN 随后读取到授权仍启用。两个角色的越权断言已通过。此前浏览器证据覆盖 OPERATOR 申请/取消和 SRE 暂停/恢复。

MK-06 原生多资源错配断言已通过：两台签名宿主机分别为 800 mCPU/256 MiB 和 200 mCPU/768 MiB。500 mCPU/512 MiB 的申请留在队列并报告宿主机资源不足；另一租户的 400 mCPU/128 MiB 申请获得授权。权限补验所在的上一轮原生容量脚本已完整通过。

QF-01 快照复核：强化 SQLite 测试，先推进预留到 RUNNING，再把并发上限降为 0。运行任务仍是 RUNNING，保留策略 v1 和 1 槽、500 mCPU、256 MiB 占用；新申请按 v2 被拒。225 项控制面检查通过。费率实际固定在可信用量绑定入口，不能将预留记录的 rateSnapshot:null 当作未固定运行中任务费率的证据；已有原生演练证明固定费率、结算及离线归档恢复。尚缺同一真实执行跨策略和费率切换的联合断言。

在途联合切换原生复验通过：真实 Agent 已进入首次模型请求，夹具暂扣响应时把并发上限降为 0，并激活输入 token 单价翻倍的费率 v2。原预留仍为策略 v1，新预留被拒。恢复模型响应后，实际工具执行和结算完成，Agent 用量上下文仍为费率 v1，结算重放与归档恢复通过。演练使用真实 Docker 和本地确定性模型响应。

Scheduler 接收记录的负例已通过原生演练：等待授权到期后取消并清理获胜任务，重启同一存储，确认任务不在清单中，但过期状态查询仍返回 expiredBeforeAcceptance=false，防止任务清理被误当作从未接收。租户自动取消尚未接线；此前工具自动审查拒绝该写入命令。

启动授权中断补证：租户对象提交 START_AUTHORIZED 后，夹具将首次响应替换为 503，主 Scheduler 无任务。重启隔离 celld，重交同一任务，恢复已有授权并接受唯一获胜任务；签发占用和预估额度没有重复增加。该断言已通过，覆盖客户端重试恢复，尚不覆盖无人重试时的到期自动释放。

启动授权中断场景已加强为真实进程硬停止：收到注入的 503 且 Scheduler 清单仍为空后，SIGKILL 仅作用于本轮隔离 celld，再从同一存储启动。重提同一请求恢复唯一授权，预估仍为 5，竞争请求仍排队。硬停止断言通过；上一轮正常重启版完整回归通过。

开发 Fleet 只读清点：运行中的 brain-02 本地存储仅见一个 AnstsSchedulerCell 对象目录，名称使用对象 ID，无法据此确认逻辑分片名，也不能代表停机成员或共享持久存储。未将该观察当成历史分片覆盖。容量回归发现宿主时钟与 celld 时钟偏差使固定 sleep 提前结束；演练已改为在有界时间内轮询服务端 STALE 状态，再验证占用保留。

三个开发 Fleet 本地卷通过无网络、只读挂载完成清点：brain-01 与 brain-02 有同一 Scheduler 对象 ID；brain-03 没有 Scheduler 目录。在线成员 /readyz 返回健康，但没有给出对象名映射或共享存储清单。因此本地卷未发现第二个 ID，仍不能证明历史分片全覆盖。含服务端 STALE 轮询修正的原生回归全部通过。

共享 RustFS 的 `ansts-fleet/cells` 只读清点发现两个不同 Scheduler 对象：`a4222cfb…` 与本地卷一致，另有仅在共享存储发现的 `145c35ef…`（含 ltx 目录）。单一默认分片名单尚无覆盖证明；需要确认第二个对象所属的分片或历史命名空间、以及是否还有在途占用。未读取任务数据，也未更改 Fleet 或启用 enforce。

无人重试正例：`ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY=1 python3 scripts/verify_capacity_inventory_native.py` 已通过。授权响应丢失后硬停并重启 Scheduler；截至到期前不返回可回收证明，到期后对准确任务和摘要返回 true，错误摘要返回 false，Scheduler 无任务。本轮未接入租户自动释放。共享存储第二对象仅发现 11 月 12 日目录中的一段 LTX（事务号 1），在线对象的 LTX 持续到 11 月 18 日；这些元数据无法证明旧对象没有在途占用。

历史 Scheduler 复核限制：RustFS 对象目录实际仅含 xl.meta，不能将其直接当作 LTX 读取。当前环境也未找到 LTX/LiteFS 解码工具。后续须通过受支持的对象读取与离线恢复，对旧对象仅统计任务状态，再决定纳入名单或记录无在途占用。不要把对象年代或本地卷缺失当作退役证明。

权威共享清单复核：在现存 brain-02 上运行 `docker exec ansts-celld-brain-02 celld cell list AnstsSchedulerCell --all --json`，结果为 2 个对象、1 次列举请求，两者均为 reserved:false，和卷目录清点一致。该命令只枚举 ID，reserved:false 不是任务占用状态；当前不据此把历史对象排除在试点门禁外。镜像帮助未列出离线 LTX 还原子命令。

隔离 celld 已确认 Scheduler 命名空间支持按对象 ID 重建和读取。试点名单现支持 `id:<64 位十六进制对象 ID>`，因此无需猜测历史对象原名；解析或查询失败仍拒绝准入。枚举历史对象后可显式纳入名单。实际开发 Fleet 尚未应用。上一轮 Scheduler 任务/接收标记同事务完整原生回归通过。

部署前名单已改为自动纳入共享存储列出的 Scheduler 对象 ID：enforce 发布时执行完整清单读取，发现的所有对象合并入配置；列举错误、无效行或超过 32 个条目时中止发布。主分片仍显式保留。原生隔离演练已通过按 ID 拦截远端旧占用及远端失联的断言；开发 Fleet 尚未发布该版本。

无人重试回收端到端通过：启动授权响应丢失后硬停重启，隔离 celld 到期后确认从未接收，租户将预留取消并推进 generation，容量释放记录由定时任务交付。断言覆盖预留和容量请求均进入终态。Scheduler 探测失败时保留占用并延后重试，避免阻塞后续到期记录。该退避改动的 225 项控制面检查已通过。

实际开发 Fleet 发布预检通过：通过在线 brain-02 执行同一部署脚本的 `--check-admission-roster` 分支，实际共享清单一次返回两个对象，生成的名单同时包含默认主分片和两个持久对象 ID。该分支在构建及发布前退出；未切换 enforce。

开发 Fleet 发布前诊断：两 Fleet 成员及两个 Gateway 的日志均记录约同一时刻的 node_lease_watchdog_fence，表明节点因未在 TTL 内续租自行停止；根因尚未确认。在线 brain-02 的 `celld diagnose --read-only` 通过共享桶与签名直连，只确认该节点当前可用，其他五条租约已过期。当前不具备双 Gateway 验收条件；先恢复或明确维护安排，在双入口与 Fleet 就绪后再发布试点。未启动、重启或改写任何服务。

开发 Fleet 入口已恢复：直接启动原来因 watchdog 退出的四个容器，沿用原镜像与存储；先通过三个 Fleet 成员的签名探测，再启动两个 Gateway。18080–18084 五个 /readyz 均返回 200 healthy=true。本轮未发布新版本或启用 enforce，故不计为 M3 真实试点验收。

发布前候选检查：五个现有容器仍健康，Compose check-only 通过；正式 Scheduler 和 Gateway 入口均成功独立打包。部署脚本会复制完整 src，当前工作树含 74 个 src 路径改动及其他里程碑变更，本轮候选不是隔离的 M3 发布，也未发布到开发 Fleet。

新版容量页 Chromium 回归完成：真实隔离 Gateway 签名三角色，390 px 无横向溢出；保留占用、报告状态和授权状态均可见，非管理员无暂停操作，原角色工作流通过。整轮原生容量验收成功。Playwright 夹具附加显式三角色结果，供后续原生验收校验。

QF-05 前置补证：以 `ANSTS_EVIDENCE_PRICE_TERMINAL=true` 分别运行 Docker 失败和取消分支，两次均通过。终止用量保留、可信费用入账，并纳入非空月份关账；重启后关账重放同一不可变记录。未发布开发 Fleet。

回收退避故障演练完成：`ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY=1` 的强化版本确认租户定时任务实际收到至少一次 Scheduler 503，读回预留仍为 START_AUTHORIZED；恢复后自动取消、提升 generation 并释放容量。用例通过，未依赖客户端重新提交。

独立双入口补证：`ANSTS_VERIFY_SEPARATE_GATEWAYS=1 python3 scripts/verify_capacity_inventory_native.py` 用两个不同容器运行正式 Gateway Worker，分别使用私有本地存储，经 HTTP 共用一个 Scheduler。两请求争抢一个槽，恰好一笔进入 Scheduler；授权重放、旧占用拦截和分片不可用断言已通过。首次共用挂载的布置出现 DurabilityUnproven，改为独立存储后该故障未复现。本次复跑完整结束，退出码为 0；另通过预估预留定时回收、退出试点保留占用和 Scheduler 清理任务并重启后保留已接收证据。

开发 Fleet 再次退出：2026-11-19 15:18:08–15:18:09（北京时间），三台 celld 与两个 Gateway 在约 1.14 秒内全部退出。brain-02 和两个 Gateway 的日志确认租约 watchdog 自我隔离；两个 Gateway 最后一次记录的续租成功距退出约 10.1 秒，之后未记录续租尝试。五个容器的 Docker 重启策略均为 no。退出原因记录不等于续租停顿的根因；此前的就绪结果不能用于当前发布门禁。本次未改动开发 Fleet。

开发 Fleet 已恢复：2026-11-19 使用现有镜像与存储启动三台 Scheduler，再启动两个 Gateway。五个入口在 30 秒内每 5 秒一次的七轮就绪检查中均返回 healthy=true；brain-02 记录一次成功续租。恢复后重新执行共享清单发布预检，仍读取到两个持久 Scheduler 对象并纳入主分片名单。此证据确认短时间恢复和当前名单，不证明此前续租停顿的根因已排除。未发布新版本。

2026-11-19 复核 QF-05 前置：开发双 Gateway 的 2026-10 结算预览均返回 HTTP 200、complete=false、nonEmpty=false 和 HISTORICAL_COVERAGE_UNAVAILABLE。执行清单核验随后首先在 Body celld 代理 18085 超时；启动原 Body celld 容器后重跑，Scheduler 18082 也已不可连接。Docker 确认三 Scheduler 和两个 Gateway 再次集中退出，brain-01 日志在北京时间 18:05:43 记录租约 watchdog 自我隔离。这次退出发生在启动 Body 代理之前。此前 30 秒就绪采样不足以作为试点发布门禁；本轮没有部署新版本或执行关账。

续租时间线补证（2026-11-19，UTC）：brain-01 和 Gateway-01 最后一次成功续租分别为 10:05:33.594 和 10:05:33.518，耗时 13 ms 和 22 ms；随后约 10 秒内，两者均未记录下一次续租发起。Gateway-01 在 10:05:43.504 成功刷新共享 Fleet 清单（13 ms），brain-01 在隔离前约 0.62 秒仍完成了复制请求。未看到对象存储续租请求失败；日志把问题缩小到续租发起或其调度。WSL 日志在北京时间 18:05:54 记录时钟向后跳变，提示应同步采样墙钟和单调时钟进行下一轮复现，尚无证据证明该跳变导致隔离。当前仍保留租约保护，未更改 TTL。

同步时钟采样：恢复现有容器期间，WSL 墙钟相对单调时钟先跳变 -1.798 秒，下一采样又向前修正 5.126 秒；六个就绪端点跨过两次跳变均维持健康。说明这两次变化未触发故障，尚不能排除更大跳变或其他条件。恢复后实际执行清单 check-live 成功：三 Fleet 入口及 Body 代理读回完全一致，双 Gateway 维持历史覆盖不足的结算门禁。仍未发布 enforce。

退出试点费用补证：隔离原生 Body 演练已通过 enforce 切回 observe、重启同一存储、读取真实 Docker 执行费用的断言。费用汇总与切换前一致，结算重放返回同一凭据。此结果与容量演练的预留及占用保留断言互补；不代表共享开发试点已发布。


2026-11-19 共享开发 Fleet 发布：用户授权在 dev-local 使用试点租户 m3-pilot，每次预估 5 信用单位。维护脚本退出 0，保留 119 条原任务及执行规格并恢复原 Drain 状态。双 Gateway 就绪，读回 dev-local 均为 1 个可用槽、0 保留占用，分配开关为默认 enabled=true（v0），队列策略为 v0、无保底份额，试点租户队列为空。创建独立试点成本中心与租户，配额 v1（日额度 10、2 个并发尝试、1000 mCPU、512 MiB、2 个排队任务）。

真实双 Gateway 提交：首次使用未启用镜像，两笔均为 503 SANDBOX_IMAGE_NOT_ENABLED；改用开发 Fleet 已启用镜像，两个网关仍均为 503 ADMISSION_FLEET_INVENTORY_UNAVAILABLE。未观测到接收成功或单槽竞争，未执行断联注入。公网就绪与共享 Scheduler 名单枚举通过均不足以证明每个对象的内部占用读取成功。已启动 observe 恢复，待读回；不据此关闭 M3 或 QF-08。

回切 observe 已完成：维护日志确认 119 条原任务及执行规格保留，原始 Drain 状态已恢复；随后从宿主机直连双 Gateway 及三个 Scheduler 的 /readyz 均返回 200。新增占用探测服务端诊断，按名单位置记录引用类型和失败类别，不记录令牌、租户 ID 或对象 ID；任何探测失败仍拒绝准入。诊断尚未发布，原生回归已启动。

共享 Fleet observe 预检：发布完成并保留 119 条任务规格及原始 Drain 状态。三个 Scheduler 对未认证预检均返回 401；对认证预检均返回 503。三节点日志一致指向名单索引 1（145c35ef… 历史持久对象）抛出异常，其他名单项未记录失败。已把异常诊断细化为 ID 解析、stub 获取、占用调用和响应解码阶段。共享试点仍维持 observe；未排除历史对象。

分阶段预检发布后，三台 Scheduler 均对未认证请求返回 401，对认证请求返回 503，并一致报告历史对象名单索引 1 为 resolve-object-id-error。故障发生于当前命名空间解析该持久对象 ID，尚未取得 stub、读取占用或解码响应；延长占用 HTTP 超时不能解决该失败。当前主对象及其 ID 引用无失败日志。不能据此判定历史对象无占用或已退役。维护退出 0，保留 119 条原任务及其执行规格并恢复原 Drain；继续维持 observe。

历史对象离线占用核查：对共享 RustFS 执行签名 ListObjectsV2/GET，历史对象前缀完整列举仅含 own.json（epoch 1）和 epoch 1 的 txid 1–1 LTX。LTX 为 2723 字节，SHA-256 为 762331095104fc04572a87a4ba78fcacadc63710331d532ec877270fefde5244。使用 celld_ltx 的 decode_database_image 在私有临时目录还原 SQLite，只读 integrity_check=ok，ansts_tasks 为 0 行，未见 claims 或试点授权表。由此证明该快照没有任务占用；未改变共享存储或开关。后续若凭此排除历史对象，须在发布时钉住对象身份、LTX 链与摘要并拒绝变化；快照结果本身不证明对象已永久退役。

历史命名空间已确认：共享桶同时包含 ansts-celld-brain 与 ansts-v2-celld-scheduler 发布前缀。按本地 celld v0.6.3 的命名空间派生算法比对，145c35ef… 精确匹配旧应用 ansts-v2-celld-scheduler 的 global-shard-0，a4222cfb… 精确匹配当前应用 ansts-celld-brain 的 global-shard-0。新增 scripts/verify_m3_historical_scheduler.py，复验完整对象清单、LTX 摘要及复验期间清单变化，已对共享 RustFS 运行成功。脚本尚未接入发布流程；仍须确认旧命名空间不存在写入者，才能使用历史空快照作为排除依据。

共享开发 Fleet enforce 复验：历史快照复验门禁通过，发布保留 119 条原任务与规格并恢复原 Drain。三台 Scheduler 预检无认证均为 401，认证均为 200，checked=2。双 Gateway 发布后均确认单槽、零占用和试点队列为空。并发提交最初两笔均返回 CAPACITY_PENDING，定时调度后使用相同任务 ID 重放，18080 的任务获唯一 GRANTED（202），18081 的任务继续 QUEUED（409 CAPACITY_PENDING）。在 18081 重放获胜任务仍返回同一执行任务 ID 和 fencing token 1；两网关读回相同的单一 GRANTED。胜者实际执行 COMPLETED/SUCCEEDED，容量请求 RELEASED；仅取消试点败者，读回 CANCELLED。共享环境未注入断联；断联拒绝准入证据仍来自隔离原生演练。

共享试点回切 observe 已完成，120 条任务（含试点获胜任务）与执行规格保留，原始 Drain 恢复。双 Gateway 读回一致：获胜任务 COMPLETED/SUCCEEDED，获胜容量请求 RELEASED，败者 CANCELLED。租户权威直接读回获胜预留 HTTP 200、SETTLING、预估信用单位 5；双 Gateway 费用项目 0，汇总因 UNSETTLED_ADMISSION_RESERVATIONS 不完整。此项证明回切未清预留或请求记录，未证明共享试点成本已结算。按计划 MK-03 仍受 QF-08/QF-05 门禁约束，不能将隔离成本演练替代该缺项。

共享试点结算根因复核：租户权威读回 terminalEvidence 已记录、预留 SETTLING、实际信用单位为空、用量 events=[]、有效费率 current=null/effective=false；Body 用量计量按显式租户名单启动，本次 m3-pilot 不在名单。新增开发 enforce 发布门禁：排空前和实际发布前都核对两台 Body 的计量名单及试点租户有效费率。当前 check-only 正确拒绝于 Body 计量配置，未改动运行服务。不能从已退出的任务补造可信用量；其未结算预留仍保留。

开发试点待审批费率：开发目录无费率卡（items=[]、watermark=0）；m3-pilot 无激活费率。已将 m3-pilot 加入两台 Body 的下次启动计量名单，保留原租户，未重启。只读 enforce 预检现通过名单并在有效费率门禁拒绝。建议仅激活给 m3-pilot 的 dev 区域容器验证费率：每 10 CPU 核秒、每 10 GiB·秒内存、每 10 槽秒各 1 credit-subunit，CEIL，模型费率为空，poolId=null（当前计价绑定不提供池）。这是待确认的开发内部费率，不是供应商价格，也不沿用预估 5 作为实际费用。

QF-05 只读复核：开发 Fleet 三 Scheduler 与 Body 代理的留存清单一致；Scheduler 权威 coverageSince=2026-11-17T17:34:48.354Z。基于当前留存，最早可能全月覆盖为 2026-12，月末门禁最早 2027-01-01T00:00:00Z；这不是届时覆盖或结算必然完成的承诺。2026-10/11 如要关账，仍需更早的连续权威清单及原执行时价格。开发试点费率提案仍待确认。

共享开发 Fleet 计量试点复验（2026-11-19）：开发费率 v1 激活后，试点任务实际执行成功；预留由预估 5 信用单位结算为实际 3，重复结算仍返回 `SETTLED` 与 3。切回 `observe` 的维护在 Fleet 恢复后中断，使用已保存的维护快照续接成功：121 条原任务及执行规格保留，原始 Drain 状态恢复。回切前后两个 Gateway 的费用读回逐字一致：同一执行对应 `RESOURCE_OPEN`、`RESOURCE_SEGMENT`、`RESOURCE_CLOSE` 各一条，均绑定费率 v1，只有 `RESOURCE_SEGMENT` 有三条费用明细。早期无可信用量的执行仍保留 `SETTLING`，租户汇总仍标记 `UNSETTLED_ADMISSION_RESERVATIONS`。本轮补齐共享试点的实际计量、结算重放与回切保账证据；QF-05 的完整月份关账及其下游 QF-08、MK-03 保持待验收。

MK-03 epoch 重启补证：隔离 celld 持久化 Daemon epoch 2 后重启，重放 epoch 1、sequence 999 且占用为零的有效签名报告，返回 HTTP 409 STALE_CAPACITY_REPORT。两个 Gateway 均保留 epoch 2 和原有两槽占用。启用无人重试回收场景的整轮原生演练退出 0。Daemon epoch 校验覆盖容量报告；grant 绑定由 reservation/attempt、generation 和 fencing 校验，现有 SQLite 重开用例拒绝变更身份或旧 fencing 的重绑。QF-02 与 QF-08 正式验收状态仍需确认。

QF-08 与 QF-02 下一批联合回归：同时设置 ANSTS_VERIFY_SEPARATE_GATEWAYS=1 和 ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY=1，运行 scripts/verify_capacity_inventory_native.py，整轮退出 0。两个独立 Gateway 进程共用 Scheduler；远端旧占用、分片断联和租户控制面不可用时拒绝准入。授权响应丢失后硬停，重启后由 Scheduler 确认从未接收，租户在无人重试时自动取消并释放容量。本轮为隔离演练，未改动共享 Fleet；历史关账不计入验收结果。

QF-02 启动入口补证：隔离 celld 硬停重启后，以原任务、执行规格和预留 ID 向真实 Scheduler 提交两次。一次在授权到期但租户仍为 START_AUTHORIZED 时，另一次在自动撤销进入 CANCELLED 后；两次均返回 409 CAPACITY_RESERVATION_MISMATCH，Scheduler 清单保持为空。ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY=1 原生演练完整退出 0。证据覆盖启动路径在 Scheduler 入口被拦截；不覆盖已进入 Scheduler 后的 Body 派发竞态。

QF-02 已接收任务的启动门禁：原生复验发现 Scheduler 重启时读取容量绑定字段，但旧写路径没有持久化这些字段；重开后任务仍在清单，却进入普通租约路径。修复写路径后，隔离 celld 接收试点任务并回切 observe 重启；授权到期后，带正确绑定的 Body 启动检查返回 409 CAPACITY_START_UNAVAILABLE，租户占用保持不变。任务取消、清理和再次重启后，已接收证明仍保留。整轮原生演练退出 0；未部署共享 Fleet。旧版本已落盘的任务不会由此修复自动补齐绑定，部署前需核对是否仍有受影响的在途任务。

部署前停机卷核对：对三个 Scheduler 容器的数据卷使用无网络、只读挂载。将最新本地 epoch 的 SQLite 与 WAL 复制到私有临时目录核查。brain-01 e48、brain-02 e49 均通过 quick_check，各有 121 条任务，m3-pilot 两条均为 COMPLETED，均缺少旧写路径未落盘的容量预留绑定；brain-03 未发现本地 Scheduler SQLite。两份本地副本未见受影响的在途试点任务。此结果不替代恢复后在线权威清单及维护前快照；未启动服务或部署。

QF-02 开发 Fleet 发布：恢复原三 Scheduler 后，认证清单在 18082–18084 一致返回 121 条任务，m3-pilot 两条均 COMPLETED。保存任务与执行规格摘要，Compose observe 预检通过；排空两台 Body 后使用现有镜像发布修复后的 Worker，保留卷及命名空间，恢复原 Drain。发布后独立比对 121 条任务与执行规格摘要一致，双 Gateway 和三 Scheduler 的 /readyz 均返回 200 healthy=true。试点结算及双 Gateway 费用明细读回与上一轮 observe 完全一致（预估 5、实际 3）。过期启动拒绝证据来自隔离演练；共享 Fleet 保持 observe。

QF-02 原生联跑：ANSTS_EVIDENCE_EXPIRE_BEFORE_START=true 下，真实 celld Scheduler 已分配试点任务，Body 权威签发执行 token，实际 BodyDaemon HTTP 请求在预留到期后发送。Body 返回 409 STALE_EXECUTION_LEASE，模型调用为零，执行证据查询返回 404。该模式整轮退出 0，未改动共享集群。

QF-02 试点队列额度：enforce 下由租户权威提供 queuedTasks，上限检查与创建在容量权威同一 SQLite 事务中完成，按租户跨池统计尚未获授权的排队请求。SQLite 重开用例覆盖跨池、重放、取消及无额度权威时拒绝新请求。启用 ANSTS_VERIFY_SEPARATE_GATEWAYS=1 和 ANSTS_VERIFY_PILOT_QUEUE_RACE=1 的隔离原生演练退出 0：暂停分配后，两个独立 Gateway 争抢唯一队列名额，一笔排队、另一笔返回 409 QUEUE_QUOTA_EXCEEDED；双入口读回同一笔，原键重放不重复计数，取消后另一请求可入队。未部署该改动到共享 Fleet。

QF-02 队列门禁 Worker 已发布至共享开发 Fleet，保持 observe。维护退出 0，保留 121 条任务及执行规格，恢复原 Drain；双 Gateway 的试点结算读回与此前 observe 基线完全一致。队列限额的 enforce 行为由独立双 Gateway 原生演练验收，本次未在共享集群开启 enforce。

### M3 验收结论

MK-01 至 MK-06 已按任务条款验收。MK-03：独立双 Gateway 单槽、硬停及丢应答重放、过期/撤销拒绝、旧 fencing 和旧报告 epoch、实际费用仅结算一次。MK-04：无人重试释放、已接收证明跨清理及重启保留、断联占用保留、签名退出与重放。MK-05：三角色浏览器及 API 校验、跨租户限制、真实池容量、借用与保障、排队原因、授权截止和隔离展示。MK-06：多租户错配、同机 Daemon 去重、断联保留占用、暂停期间在途任务完成和结算、恢复不重发已消费授权。

按用户决定，QF-05 历史关账缺口不作为此轮 M3 关闭条件。历史账务仍待核算，QF-05 保持进行中。共享开发集群保持 observe；断联和硬停负例在隔离环境演练。
