/**
 * ANSTS · 平台管理控制台 (Management Plane Prototype & Live BFF Client)
 * Supports all 11 operational edge-case scenarios, 4 RBAC identities,
 * multi-region / multi-tenant / historical time-window filtering,
 * accessible <dialog> workflows, and live BFF synchronization.
 */
(function () {
  'use strict';

  const IDENTITIES = {
    chen: {
      id: 'chen',
      avatar: '陈',
      name: '陈 · 平台管理员',
      role: 'SRE_ADMIN',
      scopeRegion: 'all',
      scopeTenant: 'all',
      canWriteGlobal: true,
      canWriteScoped: true,
      canApprove: true
    },
    zhou: {
      id: 'zhou',
      avatar: '周',
      name: '周 · 平台管理员',
      role: 'SRE_ADMIN',
      scopeRegion: 'all',
      scopeTenant: 'all',
      canWriteGlobal: true,
      canWriteScoped: true,
      canApprove: true
    },
    lin: {
      id: 'lin',
      avatar: '林',
      name: '林 · 区域运维',
      role: 'OPERATOR',
      scopeRegion: 'us-west-2',
      scopeTenant: 'Northstar',
      canWriteGlobal: false,
      canWriteScoped: true,
      canApprove: false
    },
    xu: {
      id: 'xu',
      avatar: '许',
      name: '许 · 只读观察员',
      role: 'VIEWER',
      scopeRegion: 'us-west-2',
      scopeTenant: 'Northstar',
      canWriteGlobal: false,
      canWriteScoped: false,
      canApprove: false
    }
  };

  const SCENARIO_LABELS = {
    normal: '正常场景',
    stale: '数据过期',
    offline: '接口不可用',
    capacity: '平台容量不足',
    quota: '租户额度耗尽',
    budget: '费用数据未知',
    partial: '批量执行部分失败',
    'cancel-unknown': '任务停止未确认',
    conflict: '配置版本并发冲突',
    'history-missing': '历史回合不可用',
    verification: '恢复指标再次超标'
  };

  const NAV_ITEMS = [
    { id: 'overview', label: '平台态势与容量恢复', badgeKey: 'overview' },
    { id: 'nodes', label: '节点与分片单元编排', badgeKey: 'nodes' },
    { id: 'tenants', label: '租户配额与 FinOps 治理', badgeKey: 'tenants' },
    { id: 'tasks', label: '任务运行与回合诊断', badgeKey: 'tasks' },
    { id: 'approvals', label: '高危审批与变更审计', badgeKey: 'approvals' },
    { id: 'runtime', label: '集群运行面实时拓扑', badgeKey: 'runtime' }
  ];

  function createInitialState() {
    return {
      activeNav: 'overview',
      region: 'all',
      tenant: 'all',
      timeRange: 'current',
      scenario: 'normal',
      identity: 'chen',
      lastSyncUtc: '2026-11-10 09:30:00 UTC',
      loadSheddingActive: false,
      verificationRetried: false,
      conflictResolvedVersion: 14,
      nodes: [
        { id: 'celld-brain-01', role: 'Brain Shard #1', region: 'us-west-2', slots: 85, cpuPct: 62, psiPct: 4.2, status: 'READY', activeLeases: 18, tunnelGw: 'celld-gw-01' },
        { id: 'celld-brain-02', role: 'Brain Shard #2', region: 'us-west-2', slots: 86, cpuPct: 68, psiPct: 5.8, status: 'READY', activeLeases: 21, tunnelGw: 'celld-gw-01' },
        { id: 'celld-brain-03', role: 'Brain Shard #3', region: 'eu-central-1', slots: 85, cpuPct: 54, psiPct: 3.1, status: 'READY', activeLeases: 14, tunnelGw: 'celld-gw-02' },
        { id: 'body-host-01', role: 'Body LayerFS #1', region: 'us-west-2', slots: 0, cpuPct: 74, psiPct: 8.4, status: 'READY', activeLeases: 24, tunnelGw: 'celld-gw-01 (PRIMARY)' },
        { id: 'body-host-02', role: 'Body LayerFS #2', region: 'eu-central-1', slots: 0, cpuPct: 79, psiPct: 11.2, status: 'READY', activeLeases: 19, tunnelGw: 'celld-gw-02 (SECONDARY)' },
        { id: 'body-host-03', role: 'Body LayerFS #3', region: 'ap-northeast-1', slots: 0, cpuPct: 48, psiPct: 2.5, status: 'READY', activeLeases: 10, tunnelGw: 'celld-gw-01 (PRIMARY)' }
      ],
      batchLastResult: null,
      tenants: [
        { id: 'Northstar', region: 'us-west-2', tier: 'P0_STRATEGIC', usedSlots: 88, maxSlots: 120, escrowReserved: 16, dailyBudgetUsd: 1200, spentUsd: 840, modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 14 },
        { id: 'Helios', region: 'eu-central-1', tier: 'P1_ENTERPRISE', usedSlots: 46, maxSlots: 80, escrowReserved: 8, dailyBudgetUsd: 800, spentUsd: 490, modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 9 },
        { id: 'Vanguard', region: 'ap-northeast-1', tier: 'P1_ENTERPRISE', usedSlots: 34, maxSlots: 60, escrowReserved: 6, dailyBudgetUsd: 600, spentUsd: 310, modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 7 },
        { id: 'Aegis', region: 'us-west-2', tier: 'P2_STANDARD', usedSlots: 19, maxSlots: 40, escrowReserved: 4, dailyBudgetUsd: 300, spentUsd: 145, modelPolicy: 'Qwen-3.8 Economy', configVersion: 5 }
      ],
      tasks: [
        { id: 'task-ns-8841', tenant: 'Northstar', region: 'us-west-2', priority: 'P0', brain: 'celld-brain-01', body: 'body-host-02', fence: 12, turn: 5, branch: 'feat-ns-auth', state: 'RUNNING', historyWindow: 'current', turnsAvailable: [1, 2, 3, 4, 5] },
        { id: 'task-ns-8842', tenant: 'Northstar', region: 'us-west-2', priority: 'P1', brain: 'celld-brain-02', body: 'body-host-01', fence: 8, turn: 3, branch: 'fix-ns-ledger', state: 'LEASED', historyWindow: 'current', turnsAvailable: [1, 2, 3] },
        { id: 'task-hl-3019', tenant: 'Helios', region: 'eu-central-1', priority: 'P1', brain: 'celld-brain-03', body: 'body-host-02', fence: 6, turn: 4, branch: 'gdpr-audit-eu', state: 'RUNNING', historyWindow: 'current', turnsAvailable: [1, 2, 3, 4] },
        { id: 'task-vg-1044', tenant: 'Vanguard', region: 'ap-northeast-1', priority: 'P2', brain: 'celld-brain-01', body: 'body-host-03', fence: 4, turn: 2, branch: 'apac-settlement', state: 'COMPLETED', historyWindow: 'history', turnsAvailable: [1, 2] },
        { id: 'task-ns-8790', tenant: 'Northstar', region: 'us-west-2', priority: 'P0', brain: 'celld-brain-02', body: 'body-host-01', fence: 11, turn: 6, branch: 'hist-ns-0915', state: 'COMPLETED', historyWindow: 'history', turnsAvailable: [1, 2, 3, 6] }
      ],
      approvals: [
        { id: 'APR-2026-091', type: 'QUOTA_EMERGENCY_BOOST', target: 'Northstar (120 → 180 并发槽)', region: 'us-west-2', tenant: 'Northstar', initiator: 'chen', status: 'PENDING_SECOND_ADMIN', risk: 'HIGH', createdAt: '09:24 UTC' },
        { id: 'APR-2026-092', type: 'SAGA_HIGH_RISK_TOOL', target: 'task-ns-8841 · prod_db_schema_drop', region: 'us-west-2', tenant: 'Northstar', initiator: 'lin', status: 'PENDING_SECOND_ADMIN', risk: 'CRITICAL', createdAt: '09:27 UTC' },
        { id: 'APR-2026-088', type: 'BRAIN_SHARD_DRAIN', target: 'celld-brain-02 维护排水与槽位迁移', region: 'us-west-2', tenant: 'all', initiator: 'zhou', status: 'APPROVED (chen)', risk: 'HIGH', createdAt: '09:05 UTC' }
      ],
      auditLogs: [
        { ts: '09:28:14 UTC', actor: '陈 (SRE_ADMIN)', action: '提交租户紧急提额申请 APR-2026-091', scope: 'us-west-2 / Northstar', outcome: '待双人复核' },
        { ts: '09:21:05 UTC', actor: '林 (OPERATOR)', action: '触发 LayerFS 分支 feat-ns-auth 快照同步至 s3://ansts-fleet', scope: 'us-west-2 / Northstar', outcome: '成功 (Commit C4)' },
        { ts: '09:05:42 UTC', actor: '陈 (SRE_ADMIN)', action: '复核通过周发起的分片维护单 APR-2026-088', scope: 'us-west-2 / Global', outcome: '已生效 (Epoch #12)' }
      ]
    };
  }

  let state = createInitialState();
  let currentDialogHandler = null;
  let toastTimer = null;

  // Optional live BFF sync with ANSTS v2.5 server
  async function syncWithLiveBffIfAvailable() {
    if (state.scenario === 'offline') return;
    try {
      const canCall =
        typeof window !== 'undefined' &&
        window.location &&
        (window.location.protocol === 'http:' || window.location.protocol === 'https:') &&
        (window.location.port === '8088' || window.location.port === '18080' || window.location.port === '18081');
      if (!canCall) return;
      const targetUrl = `${window.location.origin}/v1beta1/management/state`;
      const resp = await fetch(targetUrl, {
        headers: { 'accept': 'application/json' }
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data && data.cluster) {
          state.liveClusterConnected = true;
          state.liveSlotCount = data.virtualSlots || 256;
        }
      }
    } catch (_e) {
      // Remain in local deterministic management simulation
    }
  }

  function showToast(msg) {
    const toast = document.getElementById('toast');
    const announcer = document.getElementById('announcer');
    if (announcer) announcer.textContent = msg;
    if (!toast) return;
    toast.textContent = msg;
    toast.hidden = false;
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => {
      toast.hidden = true;
    }, 3500);
  }

  function getPrincipal() {
    return IDENTITIES[state.identity] || IDENTITIES.chen;
  }

  function canMutateTarget(targetRegion, targetTenant, requireGlobalAdmin) {
    const p = getPrincipal();
    if (state.scenario === 'offline') return { ok: false, reason: '当前处于“接口不可用”场景，控制面写操作已锁定' };
    if (p.role === 'VIEWER') return { ok: false, reason: '当前身份为 VIEWER (只读观察员)，无写操作权限' };
    if (requireGlobalAdmin && !p.canWriteGlobal) {
      return { ok: false, reason: `当前身份 ${p.role} 仅限区域作用域操作，全局配额/平台策略需 SRE_ADMIN` };
    }
    if (!p.canWriteGlobal) {
      if (targetRegion && targetRegion !== 'all' && targetRegion !== p.scopeRegion) {
        return { ok: false, reason: `超出 OPERATOR 区域作用域 (仅限 ${p.scopeRegion})` };
      }
      if (targetTenant && targetTenant !== 'all' && targetTenant !== p.scopeTenant) {
        return { ok: false, reason: `超出 OPERATOR 租户作用域 (仅限 ${p.scopeTenant})` };
      }
    }
    return { ok: true, reason: '' };
  }

  function populateTenantFilter() {
    const sel = document.getElementById('tenant-filter');
    if (!sel) return;
    const currentVal = state.tenant;
    const options = [
      { value: 'all', label: '所有租户' },
      ...state.tenants.map((t) => ({ value: t.id, label: `${t.id} (${t.tier.split('_')[0]})` }))
    ];
    sel.innerHTML = options
      .map((o) => `<option value="${o.value}" ${o.value === currentVal ? 'selected' : ''}>${o.label}</option>`)
      .join('');
  }

  function updateHeaderAndRail() {
    const p = getPrincipal();
    const avatarEl = document.getElementById('avatar');
    const nameEl = document.getElementById('principal-name');
    const roleEl = document.getElementById('principal-role');
    const demoSumEl = document.getElementById('demo-summary');
    if (avatarEl) avatarEl.textContent = p.avatar;
    if (nameEl) nameEl.textContent = p.name;
    if (roleEl) roleEl.textContent = `${p.role}${p.scopeTenant !== 'all' ? ' · ' + p.scopeTenant : ''}`;
    if (demoSumEl) {
      demoSumEl.textContent = `${SCENARIO_LABELS[state.scenario] || '正常场景'} · ${p.avatar} / ${p.role}`;
    }

    // Health dots update according to scenario
    const dots = document.querySelectorAll('.health-dot');
    dots.forEach((d) => {
      d.className = 'health-dot';
      if (state.scenario === 'offline' || state.scenario === 'capacity' || state.scenario === 'verification') {
        d.classList.add('danger');
      } else if (state.scenario !== 'normal') {
        d.classList.add('warn');
      }
    });

    renderNavigation();
  }

  function getNavBadge(key) {
    if (key === 'overview') {
      if (state.scenario === 'capacity' || state.scenario === 'verification' || state.scenario === 'offline') {
        return { text: '告警', cls: 'danger' };
      }
      if (state.scenario === 'stale') return { text: '过期', cls: 'warn' };
      return { text: 'SLO 正常', cls: '' };
    }
    if (key === 'nodes') {
      if (state.scenario === 'partial') return { text: '1 失败', cls: 'danger' };
      return { text: `${state.nodes.length} 节点`, cls: '' };
    }
    if (key === 'tenants') {
      if (state.scenario === 'quota') return { text: '额度满', cls: 'danger' };
      if (state.scenario === 'budget' || state.scenario === 'conflict') return { text: '需关注', cls: 'warn' };
      return { text: `${state.tenants.length} 租户`, cls: '' };
    }
    if (key === 'tasks') {
      if (state.scenario === 'cancel-unknown' || state.scenario === 'history-missing') {
        return { text: '异常', cls: 'warn' };
      }
      return { text: `${state.tasks.length}`, cls: '' };
    }
    if (key === 'approvals') {
      const pending = state.approvals.filter((a) => a.status.startsWith('PENDING')).length;
      return { text: `${pending} 待审`, cls: pending > 0 ? 'warn' : '' };
    }
    return { text: '256 槽', cls: '' };
  }

  function renderNavigation() {
    const nav = document.getElementById('navigation');
    if (!nav) return;
    nav.innerHTML = NAV_ITEMS.map((item) => {
      const badge = getNavBadge(item.badgeKey);
      const activeCls = state.activeNav === item.id ? 'active' : '';
      return `
        <button type="button" class="nav-item ${activeCls}" data-action="switch-nav" data-nav="${item.id}">
          <span>${item.label}</span>
          <span class="nav-badge ${badge.cls}">${badge.text}</span>
        </button>
      `;
    }).join('');
  }

  function filterByScope(list, regionField, tenantField) {
    return list.filter((item) => {
      if (state.region !== 'all' && regionField && item[regionField] && item[regionField] !== 'all' && item[regionField] !== state.region) {
        return false;
      }
      if (state.tenant !== 'all' && tenantField && item[tenantField] && item[tenantField] !== 'all' && item[tenantField] !== state.tenant) {
        return false;
      }
      return true;
    });
  }

  function renderScenarioBanner() {
    const p = getPrincipal();
    const banners = [];

    if (p.role === 'VIEWER') {
      banners.push(`
        <div class="state-banner info">
          <div>
            <strong>只读观察员模式 (${p.name})</strong>
            <div class="muted">当前账号作用域限定于 ${p.scopeRegion} / ${p.scopeTenant}，所有控制面变更按钮已按 RBAC 策略禁用。</div>
          </div>
          <button type="button" class="small" data-action="open-identity-dialog">切换为 SRE_ADMIN</button>
        </div>
      `);
    } else if (p.role === 'OPERATOR') {
      banners.push(`
        <div class="state-banner info">
          <div>
            <strong>区域运维作用域 (${p.name} · ${p.scopeRegion} / ${p.scopeTenant})</strong>
            <div class="muted">允许执行本区域任务干预与节点预热；全局租户配额调整与高危双人复核需切换至 SRE_ADMIN。</div>
          </div>
          <button type="button" class="small" data-action="open-identity-dialog">切换演示身份</button>
        </div>
      `);
    }

    if (state.timeRange === 'history') {
      banners.push(`
        <div class="state-banner unknown">
          <div>
            <strong>历史回放视角 · 2026-11-10 09:00–09:30 UTC</strong>
            <div class="muted">当前展示历史窗口状态快照与归档回合轨迹。若需执行实时变更，请切回“当前状态”视角。</div>
          </div>
          <button type="button" class="small primary" data-action="switch-current-time">返回当前状态</button>
        </div>
      `);
    }

    switch (state.scenario) {
      case 'stale':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[数据过期告警] 遥测快照停留在 2026-11-10 09:12:00 UTC (已延迟 18 分钟)</strong>
              <div class="muted">Prometheus / CloudEvents 聚合流出现积压。为防止基于陈旧指标误触发缩容，自动扩缩容已被保护性挂起。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="refresh-stale-data">立即强制同步快照</button>
            </div>
          </div>
        `);
        break;
      case 'offline':
        banners.push(`
          <div class="state-banner danger">
            <div>
              <strong>[控制面接口不可用] BFF 聚合端点 (/v1beta1/management/*) 响应 503 Service Unavailable</strong>
              <div class="muted">管理控制台绝不伪造“健康”状态。当前仅展示本地最后一次已验证缓存（09:29 UTC），所有写操作已安全锁定。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="recover-offline-api">重试连接控制面</button>
            </div>
          </div>
        `);
        break;
      case 'capacity':
        banners.push(`
          <div class="state-banner danger">
            <div>
              <strong>[平台容量不足] us-west-2 计算与热池饱和度达 96.4% (Warm Pool 命中率跌至 61%)</strong>
              <div class="muted">P0 紧急任务排队等待 P99 升至 340ms（超出 150ms SLO）。建议立即开启 P3/P2 优先级削峰或扩容热备节点。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-capacity-mitigation">执行容量削峰与扩容</button>
            </div>
          </div>
        `);
        break;
      case 'quota':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[租户额度耗尽] 租户 Northstar 并发沙箱配额已达 120 / 120 (100%)</strong>
              <div class="muted">当前有 14 个子 Agent 任务因 DAG Escrow 配额触顶处于排队等待，需临时提额或清理低优任务。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-quota-dialog" data-tenant="Northstar">调整 Northstar 配额</button>
            </div>
          </div>
        `);
        break;
      case 'budget':
        banners.push(`
          <div class="state-banner unknown">
            <div>
              <strong>[费用数据未知] CloudEvents 计费流水延迟，当前日累计账单状态为 UNKNOWN</strong>
              <div class="muted">系统严禁将未知费用伪造为 $0.00。已自动启用 FinOps 保护性软降级策略（优先路由至 Qwen-3.8 经济模型）。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="small" data-action="open-budget-policy-dialog">配置未知账单保护策略</button>
            </div>
          </div>
        `);
        break;
      case 'partial':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[批量执行部分失败] 最近一次节点批量维护：2 个节点成功，1 个节点 (body-host-02) 失败</strong>
              <div class="muted">失败原因：ERR_ACTIVE_LEASE_PINNED（body-host-02 上仍有未释放的 P0 沙箱租约）。支持针对失败项单独重试或强制迁移。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="go-nodes-partial">查看失败明细并重试</button>
            </div>
          </div>
        `);
        break;
      case 'cancel-unknown':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[任务停止未确认] 任务 task-ns-8841 停止指令已下发，但未收到 body-host-02 反向隧道 ACK</strong>
              <div class="muted">在宿主确认回收容器前，状态保持为“停止未确认 (CANCEL_UNCONFIRMED)”。可通过推进 Epoch Fencing Token 强制吊销旧租约。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-force-fence-dialog" data-task="task-ns-8841">强制吊销旧租约 (Bump Fence)</button>
            </div>
          </div>
        `);
        break;
      case 'conflict':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[配置版本并发冲突模拟就绪] 租户 Northstar 配置版本已被 周 (SRE_ADMIN) 更新至 v15</strong>
              <div class="muted">当前表单持有旧版本基线 v14。点击编辑配额并提交将触发 HTTP 409 CAS 版本冲突校验与差异合并流程。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-quota-dialog" data-tenant="Northstar">触发并发冲突演示</button>
            </div>
          </div>
        `);
        break;
      case 'history-missing':
        banners.push(`
          <div class="state-banner unknown">
            <div>
              <strong>[历史回合不可用] 任务 task-ns-8841 的 Turn #4–#5 LTX 增量段在对象存储中缺失</strong>
              <div class="muted">由于跨区异步复制延迟或段压缩未完成，Turn #4–#5 无法直接检出。系统支持回退至最近完整校验点 Turn #3 (Commit C2) 派生分支。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-turn-history-dialog" data-task="task-ns-8841">查看回合诊断与安全分叉</button>
            </div>
          </div>
        `);
        break;
      case 'verification':
        banners.push(`
          <div class="state-banner danger">
            <div>
              <strong>[恢复验证未通过 · 指标再次超标] 扩容动作已执行，但验证窗口内 P0 排队 P99 再次升至 410ms (> 200ms 阈值)</strong>
              <div class="muted">根因定位：热点哈希槽 (#042, #129) 在 celld-brain-02 上出现锁争用，单纯增加 Body 节点未能消除控制面瓶颈。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-rebalance-verification">执行二级恢复：分片重平衡与回滚验证</button>
            </div>
          </div>
        `);
        break;
      default:
        break;
    }

    return banners.join('');
  }

  function renderOverviewSection() {
    const isOffline = state.scenario === 'offline';
    const isCap = state.scenario === 'capacity';
    const isVer = state.scenario === 'verification';
    const isBudgetUnknown = state.scenario === 'budget';
    const isQuotaFull = state.scenario === 'quota';

    const p99Text = isOffline ? 'UNKNOWN (接口离线)' : isVer ? '410 ms (复测超标)' : isCap ? '340 ms (超标)' : '68 ms';
    const capText = isOffline ? 'UNKNOWN' : isCap && !state.loadSheddingActive ? '96.4% (饱和)' : '74.2%';
    const finopsText = isOffline || isBudgetUnknown ? 'UNKNOWN (计费流延迟)' : '$1,785 / $2,900';
    const syncLabel = state.scenario === 'stale' ? '2026-11-10 09:12:00 UTC (陈旧)' : state.lastSyncUtc;

    const scopedNodes = filterByScope(state.nodes, 'region', null);
    const scopedTenants = filterByScope(state.tenants, 'region', 'id');

    return `
      <section class="kpi-grid" aria-label="核心态势指标">
        <div class="kpi-card">
          <div class="kpi-label"><span>P0 调度排队延迟 (P99)</span><span class="status-pill ${isCap || isVer ? 'danger' : isOffline ? 'unknown' : 'ok'}">${isOffline ? '未知' : isCap || isVer ? 'SLO 违约' : '达标'}</span></div>
          <div class="kpi-value">${p99Text}</div>
          <div class="kpi-sub">SLO 目标 &lt; 150ms · 采样基准: <span class="mono">${syncLabel}</span></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>集群计算与热池水位</span><span class="status-pill ${isCap && !state.loadSheddingActive ? 'danger' : 'ok'}">${state.loadSheddingActive ? '已开启削峰' : '256 哈希槽'}</span></div>
          <div class="kpi-value">${capText}</div>
          <div class="kpi-sub">作用域内节点: ${scopedNodes.length} 个 · 热池命中率: ${isCap ? '61.0%' : '94.8%'}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>租户配额与并发担保</span><span class="status-pill ${isQuotaFull ? 'warn' : 'ok'}">${isQuotaFull ? 'Northstar 触顶' : 'DRF 均衡'}</span></div>
          <div class="kpi-value">${isQuotaFull ? '120 / 120' : '187 / 300'} 槽</div>
          <div class="kpi-sub">作用域内租户: ${scopedTenants.length} 个 · DAG 预留担保: 34 槽</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>FinOps 日预算消耗</span><span class="status-pill ${isBudgetUnknown || isOffline ? 'unknown' : 'ok'}">${isBudgetUnknown ? '数据未知保护' : '实时计量'}</span></div>
          <div class="kpi-value">${finopsText}</div>
          <div class="kpi-sub">软降级阈值: 80% (DeepSeek → Qwen-3.8) · 硬门控: 100% (402)</div>
        </div>
      </section>

      <div class="split-grid">
        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">多区域控制面与容灾恢复看板</h2>
              <p class="panel-subtitle">展示各区域 celld 分片健康度、LayerFS S3 LTX 跨区复制延迟及变更后恢复验证闭环。</p>
            </div>
            <div class="btn-row">
              <button type="button" data-action="open-capacity-mitigation" ${!canMutateTarget('us-west-2', 'all', false).ok ? 'disabled' : ''}>容量削峰与预热</button>
              <button type="button" class="primary" data-action="open-rebalance-verification" ${!canMutateTarget('all', 'all', true).ok ? 'disabled' : ''}>分片重平衡验证</button>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>区域 (Region)</th>
                  <th>主权合规域</th>
                  <th>Brain 分片 / 槽位</th>
                  <th>CPU / PSI 内存压力</th>
                  <th>S3 LTX 跨区延迟</th>
                  <th>验证状态</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td class="mono"><strong>us-west-2</strong></td>
                  <td><span class="status-pill">US_FED</span></td>
                  <td class="mono">brain-01, brain-02 (171 槽)</td>
                  <td class="mono">${isCap ? '96.4% / PSI 22.1%' : '66.0% / PSI 5.1%'}</td>
                  <td class="mono">RPO=0 (12ms)</td>
                  <td><span class="status-pill ${isVer || isCap ? 'danger' : 'ok'}">${isVer ? '复测超标 (410ms)' : isCap ? '容量告警' : '验证通过'}</span></td>
                </tr>
                <tr>
                  <td class="mono"><strong>eu-central-1</strong></td>
                  <td><span class="status-pill ok">GDPR_EU 锁定</span></td>
                  <td class="mono">brain-03 (85 槽)</td>
                  <td class="mono">54.2% / PSI 3.1%</td>
                  <td class="mono">→ eu-west-1 (18ms)</td>
                  <td><span class="status-pill ok">验证通过</span></td>
                </tr>
                <tr>
                  <td class="mono"><strong>ap-northeast-1</strong></td>
                  <td><span class="status-pill">APAC_SG</span></td>
                  <td class="mono">边缘代理接入 → brain-01</td>
                  <td class="mono">48.0% / PSI 2.5%</td>
                  <td class="mono">RPO=0 (29ms)</td>
                  <td><span class="status-pill ok">验证通过</span></td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">最近管理面审计与恢复轨迹</h2>
              <p class="panel-subtitle">所有管理动作均记录操作人身份、版本基线及恢复后指标复核结果。</p>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>时间</th>
                  <th>执行身份</th>
                  <th>动作摘要</th>
                  <th>结果</th>
                </tr>
              </thead>
              <tbody>
                ${state.auditLogs.slice(0, 6).map((log) => `
                  <tr>
                    <td class="mono">${log.ts}</td>
                    <td>${log.actor}</td>
                    <td>${log.action}</td>
                    <td><span class="status-pill ${log.outcome.includes('失败') || log.outcome.includes('超标') ? 'danger' : log.outcome.includes('待') ? 'warn' : 'ok'}">${log.outcome}</span></td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }

  function renderNodesSection() {
    const scopedNodes = filterByScope(state.nodes, 'region', null);
    const perm = canMutateTarget(state.region, state.tenant, false);

    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">节点与 256-Slot 分片单元编排 (${scopedNodes.length} 个节点)</h2>
            <p class="panel-subtitle">管理 celld Brain 调度分片与 Body LayerFS 沙箱宿主，支持批量隔离排水 (Cordon &amp; Drain) 及双归属反向隧道切换。</p>
          </div>
          <div class="btn-row">
            <button type="button" data-action="run-batch-node-op" data-op="prewarm" ${!perm.ok ? 'disabled' : ''}>批量预热沙箱池</button>
            <button type="button" class="primary" data-action="run-batch-node-op" data-op="drain" ${!perm.ok ? 'disabled' : ''}>批量执行节点维护排水</button>
          </div>
        </div>

        ${state.scenario === 'partial' || state.batchLastResult ? `
          <div class="state-banner warn">
            <div>
              <strong>批量维护执行报告：成功 2 项 (celld-brain-01, body-host-01) · 失败 1 项 (body-host-02)</strong>
              <div class="muted">失败详情：<code>body-host-02</code> 返回 <code>409 ERR_ACTIVE_LEASE_PINNED</code>（持有 P0 任务 <code>task-ns-8841</code> 互斥租约 Epoch #12）。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="small" data-action="retry-failed-batch-node" ${!perm.ok ? 'disabled' : ''}>强制迁移 P0 租约并重试失败项</button>
            </div>
          </div>
        ` : ''}

        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>节点 ID</th>
                <th>角色与分片槽</th>
                <th>区域</th>
                <th>CPU / PSI 水位</th>
                <th>活跃沙箱租约</th>
                <th>反向隧道网关</th>
                <th>状态</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              ${scopedNodes.map((n) => {
                const nodePerm = canMutateTarget(n.region, 'all', false);
                const isPartialFail = (state.scenario === 'partial' && n.id === 'body-host-02' && ! n.partialFixed);
                return `
                  <tr>
                    <td class="mono"><strong>${n.id}</strong></td>
                    <td>${n.role} ${n.slots > 0 ? `<span class="mono">(${n.slots} 槽)</span>` : ''}</td>
                    <td class="mono">${n.region}</td>
                    <td class="mono">${n.cpuPct}% / PSI ${n.psiPct}%</td>
                    <td class="mono">${n.activeLeases}</td>
                    <td class="mono">${n.tunnelGw}</td>
                    <td>
                      <span class="status-pill ${isPartialFail ? 'danger' : n.status === 'CORDONED' ? 'warn' : 'ok'}">
                        ${isPartialFail ? 'DRAIN_FAILED (锁冲突)' : n.status}
                      </span>
                    </td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small" data-action="toggle-node-cordon" data-node="${n.id}" ${!nodePerm.ok ? 'disabled' : ''}>
                          ${n.status === 'CORDONED' ? '恢复调度 (Uncordon)' : '隔离 (Cordon)'}
                        </button>
                      </div>
                    </td>
                  </tr>
                `;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderTenantsSection() {
    const scopedTenants = filterByScope(state.tenants, 'region', 'id');
    const isBudgetUnknown = state.scenario === 'budget';
    const isQuotaScenario = state.scenario === 'quota';

    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">多租户 DRF 配额、DAG 并发担保与 FinOps 预算门控</h2>
            <p class="panel-subtitle">所有配额与预算策略变更均执行乐观锁 CAS 版本校验 (expectedVersion)，防止多管理员并发覆盖。</p>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>租户名称</th>
                <th>区域 / 等级</th>
                <th>并发沙箱配额 (已用 / 上限)</th>
                <th>DAG 子任务预留 (Escrow)</th>
                <th>FinOps 日预算消耗</th>
                <th>模型路由与降级策略</th>
                <th>配置版本</th>
                <th>管理操作</th>
              </tr>
            </thead>
            <tbody>
              ${scopedTenants.map((t) => {
                const used = (isQuotaScenario && t.id === 'Northstar' && t.maxSlots === 120) ? 120 : t.usedSlots;
                const ratio = Math.round((used / t.maxSlots) * 100);
                const perm = canMutateTarget(t.region, t.id, false);
                return `
                  <tr>
                    <td><strong>${t.id}</strong></td>
                    <td class="mono">${t.region} · ${t.tier}</td>
                    <td class="mono">
                      <span class="status-pill ${ratio >= 100 ? 'danger' : ratio >= 80 ? 'warn' : 'ok'}">${used} / ${t.maxSlots} (${ratio}%)</span>
                    </td>
                    <td class="mono">${t.escrowReserved} 槽</td>
                    <td class="mono">
                      ${isBudgetUnknown ? '<span class="status-pill unknown">UNKNOWN (计费延迟)</span>' : `$${t.spentUsd} / $${t.dailyBudgetUsd}`}
                    </td>
                    <td>${isBudgetUnknown ? '保护性降级: Qwen-3.8' : t.modelPolicy}</td>
                    <td class="mono">v${t.configVersion}</td>
                    <td>
                      <button type="button" class="small primary" data-action="open-quota-dialog" data-tenant="${t.id}" ${!perm.ok ? 'disabled' : ''}>
                        编辑配额与预算
                      </button>
                    </td>
                  </tr>
                `;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderTasksSection() {
    const scopedTasks = filterByScope(state.tasks, 'region', 'tenant').filter((t) =>
      state.timeRange === 'history' ? true : t.historyWindow === 'current' || t.state !== 'ARCHIVED'
    );

    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">全局任务运行队列、停止确认与历史回合 Time-Travel 诊断</h2>
            <p class="panel-subtitle">当宿主隧道未返回停止 ACK 时严格保持“停止未确认”状态；当历史 LTX 段缺失时引导回退至最近完整校验点。</p>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>任务 ID</th>
                <th>租户 / 区域</th>
                <th>优先级</th>
                <th>Brain 分片 / Body 宿主</th>
                <th>Fencing Token</th>
                <th>当前回合 / LayerFS 分支</th>
                <th>状态</th>
                <th>诊断与干预</th>
              </tr>
            </thead>
            <tbody>
              ${scopedTasks.map((t) => {
                const perm = canMutateTarget(t.region, t.tenant, false);
                const isCancelUnconfirmed = (state.scenario === 'cancel-unknown' && t.id === 'task-ns-8841' && t.state !== 'FENCED_TERMINATED');
                const displayState = isCancelUnconfirmed ? 'CANCEL_UNCONFIRMED (停止未确认)' : t.state;
                const pillCls = isCancelUnconfirmed ? 'warn' : t.state === 'FENCED_TERMINATED' ? 'danger' : 'ok';
                return `
                  <tr>
                    <td class="mono"><strong>${t.id}</strong></td>
                    <td>${t.tenant} · <span class="mono">${t.region}</span></td>
                    <td class="mono">${t.priority}</td>
                    <td class="mono">${t.brain} → ${t.body}</td>
                    <td class="mono">epoch:${t.fence}</td>
                    <td class="mono">Turn #${t.turn} · ⎇ ${t.branch}</td>
                    <td><span class="status-pill ${pillCls}">${displayState}</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small" data-action="open-turn-history-dialog" data-task="${t.id}">
                          回合历史 / 分叉
                        </button>
                        <button type="button" class="small danger" data-action="open-force-fence-dialog" data-task="${t.id}" ${!perm.ok || t.state === 'COMPLETED' || t.state === 'FENCED_TERMINATED' ? 'disabled' : ''}>
                          ${isCancelUnconfirmed ? '强制吊销租约' : '停止任务'}
                        </button>
                      </div>
                    </td>
                  </tr>
                `;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderApprovalsSection() {
    const p = getPrincipal();
    const scopedApprovals = filterByScope(state.approvals, 'region', 'tenant');

    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">高危操作双人复核 (4-Eyes Principle) 与 Saga 审批门控</h2>
            <p class="panel-subtitle">遵循安全合规要求：高危变更需由另一位 SRE_ADMIN 复核通过（发起人与审批人不得为同一人）。当前登录：<strong>${p.name}</strong></p>
          </div>
          <div class="btn-row">
            <button type="button" class="small" data-action="open-identity-dialog">切换陈/周身份完成双人复核</button>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>审批单号</th>
                <th>变更类型</th>
                <th>操作目标与内容</th>
                <th>区域 / 租户</th>
                <th>发起人</th>
                <th>风险等级</th>
                <th>当前状态</th>
                <th>复核操作</th>
              </tr>
            </thead>
            <tbody>
              ${scopedApprovals.map((a) => {
                const isPending = a.status.startsWith('PENDING');
                const isSameUser = a.initiator === p.id;
                const canApproveThis = isPending && p.canApprove && !isSameUser && state.scenario !== 'offline';
                return `
                  <tr>
                    <td class="mono"><strong>${a.id}</strong></td>
                    <td class="mono">${a.type}</td>
                    <td>${a.target}</td>
                    <td class="mono">${a.region} / ${a.tenant}</td>
                    <td>${IDENTITIES[a.initiator]?.name || a.initiator}</td>
                    <td><span class="status-pill ${a.risk === 'CRITICAL' ? 'danger' : 'warn'}">${a.risk}</span></td>
                    <td><span class="status-pill ${isPending ? 'warn' : 'ok'}">${a.status}</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small primary" data-action="decide-approval" data-id="${a.id}" data-decision="approve" ${!canApproveThis ? 'disabled' : ''} title="${isSameUser ? '发起人不能自审，请切换至另一位 SRE_ADMIN' : ''}">
                          批准执行
                        </button>
                        <button type="button" class="small danger" data-action="decide-approval" data-id="${a.id}" data-decision="reject" ${!canApproveThis ? 'disabled' : ''}>
                          驳回
                        </button>
                      </div>
                    </td>
                  </tr>
                `;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderRuntimeSection() {
    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">ANSTS v2.5 分布式集群 11 容器运行面拓扑联动</h2>
            <p class="panel-subtitle">实时映射 <code>deploy/dev-cluster/docker-compose.dev-cluster.yml</code> 中的网关、3 分片 Brain、2 节点 Body LayerFS 与 RustFS 对象存储。</p>
          </div>
          <div class="btn-row">
            <a href="index.html" class="status-pill ok" style="text-decoration:none;padding:6px 12px;">打开 TanStack 实时运行面控制台 ↗</a>
          </div>
        </div>
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-label"><span>Edge Gateways</span><span class="status-pill ok">:18080 / :18081</span></div>
            <div class="kpi-value">celld-gw-01/02</div>
            <div class="kpi-sub">无状态全局任务 Fan-Out 与双归属反向隧道 Hub</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label"><span>Sharded celld Brains</span><span class="status-pill ok">:18082..18084</span></div>
            <div class="kpi-value">256 Virtual Slots</div>
            <div class="kpi-sub">brain-node-01 (85) · brain-02 (86) · brain-03 (85)</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label"><span>LayerFS Body Hosts</span><span class="status-pill ok">:19501..19502</span></div>
            <div class="kpi-value">CoW + S3 Sync</div>
            <div class="kpi-sub">毫秒级零拷贝分支克隆、三路合并与 SLSA/TEE 证明</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label"><span>RustFS + Prometheus</span><span class="status-pill ok">:9000 / :9090</span></div>
            <div class="kpi-value">s3://ansts-fleet</div>
            <div class="kpi-sub">RPO=0 LTX WAL 归档与 15s 指标抓取闭环</div>
          </div>
        </div>
      </div>
    `;
  }

  function renderMainContent() {
    const main = document.getElementById('content');
    if (!main) return;
    const bannerHtml = renderScenarioBanner();
    let sectionHtml = '';
    switch (state.activeNav) {
      case 'nodes':
        sectionHtml = renderNodesSection();
        break;
      case 'tenants':
        sectionHtml = renderTenantsSection();
        break;
      case 'tasks':
        sectionHtml = renderTasksSection();
        break;
      case 'approvals':
        sectionHtml = renderApprovalsSection();
        break;
      case 'runtime':
        sectionHtml = renderRuntimeSection();
        break;
      case 'overview':
      default:
        sectionHtml = renderOverviewSection();
        break;
    }
    main.innerHTML = bannerHtml + sectionHtml;
  }

  // Accessible Dialog Controller
  function openDialog(title, bodyHtml, submitLabel, onSubmit) {
    const dialog = document.getElementById('dialog');
    const titleEl = document.getElementById('dialog-title');
    const bodyEl = document.getElementById('dialog-body');
    const errEl = document.getElementById('dialog-error');
    const submitBtn = document.getElementById('dialog-submit');
    if (!dialog || !titleEl || !bodyEl || !errEl || !submitBtn) return;

    titleEl.textContent = title;
    bodyEl.innerHTML = bodyHtml;
    errEl.textContent = '';
    submitBtn.textContent = submitLabel || '确认';
    currentDialogHandler = onSubmit;

    if (typeof dialog.showModal === 'function') {
      if (!dialog.open) dialog.showModal();
    } else {
      dialog.setAttribute('open', 'open');
    }
  }

  function closeDialog() {
    const dialog = document.getElementById('dialog');
    if (!dialog) return;
    if (typeof dialog.close === 'function' && dialog.open) {
      dialog.close();
    } else {
      dialog.removeAttribute('open');
    }
    currentDialogHandler = null;
  }

  // Specific Dialog Workflows
  function openIdentityDialog() {
    const cur = state.identity;
    openDialog(
      '切换演示身份 (RBAC 权限与双人复核验证)',
      `
        <div class="form-field">
          <label for="dlg-identity-select">选择控制台当前登录身份</label>
          <select id="dlg-identity-select">
            <option value="chen" ${cur === 'chen' ? 'selected' : ''}>陈 · SRE_ADMIN · 全平台读写与审批</option>
            <option value="zhou" ${cur === 'zhou' ? 'selected' : ''}>周 · SRE_ADMIN · 全平台 (用于双人复核与并发冲突验证)</option>
            <option value="lin" ${cur === 'lin' ? 'selected' : ''}>林 · OPERATOR · 仅限 US West / Northstar 运维</option>
            <option value="xu" ${cur === 'xu' ? 'selected' : ''}>许 · VIEWER · 仅限 US West / Northstar 只读观察</option>
          </select>
        </div>
        <div class="muted">
          提示：高危审批单遵循双人复核原则（陈发起的申请需切换至周批准；林/许无审批权限）。
        </div>
      `,
      '切换身份',
      () => {
        const sel = document.getElementById('dlg-identity-select');
        if (sel) {
          applyIdentityChange(sel.value);
        }
        closeDialog();
      }
    );
  }

  function openQuotaDialog(tenantId) {
    const tenant = state.tenants.find((t) => t.id === tenantId) || state.tenants[0];
    const simulatedRemoteVersion = state.scenario === 'conflict' && tenant.id === 'Northstar' ? 15 : tenant.configVersion;

    openDialog(
      `编辑租户配额与 FinOps 策略 · ${tenant.id}`,
      `
        <div class="form-field">
          <label for="dlg-quota-max">并发沙箱槽位上限 (Max Concurrent Slots)</label>
          <input id="dlg-quota-max" type="number" value="${tenant.maxSlots === 120 ? 160 : tenant.maxSlots}" min="10" max="500" />
        </div>
        <div class="form-field">
          <label for="dlg-quota-budget">日预算上限 (Daily Budget USD)</label>
          <input id="dlg-quota-budget" type="number" value="${tenant.dailyBudgetUsd}" min="50" max="10000" />
        </div>
        <div class="form-field">
          <label for="dlg-quota-version">乐观锁基线版本 (CAS Expected Version)</label>
          <input id="dlg-quota-version" type="number" value="${tenant.configVersion}" readonly class="mono" />
        </div>
        <div class="muted">
          当前远端存储版本：<strong class="mono">v${simulatedRemoteVersion}</strong>
          ${simulatedRemoteVersion !== tenant.configVersion ? '（周 · SRE_ADMIN 刚刚提交了 v15，直接确认将触发 409 冲突拦截）' : ''}
        </div>
      `,
      '提交变更 (CAS 校验)',
      () => {
        const errEl = document.getElementById('dialog-error');
        const expectedVer = Number(document.getElementById('dlg-quota-version').value);
        const newMax = Number(document.getElementById('dlg-quota-max').value);
        const newBudget = Number(document.getElementById('dlg-quota-budget').value);

        if (state.scenario === 'conflict' && expectedVer < simulatedRemoteVersion) {
          errEl.innerHTML = `
            <strong>HTTP 409 CAS_VERSION_CONFLICT：配置版本冲突</strong><br/>
            您基于 <code>v${expectedVer}</code> 修改，但远端已被 <strong>周 · SRE_ADMIN</strong> 更新为 <code>v${simulatedRemoteVersion}</code>（并发上限调整为 150）。
            <div style="margin-top:8px;">
              <button type="button" class="small primary" data-action="resolve-cas-conflict" data-ver="${simulatedRemoteVersion}">加载最新 v${simulatedRemoteVersion} 基线并合并提交</button>
            </div>
          `;
          showToast('拦截到并发配置冲突 (HTTP 409)');
          return;
        }

        tenant.maxSlots = newMax;
        tenant.dailyBudgetUsd = newBudget;
        tenant.configVersion = Math.max(tenant.configVersion, simulatedRemoteVersion) + 1;
        if (state.scenario === 'quota' && tenant.id === 'Northstar' && newMax > 120) {
          state.scenario = 'normal';
          const scSel = document.getElementById('scenario');
          if (scSel) scSel.value = 'normal';
        }
        state.auditLogs.unshift({
          ts: '09:31:02 UTC',
          actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
          action: `更新租户 ${tenant.id} 配额上限至 ${newMax} 槽 / 日预算 $${newBudget}`,
          scope: `${tenant.region} / ${tenant.id}`,
          outcome: `已生效 (v${tenant.configVersion})`
        });
        closeDialog();
        updateHeaderAndRail();
        renderMainContent();
        showToast(`已更新 ${tenant.id} 配额与预算 (版本 v${tenant.configVersion})`);
      }
    );
  }

  function openForceFenceDialog(taskId) {
    const task = state.tasks.find((t) => t.id === taskId) || state.tasks[0];
    openDialog(
      `任务停止确认与强制租约吊销 · ${task.id}`,
      `
        <div>
          任务 <strong class="mono">${task.id}</strong> 当前位于宿主 <code class="mono">${task.body}</code>，持有租约令牌 <code class="mono">epoch:${task.fence}</code>。
        </div>
        <div class="muted">
          在“任务停止未确认”场景下，直接标记为“已停止”会导致僵尸容器继续写入共享分支。推进 Epoch Fencing Token 至 <code class="mono">epoch:${task.fence + 1}</code> 可在控制面与存储层原子拒绝旧宿主的任何后续写入（返回 HTTP 409 StaleFenceError）。
        </div>
      `,
      `强制推进至 Epoch #${task.fence + 1} 并终止`,
      () => {
        task.fence += 1;
        task.state = 'FENCED_TERMINATED';
        if (state.scenario === 'cancel-unknown') {
          state.scenario = 'normal';
          const scSel = document.getElementById('scenario');
          if (scSel) scSel.value = 'normal';
        }
        state.auditLogs.unshift({
          ts: '09:31:18 UTC',
          actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
          action: `强制推进 ${task.id} 租约令牌至 epoch:${task.fence} 并吊销旧容器写入权限`,
          scope: `${task.region} / ${task.tenant}`,
          outcome: '已吊销隔离'
        });
        closeDialog();
        updateHeaderAndRail();
        renderMainContent();
        showToast(`任务 ${task.id} 已通过 Epoch #${task.fence} 强制隔离终止`);
      }
    );
  }

  function openTurnHistoryDialog(taskId) {
    const task = state.tasks.find((t) => t.id === taskId) || state.tasks[0];
    const isMissing = state.scenario === 'history-missing';

    openDialog(
      `任务回合历史与 Time-Travel 分叉诊断 · ${task.id}`,
      `
        <div>目标任务：<strong class="mono">${task.id}</strong> · LayerFS 分支：<code class="mono">${task.branch}</code></div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>回合</th><th>LayerFS Commit</th><th>LTX 归档状态</th><th>操作</th></tr>
            </thead>
            <tbody>
              <tr>
                <td class="mono">Turn #1</td><td class="mono">C0</td>
                <td><span class="status-pill ok">S3 LTX 完整</span></td>
                <td><span class="muted">可安全检出</span></td>
              </tr>
              <tr>
                <td class="mono">Turn #2</td><td class="mono">C1</td>
                <td><span class="status-pill ok">S3 LTX 完整</span></td>
                <td><span class="muted">可安全检出</span></td>
              </tr>
              <tr>
                <td class="mono"><strong>Turn #3</strong></td><td class="mono"><strong>C2 (推荐恢复点)</strong></td>
                <td><span class="status-pill ok">S3 LTX 完整 (CRC32 校验通过)</span></td>
                <td><span class="status-pill ok">最佳分叉基线</span></td>
              </tr>
              <tr>
                <td class="mono">Turn #4–#5</td><td class="mono">C3 / C4</td>
                <td>
                  <span class="status-pill ${isMissing ? 'danger' : 'ok'}">
                    ${isMissing ? 'LTX_SEGMENT_MISSING (段不可用)' : 'S3 LTX 完整'}
                  </span>
                </td>
                <td><span class="muted">${isMissing ? '禁止从损坏段分叉' : '可安全检出'}</span></td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="form-field">
          <label for="dlg-fork-turn">选择 Time-Travel 零拷贝分叉回合</label>
          <select id="dlg-fork-turn">
            <option value="3" selected>Turn #3 (Commit C2 · 最近完整校验点)</option>
            <option value="2">Turn #2 (Commit C1)</option>
            <option value="4">${isMissing ? 'Turn #4 (缺失段 · 将触发完整性拦截)' : 'Turn #4 (Commit C3)'}</option>
          </select>
        </div>
      `,
      '从所选回合派生新任务与分支',
      () => {
        const errEl = document.getElementById('dialog-error');
        const turnVal = Number(document.getElementById('dlg-fork-turn').value);
        if (isMissing && turnVal >= 4) {
          errEl.textContent = '拒绝执行：Turn #4 的 S3 LTX 增量段尚未就绪，强制恢复将破坏 SQLite WAL 一致性。请选择 Turn #3 (Commit C2)。';
          return;
        }
        const newId = `${task.id}-fork-t${turnVal}`;
        state.tasks.unshift({
          id: newId,
          tenant: task.tenant,
          region: task.region,
          priority: task.priority,
          brain: task.brain,
          body: 'body-host-01',
          fence: 1,
          turn: turnVal,
          branch: `${task.branch}-t${turnVal}`,
          state: 'RUNNING',
          historyWindow: 'current',
          turnsAvailable: [1, 2, 3]
        });
        state.auditLogs.unshift({
          ts: '09:31:40 UTC',
          actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
          action: `从 ${task.id} Turn #${turnVal} (Commit C2) 零拷贝派生任务 ${newId}`,
          scope: `${task.region} / ${task.tenant}`,
          outcome: '已创建并运行'
        });
        closeDialog();
        renderMainContent();
        showToast(`已从 Turn #${turnVal} 零拷贝派生新任务 ${newId}`);
      }
    );
  }

  function openCapacityMitigationDialog() {
    openDialog(
      '平台容量削峰与热备沙箱池扩容 (us-west-2)',
      `
        <div>当前 <code>us-west-2</code> 计算水位 <strong>96.4%</strong>，P0 排队延迟 <strong>340ms</strong>。请选择组合缓解动作：</div>
        <div class="form-field">
          <label for="dlg-cap-action">容量治理动作</label>
          <select id="dlg-cap-action">
            <option value="shed-and-scale" selected>开启 P3/P2 优先级削峰 + 预热扩容 16 个热沙箱</option>
            <option value="shed-only">仅开启 P3_BATCH 准入限流 (HTTP 429 Retry-After)</option>
          </select>
        </div>
      `,
      '立即执行容量恢复',
      () => {
        state.loadSheddingActive = true;
        if (state.scenario === 'capacity') {
          state.scenario = 'normal';
          const scSel = document.getElementById('scenario');
          if (scSel) scSel.value = 'normal';
        }
        state.auditLogs.unshift({
          ts: '09:32:00 UTC',
          actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
          action: '在 us-west-2 开启 P3/P2 优先级削峰并扩容热沙箱池',
          scope: 'us-west-2 / Global',
          outcome: '水位降至 74.2%'
        });
        closeDialog();
        updateHeaderAndRail();
        renderMainContent();
        showToast('已启用优先级削峰与热池扩容，P0 延迟恢复至 68ms');
      }
    );
  }

  function openRebalanceVerificationDialog() {
    openDialog(
      '二级恢复：256 哈希槽重平衡与 SLO 指标复核',
      `
        <div>
          检测到初次扩容后 <code>celld-brain-02</code> 上的热点槽位（#042, #129）仍导致排队延迟复测超标（410ms）。
        </div>
        <div class="muted">
          执行“热点分片迁移与槽位重平衡”将把热点租户槽位迁移至负载较低的 <code>celld-brain-01</code> 与 <code>celld-brain-03</code>，并重新触发 60 秒 SLO 达标验证。
        </div>
      `,
      '执行槽位重平衡并复核指标',
      () => {
        state.verificationRetried = true;
        if (state.scenario === 'verification') {
          state.scenario = 'normal';
          const scSel = document.getElementById('scenario');
          if (scSel) scSel.value = 'normal';
        }
        state.auditLogs.unshift({
          ts: '09:32:15 UTC',
          actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
          action: '执行 256-Slot 热点槽位重平衡并通过 SLO 二次验证 (410ms → 64ms)',
          scope: 'Global / All Brains',
          outcome: '验证通过'
        });
        closeDialog();
        updateHeaderAndRail();
        renderMainContent();
        showToast('256 哈希槽重平衡完成，二次验证指标已恢复正常 (64ms)');
      }
    );
  }

  function openDesignSpecDialog() {
    openDialog(
      'ANSTS 平台管理控制台 · 功能设计文档 (console-management-design.md)',
      `
        <div style="line-height:1.6;">
          <p><strong>1. 管理面核心定位：</strong>面向平台 SRE 管理员（SRE_ADMIN）、区域运维（OPERATOR）与只读审计员（VIEWER），提供多区域容量治理、节点分片编排、多租户配额/FinOps 门控、任务停止确认与高危双人复核能力。</p>
          <p><strong>2. 零伪造状态原则 (Zero-Fabrication Discipline)：</strong></p>
          <ul>
            <li>当遥测流延迟时显式标记 <code>数据过期 (STALE)</code> 并冻结盲目自动缩容；</li>
            <li>当控制面接口离线时显式标记 <code>503 接口不可用</code> 并锁定写操作；</li>
            <li>当计费流缺失时显式标记 <code>UNKNOWN</code> 而非伪造 <code>$0.00</code>；</li>
            <li>当任务停止未收到宿主反向隧道 ACK 时保持 <code>CANCEL_UNCONFIRMED</code>，支持推进 Fencing Token 强制隔离。</li>
          </ul>
          <p><strong>3. 并发安全与双人复核：</strong>所有配额变更强制校验 CAS 版本号（拦截 <code>409 CONFLICT</code>），高危操作强制执行陈/周双管理员异体复核。</p>
        </div>
      `,
      '已知悉',
      () => closeDialog()
    );
  }

  function applyIdentityChange(newIdentity) {
    if (!IDENTITIES[newIdentity]) return;
    state.identity = newIdentity;
    const idSel = document.getElementById('identity');
    if (idSel) idSel.value = newIdentity;
    const p = getPrincipal();
    // If scoped role selected, align region/tenant filter to help operator focus
    if (p.scopeRegion !== 'all') {
      state.region = p.scopeRegion;
      const regSel = document.getElementById('region');
      if (regSel) regSel.value = p.scopeRegion;
    }
    updateHeaderAndRail();
    renderMainContent();
    showToast(`已切换身份为：${p.name} (${p.role})`);
  }

  function handleActionClick(target) {
    const action = target.getAttribute('data-action');
    if (!action) return;

    if (action === 'close-dialog') {
      closeDialog();
      return;
    }
    if (action === 'switch-nav') {
      state.activeNav = target.getAttribute('data-nav') || 'overview';
      updateHeaderAndRail();
      renderMainContent();
      return;
    }
    if (action === 'open-design-doc') { openDesignSpecDialog(); return; }
    if (action === 'open-identity-dialog') {
      openIdentityDialog();
      return;
    }
    if (action === 'switch-current-time') {
      state.timeRange = 'current';
      const tr = document.getElementById('time-range');
      if (tr) tr.value = 'current';
      renderMainContent();
      showToast('已切回当前实时状态视角');
      return;
    }
    if (action === 'refresh-stale-data') {
      state.scenario = 'normal';
      state.lastSyncUtc = '2026-11-10 09:30:45 UTC';
      const sc = document.getElementById('scenario');
      if (sc) sc.value = 'normal';
      updateHeaderAndRail();
      renderMainContent();
      showToast('已强制同步最新遥测快照 (09:30:45 UTC)');
      return;
    }
    if (action === 'recover-offline-api') {
      state.scenario = 'normal';
      const sc = document.getElementById('scenario');
      if (sc) sc.value = 'normal';
      updateHeaderAndRail();
      renderMainContent();
      showToast('控制面 BFF 接口连接已恢复');
      return;
    }
    if (action === 'open-capacity-mitigation') {
      openCapacityMitigationDialog();
      return;
    }
    if (action === 'open-quota-dialog') {
      const tId = target.getAttribute('data-tenant') || 'Northstar';
      openQuotaDialog(tId);
      return;
    }
    if (action === 'resolve-cas-conflict') {
      const ver = Number(target.getAttribute('data-ver') || 15);
      const verInp = document.getElementById('dlg-quota-version');
      const errEl = document.getElementById('dialog-error');
      if (verInp) verInp.value = String(ver);
      if (errEl) errEl.textContent = '';
      showToast(`已加载最新基线版本 v${ver}，请再次点击提交完成合并`);
      return;
    }
    if (action === 'open-budget-policy-dialog') {
      openDialog(
        '未知账单保护策略配置 (FinOps Fallback)',
        `
          <div>当计费遥测流处于 <code>UNKNOWN</code> 状态时，平台采取以下保护措施：</div>
          <div class="form-field">
            <label for="dlg-finops-fallback">未知费用期间默认路由策略</label>
            <select id="dlg-finops-fallback">
              <option value="economy" selected>强制软降级至经济模型 (Qwen-3.8) 并保留 P0 额度</option>
              <option value="strict">暂停非 P0 任务的新增推理调用</option>
            </select>
          </div>
        `,
        '应用保护策略',
        () => {
          state.scenario = 'normal';
          const sc = document.getElementById('scenario');
          if (sc) sc.value = 'normal';
          closeDialog();
          updateHeaderAndRail();
          renderMainContent();
          showToast('已应用未知账单保护策略并恢复计费流同步');
        }
      );
      return;
    }
    if (action === 'go-nodes-partial') {
      state.activeNav = 'nodes';
      updateHeaderAndRail();
      renderMainContent();
      return;
    }
    if (action === 'run-batch-node-op') {
      const op = target.getAttribute('data-op');
      state.batchLastResult = { op, ts: Date.now() };
      if (op === 'drain') {
        state.scenario = 'partial';
        const sc = document.getElementById('scenario');
        if (sc) sc.value = 'partial';
        updateHeaderAndRail();
        renderMainContent();
        showToast('批量排水部分完成：2 成功，1 失败 (body-host-02 锁冲突)');
      } else {
        showToast('已向作用域内全部节点下发沙箱池预热指令');
      }
      return;
    }
    if (action === 'retry-failed-batch-node') {
      const n = state.nodes.find((x) => x.id === 'body-host-02');
      if (n) {
        n.partialFixed = true;
        n.status = 'CORDONED';
        n.activeLeases = 0;
      }
      state.batchLastResult = null;
      if (state.scenario === 'partial') {
        state.scenario = 'normal';
        const sc = document.getElementById('scenario');
        if (sc) sc.value = 'normal';
      }
      state.auditLogs.unshift({
        ts: '09:32:30 UTC',
        actor: `${getPrincipal().avatar} (${getPrincipal().role})`,
        action: '迁移 body-host-02 上的 P0 租约并重试完成节点隔离排水',
        scope: 'eu-central-1 / body-host-02',
        outcome: '全部成功'
      });
      updateHeaderAndRail();
      renderMainContent();
      showToast('body-host-02 租约迁移与重试排水已成功完成');
      return;
    }
    if (action === 'toggle-node-cordon') {
      const nodeId = target.getAttribute('data-node');
      const node = state.nodes.find((n) => n.id === nodeId);
      if (node) {
        node.status = node.status === 'CORDONED' ? 'READY' : 'CORDONED';
        renderMainContent();
        showToast(`节点 ${node.id} 状态已更新为 ${node.status}`);
      }
      return;
    }
    if (action === 'open-force-fence-dialog') {
      openForceFenceDialog(target.getAttribute('data-task') || 'task-ns-8841');
      return;
    }
    if (action === 'open-turn-history-dialog') {
      openTurnHistoryDialog(target.getAttribute('data-task') || 'task-ns-8841');
      return;
    }
    if (action === 'open-rebalance-verification') {
      openRebalanceVerificationDialog();
      return;
    }
    if (action === 'decide-approval') {
      const id = target.getAttribute('data-id');
      const dec = target.getAttribute('data-decision');
      const item = state.approvals.find((a) => a.id === id);
      const p = getPrincipal();
      if (item) {
        if (item.initiator === p.id) {
          showToast('双人复核拦截：发起人不能审批自己的申请单');
          return;
        }
        item.status = dec === 'approve' ? `APPROVED (${p.id})` : `REJECTED (${p.id})`;
        state.auditLogs.unshift({
          ts: '09:33:00 UTC',
          actor: `${p.avatar} (${p.role})`,
          action: `${dec === 'approve' ? '复核批准' : '驳回'}高危变更单 ${item.id} (${item.target})`,
          scope: `${item.region} / ${item.tenant}`,
          outcome: item.status
        });
        updateHeaderAndRail();
        renderMainContent();
        showToast(`审批单 ${item.id} 已${dec === 'approve' ? '批准生效' : '驳回'}`);
      }
    }
  }

  function bindGlobalEvents() {
    const regionSel = document.getElementById('region');
    if (regionSel) {
      regionSel.addEventListener('change', (e) => {
        state.region = e.target.value;
        renderMainContent();
        showToast(`区域筛选已切换为：${state.region}`);
      });
    }

    const tenantSel = document.getElementById('tenant-filter');
    if (tenantSel) {
      tenantSel.addEventListener('change', (e) => {
        state.tenant = e.target.value;
        renderMainContent();
        showToast(`租户筛选已切换为：${state.tenant}`);
      });
    }

    const timeSel = document.getElementById('time-range');
    if (timeSel) {
      timeSel.addEventListener('change', (e) => {
        state.timeRange = e.target.value;
        renderMainContent();
        showToast(`时间视角已切换为：${state.timeRange === 'history' ? '历史 09:00–09:30' : '当前状态'}`);
      });
    }

    const shareBtn = document.getElementById('share-view');
    if (shareBtn) {
      shareBtn.addEventListener('click', () => {
        const qs = `?nav=${state.activeNav}&region=${state.region}&tenant=${state.tenant}&time=${state.timeRange}&scenario=${state.scenario}&id=${state.identity}`;
        try {
          window.history.replaceState(null, '', qs);
        } catch (_e) {
          // ignore in sandboxed iframe
        }
        showToast(`已生成当前视角分享链接参数：${qs}`);
      });
    }

    const scenarioSel = document.getElementById('scenario');
    if (scenarioSel) {
      scenarioSel.addEventListener('change', (e) => {
        state.scenario = e.target.value;
        if (state.scenario === 'partial') state.activeNav = 'nodes';
        if (state.scenario === 'quota' || state.scenario === 'budget' || state.scenario === 'conflict') state.activeNav = 'tenants';
        if (state.scenario === 'cancel-unknown' || state.scenario === 'history-missing') state.activeNav = 'tasks';
        updateHeaderAndRail();
        renderMainContent();
        showToast(`已切换演示场景：${SCENARIO_LABELS[state.scenario]}`);
      });
    }

    const identitySel = document.getElementById('identity');
    if (identitySel) {
      identitySel.addEventListener('change', (e) => {
        applyIdentityChange(e.target.value);
      });
    }

    const resetBtn = document.getElementById('reset-demo');
    if (resetBtn) {
      resetBtn.addEventListener('click', () => {
        state = createInitialState();
        if (regionSel) regionSel.value = 'all';
        if (timeSel) timeSel.value = 'current';
        if (scenarioSel) scenarioSel.value = 'normal';
        if (identitySel) identitySel.value = 'chen';
        populateTenantFilter();
        updateHeaderAndRail();
        renderMainContent();
        showToast('演示数据与所有场景状态已重置为初始基准');
      });
    }

    const idOpenBtn = document.getElementById('identity-open');
    if (idOpenBtn) {
      idOpenBtn.addEventListener('click', () => {
        openIdentityDialog();
      });
    }

    const dialogForm = document.getElementById('dialog-form');
    if (dialogForm) {
      dialogForm.addEventListener('submit', (e) => {
        e.preventDefault();
        if (typeof currentDialogHandler === 'function') {
          currentDialogHandler();
        } else {
          closeDialog();
        }
      });
    }

    // Intercept footer design doc link so it works in-page as well as standalone
    document.addEventListener('click', (e) => {
      const link = e.target.closest('a[href="console-management-design.md"]');
      if (link) {
        e.preventDefault();
        openDesignSpecDialog();
        return;
      }
      const actionEl = e.target.closest('[data-action]');
      if (actionEl) {
        handleActionClick(actionEl);
      }
    });
  }

  function init() {
    populateTenantFilter();
    updateHeaderAndRail();
    renderMainContent();
    bindGlobalEvents();
    syncWithLiveBffIfAvailable();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
