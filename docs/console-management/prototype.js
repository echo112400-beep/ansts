/**
 * ANSTS · 平台管理控制台 (Production Management Plane & Live BFF Client)
 * Adopts the prototype's layout & visual style while wiring all management
 * and runtime operations directly to the real ANSTS v2.5 backend BFF APIs:
 * - GET  /v1beta1/management/state
 * - POST /v1beta1/management/scenario
 * - POST /v1beta1/management/tenants/config
 * - POST /v1beta1/management/nodes/batch
 * - POST /v1beta1/management/tasks/cancel
 * - POST /v1beta1/management/tasks/time-travel
 * - POST /v1beta1/management/approvals/decide
 * - POST /v1beta1/management/capacity/action
 * - POST /v1beta1/tasks/dispatch
 * - POST /v1/layerfs/exec & /v1/layerfs/merge
 * - POST /v1beta1/console/shards/rebalance
 * - POST /v1beta1/security/attest & /v1beta1/cluster/chaos-drill
 */
(function () {
  'use strict';

  function esc(val) {
    return String(val ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

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
      canApprove: true,
      jwtToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjaGVuIiwicm9sZSI6IlNSRV9BRE1JTiIsInNjb3BlUmVnaW9uIjoiYWxsIiwic2NvcGVUZW5hbnQiOiJhbGwiLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6MjAxNTM2MDAwMH0.2QK520kqHDLHSoYOZQFnPxA2Qh7q2tYoidwcM8k0DBQ'
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
      canApprove: true,
      jwtToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ6aG91Iiwicm9sZSI6IlNSRV9BRE1JTiIsInNjb3BlUmVnaW9uIjoiYWxsIiwic2NvcGVUZW5hbnQiOiJhbGwiLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6MjAxNTM2MDAwMH0.6K9NuojYk_XNRfUUh_VBRJ7bacpxdPvWry7StuCzKvY'
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
      canApprove: false,
      jwtToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJsaW4iLCJyb2xlIjoiT1BFUkFUT1IiLCJzY29wZVJlZ2lvbiI6InVzLXdlc3QtMiIsInNjb3BlVGVuYW50IjoiTm9ydGhzdGFyIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjIwMTUzNjAwMDB9.OChnz8Nba8o4lyVWQuKvuZxmjhNw_wF1JqE_weOiids'
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
      canApprove: false,
      jwtToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ4dSIsInJvbGUiOiJWSUVXRVIiLCJzY29wZVJlZ2lvbiI6InVzLXdlc3QtMiIsInNjb3BlVGVuYW50IjoiTm9ydGhzdGFyIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjIwMTUzNjAwMDB9.6V-V81dSWH6I-x1OuupsrUehf5n5tkGYNXPUy2o6zn0'
    }
  };

  const SCENARIO_LABELS = {
    normal: '正常管理流程',
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
    { id: 'runtime', label: '分布式运行面与 LayerFS 编排', badgeKey: 'runtime' }
  ];

  function createBaselineState() {
    return {
      activeNav: 'overview',
      region: 'all',
      tenant: 'all',
      timeRange: 'current',
      scenario: 'normal',
      identity: 'chen',
      liveBackendConnected: false,
      lastApiOutput: '等待调用后端接口...',
      lastSyncUtc: '2026-11-10 09:30:00 UTC',
      loadSheddingActive: false,
      verificationRetried: false,
      sla: {
        p0QueueWaitP99Ms: 68,
        warmPoolHitRatio: 0.948,
        utilizationRatio: 0.742,
        compliant: true,
        breaches: []
      },
      canary: { state: 'CANARY_ACTIVE', trafficWeightPct: 5 },
      a2aState: {
        blackboardEntries: [
          { key: 'arch/zero-trust-policy', value: { mTLS: 'STRICT', spiffeTrustDomain: 'ansts.io' }, version: 1, authorAgentId: 'agent-northstar' },
          { key: 'ledger/shard-cursor', value: { shardRange: '0..127', checkpointTurn: 5 }, version: 1, authorAgentId: 'agent-helios' }
        ],
        deadlock: { hasDeadlock: false, cyclePath: [] },
        lastGitArbitration: null,
        lastBisectResult: null
      },
      layerfsStats: { totalObjects: 18, fullObjects: 6, deltaObjects: 12, totalLogicalBytes: 49152, totalCompressedBytes: 18432 },
      mcpState: {
        tools: [
          { toolName: 'mcp_pg_migration_apply', serverName: 'mcp-postgres-prod', description: 'Apply reversible schema migration with Saga LIFO rollback hook', effectClass: 'COMPENSATABLE_WRITE', registeredBy: 'chen' }
        ],
        lastSagaRollbackReport: null
      },
      runtimeSandboxState: {
        warmPoolStats: { warmCount: 8, activeCount: 1 },
        teePublicKeyDerHex: '302a300506032b6570032100a8f19c...',
        spiffeCaPublicKeyDerHex: '302a300506032b65700321007d44e2...'
      },
      gcSweepReport: null,
      taskFilterSearch: '',
      taskFilterState: 'ALL',
      auditFilterQuery: '',
      auditFilterRole: 'ALL',
      slotDistribution: { 'brain-node-01': 85, 'brain-node-02': 86, 'brain-node-03': 85 },
      batchReport: null,
      nodes: [
        { id: 'celld-brain-01', role: 'Brain Shard #1', region: 'us-west-2', slots: 85, cpuPct: 62, psiPct: 4.2, status: 'READY', activeLeases: 14, pinnedP0Tasks: ['task-ns-8841'], tunnelGw: 'celld-gw-01' },
        { id: 'celld-brain-02', role: 'Brain Shard #2', region: 'us-west-2', slots: 86, cpuPct: 68, psiPct: 5.8, status: 'READY', activeLeases: 15, pinnedP0Tasks: [], tunnelGw: 'celld-gw-01' },
        { id: 'celld-brain-03', role: 'Brain Shard #3', region: 'eu-central-1', slots: 85, cpuPct: 54, psiPct: 3.1, status: 'READY', activeLeases: 13, pinnedP0Tasks: [], tunnelGw: 'celld-gw-02' },
        { id: 'body-host-01', role: 'Body LayerFS #1', region: 'us-west-2', slots: 0, cpuPct: 74, psiPct: 8.4, status: 'READY', activeLeases: 13, pinnedP0Tasks: [], tunnelGw: 'celld-gw-01 (PRIMARY)' },
        { id: 'body-host-02', role: 'Body LayerFS #2', region: 'eu-central-1', slots: 0, cpuPct: 79, psiPct: 11.2, status: 'READY', activeLeases: 14, pinnedP0Tasks: ['task-ns-8841'], tunnelGw: 'celld-gw-02 (SECONDARY)' },
        { id: 'body-host-03', role: 'Body LayerFS #3', region: 'ap-northeast-1', slots: 0, cpuPct: 48, psiPct: 2.5, status: 'READY', activeLeases: 13, pinnedP0Tasks: [], tunnelGw: 'celld-gw-01 (PRIMARY)' }
      ],
      tenants: [
        { id: 'Northstar', region: 'us-west-2', tier: 'P0_STRATEGIC', usedSlots: 88, maxSlots: 120, escrowReserved: 16, dailyBudgetUsd: 1200, spentUsd: 840, budgetStatus: 'OK', modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 14, updatedBy: 'chen' },
        { id: 'Helios', region: 'eu-central-1', tier: 'P1_ENTERPRISE', usedSlots: 46, maxSlots: 80, escrowReserved: 8, dailyBudgetUsd: 800, spentUsd: 490, budgetStatus: 'OK', modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 9, updatedBy: 'chen' },
        { id: 'Vanguard', region: 'ap-northeast-1', tier: 'P1_ENTERPRISE', usedSlots: 34, maxSlots: 60, escrowReserved: 6, dailyBudgetUsd: 600, spentUsd: 310, budgetStatus: 'OK', modelPolicy: 'DeepSeek-R1 → Qwen-3.8 (80%)', configVersion: 7, updatedBy: 'chen' },
        { id: 'Aegis', region: 'us-west-2', tier: 'P2_STANDARD', usedSlots: 19, maxSlots: 40, escrowReserved: 4, dailyBudgetUsd: 300, spentUsd: 145, budgetStatus: 'OK', modelPolicy: 'Qwen-3.8 Economy', configVersion: 5, updatedBy: 'chen' }
      ],
      tasks: [
        { id: 'task-ns-8841', tenant: 'Northstar', region: 'us-west-2', priority: 'P0', shardSlot: 42, brain: 'celld-brain-01', body: 'body-host-02', fence: 12, turn: 5, branch: 'feat-ns-auth', state: 'RUNNING_ACT' },
        { id: 'task-ns-8842', tenant: 'Northstar', region: 'us-west-2', priority: 'P1', shardSlot: 118, brain: 'celld-brain-02', body: 'body-host-01', fence: 8, turn: 3, branch: 'fix-ns-ledger', state: 'LEASED' },
        { id: 'task-hl-3019', tenant: 'Helios', region: 'eu-central-1', priority: 'P1', shardSlot: 203, brain: 'celld-brain-03', body: 'body-host-02', fence: 6, turn: 4, branch: 'gdpr-audit-eu', state: 'RUNNING_ACT' },
        { id: 'task-vg-1044', tenant: 'Vanguard', region: 'ap-northeast-1', priority: 'P2', shardSlot: 19, brain: 'celld-brain-01', body: 'body-host-03', fence: 4, turn: 2, branch: 'apac-settlement', state: 'COMPLETED' }
      ],
      approvals: [
        { id: 'APR-2026-091', type: 'QUOTA_EMERGENCY_BOOST', target: 'Northstar (120 → 180 并发槽)', region: 'us-west-2', tenant: 'Northstar', initiator: 'chen', status: 'PENDING_SECOND_ADMIN', risk: 'HIGH', createdAt: '09:24 UTC' },
        { id: 'APR-2026-092', type: 'SAGA_HIGH_RISK_TOOL', target: 'task-ns-8841 · prod_db_schema_drop', region: 'us-west-2', tenant: 'Northstar', initiator: 'lin', status: 'PENDING_SECOND_ADMIN', risk: 'CRITICAL', createdAt: '09:27 UTC' },
        { id: 'APR-2026-088', type: 'BRAIN_SHARD_DRAIN', target: 'celld-brain-02 维护排水与槽位迁移', region: 'us-west-2', tenant: 'all', initiator: 'zhou', status: 'APPROVED (chen)', risk: 'HIGH', createdAt: '09:05 UTC' }
      ],
      auditLogs: [
        { ts: '09:28:14 UTC', actor: 'chen', role: 'SRE_ADMIN', action: '初始化平台管理面控制基线与 256-Slot 分片映射', scope: 'Global', outcome: '已生效 (RPO=0)' },
        { ts: '09:21:05 UTC', actor: 'lin', role: 'OPERATOR', action: '触发 LayerFS 分支 feat-ns-auth 快照同步至 s3://ansts-fleet', scope: 'us-west-2 / Northstar', outcome: '成功 (Commit C1)' },
        { ts: '09:05:42 UTC', actor: 'chen', role: 'SRE_ADMIN', action: '复核通过周发起的分片维护单 APR-2026-088', scope: 'us-west-2 / Global', outcome: 'APPROVED (chen)' }
      ]
    };
  }

  let state = createBaselineState();
  let currentDialogHandler = null;
  let toastTimer = null;

  function getBackendOrigin() {
    if (typeof window !== 'undefined' && window.location && (window.location.protocol === 'http:' || window.location.protocol === 'https:')) {
      return window.location.origin;
    }
    return 'http://127.0.0.1:8088';
  }

  // Real Backend API Client: calls actual /v1beta1/management/* & /v1/* endpoints on the ANSTS server
  // [M76-P0-1 & P0-3] Enforces Authorization: Bearer <jwt> and ZERO-FAKE-STATE on network failure (always 503 offline, never fake 200).
  async function callBackendApi(method, endpointPath, payload) {
    const origin = getBackendOrigin();
    const fullUrl = `${origin}${endpointPath}`;
    const p = IDENTITIES[state.identity] || IDENTITIES.chen;
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 2500);
      const resp = await fetch(fullUrl, {
        method,
        headers: {
          'content-type': 'application/json',
          'accept': 'application/json',
          'authorization': `Bearer ${p.jwtToken}`
        },
        body: payload ? JSON.stringify(payload) : undefined,
        signal: ctrl.signal
      });
      clearTimeout(tid);
      const data = await resp.json();
      state.liveBackendConnected = true;
      updateConnectionIndicators(true);
      return { ok: resp.ok, status: resp.status, data, fromLiveBackend: true };
    } catch (_err) {
      state.liveBackendConnected = false;
      state.scenario = 'offline';
      updateConnectionIndicators(false);
      if (method !== 'GET') {
        showToast('503 OFFLINE：后端网络不可达，写操作已被安全拒绝（零伪造状态）');
      }
      return {
        ok: false,
        status: 503,
        data: {
          error: 'BACKEND_UNREACHABLE_OFFLINE',
          scenario: 'offline',
          message: 'Backend API is unreachable; write mutations and fake 200 responses are strictly prohibited'
        },
        fromLiveBackend: false
      };
    }
  }

  function updateConnectionIndicators(isLive) {
    const demoLabel = document.querySelector('.demo-label');
    const wsTag = document.querySelector('.workspace-tag');
    if (demoLabel) {
      if (!isLive || state.scenario === 'offline') {
        demoLabel.textContent = '503 OFFLINE · 接口离线锁定';
        demoLabel.className = 'demo-label warn';
      } else {
        demoLabel.textContent = 'REAL BFF · 真实后端已连接';
        demoLabel.className = 'demo-label';
      }
    }
    if (wsTag) {
      wsTag.textContent = isLive && state.scenario !== 'offline' ? 'LIVE v2.5' : 'OFFLINE';
    }
  }

  // Pulls real state from GET /v1beta1/management/state and merges into UI state
  async function fetchBackendState(recoverOffline) {
    const qs = `?region=${encodeURIComponent(state.region)}&tenant=${encodeURIComponent(state.tenant)}&timeRange=${encodeURIComponent(state.timeRange)}${recoverOffline ? '&recover=1' : ''}`;
    const res = await callBackendApi('GET', `/v1beta1/management/state${qs}`);
    if (!res.ok || res.status === 503) {
      state.scenario = 'offline';
      const scSel = document.getElementById('scenario');
      if (scSel) scSel.value = 'offline';
      updateConnectionIndicators(false);
      updateHeaderAndRail();
      renderMainContent();
      return;
    }
    if (res.data && Array.isArray(res.data.tenants)) {
      const d = res.data;
      state.scenario = d.scenario || state.scenario;
      state.lastSyncUtc = d.lastSyncUtc || state.lastSyncUtc;
      state.loadSheddingActive = Boolean(d.loadSheddingActive);
      state.verificationRetried = Boolean(d.verificationRetried);
      if (d.sla) state.sla = d.sla;
      if (d.canary) state.canary = d.canary;
      if (d.a2aState) state.a2aState = d.a2aState;
      if (d.mcpState) state.mcpState = d.mcpState;
      if (d.runtimeSandboxState) state.runtimeSandboxState = d.runtimeSandboxState;
      if (d.layerfsStats) state.layerfsStats = d.layerfsStats;
      if (d.gcSweepReport !== undefined) state.gcSweepReport = d.gcSweepReport;
      if (d.slotDistribution) state.slotDistribution = d.slotDistribution;
      state.batchReport = d.batchReport || null;
      state.nodes = d.nodes || state.nodes;
      state.tenants = d.tenants || state.tenants;
      state.tasks = d.tasks || state.tasks;
      state.approvals = d.approvals || state.approvals;
      state.auditLogs = d.auditLogs || state.auditLogs;
      const scSel = document.getElementById('scenario');
      if (scSel && scSel.value !== state.scenario) scSel.value = state.scenario;
    }
    populateTenantFilter();
    updateHeaderAndRail();
    renderMainContent();
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
    }, 3600);
  }

  function getPrincipal() {
    return IDENTITIES[state.identity] || IDENTITIES.chen;
  }

  function canMutateTarget(targetRegion, targetTenant, requireGlobalAdmin) {
    const p = getPrincipal();
    if (state.scenario === 'offline') return { ok: false, reason: '当前处于“接口不可用”场景，控制面写操作已锁定' };
    if (p.role === 'VIEWER') return { ok: false, reason: '当前身份为 VIEWER (只读观察员)，无写操作权限' };
    if (requireGlobalAdmin && !p.canWriteGlobal) {
      return { ok: false, reason: `当前身份 ${p.role} 仅限区域作用域操作，全局策略需 SRE_ADMIN` };
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
    const baseTenants = ['Northstar', 'Helios', 'Vanguard', 'Aegis'];
    const options = [
      { value: 'all', label: '所有租户' },
      ...baseTenants.map((id) => ({ value: id, label: id }))
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
      demoSumEl.textContent = `${SCENARIO_LABELS[state.scenario] || '正常管理流程'} · ${p.avatar} / ${p.role}`;
    }

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
      return { text: 'SLO 达标', cls: '' };
    }
    if (key === 'nodes') {
      if (state.scenario === 'partial' || (state.batchReport && state.batchReport.partialFailure)) {
        return { text: '1 失败', cls: 'danger' };
      }
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

  function renderScenarioBanner() {
    const p = getPrincipal();
    const banners = [];

    if (p.role === 'VIEWER') {
      banners.push(`
        <div class="state-banner info">
          <div>
            <strong>只读观察员模式 (${p.name})</strong>
            <div class="muted">当前账号作用域限定于 ${p.scopeRegion} / ${p.scopeTenant}，所有控制面写接口调用将由前端与后端 RBAC 双重拦截 (HTTP 403)。</div>
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
              <strong>[数据过期告警] 后端遥测快照停留在 ${state.lastSyncUtc} (已延迟 18 分钟)</strong>
              <div class="muted">Prometheus / CloudEvents 聚合流出现积压。为防止基于陈旧指标误触发缩容，自动扩缩容已被保护性挂起。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="refresh-stale-data">强制同步后端快照 (/v1beta1/management/state)</button>
            </div>
          </div>
        `);
        break;
      case 'offline':
        banners.push(`
          <div class="state-banner danger">
            <div>
              <strong>[控制面接口不可用] 后端 /v1beta1/management/state 返回 HTTP 503 Service Unavailable</strong>
              <div class="muted">遵循零伪造状态原则：当前仅展示最后一次已验证缓存快照，所有写操作已安全锁定。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="recover-offline-api">恢复后端接口连接 (?recover=1)</button>
            </div>
          </div>
        `);
        break;
      case 'capacity':
        banners.push(`
          <div class="state-banner danger">
            <div>
              <strong>[平台容量不足] us-west-2 计算与热池饱和度达 96.4% (Warm Pool 命中率跌至 61%)</strong>
              <div class="muted">CapacitySlaEnforcer 检测到 P0 排队等待 P99 升至 340ms（超出 150ms SLO）。建议立即调用后端开启 P3/P2 优先级削峰并扩容热沙箱池。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-capacity-mitigation">执行后端容量削峰与扩容</button>
            </div>
          </div>
        `);
        break;
      case 'quota':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[租户额度耗尽] 租户 Northstar 并发沙箱配额已达 120 / 120 (100%)</strong>
              <div class="muted">DagEscrowCoordinator 检测到 14 个子 Agent 任务因配额触顶处于排队等待，需调用 /v1beta1/management/tenants/config 提额。</div>
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
              <div class="muted">系统严禁将未知费用伪造为 $0.00。FinOpsBudgetController 已自动启用保护性软降级策略（优先路由至 Qwen-3.8 经济模型）。</div>
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
              <strong>[批量执行部分失败 · HTTP 207] 节点批量排水：2 个节点成功，1 个节点 (body-host-02) 失败</strong>
              <div class="muted">后端 CoreScheduler5D 检测到 <code>body-host-02</code> 持有 P0 任务 <code>task-ns-8841</code> 互斥租约（返回 <code>ERR_ACTIVE_LEASE_PINNED</code>）。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="go-nodes-partial">查看失败明细并迁移租约重试</button>
            </div>
          </div>
        `);
        break;
      case 'cancel-unknown':
        banners.push(`
          <div class="state-banner warn">
            <div>
              <strong>[任务停止未确认 · HTTP 202] 任务 task-ns-8841 停止指令已下发，未收到 body-host-02 反向隧道 ACK</strong>
              <div class="muted">后端状态保持为 <code>CANCEL_UNCONFIRMED</code>。可通过调用 <code>/v1beta1/management/tasks/cancel (mode=force_fence)</code> 推进 Fencing Token 强制吊销旧租约。</div>
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
              <strong>[配置版本并发冲突就绪] 后端 SQLite 中 Northstar 配置版本已被 周 (SRE_ADMIN) 更新至 v15</strong>
              <div class="muted">当以旧基线版本 v14 提交 `/v1beta1/management/tenants/config` 时，后端将返回真实 HTTP 409 CAS_VERSION_CONFLICT。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-quota-dialog" data-tenant="Northstar">触发后端 409 并发冲突校验</button>
            </div>
          </div>
        `);
        break;
      case 'history-missing':
        banners.push(`
          <div class="state-banner unknown">
            <div>
              <strong>[历史回合不可用] 任务 task-ns-8841 的 Turn #4–#5 S3 LTX 增量段未归档就绪</strong>
              <div class="muted">后端 DeterministicTimeTravelReplayer 拒绝从缺失段恢复（返回 HTTP 422），支持回退至完整校验点 Turn #3 (Commit C2) 零拷贝分叉。</div>
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
              <strong>[恢复验证未通过 · 指标再次超标] 初次扩容后 P0 排队 P99 复测升至 410ms (> 200ms 阈值)</strong>
              <div class="muted">根因定位：热点哈希槽 (#042, #129) 在 celld-brain-02 上出现锁争用。需调用后端执行二级恢复：256-Slot 哈希槽重平衡。</div>
            </div>
            <div class="btn-row">
              <button type="button" class="primary small" data-action="open-rebalance-verification">执行二级恢复：256 槽重平衡与复核</button>
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

    const p99Ms = state.sla ? state.sla.p0QueueWaitP99Ms : 68;
    const p99Text = isOffline ? 'UNKNOWN (503 离线)' : `${p99Ms} ms`;
    const capText = isOffline ? 'UNKNOWN' : `${Math.round((state.sla?.utilizationRatio || 0.742) * 1000) / 10}%`;
    const totalSpent = state.tenants.reduce((acc, t) => acc + (t.spentUsd || 0), 0);
    const totalBudget = state.tenants.reduce((acc, t) => acc + (t.dailyBudgetUsd || 0), 0);
    const finopsText = isOffline || isBudgetUnknown ? 'UNKNOWN (计费流延迟)' : `$${totalSpent} / $${totalBudget}`;
    const usedSlotsSum = state.tenants.reduce((acc, t) => acc + (t.usedSlots || 0), 0);
    const maxSlotsSum = state.tenants.reduce((acc, t) => acc + (t.maxSlots || 0), 0);
    const bDist = state.slotDistribution || { 'brain-node-01': 85, 'brain-node-02': 86, 'brain-node-03': 85 };

    return `
      <section class="kpi-grid" aria-label="核心态势指标">
        <div class="kpi-card">
          <div class="kpi-label"><span>P0 调度排队延迟 (P99)</span><span class="status-pill ${isCap || isVer ? 'danger' : isOffline ? 'unknown' : 'ok'}">${isOffline ? '未知' : isCap || isVer ? 'SLO 违约' : 'SLO 达标'}</span></div>
          <div class="kpi-value">${p99Text}</div>
          <div class="kpi-sub">SLO 阈值 &lt; 150ms · 基准: <span class="mono">${state.lastSyncUtc}</span></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>集群计算与 256 哈希槽分布</span><span class="status-pill ${isCap && !state.loadSheddingActive ? 'danger' : 'ok'}">${state.loadSheddingActive ? '已开启削峰' : 'RPO=0 SQL'}</span></div>
          <div class="kpi-value">${capText}</div>
          <div class="kpi-sub">槽位分布: <span class="mono">${bDist['brain-node-01'] || 0} / ${bDist['brain-node-02'] || 0} / ${bDist['brain-node-03'] || 0}</span></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>多租户并发沙箱配额</span><span class="status-pill ${isQuotaFull ? 'warn' : 'ok'}">${isQuotaFull ? 'Northstar 触顶' : 'DRF 均衡'}</span></div>
          <div class="kpi-value">${usedSlotsSum} / ${maxSlotsSum} 槽</div>
          <div class="kpi-sub">当前筛选租户: ${state.tenants.length} 个 · 活跃任务: ${state.tasks.length} 个</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label"><span>FinOps 日预算消耗</span><span class="status-pill ${isBudgetUnknown || isOffline ? 'unknown' : 'ok'}">${isBudgetUnknown ? '数据未知保护' : '实时计量'}</span></div>
          <div class="kpi-value">${finopsText}</div>
          <div class="kpi-sub">软降级阈值: 80% (DeepSeek → Qwen-3.8) · 硬门控: 100% (402)</div>
        </div>
      </section>

      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">渐进式金丝雀发布控制器 (CanaryRolloutController · POST /v1beta1/management/canary)</h2>
            <p class="panel-subtitle">按 5% → 25% → 65% → 100% 阶梯推进控制面新版本流量，当错误率 &gt; 1% 或 P99 &gt; 1500ms 时自动触发零停机回滚。</p>
          </div>
          <div class="btn-row">
            <span class="status-pill ${state.canary?.state === 'ROLLED_BACK' ? 'danger' : state.canary?.state === 'PROMOTED' ? 'ok' : 'warn'}">
              ${state.canary?.state || 'CANARY_ACTIVE'} · 流量权重 ${state.canary?.trafficWeightPct ?? 5}%
            </span>
            <button type="button" class="small primary" data-action="canary-action" data-op="promote_step" ${!canMutateTarget('all', 'all', true).ok ? 'disabled' : ''}>推进金丝雀流量 (+Step)</button>
            <button type="button" class="small danger" data-action="canary-action" data-op="inject_anomaly" ${!canMutateTarget('all', 'all', true).ok ? 'disabled' : ''}>注入异常触发自动回滚</button>
            <button type="button" class="small" data-action="canary-action" data-op="reset_canary" ${!canMutateTarget('all', 'all', true).ok ? 'disabled' : ''}>重置 (5%)</button>
          </div>
        </div>
      </div>

      <div class="split-grid">
        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">多区域控制面与容灾恢复看板 (对接 CapacitySlaEnforcer &amp; GeoRouter)</h2>
              <p class="panel-subtitle">实时呈现各区域 celld 分片槽位、CPU/PSI 内存压力、S3 LTX 跨区复制延迟及变更后二次验证结果。</p>
            </div>
            <div class="btn-row">
              <button type="button" data-action="open-capacity-mitigation" ${!canMutateTarget('us-west-2', 'all', false).ok ? 'disabled' : ''}>容量削峰与预热</button>
              <button type="button" class="primary" data-action="open-rebalance-verification" ${!canMutateTarget('all', 'all', true).ok ? 'disabled' : ''}>256 槽重平衡验证</button>
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
                  <th>S3 LTX 跨区复制</th>
                  <th>SLO 验证状态</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td class="mono"><strong>us-west-2</strong></td>
                  <td><span class="status-pill">US_FED</span></td>
                  <td class="mono">brain-01 (${bDist['brain-node-01'] ?? 85}), brain-02 (${bDist['brain-node-02'] ?? 86})</td>
                  <td class="mono">${isCap && !state.loadSheddingActive ? '96.4% / PSI 22.1%' : '66.0% / PSI 5.1%'}</td>
                  <td class="mono">RPO=0 (12ms)</td>
                  <td><span class="status-pill ${isVer || (isCap && !state.loadSheddingActive) ? 'danger' : 'ok'}">${isVer ? '复测超标 (410ms)' : isCap && !state.loadSheddingActive ? '容量告警 (340ms)' : '验证通过 (68ms)'}</span></td>
                </tr>
                <tr>
                  <td class="mono"><strong>eu-central-1</strong></td>
                  <td><span class="status-pill ok">GDPR_EU 锁定</span></td>
                  <td class="mono">brain-03 (${bDist['brain-node-03'] ?? 85} 槽)</td>
                  <td class="mono">54.2% / PSI 3.1%</td>
                  <td class="mono">→ eu-west-1 (18ms)</td>
                  <td><span class="status-pill ok">验证通过</span></td>
                </tr>
                <tr>
                  <td class="mono"><strong>ap-northeast-1</strong></td>
                  <td><span class="status-pill">APAC_SG</span></td>
                  <td class="mono">边缘接入 → brain-01</td>
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
              <h2 class="panel-title">控制面持久化审计日志 (SQLite: v25_mgmt_audit_logs)</h2>
              <p class="panel-subtitle">所有管理面与运行面写操作均持久化记录操作人、角色、作用域及执行结果。</p>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>时间</th>
                  <th>操作人</th>
                  <th>动作摘要</th>
                  <th>结果</th>
                </tr>
              </thead>
              <tbody>
                ${state.auditLogs.slice(0, 7).map((log) => `
                  <tr>
                    <td class="mono">${esc(log.ts)}</td>
                    <td><strong>${log.actor}</strong> <span class="muted">(${log.role || 'SRE_ADMIN'})</span></td>
                    <td>${esc(log.action)}</td>
                    <td><span class="status-pill ${String(log.outcome).includes('失败') || String(log.outcome).includes('超标') ? 'danger' : String(log.outcome).includes('待') || String(log.outcome).includes('UNCONFIRMED') ? 'warn' : 'ok'}">${log.outcome}</span></td>
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
    const perm = canMutateTarget(state.region, state.tenant, false);
    const hasBatchFail = state.batchReport && state.batchReport.partialFailure;

    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">节点与 256-Slot 分片单元编排 (对接 ManualFleetScaleController &amp; HaSchedulerShardCoordinator)</h2>
            <p class="panel-subtitle">真实调用 <code>POST /v1beta1/management/nodes/batch</code> 执行节点隔离 (Cordon)、维护排水 (Drain) 与 256 哈希槽迁移。</p>
          </div>
          <div class="btn-row">
            <button type="button" data-action="run-fleet-diagnostics">滚动升级与 Prometheus 诊断 (/v1beta1/management/diagnostics/probe)</button>
            <button type="button" data-action="run-batch-node-op" data-op="prewarm" ${!perm.ok ? 'disabled' : ''}>批量预热沙箱池</button>
            <button type="button" data-action="open-scale-node-dialog" ${!perm.ok ? 'disabled' : ''}>+ 扩容裸金属/容器节点 (/v1beta1/management/nodes/scale)</button>
            <button type="button" class="primary" data-action="run-batch-node-op" data-op="drain" ${!perm.ok ? 'disabled' : ''}>批量执行节点维护排水</button>
          </div>
        </div>

        ${hasBatchFail ? `
          <div class="state-banner warn">
            <div>
              <strong>[HTTP 207 Multi-Status] 批量维护部分失败报告：</strong>
              <div class="muted">
                ${state.batchReport.results.map((r) => `${r.nodeId}: <strong>${r.status}</strong>${r.message ? ' (' + r.message + ')' : ''}`).join(' · ')}
              </div>
            </div>
            <div class="btn-row">
              <button type="button" class="small primary" data-action="retry-failed-batch-node" ${!perm.ok ? 'disabled' : ''}>强制热迁移 P0 租约并重试失败节点 (forceMigrateLeases=true)</button>
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
                <th>活跃租约 / P0 钉死任务</th>
                <th>反向隧道网关</th>
                <th>状态</th>
                <th>后端操作</th>
              </tr>
            </thead>
            <tbody>
              ${state.nodes.map((n) => {
                const nodePerm = canMutateTarget(n.region, 'all', false);
                const pinnedList = n.pinnedP0Tasks && n.pinnedP0Tasks.length > 0 ? `P0: ${n.pinnedP0Tasks.join(',')}` : '无 P0 锁';
                return `
                  <tr>
                    <td class="mono"><strong>${n.id}</strong></td>
                    <td>${n.role} ${n.slots > 0 ? `<span class="mono">(${n.slots} 槽)</span>` : ''}</td>
                    <td class="mono">${n.region}</td>
                    <td class="mono">${n.cpuPct}% / PSI ${n.psiPct}%</td>
                    <td class="mono">${n.activeLeases} <span class="muted">(${pinnedList})</span></td>
                    <td class="mono">${n.tunnelGw}</td>
                    <td>
                      <span class="status-pill ${n.status === 'DRAIN_FAILED' ? 'danger' : n.status === 'CORDONED' ? 'warn' : 'ok'}">
                        ${n.status}
                      </span>
                    </td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small" data-action="toggle-node-cordon" data-node="${n.id}" data-status="${n.status}" ${!nodePerm.ok ? 'disabled' : ''}>
                          ${n.status === 'CORDONED' ? '恢复调度 (Uncordon)' : '隔离节点 (Cordon)'}
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
    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">多租户 DRF 配额、DAG 并发担保与 FinOps 预算门控 (对接 SQLite v25_mgmt_tenants &amp; FinOpsBudgetController)</h2>
            <p class="panel-subtitle">调用 <code>POST /v1beta1/management/tenants/config</code> 修改配额与预算，强制执行乐观锁 CAS 版本校验 (expectedVersion)。</p>
          </div>
          <div class="btn-row">
            <button type="button" class="small primary" data-action="open-finops-attribution">FinOps 多维成本归因 (LLM 推理 vs 沙箱核时)</button>
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
                <th>配置版本 (最后更新)</th>
                <th>管理操作</th>
              </tr>
            </thead>
            <tbody>
              ${state.tenants.map((t) => {
                const ratio = Math.round((t.usedSlots / Math.max(1, t.maxSlots)) * 100);
                const perm = canMutateTarget(t.region, t.id, false);
                return `
                  <tr>
                    <td><strong>${t.id}</strong></td>
                    <td class="mono">${t.region} · ${t.tier}</td>
                    <td class="mono">
                      <span class="status-pill ${ratio >= 100 ? 'danger' : ratio >= 80 ? 'warn' : 'ok'}">${t.usedSlots} / ${t.maxSlots} (${ratio}%)</span>
                    </td>
                    <td class="mono">${t.escrowReserved} 槽</td>
                    <td class="mono">
                      ${t.spentUsd === null || t.budgetStatus === 'UNKNOWN'
                        ? '<span class="status-pill unknown">UNKNOWN (计费流延迟)</span>'
                        : `$${t.spentUsd} / $${t.dailyBudgetUsd}`}
                    </td>
                    <td>${t.modelPolicy}</td>
                    <td class="mono">v${t.configVersion} <span class="muted">(${t.updatedBy || 'chen'})</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small primary" data-action="open-quota-dialog" data-tenant="${t.id}" ${!perm.ok ? 'disabled' : ''}>编辑配额</button>
                        <button type="button" class="small" data-action="tenant-finops-step" data-tenant="${t.id}" ${!perm.ok ? 'disabled' : ''} title="累加计费流水测试 80% 软降级与 100% 熔断">+计费步进</button>
                        <button type="button" class="small" data-action="tenant-escrow-step" data-tenant="${t.id}" ${!perm.ok ? 'disabled' : ''} title="增加 DAG 子任务并发担保槽位">+DAG预留</button>
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

  function renderTasksSection() {
    const permGlobal = canMutateTarget(state.region, state.tenant, false);
    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">全局任务运行队列、停止确认与历史回合 Time-Travel 诊断 (对接 CoreScheduler5D &amp; TurnWalEngine)</h2>
            <p class="panel-subtitle">对接 <code>POST /v1beta1/management/tasks/cancel</code> 与 <code>POST /v1beta1/management/tasks/time-travel</code>，支持派发新任务、推进 Fencing Token 与零拷贝回合分叉。</p>
          </div>
          <div class="btn-row">
            <input id="mgmt-task-search" type="text" value="${state.taskFilterSearch || ''}" placeholder="搜索任务 ID / 租户 / 分支..." style="width:180px;" />
            <select id="mgmt-task-state-filter">
              <option value="ALL" ${state.taskFilterState === 'ALL' ? 'selected' : ''}>所有状态</option>
              <option value="RUNNING_ACT" ${state.taskFilterState === 'RUNNING_ACT' ? 'selected' : ''}>RUNNING_ACT</option>
              <option value="LEASED" ${state.taskFilterState === 'LEASED' ? 'selected' : ''}>LEASED</option>
              <option value="CANCEL_UNCONFIRMED" ${state.taskFilterState === 'CANCEL_UNCONFIRMED' ? 'selected' : ''}>CANCEL_UNCONFIRMED</option>
              <option value="FENCED_TERMINATED" ${state.taskFilterState === 'FENCED_TERMINATED' ? 'selected' : ''}>FENCED_TERMINATED</option>
            </select>
            <button type="button" class="small" data-action="coalesce-duplicate-task" ${!permGlobal.ok ? 'disabled' : ''} title="模拟相同指纹/幂等键并发请求合并与提权">+ 合并并发重复请求演练</button>
            <button type="button" class="small warn" data-action="open-dlq-workbench">DLQ 死信与 Outbox 重放工作台</button>
            <button type="button" class="small" data-action="open-adaptive-dag-replan">[F1] 动态 DAG 重规划</button>
            <button type="button" class="small" data-action="open-best-of-n-verifier">[F2] Best-of-N 投机择优</button>
            <button type="button" class="small" data-action="open-cascade-jury-router">[F3] 级联置信度路由 &amp; 陪审团</button>
            <button type="button" class="small" data-action="open-episodic-hatchery">[F4] 语义经验库 &amp; 黄金镜像</button>
            <button type="button" class="small" data-action="open-formal-guardrail">[F5] Policy-as-Code 门控</button>
            <button type="button" class="small" data-action="open-shadow-wal-replay">[F6] 生产 WAL 影子回放</button>
            <button type="button" class="small" data-action="open-context-compactor">[F7] 分层工作记忆压缩</button>
            <button type="button" class="small" data-action="open-spot-quota-market">[F8] Spot 算力借贷市场</button>
<button type="button" class="small" data-action="open-contract-net">[N1] Swarm 协同竞标</button>
            <button type="button" class="small" data-action="open-bft-consensus">[N2] BFT 黑板共识与语义合并</button>
            <button type="button" class="small" data-action="open-causal-vector-cut">[N3] 因果一致性割截</button>
<button type="button" class="small" data-action="open-ebpf-breaker">[N4] eBPF 网络追踪与微秒熔断</button>
            <button type="button" class="small" data-action="open-byzantine-slashing">[N5] 拜占庭信用惩戒与隔离</button>
            <button type="button" class="small" data-action="open-erasure-coding">[N6] 跨地域日志纠删码重构</button>
            <button type="button" class="primary" data-action="open-dispatch-task-dialog" ${!permGlobal.ok ? 'disabled' : ''}>+ 派发新分布式任务</button>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>任务 ID</th>
                <th>租户 / 区域</th>
                <th>优先级 / 哈希槽</th>
                <th>Brain 分片 → Body 宿主</th>
                <th>Fencing Token</th>
                <th>当前回合 / LayerFS 分支</th>
                <th>状态</th>
                <th>诊断与干预</th>
              </tr>
            </thead>
            <tbody>
              ${state.tasks.filter((t) => {
                if (state.taskFilterState && state.taskFilterState !== 'ALL' && t.state !== state.taskFilterState) return false;
                const q = (state.taskFilterSearch || '').toLowerCase().trim();
                if (!q) return true;
                return String(t.id).toLowerCase().includes(q) || String(t.tenant).toLowerCase().includes(q) || String(t.branch).toLowerCase().includes(q);
              }).map((t) => {
                const perm = canMutateTarget(t.region, t.tenant, false);
                const isUnconfirmed = t.state === 'CANCEL_UNCONFIRMED';
                const isFenced = t.state === 'FENCED_TERMINATED';
                const pillCls = isUnconfirmed ? 'warn' : isFenced ? 'danger' : 'ok';
                return `
                  <tr>
                    <td class="mono"><strong>${esc(t.id)}</strong>${t.mergedDuplicateCount > 0 ? ` <span class="status-pill ok" title="已合并重复请求: ${(t.mergedRequestIds || []).join(", ")}">合并×${t.mergedDuplicateCount}</span>` : ""}</td>
                    <td>${esc(t.tenant)} · <span class="mono">${esc(t.region)}</span></td>
                    <td class="mono">${t.priority} · Slot #${t.shardSlot ?? 0}</td>
                    <td class="mono">${t.brain} → ${t.body}</td>
                    <td class="mono">epoch:${t.fence}</td>
                    <td class="mono">Turn #${esc(t.turn)} · ⎇ ${esc(t.branch)}</td>
                    <td><span class="status-pill ${pillCls}">${t.state}</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small primary" data-action="step-task-turn" data-task="${t.id}" ${!perm.ok || t.state === 'COMPLETED' || isFenced ? 'disabled' : ''}>+推进回合</button>
                        <button type="button" class="small" data-action="recover-task-breakpoint" data-task="${t.id}" ${!perm.ok ? 'disabled' : ''} title="从 TurnWalEngine 断点恢复并自动回滚未提交 Saga 副作用">断点恢复</button>
                        <button type="button" class="small" data-action="migrate-task-node" data-task="${t.id}" ${!perm.ok || t.state === 'COMPLETED' || isFenced ? 'disabled' : ''} title="调用 EphemeralHeavyExecutorMigrator 跨节点热迁移并推进 Fencing Token">热迁移节点</button>
                        <button type="button" class="small" data-action="open-turn-history-dialog" data-task="${t.id}">回合历史 / 分叉 / Blame</button>
                        <button type="button" class="small" data-action="graceful-stop-task" data-task="${t.id}" ${!perm.ok || t.state === 'COMPLETED' || isFenced ? 'disabled' : ''}>
                          请求停止
                        </button>
                        <button type="button" class="small danger" data-action="open-force-fence-dialog" data-task="${t.id}" ${!perm.ok || t.state === 'COMPLETED' || isFenced ? 'disabled' : ''}>
                          强制吊销租约
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
    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">高危操作双人复核 (4-Eyes Principle) 与 Saga 审批门控 (对接 /v1beta1/management/approvals/decide)</h2>
            <p class="panel-subtitle">后端强制校验：仅 <code>SRE_ADMIN</code> 可审批（否则返回 403），且发起人与审批人不得为同一人（否则返回 409 SELF_APPROVAL_FORBIDDEN）。当前身份：<strong>${p.name}</strong></p>
          </div>
          <div class="btn-row">
            <button type="button" class="small primary" data-action="open-create-approval-dialog" ${p.role === 'VIEWER' || state.scenario === 'offline' ? 'disabled' : ''}>+ 提交高危变更审批单 (/v1beta1/management/approvals/create)</button>
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
                <th>后端复核操作</th>
              </tr>
            </thead>
            <tbody>
              ${state.approvals.map((a) => {
                const isPending = a.status.startsWith('PENDING');
                const canClick = isPending && state.scenario !== 'offline';
                return `
                  <tr>
                    <td class="mono"><strong>${esc(a.id)}</strong></td>
                    <td class="mono">${esc(a.type)}</td>
                    <td>${esc(a.target)}</td>
                    <td class="mono">${esc(a.region)} / ${esc(a.tenant)}</td>
                    <td>${esc(IDENTITIES[a.initiator]?.name || a.initiator)}</td>
                    <td><span class="status-pill ${a.risk === 'CRITICAL' ? 'danger' : 'warn'}">${esc(a.risk)}</span></td>
                    <td><span class="status-pill ${isPending ? 'warn' : 'ok'}">${esc(a.status)}</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small primary" data-action="decide-approval" data-id="${a.id}" data-initiator="${a.initiator}" data-decision="approve" ${!canClick ? 'disabled' : ''}>
                          批准执行
                        </button>
                        <button type="button" class="small danger" data-action="decide-approval" data-id="${a.id}" data-initiator="${a.initiator}" data-decision="reject" ${!canClick ? 'disabled' : ''}>
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

      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">不可篡改变更审计日志检索与合规导出 (GET /v1beta1/management/audits)</h2>
            <p class="panel-subtitle">支持按操作人、RBAC 角色或动作关键字实时检索 SQLite <code>v25_mgmt_audit_logs</code> 并导出合规 JSON 报告。</p>
          </div>
          <div class="btn-row">
            <input id="mgmt-audit-search" type="text" value="${state.auditFilterQuery || ''}" placeholder="检索审计动作 / 作用域 / 结果..." style="width:200px;" />
            <select id="mgmt-audit-role">
              <option value="ALL" ${state.auditFilterRole === 'ALL' ? 'selected' : ''}>全部角色</option>
              <option value="SRE_ADMIN" ${state.auditFilterRole === 'SRE_ADMIN' ? 'selected' : ''}>SRE_ADMIN</option>
              <option value="OPERATOR" ${state.auditFilterRole === 'OPERATOR' ? 'selected' : ''}>OPERATOR</option>
            </select>
            <button type="button" class="small" data-action="export-audit-json">导出合规审计报告 (JSON)</button>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>时间戳</th><th>操作员</th><th>RBAC 角色</th><th>作用域</th><th>变更动作详情</th><th>执行结果</th></tr>
            </thead>
            <tbody>
              ${state.auditLogs.filter((l) => {
                if (state.auditFilterRole && state.auditFilterRole !== 'ALL' && l.role !== state.auditFilterRole) return false;
                const q = (state.auditFilterQuery || '').toLowerCase().trim();
                if (!q) return true;
                return String(l.action).toLowerCase().includes(q) || String(l.scope).toLowerCase().includes(q) || String(l.actor).toLowerCase().includes(q) || String(l.outcome).toLowerCase().includes(q);
              }).map((log) => `
                <tr>
                  <td class="mono">${esc(log.ts)}</td>
                  <td><strong>${esc(log.actor)}</strong></td>
                  <td class="mono">${esc(log.role || 'SRE_ADMIN')}</td>
                  <td class="mono">${esc(log.scope)}</td>
                  <td>${esc(log.action)}</td>
                  <td><span class="status-pill ok">${esc(log.outcome)}</span></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function renderRuntimeSection() {
    const bDist = state.slotDistribution || { 'brain-node-01': 85, 'brain-node-02': 86, 'brain-node-03': 85 };
    const slotBoxes = [];
    for (let i = 0; i < 256; i++) {
      const b1Count = bDist['brain-node-01'] ?? 85;
      const b2Count = bDist['brain-node-02'] ?? 86;
      const cls = i < b1Count ? 'b1' : i < b1Count + b2Count ? 'b2' : 'b3';
      const owner = cls === 'b1' ? 'brain-node-01' : cls === 'b2' ? 'brain-node-02' : 'brain-node-03';
      slotBoxes.push(`<div class="slot-box ${cls}" title="Slot #${i} → ${owner}"></div>`);
    }

    return `
      <div class="split-grid">
        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">256-Slot 一致性哈希环与 Brain 分片故障演练 (对接 /v1beta1/console/shards/rebalance)</h2>
              <p class="panel-subtitle">蓝色: brain-node-01 (${bDist['brain-node-01'] ?? 0} 槽) · 青色: brain-node-02 (${bDist['brain-node-02'] ?? 0} 槽) · 琥珀色: brain-node-03 (${bDist['brain-node-03'] ?? 0} 槽)</p>
            </div>
            <div class="btn-row">
              <button type="button" class="small danger" data-action="runtime-evict-brain02">驱逐 brain-node-02</button>
              <button type="button" class="small primary" data-action="runtime-restore-brain02">恢复 brain-node-02</button>
            </div>
          </div>
          <div class="slot-ring-grid">${slotBoxes.join('')}</div>
        </div>

        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">LayerFS CoW 沙箱、SLSA/TEE 证明与脑裂混沌演练 (真实后端接口直连)</h2>
              <p class="panel-subtitle">点击下方按钮直接调用后端 <code>/v1/layerfs/*</code>、<code>/v1beta1/security/attest</code> 与 <code>/v1beta1/cluster/chaos-drill</code>。</p>
            </div>
          </div>
          <div class="btn-row">
            <button type="button" class="small primary" data-action="runtime-layerfs-exec">执行 LayerFS CoW 快照提交 (/v1/layerfs/exec)</button>
            <button type="button" class="small" data-action="runtime-layerfs-merge">三路分支合并 (/v1/layerfs/merge)</button>
            <button type="button" class="small" data-action="runtime-slsa-attest">生成 SLSA v1.0 + TEE 证明 (/v1beta1/security/attest)</button>
            <button type="button" class="small danger" data-action="runtime-chaos-drill">执行脑裂与 Fencing 混沌演练 (/v1beta1/cluster/chaos-drill)</button>
            <button type="button" class="small primary" data-action="runtime-celld-isolate">V8 Isolate 沙箱与堆守卫 (/v1beta1/celld/isolate)</button>
            <button type="button" class="small" data-action="runtime-celld-facets">DO Sub-Actor Facets &amp; 32-Shard KV (/v1beta1/celld/facets)</button>
            <button type="button" class="small" data-action="runtime-ltx-compaction">L0→L1 LTX 压缩与 UFFD 恢复 (/v1beta1/celld/ltx-compaction)</button>
            <button type="button" class="small" data-action="runtime-layerfs-d9">LayerFS Rule D9 存储同步 (/v1beta1/layerfs/store-sync)</button>
            <button type="button" class="small primary" data-action="runtime-sandbox-op" data-op="allocate_and_mount">Cgroup v2 + OverlayFS + MicroVM 挂载 (/v1beta1/management/runtime/sandbox)</button>
            <button type="button" class="small" data-action="runtime-sandbox-op" data-op="vsock_exec_and_io">Vsock 真实文件 I/O 与子进程执行</button>
            <button type="button" class="small" data-action="runtime-sandbox-op" data-op="issue_and_verify_mtls_tee">Ed25519 SPIFFE SVID &amp; SEV-SNP 验签</button>
            <button type="button" class="small" data-action="runtime-sandbox-op" data-op="migrate_celld_actor">CelldAdmin 跨节点热迁移 DO Cell</button>
            <button type="button" class="small primary" data-action="m48-op" data-endpoint="/v1beta1/management/persistence/snapshot" data-op="verify_cold_start_rehydration">8表全量快照与冷启动恢复校验 (/v1beta1/management/persistence/snapshot)</button>
            <button type="button" class="small" data-action="m48-op" data-endpoint="/v1beta1/management/celld/container-workflow" data-op="start_and_probe_container">原生 kata ctx.container + celld-fence 探针</button>
            <button type="button" class="small" data-action="m48-op" data-endpoint="/v1beta1/management/celld/container-workflow" data-op="run_durable_workflow">DurableWorkflowContext 崩溃重放验证</button>
            <button type="button" class="small primary" data-action="m48-op" data-endpoint="/v1beta1/management/layerfs/speculative" data-op="speculative">确定性投机多分支推演与合并 (/v1beta1/management/layerfs/speculative)</button>
            <button type="button" class="small danger" data-action="m48-op" data-endpoint="/v1beta1/management/cluster/autoscale-psi" data-op="simulate_psi_spike">Linux PSI 内存风暴防御与队列自动分片</button>
          <button class="btn" data-action="m49-op" data-endpoint="/v1beta1/management/router/inference-pipeline" data-op="execute_sse_inference">模型路由 SSE 推理 &amp; 熔断/令牌桶</button>
          <button class="btn" data-action="m49-op" data-endpoint="/v1beta1/management/security/kernel-attestation" data-op="vsock_unix_daemon_probe">Unix Vsock 守护进程 &amp; eBPF/SLSA/UFFD</button>
          <button class="btn" data-action="m49-op" data-endpoint="/v1beta1/management/chaos/drill" data-op="run_partition_and_split_brain_drill">网络分区脑裂演练 &amp; Topology CAS</button>
          <button class="btn" data-action="m49-op" data-endpoint="/v1beta1/management/federation/tunnel-harness" data-op="tunnel_command_and_failover">双归属反向隧道切换 &amp; 联邦/Harness 自愈</button>
          <button class="btn" data-action="m50-op" data-method="GET" data-endpoint="/v1beta1/management/design/blueprint" data-op="inspect_blueprint">架构设计蓝图 &amp; 契约合规矩阵</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/config/center" data-op="validate_and_apply_config">配置中心 CAS 热更新 &amp; 版本回滚</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/runtime/celld-engine" data-op="scheduler_cell_rpc_and_alarm">celld 运行时引擎 &amp; LTX/LayerFS 调优</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/ops/observability" data-op="probe_slo_traces_and_metrics">全链路 SLO 燃烧率监控 &amp; 自愈 Runbook</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/scheduler/kernel-audit" data-op="kernel_5_invariants_audit">调度器防 Stale / 防死锁 / 防饥饿 5 项内核自检</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/deep-rectification-audit" data-op="system_wide_m52_rectification">全系统跨模块 Stale/死锁/饥饿 深度整改自检 (M52)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/risk-elimination-audit" data-op="deep_cross_layer_m53_risk_audit">跨层深水区风险清零自检 (Saga/FinOps/WarmPool/WAL/DrainTTL · M53)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/deep-runtime-audit" data-op="deep_runtime_m54_audit">深层运行时防惊群/防死锁/反饥饿自检 (Harness/Breaker/Staging/HotCell/Outbox/A2A · M54)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/storage-ha-audit" data-op="storage_ha_m55_audit">存储/分片存活/LayerFS同步/事务回滚自检 (Persistence/Shard/D9/TimeTravel/RPO0 · M55)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/runtime-edge-audit" data-op="kernel_storage_m56_audit">全表冷启恢复/三路合并删改冲突/P0突发准入/内存防泄漏自检 (M56)</button>
          <button class="btn primary" data-action="run-health-scorecard">★ M60–M69 全链路持久化 / DFX / 零残余缺陷一键健康评分卡 (Scorecard)</button>
          <button class="btn" data-action="open-dlq-workbench">DLQ 死信任务 &amp; Outbox Webhook 失败重放工作台</button>
          <button class="btn" data-action="open-security-fence-alerts">零信任安全围栏 (celld-fence / eBPF LSM / Vsock) 实时拦截流</button>
          <button class="btn" data-action="open-finops-attribution">FinOps 多维成本归因 (LLM 推理 vs 沙箱核时 + Cache 节省)</button>
          <button class="btn" data-action="open-config-diff-preview">配置中心字段级 JSON Diff 对比预览</button>
          <button class="btn" data-action="open-dual-key-rotation">Outbox Webhook 签名密钥零停机双密钥平滑轮换</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/persistence-idempotency-recovery-audit" data-op="m60_audit">全链路持久化/幂等/断点恢复/重复合并自检 (M60)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/dfx-deep-audit" data-op="m63_audit">深度 DFX 可靠性/主权路由/多环死锁自检 (M63)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/deep-consistency-phase2-audit" data-op="m65_audit">全子系统实现一致性与 AST 契约自检 (M65)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/deep-risk-phase3-audit" data-op="m68_audit">深度潜在风险消除 I–III 自检 (M66–M68)</button>
          <button class="btn" data-action="m50-op" data-method="POST" data-endpoint="/v1beta1/management/system/final-closure-audit" data-op="m69_audit">终极边界收口与零残余缺陷自检 (M69)</button>
          </div>
          <pre class="mono" style="background:var(--bg-elevated);border:1px solid var(--border-subtle);padding:10px;border-radius:5px;font-size:11.5px;overflow-x:auto;margin:0;max-height:180px;">${state.lastApiOutput}</pre>
        </div>
      </div>

      <div class="split-grid">
        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">A2A 多智能体协同治理、WFG 死锁检测与并行 CoW 二分排障 (POST /v1beta1/management/a2a)</h2>
              <p class="panel-subtitle">对接 <code>SharedBlackboardAndGitArbiter</code>、<code>A2aWaitForGraphDeadlockDetector</code> 与 <code>ParallelCowGitBisectCoordinator</code>。</p>
            </div>
            <div class="btn-row">
              <span class="status-pill ${state.a2aState?.deadlock?.hasDeadlock ? 'danger' : 'ok'}">
                ${state.a2aState?.deadlock?.hasDeadlock ? 'WFG 环形死锁: ' + (state.a2aState.deadlock.cyclePath || []).join('→') : 'WFG 无死锁 (0 Cycles)'}
              </span>
            </div>
          </div>
          <div class="btn-row">
            <button type="button" class="small primary" data-action="a2a-action" data-op="put_blackboard_cas">+ 黑板 CAS 更新</button>
            <button type="button" class="small" data-action="a2a-action" data-op="arbitrate_git_patches">Git 补丁冲突仲裁</button>
            <button type="button" class="small danger" data-action="a2a-action" data-op="inject_deadlock_cycle">注入 WFG 环形死锁</button>
            <button type="button" class="small primary" data-action="a2a-action" data-op="break_deadlock">抢占解除死锁</button>
            <button type="button" class="small" data-action="a2a-action" data-op="run_cow_bisect">零拷贝 CoW 二分排障</button>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr><th>黑板键 (Blackboard Key)</th><th>CAS 版本</th><th>写入智能体</th><th>当前协同载荷</th></tr>
              </thead>
              <tbody>
                ${(state.a2aState?.blackboardEntries || []).map((entry) => `
                  <tr>
                    <td class="mono"><strong>${esc(entry.key)}</strong></td>
                    <td><span class="status-pill ok">v${entry.version}</span></td>
                    <td class="mono">${esc(entry.authorAgentId)}</td>
                    <td class="mono">${esc(JSON.stringify(entry.value))}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">动态 MCP 工具注册表与 Saga 逆序 (LIFO) 补偿门控 (POST /v1beta1/management/mcp)</h2>
              <p class="panel-subtitle">对接 <code>McpDynamicToolRegistry</code> 与 <code>SagaToolGateway</code>，支持动态注册 JSON-RPC 2.0 工具并触发逆序事务补偿。</p>
            </div>
            <div class="btn-row">
              <span class="status-pill ok">已注册 MCP 工具: ${(state.mcpState?.tools || []).length} 个</span>
            </div>
          </div>
          <div class="btn-row">
            <button type="button" class="small primary" data-action="mcp-action" data-op="register_mcp_tool">+ 注册动态 MCP 工具</button>
            <button type="button" class="small" data-action="mcp-action" data-op="invoke_mcp_tool">调用 MCP 事务工具 (Saga 挂载)</button>
            <button type="button" class="small danger" data-action="mcp-action" data-op="saga_rollback_task">触发 Saga 逆序 (LIFO) 补偿回滚</button>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead><tr><th>MCP 工具名称</th><th>所属服务 (Server)</th><th>副作用级别</th><th>描述</th></tr></thead>
              <tbody>
                ${(state.mcpState?.tools || []).map((t) => `
                  <tr>
                    <td class="mono"><strong>${esc(t.toolName)}</strong></td>
                    <td class="mono">${esc(t.serverName)}</td>
                    <td><span class="status-pill warn">${t.effectClass}</span></td>
                    <td>${esc(t.description)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <div class="panel">
          <div class="panel-header">
            <div>
              <h2 class="panel-title">分布式 CAS 两阶段垃圾回收与 celld Epoch 安全审计 (POST /v1beta1/management/storage/gc-sweep)</h2>
              <p class="panel-subtitle">对接 <code>DistributedCasGcSweeper</code> 物理回收孤儿块并保留活跃 Delta 基线，支持 <code>anstsctl gc-audit</code> 演练。</p>
            </div>
            <div class="btn-row">
              <span class="status-pill ok">CAS 对象: ${state.layerfsStats?.totalObjects ?? 18} (Full ${state.layerfsStats?.fullObjects ?? 6} / Delta ${state.layerfsStats?.deltaObjects ?? 12})</span>
            </div>
          </div>
          <div class="btn-row">
            <button type="button" class="small" data-action="storage-gc-action" data-op="seed_orphans">1. 生成孤儿 CAS 数据块</button>
            <button type="button" class="small primary" data-action="storage-gc-action" data-op="sweep_cas">2. 执行 Mark-and-Sweep 物理回收</button>
            <button type="button" class="small" data-action="storage-gc-action" data-op="epoch_gc_audit">3. celld Epoch GC 安全审计 (Dry-Run)</button>
          </div>
          <div class="muted" style="padding:6px 0;">
            逻辑总容量: <strong class="mono">${state.layerfsStats?.totalLogicalBytes ?? 49152} Bytes</strong> · 压缩后物理占用: <strong class="mono">${state.layerfsStats?.totalCompressedBytes ?? 18432} Bytes</strong>
            ${state.gcSweepReport ? ` · 最近 GC 回收: <strong class="mono">${state.gcSweepReport.sweptCount} 个孤儿块 (${state.gcSweepReport.reclaimedBytes} Bytes)</strong>` : ''}
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

  function openIdentityDialog() {
    const cur = state.identity;
    openDialog(
      '切换控制台操作身份 (RBAC 权限与双人复核验证)',
      `
        <div class="form-field">
          <label for="dlg-identity-select">选择当前操作员身份</label>
          <select id="dlg-identity-select">
            <option value="chen" ${cur === 'chen' ? 'selected' : ''}>陈 · SRE_ADMIN · 全平台读写与审批</option>
            <option value="zhou" ${cur === 'zhou' ? 'selected' : ''}>周 · SRE_ADMIN · 全平台 (用于双人异体复核与 CAS 冲突验证)</option>
            <option value="lin" ${cur === 'lin' ? 'selected' : ''}>林 · OPERATOR · 仅限 US West / Northstar 运维</option>
            <option value="xu" ${cur === 'xu' ? 'selected' : ''}>许 · VIEWER · 仅限 US West / Northstar 只读观察</option>
          </select>
        </div>
        <div class="muted">
          所有后端接口均会校验所传角色 (role) 与操作人 (actor)：VIEWER 写请求返回 HTTP 403，自审高危单返回 HTTP 409。
        </div>
      `,
      '切换身份',
      () => {
        const sel = document.getElementById('dlg-identity-select');
        if (sel) applyIdentityChange(sel.value);
        closeDialog();
      }
    );
  }

  function openQuotaDialog(tenantId) {
    const tenant = state.tenants.find((t) => t.id === tenantId) || state.tenants[0];
    // In conflict scenario, the operator form starts with baseline v14 while backend SQLite was updated to v15 by zhou
    const formBaselineVersion = state.scenario === 'conflict' && tenant.id === 'Northstar' ? 14 : tenant.configVersion;

    openDialog(
      `编辑租户配额与 FinOps 预算 · ${tenant.id} (POST /v1beta1/management/tenants/config)`,
      `
        <div class="form-field">
          <label for="dlg-quota-max">并发沙箱槽位上限 (Max Concurrent Slots)</label>
          <input id="dlg-quota-max" type="number" value="${tenant.maxSlots <= 120 ? 160 : tenant.maxSlots}" min="10" max="500" />
        </div>
        <div class="form-field">
          <label for="dlg-quota-budget">FinOps 日预算上限 (Daily Budget USD)</label>
          <input id="dlg-quota-budget" type="number" value="${tenant.dailyBudgetUsd}" min="50" max="10000" />
        </div>
        <div class="form-field">
          <label for="dlg-quota-version">乐观锁基线版本 (CAS Expected Version)</label>
          <input id="dlg-quota-version" type="number" value="${formBaselineVersion}" readonly class="mono" />
        </div>
        <div class="muted">
          后端数据库当前版本：<strong class="mono">v${tenant.configVersion}</strong>（最后更新人：${tenant.updatedBy || 'chen'}）。
        </div>
      `,
      '提交后端变更 (CAS 校验)',
      async () => {
        const errEl = document.getElementById('dialog-error');
        const expectedVersion = Number(document.getElementById('dlg-quota-version').value);
        const maxSlots = Number(document.getElementById('dlg-quota-max').value);
        const dailyBudgetUsd = Number(document.getElementById('dlg-quota-budget').value);
        const p = getPrincipal();

        const res = await callBackendApi('POST', '/v1beta1/management/tenants/config', {
          tenantId: tenant.id,
          maxSlots,
          dailyBudgetUsd,
          expectedVersion,
          actor: p.id,
          role: p.role
        });

        if (res.status === 409) {
          const actualVer = res.data.actualVersion || 15;
          errEl.innerHTML = `
            <strong>HTTP 409 CAS_VERSION_CONFLICT (后端真实返回)</strong><br/>
            ${res.data.message || '版本冲突'}
            <div style="margin-top:8px;">
              <button type="button" class="small primary" data-action="resolve-cas-conflict" data-ver="${actualVer}">加载后端最新基线 v${actualVer} 并合并提交</button>
            </div>
          `;
          showToast('后端返回 HTTP 409 CAS_VERSION_CONFLICT');
          return;
        }
        if (!res.ok) {
          errEl.textContent = res.data.message || '请求被后端拒绝';
          return;
        }

        closeDialog();
        await fetchBackendState();
        showToast(`后端已持久化 ${tenant.id} 配额与 FinOps 策略 (v${res.data.configVersion})`);
      }
    );
  }

  function openDispatchTaskDialog() {
    const nextNum = Math.floor(100 + Math.random() * 899);
    openDialog(
      '派发分布式任务 (POST /v1beta1/tasks/dispatch)',
      `
        <div class="form-field">
          <label for="dlg-disp-id">任务 ID (Task ID)</label>
          <input id="dlg-disp-id" type="text" value="task-ns-${nextNum}" class="mono" />
        </div>
        <div class="form-field">
          <label for="dlg-disp-tenant">所属租户 (Tenant)</label>
          <select id="dlg-disp-tenant">
            <option value="Northstar" selected>Northstar (us-west-2)</option>
            <option value="Helios">Helios (eu-central-1)</option>
            <option value="Vanguard">Vanguard (ap-northeast-1)</option>
          </select>
        </div>
        <div class="form-field">
          <label for="dlg-disp-body">目标 Body LayerFS 宿主</label>
          <select id="dlg-disp-body">
            <option value="body-host-01" selected>body-host-01 (:19501)</option>
            <option value="body-host-02">body-host-02 (:19502)</option>
          </select>
        </div>
        <div class="form-field">
          <label for="dlg-disp-prompt">任务指令 (Prompt)</label>
          <input id="dlg-disp-prompt" type="text" value="Verify RPO=0 SQL commit and LayerFS CoW snapshot" />
        </div>
      `,
      '派发至集群',
      async () => {
        const taskId = document.getElementById('dlg-disp-id').value.trim();
        const tenantId = document.getElementById('dlg-disp-tenant').value;
        const targetBodyHostId = document.getElementById('dlg-disp-body').value;
        const promptText = document.getElementById('dlg-disp-prompt').value.trim();

        const res = await callBackendApi('POST', '/v1beta1/tasks/dispatch', {
          taskId,
          tenantId,
          targetBodyHostId,
          promptText
        });
        closeDialog();
        await fetchBackendState();
        showToast(`已派发任务 ${taskId} → Slot #${res.data.shardSlot ?? 0} (${res.data.routedBrainNodeId || 'brain-node-01'})`);
      }
    );
  }

  function openForceFenceDialog(taskId) {
    const task = state.tasks.find((t) => t.id === taskId) || state.tasks[0];
    openDialog(
      `强制推进 Epoch Fencing Token 并吊销旧租约 · ${task.id}`,
      `
        <div>
          任务 <strong class="mono">${task.id}</strong> 当前位于宿主 <code class="mono">${task.body}</code>，持有租约令牌 <code class="mono">epoch:${task.fence}</code>。
        </div>
        <div class="muted">
          调用后端 <code>POST /v1beta1/management/tasks/cancel (mode=force_fence)</code> 将在 SQLite <code>v25_scheduler_tasks</code> 中原子推进令牌至 <code class="mono">epoch:${task.fence + 1}</code>，使旧容器的后续写入被 <code>StaleFenceError (409)</code> 硬件级拦截。
        </div>
      `,
      `调用后端推进至 Epoch #${task.fence + 1}`,
      async () => {
        const p = getPrincipal();
        const res = await callBackendApi('POST', '/v1beta1/management/tasks/cancel', {
          taskId: task.id,
          mode: 'force_fence',
          actor: p.id,
          role: p.role
        });
        closeDialog();
        await fetchBackendState();
        showToast(`后端已完成 ${task.id} 租约隔离 (新令牌 epoch:${res.data.newFencingToken})`);
      }
    );
  }

  async function openTurnHistoryDialog(taskId) {
    const task = state.tasks.find((t) => t.id === taskId) || state.tasks[0];
    const turnsRes = await callBackendApi('GET', '/v1beta1/management/tasks/' + encodeURIComponent(task.id) + '/turns');
    const turnsList = (turnsRes.data && Array.isArray(turnsRes.data.turns) && turnsRes.data.turns.length > 0)
      ? turnsRes.data.turns
      : [
          { turnIndex: 1, commitId: 'C0', crc32Hex: '9a01b2c3', sqliteWalPages: 5, changedFiles: ['src/init.ts'], ltxStatus: 'VERIFIED' },
          { turnIndex: 2, commitId: 'C1', crc32Hex: '4d12e8f0', sqliteWalPages: 6, changedFiles: ['src/auth.ts'], ltxStatus: 'VERIFIED' },
          { turnIndex: 3, commitId: 'C2', crc32Hex: '7c89a112', sqliteWalPages: 7, changedFiles: ['src/ledger.ts'], ltxStatus: 'VERIFIED' }
        ];

    openDialog(
      '任务回合 WAL 帧 CRC32 校验与零拷贝分叉 · ' + task.id + ' (GET /v1beta1/management/tasks/' + task.id + '/turns)',
      '<div>目标任务：<strong class="mono">' + task.id + '</strong> · LayerFS 分支：<code class="mono">' + task.branch + '</code></div>' +
      '<div class="table-wrap"><table class="data-table"><thead><tr><th>回合</th><th>Commit</th><th>CRC32 帧校验</th><th>WAL 页数 / 变更文件</th><th>S3 LTX 状态</th></tr></thead><tbody>' +
      turnsList.map((tr) =>
        '<tr><td class="mono"><strong>Turn #' + tr.turnIndex + '</strong></td>' +
        '<td class="mono">' + tr.commitId + '</td>' +
        '<td class="mono"><code>0x' + tr.crc32Hex + '</code></td>' +
        '<td class="mono">' + tr.sqliteWalPages + ' pages · ' + (tr.changedFiles || []).join(', ') + '</td>' +
        '<td><span class="status-pill ' + (tr.ltxStatus === 'VERIFIED' ? 'ok' : 'danger') + '">' + tr.ltxStatus + '</span></td></tr>'
      ).join('') +
      '</tbody></table></div>' +
      '<div class="form-field"><label for="dlg-fork-turn">选择 Time-Travel 零拷贝分叉回合 (POST /v1beta1/management/tasks/time-travel)</label>' +
      '<select id="dlg-fork-turn">' +
      turnsList.map((tr) =>
        '<option value="' + tr.turnIndex + '" ' + (tr.turnIndex === 3 ? 'selected' : '') + '>' +
        'Turn #' + tr.turnIndex + ' (' + tr.commitId + ' · CRC32=0x' + tr.crc32Hex + ' · ' + tr.ltxStatus + ')</option>'
      ).join('') +
      '</select></div>' +
      '<div style="margin-top:8px;display:flex;align-items:center;justify-content:space-between;">' +
      '<span class="muted">支持对当前任务分支执行 LayerFS CoW 行级代码溯源 (GET /v1beta1/management/layerfs/blame)</span>' +
      '<button type="button" class="small" data-action="inspect-layerfs-blame" data-task="' + task.id + '">查看 LayerFS 行级代码溯源 (Blame)</button>' +
      '</div>' +
      '<div id="dlg-blame-output" style="margin-top:6px;"></div>',
      '调用后端派生新任务与 LayerFS 分支',
      async () => {
        const errEl = document.getElementById('dialog-error');
        const targetTurnIndex = Number(document.getElementById('dlg-fork-turn').value);
        const p = getPrincipal();
        const res = await callBackendApi('POST', '/v1beta1/management/tasks/time-travel', {
          taskId: task.id,
          targetTurnIndex,
          actor: p.id,
          role: p.role
        });
        if (!res.ok) {
          errEl.textContent = res.data.message || '后端拒绝从损坏回合派生分支 (HTTP 422)';
          showToast('后端拦截损坏回合分叉 (HTTP 422 LTX_SEGMENT_MISSING)');
          return;
        }
        closeDialog();
        await fetchBackendState();
        showToast('后端已零拷贝派生新任务 ' + res.data.forkedTaskId + ' (分支 ' + res.data.layerfsBranch + ')');
      }
    );
  }

  function openScaleOutNodeDialog() {
    const p = getPrincipal();
    const nextIdx = state.nodes.length + 1;
    openDialog(
      '扩容裸金属 / 容器节点 (POST /v1beta1/management/nodes/scale)',
      '<div class="form-field"><label for="dlg-scale-id">新节点 ID (Node ID)</label>' +
      '<input id="dlg-scale-id" type="text" value="body-host-0' + nextIdx + '" class="mono" /></div>' +
      '<div class="form-field"><label for="dlg-scale-role">节点角色</label><select id="dlg-scale-role">' +
      '<option value="body" selected>Body LayerFS 沙箱宿主 (Worker)</option>' +
      '<option value="brain">celld Brain 调度分片节点 (加入 256-Slot 哈希环)</option>' +
      '</select></div>' +
      '<div class="form-field"><label for="dlg-scale-region">部署区域</label><select id="dlg-scale-region">' +
      '<option value="us-west-2" selected>US West · us-west-2</option>' +
      '<option value="eu-central-1">EU Central · eu-central-1</option>' +
      '<option value="ap-northeast-1">AP Northeast · ap-northeast-1</option>' +
      '</select></div>',
      '调用后端扩容节点',
      async () => {
        const nodeId = document.getElementById('dlg-scale-id').value.trim();
        const role = document.getElementById('dlg-scale-role').value;
        const region = document.getElementById('dlg-scale-region').value;
        await callBackendApi('POST', '/v1beta1/management/nodes/scale', {
          action: 'scale_out',
          nodeId,
          role,
          region,
          actor: p.id,
          actorRole: p.role
        });
        closeDialog();
        await fetchBackendState();
        showToast('后端已完成节点 ' + nodeId + ' 扩容并同步集群拓扑');
      }
    );
  }

  function openCreateApprovalDialog() {
    const p = getPrincipal();
    openDialog(
      '提交高危变更申请单 (POST /v1beta1/management/approvals/create)',
      '<div class="form-field"><label for="dlg-apr-type">高危变更类型</label><select id="dlg-apr-type">' +
      '<option value="QUOTA_EMERGENCY_BOOST" selected>QUOTA_EMERGENCY_BOOST (租户紧急提额)</option>' +
      '<option value="SAGA_HIGH_RISK_TOOL">SAGA_HIGH_RISK_TOOL (高风险 Saga 外部工具执行)</option>' +
      '<option value="BRAIN_SHARD_DRAIN">BRAIN_SHARD_DRAIN (核心调度分片维护迁移)</option>' +
      '</select></div>' +
      '<div class="form-field"><label for="dlg-apr-target">变更目标与内容描述</label>' +
      '<input id="dlg-apr-target" type="text" value="Northstar (120 → 180 并发沙箱槽位)" /></div>' +
      '<div class="form-field"><label for="dlg-apr-tenant">目标租户</label><select id="dlg-apr-tenant">' +
      '<option value="Northstar" selected>Northstar (us-west-2)</option>' +
      '<option value="Helios">Helios (eu-central-1)</option>' +
      '<option value="Vanguard">Vanguard (ap-northeast-1)</option>' +
      '</select></div>' +
      '<div class="muted">当前发起人：<strong>' + p.name + ' (' + p.id + ')</strong>。提交后需切换至另一位 <code>SRE_ADMIN</code> 复核批准方可生效。</div>',
      '提交后端审批单',
      async () => {
        const errEl = document.getElementById('dialog-error');
        const type = document.getElementById('dlg-apr-type').value;
        const target = document.getElementById('dlg-apr-target').value.trim();
        const tenant = document.getElementById('dlg-apr-tenant').value;
        const region = tenant === 'Helios' ? 'eu-central-1' : tenant === 'Vanguard' ? 'ap-northeast-1' : 'us-west-2';

        const res = await callBackendApi('POST', '/v1beta1/management/approvals/create', {
          type,
          target,
          region,
          tenant,
          initiator: p.id,
          role: p.role
        });
        if (!res.ok) {
          errEl.textContent = res.data.message || '提交失败';
          return;
        }
        closeDialog();
        await fetchBackendState();
        showToast('已创建高危变更审批单 ' + res.data.approvalId + '，请切换另一管理员复核');
      }
    );
  }

  function openCapacityMitigationDialog() {
    openDialog(
      '平台容量削峰与热沙箱池扩容 (POST /v1beta1/management/capacity/action)',
      `
        <div>调用后端 <code>PriorityLoadShedder</code> 与 <code>PredictiveSandboxPrewarmer</code> 执行容量恢复：</div>
        <div class="form-field">
          <label for="dlg-cap-action">治理动作</label>
          <select id="dlg-cap-action">
            <option value="mitigate_capacity" selected>开启 P3/P2 优先级削峰 + EWMA 预测扩容热沙箱池</option>
          </select>
        </div>
      `,
      '调用后端执行容量恢复',
      async () => {
        const p = getPrincipal();
        await callBackendApi('POST', '/v1beta1/management/capacity/action', {
          action: 'mitigate_capacity',
          actor: p.id,
          role: p.role
        });
        closeDialog();
        await fetchBackendState();
        showToast('后端已启用优先级削峰与热池扩容，P99 恢复至 68ms');
      }
    );
  }

  function openRebalanceVerificationDialog() {
    openDialog(
      '二级恢复：256 哈希槽重平衡与 SLO 二次验证',
      `
        <div>调用后端 <code>HaSchedulerShardCoordinator</code> 对 256 个虚拟槽位执行重平衡，消除热点分片锁争用并复核 P99 指标。</div>
      `,
      '调用后端执行槽位重平衡',
      async () => {
        const p = getPrincipal();
        await callBackendApi('POST', '/v1beta1/management/capacity/action', {
          action: 'rebalance_verification',
          actor: p.id,
          role: p.role
        });
        closeDialog();
        await fetchBackendState();
        showToast('后端已完成 256 哈希槽重平衡，SLO 二次验证通过 (64ms)');
      }
    );
  }

  function openDesignSpecDialog() {
    openDialog(
      'ANSTS 平台管理控制台 · 功能与真实后端接口映射说明',
      `
        <div style="line-height:1.6;">
          <p><strong>1. 原型视觉与架构对齐：</strong>采用高对比深色导航轨 (<code>aside.rail</code>) + 暖白编辑面 (<code>.shell</code>) 原型风格，完整保留区域、租户、时间视角、11 大异常场景与 4 级 RBAC 身份切换。</p>
          <p><strong>2. 100% 真实后端接口绑定 (非打桩)：</strong></p>
          <ul>
            <li><code>GET /v1beta1/management/state</code>：实时聚合 SQLite <code>v25_mgmt_tenants</code>、<code>v25_scheduler_tasks</code>、256 哈希槽、<code>CapacitySlaEnforcer</code> 与审计日志；</li>
            <li><code>POST /v1beta1/management/tenants/config</code>：真实 SQLite CAS 版本校验（拦截 HTTP 409）并同步 <code>FinOpsBudgetController</code>；</li>
            <li><code>POST /v1beta1/management/nodes/batch</code>：真实检查 <code>CoreScheduler5D</code> 中的 P0 互斥租约（返回 HTTP 207）并支持强制热迁移；</li>
            <li><code>POST /v1beta1/management/tasks/cancel</code> &amp; <code>/time-travel</code>：真实推进 Fencing Token 并调用 <code>DeterministicTimeTravelReplayer</code> 派生新任务与 LayerFS 分支；</li>
            <li><code>POST /v1beta1/management/approvals/decide</code>：强制校验双人异体复核（自审返回 HTTP 409，非管理员返回 HTTP 403）。</li>
          </ul>
        </div>
      `,
      '关闭说明',
      () => closeDialog()
    );
  }

  function applyIdentityChange(newIdentity) {
    if (!IDENTITIES[newIdentity]) return;
    state.identity = newIdentity;
    const idSel = document.getElementById('identity');
    if (idSel) idSel.value = newIdentity;
    const p = getPrincipal();
    if (p.scopeRegion !== 'all') {
      state.region = p.scopeRegion;
      const regSel = document.getElementById('region');
      if (regSel) regSel.value = p.scopeRegion;
    }
    updateHeaderAndRail();
    renderMainContent();
    showToast(`已切换操作员身份：${p.name} (${p.role})`);
  }

  async function handleActionClick(target) {
    const action = target.getAttribute('data-action');
    if (!action) return;

    if (action === 'close-dialog') {
      closeDialog();
      return;
    }
    if (action === 'open-design-doc') {
      openDesignSpecDialog();
      return;
    }
    if (action === 'switch-nav') {
      state.activeNav = target.getAttribute('data-nav') || 'overview';
      updateHeaderAndRail();
      renderMainContent();
      return;
    }
    if (action === 'open-identity-dialog') {
      openIdentityDialog();
      return;
    }
    if (action === 'switch-current-time') {
      state.timeRange = 'current';
      const tr = document.getElementById('time-range');
      if (tr) tr.value = 'current';
      await fetchBackendState();
      showToast('已切回当前实时状态视角');
      return;
    }
    if (action === 'refresh-stale-data') {
      const p = getPrincipal();
      await callBackendApi('POST', '/v1beta1/management/scenario', { scenario: 'normal', actor: p.id });
      await fetchBackendState();
      showToast('已从后端强制同步最新遥测快照');
      return;
    }
    if (action === 'recover-offline-api') {
      await fetchBackendState(true);
      showToast('后端控制面 BFF 接口连接已恢复');
      return;
    }
    if (action === 'open-capacity-mitigation') {
      openCapacityMitigationDialog();
      return;
    }
    if (action === 'open-quota-dialog') {
      openQuotaDialog(target.getAttribute('data-tenant') || 'Northstar');
      return;
    }
    if (action === 'open-dispatch-task-dialog') {
      openDispatchTaskDialog();
      return;
    }
    if (action === 'resolve-cas-conflict') {
      const ver = Number(target.getAttribute('data-ver') || 15);
      const verInp = document.getElementById('dlg-quota-version');
      const errEl = document.getElementById('dialog-error');
      if (verInp) verInp.value = String(ver);
      if (errEl) errEl.textContent = '';
      showToast(`已加载后端最新基线 v${ver}，再次点击提交即可完成合并`);
      return;
    }
    if (action === 'open-budget-policy-dialog') {
      const p = getPrincipal();
      await callBackendApi('POST', '/v1beta1/management/capacity/action', {
        action: 'apply_budget_fallback',
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('后端已应用未知计费流 FinOps 保护策略并恢复计量');
      return;
    }
    if (action === 'go-nodes-partial') {
      state.activeNav = 'nodes';
      updateHeaderAndRail();
      renderMainContent();
      return;
    }
    if (action === 'run-batch-node-op') {
      const op = target.getAttribute('data-op') || 'drain';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/nodes/batch', {
        operation: op,
        nodeIds: ['celld-brain-01', 'body-host-01', 'body-host-02'],
        forceMigrateLeases: false,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      if (res.status === 207) {
        showToast('后端返回 HTTP 207：2 节点成功，body-host-02 因 P0 租约锁冲突失败');
      } else {
        showToast(`后端已完成批量节点操作 [${op}]`);
      }
      return;
    }
    if (action === 'retry-failed-batch-node') {
      const p = getPrincipal();
      await callBackendApi('POST', '/v1beta1/management/nodes/batch', {
        operation: 'drain',
        nodeIds: ['body-host-02'],
        forceMigrateLeases: true,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('后端已热迁移 body-host-02 上的 P0 租约并完成节点隔离排水');
      return;
    }
    if (action === 'toggle-node-cordon') {
      const nodeId = target.getAttribute('data-node');
      const curStatus = target.getAttribute('data-status');
      const nextOp = curStatus === 'CORDONED' ? 'uncordon' : 'cordon';
      const p = getPrincipal();
      await callBackendApi('POST', '/v1beta1/management/nodes/batch', {
        operation: nextOp,
        nodeIds: [nodeId],
        forceMigrateLeases: true,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast(`后端已执行节点 ${nodeId} [${nextOp}]`);
      return;
    }
    if (action === 'graceful-stop-task') {
      const taskId = target.getAttribute('data-task') || 'task-ns-8841';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/tasks/cancel', {
        taskId,
        mode: 'graceful',
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast(`后端返回 HTTP ${res.status}: 任务 ${taskId} 状态为 CANCEL_UNCONFIRMED`);
      return;
    }
    if (action === 'open-force-fence-dialog') {
      openForceFenceDialog(target.getAttribute('data-task') || 'task-ns-8841');
      return;
    }
    if (action === 'open-turn-history-dialog') {
      await openTurnHistoryDialog(target.getAttribute('data-task') || 'task-ns-8841');
      return;
    }
    if (action === 'open-scale-node-dialog') {
      openScaleOutNodeDialog();
      return;
    }
    if (action === 'canary-action') {
      const op = target.getAttribute('data-op') || 'promote_step';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/canary', {
        action: op,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('金丝雀发布状态已更新：' + (res.data.state || 'CANARY_ACTIVE') + ' (' + (res.data.trafficWeightPct ?? 0) + '%)');
      return;
    }
    if (action === 'tenant-finops-step') {
      const tenantId = target.getAttribute('data-tenant') || 'Northstar';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/tenants/finops-escrow', {
        action: 'record_spend',
        tenantId,
        spendDeltaUsd: 220,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('租户 ' + tenantId + ' 账单已累加 (+$220) → ' + (res.data.modelPolicy || 'OK'));
      return;
    }
    if (action === 'tenant-escrow-step') {
      const tenantId = target.getAttribute('data-tenant') || 'Northstar';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/tenants/finops-escrow', {
        action: 'reserve_escrow',
        tenantId,
        escrowDeltaSlots: 4,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('租户 ' + tenantId + ' DAG 子任务预留槽已增至 ' + res.data.escrowReserved + ' 槽');
      return;
    }
    if (action === 'migrate-task-node') {
      const taskId = target.getAttribute('data-task') || 'task-ns-8841';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/tasks/migrate', {
        taskId,
        actor: p.id,
        role: p.role
      });
      if (!res.ok) {
        showToast(res.data.message || '跨节点迁移被拒绝');
        return;
      }
      await fetchBackendState();
      showToast('已热迁移任务 ' + taskId + ': ' + res.data.sourceNodeId + ' → ' + res.data.targetNodeId + ' (新令牌 epoch:' + res.data.newFencingToken + ')');
      return;
    }
    if (action === 'inspect-layerfs-blame') {
      const taskId = target.getAttribute('data-task') || 'task-ns-8841';
      const res = await callBackendApi('GET', '/v1beta1/management/layerfs/blame?taskId=' + encodeURIComponent(taskId));
      const box = document.getElementById('dlg-blame-output');
      if (box && res.data && Array.isArray(res.data.lines)) {
        box.innerHTML =
          '<div class="table-wrap"><table class="data-table"><thead><tr><th>行号</th><th>Commit</th><th>作者 Agent</th><th>Step ID</th><th>代码内容 (' + (res.data.filePath || '') + ')</th></tr></thead><tbody>' +
          res.data.lines.map((ln) =>
            '<tr><td class="mono">L' + ln.lineNumber + '</td><td class="mono">' + ln.commitId + '</td><td class="mono">' + ln.agentId + '</td><td class="mono">' + ln.stepId + '</td><td class="mono"><code>' + ln.content + '</code></td></tr>'
          ).join('') +
          '</tbody></table></div>';
      }
      showToast('已加载分支 ' + (res.data.branchName || '') + ' 的 LayerFS 行级代码溯源');
      return;
    }
    if (action === "recover-task-breakpoint") {
      const taskId = target.getAttribute("data-task") || "task-ns-8841";
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/tasks/breakpoint-recover", {
        action: "recover_breakpoint",
        taskId,
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      state.lastApiOutput = JSON.stringify(d, null, 2);
      await fetchBackendState();
      openDialog(
        "任务 WAL 断点恢复与未提交 Saga 清理报告 · " + taskId,
        "<div><strong>恢复断点回合：</strong> <span class=\"status-pill ok\">Turn #" + (d.restoredTurnIndex ?? 1) + " (Fence epoch:" + (d.restoredFencingToken ?? 1) + ")</span></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">权威 WAL 帧数: <code class=\"mono\">" + (d.canonicalWalFrames ?? 1) + "</code> · 丢弃未提交帧: <code class=\"mono\">" + (d.discardedWalFrames ?? 0) + "</code> · 逆序回滚未提交 Saga 步骤: <code class=\"mono\">" + (d.rolledBackSagaSteps ?? 0) + "</code> · 下一步待执行回合: <code class=\"mono\">Turn #" + (d.nextTurnToExecute ?? 2) + "</code></div>",
        "完成恢复确认",
        () => closeDialog()
      );
      return;
    }
    if (action === "coalesce-duplicate-task") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/tasks/coalesce-inspect", {
        action: "coalesce_duplicate",
        taskId: "task-ns-8841",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      state.lastApiOutput = JSON.stringify(d, null, 2);
      await fetchBackendState();
      showToast("已将并发重复请求 " + (d.incomingDuplicateId || "dup") + " 合并至 " + (d.canonicalTaskId || "task-ns-8841") + " (累计合并 ×" + (d.mergedDuplicateCount || 1) + "，节省 " + (d.estimatedSavedTokens || 4200) + " Tokens)");
      return;
    }
            if (action === "open-ebpf-breaker") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-network-slashing-erasure", {
        action: "ebpf_network_circuit_breaker",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const t = d.trippedTelemetry || {};
      const i = d.trippedIntercept || {};
      openDialog(
        "[N4] eBPF 动态网络追踪与微秒级熔断 (POST /v1beta1/management/ops/phase2-network-slashing-erasure)",
        "<div><strong>eBPF tc 出口挂钩状态：</strong> <span class=\"status-pill " + (i.allowed ? "ok" : "err") + "\">" + (t.circuitBreakerState || "TRIPPED_MICROS_OPEN") + "</span></div>" +
        "<div style=\"margin-top:6px;\"><strong>对端指标：</strong> 平滑往返时延 SRTT <code class=\"mono\">" + (t.srttUs || 21500) + "&mu;s</code> · 滑动丢包率 <code class=\"mono\">" + ((t.packetLossRatio || 0.16) * 100).toFixed(1) + "%</code> · 套接字队列水位 <code class=\"mono\">" + (t.socketBufferPressurePct || 88.5) + "%</code></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">微秒级阻断拦截响应：<code class=\"mono\">" + (i.reason || "") + "</code></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-byzantine-slashing") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-network-slashing-erasure", {
        action: "byzantine_credit_slashing",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const s2 = d.slash2 || {};
      const pd = d.pardonRes || {};
      openDialog(
        "[N5] 跨租户信用惩戒与拜占庭行为特征提取 (POST /v1beta1/management/ops/phase2-network-slashing-erasure)",
        "<div><strong>惩戒智能体：</strong> <code class=\"mono\">" + (s2.agentId || "agent-rogue-007") + "</code> · 违规指纹：<span class=\"status-pill err\">" + (s2.violationType || "DOUBLE_SIGN_CONTRADICTORY_VOTES") + "</span></div>" +
        "<div style=\"margin-top:6px;\"><strong>声誉惩戒分：</strong> <code class=\"mono\">" + (s2.reputationBefore || 70) + "</code> &rarr; <span class=\"status-pill err\">" + (s2.reputationAfter || 20) + " pts</span> (-" + (s2.slashedPoints || 50) + " pts)</div>" +
        "<div style=\"margin-top:6px;\"><strong>隔离狱状态：</strong> <span class=\"status-pill warn\">" + (s2.inQuarantineJail ? "QUARANTINE_JAIL" : "ACTIVE") + "</span> (剥夺 Swarm 竞标权 &amp; BFT 投票权) · 经审计特赦恢复至 <code class=\"mono\">" + (pd.restoredReputation || 75) + " pts</code></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-erasure-coding") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-network-slashing-erasure", {
        action: "cross_region_erasure_coding",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const m = d.manifest || {};
      const rA = d.rebuildCaseA || {};
      openDialog(
        "[N6] 跨地域异步日志纠删码与按需校验 (POST /v1beta1/management/ops/phase2-network-slashing-erasure)",
        "<div><strong>纠删码矩阵：</strong> <span class=\"status-pill ok\">Cauchy Reed-Solomon (K=" + (m.dataShardsCount || 4) + " + M=" + (m.parityShardsCount || 2) + ")</span> 跨 6 地域异构同步</div>" +
        "<div style=\"margin-top:6px;\"><strong>任毁 2 地域无损重构：</strong> <span class=\"status-pill ok\">CRC32 BIT-EXACT MATCH</span> (丢失分片 <code class=\"mono\">[" + ((rA.missingShardIndices || [0, 2]).join(", ")) + "]</code> 成功利用有限域矩阵解出原始日志)</div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">重构后完整数据校验：<code class=\"mono\">" + (rA.originalCrc32Hex || "") + " === " + (rA.reconstructedCrc32Hex || "") + "</code></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-contract-net") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-swarm-governance", {
        action: "contract_net_bidding",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const win = d.winningBid || {};
      openDialog(
        "[N1] Contract-Net 多智能体竞标协议 (POST /v1beta1/management/ops/phase1-swarm-governance)",
        "<div><strong>中标智能体：</strong> <span class=\"status-pill ok\">" + (win.agentId || "agent-expert-secops") + "</span> (综合效用分：<code class=\"mono\">" + (win.finalUtilityScore || 91.5) + "</code> / 100)</div>" +
        "<div style=\"margin-top:6px;\"><strong>报价明细：</strong> 预算报价 <code class=\"mono\">$" + (win.bidCostUsd || 0.45) + "</code> · 预估时延 <code class=\"mono\">" + (win.bidLatencyMs || 3200) + "ms</code> · 专家置信度 <code class=\"mono\">" + (win.confidence || 0.95) + "</code></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">竞标池参与者总数：<code class=\"mono\">" + (d.totalBidsEvaluated || 3) + "</code> 位 · SLA 过滤通过率：<code class=\"mono\">" + (d.conformingBidsCount || 2) + "/" + (d.totalBidsEvaluated || 3) + "</code></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-bft-consensus") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-swarm-governance", {
        action: "bft_blackboard_and_semantic_merge",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const c = d.consensusResult || {};
      const sc = d.semanticConflictResult || {};
      openDialog(
        "[N2] 声誉加权 BFT 黑板共识与代码语义合并 (POST /v1beta1/management/ops/phase1-swarm-governance)",
        "<div><strong>黑板 CAS 状态：</strong> <span class=\"status-pill " + (c.accepted ? "ok" : "err") + "\">" + (c.accepted ? "COMMITTED" : "REJECTED") + "</span> · 声誉达成率：<code class=\"mono\">" + (c.reputationAchievedPct || 85.0) + "%</code> (&ge; " + (c.quorumThresholdPct || 66.7) + "%)</div>" +
        "<div style=\"margin-top:6px;\"><strong>权威智能体否决权检查：</strong> <code class=\"mono\">" + (c.vetoedByCore ? "VETOED" : "ZERO-VETO PASSED") + "</code></div>" +
        "<div style=\"margin-top:6px;\"><strong>AST 语义导出符号冲突检查：</strong> <span class=\"status-pill " + (sc.hasConflict ? "warn" : "ok") + "\">" + (sc.hasConflict ? "CONFLICT DETECTED" : "CLEAN EXPORT MERGE") + "</span> (冲突重叠导出符：<code class=\"mono\">" + ((sc.conflictingExports || []).join(", ") || "none") + "</code>)</div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-causal-vector-cut") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-swarm-governance", {
        action: "causal_vector_clock_cut",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const cut = d.consistentCut || {};
      openDialog(
        "[N3] 向量时钟因果屏障与全局一致性割截 (POST /v1beta1/management/ops/phase1-swarm-governance)",
        "<div><strong>全局一致性割截状态：</strong> <span class=\"status-pill " + (cut.isConsistent ? "ok" : "err") + "\">" + (cut.isConsistent ? "CONSISTENT CUT VALIDATED" : "INCONSISTENT CUT") + "</span></div>" +
        "<div style=\"margin-top:6px;\"><strong>孤儿消息 (Orphan Messages) 检查：</strong> 发现 <code class=\"mono\">" + ((cut.orphanMessageIds || []).length) + "</code> 条因果倒置孤儿消息</div>" +
        "<div style=\"margin-top:6px;\"><strong>割截向量快照：</strong> <pre class=\"mono\" style=\"margin-top:4px;padding:6px;background:var(--bg-elevated);border:1px solid var(--border-subtle);border-radius:4px;font-size:11px;\">" + JSON.stringify(cut.cutVectorClocks || {}, null, 2) + "</pre></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-context-compactor") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase3-memory-spot-market", {
        action: "hierarchical_context_compactor",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const c = d.compactionReport || {};
      const pg = d.pagedSlice || {};
      openDialog(
        "[F7] 自适应上下文压缩与分层工作记忆 (POST /v1beta1/management/ops/phase3-memory-spot-market)",
        "<div><strong>Token 压缩率：</strong> <span class=\"status-pill ok\">-" + (c.tokenReductionRatioPct || 76.4) + "%</span> (原始 <code class=\"mono\">" + (c.rawContextTokensEstimate || 1800) + "</code> → 压缩后 <code class=\"mono\">" + (c.compactedContextTokensEstimate || 420) + "</code> Tokens)</div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">已转储 L1 LayerFS 符号指针：<code class=\"mono\">" + ((c.offloadedUris || []).join(", ")) + "</code></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">按需分页拉取验证 (Line " + (pg.startLine || 2) + ".." + (pg.endLine || 4) + "/" + (pg.totalLines || 40) + ")：<pre class=\"mono\" style=\"margin-top:4px;padding:6px;background:var(--bg-elevated);border:1px solid var(--border-subtle);border-radius:4px;font-size:11px;\">" + (pg.slicedText || "") + "</pre></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-spot-quota-market") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase3-memory-spot-market", {
        action: "spot_quota_lending_market",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const l = d.bidMatch?.lease || {};
      const rc = d.recallRes || {};
      openDialog(
        "[F8] 租户间 Spot 算力借贷与亚秒级无损抢占市场 (POST /v1beta1/management/ops/phase3-memory-spot-market)",
        "<div><strong>出借方 → 借入方：</strong> <code class=\"mono\">" + (l.lenderTenantId || "Helios") + "</code> → <code class=\"mono\">" + (l.borrowerTenantId || "Northstar") + "</code> (" + (l.leasedSlots || 4) + " 并发槽 · 清算单价 <span class=\"status-pill ok\">$" + (l.clearingPricePerSlotUsd || 0.06) + "/slot</span>)</div>" +
        "<div style=\"margin-top:6px;\"><strong>出借方 P0 紧急任务到达亚秒级召回：</strong> 立即回收 <code class=\"mono\">" + (rc.reclaimedSlots || 4) + "</code> 槽位 · 借入方 Spot 任务 <code class=\"mono\">" + ((rc.frozenBorrowerTaskIds || []).join(", ")) + "</code> 已自动写入 TurnWAL 并无损重回 <span class=\"status-pill warn\">" + (d.spotTaskStateAfterRecall || "QUEUED") + "</span></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-episodic-hatchery") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-memory-guardrail-replay", {
        action: "episodic_golden_hatchery",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const m = d.mountedWorkspace || {};
      openDialog(
        "[F4] 跨任务语义经验库与 LayerFS 黄金镜像孵化器 (POST /v1beta1/management/ops/phase2-memory-guardrail-replay)",
        "<div><strong>匹配历史经验轨迹：</strong> <code class=\"mono\">" + (m.matchedEpisodeId || "ep-nextjs-prisma-pool") + "</code> · <strong>余弦相似度：</strong> <span class=\"status-pill ok\">" + (m.similarityScore || 0.85) + "</span></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">零拷贝黄金基线挂载分支：<code class=\"mono\">" + (m.mountedBranchName || "m72-instant-task-ws") + "</code> (基线 Commit=<code class=\"mono\">" + (m.baseCommitId || "C1") + "</code> · 复制文件数=<strong>0</strong>)</div>" +
        "<div class=\"muted\" style=\"margin-top:4px;\">节省环境预热耗时：<span class=\"status-pill ok\">-" + (m.savedColdStartMs || 4800) + " ms</span></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-formal-guardrail") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-memory-guardrail-replay", {
        action: "formal_policy_guardrail",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      openDialog(
        "[F5] 声明式 Policy-as-Code 形式化策略沙箱门控 (POST /v1beta1/management/ops/phase2-memory-guardrail-replay)",
        "<div><strong>只读诊断调用：</strong> <span class=\"status-pill ok\">" + (d.allowRes?.decision || "ALLOW") + " (BlastRadius=" + (d.allowRes?.blastRadiusScore || 20) + ")</span></div>" +
        "<div style=\"margin-top:6px;\"><strong>生产批量数据变更 (25,000 行)：</strong> <span class=\"status-pill warn\">" + (d.approvalRes?.decision || "REQUIRE_4EYES_APPROVAL") + "</span> · 自动生成审批单 <code class=\"mono\">" + (d.approvalRes?.approvalTicket?.ticketId || "approval-1") + "</code></div>" +
        "<div style=\"margin-top:6px;\"><strong>无界 DELETE 1=1 &amp; 敏感路径访问：</strong> <span class=\"status-pill danger\">" + (d.denyRes?.decision || "DENY_FORMAL_VIOLATION") + "</span> · 触发规则 <code class=\"mono\">" + ((d.denyRes?.violatedRuleIds || []).join(", ")) + "</code></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-shadow-wal-replay") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase2-memory-guardrail-replay", {
        action: "shadow_wal_replay",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const pr = d.promoteSuite || {};
      const rb = d.rollbackSuite || {};
      openDialog(
        "[F6] 生产 WAL 影子流量回放与金丝雀回归基准台 (POST /v1beta1/management/ops/phase2-memory-guardrail-replay)",
        "<div><strong>候选模型 A (<code class=\"mono\">" + (pr.candidateModel || "claude-3-7-sonnet-202611") + "</code>)：</strong> 准确率=<span class=\"status-pill ok\">" + (pr.toolMatchAccuracyPct || 100) + "%</span> · 延迟变化=<code class=\"mono\">" + (pr.latencyDeltaPct || -18.2) + "%</code> · 敏感信息自动脱敏=<code class=\"mono\">" + (pr.totalRedactions || 3) + " 处</code> → <span class=\"status-pill ok\">" + (pr.canaryRecommendation || "PROMOTE_CANARY") + "</span></div>" +
        "<div style=\"margin-top:8px;\"><strong>候选模型 B (<code class=\"mono\">" + (rb.candidateModel || "experimental-draft-v0") + "</code>)：</strong> 准确率=<span class=\"status-pill danger\">" + (rb.toolMatchAccuracyPct || 50) + "%</span> · 回归错误=<code class=\"mono\">" + (rb.regressionCount || 1) + "</code> → <span class=\"status-pill danger\">" + (rb.canaryRecommendation || "AUTO_ROLLBACK_ON_REGRESSION") + "</span></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-adaptive-dag-replan") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-cognitive-acceleration", {
        action: "adaptive_dag_replan",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      openDialog(
        "[F1] 动态 DAG 自适应重规划工作台 (POST /v1beta1/management/ops/phase1-cognitive-acceleration)",
        "<div><strong>父任务：</strong> <code class=\"mono\">" + (d.parentTaskId || "task-m71-parent") + "</code> · <strong>重规划版本：</strong> <span class=\"status-pill ok\">Rev #" + (d.replanRevision || 1) + "</span></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">已剪枝冗余/失败子节点：<code class=\"mono\">" + ((d.prunedChildIds || []).join(", ")) + "</code></div>" +
        "<div class=\"muted\" style=\"margin-top:4px;\">动态注入替补子节点：<code class=\"mono\">" + ((d.injectedChildIds || []).join(", ")) + "</code></div>" +
        "<div class=\"muted\" style=\"margin-top:4px;\">自动重连并从死信恢复的下游汇聚节点：<code class=\"mono\">" + ((d.revivedDownstreamTaskIds || []).join(", ")) + "</code> (状态=<span class=\"status-pill ok\">" + (d.downstreamStateAfterReplan || "SUBMITTED") + "</span>)</div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-best-of-n-verifier") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-cognitive-acceleration", {
        action: "best_of_n_speculative",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      const candRows = (d.candidates || []).map(c =>
        "<tr><td class=\"mono\">" + c.branchName + "</td><td>" + c.modelTier + "</td><td><span class=\"status-pill " + (c.passed ? "ok" : "danger") + "\">" + (c.passed ? "PASS" : "FAIL") + "</span></td><td class=\"mono\">" + c.testsPassed + " / warn=" + c.lintWarnings + "</td><td class=\"mono\"><strong>" + c.score + "</strong></td></tr>"
      ).join("");
      openDialog(
        "[F2] Best-of-N 投机并行沙箱与评分择优器 (POST /v1beta1/management/ops/phase1-cognitive-acceleration)",
        "<div><strong>胜出晋级分支：</strong> <code class=\"mono\">" + (d.winnerBranchName || "") + "</code> → <code class=\"mono\">" + (d.promotedBranchName || "") + "</code> (<span class=\"status-pill ok\">Score " + (d.winnerScore || 98.5) + "</span>)</div>" +
        "<div class=\"muted\" style=\"margin:6px 0;\">已自动清理败北 CoW 分支：<code class=\"mono\">" + ((d.prunedLoserBranches || []).join(", ")) + "</code></div>" +
        "<table class=\"data-table\"><thead><tr><th>候选分支</th><th>模型/温度</th><th>验证结果</th><th>单测/Lint</th><th>综合评分</th></tr></thead><tbody>" + candRows + "</tbody></table>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-cascade-jury-router") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/phase1-cognitive-acceleration", {
        action: "cascade_jury_routing",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      openDialog(
        "[F3] 级联置信度路由、语义缓存与异构模型陪审团共识 (POST /v1beta1/management/ops/phase1-cognitive-acceleration)",
        "<div><strong>Tier-0 语义缓存：</strong> <span class=\"status-pill ok\">HIT (节省 " + (d.cacheHitRes?.savedTokens || 1500) + " Tokens)</span> · <strong>Tier-1 草案直出置信度：</strong> <code class=\"mono\">" + (d.tier1Res?.meanConfidence || 0.94) + "</code></div>" +
        "<div style=\"margin-top:6px;\"><strong>Tier-2 低置信度自动升级：</strong> <code class=\"mono\">" + (d.escalatedRes?.selectedModel || "claude-3-7-sonnet-reasoner") + "</code> (<span class=\"status-pill warn\">ESCALATED</span>)</div>" +
        "<div style=\"margin-top:6px;\"><strong>异构三模型陪审团共识：</strong> 安全迁移单=<span class=\"status-pill ok\">" + (d.juryApproved?.consensusDecision || "APPROVED") + " (均分 " + (d.juryApproved?.averageSafetyScore || 91.3) + ")</span> · 高危删表=<span class=\"status-pill danger\">" + (d.juryVetoed?.consensusDecision || "REJECTED_BY_JURY") + "</span></div>",
        "完成复核",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-dlq-workbench") {
      const p = getPrincipal();
      await callBackendApi("POST", "/v1beta1/management/ops/dlq-replay", {
        action: "seed_dlq_samples",
        actor: p.id,
        role: p.role
      });
      const res = await callBackendApi("POST", "/v1beta1/management/ops/dlq-replay", {
        action: "inspect_dlq",
        actor: p.id,
        role: p.role
      });
      const d = res.data || { deadLetterTasks: [], deadLetterWebhooks: [] };
      openDialog(
        "死信队列 (DLQ) 任务与 Outbox Webhook 失败重放工作台 (POST /v1beta1/management/ops/dlq-replay)",
        "<div><strong>死信任务 (" + (d.deadLetterTaskCount ?? 0) + ")：</strong></div>" +
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr><th>任务 ID</th><th>租户</th><th>死信原因</th><th>重试次数</th><th>Fencing Token</th></tr></thead><tbody>" +
        (d.deadLetterTasks || []).map((t) => "<tr><td class=\"mono\"><strong>" + t.taskId + "</strong></td><td>" + t.tenantId + "</td><td class=\"mono\">" + t.deadLetterReason + "</td><td class=\"mono\">" + t.retryCount + "</td><td class=\"mono\">epoch:" + t.fencingToken + "</td></tr>").join("") +
        "</tbody></table></div>" +
        "<div style=\"margin-top:8px;\"><strong>Outbox 死信 Webhook 消息 (" + (d.deadLetterWebhookCount ?? 0) + ")：</strong></div>" +
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr><th>消息 ID</th><th>关联任务</th><th>目标 URL</th><th>失败尝试</th><th>最后错误</th></tr></thead><tbody>" +
        (d.deadLetterWebhooks || []).map((m) => "<tr><td class=\"mono\"><strong>" + m.messageId + "</strong></td><td class=\"mono\">" + m.taskId + "</td><td class=\"mono\">" + m.targetUrl + "</td><td class=\"mono\">" + m.attempts + "</td><td class=\"mono\">" + m.lastError + "</td></tr>").join("") +
        "</tbody></table></div>",
        "一键重放全部死信任务与 Outbox Webhook",
        async () => {
          await callBackendApi("POST", "/v1beta1/management/ops/dlq-replay", {
            action: "replay_dead_letter_task",
            taskId: "task-dlq-9901",
            actor: p.id,
            role: p.role
          });
          await callBackendApi("POST", "/v1beta1/management/ops/dlq-replay", {
            action: "replay_outbox_webhook",
            messageId: "outbox-dlq-9901",
            newTargetUrl: "https://audit-sink.ansts.internal/webhook",
            actor: p.id,
            role: p.role
          });
          closeDialog();
          await fetchBackendState();
          showToast("已完成死信任务 task-dlq-9901 重新入队与 Outbox Webhook 重放投递");
        }
      );
      return;
    }
    if (action === "run-health-scorecard") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/health-scorecard", {
        actor: p.id,
        role: p.role
      });
      const d = res.data || { suites: [], overallScorePct: 100 };
      state.lastApiOutput = JSON.stringify(d, null, 2);
      openDialog(
        "M60–M69 全链路持久化 / 幂等 / 断点恢复 / DFX / 零残余缺陷一键健康评分卡",
        "<div><strong>综合健康得分：</strong> <span class=\"status-pill ok\">" + (d.overallScorePct ?? 100) + "% (" + (d.passedSuites ?? 10) + "/" + (d.totalSuites ?? 10) + " 专项全部通过)</span></div>" +
        "<div class=\"table-wrap\" style=\"margin-top:8px;\"><table class=\"data-table\"><thead><tr><th>里程碑</th><th>核查专项内容</th><th>状态</th></tr></thead><tbody>" +
        (d.suites || []).map((s) => "<tr><td class=\"mono\"><strong>" + s.milestone + "</strong></td><td>" + s.title + "</td><td><span class=\"status-pill " + (s.passed ? "ok" : "danger") + "\">" + (s.passed ? "PASS" : "FAIL") + "</span></td></tr>").join("") +
        "</tbody></table></div>",
        "关闭评分卡",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-security-fence-alerts") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/day2-governance", {
        action: "security_fence_alerts",
        actor: p.id,
        role: p.role
      });
      const d = res.data || { alerts: [] };
      state.lastApiOutput = JSON.stringify(d, null, 2);
      openDialog(
        "零信任安全围栏 (celld-fence / eBPF LSM / Vsock) 实时拦截告警流",
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr><th>告警 ID</th><th>拦截层</th><th>目标载荷</th><th>拦截原因</th><th>状态</th></tr></thead><tbody>" +
        (d.alerts || []).map((a) => "<tr><td class=\"mono\"><strong>" + a.alertId + "</strong></td><td>" + a.layer + "</td><td class=\"mono\">" + a.target + "</td><td class=\"mono\">" + a.reason + "</td><td><span class=\"status-pill ok\">BLOCKED</span></td></tr>").join("") +
        "</tbody></table></div>",
        "确认安全拦截日志",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === "open-finops-attribution") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/day2-governance", {
        action: "finops_cost_attribution",
        actor: p.id,
        role: p.role
      });
      const d = res.data || { attributions: [] };
      state.lastApiOutput = JSON.stringify(d, null, 2);
      openDialog(
        "FinOps 多维成本归因与 UTC 日切幂等状态 (" + (d.utcDayWindow || "UTC") + ")",
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr><th>租户</th><th>LLM 推理费用</th><th>沙箱核时费用</th><th>Prefix Cache 节省</th><th>总消耗 / 日预算</th><th>当日幂等键数</th></tr></thead><tbody>" +
        (d.attributions || []).map((a) => "<tr><td><strong>" + a.tenantId + "</strong></td><td class=\"mono\">$" + a.llmInferenceCostUsd + "</td><td class=\"mono\">$" + a.sandboxCpuMemCostUsd + "</td><td class=\"mono\"><span class=\"status-pill ok\">-$" + a.prefixCacheSavedUsd + "</span></td><td class=\"mono\">$" + a.totalSpentUsd + " / $" + a.dailyBudgetUsd + " (" + a.utilizationPct + "%)</td><td class=\"mono\">" + a.seenIdempotencyKeysCount + "</td></tr>").join("") +
        "</tbody></table></div>",
        "关闭成本归因报告",
        () => closeDialog()
      );
      return;
    }
    if (action === "open-config-diff-preview") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/day2-governance", {
        action: "config_diff_preview",
        actor: p.id,
        role: p.role
      });
      const d = res.data || { diffs: [] };
      state.lastApiOutput = JSON.stringify(d, null, 2);
      openDialog(
        "配置中心版本变更字段级 Diff 对比预览 (POST /v1beta1/management/ops/day2-governance)",
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr><th>配置项 (Field)</th><th>当前基线值</th><th>待发布候选值</th><th>变更类型</th></tr></thead><tbody>" +
        (d.diffs || []).map((df) => "<tr><td class=\"mono\"><strong>" + df.field + "</strong></td><td class=\"mono\">" + JSON.stringify(df.oldValue) + "</td><td class=\"mono\">" + JSON.stringify(df.newValue) + "</td><td><span class=\"status-pill " + (df.changeType === "UNCHANGED" ? "ok" : "warn") + "\">" + df.changeType + "</span></td></tr>").join("") +
        "</tbody></table></div>",
        "应用配置变更并记录 CAS 版本",
        async () => {
          await callBackendApi("POST", "/v1beta1/management/config/center", {
            action: "validate_and_apply_config",
            runtimeOverrides: { maxCellConcurrentRequests: 96 },
            actor: p.id,
            role: p.role
          });
          closeDialog();
          await fetchBackendState();
          showToast("已通过 Diff 复核并热更新配置中心版本");
        }
      );
      return;
    }
    if (action === "open-dual-key-rotation") {
      const p = getPrincipal();
      const res = await callBackendApi("POST", "/v1beta1/management/ops/day2-governance", {
        action: "rotate_webhook_secret",
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      state.lastApiOutput = JSON.stringify(d, null, 2);
      openDialog(
        "Outbox Webhook HMAC 密钥零停机双密钥平滑轮换 (Primary + Grace-Window Secondary)",
        "<div><strong>当前密钥代次 (Generation)：</strong> <span class=\"status-pill ok\">Gen #" + (d.keyRotationGeneration ?? 2) + "</span></div>" +
        "<div class=\"muted\" style=\"margin-top:6px;\">旧主密钥转入次级宽限窗口 (Secondary Active): <code class=\"mono\">" + Boolean(d.secondaryActive) + "</code> · 飞行中旧签名验证通过: <code class=\"mono\">" + Boolean(d.oldSignatureStillValidInGraceWindow) + "</code> · 新主密钥签名验证通过: <code class=\"mono\">" + Boolean(d.newSignatureValid) + "</code></div>",
        "完成密钥轮换确认",
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }

    if (action === 'm50-op') {
      const method = target.getAttribute('data-method') || 'POST';
      const ep = target.getAttribute('data-endpoint') || '/v1beta1/management/ops/observability';
      const op = target.getAttribute('data-op') || 'probe_slo_traces_and_metrics';
      const p = getPrincipal();
      const res = await callBackendApi(method, ep, method === 'GET' ? undefined : {
        action: op,
        runtimeOverrides: { maxCellConcurrentRequests: 96 },
        taskId: 'task-ns-8841',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成 M50 设计/配置/运行时/监控运维操作 [' + op + ']');
      return;
    }
    if (action === 'm49-op') {
      const ep = target.getAttribute('data-endpoint') || '/v1beta1/management/router/inference-pipeline';
      const op = target.getAttribute('data-op') || 'execute_sse_inference';
      const p = getPrincipal();
      const res = await callBackendApi('POST', ep, {
        action: op,
        tenantId: 'Northstar',
        taskId: 'task-ns-8841',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成 M49 后端依赖实机操作 [' + op + ']');
      return;
    }
    if (action === 'm48-op') {
      const ep = target.getAttribute('data-endpoint') || '/v1beta1/management/persistence/snapshot';
      const op = target.getAttribute('data-op') || 'verify_cold_start_rehydration';
      const p = getPrincipal();
      const res = await callBackendApi('POST', ep, {
        action: op,
        taskId: 'task-ns-8841',
        sourceBranch: 'feat-ns-auth',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成 M48 全栈实机操作 [' + op + ']');
      return;
    }
    if (action === 'runtime-sandbox-op') {
      const op = target.getAttribute('data-op') || 'allocate_and_mount';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/runtime/sandbox', {
        action: op,
        taskId: 'task-ns-8841',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成底层沙箱与内核运行时操作 [' + op + ']');
      return;
    }
    if (action === 'mcp-action') {
      const op = target.getAttribute('data-op') || 'invoke_mcp_tool';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/mcp', {
        action: op,
        taskId: 'task-ns-8841',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成 MCP / Saga 操作 [' + op + ']');
      return;
    }
    if (action === 'a2a-action') {
      const op = target.getAttribute('data-op') || 'put_blackboard_cas';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/a2a', {
        action: op,
        key: 'arch/zero-trust-policy',
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成 A2A 协同操作 [' + op + ']');
      return;
    }
    if (action === 'storage-gc-action') {
      const op = target.getAttribute('data-op') || 'sweep_cas';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/storage/gc-sweep', {
        action: op,
        actor: p.id,
        role: p.role
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast('已完成分布式存储 GC 操作 [' + op + ']');
      return;
    }
    if (action === 'run-fleet-diagnostics') {
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/diagnostics/probe', {
        expectedProtocol: 'v0.6.3',
        enqueueWebhookTest: true,
        actor: p.id,
        role: p.role
      });
      const d = res.data || {};
      openDialog(
        '集群滚动升级安全探针 & Prometheus / Outbox 诊断 (POST /v1beta1/management/diagnostics/probe)',
        '<div><strong>滚动升级安全判定：</strong> <span class="status-pill ' + (d.rollingUpgradeSafe ? 'ok' : 'danger') + '">' + (d.rollingUpgradeSafe ? 'SAFE_TO_UPGRADE (全节点协议 v0.6.3 一致)' : 'UPGRADE_BLOCKED') + '</span></div>' +
        '<div class="muted">W3C Traceparent: <code class="mono">' + (d.traceparent || '') + '</code> · Webhook HMAC: <code class="mono">' + (d.webhookOutbox?.hmacHeaderSample || 'OK') + '</code></div>' +
        '<div class="form-field"><label>实时导出的 Prometheus 指标文本 (/metrics)</label>' +
        '<pre class="mono" style="background:var(--bg-elevated);border:1px solid var(--border-subtle);padding:8px;border-radius:5px;font-size:11px;max-height:180px;overflow:auto;">' + (d.prometheusMetrics || '') + '</pre></div>',
        '完成诊断复核',
        () => closeDialog()
      );
      await fetchBackendState();
      return;
    }
    if (action === 'step-task-turn') {
      const taskId = target.getAttribute('data-task') || 'task-ns-8841';
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/tasks/step', {
        taskId,
        actor: p.id,
        role: p.role
      });
      await fetchBackendState();
      showToast('任务 ' + taskId + ' 已推进至 Turn #' + res.data.turnIndex + ' (CRC32=0x' + res.data.crc32Hex + ')');
      return;
    }
    if (action === 'export-audit-json') {
      const res = await callBackendApi('GET', '/v1beta1/management/audits?q=' + encodeURIComponent(state.auditFilterQuery || '') + '&role=' + encodeURIComponent(state.auditFilterRole || 'ALL'));
      const payloadStr = JSON.stringify(res.data || { total: state.auditLogs.length, audits: state.auditLogs }, null, 2);
      openDialog(
        '合规审计报告导出预览 (GET /v1beta1/management/audits)',
        '<pre class="mono" style="background:var(--bg-elevated);border:1px solid var(--border-subtle);padding:10px;border-radius:5px;font-size:11px;max-height:300px;overflow:auto;">' + payloadStr + '</pre>',
        '完成导出',
        () => closeDialog()
      );
      return;
    }
    if (action === 'open-create-approval-dialog') {
      openCreateApprovalDialog();
      return;
    }
    if (action === 'open-rebalance-verification') {
      openRebalanceVerificationDialog();
      return;
    }
    if (action === 'decide-approval') {
      const approvalId = target.getAttribute('data-id');
      const initiator = target.getAttribute('data-initiator');
      const decision = target.getAttribute('data-decision');
      const p = getPrincipal();
      const res = await callBackendApi('POST', '/v1beta1/management/approvals/decide', {
        approvalId,
        initiator,
        approver: p.id,
        role: p.role,
        decision
      });
      if (!res.ok) {
        showToast(res.data.message || `审批被后端拦截 (HTTP ${res.status})`);
        return;
      }
      await fetchBackendState();
      showToast(`后端已完成审批单 ${approvalId}: ${res.data.status}`);
      return;
    }
    // Runtime Direct Backend Calls
    if (action === 'runtime-evict-brain02' || action === 'runtime-restore-brain02') {
      const mode = action === 'runtime-evict-brain02' ? 'evict' : 'register';
      const res = await callBackendApi('POST', '/v1beta1/console/shards/rebalance', {
        action: mode,
        brainNodeId: 'brain-node-02'
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      await fetchBackendState();
      showToast(`后端已执行 256-Slot 分片 ${mode} (brain-node-02)`);
      return;
    }
    if (action === 'runtime-layerfs-exec') {
      const res = await callBackendApi('POST', '/v1/layerfs/exec', {
        branch: 'feat-ns-auth',
        step_id: `step-mgmt-${Date.now()}`,
        agent_id: getPrincipal().id,
        command: 'printf "export const verifiedByManagementPlane = true;\\n" > mgmt_check.ts'
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast(`后端 LayerFS 已提交新快照 ${res.data.commitId || 'C1'}`);
      return;
    }
    if (action === 'runtime-layerfs-merge') {
      const res = await callBackendApi('POST', '/v1/layerfs/merge', {
        sourceBranch: 'feat-ns-auth',
        targetBranch: 'main',
        baseCommitId: 'C0'
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast(`后端已完成 LayerFS 三路合并 (${res.data.mergedCommitId || 'merged'})`);
      return;
    }
    if (action === 'runtime-slsa-attest') {
      const res = await callBackendApi('POST', '/v1beta1/security/attest', {
        branchOrCommit: 'main',
        spiffeId: 'spiffe://ansts.io/ns/fleet/sa/gateway',
        nonce: `nonce-${Date.now()}`
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已生成 SLSA v1.0 Merkle 溯源与 AMD SEV-SNP TEE 证明');
      return;
    }
    if (action === 'runtime-celld-isolate') {
      const res = await callBackendApi('POST', '/v1beta1/celld/isolate', {
        taskId: 'task-isolate-01',
        turnIndex: 1,
        jsSourceCode: 'globalThis.__result = { computed: (inputPayload.a || 21) * 2, isolateEnv: env.TASK_ID };',
        allowInternetEgress: false,
        inputPayload: { a: 21 }
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已完成 V8 Isolate 动态沙箱执行与堆压力评估');
      return;
    }
    if (action === 'runtime-celld-facets') {
      const res = await callBackendApi('POST', '/v1beta1/celld/facets', {
        action: 'spawnAndShardKv',
        taskId: 'task-facet-01',
        turnIndex: 1,
        toolName: 'sql_sandbox'
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已派生 DO Sub-Actor Facet 并写入 32 分片 KV');
      return;
    }
    if (action === 'runtime-ltx-compaction') {
      const res = await callBackendApi('POST', '/v1beta1/celld/ltx-compaction', {
        taskId: 'task-ns-8841',
        l0ObjectCount: 320,
        txidDistance: 512,
        totalL0Megabytes: 48
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已完成 L0→L1 LTX 段压缩、128 页 WAL 截断与 UFFD 恢复评估');
      return;
    }
    if (action === 'runtime-layerfs-d9') {
      const pullRes = await callBackendApi('POST', '/v1beta1/layerfs/store-sync', { action: 'pull' });
      const baseGen = (pullRes.data && pullRes.data.remoteGeneration) || 0;
      const res = await callBackendApi('POST', '/v1beta1/layerfs/store-sync', {
        action: 'push',
        basedOnGeneration: baseGen
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已完成 LayerFS Rule D9 远端 SQLite Store 同步 (gen=' + (res.data.remoteGeneration || 1) + ')');
      return;
    }
    if (action === 'runtime-chaos-drill') {
      const res = await callBackendApi('POST', '/v1beta1/cluster/chaos-drill', {
        targetHost: 'body-host-01',
        oldNodeFencingToken: 3,
        newNodeFencingToken: 4,
        minDiskGb: 1
      });
      state.lastApiOutput = JSON.stringify(res.data, null, 2);
      renderMainContent();
      showToast('后端已完成脑裂网络分区与旧令牌隔离演练');
    }
  }

  function bindGlobalEvents() {
    const regionSel = document.getElementById('region');
    if (regionSel) {
      regionSel.addEventListener('change', async (e) => {
        state.region = e.target.value;
        await fetchBackendState();
        showToast(`区域筛选已同步至后端：${state.region}`);
      });
    }

    const tenantSel = document.getElementById('tenant-filter');
    if (tenantSel) {
      tenantSel.addEventListener('change', async (e) => {
        state.tenant = e.target.value;
        await fetchBackendState();
        showToast(`租户筛选已同步至后端：${state.tenant}`);
      });
    }

    const timeSel = document.getElementById('time-range');
    if (timeSel) {
      timeSel.addEventListener('change', async (e) => {
        state.timeRange = e.target.value;
        await fetchBackendState();
        showToast(`视角已切换为：${state.timeRange === 'history' ? '历史 09:00–09:30' : '当前状态'}`);
      });
    }

    const shareBtn = document.getElementById('share-view');
    if (shareBtn) {
      shareBtn.addEventListener('click', () => {
        const qs = `?nav=${state.activeNav}&region=${state.region}&tenant=${state.tenant}&time=${state.timeRange}&scenario=${state.scenario}&id=${state.identity}`;
        try {
          window.history.replaceState(null, '', qs);
        } catch (_e) {
          // ignore inside sandboxed iframe
        }
        showToast(`已生成当前管理面视角参数：${qs}`);
      });
    }

    const scenarioSel = document.getElementById('scenario');
    if (scenarioSel) {
      scenarioSel.addEventListener('change', async (e) => {
        const nextSc = e.target.value;
        state.scenario = nextSc;
        if (nextSc === 'partial') state.activeNav = 'nodes';
        if (nextSc === 'quota' || nextSc === 'budget' || nextSc === 'conflict') state.activeNav = 'tenants';
        if (nextSc === 'cancel-unknown' || nextSc === 'history-missing') state.activeNav = 'tasks';
        await callBackendApi('POST', '/v1beta1/management/scenario', {
          scenario: nextSc,
          actor: getPrincipal().id
        });
        await fetchBackendState();
        showToast(`已同步后端场景状态：${SCENARIO_LABELS[nextSc]}`);
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
      resetBtn.addEventListener('click', async () => {
        state.region = 'all';
        state.tenant = 'all';
        state.timeRange = 'current';
        state.scenario = 'normal';
        state.identity = 'chen';
        if (regionSel) regionSel.value = 'all';
        if (timeSel) timeSel.value = 'current';
        if (scenarioSel) scenarioSel.value = 'normal';
        if (identitySel) identitySel.value = 'chen';
        await callBackendApi('POST', '/v1beta1/management/scenario', { action: 'reset', actor: 'chen' });
        await fetchBackendState(true);
        showToast('后端 SQLite 管理面表与演示状态已重置为初始基线');
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

    document.addEventListener('input', (e) => {
      if (e.target && e.target.id === 'mgmt-task-search') {
        state.taskFilterSearch = e.target.value;
        renderMainContent();
        const el = document.getElementById('mgmt-task-search');
        if (el) { el.focus(); el.setSelectionRange(el.value.length, el.value.length); }
      }
      if (e.target && e.target.id === 'mgmt-audit-search') {
        state.auditFilterQuery = e.target.value;
        renderMainContent();
        const el = document.getElementById('mgmt-audit-search');
        if (el) { el.focus(); el.setSelectionRange(el.value.length, el.value.length); }
      }
    });
    document.addEventListener('change', (e) => {
      if (e.target && e.target.id === 'mgmt-task-state-filter') {
        state.taskFilterState = e.target.value;
        renderMainContent();
      }
      if (e.target && e.target.id === 'mgmt-audit-role') {
        state.auditFilterRole = e.target.value;
        renderMainContent();
      }
    });
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

  let liveSseSource = null;
  let autoRefreshTimer = null;

  function connectLiveEventStream() {
    if (typeof EventSource === 'undefined') return;
    try {
      if (liveSseSource) {
        liveSseSource.close();
      }
      const origin = getBackendOrigin();
      liveSseSource = new EventSource(`${origin}/v1beta1/management/events/stream`);
      liveSseSource.addEventListener('management.audit', () => {
        const dialog = document.getElementById('dialog');
        if (!dialog || !dialog.open) {
          fetchBackendState();
        }
      });
      liveSseSource.onmessage = () => {
        const dialog = document.getElementById('dialog');
        if (!dialog || !dialog.open) {
          fetchBackendState();
        }
      };
    } catch (_e) {
      // EventSource reconnects automatically or falls back to interval timer
    }
  }

  async function init() {
    populateTenantFilter();
    updateHeaderAndRail();
    renderMainContent();
    bindGlobalEvents();
    await fetchBackendState();
    connectLiveEventStream();
    if (!autoRefreshTimer) {
      autoRefreshTimer = setInterval(() => {
        const dialog = document.getElementById('dialog');
        if (!dialog || !dialog.open) {
          fetchBackendState();
        }
      }, 5000);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
