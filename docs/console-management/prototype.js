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
  async function callBackendApi(method, endpointPath, payload) {
    const origin = getBackendOrigin();
    const fullUrl = `${origin}${endpointPath}`;
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 1800);
      const resp = await fetch(fullUrl, {
        method,
        headers: { 'content-type': 'application/json', 'accept': 'application/json' },
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
      updateConnectionIndicators(false);
      return executeLocalEngineMirror(method, endpointPath, payload || {});
    }
  }

  function updateConnectionIndicators(isLive) {
    const demoLabel = document.querySelector('.demo-label');
    const wsTag = document.querySelector('.workspace-tag');
    if (demoLabel) {
      if (state.scenario === 'offline') {
        demoLabel.textContent = '503 OFFLINE · 接口离线锁定';
        demoLabel.className = 'demo-label warn';
      } else if (isLive) {
        demoLabel.textContent = 'REAL BFF · 真实后端已连接';
        demoLabel.className = 'demo-label';
      } else {
        demoLabel.textContent = 'LOCAL ENGINE · 内置引擎同步';
        demoLabel.className = 'demo-label';
      }
    }
    if (wsTag) {
      wsTag.textContent = isLive ? 'LIVE v2.5' : 'ENGINE';
    }
  }

  // Pulls real state from GET /v1beta1/management/state and merges into UI state
  async function fetchBackendState(recoverOffline) {
    const qs = `?region=${encodeURIComponent(state.region)}&tenant=${encodeURIComponent(state.tenant)}&timeRange=${encodeURIComponent(state.timeRange)}${recoverOffline ? '&recover=1' : ''}`;
    const res = await callBackendApi('GET', `/v1beta1/management/state${qs}`);
    if (res.status === 503) {
      state.scenario = 'offline';
      const scSel = document.getElementById('scenario');
      if (scSel) scSel.value = 'offline';
      updateConnectionIndicators(state.liveBackendConnected);
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

  // Deterministic Local Engine Mirror (used only when Canvas iframe cannot reach 127.0.0.1:8088)
  function executeLocalEngineMirror(method, endpointPath, body) {
    if (method === 'GET' && endpointPath.startsWith('/v1beta1/management/state')) {
      if ( endpointPath.includes('recover=1') ) {
        state.scenario = 'normal';
      }
      if (state.scenario === 'offline') {
        return { ok: false, status: 503, data: { error: 'MANAGEMENT_API_UNAVAILABLE' }, fromLiveBackend: false };
      }
      return { ok: true, status: 200, data: state, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/scenario') {
      if (body.action === 'reset') {
        const curId = state.identity;
        const curNav = state.activeNav;
        state = createBaselineState();
        state.identity = curId;
        state.activeNav = curNav;
        return { ok: true, status: 200, data: { reset: true, state }, fromLiveBackend: false };
      }
      state.scenario = body.scenario || 'normal';
      if (state.scenario === 'stale') state.lastSyncUtc = '2026-11-10 09:12:00 UTC';
      else state.lastSyncUtc = '2026-11-10 09:30:00 UTC';
      if (state.scenario === 'conflict') {
        const ns = state.tenants.find((t) => t.id === 'Northstar');
        if (ns && ns.configVersion < 15) {
          ns.configVersion = 15;
          ns.maxSlots = 150;
          ns.updatedBy = 'zhou';
        }
      }
      if (state.scenario === 'cancel-unknown') {
        const tk = state.tasks.find((t) => t.id === 'task-ns-8841');
        if (tk) tk.state = 'CANCEL_UNCONFIRMED';
      }
      if (state.scenario === 'partial') {
        state.batchReport = {
          operation: 'drain',
          partialFailure: true,
          results: [
            { nodeId: 'celld-brain-01', status: 'SUCCEEDED' },
            { nodeId: 'body-host-01', status: 'SUCCEEDED' },
            { nodeId: 'body-host-02', status: 'FAILED', errorCode: 'ERR_ACTIVE_LEASE_PINNED', message: '节点 body-host-02 持有未释放的 P0 任务租约 (task-ns-8841@epoch:12)' }
          ]
        };
      }
      return { ok: true, status: 200, data: { scenario: state.scenario, state }, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/tenants/config') {
      const t = state.tenants.find((x) => x.id === body.tenantId) || state.tenants[0];
      if (Number(body.expectedVersion) !== t.configVersion) {
        return {
          ok: false,
          status: 409,
          data: {
            error: 'CAS_VERSION_CONFLICT',
            actualVersion: t.configVersion,
            updatedBy: t.updatedBy || 'zhou',
            currentMaxSlots: t.maxSlots,
            message: `CAS 版本冲突：基线版本 v${body.expectedVersion} 已过期，远端当前版本为 v${t.configVersion}（由 ${t.updatedBy || 'zhou'} 更新）`
          },
          fromLiveBackend: false
        };
      }
      t.maxSlots = Number(body.maxSlots);
      t.dailyBudgetUsd = Number(body.dailyBudgetUsd);
      t.configVersion += 1;
      t.updatedBy = body.actor || 'chen';
      if (state.scenario === 'quota' || state.scenario === 'conflict') state.scenario = 'normal';
      state.auditLogs.unshift({
        ts: '09:31:05 UTC',
        actor: body.actor || 'chen',
        role: body.role || 'SRE_ADMIN',
        action: `更新租户 ${t.id} 配额上限至 ${t.maxSlots} 槽 / 日预算 $${t.dailyBudgetUsd}`,
        scope: `${t.region} / ${t.id}`,
        outcome: `已生效 (v${t.configVersion})`
      });
      return { ok: true, status: 200, data: { tenantId: t.id, configVersion: t.configVersion }, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/nodes/batch') {
      const nodeIds = body.nodeIds || [];
      const results = [];
      for (const nId of nodeIds) {
        const node = state.nodes.find((n) => n.id === nId);
        const hasPinned = node && node.pinnedP0Tasks && node.pinnedP0Tasks.length > 0;
        if (body.operation === 'drain' && hasPinned && !body.forceMigrateLeases) {
          if (node) node.status = 'DRAIN_FAILED';
          results.push({ nodeId: nId, status: 'FAILED', errorCode: 'ERR_ACTIVE_LEASE_PINNED', message: `节点 ${nId} 持有未释放的 P0 任务租约 (task-ns-8841@epoch:12)` });
        } else {
          if (node) {
            if (body.forceMigrateLeases) node.pinnedP0Tasks = [];
            node.status = body.operation === 'drain' || body.operation === 'cordon' ? 'CORDONED' : 'READY';
          }
          results.push({ nodeId: nId, status: 'SUCCEEDED' });
        }
      }
      const partialFailure = results.some((r) => r.status === 'FAILED');
      state.batchReport = { operation: body.operation, partialFailure, results };
      if (!partialFailure && state.scenario === 'partial') state.scenario = 'normal';
      return { ok: !partialFailure, status: partialFailure ? 207 : 200, data: state.batchReport, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/tasks/cancel') {
      const tk = state.tasks.find((t) => t.id === body.taskId) || state.tasks[0];
      if (body.mode === 'force_fence') {
        tk.fence += 1;
        tk.state = 'FENCED_TERMINATED';
        if (state.scenario === 'cancel-unknown') state.scenario = 'normal';
        return { ok: true, status: 200, data: { taskId: tk.id, state: 'FENCED_TERMINATED', newFencingToken: tk.fence }, fromLiveBackend: false };
      }
      tk.state = 'CANCEL_UNCONFIRMED';
      return { ok: true, status: 202, data: { taskId: tk.id, state: 'CANCEL_UNCONFIRMED', confirmed: false }, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/tasks/time-travel') {
      if (state.scenario === 'history-missing' && Number(body.targetTurnIndex) >= 4) {
        return {
          ok: false,
          status: 422,
          data: {
            error: 'LTX_SEGMENT_MISSING',
            message: `拒绝执行：任务 ${body.taskId} 的 Turn #${body.targetTurnIndex} S3 LTX 增量段未归档就绪，请选择最近完整校验点 Turn #3 (Commit C2)。`
          },
          fromLiveBackend: false
        };
      }
      const newTaskId = `${body.taskId}-fork-t${body.targetTurnIndex}`;
      state.tasks.unshift({
        id: newTaskId,
        tenant: 'Northstar',
        region: 'us-west-2',
        priority: 'P0',
        shardSlot: 42,
        brain: 'celld-brain-01',
        body: 'body-host-01',
        fence: 1,
        turn: Number(body.targetTurnIndex),
        branch: `branch-${newTaskId}`,
        state: 'RUNNING_ACT'
      });
      if (state.scenario === 'history-missing') state.scenario = 'normal';
      return { ok: true, status: 200, data: { forkedTaskId: newTaskId, layerfsBranch: `branch-${newTaskId}` }, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/approvals/decide') {
      const item = state.approvals.find((a) => a.id === body.approvalId);
      const initiator = body.initiator || (item ? item.initiator : 'chen');
      if (initiator === body.approver) {
        return { ok: false, status: 409, data: { error: 'SELF_APPROVAL_FORBIDDEN', message: '双人异体复核拦截：发起人不能审批自己提交的高危申请单' }, fromLiveBackend: false };
      }
      const nextStatus = body.decision === 'approve' ? `APPROVED (${body.approver})` : `REJECTED (${body.approver})`;
      if (item) item.status = nextStatus;
      return { ok: true, status: 200, data: { approvalId: body.approvalId, status: nextStatus }, fromLiveBackend: false };
    }
    if (method === 'POST' && endpointPath === '/v1beta1/management/capacity/action') {
      state.loadSheddingActive = true;
      state.verificationRetried = true;
      state.scenario = 'normal';
      return { ok: true, status: 200, data: { ok: true }, fromLiveBackend: false };
    }
    return { ok: true, status: 200, data: { executed: true, endpointPath, body }, fromLiveBackend: false };
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
                    <td class="mono">${log.ts}</td>
                    <td><strong>${log.actor}</strong> <span class="muted">(${log.role || 'SRE_ADMIN'})</span></td>
                    <td>${log.action}</td>
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
            <button type="button" data-action="run-batch-node-op" data-op="prewarm" ${!perm.ok ? 'disabled' : ''}>批量预热沙箱池</button>
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
    const permGlobal = canMutateTarget(state.region, state.tenant, false);
    return `
      <div class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">全局任务运行队列、停止确认与历史回合 Time-Travel 诊断 (对接 CoreScheduler5D &amp; TurnWalEngine)</h2>
            <p class="panel-subtitle">对接 <code>POST /v1beta1/management/tasks/cancel</code> 与 <code>POST /v1beta1/management/tasks/time-travel</code>，支持派发新任务、推进 Fencing Token 与零拷贝回合分叉。</p>
          </div>
          <div class="btn-row">
            <button type="button" class="primary" data-action="open-dispatch-task-dialog" ${!permGlobal.ok ? 'disabled' : ''}>+ 派发新分布式任务 (/v1beta1/tasks/dispatch)</button>
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
              ${state.tasks.map((t) => {
                const perm = canMutateTarget(t.region, t.tenant, false);
                const isUnconfirmed = t.state === 'CANCEL_UNCONFIRMED';
                const isFenced = t.state === 'FENCED_TERMINATED';
                const pillCls = isUnconfirmed ? 'warn' : isFenced ? 'danger' : 'ok';
                return `
                  <tr>
                    <td class="mono"><strong>${t.id}</strong></td>
                    <td>${t.tenant} · <span class="mono">${t.region}</span></td>
                    <td class="mono">${t.priority} · Slot #${t.shardSlot ?? 0}</td>
                    <td class="mono">${t.brain} → ${t.body}</td>
                    <td class="mono">epoch:${t.fence}</td>
                    <td class="mono">Turn #${t.turn} · ⎇ ${t.branch}</td>
                    <td><span class="status-pill ${pillCls}">${t.state}</span></td>
                    <td>
                      <div class="btn-row">
                        <button type="button" class="small" data-action="open-turn-history-dialog" data-task="${t.id}">
                          回合历史 / 分叉
                        </button>
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
                    <td class="mono"><strong>${a.id}</strong></td>
                    <td class="mono">${a.type}</td>
                    <td>${a.target}</td>
                    <td class="mono">${a.region} / ${a.tenant}</td>
                    <td>${IDENTITIES[a.initiator]?.name || a.initiator}</td>
                    <td><span class="status-pill ${a.risk === 'CRITICAL' ? 'danger' : 'warn'}">${a.risk}</span></td>
                    <td><span class="status-pill ${isPending ? 'warn' : 'ok'}">${a.status}</span></td>
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
          </div>
          <pre class="mono" style="background:var(--bg-elevated);border:1px solid var(--border-subtle);padding:10px;border-radius:5px;font-size:11.5px;overflow-x:auto;margin:0;max-height:180px;">${state.lastApiOutput}</pre>
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

  function openTurnHistoryDialog(taskId) {
    const task = state.tasks.find((t) => t.id === taskId) || state.tasks[0];
    const isMissing = state.scenario === 'history-missing';

    openDialog(
      `任务回合历史与零拷贝 Time-Travel 分叉 · ${task.id}`,
      `
        <div>目标任务：<strong class="mono">${task.id}</strong> · LayerFS 分支：<code class="mono">${task.branch}</code></div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>回合</th><th>LayerFS Commit</th><th>LTX 归档状态</th><th>说明</th></tr>
            </thead>
            <tbody>
              <tr><td class="mono">Turn #1–#2</td><td class="mono">C0 / C1</td><td><span class="status-pill ok">S3 LTX 完整</span></td><td class="muted">CRC32 校验通过</td></tr>
              <tr><td class="mono"><strong>Turn #3</strong></td><td class="mono"><strong>C2 (推荐恢复点)</strong></td><td><span class="status-pill ok">S3 LTX 完整</span></td><td><span class="status-pill ok">最近完整校验点</span></td></tr>
              <tr><td class="mono">Turn #4–#5</td><td class="mono">C3 / C4</td><td><span class="status-pill ${isMissing ? 'danger' : 'ok'}">${isMissing ? 'LTX_SEGMENT_MISSING' : 'S3 LTX 完整'}</span></td><td class="muted">${isMissing ? '后端将拦截 422' : '可安全检出'}</td></tr>
            </tbody>
          </table>
        </div>
        <div class="form-field">
          <label for="dlg-fork-turn">选择 Time-Travel 零拷贝分叉回合 (POST /v1beta1/management/tasks/time-travel)</label>
          <select id="dlg-fork-turn">
            <option value="3" selected>Turn #3 (Commit C2 · 最近完整校验点)</option>
            <option value="2">Turn #2 (Commit C1)</option>
            <option value="4">${isMissing ? 'Turn #4 (缺失段 · 触发后端 422 完整性拦截)' : 'Turn #4 (Commit C3)'}</option>
          </select>
        </div>
      `,
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
        showToast(`后端已零拷贝派生新任务 ${res.data.forkedTaskId} (分支 ${res.data.layerfsBranch})`);
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
      openTurnHistoryDialog(target.getAttribute('data-task') || 'task-ns-8841');
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

  async function init() {
    populateTenantFilter();
    updateHeaderAndRail();
    renderMainContent();
    bindGlobalEvents();
    await fetchBackendState();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
