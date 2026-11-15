'use client';
import React, { useCallback, useEffect, useState } from 'react';
import { HruiShell } from './Shell';

export interface DynamicClusterNode {
  nodeId: string;
  brainKey?: string;
  role: 'brain' | 'body' | 'gateway' | string;
  region: string;
  baseUrl: string;
  status: string;
  cpuPct: number;
  psiPct: number;
  activeLeases: number;
  capacitySlots: number;
  ownedSlots: number;
  registeredVia: string;
  heartbeatAgeMs: number;
  leaseEpoch: number;
}

export interface DynamicClusterTopology {
  topologyGeneration: number;
  ringDigest: string;
  virtualSlots: number;
  servicesCount: number;
  activeBrainMembers: string[];
  totalRegisteredNodes: number;
  countsByRole: { brain: number; body: number; gateway: number };
  slotDistribution: Record<string, number>;
  slotOwners: string[];
  brains: DynamicClusterNode[];
  bodies: DynamicClusterNode[];
  gateways: DynamicClusterNode[];
}

export interface ManagementTenant {
  id: string;
  region: string;
  tier: string;
  usedSlots: number;
  maxSlots: number;
  escrowReserved: number;
  dailyBudgetUsd: number;
  spentUsd: number | null;
  budgetStatus: string;
  modelPolicy: string;
  configVersion: number;
  updatedBy: string;
  updatedAt: string;
}

export interface ManagementTask {
  id: string;
  tenant: string;
  region: string;
  priority: string;
  shardSlot: number;
  brain: string;
  body: string;
  fence: number;
  turn: number;
  branch: string;
  state: string;
  prompt: string;
  deadLetterReason?: string;
}

export interface ManagementApproval {
  id: string;
  type: string;
  target: string;
  region: string;
  tenant: string;
  initiator: string;
  status: string;
  risk: string;
  createdAt: string;
  decidedBy?: string;
}

export interface ManagementAuditLog {
  auditId: string;
  ts: string;
  actor: string;
  role: string;
  action: string;
  scope: string;
  outcome: string;
}

export interface ManagementRegion {
  regionId: string;
  jurisdiction: string;
  capacityAvailable: boolean;
  rttMs: number;
  brainSummary: string;
  avgCpuPct: number;
  avgPsiPct: number;
  nodeCount: number;
  replicationLabel: string;
  sloStatus: string;
}

export interface ManagementStatePayload {
  cluster: string;
  gatewayNodeId: string;
  scenario: string;
  lastSyncUtc: string;
  virtualSlots: number;
  slotDistribution: Record<string, number>;
  slotOwners: string[];
  sla: {
    p0QueueWaitP99Ms: number;
    warmPoolHitRatio: number;
    utilizationRatio: number;
    compliant: boolean;
    breaches: string[];
  };
  canary: {
    state: string;
    trafficWeightPct: number;
  };
  regions: ManagementRegion[];
  tenants: ManagementTenant[];
  tasks: ManagementTask[];
  approvals: ManagementApproval[];
  auditLogs: ManagementAuditLog[];
  healthMatrix: {
    overallHealthy: boolean;
    overallStatus: string;
    probes: Array<{ id: string; name: string; status: string; latencyMs: number; detail: string }>;
    processMemory: { rssMb: number; heapUsedMb: number; heapTotalMb: number };
  };
}

export function AnstsHruiConsole() {
  const [activeNav, setActiveNav] = useState<string>('overview');
  const [darkTheme, setDarkTheme] = useState<boolean>(false);
  const [backendOrigin, setBackendOrigin] = useState<string>('http://127.0.0.1:8088');
  const [liveConnected, setLiveConnected] = useState<boolean>(false);
  const [sseConnected, setSseConnected] = useState<boolean>(false);

  // Real RBAC Identity & Bearer JWT issued by POST /v1beta1/management/auth/token
  const [principalId, setPrincipalId] = useState<string>('chen');
  const [principalRole, setPrincipalRole] = useState<string>('SRE_ADMIN');
  const [jwtToken, setJwtToken] = useState<string>('');

  // Zero-Stub Backend State Collections (initialized strictly to null; populated 100% from real backend BFF endpoints)
  const [topology, setTopology] = useState<DynamicClusterTopology | null>(null);
  const [mgmtState, setMgmtState] = useState<ManagementStatePayload | null>(null);
  const [finopsData, setFinopsData] = useState<Record<string, unknown> | null>(null);
  const [queuesData, setQueuesData] = useState<Record<string, unknown> | null>(null);
  const [federationData, setFederationData] = useState<Record<string, unknown> | null>(null);
  const [dagData, setDagData] = useState<Record<string, unknown> | null>(null);
  const [blueprintsData, setBlueprintsData] = useState<Record<string, unknown> | null>(null);
  const [runtimeData, setRuntimeData] = useState<Record<string, unknown> | null>(null);
  const [configsData, setConfigsData] = useState<Record<string, unknown> | null>(null);
  const [profilerData, setProfilerData] = useState<Record<string, unknown> | null>(null);
  const [swarmData, setSwarmData] = useState<Record<string, unknown> | null>(null);
  const [approvalsSagaData, setApprovalsSagaData] = useState<Record<string, unknown> | null>(null);
  const [securityData, setSecurityData] = useState<Record<string, unknown> | null>(null);
  const [observabilityData, setObservabilityData] = useState<Record<string, unknown> | null>(null);
  const [celldStatusData, setCelldStatusData] = useState<Record<string, unknown> | null>(null);

  // Interactive workbench selections & real backend response output
  const [selectedTaskTurns, setSelectedTaskTurns] = useState<Record<string, unknown> | null>(null);
  const [layerfsFileView, setLayerfsFileView] = useState<Record<string, unknown> | null>(null);
  const [layerfsBranchInput, setLayerfsBranchInput] = useState<string>('main');
  const [layerfsFileInput, setLayerfsFileInput] = useState<string>('package.json');
  const [newTenantId, setNewTenantId] = useState<string>('');
  const [newTenantMaxSlots, setNewTenantMaxSlots] = useState<number>(120);
  const [newTenantBudgetUsd, setNewTenantBudgetUsd] = useState<number>(1000);
  const [newTaskPrompt, setNewTaskPrompt] = useState<string>('Run distributed LayerFS CoW verification');
  const [newTaskTenant, setNewTaskTenant] = useState<string>('Northstar');
  const [auditSearchQuery, setAuditSearchQuery] = useState<string>('');
  const [lastApiResult, setLastApiResult] = useState<string>('');

  const buildAuthHeaders = useCallback(
    (extra?: Record<string, string>): Record<string, string> => {
      const headers: Record<string, string> = {
        'content-type': 'application/json',
        ...(extra ?? {}),
      };
      if (jwtToken) {
        headers['authorization'] = `Bearer ${jwtToken}`;
      }
      return headers;
    },
    [jwtToken]
  );

  const issueLiveJwt = useCallback(
    async (targetPrincipal: string) => {
      try {
        const resp = await fetch(`${backendOrigin}/v1beta1/management/auth/token`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ principalId: targetPrincipal }),
        });
        if (resp.ok) {
          const data = (await resp.json()) as {
            token?: string;
            claims?: { sub: string; role: string };
          };
          if (data.token) {
            setJwtToken(data.token);
          }
          if (data.claims?.role) {
            setPrincipalRole(data.claims.role);
          }
        }
      } catch {
        // Backend unreachable
      }
    },
    [backendOrigin]
  );

  const loadAllRealBackendData = useCallback(async () => {
    try {
      const headers = buildAuthHeaders();
      const [
        stateResp,
        topoResp,
        finopsResp,
        queuesResp,
        fedResp,
        dagResp,
        bpResp,
        rtResp,
        cfgResp,
        profResp,
        swarmResp,
        appSagaResp,
        secResp,
        obsResp,
        celldResp,
      ] = await Promise.all([
        fetch(`${backendOrigin}/v1beta1/management/state`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/topology`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/finops`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/queues`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/federation`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/dag`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/tasks/blueprints`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/runtime`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/configs`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/profiler`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/swarm`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/approvals/saga`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/security`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/observability`, { headers }),
        fetch(`${backendOrigin}/v1beta1/console/celld-dev-status`, { headers }),
      ]);

      if (!stateResp.ok || !topoResp.ok) {
        setLiveConnected(false);
        return;
      }

      setMgmtState((await stateResp.json()) as ManagementStatePayload);
      setTopology((await topoResp.json()) as DynamicClusterTopology);
      if (finopsResp.ok) setFinopsData((await finopsResp.json()) as Record<string, unknown>);
      if (queuesResp.ok) setQueuesData((await queuesResp.json()) as Record<string, unknown>);
      if (fedResp.ok) setFederationData((await fedResp.json()) as Record<string, unknown>);
      if (dagResp.ok) setDagData((await dagResp.json()) as Record<string, unknown>);
      if (bpResp.ok) setBlueprintsData((await bpResp.json()) as Record<string, unknown>);
      if (rtResp.ok) setRuntimeData((await rtResp.json()) as Record<string, unknown>);
      if (cfgResp.ok) setConfigsData((await cfgResp.json()) as Record<string, unknown>);
      if (profResp.ok) setProfilerData((await profResp.json()) as Record<string, unknown>);
      if (swarmResp.ok) setSwarmData((await swarmResp.json()) as Record<string, unknown>);
      if (appSagaResp.ok) setApprovalsSagaData((await appSagaResp.json()) as Record<string, unknown>);
      if (secResp.ok) setSecurityData((await secResp.json()) as Record<string, unknown>);
      if (obsResp.ok) setObservabilityData((await obsResp.json()) as Record<string, unknown>);
      if (celldResp.ok) setCelldStatusData((await celldResp.json()) as Record<string, unknown>);
      setLiveConnected(true);
    } catch {
      setLiveConnected(false);
    }
  }, [backendOrigin, buildAuthHeaders]);

  const callMutationApi = useCallback(
    async (path: string, method: 'POST' | 'GET' | 'DELETE', payload?: Record<string, unknown>) => {
      try {
        const resp = await fetch(`${backendOrigin}${path}`, {
          method,
          headers: buildAuthHeaders(),
          body: method !== 'GET' && payload !== undefined ? JSON.stringify(payload) : undefined,
        });
        const data = (await resp.json()) as Record<string, unknown>;
        setLastApiResult(JSON.stringify({ status: resp.status, path, response: data }, null, 2));
        await loadAllRealBackendData();
        return { status: resp.status, data };
      } catch (err) {
        const offlinePayload = {
          status: 503,
          error: 'BACKEND_UNREACHABLE',
          path,
          message: (err as Error).message,
        };
        setLastApiResult(JSON.stringify(offlinePayload, null, 2));
        setLiveConnected(false);
        return { status: 503, data: offlinePayload };
      }
    },
    [backendOrigin, buildAuthHeaders, loadAllRealBackendData]
  );

  useEffect(() => {
    if (typeof window !== 'undefined' && (window.location.protocol === 'http:' || window.location.protocol === 'https:')) {
      setBackendOrigin(window.location.origin);
    }
  }, []);

  useEffect(() => {
    void issueLiveJwt(principalId);
  }, [principalId, issueLiveJwt]);

  useEffect(() => {
    void loadAllRealBackendData();
    const tid = setInterval(() => {
      void loadAllRealBackendData();
    }, 5000);
    return () => clearInterval(tid);
  }, [loadAllRealBackendData]);

  // Real persistent SSE subscription (/v1beta1/management/events/stream?live=1)
  useEffect(() => {
    if (typeof EventSource === 'undefined') return;
    let es: EventSource | null = null;
    try {
      es = new EventSource(`${backendOrigin}/v1beta1/management/events/stream?live=1`);
      es.onopen = () => setSseConnected(true);
      es.onerror = () => setSseConnected(false);
      es.addEventListener('management.audit', () => {
        void loadAllRealBackendData();
      });
    } catch {
      setSseConnected(false);
    }
    return () => {
      if (es) es.close();
    };
  }, [backendOrigin, loadAllRealBackendData]);

  // Actions backed 100% by real backend endpoints
  const registerDynamicNode = async (role: 'brain' | 'body' | 'gateway') => {
    const roleCounts = topology?.countsByRole;
    const currentRoleCount = roleCounts ? roleCounts[role] : 0;
    const nextIdx = currentRoleCount + 1;
    const nodeId =
      role === 'brain'
        ? `celld-brain-0${nextIdx}`
        : role === 'gateway'
        ? `celld-gw-0${nextIdx}`
        : `body-host-0${nextIdx}`;
    const port = role === 'brain' ? 18081 + nextIdx : role === 'gateway' ? 18085 + nextIdx : 19500 + nextIdx;
    await callMutationApi('/internal/v1beta1/cluster/register', 'POST', {
      nodeId,
      role,
      region: 'us-west-2',
      baseUrl: `http://127.0.0.1:${port}`,
      registeredVia: 'SELF_REG',
      actor: principalId,
    });
  };

  const inspectTaskTurns = async (taskId: string) => {
    const resp = await fetch(`${backendOrigin}/v1beta1/management/tasks/${encodeURIComponent(taskId)}/turns`, {
      headers: buildAuthHeaders(),
    });
    if (resp.ok) {
      const detail = (await resp.json()) as Record<string, unknown>;
      setSelectedTaskTurns(detail);
      setLastApiResult(JSON.stringify(detail, null, 2));
    }
  };

  const loadLayerFsFile = async (branchName: string, filePath: string) => {
    const resp = await fetch(
      `${backendOrigin}/v1beta1/console/layerfs/file?branch=${encodeURIComponent(branchName)}&filePath=${encodeURIComponent(filePath)}`,
      { headers: buildAuthHeaders() }
    );
    const data = (await resp.json()) as Record<string, unknown>;
    setLayerfsFileView(data);
    setLastApiResult(JSON.stringify(data, null, 2));
  };

  const queryAuditLogsFromBackend = async () => {
    const resp = await fetch(
      `${backendOrigin}/v1beta1/management/audits?q=${encodeURIComponent(auditSearchQuery)}`,
      { headers: buildAuthHeaders() }
    );
    if (resp.ok) {
      const data = (await resp.json()) as Record<string, unknown>;
      setLastApiResult(JSON.stringify(data, null, 2));
    }
  };

  const slotDist = topology?.slotDistribution ?? {};
  const brainKeys = Object.keys(slotDist);
  const slotOwners = topology?.slotOwners ?? [];
  const allClusterNodes: DynamicClusterNode[] = [
    ...(topology?.brains ?? []),
    ...(topology?.bodies ?? []),
    ...(topology?.gateways ?? []),
  ];

  const navBadges: Record<string, string> = {
    overview: topology ? `${topology.virtualSlots} 槽` : '',
    tenants: mgmtState ? `${mgmtState.tenants.length}` : '',
    nodes: topology ? `${topology.totalRegisteredNodes}` : '',
    tasks: mgmtState ? `${mgmtState.tasks.length}` : '',
    models: runtimeData && Array.isArray(runtimeData.cognitiveRouterEndpoints)
      ? `${(runtimeData.cognitiveRouterEndpoints as unknown[]).length}`
      : '',
    audit: mgmtState
      ? `${mgmtState.approvals.filter((a) => a.status.startsWith('PENDING')).length} 待批`
      : '',
    celld: celldStatusData ? String(celldStatusData.status ?? '') : '',
  };

  return (
    <HruiShell
      activeNav={activeNav}
      onSelectNav={setActiveNav}
      darkTheme={darkTheme}
      onToggleTheme={() => setDarkTheme((d) => !d)}
      backendOrigin={backendOrigin}
      onChangeOrigin={setBackendOrigin}
      liveConnected={liveConnected}
      sseConnected={sseConnected}
      principalId={principalId}
      principalRole={principalRole}
      onSelectPrincipal={setPrincipalId}
      navBadges={navBadges}
      onRefresh={() => void loadAllRealBackendData()}
    >
      {!liveConnected ? (
        <div className="state-banner danger">
          <div>
            <strong>后端 API 未连接或处于 503 离线状态 ({backendOrigin})</strong>
            <div className="muted">
              零打桩模式已开启：前端拒绝渲染任何静态伪造数据或本地 fake-200 镜像，所有视图严格等待真实后端响应。
            </div>
          </div>
          <button
            type="button"
            className="btn primary small"
            onClick={async () => {
              await fetch(`${backendOrigin}/v1beta1/management/state?recover=1`);
              await loadAllRealBackendData();
            }}
          >
            恢复后端连接 (?recover=1)
          </button>
        </div>
      ) : null}

      {/* ======================= TAB 1: OVERVIEW ======================= */}
      {activeNav === 'overview' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">HRUI DESIGN SYSTEM · NEXT.JS APP ROUTER (UI/) · ZERO-STUB BFF</div>
              <h1>ANSTS 平台控制面与动态拓扑总览</h1>
              <p className="muted">
                全部数据实时读取自 <code>GET /v1beta1/management/state</code>、<code>GET /v1beta1/console/topology</code> 与 <code>GET /v1beta1/console/health</code>。
              </p>
            </div>
            <div className="btn-row">
              <button type="button" className="btn primary small" onClick={() => void registerDynamicNode('brain')}>
                + 自注册新 Brain
              </button>
              <button type="button" className="btn small" onClick={() => void registerDynamicNode('body')}>
                + 自注册新 Body
              </button>
              <button type="button" className="btn small" onClick={() => void registerDynamicNode('gateway')}>
                + 自注册新 Gateway
              </button>
            </div>
          </div>

          <section className="metrics kpi-grid" aria-label="核心态势指标">
            <div className="metric kpi-card">
              <small>
                <span>注册节点总数 (v25_cluster_registry)</span>
                <span className="pill ok">AUTO-REG</span>
              </small>
              <b className="num">{topology ? topology.totalRegisteredNodes : '—'}</b>
              <div className="sub mono">
                {topology
                  ? `Brain:${topology.countsByRole.brain} · Body:${topology.countsByRole.body} · GW:${topology.countsByRole.gateway}`
                  : '等待后端同步...'}
              </div>
            </div>
            <div className="metric kpi-card">
              <small>
                <span>256-Slot 一致性哈希环</span>
                <span className="pill run">Gen #{topology ? topology.topologyGeneration : '—'}</span>
              </small>
              <b className="num">{brainKeys.length} 分片</b>
              <div className="sub mono">
                {Object.entries(slotDist)
                  .map(([k, v]) => `${k.replace('brain-node-', 'B')}:${v}`)
                  .join(' / ') || '无分片数据'}
              </div>
            </div>
            <div className="metric kpi-card">
              <small>
                <span>P0 调度延迟与集群负载 (CapacitySlaEnforcer)</span>
                <span className={`pill ${mgmtState?.sla?.compliant ? 'ok' : 'bad'}`}>
                  {mgmtState?.sla ? (mgmtState.sla.compliant ? 'SLA COMPLIANT' : 'SLA BREACH') : '—'}
                </span>
              </small>
              <b className="num">{mgmtState?.sla ? `${mgmtState.sla.p0QueueWaitP99Ms} ms` : '—'}</b>
              <div className="sub mono">
                {mgmtState?.sla
                  ? `利用率: ${(mgmtState.sla.utilizationRatio * 100).toFixed(1)}% · WarmHit: ${(mgmtState.sla.warmPoolHitRatio * 100).toFixed(1)}%`
                  : '等待后端同步...'}
              </div>
            </div>
            <div className="metric kpi-card">
              <small>
                <span>活跃租户与分布式任务 (SQLite RPO=0)</span>
                <span className="pill ok">{mgmtState ? mgmtState.gatewayNodeId : '—'}</span>
              </small>
              <b className="num">{mgmtState ? `${mgmtState.tasks.length} 任务` : '—'}</b>
              <div className="sub mono">
                {mgmtState
                  ? `租户: ${mgmtState.tenants.length} · 审批单: ${mgmtState.approvals.length}`
                  : '等待后端同步...'}
              </div>
            </div>
          </section>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">256-Slot 动态分片哈希环 (GET /v1beta1/console/topology)</h2>
                <p className="panel-subtitle">
                  {Object.entries(slotDist)
                    .map(([k, v]) => `${k}: ${v} 槽`)
                    .join(' · ') || '等待后端返回分片归属...'}
                </p>
              </div>
            </div>
            <div className="slot-ring-grid">
              {slotOwners.map((owner, idx) => {
                const bIdx = Math.max(0, brainKeys.indexOf(owner));
                const cls = bIdx === 0 ? 'b1' : bIdx === 1 ? 'b2' : 'b3';
                return <div key={idx} className={`slot-box ${cls}`} title={`Slot #${idx} → ${owner}`} />;
              })}
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">多地域主权路由与实时 SLA 矩阵 (POST /v1beta1/management/canary)</h2>
                <p className="panel-subtitle">实时聚合各 Region 注册节点 CPU、PSI 与 GDPR 数据主权状态。</p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Region</th>
                    <th>数据主权域</th>
                    <th>Brain 分片摘要</th>
                    <th>节点数</th>
                    <th>平均 CPU / PSI</th>
                    <th>复制拓扑</th>
                    <th>SLO 状态</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.regions ?? []).map((reg) => (
                    <tr key={reg.regionId}>
                      <td className="mono">{reg.regionId}</td>
                      <td className="mono">{reg.jurisdiction}</td>
                      <td className="mono">{reg.brainSummary}</td>
                      <td className="mono">{reg.nodeCount}</td>
                      <td className="mono">
                        {reg.avgCpuPct}% / {reg.avgPsiPct}%
                      </td>
                      <td className="mono">{reg.replicationLabel}</td>
                      <td>
                        <span className={`pill ${reg.capacityAvailable ? 'ok' : 'bad'}`}>{reg.sloStatus}</span>
                      </td>
                      <td>
                        <button
                          type="button"
                          className="btn small"
                          onClick={() =>
                            void callMutationApi('/v1beta1/management/canary', 'POST', {
                              action: 'toggle_region',
                              regionId: reg.regionId,
                              capacityAvailable: !reg.capacityAvailable,
                            })
                          }
                        >
                          {reg.capacityAvailable ? '演练故障排水' : '恢复可用区'}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}

      {/* ======================= TAB 2: TENANTS, FINOPS & QUEUES ======================= */}
      {activeNav === 'tenants' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">SQLITE V25_MGMT_TENANTS · FINOPS BUDGET CONTROLLER · DRF BINARY MAX-HEAP</div>
              <h1>多租户配额、FinOps 成本归因与 DRF 优先级队列工作台</h1>
              <p className="muted">
                全部读写操作实时对接 <code>POST /v1beta1/management/tenants/config</code>、<code>POST /v1beta1/management/finops/action</code> 与 <code>POST /v1beta1/management/queues/action</code>。
              </p>
            </div>
            <div className="btn-row">
              <input
                type="text"
                placeholder="新租户名 (如 Orion)"
                value={newTenantId}
                onChange={(e) => setNewTenantId(e.target.value)}
                style={{ width: 140 }}
              />
              <input
                type="number"
                value={newTenantMaxSlots}
                onChange={(e) => setNewTenantMaxSlots(Number(e.target.value))}
                style={{ width: 90 }}
              />
              <input
                type="number"
                value={newTenantBudgetUsd}
                onChange={(e) => setNewTenantBudgetUsd(Number(e.target.value))}
                style={{ width: 100 }}
              />
              <button
                type="button"
                className="btn primary small"
                onClick={() => {
                  if (!newTenantId.trim()) return;
                  void callMutationApi('/v1beta1/management/tenants/config', 'POST', {
                    action: 'create',
                    tenantId: newTenantId.trim(),
                    region: 'us-west-2',
                    tier: 'P1_ENTERPRISE',
                    maxSlots: newTenantMaxSlots,
                    dailyBudgetUsd: newTenantBudgetUsd,
                    expectedVersion: 0,
                  });
                  setNewTenantId('');
                }}
              >
                + 创建租户 (SQLite)
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">租户配额与乐观锁 CAS 治理表 (v25_mgmt_tenants)</h2>
                <p className="panel-subtitle">所有修改强制校验 CAS expectedVersion，拒绝覆盖并发提交。</p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>租户 ID</th>
                    <th>Region / Tier</th>
                    <th>已用 / 最大并发槽</th>
                    <th>DAG 预留槽</th>
                    <th>今日消费 / 预算</th>
                    <th>预算状态</th>
                    <th>CAS 版本</th>
                    <th>真实后端写操作</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.tenants ?? []).map((t) => (
                    <tr key={t.id}>
                      <td className="mono">
                        <strong>{t.id}</strong>
                      </td>
                      <td className="mono">
                        {t.region} · {t.tier}
                      </td>
                      <td className="mono">
                        {t.usedSlots} / {t.maxSlots}
                      </td>
                      <td className="mono">{t.escrowReserved}</td>
                      <td className="mono">
                        ${t.spentUsd ?? 0} / ${t.dailyBudgetUsd}
                      </td>
                      <td>
                        <span className={`pill ${t.budgetStatus === 'OK' ? 'ok' : t.budgetStatus === 'SOFT_DOWNGRADED' ? 'warn' : 'bad'}`}>
                          {t.budgetStatus}
                        </span>
                      </td>
                      <td className="mono">v{t.configVersion}</td>
                      <td>
                        <div className="btn-row">
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/tenants/config', 'POST', {
                                tenantId: t.id,
                                expectedVersion: t.configVersion,
                                maxSlots: t.maxSlots + 20,
                                dailyBudgetUsd: t.dailyBudgetUsd + 200,
                              })
                            }
                          >
                            CAS 扩容 (+20槽)
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/finops/action', 'POST', {
                                action: 'simulate_spend_step',
                                tenantId: t.id,
                                amountUsd: 150,
                              })
                            }
                          >
                            计费步进 (+$150)
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/finops/action', 'POST', {
                                action: 'reset_tenant_spend',
                                tenantId: t.id,
                              })
                            }
                          >
                            重置消费
                          </button>
                          <button
                            type="button"
                            className="btn danger small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/tenants/config', 'POST', {
                                action: 'delete',
                                tenantId: t.id,
                              })
                            }
                          >
                            删除租户
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">DRF 二叉最大堆优先级队列与死信重放 (GET /v1beta1/console/queues)</h2>
                <p className="panel-subtitle">
                  当前排队总数: <code className="mono">{String(queuesData?.totalQueuedCount ?? 0)}</code> · 活跃租户堆:{' '}
                  <code className="mono">{String(queuesData?.activeTenantHeapsCount ?? 0)}</code> · FinOps 前缀缓存节省:{' '}
                  <code className="mono">${String(finopsData?.totalPrefixSavedUsd ?? 0)}</code>
                </p>
              </div>
              <div className="btn-row">
                <button
                  type="button"
                  className="btn primary small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/queues/action', 'POST', {
                      action: 'enqueue_multi_tenant_burst',
                    })
                  }
                >
                  + 压入多租户并发队列突发
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/queues/action', 'POST', {
                      action: 'pop_and_schedule_next',
                    })
                  }
                >
                  弹出堆顶任务并调度
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/queues/dead-letter/replay', 'POST', {
                      targetPriority: 0,
                    })
                  }
                >
                  重放全部 DLQ 死信任务
                </button>
              </div>
            </div>
          </div>
        </>
      ) : null}

      {/* ======================= TAB 3: NODES, TUNNELS & DR ======================= */}
      {activeNav === 'nodes' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">DYNAMIC CLUSTER REGISTRY · MTLS REVERSE TUNNEL · RS(4,2) ERASURE WAL</div>
              <h1>全角色动态拓扑注册表、节点批操作与跨地域容灾工作台</h1>
              <p className="muted">
                实时对接 <code>v25_cluster_registry</code>、<code>POST /internal/v1beta1/cluster/register</code>、<code>POST /v1beta1/management/nodes/scale</code> 与 <code>POST /v1beta1/management/federation/action</code>。
              </p>
            </div>
            <div className="btn-row">
              <button type="button" className="btn primary small" onClick={() => void registerDynamicNode('brain')}>
                + 扩容 Brain 分片节点
              </button>
              <button type="button" className="btn primary small" onClick={() => void registerDynamicNode('body')}>
                + 扩容 Body 沙箱节点
              </button>
              <button type="button" className="btn primary small" onClick={() => void registerDynamicNode('gateway')}>
                + 扩容 Gateway 接入节点
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/topology/evict-expired', 'POST', {
                    ttlMs: 15000,
                  })
                }
              >
                扫描并摘除失联节点 (TTL)
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">动态注册集群节点列表 (Brain / Body / Gateway)</h2>
                <p className="panel-subtitle">
                  Ring Digest: <code className="mono">{topology?.ringDigest ?? '—'}</code> · Generation:{' '}
                  <code className="mono">#{topology?.topologyGeneration ?? '—'}</code>
                </p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>节点 ID</th>
                    <th>角色 (Role)</th>
                    <th>Region</th>
                    <th>Base URL</th>
                    <th>槽位 / 容量</th>
                    <th>CPU / PSI</th>
                    <th>注册方式 / Epoch</th>
                    <th>状态</th>
                    <th>真实后端控制操作</th>
                  </tr>
                </thead>
                <tbody>
                  {allClusterNodes.map((n) => (
                    <tr key={n.nodeId}>
                      <td className="mono">
                        <strong>{n.nodeId}</strong>
                      </td>
                      <td>
                        <span className="pill run">{n.role.toUpperCase()}</span>
                      </td>
                      <td className="mono">{n.region}</td>
                      <td className="mono">{n.baseUrl || 'mTLS Reverse Tunnel'}</td>
                      <td className="mono">
                        {n.role === 'brain' ? `${n.ownedSlots} 哈希槽` : `${n.capacitySlots} 并发槽`}
                      </td>
                      <td className="mono">
                        {n.cpuPct}% / {n.psiPct}%
                      </td>
                      <td className="mono">
                        {n.registeredVia} · ep#{n.leaseEpoch}
                      </td>
                      <td>
                        <span className={`pill ${n.status === 'READY' ? 'ok' : 'warn'}`}>{n.status}</span>
                      </td>
                      <td>
                        <div className="btn-row">
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/internal/v1beta1/cluster/heartbeat', 'POST', {
                                nodeId: n.nodeId,
                                role: n.role,
                              })
                            }
                          >
                            真实内核心跳采样
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/nodes/batch', 'POST', {
                                operation: 'prewarm',
                                nodeIds: [n.nodeId],
                              })
                            }
                          >
                            预热沙箱
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/nodes/batch', 'POST', {
                                operation: n.status === 'CORDONED' ? 'uncordon' : 'cordon',
                                nodeIds: [n.nodeId],
                              })
                            }
                          >
                            {n.status === 'CORDONED' ? 'Uncordon 恢复' : 'Cordon 封锁'}
                          </button>
                          <button
                            type="button"
                            className="btn danger small"
                            onClick={() =>
                              void callMutationApi('/internal/v1beta1/cluster/deregister', 'POST', {
                                nodeId: n.nodeId,
                                reason: 'CONSOLE_DECOMMISSION',
                              })
                            }
                          >
                            下线摘除
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">双归属 mTLS 反向隧道与 RS(K=4, M=2) 跨地域纠删码灾备 (GET /v1beta1/console/federation)</h2>
                <p className="panel-subtitle">
                  反向隧道绑定数:{' '}
                  <code className="mono">
                    {String((federationData?.reverseTunnels as Record<string, unknown>)?.totalBindingsCount ?? 0)}
                  </code>{' '}
                  · 纠删码清单数:{' '}
                  <code className="mono">
                    {String((federationData?.erasureCoding as Record<string, unknown>)?.manifestsCount ?? 0)}
                  </code>
                </p>
              </div>
              <div className="btn-row">
                <button
                  type="button"
                  className="btn primary small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/federation/action', 'POST', {
                      action: 'encode_erasure_wal',
                      walId: `wal-rs-${Date.now()}`,
                      payloadText: 'ANSTS RPO=0 Cross-Region Erasure Coded WAL Segment',
                    })
                  }
                >
                  编码新 RS(4,2) WAL 段
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/federation/action', 'POST', {
                      action: 'simulate_dual_region_outage_rebuild',
                      droppedShardIndices: [1, 4],
                    })
                  }
                >
                  双可用区损毁无损重构校验
                </button>
              </div>
            </div>
          </div>
        </>
      ) : null}

      {/* ======================= TAB 4: TASKS, WAL & LAYERFS ======================= */}
      {activeNav === 'tasks' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">CORESCHEDULER5D · TURN WAL CRC32 · LAYERFS 2.0 COW WORKSPACE</div>
              <h1>分布式任务调度、Time-Travel 回合分叉与 LayerFS 工作台</h1>
              <p className="muted">
                对接 <code>POST /v1beta1/tasks/dispatch</code>、<code>POST /v1beta1/management/tasks/time-travel</code>、<code>POST /v1beta1/management/tasks/batch-plan</code> 与 <code>GET /v1beta1/console/layerfs/file</code>。
              </p>
            </div>
            <div className="btn-row">
              <select value={newTaskTenant} onChange={(e) => setNewTaskTenant(e.target.value)}>
                {(mgmtState?.tenants ?? []).map((t) => (
                  <option key={t.id} value={t.id}>
                    {t.id}
                  </option>
                ))}
              </select>
              <input
                type="text"
                value={newTaskPrompt}
                onChange={(e) => setNewTaskPrompt(e.target.value)}
                placeholder="输入分布式任务 Prompt..."
                style={{ width: 220 }}
              />
              <button
                type="button"
                className="btn primary small"
                onClick={() =>
                  void callMutationApi('/v1beta1/tasks/dispatch', 'POST', {
                    taskId: `task-${newTaskTenant.toLowerCase()}-${(mgmtState?.tasks.length ?? 0) + 901}`,
                    tenantId: newTaskTenant,
                    priority: 1,
                    promptText: newTaskPrompt,
                  })
                }
              >
                + 派发分布式任务
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/tasks/batch-plan', 'POST', {
                    mode: 'commit_and_dispatch',
                    blueprintId: 'bp-fullstack-release',
                    tenantId: newTaskTenant,
                  })
                }
              >
                一键派发 DAG 蓝图批次 ({Array.isArray(blueprintsData?.blueprints) ? blueprintsData.blueprints.length : 0} 模板)
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">全局分布式任务与 Fencing 租约表 (GET /v1beta1/management/state)</h2>
                <p className="panel-subtitle">
                  活跃 DAG 担保组: <code className="mono">{String(dagData?.totalEscrowsCount ?? 0)}</code> · 依赖边:{' '}
                  <code className="mono">{String(dagData?.totalEdgesCount ?? 0)}</code>
                </p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>任务 ID</th>
                    <th>租户 / 优先级</th>
                    <th>Slot → Brain / Body</th>
                    <th>Fencing / Turn</th>
                    <th>LayerFS 分支</th>
                    <th>状态</th>
                    <th>真实后端任务操作</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.tasks ?? []).map((t) => (
                    <tr key={t.id}>
                      <td className="mono">
                        <strong>{t.id}</strong>
                      </td>
                      <td className="mono">
                        {t.tenant} · <span className="pill run">{t.priority}</span>
                      </td>
                      <td className="mono">
                        #{t.shardSlot} → {t.brain} / {t.body}
                      </td>
                      <td className="mono">
                        fence={t.fence} · turn={t.turn}
                      </td>
                      <td className="mono">{t.branch}</td>
                      <td>
                        <span
                          className={`pill ${
                            t.state === 'RUNNING_ACT' || t.state === 'COMPLETED'
                              ? 'ok'
                              : t.state === 'DEAD_LETTER' || t.state === 'FENCED_TERMINATED'
                              ? 'bad'
                              : 'warn'
                          }`}
                        >
                          {t.state}
                        </span>
                      </td>
                      <td>
                        <div className="btn-row">
                          <button type="button" className="btn small" onClick={() => void inspectTaskTurns(t.id)}>
                            检视 CRC32 WAL
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/tasks/step', 'POST', {
                                taskId: t.id,
                                toolName: 'shell_exec',
                                executeRealBash: true,
                              })
                            }
                          >
                            +推进 Turn
                          </button>
                          <button
                            type="button"
                            className="btn small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/tasks/time-travel', 'POST', {
                                taskId: t.id,
                                targetTurnIndex: Math.max(1, t.turn - 1),
                              })
                            }
                          >
                            Time-Travel 分叉
                          </button>
                          <button
                            type="button"
                            className="btn danger small"
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/tasks/cancel', 'POST', {
                                taskId: t.id,
                                mode: t.state === 'CANCEL_UNCONFIRMED' ? 'force_fence' : 'graceful',
                              })
                            }
                          >
                            {t.state === 'CANCEL_UNCONFIRMED' ? '强制 Fencing 终止' : '优雅停止'}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {selectedTaskTurns ? (
            <div className="panel card">
              <div className="panel-header">
                <div>
                  <h2 className="panel-title">
                    Turn WAL CRC32 帧检视器 · {String(selectedTaskTurns.taskId ?? '')}
                  </h2>
                </div>
              </div>
              <pre className="code-block mono">{JSON.stringify(selectedTaskTurns, null, 2)}</pre>
            </div>
          ) : null}

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">LayerFS CoW 分支与行级 Blame 文件浏览器 (GET /v1beta1/console/layerfs/file)</h2>
                <p className="panel-subtitle">实时读取 LayerFS 内容寻址存储中的文件内容与行级提交溯源。</p>
              </div>
              <div className="btn-row">
                <input
                  type="text"
                  className="mono"
                  value={layerfsBranchInput}
                  onChange={(e) => setLayerfsBranchInput(e.target.value)}
                  placeholder="分支名 (main)"
                  style={{ width: 130 }}
                />
                <input
                  type="text"
                  className="mono"
                  value={layerfsFileInput}
                  onChange={(e) => setLayerfsFileInput(e.target.value)}
                  placeholder="文件路径 (package.json)"
                  style={{ width: 160 }}
                />
                <button
                  type="button"
                  className="btn primary small"
                  onClick={() => void loadLayerFsFile(layerfsBranchInput, layerfsFileInput)}
                >
                  读取文件与 Blame
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/runtime/layerfs-branch', 'POST', {
                      action: 'fork_branch',
                      sourceBranch: 'main',
                      branchName: `feat-cow-${(mgmtState?.tasks.length ?? 0) + 101}`,
                    })
                  }
                >
                  + 零拷贝分叉新分支
                </button>
              </div>
            </div>
            {layerfsFileView ? (
              <pre className="code-block mono">{JSON.stringify(layerfsFileView, null, 2)}</pre>
            ) : null}
          </div>
        </>
      ) : null}

      {/* ======================= TAB 5: MODELS, CONFIGS & PROFILER ======================= */}
      {activeNav === 'models' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">COGNITIVE ROUTER · VERSIONED CONFIG CENTER · BAYESIAN EWMA PROFILER</div>
              <h1>模型路由池、版本化配置中心与算力画像工作台</h1>
              <p className="muted">
                实时对接 <code>GET /v1beta1/console/runtime</code>、<code>GET /v1beta1/console/configs</code>、<code>GET /v1beta1/console/profiler</code> 与 <code>GET /v1beta1/console/swarm</code>。
              </p>
            </div>
            <div className="btn-row">
              <button
                type="button"
                className="btn primary small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/canary', 'POST', {
                    action: 'promote_step',
                  })
                }
              >
                Canary 灰度推进 ({mgmtState?.canary?.trafficWeightPct ?? 0}%)
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/canary', 'POST', {
                    action: 'reset_canary',
                  })
                }
              >
                重置金丝雀权重
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/router/inference-pipeline', 'POST', {
                    action: 'execute_sse_inference',
                    tenantId: 'Northstar',
                  })
                }
              >
                触发真实 SSE 推理路由
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">CognitiveRouter 真实模型端点池 (GET /v1beta1/console/runtime)</h2>
                <p className="panel-subtitle">所有端点均由后端 CognitiveRouter 实例直接管理，零硬编码假对象。</p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Endpoint ID</th>
                    <th>Provider / 模型</th>
                    <th>TTFT EWMA</th>
                    <th>熔断器状态</th>
                    <th>剩余 RPM / TPM</th>
                    <th>真实后端动作</th>
                  </tr>
                </thead>
                <tbody>
                  {((runtimeData?.cognitiveRouterEndpoints as Array<Record<string, unknown>>) ?? []).map((ep) => (
                    <tr key={String(ep.endpointId)}>
                      <td className="mono">
                        <strong>{String(ep.endpointId)}</strong>
                      </td>
                      <td className="mono">
                        {String(ep.provider)} / {String(ep.modelName)}
                      </td>
                      <td className="mono">{String(ep.ttftEwmaMs)} ms</td>
                      <td>
                        <span className={`pill ${ep.healthy ? 'ok' : 'bad'}`}>{String(ep.circuitState)}</span>
                      </td>
                      <td className="mono">
                        {String(ep.rpmRemaining)} / {String(ep.tpmRemaining)}
                      </td>
                      <td>
                        <button
                          type="button"
                          className="btn small"
                          onClick={() =>
                            void callMutationApi('/v1beta1/management/router/inference-pipeline', 'POST', {
                              action: 'trip_and_recover_circuit_breaker',
                              endpointId: String(ep.endpointId),
                            })
                          }
                        >
                          演练熔断与半开探测恢复
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">
                  版本化动态配置中心 · Rev #{String(configsData?.activeRevision ?? '—')} (GET /v1beta1/console/configs)
                </h2>
                <p className="panel-subtitle">
                  SHA-256 Checksum: <code className="mono">{String(configsData?.sha256Checksum ?? '—')}</code> · Policy-as-Code 规则数:{' '}
                  <code className="mono">
                    {String((swarmData?.formalGuardrail as Record<string, unknown>)?.totalRulesCount ?? 0)}
                  </code>
                </p>
              </div>
              <div className="btn-row">
                <button
                  type="button"
                  className="btn primary small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/configs/dry-run', 'POST', {
                      patch: { maxCellConcurrentRequests: 96, actCpuCores: 6.0 },
                    })
                  }
                >
                  Dry-Run 爆炸半径评估
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/config/center', 'POST', {
                      action: 'validate_and_apply_config',
                      expectedRevision: Number(configsData?.activeRevision ?? 1),
                      runtimeOverrides: { maxCellConcurrentRequests: 96 },
                      note: 'Updated via Next.js hrui console',
                    })
                  }
                >
                  CAS 提交新配置版本
                </button>
                <button
                  type="button"
                  className="btn small"
                  onClick={() =>
                    void callMutationApi('/v1beta1/management/profiler/action', 'POST', {
                      action: 'predict_and_prewarm_burst',
                      observedArrivalsPerSec: 28,
                      avgSandboxHoldSec: 3.5,
                    })
                  }
                >
                  EWMA 预测预热 ({String((profilerData?.predictivePrewarmer as Record<string, unknown>)?.ewmaArrivalRatePerSec ?? 0)} req/s)
                </button>
              </div>
            </div>
          </div>
        </>
      ) : null}

      {/* ======================= TAB 6: 4-EYES APPROVALS, AUDIT & OBSERVABILITY ======================= */}
      {activeNav === 'audit' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">4-EYES APPROVAL GATE · SAGA LIFO COMPENSATOR · SHA-256 AUDIT CHAIN · W3C TRACE</div>
              <h1>高危双人复核、Saga 逆序回滚与全链路可观测工作台</h1>
              <p className="muted">
                实时对接 <code>GET /v1beta1/console/approvals/saga</code>、<code>GET /v1beta1/console/security</code>、<code>GET /v1beta1/console/observability</code> 与 <code>GET /v1beta1/management/audits</code>。
              </p>
            </div>
            <div className="btn-row">
              <button
                type="button"
                className="btn primary small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/approvals/create', 'POST', {
                    type: 'QUOTA_OVERRIDE',
                    target: 'Northstar: maxSlots -> 240',
                    region: 'us-west-2',
                    tenant: 'Northstar',
                    risk: 'HIGH',
                  })
                }
              >
                + 发起高危 4-Eyes 审批单
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/approvals/saga-action', 'POST', {
                    action: 'stage_high_risk_effect',
                    taskId: 'task-ns-8841',
                    toolName: 'sql_schema_drop_column',
                  })
                }
              >
                暂存高危工具副作用 (Staging)
              </button>
              <button
                type="button"
                className="btn small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/security/probe', 'POST', {
                    probeType: 'ssrf_imds_hex',
                  })
                }
              >
                触发零信任 SSRF/IMDS 阻断探针
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">4-Eyes 异体双人复核单据表 (v25_mgmt_approvals)</h2>
                <p className="panel-subtitle">
                  强制校验发起人 (initiator) ≠ 审批人 (approver)；切换顶部身份至 <code>zhou</code> 可复核由 <code>chen</code> 发起的审批单。
                </p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>审批单号</th>
                    <th>类型 / 风险</th>
                    <th>目标对象</th>
                    <th>Region / 租户</th>
                    <th>发起人</th>
                    <th>状态 / 复核人</th>
                    <th>真实后端复核操作</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.approvals ?? []).map((a) => (
                    <tr key={a.id}>
                      <td className="mono">
                        <strong>{a.id}</strong>
                      </td>
                      <td className="mono">
                        {a.type} · <span className="pill warn">{a.risk}</span>
                      </td>
                      <td className="mono">{a.target}</td>
                      <td className="mono">
                        {a.region} / {a.tenant}
                      </td>
                      <td className="mono">{a.initiator}</td>
                      <td>
                        <span
                          className={`pill ${
                            a.status.startsWith('APPROVED')
                              ? 'ok'
                              : a.status.startsWith('REJECTED')
                              ? 'bad'
                              : 'warn'
                          }`}
                        >
                          {a.status} {a.decidedBy ? `(${a.decidedBy})` : ''}
                        </span>
                      </td>
                      <td>
                        <div className="btn-row">
                          <button
                            type="button"
                            className="btn primary small"
                            disabled={!a.status.startsWith('PENDING')}
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/approvals/decide', 'POST', {
                                approvalId: a.id,
                                decision: 'approve',
                              })
                            }
                          >
                            异体批准 (Approve)
                          </button>
                          <button
                            type="button"
                            className="btn danger small"
                            disabled={!a.status.startsWith('PENDING')}
                            onClick={() =>
                              void callMutationApi('/v1beta1/management/approvals/decide', 'POST', {
                                approvalId: a.id,
                                decision: 'reject',
                              })
                            }
                          >
                            驳回 (Reject)
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">
                  SHA-256 哈希链审计日志与 W3C Trace 可观测性 (GET /v1beta1/management/audits)
                </h2>
                <p className="panel-subtitle">
                  审计链完整性:{' '}
                  <code className="mono">
                    {String((securityData?.auditChainIntegrity as Record<string, unknown>)?.valid ?? true)}
                  </code>{' '}
                  · 暂存副作用数:{' '}
                  <code className="mono">
                    {String((approvalsSagaData?.stagingStats as Record<string, unknown>)?.totalEffects ?? 0)}
                  </code>{' '}
                  · W3C Trace 瀑布流数:{' '}
                  <code className="mono">
                    {Array.isArray(observabilityData?.traceWaterfalls) ? observabilityData.traceWaterfalls.length : 0}
                  </code>
                </p>
              </div>
              <div className="btn-row">
                <input
                  type="text"
                  value={auditSearchQuery}
                  onChange={(e) => setAuditSearchQuery(e.target.value)}
                  placeholder="按动作/作用域/操作人检索..."
                  style={{ width: 190 }}
                />
                <button type="button" className="btn small" onClick={() => void queryAuditLogsFromBackend()}>
                  检索后端审计表
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>审计 ID</th>
                    <th>UTC 时间</th>
                    <th>操作人 (Role)</th>
                    <th>动作 (Action)</th>
                    <th>作用域 (Scope)</th>
                    <th>执行结果 (Outcome)</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.auditLogs ?? []).slice(0, 12).map((l) => (
                    <tr key={l.auditId}>
                      <td className="mono">{l.auditId}</td>
                      <td className="mono">{l.ts}</td>
                      <td className="mono">
                        {l.actor} ({l.role})
                      </td>
                      <td className="mono">{l.action}</td>
                      <td className="mono">{l.scope}</td>
                      <td className="mono">{l.outcome}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}

      {/* ======================= TAB 7: CELLD RUNTIME & PROBES ======================= */}
      {activeNav === 'celld' ? (
        <>
          <div className="page-head">
            <div>
              <div className="eyebrow">DENOLAND/CELLD V0.6.3 · DO CELL PLACEMENT · 6-POINT SUBSYSTEM PROBE</div>
              <h1>celld 开发服务运行时、DO Cell 放置与实时探针工作台</h1>
              <p className="muted">
                全部数据实时读取自 <code>GET /v1beta1/console/celld-dev-status</code>、<code>GET /.well-known/celld/health</code>、<code>GET /state</code> 与 <code>POST /v1beta1/management/celld/probe</code>。
              </p>
            </div>
            <div className="btn-row">
              <button
                type="button"
                className="btn primary small"
                onClick={() =>
                  void callMutationApi('/v1beta1/management/celld/probe', 'POST', {
                    reconcileCells: true,
                  })
                }
              >
                执行 6 项 celld 子系统主动对账探针
              </button>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">动态 Durable Object Cell 放置与 Epoch Fencing 表</h2>
                <p className="panel-subtitle">
                  由当前集群注册表 (<code>v25_cluster_registry</code>) 中的活跃 Brain / Gateway / Body 节点动态派生并由 <code>CelldAdminClient</code> 管理。
                </p>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Cell ID</th>
                    <th>宿主节点 (Host Node)</th>
                    <th>Epoch Fencing Token</th>
                    <th>最新 LTX TxID</th>
                    <th>状态</th>
                  </tr>
                </thead>
                <tbody>
                  {((celldStatusData?.registeredCells as Array<Record<string, unknown>>) ?? []).map((cell) => (
                    <tr key={String(cell.cellId)}>
                      <td className="mono">
                        <strong>{String(cell.cellId)}</strong>
                      </td>
                      <td className="mono">{String(cell.hostNodeId)}</td>
                      <td className="mono">epoch #{String(cell.epochFencingToken)}</td>
                      <td className="mono">tx #{String(cell.lastLtxTxId)}</td>
                      <td>
                        <span className="pill ok">{String(cell.state)}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="panel card">
            <div className="panel-header">
              <div>
                <h2 className="panel-title">6 项健康探针矩阵 (GET /v1beta1/console/health)</h2>
              </div>
            </div>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>探针 ID</th>
                    <th>子系统名称</th>
                    <th>状态</th>
                    <th>延迟</th>
                    <th>实时诊断详情</th>
                  </tr>
                </thead>
                <tbody>
                  {(mgmtState?.healthMatrix?.probes ?? []).map((p) => (
                    <tr key={p.id}>
                      <td className="mono">{p.id}</td>
                      <td>{p.name}</td>
                      <td>
                        <span className={`pill ${p.status === 'HEALTHY' ? 'ok' : 'bad'}`}>{p.status}</span>
                      </td>
                      <td className="mono">{p.latencyMs} ms</td>
                      <td className="mono">{p.detail}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}

      {lastApiResult ? (
        <div className="panel card">
          <div className="panel-header">
            <div>
              <h2 className="panel-title">最近一次真实后端 API 响应载荷 (Zero-Stub Live BFF Payload)</h2>
            </div>
            <button type="button" className="btn small" onClick={() => setLastApiResult('')}>
              清空响应面板
            </button>
          </div>
          <pre className="code-block mono">{lastApiResult}</pre>
        </div>
      ) : null}
    </HruiShell>
  );
}
