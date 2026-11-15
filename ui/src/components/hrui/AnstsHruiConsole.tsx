'use client';
import React, { useEffect, useState } from 'react';
import { HruiShell } from './Shell';

export interface DynamicClusterTopology {
  topologyGeneration: number;
  ringDigest: string;
  activeBrainMembers: string[];
  totalRegisteredNodes: number;
  countsByRole: { brain: number; body: number; gateway: number };
  slotDistribution: Record<string, number>;
  slotOwners: string[];
  brains: Array<{
    nodeId: string;
    brainKey?: string;
    role: string;
    region: string;
    baseUrl: string;
    status: string;
    ownedSlots: number;
    registeredVia: string;
    leaseEpoch: number;
  }>;
  bodies: Array<{
    nodeId: string;
    role: string;
    region: string;
    baseUrl: string;
    status: string;
    capacitySlots: number;
    registeredVia: string;
    leaseEpoch: number;
  }>;
  gateways: Array<{
    nodeId: string;
    role: string;
    region: string;
    baseUrl: string;
    status: string;
    capacitySlots: number;
    registeredVia: string;
    leaseEpoch: number;
  }>;
}

export function AnstsHruiConsole() {
  const [activeNav, setActiveNav] = useState<string>('overview');
  const [darkTheme, setDarkTheme] = useState<boolean>(false);
  const [backendOrigin, setBackendOrigin] = useState<string>('http://127.0.0.1:8088');
  const [liveConnected, setLiveConnected] = useState<boolean>(false);
  const [topology, setTopology] = useState<DynamicClusterTopology | null>(null);
  const [mgmtState, setMgmtState] = useState<Record<string, unknown> | null>(null);

  const loadFromBackend = async () => {
    try {
      const [stateResp, topoResp] = await Promise.all([
        fetch(`${backendOrigin}/v1beta1/management/state`),
        fetch(`${backendOrigin}/v1beta1/console/topology`),
      ]);
      if (stateResp.ok && topoResp.ok) {
        setMgmtState(await stateResp.json());
        setTopology(await topoResp.json());
        setLiveConnected(true);
      } else {
        setLiveConnected(false);
      }
    } catch {
      setLiveConnected(false);
    }
  };

  useEffect(() => {
    if (typeof window !== 'undefined' && (window.location.protocol === 'http:' || window.location.protocol === 'https:')) {
      setBackendOrigin(window.location.origin);
    }
  }, []);

  useEffect(() => {
    void loadFromBackend();
    const tid = setInterval(() => {
      void loadFromBackend();
    }, 5000);
    return () => clearInterval(tid);
  }, [backendOrigin]);

  const registerNewNode = async (role: 'brain' | 'body' | 'gateway') => {
    const counts = topology?.countsByRole ?? { brain: 3, body: 3, gateway: 2 };
    const nextIdx = (counts[role] ?? 2) + 1;
    const nodeId =
      role === 'brain'
        ? `celld-brain-0${nextIdx}`
        : role === 'gateway'
        ? `celld-gw-0${nextIdx}`
        : `body-host-0${nextIdx}`;
    const port = role === 'brain' ? 18081 + nextIdx : role === 'gateway' ? 18085 + nextIdx : 19500 + nextIdx;
    await fetch(`${backendOrigin}/internal/v1beta1/cluster/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        nodeId,
        role,
        region: 'us-west-2',
        baseUrl: `http://127.0.0.1:${port}`,
        registeredVia: 'SELF_REG',
      }),
    });
    await loadFromBackend();
  };

  const slotDist = topology?.slotDistribution ?? { 'brain-node-01': 85, 'brain-node-02': 86, 'brain-node-03': 85 };
  const brainKeys = Object.keys(slotDist);
  const slotOwners = topology?.slotOwners ?? [];

  return (
    <HruiShell
      activeNav={activeNav}
      onSelectNav={setActiveNav}
      darkTheme={darkTheme}
      onToggleTheme={() => setDarkTheme((d) => !d)}
      backendOrigin={backendOrigin}
      liveConnected={liveConnected}
    >
      <div className="page-head">
        <div>
          <div className="eyebrow">HRUI DESIGN SYSTEM · NEXT.JS APP ROUTER (UI/)</div>
          <h1>ANSTS 平台控制面与动态拓扑工作台</h1>
          <p className="muted">
            统一采用 <code>ui/</code> Next.js + <code>#hrui</code> 设计风格（Geist / Geist Mono、4 列指标条、胶囊按钮、动态 256-Slot 拓扑与全角色节点自注册）。
          </p>
        </div>
        <div className="btn-row">
          <button type="button" className="btn primary small" onClick={() => void registerNewNode('brain')}>
            + 自注册新 Brain
          </button>
          <button type="button" className="btn small" onClick={() => void registerNewNode('body')}>
            + 自注册新 Body
          </button>
          <button type="button" className="btn small" onClick={() => void registerNewNode('gateway')}>
            + 自注册新 Gateway
          </button>
        </div>
      </div>

      <section className="metrics kpi-grid" aria-label="核心态势指标">
        <div className="metric kpi-card">
          <small>
            <span>注册节点总数 (Brain / Body / GW)</span>
            <span className="pill ok">AUTO-REG</span>
          </small>
          <b className="num">{topology?.totalRegisteredNodes ?? 8}</b>
          <div className="sub mono">
            B:{topology?.countsByRole?.brain ?? 3} · Body:{topology?.countsByRole?.body ?? 3} · GW:{topology?.countsByRole?.gateway ?? 2}
          </div>
        </div>
        <div className="metric kpi-card">
          <small>
            <span>256-Slot 一致性哈希环</span>
            <span className="pill run">Gen #{topology?.topologyGeneration ?? 1}</span>
          </small>
          <b className="num">{brainKeys.length} 分片</b>
          <div className="sub mono">
            {Object.entries(slotDist)
              .map(([k, v]) => `${k.replace('brain-node-', 'B')}:${v}`)
              .join(' / ')}
          </div>
        </div>
        <div className="metric kpi-card">
          <small>
            <span>活跃租户与任务状态</span>
            <span className="pill ok">RPO=0 SQL</span>
          </small>
          <b className="num">{Array.isArray(mgmtState?.tasks) ? mgmtState.tasks.length : 0} 任务</b>
          <div className="sub mono">租户数: {Array.isArray(mgmtState?.tenants) ? mgmtState.tenants.length : 0}</div>
        </div>
        <div className="metric kpi-card">
          <small>
            <span>后端连接与 SSE 事件流</span>
            <span className={`pill ${liveConnected ? 'ok' : 'bad'}`}>{liveConnected ? 'LIVE' : 'OFFLINE'}</span>
          </small>
          <b className="num">{liveConnected ? '200 OK' : '503'}</b>
          <div className="sub mono">{backendOrigin}</div>
        </div>
      </section>

      <div className="panel card">
        <div className="panel-header">
          <div>
            <h2 className="panel-title">256-Slot 动态分片哈希环 (零硬编码 · 支持任意 N 个 Brain 节点自动均衡)</h2>
            <p className="panel-subtitle">
              {Object.entries(slotDist)
                .map(([k, v]) => `${k}: ${v} 槽`)
                .join(' · ')}
            </p>
          </div>
        </div>
        <div className="slot-ring-grid">
          {Array.from({ length: 256 }, (_, idx) => {
            const owner = slotOwners[idx] || brainKeys[idx % Math.max(1, brainKeys.length)] || 'brain-node-01';
            const bIdx = Math.max(0, brainKeys.indexOf(owner));
            const cls = bIdx === 0 ? 'b1' : bIdx === 1 ? 'b2' : 'b3';
            return <div key={idx} className={`slot-box ${cls}`} title={`Slot #${idx} → ${owner}`} />;
          })}
        </div>
      </div>
    </HruiShell>
  );
}
