'use client';
import React from 'react';
import {
  HrMark,
  IconCheck,
  IconCpu,
  IconFolder,
  IconKey,
  IconLayers,
  IconRefresh,
  IconRoute,
  IconSun,
  IconTerminal,
} from './icons';

export interface HruiNavItem {
  id: string;
  label: string;
  icon: React.ReactNode;
}

export interface HruiShellProps {
  activeNav: string;
  onSelectNav: (id: string) => void;
  darkTheme: boolean;
  onToggleTheme: () => void;
  backendOrigin: string;
  onChangeOrigin?: (nextOrigin: string) => void;
  liveConnected: boolean;
  sseConnected?: boolean;
  principalId?: string;
  principalRole?: string;
  onSelectPrincipal?: (principalId: string) => void;
  navBadges?: Record<string, string>;
  onRefresh?: () => void;
  children: React.ReactNode;
}

export function HruiShell({
  activeNav,
  onSelectNav,
  darkTheme,
  onToggleTheme,
  backendOrigin,
  onChangeOrigin,
  liveConnected,
  sseConnected,
  principalId = 'chen',
  principalRole = 'SRE_ADMIN',
  onSelectPrincipal,
  navBadges = {},
  onRefresh,
  children,
}: HruiShellProps) {
  const navItems: HruiNavItem[] = [
    { id: 'overview', label: '集群总览与动态拓扑', icon: <IconLayers size={15} /> },
    { id: 'tenants', label: '租户、FinOps 与队列', icon: <IconKey size={15} /> },
    { id: 'nodes', label: '节点伸缩、隧道与灾备', icon: <IconCpu size={15} /> },
    { id: 'tasks', label: '任务、WAL 与 LayerFS', icon: <IconFolder size={15} /> },
    { id: 'models', label: '模型路由、配置与画像', icon: <IconRoute size={15} /> },
    { id: 'audit', label: '4-Eyes 审批、审计与可观测', icon: <IconCheck size={15} /> },
    { id: 'celld', label: 'celld 运行时与探针', icon: <IconTerminal size={15} /> },
  ];

  return (
    <div id="hrui" data-theme={darkTheme ? 'dark' : 'light'}>
      <div className="shell app-shell">
        <aside className="rail" aria-label="ANSTS 管理导航">
          <div className="brand">
            <HrMark size={20} />
            <div className="brand-title">
              <strong>ANSTS Control</strong>
              <span className="tag">HRUI · NEXT.JS</span>
            </div>
          </div>

          <div className="sect-lbl">GOVERNANCE</div>
          <nav className="navigation">
            {navItems.map((it) => (
              <button
                key={it.id}
                type="button"
                className={`rail-btn nav-item ${activeNav === it.id ? 'on active' : ''}`}
                onClick={() => onSelectNav(it.id)}
              >
                <span className="nav-item-label">
                  {it.icon}
                  <span>{it.label}</span>
                </span>
                {navBadges[it.id] ? <span className="nav-badge">{navBadges[it.id]}</span> : null}
              </button>
            ))}
          </nav>

          <div className="rail-foot rail-footer">
            <button type="button" className="rail-btn nav-item" onClick={onToggleTheme}>
              <span className="nav-item-label">
                <IconSun size={15} />
                <span>{darkTheme ? '亮色模式 (Light)' : '暗色模式 (Dark)'}</span>
              </span>
            </button>
            <div className="muted mono" style={{ fontSize: 11, padding: '6px 10px' }}>
              {liveConnected ? `LIVE · ${backendOrigin}` : `OFFLINE · ${backendOrigin}`}
            </div>
          </div>
        </aside>

        <div className="main-area">
          <header className="topbar">
            <div className="topbar-title-group">
              <span className={`pill ${liveConnected ? 'ok' : 'bad'}`}>
                {liveConnected ? 'CELLD LIVE CONNECTED' : 'BACKEND OFFLINE (503)'}
              </span>
              <span className={`pill ${sseConnected ? 'run' : 'neutral'}`}>
                {sseConnected ? 'SSE ?live=1 STREAMING' : 'POLLING'}
              </span>
              <span className="pill neutral mono">RBAC: {principalId} ({principalRole})</span>
            </div>
            <div className="topbar-controls">
              {onSelectPrincipal ? (
                <label className="ctrl-group">
                  <span>RBAC 身份</span>
                  <select
                    value={principalId}
                    onChange={(e) => onSelectPrincipal(e.target.value)}
                    aria-label="RBAC 身份切换"
                  >
                    <option value="chen">chen (SRE_ADMIN)</option>
                    <option value="zhou">zhou (SRE_ADMIN · 异体复核人)</option>
                    <option value="lin">lin (TENANT_OPERATOR)</option>
                    <option value="xu">xu (VIEWER · 只读观察员)</option>
                  </select>
                </label>
              ) : null}
              {onChangeOrigin ? (
                <label className="ctrl-group">
                  <span>后端 Origin</span>
                  <input
                    type="text"
                    className="mono"
                    value={backendOrigin}
                    onChange={(e) => onChangeOrigin(e.target.value)}
                    style={{ width: 175 }}
                  />
                </label>
              ) : null}
              {onRefresh ? (
                <button type="button" className="btn small" onClick={onRefresh}>
                  <IconRefresh size={13} />
                  <span>刷新同步</span>
                </button>
              ) : null}
              <button type="button" className="btn small" onClick={onToggleTheme}>
                {darkTheme ? '切换亮色' : '切换暗色'}
              </button>
            </div>
          </header>

          <main className="main content">{children}</main>
        </div>
      </div>
    </div>
  );
}
