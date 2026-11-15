'use client';
import React from 'react';
import './hrui.css';
import { HrMark, Ic } from './icons';

export interface HruiNavItem {
  id: string;
  label: string;
  shortLabel: string;
  iconKey: keyof typeof Ic;
}

export const HRUI_NAV_ITEMS: HruiNavItem[] = [
  { id: 'overview', label: '平台态势与容量恢复', shortLabel: '态势', iconKey: 'overview' },
  { id: 'queues', label: '队列与健康状态可视', shortLabel: '队列', iconKey: 'queues' },
  { id: 'nodes', label: '节点与分片单元编排', shortLabel: '拓扑', iconKey: 'nodes' },
  { id: 'tenants', label: '租户配额与 FinOps 治理', shortLabel: '租户', iconKey: 'tenants' },
  { id: 'tasks', label: '任务运行与回合诊断', shortLabel: '任务', iconKey: 'tasks' },
  { id: 'approvals', label: '高危审批与变更审计', shortLabel: '审批', iconKey: 'approvals' },
  { id: 'configs', label: '配置中心与版本治理', shortLabel: '配置', iconKey: 'configs' },
  { id: 'runtime', label: '分布式运行面与 LayerFS', shortLabel: '运行面', iconKey: 'runtime' },
  { id: 'observability', label: '可观测性、SLO 与自愈', shortLabel: '观测', iconKey: 'observability' },
  { id: 'swarm', label: '智能体协同与算力市场', shortLabel: '协同', iconKey: 'swarm' },
  { id: 'federation', label: '多地域联邦与隧道灾备', shortLabel: '联邦', iconKey: 'federation' },
  { id: 'security', label: '安全合规、RBAC 与门控', shortLabel: '安全', iconKey: 'security' },
];

export function HruiShell({
  activeNav,
  onSelectNav,
  darkTheme,
  onToggleTheme,
  backendOrigin,
  liveConnected,
  children,
}: {
  activeNav: string;
  onSelectNav: (id: string) => void;
  darkTheme: boolean;
  onToggleTheme: () => void;
  backendOrigin: string;
  liveConnected: boolean;
  children: React.ReactNode;
}) {
  return (
    <div id="hrui" data-theme={darkTheme ? 'dark' : 'light'}>
      <div className="app">
        <aside className="rail" aria-label="hrui 主导航栏">
          <div className="wordmark">
            <span className="brand-mark" aria-hidden="true">
              <HrMark />
            </span>
            <span>
              ANSTS
              <small>HRUI · NEXT.JS</small>
            </span>
          </div>
          <div className="workspace">
            <span className={`dot ${liveConnected ? 'ok' : 'bad'}`} />
            <span>
              <strong>平台管理工作区</strong>
              <small className="mono">ui/nextjs · hrui v2.5</small>
            </span>
            <span className="pill run">HRUI</span>
          </div>
          <nav id="navigation" aria-label="主导航">
            {HRUI_NAV_ITEMS.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`nav-item rail-btn ${activeNav === item.id ? 'active on' : ''}`}
                onClick={() => onSelectNav(item.id)}
              >
                <span className="nav-item-label">
                  {Ic[item.iconKey]}
                  <span>{item.label}</span>
                </span>
              </button>
            ))}
          </nav>
        </aside>
        <div className="shell page">
          <header className="context topbar">
            <div className="breadcrumbs">
              <span className={`dot ${liveConnected ? 'ok' : 'bad'}`} />
              <span>ANSTS Console</span>
              <span>/</span>
              <b>{HRUI_NAV_ITEMS.find((i) => i.id === activeNav)?.label ?? '平台控制面'}</b>
            </div>
            <div className="account">
              <span className="pill ok mono">{backendOrigin}</span>
              <button type="button" className="btn small" onClick={onToggleTheme}>
                {darkTheme ? '☀ 亮色模式' : '☾ 暗色模式'}
              </button>
            </div>
          </header>
          <main id="content" className="scroll view">
            {children}
          </main>
        </div>
      </div>
    </div>
  );
}
