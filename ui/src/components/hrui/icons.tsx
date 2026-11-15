import type { ReactNode } from 'react';

function Svg({ children, cap = true }: { children: ReactNode; cap?: boolean }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.8}
      strokeLinecap={cap ? 'round' : undefined}
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}

export function HrMark() {
  return (
    <svg className="hr-mark" viewBox="0 0 28 28" fill="none" aria-hidden="true">
      <rect x="2" y="2" width="24" height="24" rx="7" fill="var(--accent)" />
      <path d="M9 19L14 8L19 19M10.8 15.5H17.2" stroke="#ffffff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export const Ic = {
  overview: <Svg><circle cx="12" cy="12" r="8.5" /><path d="M10.2 8.9l5 3.1-5 3.1z" /></Svg>,
  queues: <Svg><path d="M4 20V10M10 20V4M16 20v-7M22 20H2" /></Svg>,
  nodes: <Svg cap={false}><rect x="3.5" y="9" width="17" height="10.5" rx="2" /><rect x="6.5" y="5" width="4" height="4" rx="1" /><rect x="13.5" y="5" width="4" height="4" rx="1" /></Svg>,
  tenants: <Svg><rect x="3" y="5.5" width="18" height="13" rx="2.2" /><path d="M3 10h18M7 15h4" /></Svg>,
  tasks: <Svg><path d="M3 6l2 2 3-3M3 13l2 2 3-3M12 7h9M12 14h9M3 20h18" /></Svg>,
  approvals: <Svg><path d="M5 12.5l4.5 4.5L19 7.5" /></Svg>,
  configs: <Svg cap={false}><rect x="3" y="10" width="8" height="10" rx="1.5" /><rect x="13" y="10" width="8" height="10" rx="1.5" /><path d="M5 10V8M9 10V8M15 10V8M19 10V8" /></Svg>,
  runtime: <Svg cap={false}><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M9 4v16M15 4v16" /></Svg>,
  observability: <Svg><circle cx="6" cy="7" r="2.4" /><circle cx="18" cy="7" r="2.4" /><circle cx="12" cy="17.5" r="2.4" /><path d="M8.4 7h7.2M7.3 9.1l3.4 6.2M16.7 9.1l-3.4 6.2" /></Svg>,
  swarm: <Svg><circle cx="9" cy="8.5" r="3.2" /><path d="M3 19.5c.7-3.2 3-5 6-5s5.3 1.8 6 5" /><path d="M15.5 5.6a3.2 3.2 0 0 1 0 5.8M17.5 14.8c1.8.6 3 2.2 3.5 4.7" /></Svg>,
  federation: <Svg><circle cx="12" cy="12" r="9" /><path d="M3.6 9h16.8M3.6 15h16.8M12 3a14.5 14.5 0 0 0 0 18M12 3a14.5 14.5 0 0 1 0 18" /></Svg>,
  security: <Svg cap={false}><circle cx="8" cy="15" r="4" /><path d="M11 12l9-9M17 6l3 3" /></Svg>,
};
