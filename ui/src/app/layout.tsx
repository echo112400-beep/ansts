import type { Metadata } from 'next';
import '../components/hrui/hrui.css';

export const metadata: Metadata = {
  title: 'ANSTS · 平台管理控制台 (Next.js + hrui Design System)',
  description: 'ANSTS v2.5 Control Plane & celld Cluster Management Console powered by Next.js App Router and hrui design language',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
