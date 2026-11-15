import type { NextConfig } from 'next';

const BACKEND_ORIGIN = process.env.ANSTS_BACKEND_ORIGIN || 'http://127.0.0.1:8088';

const nextConfig: NextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      {
        source: '/v1beta1/:path*',
        destination: `${BACKEND_ORIGIN}/v1beta1/:path*`,
      },
      {
        source: '/v1/:path*',
        destination: `${BACKEND_ORIGIN}/v1/:path*`,
      },
      {
        source: '/internal/v1beta1/:path*',
        destination: `${BACKEND_ORIGIN}/internal/v1beta1/:path*`,
      },
      {
        source: '/.well-known/celld/health',
        destination: `${BACKEND_ORIGIN}/.well-known/celld/health`,
      },
      {
        source: '/state',
        destination: `${BACKEND_ORIGIN}/state`,
      },
      {
        source: '/metrics',
        destination: `${BACKEND_ORIGIN}/metrics`,
      },
    ];
  },
};

export default nextConfig;
