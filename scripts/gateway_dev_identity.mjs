/** Short-lived test/development identities; callers choose role and tenant explicitly. */
import { readFileSync } from 'node:fs';
import { createHmac } from 'node:crypto';
export function gatewayDevAuthorization(tenant = 'all', role = 'SRE_ADMIN', now = Math.floor(Date.now() / 1000)) {
  const secret = readFileSync(new URL('../.dev-secrets/user-signing.key', import.meta.url), 'utf8').trim();
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({ sub: `development-${role.toLowerCase()}`, role, scopeTenant: tenant, scopeRegion: 'all', iat: now, exp: now + 900 })).toString('base64url');
  const signature = createHmac('sha256', secret).update(`${header}.${payload}`).digest('base64url');
  return `Bearer ${header}.${payload}.${signature}`;
}
