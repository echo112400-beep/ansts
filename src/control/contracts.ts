/** Control-plane contracts. Actors must originate from verified server-side claims. */
export interface ControlActor {
  sub: string;
  role: 'VIEWER' | 'OPERATOR' | 'SRE_ADMIN';
  scopeTenant: string;
  scopeRegion: string;
}
export type ControlAction = 'read' | 'request_capacity' | 'administer' | 'approve_release';
export class ControlError extends Error {
  constructor(readonly code: string, readonly status: number) { super(code); }
}
export function assertControlAccess(actor: ControlActor, action: ControlAction,
  tenantId: string, region: string, requestedBy?: string): void {
  if (!actor.sub || !tenantId || !region ||
      !['VIEWER', 'OPERATOR', 'SRE_ADMIN'].includes(actor.role) ||
      (actor.scopeTenant !== 'all' && actor.scopeTenant !== tenantId) ||
      (actor.scopeRegion !== 'all' && actor.scopeRegion !== region)) {
    throw new ControlError('CONTROL_FORBIDDEN', 403);
  }
  if (action === 'read') return;
  if (action === 'request_capacity' && ['OPERATOR', 'SRE_ADMIN'].includes(actor.role)) return;
  if (actor.role !== 'SRE_ADMIN') throw new ControlError('CONTROL_FORBIDDEN', 403);
  if (action === 'approve_release' && (!requestedBy || requestedBy === actor.sub)) {
    throw new ControlError('INDEPENDENT_APPROVER_REQUIRED', 403);
  }
  if (action !== 'administer' && action !== 'approve_release') throw new ControlError('CONTROL_FORBIDDEN', 403);
}
export interface ResourceQuota {
  concurrentAttempts: number;
  cpuMilliCores: number;
  memoryMiB: number;
  queuedTasks: number;
}
export function validateQuota(value: ResourceQuota): void {
  const fields: (keyof ResourceQuota)[] = ['concurrentAttempts', 'cpuMilliCores', 'memoryMiB', 'queuedTasks'];
  if (!value || fields.some(key => !Number.isSafeInteger(value[key]) || value[key] < 0)) {
    throw new ControlError('INVALID_RESOURCE_QUOTA', 422);
  }
}
export function assertExpectedVersion(expected: number, current: number): void {
  if (!Number.isSafeInteger(expected) || expected < 0) throw new ControlError('INVALID_VERSION', 422);
  if (expected !== current) throw new ControlError('VERSION_CONFLICT', 409);
}
/** JSON transports decimal strings; never round a credit quantity through Number. */
export function parseCreditUnits(value: string): bigint {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]{0,77})$/.test(value)) {
    throw new ControlError('INVALID_CREDIT_UNITS', 422);
  }
  return BigInt(value);
}
