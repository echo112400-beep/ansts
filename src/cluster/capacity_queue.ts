import type { CapacityResources } from '../control/capacity_report.js';

const dimensions = ['cpuMilliCores', 'memoryMiB', 'slots'] as const;

export interface CapacityQueueEntry {
  id: string;
  tenantId: string;
  resources: CapacityResources;
  enqueuedAt: number;
  priority: number;
}

export interface CapacityQueuePolicy {
  version: number;
  maxWaitMs: number;
  weights: ReadonlyMap<string, number>;
  priorities?: ReadonlyMap<string, number>;
}

interface QueueSnapshot {
  poolCapacity: CapacityResources;
  poolAvailable: CapacityResources;
  tenantPoolAvailable?: ReadonlyMap<string, CapacityResources>;
  hosts: readonly { id: string; capacity: CapacityResources; available: CapacityResources }[];
  tenantUsage: ReadonlyMap<string, CapacityResources>;
  tenantRemaining: ReadonlyMap<string, CapacityResources>;
}

const fits = (demand: CapacityResources, available: CapacityResources) =>
  dimensions.every(dimension => demand[dimension] <= available[dimension]);

/** Select one request against a trusted snapshot. The pool authority must recheck
 * the snapshot and account for outstanding grants before committing a placement. */
export function selectCapacityRequest(
  queue: readonly CapacityQueueEntry[],
  snapshot: QueueSnapshot,
  policy: CapacityQueuePolicy,
  now: number,
): { requestId: string; hostId: string; policyVersion: number } | null {
  const ordered = queue.flatMap(entry => {
    const remaining = snapshot.tenantRemaining.get(entry.tenantId);
    const weight = policy.weights.get(entry.tenantId) ?? 1;
    if (!remaining || !fits(entry.resources, remaining) ||
        !fits(entry.resources, snapshot.poolCapacity) ||
        !snapshot.hosts.some(host => fits(entry.resources, host.capacity)) ||
        !Number.isFinite(weight) || weight <= 0 || entry.enqueuedAt > now) return [];
    const usage = snapshot.tenantUsage.get(entry.tenantId);
    const share = Math.max(...dimensions.map(dimension => {
      const demand = (usage?.[dimension] ?? 0) + entry.resources[dimension];
      return demand === 0 ? 0 : demand / snapshot.poolCapacity[dimension];
    })) / weight;
    return [{ entry, share, aged: now - entry.enqueuedAt >= policy.maxWaitMs }];
  });

  const oldestFirst = (a: CapacityQueueEntry, b: CapacityQueueEntry) =>
    a.enqueuedAt - b.enqueuedAt || a.id.localeCompare(b.id, 'en');
  ordered.sort((a, b) => {
    if (a.aged !== b.aged) return a.aged ? -1 : 1;
    if (a.aged) return oldestFirst(a.entry, b.entry);
    return (policy.priorities?.get(b.entry.tenantId) ?? b.entry.priority) -
      (policy.priorities?.get(a.entry.tenantId) ?? a.entry.priority) || a.share - b.share || oldestFirst(a.entry, b.entry);
  });

  const hosts = [...snapshot.hosts].sort((a, b) => a.id.localeCompare(b.id, 'en'));
  for (const { entry, aged } of ordered) {
    const available = snapshot.tenantPoolAvailable?.get(entry.tenantId) ?? snapshot.poolAvailable;
    if (!fits(entry.resources, available) && fits(entry.resources, snapshot.poolAvailable)) continue;
    const host = fits(entry.resources, available)
      ? hosts.find(candidate => fits(entry.resources, candidate.available))
      : undefined;
    if (host) return { requestId: entry.id, hostId: host.id, policyVersion: policy.version };
    // Let running allocations drain once the oldest eligible request has aged.
    if (aged) return null;
  }
  return null;
}




