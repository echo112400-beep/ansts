/**
 * TASK-18: Warm Sandbox Pool Manager and Isolation Driver Abstraction (Firecracker / Kata / gVisor).
 */

import { IsolationDriver } from "../models";

export interface WarmSandboxInstance {
  instanceId: string;
  poolId: string;
  driver: IsolationDriver;
  bootLatencyMs: number;
  dirtyBlocksZeroed: boolean;
}

export class WarmSandboxPoolManager {
  public static readonly DRIVER_BOOT_LATENCY_MS: Record<IsolationDriver, number> = {
    firecracker: 62,
    kata: 115,
    gvisor: 48,
    runc: 35,
  };

  private pools: Map<string, WarmSandboxInstance[]> = new Map();
  private seq: number = 1;

  constructor(public readonly targetWatermark: number = 4) {
    for (const pid of ["pool-py312-ds", "pool-rust-toolchains", "pool-playwright-browser"]) {
      this.replenishPool(pid, "firecracker");
    }
  }

  public replenishPool(poolId: string, driver: IsolationDriver = "firecracker"): number {
    const pool = this.pools.get(poolId) ?? [];
    let added = 0;
    while (pool.length < this.targetWatermark) {
      pool.push({
        instanceId: `sbx_warm_${this.seq++}`,
        poolId,
        driver,
        bootLatencyMs: WarmSandboxPoolManager.DRIVER_BOOT_LATENCY_MS[driver] ?? 65,
        dirtyBlocksZeroed: true,
      });
      added += 1;
    }
    this.pools.set(poolId, pool);
    return added;
  }

  public acquireWarmSandbox(poolId: string, driver: IsolationDriver = "firecracker"): WarmSandboxInstance {
    let pool = this.pools.get(poolId);
    if (!pool || pool.length === 0) {
      this.replenishPool(poolId, driver);
      pool = this.pools.get(poolId)!;
    }
    const inst = pool.shift()!;
    this.replenishPool(poolId, driver);
    return inst;
  }

  public teardownAndZeroBlocks(instance: WarmSandboxInstance): boolean {
    instance.dirtyBlocksZeroed = true;
    return true;
  }
}
