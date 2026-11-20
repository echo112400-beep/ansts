import {ControlError} from '../control/contracts.js';

export interface RegionAdapter {
  readonly mode: 'SIMULATION';
  capabilities(): {models: string[]; dataTypes: string[]};
  probe(identity: string): {healthy: boolean; rttMs: number};
  submit(identity: string, request: {id: string; model: string; dataType: string}): {id: string; simulated: true};
  drain(identity: string): void;
}

export class SimulatedRegionAdapter implements RegionAdapter {
  readonly mode = 'SIMULATION' as const;
  private draining = false;
  private readonly submissions = new Map<string, string>();

  constructor(private identity: string, private readonly models: readonly string[],
    private readonly dataTypes: readonly string[], private readonly rttMs: number) {
    if (!identity || !Number.isFinite(rttMs) || rttMs < 0) throw new ControlError('INVALID_ADAPTER_CONFIG',422);
  }

  private authenticate(identity: string) {
    if (identity !== this.identity) throw new ControlError('REGION_IDENTITY_REJECTED',401);
  }

  rotateIdentity(current: string, replacement: string) {
    this.authenticate(current);
    if (!replacement || replacement === current) throw new ControlError('INVALID_ADAPTER_IDENTITY',422);
    this.identity = replacement;
  }

  capabilities() {
    return {models: [...this.models], dataTypes: [...this.dataTypes]};
  }

  probe(identity: string) {
    this.authenticate(identity);
    return {healthy: !this.draining, rttMs: this.rttMs};
  }

  submit(identity: string, {id,model,dataType}: {id: string; model: string; dataType: string}) {
    this.authenticate(identity);
    if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(id)) throw new ControlError('INVALID_PLACEMENT_ID',422);
    const signature = JSON.stringify([model,dataType]);
    const previous = this.submissions.get(id);
    if (previous !== undefined) {
      if (previous !== signature) throw new ControlError('IDEMPOTENCY_CONFLICT',409);
      return {id,simulated:true as const};
    }
    if (this.draining) throw new ControlError('REGION_DRAINING',409);
    if (!this.models.includes(model) || !this.dataTypes.includes(dataType))
      throw new ControlError('REGION_CAPABILITY_MISMATCH',409);
    this.submissions.set(id,signature);
    return {id,simulated:true as const};
  }

  drain(identity: string) {
    this.authenticate(identity);
    this.draining = true;
  }
}
