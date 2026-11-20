import {ControlError} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';

export interface RegionRecord {
  id: string;
  jurisdiction: string | null;
  failureDomain: string | null;
  identityRef: string | null;
  adapterRef: string | null;
  models: string[];
  dataTypes: string[];
  environment: 'development' | 'production';
  evidence: {ref: string; verifiedAt: number; expiresAt: number} | null;
  version: number;
}
const identifier = (value: unknown): value is string =>
  typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(value);

export function regionTrust(region: RegionRecord, now: number) {
  if (region.environment === 'development') return 'DEVELOPMENT_ONLY';
  if (!region.jurisdiction || !region.failureDomain || !region.identityRef || !region.adapterRef || !region.evidence)
    return 'UNCONFIGURED';
  if (now < region.evidence.verifiedAt) return 'UNVERIFIED';
  return now < region.evidence.expiresAt ? 'CURRENT' : 'EXPIRED';
}

export class RegionCatalog {
  constructor(private readonly storage: DirectoryStorage) {
    storage.sql.exec('CREATE TABLE IF NOT EXISTS region_catalog (id TEXT PRIMARY KEY, payload TEXT NOT NULL)');
  }

  list(): RegionRecord[] {
    return this.storage.sql.exec<{payload: string}>('SELECT payload FROM region_catalog ORDER BY id')
      .toArray().map(row => JSON.parse(row.payload) as RegionRecord);
  }

  write(input: Omit<RegionRecord, 'version'>, expectedVersion: number, now: number): RegionRecord {
    const {id,jurisdiction,failureDomain,identityRef,adapterRef,models,dataTypes,environment,evidence} = input;
    const validList = (items: unknown): items is string[] =>
      Array.isArray(items) && items.length <= 64 && items.every(identifier) && new Set(items).size === items.length;
    if (!identifier(id) || ![jurisdiction,failureDomain,identityRef,adapterRef].every(value => value === null || identifier(value)) ||
        !validList(models) || !validList(dataTypes) || !['development','production'].includes(environment) ||
        !Number.isSafeInteger(expectedVersion) || expectedVersion < 0 || expectedVersion >= Number.MAX_SAFE_INTEGER ||
        (evidence !== null && (!evidence || !identifier(evidence.ref) || !Number.isSafeInteger(evidence.verifiedAt) ||
          !Number.isSafeInteger(evidence.expiresAt) || evidence.verifiedAt < 0 || evidence.verifiedAt > now ||
          evidence.expiresAt <= evidence.verifiedAt))) throw new ControlError('INVALID_REGION_RECORD',422);
    return this.storage.transactionSync(() => {
      const [existing] = this.storage.sql.exec<{payload:string}>('SELECT payload FROM region_catalog WHERE id = ?',id).toArray();
      if ((existing ? (JSON.parse(existing.payload) as RegionRecord).version : 0) !== expectedVersion)
        throw new ControlError('VERSION_CONFLICT',409);
      const region: RegionRecord = {id,jurisdiction,failureDomain,identityRef,adapterRef,models:[...models].sort(),
        dataTypes:[...dataTypes].sort(),environment,evidence:evidence && {ref:evidence.ref,verifiedAt:evidence.verifiedAt,
          expiresAt:evidence.expiresAt},version:expectedVersion+1};
      this.storage.sql.exec('INSERT OR REPLACE INTO region_catalog (id, payload) VALUES (?, ?)',id,JSON.stringify(region));
      return region;
    });
  }
}
