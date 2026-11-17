import { test } from 'node:test';
import assert from 'node:assert/strict';
import { assertControlAccess, validateQuota, assertExpectedVersion, parseCreditUnits } from '../../src/control/contracts.js';
const admin = {sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'tenant-a',scopeRegion:'dev'};
test('admin role cannot cross tenant or region boundaries', () => {
  assert.throws(() => assertControlAccess(admin,'administer','tenant-b','dev'));
  assert.throws(() => assertControlAccess(admin,'read','tenant-a','other'));
  assertControlAccess(admin,'administer','tenant-a','dev');
});
test('approval requires independent verified subject', () => {
  for (const creator of [undefined,admin.sub]) assert.throws(() => assertControlAccess(admin,'approve_release','tenant-a','dev',creator));
  assertControlAccess(admin,'approve_release','tenant-a','dev','github:2');
});
test('operator may request capacity but cannot administer', () => {
  const actor={...admin,role:'OPERATOR' as const};
  assertControlAccess(actor,'request_capacity','tenant-a','dev');
  assert.throws(() => assertControlAccess(actor,'administer','tenant-a','dev'));
  assert.throws(() => assertControlAccess({...actor,role:'VIEWER'},'request_capacity','tenant-a','dev'));
});
test('quotas reject fractional, negative, missing and unsafe values', () => {
  const q={concurrentAttempts:0,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10};
  validateQuota(q);
  for (const v of [-1,0.5,Infinity,NaN,Number.MAX_SAFE_INTEGER+1,undefined]) assert.throws(() => validateQuota({...q,memoryMiB:v as number}));
});
test('CAS distinguishes invalid version and stale write', () => {
  assertExpectedVersion(3,3);
  assert.throws(() => assertExpectedVersion(2,3),{code:'VERSION_CONFLICT'});
  assert.throws(() => assertExpectedVersion(-1,3),{code:'INVALID_VERSION'});
});
test('credits preserve values beyond Number precision', () => {
  assert.equal(parseCreditUnits('9007199254740993'),9007199254740993n);
  for (const v of ['01','-1','1.2','1e6','', '9'.repeat(79)]) assert.throws(() => parseCreditUnits(v));
});
