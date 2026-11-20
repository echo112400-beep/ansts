"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { User } from "../lib/console";
import { controlClient, type CapacityPoolSummary, type CapacityRequestRow } from "../lib/control";

const formatResources = (value:{cpuMilliCores:number;memoryMiB:number;slots:number}) =>
  value.cpuMilliCores+" mCPU · "+value.memoryMiB+" MiB · "+value.slots+" 槽位";

const states = {QUEUED:"排队中", CANCELLED:"已取消", GRANTED:"已授权", RELEASED:"已释放"};

const queueReasons = {
  POOL_DISABLED:"容量池已停用",
  ALLOCATION_PAUSED:"容量池已暂停新授权",
  NO_HEALTHY_HOST:"没有健康宿主机",
  HOST_RESOURCES_UNAVAILABLE:"没有单台宿主机满足当前资源需求",
  TENANT_QUOTA_EXCEEDED:"租户配额余量不足",
  TENANT_QUOTA_UNAVAILABLE:"租户配额暂不可用",
  AWAITING_ALLOCATION:"等待分配",
};

export default function CapacityAllocation({user,fetcher}:{user:User;fetcher?:typeof fetch}) {
  const client = useMemo(() => controlClient(fetcher), [fetcher]);
  const admin = user.role === "SRE_ADMIN" && user.scopeTenant === "all" && user.scopeRegion === "all";
  const canRequest = user.canWrite && ["OPERATOR","SRE_ADMIN"].includes(user.role);
  const [pools,setPools] = useState<CapacityPoolSummary[]>([]);
  const [poolId,setPoolId] = useState("");
  const [tenantId,setTenantId] = useState(user.scopeTenant === "all" ? "" : user.scopeTenant);
  const [requests,setRequests] = useState<CapacityRequestRow[]>([]);
  const [control,setControl] = useState<{version:number;enabled:boolean}|null>(null);
  const [error,setError] = useState("");
  const [busy,setBusy] = useState(false);
  const [revision,refresh] = useState(0);
  const pendingSubmission = useRef<{signature:string;key:string}|null>(null);

  useEffect(() => {
    setPools([]);
    if (!tenantId) return;
    const controller = new AbortController();
    client.capacityPools(tenantId,controller.signal).then(result => { if (!controller.signal.aborted) setPools(result.pools); })
      .catch(err => { if (!controller.signal.aborted) setError(String(err.message)); });
    return () => controller.abort();
  },[tenantId,client,revision]);

  useEffect(() => {
    const controller = new AbortController();
    setRequests([]);
    setControl(null);
    const failed = (err:Error) => { if (!controller.signal.aborted) setError(err.message); };
    if (poolId && tenantId) client.capacityRequests(poolId,tenantId,controller.signal)
      .then(result => { if (!controller.signal.aborted) setRequests(result.requests); }).catch(failed);
    if (admin && poolId) client.capacityAllocationControl(poolId,controller.signal)
      .then(result => { if (!controller.signal.aborted) setControl(result); }).catch(failed);
    return () => controller.abort();
  },[admin,client,poolId,tenantId,revision]);

  async function mutate(action:()=>Promise<unknown>) {
    setBusy(true);
    setError("");
    try { await action(); refresh(value => value+1); }
    catch (err) { setError(err instanceof Error ? err.message : String(err)); }
    finally { setBusy(false); }
  }

  const pool = pools.find(item => item.id === poolId);
  return <div className="admin-panel">
    <div className="admin-panel-heading"><h2>容量分配</h2>
      <button type="button" onClick={() => refresh(value => value+1)}>刷新</button>
    </div>
    {error && <div className="banner error" role="alert">{error}</div>}
    <label>容量池 <select disabled={busy} value={poolId} onChange={event => setPoolId(event.target.value)}>
      <option value="">选择容量池</option>
      {pools.map(item => <option key={item.id} value={item.id}>{item.name} · {item.region}</option>)}
    </select></label>
    <label>租户 <input disabled={busy || user.scopeTenant !== "all"} value={tenantId}
      onChange={event => {setTenantId(event.target.value);setPoolId("");setError("");}} /></label>
    {pool && <dl className="admin-settings">
      <div><dt>容量池总量</dt><dd>{formatResources(pool.capacity)}</dd></div>
      <div><dt>租户保障份额</dt><dd>{pool.guaranteedShare ? formatResources(pool.guaranteedShare) : "未配置"}</dd></div>
      <div><dt>当前可借</dt><dd>{formatResources(pool.available)}</dd></div>
      <div><dt>借用占用</dt><dd>{formatResources(pool.borrowed)} · {pool.activeGrants} 笔授权</dd></div>
      <div><dt>保留占用</dt><dd>{formatResources(pool.retainedReserved)}</dd></div>
      <div><dt>容量报告</dt><dd>{pool.complete ? "完整" : "不完整；失联容量不计为可借"}</dd></div>
      <div><dt>新授权状态</dt><dd>{pool.status === "DISABLED" ? "池已停用" :
        (control?.enabled ?? pool.allocationEnabled) === false ? "已暂停" :
        (control?.enabled ?? pool.allocationEnabled) === true ? "允许" : "未知"}</dd></div>
      <div><dt>隔离宿主机</dt><dd>{pool.isolatedHostCount} / {pool.hostCount}</dd></div>
    </dl>}
    {admin && control && user.canWrite && <p>
      <button type="button" disabled={busy} onClick={() => void mutate(() =>
        client.setCapacityAllocationControl(poolId,control.version,!control.enabled))}>
        {control.enabled ? "暂停新授权" : "恢复授权"}
      </button></p>}
    {canRequest && poolId && tenantId && <form onSubmit={event => {
      event.preventDefault();
      const form = new FormData(event.currentTarget);
      const resources = {cpuMilliCores:Number(form.get("cpu")),memoryMiB:Number(form.get("memory")),slots:1};
      const signature = JSON.stringify([poolId,tenantId,resources]);
      let submission = pendingSubmission.current;
      if (!submission || submission.signature !== signature) {
        submission = {signature,key:crypto.randomUUID()};
        pendingSubmission.current = submission;
      }
      const {key} = submission;
      void mutate(async () => {
        await client.submitCapacityRequest(poolId,tenantId,resources,key);
        pendingSubmission.current = null;
      });
    }}>
      <label>CPU (mCPU) <input name="cpu" type="number" min="1" required /></label>
      <label>内存 (MiB) <input name="memory" type="number" min="1" required /></label>

      <button type="submit" disabled={busy}>提交容量申请</button>
    </form>}
    {poolId && tenantId && <><h3>容量申请</h3>
      {requests.length ? <ul>{requests.map(request => <li key={request.id}>
        {states[request.state]} · {request.resources.cpuMilliCores} mCPU · {request.resources.memoryMiB} MiB · {request.resources.slots} 槽位
        {request.grantExpiresAt !== undefined && <> · 绑定截止 {new Date(request.grantExpiresAt).toLocaleString()}</>}
        {request.queueReason && <> · {queueReasons[request.queueReason]}</>}
        {canRequest && request.state === "QUEUED" && <button type="button" disabled={busy}
          onClick={() => void mutate(() => client.cancelCapacityRequest(poolId,tenantId,request.id))}>取消</button>}
      </li>)}</ul> : <p>暂无容量申请</p>}
    </>}
  </div>;
}








