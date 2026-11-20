"use client";
import {useCallback,useEffect,useMemo,useRef,useState,type FormEvent} from 'react';
import {controlClient,ControlApiError,type CostCenter,type ControlPage,type DirectoryTenant,type TenantEdit} from '../lib/control';
import TenantQuota from "./tenant-quota";
import FinopsSummary from "./finops-summary";
import TenantCostExport from "./tenant-cost-export";
import TenantCostDetails from "./tenant-cost-details";
import TenantActivity from "./tenant-activity";
import CostCenterDirectory from "./cost-center-directory";
import type {User} from '../lib/console';
export default function TenantDirectory({user,fetcher=fetch}:{user:User;fetcher?:typeof fetch}) {
  const client=useMemo(()=>controlClient(fetcher),[fetcher]);
  const [quotaTenant,setQuotaTenant]=useState<DirectoryTenant|null>(null);
  const [centers,setCenters]=useState<CostCenter[]>([]);
  const [page,setPage]=useState<ControlPage<DirectoryTenant>|null>(null);
  const [busy,setBusy]=useState(false),[saving,setSaving]=useState(false);
  const [error,setError]=useState(''),[notice,setNotice]=useState('');
  const [loadedAt,setLoadedAt]=useState('');
  const [editor,setEditor]=useState<{id:string;create:boolean;input:TenantEdit}|null>(null);
  const alive=useRef(true),serial=useRef(0);
  const form=useRef<HTMLFormElement>(null),opener=useRef<HTMLElement|null>(null);
  const receipt=useRef<{payload:string;key:string}|null>(null);
  const editable=user.role==='SRE_ADMIN'&&user.canWrite;
  const load=useCallback(async(cursor?:string,signal?:AbortSignal)=>{
    const request=++serial.current;setBusy(true);setError('');
    try {
      const [next,firstCenters]=await Promise.all([client.tenants({cursor,limit:25,signal}),client.costCenters({limit:100,signal})]);
      const collected=[...firstCenters.items];let remaining=firstCenters.nextCursor;let pages=1;
      while(remaining){if(pages++>=100)throw new ControlApiError('DIRECTORY_TOO_LARGE',503);const extra=await client.costCenters({cursor:remaining,limit:100,signal});collected.push(...extra.items);remaining=extra.nextCursor;}

      if(alive.current&&request===serial.current){setPage(next);setCenters(collected);setLoadedAt(new Date().toLocaleTimeString('zh-CN'));}
    } catch(e) {
      if(alive.current&&request===serial.current&&!(e instanceof Error&&e.name==='AbortError'))
        setError(e instanceof ControlApiError&&e.unavailable?'租户目录服务尚未接入或暂不可用。':e instanceof ControlApiError&&e.conflict?'目录已变化，请从首页重新加载。':'目录读取失败，请重试。');
    } finally {if(alive.current&&request===serial.current)setBusy(false);}
  },[client]);
  useEffect(()=>{alive.current=true;const abort=new AbortController();void load(undefined,abort.signal);return()=>{alive.current=false;abort.abort();};},[load]);
  function edit(tenant?:DirectoryTenant) {
    opener.current=document.activeElement as HTMLElement;receipt.current=null;setNotice('');
    requestAnimationFrame(()=>form.current?.querySelector<HTMLInputElement>('input:not(:disabled)')?.focus());
    setEditor({id:tenant?.id??(user.scopeTenant==='all'?'':user.scopeTenant),create:!tenant,input:{region:tenant?.region??'',name:tenant?.name??'',costCenter:tenant?.costCenter??centers.find(c=>c.status==='ACTIVE')?.id??'',status:tenant?.status??'ACTIVE',expectedVersion:tenant?.version??0,reason:''}});
  }
  function closeEditor(){setEditor(null);requestAnimationFrame(()=>{if(opener.current?.isConnected)opener.current.focus();});}
  function field<K extends keyof TenantEdit>(key:K,value:TenantEdit[K]) {
    setEditor(current=>current?{...current,input:{...current.input,[key]:value}}:null);
  }
  async function save(event:FormEvent) {
    event.preventDefault();if(!editor||saving||!editable)return;
    const current=editor;const payload=JSON.stringify(current);
    if(receipt.current?.payload!==payload)receipt.current={payload,key:crypto.randomUUID()};
    setSaving(true);setNotice('');
    try {
      const result=current.create
        ?await client.createTenant(current.id,current.input,receipt.current!.key)
        :await client.saveTenant(current.id,current.input,receipt.current!.key);
      if(!alive.current)return;
      setNotice(`已保存租户 ${result.id}，版本 ${result.version}。${result.operationId?` 操作 ID：${result.operationId}`:""}`);await load();if(alive.current)closeEditor();
    } catch(e) {
      if(alive.current)setNotice(e instanceof ControlApiError&&e.conflict
        ?'版本或幂等键冲突，输入已保留。刷新列表核对最新记录后，再载入版本编辑。'
        :'保存失败，输入已保留。请检查权限与服务状态后重试。');
    } finally {if(alive.current)setSaving(false);}
  }
  return <><CostCenterDirectory client={client} user={user} onChanged={load}/><section className="admin-panel management-directory-panel" aria-labelledby="tenant-directory-title">
    <div className="admin-panel-heading"><h2 id="tenant-directory-title">租户目录</h2><span>{loadedAt?`最近读取 ${loadedAt}`:'持久化目录'}</span></div>
    <div className="directory-actions">
      <button className="secondary" disabled={busy} onClick={()=>void load()}>刷新首页</button>
      {editable&&<button className="primary" disabled={!page||busy||!!error||saving||!centers.some(c=>c.status==='ACTIVE')} onClick={()=>edit()}>登记租户</button>}
      {page?.nextCursor&&<button className="secondary" disabled={busy} onClick={()=>void load(page.nextCursor!)}>下一页</button>}
      {busy&&<span role="status">正在读取…</span>}
    </div>
    {error&&<div className="banner error" role="alert">{error}{page&&' 下方保留上次读取的数据。'}</div>}
    {notice&&<p role="status">{notice}</p>}
    {!page&&!busy&&!error&&<div className="admin-empty">尚未读取目录。</div>}
    {page&&<div className="admin-table-scroll"><table><thead><tr><th>租户 / 名称</th><th>地域</th><th>成本中心</th><th>状态</th><th>版本</th><th>记录</th></tr></thead>
      <tbody>{page.items.map(tenant=><tr key={tenant.id}><td><span className="mono">{tenant.id}</span><br/>{tenant.name}</td><td>{tenant.region}</td><td>{tenant.costCenter}</td><td>{tenant.status==='ACTIVE'?'有效':'已停用'}</td><td className="mono">{tenant.version}</td><td>
        <button className="secondary" onClick={()=>setQuotaTenant(tenant)}>查看配额策略</button>
        {editable&&<button className="secondary" disabled={saving||!!error} onClick={()=>edit(tenant)}>载入版本编辑</button>}
        <details><summary>归属历史</summary><ul>{tenant.costCenterHistory?.map(entry=><li key={entry.version}>{entry.costCenter} · v{entry.version} · {entry.timestamp?new Date(entry.timestamp).toLocaleString('zh-CN'):'时间未知'}</li>)}</ul></details>
      </td></tr>)}</tbody></table>{!page.items.length&&<div className="admin-empty">当前授权范围没有已登记租户。</div>}</div>}
    {editor&&<form ref={form} className="create-form directory-editor" onSubmit={save}>
      <h3>{editor.create?'登记租户':`编辑 ${editor.id} · 基于版本 ${editor.input.expectedVersion}`}</h3>
      <label>租户 ID<input required maxLength={128} pattern="[a-zA-Z0-9][a-zA-Z0-9_.:\-]*" disabled={!editor.create||saving} value={editor.id} onChange={e=>setEditor({...editor,id:e.target.value})}/></label>
      <label>名称<input required maxLength={200} disabled={saving} value={editor.input.name} onChange={e=>field('name',e.target.value)}/></label>
      <label>地域<input required maxLength={128} disabled={saving} value={editor.input.region} onChange={e=>field('region',e.target.value)}/></label>
      <label>成本中心<select required disabled={saving} value={editor.input.costCenter} onChange={e=>field('costCenter',e.target.value)}><option value="">请选择已登记中心</option>{centers.filter(center=>center.status==='ACTIVE'||center.id===editor.input.costCenter).map(center=><option key={center.id} value={center.id} disabled={center.status!=='ACTIVE'}>{center.name} · {center.id}{center.status!=='ACTIVE'?'（已停用）':''}</option>)}{editor.input.costCenter&&!centers.some(center=>center.id===editor.input.costCenter)&&<option value={editor.input.costCenter} disabled>{editor.input.costCenter}（历史归属，未登记）</option>}</select></label>
      <label>状态<select disabled={saving} value={editor.input.status} onChange={e=>field('status',e.target.value as TenantEdit['status'])}><option value="ACTIVE">有效</option><option value="DISABLED">已停用</option></select></label>
      <label>变更原因<textarea required maxLength={1000} disabled={saving} value={editor.input.reason} onChange={e=>field('reason',e.target.value)} rows={3}/></label>
      <p>目录元数据变更会记录审计。执行准入、资源配额和计费仍需各自模块接入。</p>
      <div className="directory-actions"><button type="submit" className="primary" disabled={saving}>{saving?'正在保存…':'确认保存'}</button><button type="button" className="secondary" disabled={saving} onClick={closeEditor}>取消编辑</button></div>
    </form>}
  </section><FinopsSummary fetcher={fetcher}/>{quotaTenant&&<div key={quotaTenant.id}><TenantQuota tenant={quotaTenant} user={user} client={client}/><TenantActivity tenantId={quotaTenant.id} fetcher={fetcher}/><TenantCostDetails tenantId={quotaTenant.id} fetcher={fetcher}/>{user.role!=="VIEWER"&&user.canWrite&&<TenantCostExport tenantId={quotaTenant.id} fetcher={fetcher}/>}</div>}</>;
}
