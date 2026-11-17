"use client";
import {useCallback,useEffect,useRef,useState,type FormEvent} from 'react';
import {controlClient,ControlApiError,type ControlPage,type CostCenter,type CostCenterEdit} from '../lib/control';
import type {User} from '../lib/console';
export default function CostCenterDirectory({client,user,onChanged}:{client:ReturnType<typeof controlClient>;user:User;onChanged:()=>Promise<void>}) {
 const [page,setPage]=useState<ControlPage<CostCenter>|null>(null);
 const [busy,setBusy]=useState(false),[saving,setSaving]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
 const [editor,setEditor]=useState<{id:string;create:boolean;input:CostCenterEdit}|null>(null);
 const form=useRef<HTMLFormElement>(null),opener=useRef<HTMLElement|null>(null);
 const sequence=useRef(0),alive=useRef(true),receipt=useRef<{payload:string;key:string}|null>(null);
 const canManage=user.canWrite&&user.role==='SRE_ADMIN'&&user.scopeTenant==='all'&&user.scopeRegion==='all';
 const load=useCallback(async(cursor?:string)=>{
  const version=++sequence.current;setBusy(true);setError('');
  try {const next=await client.costCenters({limit:25,cursor});if(alive.current&&version===sequence.current)setPage(next);}
  catch(e){if(alive.current&&version===sequence.current)setError(e instanceof ControlApiError&&e.conflict?'目录已变化，请刷新首页。':'成本中心读取失败，保留上次数据。');}
  finally{if(alive.current&&version===sequence.current)setBusy(false);}
 },[client]);
 useEffect(()=>{alive.current=true;void load();return()=>{alive.current=false;};},[load]);
 function edit(center?:CostCenter){opener.current=document.activeElement as HTMLElement;requestAnimationFrame(()=>form.current?.querySelector<HTMLInputElement>('input:not(:disabled)')?.focus());receipt.current=null;setNotice('');setEditor({id:center?.id??'',create:!center,input:{name:center?.name??'',status:center?.status??'ACTIVE',expectedVersion:center?.version??0,reason:''}});}
 function closeEditor(){setEditor(null);requestAnimationFrame(()=>{if(opener.current?.isConnected)opener.current.focus();});}
 function field<K extends keyof CostCenterEdit>(key:K,value:CostCenterEdit[K]){setEditor(current=>current?{...current,input:{...current.input,[key]:value}}:null);}
 async function save(event:FormEvent){
  event.preventDefault();if(!editor||saving||!canManage)return;
  const payload=JSON.stringify(editor);if(receipt.current?.payload!==payload)receipt.current={payload,key:crypto.randomUUID()};
  setSaving(true);setNotice('');
  try{const result=await client.saveCostCenter(editor.id,editor.input,receipt.current!.key,editor.create);
   if(!alive.current)return;setNotice(`已保存成本中心 ${result.id}，版本 ${result.version}。${result.operationId?` 操作 ID：${result.operationId}`:""}`);await Promise.all([load(),onChanged()]);if(alive.current)closeEditor();}
  catch(e){if(alive.current)setNotice(e instanceof ControlApiError&&e.conflict?'版本冲突，输入已保留。刷新列表并核对最新版本。':'保存失败，输入已保留，请检查权限与服务状态。');}
  finally{if(alive.current)setSaving(false);}
 }
 return <section className="admin-panel management-directory-panel" aria-labelledby="cost-centers-title">
  <div className="admin-panel-heading"><h2 id="cost-centers-title">成本中心</h2><span>归属目录 · 保留历史</span></div>
  <div className="directory-actions"><button className="secondary" disabled={busy} onClick={()=>void load()}>刷新成本中心</button>{canManage&&<button className="primary" disabled={!page||!!error||busy||saving} onClick={()=>edit()}>登记成本中心</button>}{page?.nextCursor&&<button className="secondary" disabled={busy} onClick={()=>void load(page.nextCursor!)}>下一页</button>}</div>
  {error&&<div className="banner error" role="alert">{error}</div>}{notice&&<p role="status">{notice}</p>}
  {page&&<div className="admin-table-scroll"><table><thead><tr><th>成本中心 ID</th><th>名称</th><th>状态</th><th>版本</th>{canManage&&<th>管理</th>}</tr></thead><tbody>{page.items.map(center=><tr key={center.id}><td className="mono">{center.id}</td><td>{center.name}</td><td>{center.status==='ACTIVE'?'有效':'已停用'}</td><td>{center.version}</td>{canManage&&<td><button className="secondary" disabled={saving||!!error} onClick={()=>edit(center)}>编辑成本中心</button></td>}</tr>)}</tbody></table>{!page.items.length&&<div className="admin-empty">当前授权范围没有已登记成本中心。</div>}</div>}
  {!canManage&&<p>全局范围的系统管理员可管理成本中心。</p>}
  {editor&&<form ref={form} className="create-form directory-editor" onSubmit={save}>
   <h3>{editor.create?'登记成本中心':`编辑成本中心 ${editor.id} · v${editor.input.expectedVersion}`}</h3>
   <label>成本中心 ID<input required maxLength={128} disabled={!editor.create||saving} value={editor.id} onChange={e=>setEditor({...editor,id:e.target.value})}/></label>
   <label>成本中心名称<input required maxLength={200} disabled={saving} value={editor.input.name} onChange={e=>field('name',e.target.value)}/></label>
   <label>成本中心状态<select disabled={saving} value={editor.input.status} onChange={e=>field('status',e.target.value as CostCenterEdit['status'])}><option value="ACTIVE">有效</option><option value="DISABLED">已停用</option></select></label>
   <label>成本中心变更原因<textarea required maxLength={1000} disabled={saving} value={editor.input.reason} onChange={e=>field('reason',e.target.value)} rows={3}/></label>
   <p>停用后禁止新增归属，已有租户和历史记录仍保留。</p>
   <div className="directory-actions"><button className="primary" disabled={saving} type="submit">确认保存成本中心</button><button className="secondary" disabled={saving} type="button" onClick={closeEditor}>取消成本中心编辑</button></div>
  </form>}
 </section>;
}
