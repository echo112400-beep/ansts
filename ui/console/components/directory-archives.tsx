"use client";
import {useCallback,useEffect,useRef,useState,type FormEvent} from 'react';
import {controlClient,ControlApiError,verifyDirectoryArchive,type DirectoryArchiveStatus} from '../lib/control';
import type {User} from '../lib/console';
export default function DirectoryArchives({client,user}:{client:ReturnType<typeof controlClient>;user:User}) {
 const globalAdmin=user.role==='SRE_ADMIN'&&user.scopeTenant==='all'&&user.scopeRegion==='all';
 const [data,setData]=useState<DirectoryArchiveStatus|null>(null),[reason,setReason]=useState('');
 const [busy,setBusy]=useState(false),[working,setWorking]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
 const alive=useRef(true),sequence=useRef(0),receipt=useRef<{payload:string;key:string}|null>(null);
 const load=useCallback(async()=>{
  const serial=++sequence.current;setBusy(true);setError('');
  try {const value=await client.archives();if(alive.current&&serial===sequence.current)setData(value);}
  catch {if(alive.current&&serial===sequence.current)setError('归档服务暂不可用，保留上次读取结果。');}
  finally{if(alive.current&&serial===sequence.current)setBusy(false);}
 },[client]);
 useEffect(()=>{alive.current=true;if(globalAdmin)void load();return()=>{alive.current=false;};},[load,globalAdmin]);
 if(!globalAdmin)return null;
 async function create(event:FormEvent){
  event.preventDefault();if(!data||working||!user.canWrite)return;
  const payload=JSON.stringify([data.currentVersion,reason]);if(receipt.current?.payload!==payload)receipt.current={payload,key:crypto.randomUUID()};
  setWorking(true);setNotice('');
  try {const manifest=await client.createArchive(data.currentVersion,reason,receipt.current!.key);if(!alive.current)return;
   setNotice(`已保存检查点 ${manifest.id}。下载后请另行保存到独立存储。`);setReason('');await load();}
  catch(e){if(alive.current)setNotice(e instanceof ControlApiError&&e.conflict?'源目录已变化或幂等键冲突，原因已保留，请刷新后核对。':'创建失败，原因已保留，可使用同一请求重试。');}
  finally{if(alive.current)setWorking(false);}
 }
 async function download(id:string){
  if(working)return;setWorking(true);setNotice('');
  try {const artifact=await client.archive(id);if(!await verifyDirectoryArchive(artifact,id))throw new Error('INTEGRITY');
   if(!alive.current)return;
   const url=URL.createObjectURL(new Blob([JSON.stringify(artifact,null,2)],{type:'application/json'}));
   const anchor=document.createElement('a');anchor.href=url;anchor.download=`directory-archive-${id}.json`;document.body.appendChild(anchor);anchor.click();anchor.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
   setNotice('摘要校验通过，归档文件已下载。');
  }catch{if(alive.current)setNotice('下载或摘要校验失败，未导出文件。');}
  finally{if(alive.current)setWorking(false);}
 }
 return <section className="admin-panel management-directory-panel" aria-labelledby="directory-archives-title">
  <div className="admin-panel-heading"><h2 id="directory-archives-title">目录检查点与归档</h2><span>全局管理 · 校验导出</span></div>
  <p className="audit-record-detail">保留全部幂等回执，未启用清理。同库检查点需导出到独立存储后才能作为独立备份使用。</p>
  <div className="directory-actions"><button className="secondary" disabled={busy||working} onClick={()=>void load()}>刷新归档</button>{data&&<span>源版本 {data.currentVersion} · {data.items.length} 个检查点</span>}</div>
  {error&&<div className="banner error" role="alert">{error}</div>}{notice&&<p role="status">{notice}</p>}
  {data&&<div className="admin-table-scroll"><table><thead><tr><th>创建时间</th><th>源版本 / 大小</th><th>操作者 / 原因</th><th>导出</th></tr></thead><tbody>{data.items.map(item=><tr key={item.id}><td>{new Date(item.createdAt).toLocaleString('zh-CN')}</td><td>v{item.sourceVersion} · {item.bytes} B</td><td><span className="mono">{item.actor}</span><p className="audit-record-detail">{item.reason}</p></td><td><button className="secondary" disabled={working} onClick={()=>void download(item.id)}>校验并下载</button></td></tr>)}</tbody></table>{!data.items.length&&<div className="admin-empty">尚无检查点。</div>}</div>}
  {user.canWrite&&<form className="create-form directory-editor" onSubmit={create}><label>归档原因<textarea required maxLength={1000} rows={2} disabled={working} value={reason} onChange={e=>setReason(e.target.value)}/></label><button type="submit" className="primary" disabled={!data||!!error||busy||working}>创建检查点</button></form>}
 </section>;
}
