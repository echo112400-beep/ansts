"use client";
import {useEffect,useState} from "react";

type Rental={policyVersion:number;used:{slots:number;cpuMilliCores:number;memoryMiB:number}};
type Usage={watermark:number;nextCursor:string|null;events:{event:{eventId:string;kind:string;at:number;
 allocation?:{cpuNanoCoreMilliseconds:string|null;memoryMiBMilliseconds:string|null;slotMilliseconds:string|null}}}[]};

export default function TenantActivity({tenantId,fetcher=fetch}:{tenantId:string;fetcher?:typeof fetch}){
 const [holds,setHolds]=useState<Rental|null>(null),[usage,setUsage]=useState<Usage|null>(null);
 const [failed,setFailed]=useState<Record<string,boolean>>({}),[cursor,setCursor]=useState<string|null>(null);
 const base="/v1/admin/tenants/"+encodeURIComponent(tenantId);
 useEffect(()=>{
  const controller=new AbortController();
  void fetcher(base+"/reservation-summary",{signal:controller.signal,cache:"no-store"}).then(async response=>{
   if(!response.ok)throw new Error();
   const value=await response.json();
   if(!controller.signal.aborted)setHolds(value);
  }).catch(()=>{if(!controller.signal.aborted)setFailed(value=>({...value,holds:true}));});
  return ()=>controller.abort();
 },[base,fetcher]);
 useEffect(()=>{
  const controller=new AbortController();
  setUsage(null);setFailed(value=>({...value,usage:false}));
  void fetcher(base+"/usage?limit=25"+(cursor?"&cursor="+encodeURIComponent(cursor):""),{signal:controller.signal,cache:"no-store"}).then(async response=>{
   if(!response.ok)throw new Error();
   const value=await response.json();
   if(!controller.signal.aborted)setUsage(value);
  }).catch(()=>{if(!controller.signal.aborted)setFailed(value=>({...value,usage:true}));});
  return ()=>controller.abort();
 },[base,cursor,fetcher]);
 return <>
  <section aria-label="预留资源占用">
   <h3>预留资源占用</h3>
   {holds?<p>策略 v{holds.policyVersion} · {holds.used.slots} 执行槽 · {holds.used.cpuMilliCores} mCPU · {holds.used.memoryMiB} MiB</p>:
    <p>{failed.holds?"暂无可读的预留占用，可能尚未配置有效策略。":"正在读取预留占用…"}</p>}
  </section>
  <section aria-label="用量事件">
   <h3>用量事件</h3>
   {failed.usage?<p role="alert">无法读取用量。</p>:!usage?<p role="status">正在读取用量…</p>:<>
    <p>水位：{usage.watermark}。资源用量按分配时长记录。</p>
    {!usage.events.length&&<p>尚无用量事件。</p>}
    <ul>{usage.events.map(({event})=><li key={event.eventId}>{new Date(event.at).toLocaleString()} · {event.kind}
      {event.allocation&&<> · CPU {event.allocation.cpuNanoCoreMilliseconds??"未知"} 纳核·ms · 内存 {event.allocation.memoryMiBMilliseconds??"未知"} MiB·ms · 槽 {event.allocation.slotMilliseconds??"未知"} 槽·ms</>}</li>)}</ul>
    {cursor&&<button type="button" onClick={()=>setCursor(null)}>返回用量首页</button>}{" "}
    {usage.nextCursor&&<button type="button" onClick={()=>setCursor(usage.nextCursor)}>下一页用量</button>}
   </>}
  </section>
 </>;
}
