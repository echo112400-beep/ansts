"use client";
import {useEffect,useState} from "react";

type Page={summary:{knownSubtotalCreditUnits:string;totalCreditUnits:string|null;unit:string;reasons:string[]};
 watermark:number;nextCursor:string|null;items:{eventId:string;pending?:string;entry:null|{
 periodId:string;pending:string[];context:null|{costCenter:string;costCenterVersion:number|null;
 activation:null|{version:number;rateCard:{version:number}}};lines:{dimension:string;
 exactCredit:null|{numerator:string;denominator:string};pending:string[]}[]}}[]};

export default function TenantCostDetails({tenantId,fetcher=fetch}:{tenantId:string;fetcher?:typeof fetch}){
 const [page,setPage]=useState<Page|null>(null),[error,setError]=useState(false),[busy,setBusy]=useState(false);
 const [history,setHistory]=useState<string[]>([]);
 const cursor=history.at(-1);
 useEffect(()=>{
  const controller=new AbortController();
  setBusy(true);setError(false);
  const path="/v1/admin/tenants/"+encodeURIComponent(tenantId)+"/costs?limit=25"+
    (cursor?"&cursor="+encodeURIComponent(cursor):"");
  void (async()=>{
   try{
    const response=await fetcher(path,{signal:controller.signal,cache:"no-store"});
    if(!response.ok)throw new Error(String(response.status));
    const data=await response.json() as Page;
    if(!controller.signal.aborted)setPage(data);
   }catch{
    if(!controller.signal.aborted){setError(true);setPage(null);}
   }finally{if(!controller.signal.aborted)setBusy(false);}
  })();
  return ()=>controller.abort();
 },[tenantId,fetcher,cursor]);
 return <section aria-label="租户费用明细" aria-busy={busy}>
  <h3>费用明细</h3>
  {busy&&<p role="status">正在读取费用…</p>}
  {error&&<p role="alert">费用明细暂不可用。</p>}
  {page&&!busy&&<>
   <p>已知小计：{page.summary.knownSubtotalCreditUnits} {page.summary.unit} · 总额：{page.summary.totalCreditUnits??"待核算"} · 水位：{page.watermark}</p>
   {page.summary.reasons.length>0&&<p>待核算原因：{page.summary.reasons.join("、")}</p>}
   {!page.items.length&&<p>尚无费用事件。</p>}
   <ul>{page.items.map(item=><li key={item.eventId}>
    <details><summary>{item.entry?.periodId??"日期待核算"} · {item.eventId}</summary>
     <p>成本中心：{item.entry?.context?.costCenter??"未知"} · 归属版本：{item.entry?.context?.costCenterVersion??"未知"} · 费率版本：{item.entry?.context?.activation?.rateCard.version??"未配置"} · 激活版本：{item.entry?.context?.activation?.version??"未知"}</p>
     {[...(item.entry?.pending??[]),...(item.pending?[item.pending]:[])].map(reason=><p key={reason}>{reason}</p>)}
     <ul>{item.entry?.lines.map(line=><li key={line.dimension}>{line.dimension}：{line.exactCredit?line.exactCredit.numerator+"/"+line.exactCredit.denominator:"待核算"} {line.pending.join("、")}</li>)}</ul>
    </details>
   </li>)}</ul>
   <button type="button" disabled={!history.length} onClick={()=>setHistory(previous=>previous.slice(0,-1))}>上一页</button>{" "}
   <button type="button" disabled={!page.nextCursor} onClick={()=>{if(page.nextCursor)setHistory(previous=>[...previous,page.nextCursor!]);}}>下一页</button>
  </>}
 </section>;
}
