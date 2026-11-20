"use client";
import {useState} from "react";

export default function TenantCostExport({tenantId,fetcher=fetch}:{tenantId:string;fetcher?:typeof fetch}){
  const [job,setJob]=useState<string|null>(null);
  const [busy,setBusy]=useState(false),[message,setMessage]=useState("");
  const base="/v1/admin/tenants/"+encodeURIComponent(tenantId)+"/cost-exports";
  async function download(){
    setBusy(true);setMessage("");
    try{
      let id=job;
      if(!id){
        const created=await fetcher(base,{method:"POST",headers:{"content-type":"application/json","idempotency-key":crypto.randomUUID()},body:"{}"});
        if(!created.ok)throw new Error("创建失败");
        const data=await created.json();
        if(typeof data.id!=="string"||!/^[a-f0-9]{64}$/.test(data.id))throw new Error("任务标识无效");
        id=data.id as string;setJob(id);
      }
      const generated=await fetcher(base+"/"+id+"/run",{method:"POST",headers:{"content-type":"application/json"},body:"{}"});
      if(!generated.ok)throw new Error("生成失败");
      const response=await fetcher(base+"/"+id+"/download",{cache:"no-store"});
      if(!response.ok)throw new Error("下载失败");
      const url=URL.createObjectURL(await response.blob());
      const anchor=document.createElement("a");anchor.href=url;anchor.download="cost-export.csv";
      anchor.click();
      window.setTimeout(()=>URL.revokeObjectURL(url),1000);
      setMessage("费用 CSV 已生成。");setJob(null);
    }catch{setMessage("费用导出未完成，请检查权限或稍后重试。");}
    finally{setBusy(false);}
  }
  return <div>
    <button type="button" disabled={busy} onClick={()=>void download()}>{busy?"正在导出…":"导出费用 CSV"}</button>
    {message&&<p role="status">{message}</p>}
  </div>;
}
