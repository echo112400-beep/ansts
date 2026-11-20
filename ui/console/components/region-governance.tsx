"use client";

import {useEffect,useState,type FormEvent} from "react";

type Region={id:string;jurisdiction:string|null;failureDomain:string|null;models:string[];dataTypes:string[];
  trust:string;evidence:{expiresAt:number}|null};
type Decision={selectedRegion:string|null;candidates:{regionId:string;reasons:string[];rttMs:number|null}[]};
const labels:Record<string,string>={
  CURRENT:"目录证据有效",UNCONFIGURED:"未配置",DEVELOPMENT_ONLY:"仅开发环境",UNVERIFIED:"未验证",EXPIRED:"证据已过期",
  JURISDICTION_NOT_ALLOWED:"不符合驻留辖区",MODEL_UNSUPPORTED:"模型不受支持",DATA_TYPE_UNSUPPORTED:"数据类型不受支持",
  HEALTH_UNKNOWN:"缺少模拟健康数据",UNHEALTHY:"不可用",INVALID_RTT:"延迟数据无效",
};

export default function RegionGovernance({fetcher=fetch}:{fetcher?:typeof fetch}){
  const [regions,setRegions]=useState<Region[]|null>(null),[error,setError]=useState("");
  const [jurisdiction,setJurisdiction]=useState(""),[model,setModel]=useState(""),[dataType,setDataType]=useState("");
  const [result,setResult]=useState<{baseline:Decision;proposed:Decision;policy:string}|null>(null);
  const [busy,setBusy]=useState(false);
  useEffect(()=>{
    const controller=new AbortController();
    void fetcher("/v1/admin/regions",{signal:controller.signal,cache:"no-store"}).then(async response=>{
      if(!response.ok)throw new Error(String(response.status));
      setRegions((await response.json()).regions);
    }).catch(()=>{if(!controller.signal.aborted)setError("无法读取地域目录。");});
    return ()=>controller.abort();
  },[fetcher]);
  async function simulate(event:FormEvent){
    event.preventDefault();setBusy(true);setError("");setResult(null);
    const proposal={jurisdictions:jurisdiction.trim()?[jurisdiction.trim()]:[],model:model.trim()||null,dataType:dataType.trim()||null};
    const health=(regions??[]).map(region=>({regionId:region.id,healthy:true,rttMs:50,expiresAt:Date.now()+60000}));
    async function run(input:object){
      const response=await fetcher("/v1/admin/regions/simulate",{method:"POST",headers:{"content-type":"application/json"},
        body:JSON.stringify({input:{...input,health}})});
      if(!response.ok)throw new Error(String(response.status));
      return await response.json() as Decision;
    }
    try{
      const [baseline,proposed]=await Promise.all([run({jurisdictions:[],model:null,dataType:null}),run(proposal)]);
      setResult({baseline,proposed,policy:[proposal.jurisdictions.join(", "),proposal.model,proposal.dataType].map(value=>value||"不限").join(" / ")});
    }catch{setError("模拟未完成，请核对策略输入和服务状态。");}
    finally{setBusy(false);}
  }
  return <section className="admin-workspace">
    <header className="admin-heading"><h1>地域治理</h1></header>
    {error&&<p role="alert">{error}</p>}
    <h2>地域目录</h2>
    {!regions&&!error&&<p>正在读取目录…</p>}
    {regions?.length===0&&<p>尚未登记地域。</p>}
    {regions?.map(region=><article key={region.id}>
      <h3>{region.id}</h3><p>{labels[region.trust]??region.trust} · 司法辖区：{region.jurisdiction??"未知"} · 故障域：{region.failureDomain??"未知"}</p>
      <p>模型：{region.models.join("、")||"未登记"} · 数据类型：{region.dataTypes.join("、")||"未登记"}</p>
      <p>证据有效期：{region.evidence?new Date(region.evidence.expiresAt).toLocaleString():"未知"}</p>
    </article>)}
    <h2>策略与放置模拟</h2>
    <p>基线不限制辖区、模型或数据类型。模拟假设各地域健康，延迟为 50 ms。</p>
    <form onSubmit={simulate}>
      <label>司法辖区 <input value={jurisdiction} onChange={event=>setJurisdiction(event.target.value)} /></label>{" "}
      <label>模型 <input value={model} onChange={event=>setModel(event.target.value)} /></label>{" "}
      <label>数据类型 <input value={dataType} onChange={event=>setDataType(event.target.value)} /></label>{" "}
      <button disabled={!regions||busy}>{busy?"模拟中…":"对比策略"}</button>
    </form>
    {result&&<div aria-live="polite">
      <p>拟议策略（辖区 / 模型 / 数据类型）：{result.policy}</p>
      <p>基线：{result.baseline.selectedRegion??"无可放置地域"}；拟议：{result.proposed.selectedRegion??"无可放置地域"}</p>
      <ul>{result.proposed.candidates.map(item=>{
        const baseline=result.baseline.candidates.find(candidate=>candidate.regionId===item.regionId);
        const explain=(candidate:typeof item|undefined)=>candidate
          ?candidate.reasons.map(reason=>labels[reason]??reason).join("、")||"可放置"
          :"目录已变化";
        return <li key={item.regionId}>{item.regionId}：基线「{explain(baseline)}」，拟议「{explain(item)}」</li>;
      })}</ul>
    </div>}
    <h2>灾备证据</h2>
    <p>供应商：未知。跨地域灾备：未验收。</p>
    <dl><dt>RPO 目标</dt><dd>未登记</dd><dt>RPO 实测</dt><dd>无测量证据</dd>
      <dt>RTO 目标</dt><dd>未登记</dd><dt>RTO 实测</dt><dd>无测量证据</dd></dl>
  </section>;
}
