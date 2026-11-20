"use client";
import {useEffect,useState} from "react";

type Summary={knownSubtotalCreditUnits:string;totalCreditUnits:string|null;unit:string;
 directoryWatermark:number;tenantCount:number;missingTenantIds:string[];pendingTenantIds:string[];
 costCenters?:{costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number;knownSubtotalCreditUnits:string}[]};

export default function FinopsSummary({fetcher}:{fetcher:typeof fetch}){
 const [summary,setSummary]=useState<Summary|null>(null),[failed,setFailed]=useState(false);
 const [revision,refresh]=useState(0);
 useEffect(()=>{
  const controller=new AbortController();
  setSummary(null);setFailed(false);
  void fetcher("/v1/admin/finops/summary",{signal:controller.signal,cache:"no-store"}).then(async response=>{
   if(!response.ok)throw new Error(String(response.status));
   const value=await response.json() as Summary;
   if(!controller.signal.aborted)setSummary(value);
  }).catch(()=>{if(!controller.signal.aborted)setFailed(true);});
  return ()=>controller.abort();
 },[fetcher,revision]);
 return <section className="admin-panel" aria-label="授权范围内费用汇总">
  <h2>费用汇总</h2>
  <button type="button" onClick={()=>refresh(value=>value+1)}>刷新费用汇总</button>
  {failed?<p role="alert">无法读取费用汇总。请重试或检查访问范围。</p>:!summary?<p role="status">正在读取费用汇总…</p>:<>
   <p>租户数：{summary.tenantCount} · 目录水位：{summary.directoryWatermark}</p>
   <p>已知小计：{summary.knownSubtotalCreditUnits} {summary.unit} · 总额：{summary.totalCreditUnits??"待核算"}</p>
   {!!summary.costCenters?.length&&<details>
    <summary>成本中心小计与版本</summary>
    <div className="admin-table-scroll"><table>
     <thead><tr><th scope="col">成本中心</th><th scope="col">归属版本</th><th scope="col">费率版本</th><th scope="col">激活版本</th><th scope="col">已知小计</th></tr></thead>
     <tbody>{summary.costCenters.map(group=><tr key={JSON.stringify([group.costCenter,group.costCenterVersion,group.rateCardVersion,group.activationVersion])}>
      <th scope="row">{group.costCenter}</th><td>{group.costCenterVersion}</td><td>{group.rateCardVersion}</td><td>{group.activationVersion}</td>
      <td>{group.knownSubtotalCreditUnits} {summary.unit}</td>
     </tr>)}</tbody>
    </table></div>
   </details>}
   {summary.pendingTenantIds.length>0&&<p>待核算租户：{summary.pendingTenantIds.join("、")}</p>}
   {summary.missingTenantIds.length>0&&<p>暂未返回数据的租户：{summary.missingTenantIds.join("、")}</p>}
  </>}
 </section>;
}
