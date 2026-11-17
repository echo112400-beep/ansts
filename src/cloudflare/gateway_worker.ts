/**
 * @fileoverview Gateway celld entrypoint. Serves the gateway HTTP API and AnstsGatewayCell.
 * The scheduler is a different celld process reached at ANSTS_SCHEDULER_URL.
 */
import {monitoringAuthorized,gatewayMetrics,recordGatewayRequest} from "../control/monitoring.js";
import { UnifiedGatewayEnv, handleUnifiedGatewayRequest } from "./gateway_cell.js";

export { TenantDirectoryCell } from "./tenant_directory_cell.js";
export { AnstsGatewayCell } from "./gateway_cell.js";

const JSON_HEADERS = { "content-type": "application/json" };

export default {
  async fetch(request: Request, env: UnifiedGatewayEnv): Promise<Response> {
    const path=new URL(request.url).pathname,started=Date.now();let status=500;
    try {
      if(request.method==='GET'&&path==='/metrics') {
        if(!monitoringAuthorized(request.headers.get('authorization')??undefined,env.ANSTS_MONITORING_TOKEN)) {status=401;return Response.json({error:'MONITORING_AUTH_REQUIRED'},{status});}
        status=200;return new Response(gatewayMetrics(),{headers:{'content-type':'text/plain; version=0.0.4; charset=utf-8','cache-control':'no-store'}});
      }
      const handled=await handleUnifiedGatewayRequest(request,env);
      const response=handled??new Response(JSON.stringify({error:'NOT_FOUND',controlPlane:'celld-gateway'}),{status:404,headers:JSON_HEADERS});
      status=response.status;return response;
    } finally {recordGatewayRequest(path,request.method,status,Date.now()-started);}
  },
};
