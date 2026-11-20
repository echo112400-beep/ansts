import {ControlError} from '../control/contracts.js';

type ProbeBinding = {fetch(request: Request): Promise<Response>};
export type RegionProbeConfig =
  | {kind:'service-binding'; binding:ProbeBinding}
  | {kind:'development-loopback'; port:number};

export class RegionProbeRegistry {
  private readonly entries=new Map<string,RegionProbeConfig>();

  constructor(private readonly profile:'development'|'production',
    entries:ReadonlyMap<string,RegionProbeConfig>) {
    for(const [ref,entry] of entries){
      if(!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(ref))
        throw new ControlError('INVALID_ADAPTER_CONFIG',422);
      if(entry.kind==='development-loopback'){
        if(profile!=='development'||!Number.isSafeInteger(entry.port)||entry.port<1||entry.port>65535)
          throw new ControlError('INVALID_ADAPTER_CONFIG',422);
        this.entries.set(ref,{kind:entry.kind,port:entry.port});
      }else if(entry.kind==='service-binding'&&typeof entry.binding?.fetch==='function'){
        this.entries.set(ref,{kind:entry.kind,binding:entry.binding});
      }else throw new ControlError('INVALID_ADAPTER_CONFIG',422);
    }
  }

  async probe(ref:string, credential:string) {
    const entry=this.entries.get(ref);
    if(!entry)throw new ControlError('REGION_ADAPTER_NOT_CONFIGURED',404);
    if(!credential||/[\r\n]/.test(credential))throw new ControlError('REGION_IDENTITY_UNAVAILABLE',503);
    const started=performance.now();
    const request=new Request(entry.kind==='service-binding'?'https://region.internal/readyz':
      'http://127.0.0.1:'+entry.port+'/readyz',{
      redirect:'error',signal:AbortSignal.timeout(2000),headers:{authorization:'Bearer '+credential},
    });
    try{
      const response=await (entry.kind==='service-binding'?entry.binding.fetch(request):fetch(request));
      await response.body?.cancel();
      return {healthy:response.status===200,rttMs:performance.now()-started};
    }catch{
      return {healthy:false,rttMs:performance.now()-started};
    }
  }
}
