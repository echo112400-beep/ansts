import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
/** Join nested synchronous domain writes to one native transaction; never cross an await. */
export function joinedTransactions(storage:DirectoryStorage):DirectoryStorage {
 let depth=0;return {sql:storage.sql,transactionSync<T>(fn:()=>T):T{if(depth)return fn();return storage.transactionSync(()=>{depth++;try{return fn();}finally{depth--;}});}};
}
export interface FinopsPage {limit?:number;cursor?:string}
export function pageWindow(actor:ControlActor,kind:string,version:number,maximum:number,options:FinopsPage,secret:string){
 if(!secret||secret.length<32)throw new ControlError('FINOPS_CURSOR_UNAVAILABLE',503);
 const limit=options.limit??25;if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new ControlError('INVALID_PAGE_LIMIT',422);
 const scope=createHash('sha256').update(JSON.stringify([actor.sub,actor.role,actor.scopeTenant,actor.scopeRegion,kind,version])).digest('hex');
 let after=0,upper=maximum;
 if(options.cursor){try{
  if(options.cursor.length>2048||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/.test(options.cursor))throw new Error();
  const [raw,sig]=options.cursor.split('.'),actual=Buffer.from(sig,'base64url'),expected=createHmac('sha256',secret).update('finops-page-v1.'+raw).digest();
  if(actual.toString('base64url')!==sig||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new Error();
  const c=JSON.parse(Buffer.from(raw,'base64url').toString());if(c.scope!==scope)throw new ControlError('CURSOR_STALE',409);
  if(!Number.isSafeInteger(c.after)||!Number.isSafeInteger(c.upper)||c.after<0||c.after>c.upper||c.upper>maximum)throw new Error();after=c.after;upper=c.upper;
 }catch(error){if(error instanceof ControlError)throw error;throw new ControlError('INVALID_CURSOR',422);}}
 return {after,upper,limit,next(last:number,more:boolean){if(!more)return null;const raw=Buffer.from(JSON.stringify({scope,after:last,upper})).toString('base64url');return raw+'.'+createHmac('sha256',secret).update('finops-page-v1.'+raw).digest('base64url');}};
}
