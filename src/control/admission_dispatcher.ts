/** At-least-once shadow delivery; receiver recording is not proof of execution or capacity. */
import {ControlError,type ControlActor} from './contracts.js';
import type {TenantRecord} from './tenant_directory.js';
import type {AdmissionReservationStore} from './admission_reservations.js';
import type {AdmissionMessage,AdmissionMessageReceipt} from './admission_messages.js';
export interface AdmissionTransport {deliver(message:AdmissionMessage,signal:AbortSignal):Promise<AdmissionMessageReceipt>}
export async function dispatchAdmissionMessage(store:AdmissionReservationStore,actor:ControlActor,tenant:TenantRecord,transport:AdmissionTransport,clock=Date.now):Promise<{eventId:string;status:string}|null> {
 const lease=store.claimMessage(actor,tenant,clock());if(!lease)return null;
 const controller=new AbortController();let timeout:ReturnType<typeof setTimeout>|undefined,receipt:AdmissionMessageReceipt|null;
 try {
  receipt=await Promise.race([transport.deliver(structuredClone(lease.message),controller.signal),new Promise<never>((_,reject)=>{timeout=setTimeout(()=>{controller.abort();reject(new Error('DELIVERY_TIMEOUT'));},3000);})]);
 }catch{receipt=null;}
 finally{if(timeout)clearTimeout(timeout);}
 try{const finished=store.finishMessage(actor,tenant,lease.message.eventId,lease.attempts,receipt,clock());return {eventId:lease.message.eventId,status:finished.status};}
 catch(error){if(error instanceof ControlError&&error.code==='STALE_DELIVERY_LEASE')return {eventId:lease.message.eventId,status:'STALE_DELIVERY_LEASE'};throw error;}
}
