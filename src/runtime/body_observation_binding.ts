/** Resolve an existing shadow context. A lookup is never an execution or capacity grant. */
import {canonicalBodyEvidence,canonicalBodyObservationLookup,type BodyObservationLookup,type BodyObservationBinding} from '../control/body_execution_evidence.js';
export async function lookupBodyObservationBinding(input:BodyObservationLookup,hostId:string,request:(input:BodyObservationLookup)=>Promise<unknown>):Promise<BodyObservationBinding|null> {
 input=canonicalBodyObservationLookup(input);
 const result=await request(input) as {binding:BodyObservationBinding|null;protocolMode:string;enforcement:string};
 if(!result||result.protocolMode!=='SHADOW'||result.enforcement!=='NOT_CONNECTED')throw new Error('INVALID_OBSERVATION_BINDING_MODE');
 if(result.binding===null)return null;
 if(!result.binding||result.binding.executionInputDigest!==input.executionInputDigest)throw new Error('INVALID_OBSERVATION_BINDING');
 // Apply the same allowlist and bounds as the Daemon before handing it the binding.
 const validated=canonicalBodyEvidence({...result.binding,...input,hostId,schemaVersion:1,purpose:'ansts-body-execution-observation-v1',sequence:1,phase:'UNCERTAIN',executionObserved:false,observedAt:0});
 return {reservationId:validated.reservationId,generation:validated.generation,policyVersion:validated.policyVersion,tenantVersion:validated.tenantVersion,requestDigest:validated.requestDigest,executionInputDigest:validated.executionInputDigest};
}
