/** Authenticated shadow-protocol credentials. They are deliberately not Body execution grants. */
import {createHmac,timingSafeEqual} from 'node:crypto';
import {ControlError} from './contracts.js';
import type {AdmissionReservation} from './admission_reservations.js';
export interface AdmissionCredentialClaims {
 schemaVersion:1;purpose:'ansts-admission-shadow-v1';reservationId:string;tenantId:string;
 taskId:string;attemptId:string;target:string;fencing:number;generation:number;
 reservationVersion:number;policyVersion:number;tenantVersion:number;requestDigest:string;
 resources:AdmissionReservation['resources'];expiresAt:number;
}
function configured(key:string):void {
 if(typeof key!=='string'||!/^[a-f0-9]{64}$/.test(key))throw new ControlError('ADMISSION_SIGNING_UNAVAILABLE',503);
}
function claimsFor(reservation:AdmissionReservation):AdmissionCredentialClaims {
 return {schemaVersion:1,purpose:'ansts-admission-shadow-v1',reservationId:reservation.id,tenantId:reservation.tenantId,taskId:reservation.taskId,attemptId:reservation.attemptId,target:reservation.target!,fencing:reservation.fencing,generation:reservation.generation,reservationVersion:reservation.version,policyVersion:reservation.policy.version,tenantVersion:reservation.policy.tenantVersion,requestDigest:reservation.requestDigest,resources:reservation.resources,expiresAt:reservation.expiresAt};
}
function available(reservation:AdmissionReservation,now:number,target:string):void {
 if(!Number.isSafeInteger(now)||!Number.isSafeInteger(reservation.expiresAt)||now>=reservation.expiresAt||now<reservation.createdAt)throw new ControlError('ADMISSION_EXPIRED',409);
 if(reservation.phase!=='START_AUTHORIZED'||!reservation.startAuthorized||!reservation.capacityReceipt||!reservation.target||reservation.target!==target)throw new ControlError('ADMISSION_NOT_AUTHORIZED',409);
}
export function issueAdmissionCredential(reservation:AdmissionReservation,key:string,now:number):string {
 configured(key);available(reservation,now,reservation.target!);
 const payload=Buffer.from(JSON.stringify(claimsFor(reservation))).toString('base64url');
 const signature=createHmac('sha256',Buffer.from(key,'hex')).update('ansts-admission-shadow-v1.'+payload).digest('base64url');
 return payload+'.'+signature;
}
/** Must receive a fresh authoritative reservation. Offline signature verification cannot prove revocation. */
export function verifyAdmissionCredential(token:string,key:string,current:AdmissionReservation,now:number,target:string):AdmissionCredentialClaims {
 configured(key);
 if(typeof token!=='string'||token.length>8192||!token.match(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/))throw new ControlError('INVALID_ADMISSION_CREDENTIAL',401);
 const [payload,signature]=token.split('.');
 const expected=createHmac('sha256',Buffer.from(key,'hex')).update('ansts-admission-shadow-v1.'+payload).digest();
 const actual=Buffer.from(signature,'base64url');
 if(signature!==actual.toString('base64url')||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new ControlError('INVALID_ADMISSION_CREDENTIAL',401);
 // Compare to server-generated canonical bytes: no caller-selected algorithm, claims, or unsigned fields.
 const canonical=Buffer.from(JSON.stringify(claimsFor(current))).toString('base64url');
 if(payload!==canonical)throw new ControlError('STALE_ADMISSION_CREDENTIAL',409);
 available(current,now,target);
 return structuredClone(claimsFor(current));
}
