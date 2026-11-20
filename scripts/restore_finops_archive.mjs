/** Offline recovery only: refuses an existing destination and never launches control/alarm/execution services. */
import {readFileSync,statSync,openSync,closeSync,fsyncSync,unlinkSync} from 'node:fs';
import {dirname,resolve} from 'node:path';
import {createRequire} from 'node:module';
const [archivePath,keyPath,destination,kind,tenantId]=process.argv.slice(2);
if(!archivePath||!keyPath||!destination||!['DIRECTORY','TENANT'].includes(kind)||kind==='TENANT'&&!tenantId){throw new Error('Usage: node scripts/restore_finops_archive.mjs <archive.json> <verification-key-file> <new-offline.sqlite> <DIRECTORY|TENANT> [tenantId]');}
if(statSync(archivePath).size>32*1024*1024+65536||statSync(keyPath).size>4096)throw new Error('Input size exceeds offline recovery limit');
const require=createRequire(import.meta.url),{restoreIntoEmptyOfflineDatabase,verifyAuthorityArchive}=require('../dist/src/finops/authority_archive.js'),{UsageCostLedger}=require('../dist/src/finops/usage_cost_ledger.js'),{DatabaseSync}=require('node:sqlite');
const archive=JSON.parse(readFileSync(archivePath,'utf8')),key=readFileSync(keyPath,'utf8').trim();verifyAuthorityArchive(archive,key);
const target=resolve(destination);let created=false,db;
try{
 const fd=openSync(target,'wx',0o600);created=true;closeSync(fd);db=new DatabaseSync(target);
 const storage={sql:{exec(query,...args){const rows=db.prepare(query).all(...args);return {toArray:()=>rows};}},transactionSync(fn){db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}};
 const recovered=restoreIntoEmptyOfflineDatabase(storage,archive,key,{kind,tenantId:kind==='TENANT'?tenantId:null});
 const summary=kind==='TENANT'?new UsageCostLedger(storage).summary({sub:'offline:archive-verifier',role:'SRE_ADMIN',scopeTenant:tenantId,scopeRegion:'all'},archive.snapshot.tenant):undefined;
 db.close();db=undefined;const result=openSync(target,'r');try{fsyncSync(result);}finally{closeSync(result);}const parent=openSync(dirname(target),'r');try{fsyncSync(parent);}finally{closeSync(parent);}
 console.log(JSON.stringify({...recovered,summary}));
}catch(error){db?.close();if(created)unlinkSync(target);throw error;}
