import {test} from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'node:http';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {ContainerSandbox} from '../../src/runtime/container_sandbox.js';
for(const cleanupStatus of [204,404])test('lost Engine create response with cleanup '+cleanupStatus+' emits only confirmed facts',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-engine-evidence-')),socket=join(directory,'engine.sock');let started=0,stopped=0;
 const engine=http.createServer((request,response)=>{
  request.resume();if(request.method==='POST'&&request.url?.startsWith('/v1.45/containers/create')){request.on('end',()=>response.destroy());return;}
  if(request.method==='DELETE'){response.writeHead(cleanupStatus);response.end();return;}response.writeHead(500);response.end();
 });
 await new Promise<void>(resolve=>engine.listen(socket,resolve));
 try{
  const runtime=new ContainerSandbox('node@sha256:'+'a'.repeat(64),socket);
  await assert.rejects(runtime.execute({argv:['true'],cpu:.25,memoryMb:128,timeoutMs:1000},new AbortController().signal,undefined,{started(){started++;},stopped(){stopped++;}}),cleanupStatus===404?/BODY_STOP_UNCONFIRMED/:/socket hang up/);
  assert.equal(started,0);assert.equal(stopped,cleanupStatus===204?1:0);
 }finally{await new Promise<void>(resolve=>engine.close(()=>resolve()));rmSync(directory,{recursive:true,force:true});}
});
