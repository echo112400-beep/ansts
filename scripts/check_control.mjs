import { mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { spawnSync } from 'node:child_process';
const root = resolve(import.meta.dirname, '..');
const output = mkdtempSync(join(tmpdir(), 'ansts-control-check-'));
function run(command,args) {
  const result=spawnSync(command,args,{cwd:root,stdio:'inherit'});
  if(result.error) throw result.error;
  if(result.status!==0) throw new Error(`${command} failed (${result.status})`);
}
try {
  run(process.execPath,['node_modules/typescript/bin/tsc','-p','tsconfig.control.json','--pretty','false']);
  const tests=readdirSync(join(root,'tests/control')).filter(f=>f.endsWith('.test.ts')).map(f=>'tests/control/'+f).concat('tests/monitoring_daemon.test.ts');
  const bundles=[];
  for(const test of tests) {
    const target=join(output,basename(test).replace(/\.ts$/,'.mjs'));
    run(join(root,'node_modules/.bin/esbuild'),[test,'--bundle','--platform=node','--format=esm','--outfile='+target]);
    bundles.push(target);
  }
  run(process.execPath,['--test',...bundles]);
} finally {rmSync(output,{recursive:true,force:true});}
