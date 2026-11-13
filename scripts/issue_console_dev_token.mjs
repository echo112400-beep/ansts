/** Issue an explicitly scoped, short-lived development credential to an ignored local file. */
import { writeFileSync, chmodSync } from 'node:fs';
import { gatewayDevAuthorization } from './gateway_dev_identity.mjs';
const args = process.argv.slice(2);
function value(flag) { const index = args.indexOf(flag); return index >= 0 ? args[index + 1] : undefined; }
const tenant = value('--tenant'), role = value('--role'), subject = value('--subject');
if (!tenant?.trim() || tenant.length > 128 || !subject?.trim() || subject.length > 128 || !['VIEWER', 'OPERATOR', 'SRE_ADMIN'].includes(role)) throw new Error('usage: --tenant <tenant|all> --role <VIEWER|OPERATOR|SRE_ADMIN> --subject <identity>');
const target = new URL('../.dev-secrets/console-login.jwt', import.meta.url);
writeFileSync(target, gatewayDevAuthorization(tenant, role, undefined, subject), { mode: 0o600 });
chmodSync(target, 0o600);
console.log('Wrote a 15-minute credential to .dev-secrets/console-login.jwt (value hidden). Paste its contents into the console login form.');
