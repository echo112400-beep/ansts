"""Copy only explicitly needed provider credentials; never source or print the supplied env file."""
import argparse
import json
import os
from pathlib import Path
import re

parser = argparse.ArgumentParser()
parser.add_argument('--env-file', required=True)
parser.add_argument('--github-user-id', required=True)
parser.add_argument('--origin', default='http://127.0.0.1:8088')
parser.add_argument('--output-dir', type=Path, default=Path(__file__).resolve().parent.parent / '.dev-secrets')
parser.add_argument('--github-client-env-prefix', choices=['auto', 'TINYAGENT_GITHUB', 'TINYAGENT_VUE_GITHUB'], default='auto')
args = parser.parse_args()
assert args.github_user_id.isdigit()
values = {}
for line in Path(args.env_file).read_text().splitlines():
    match = re.match(r'^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$', line)
    if not match:
        continue
    value = match.group(2)
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    values[match.group(1)] = value
prefix = args.github_client_env_prefix
if prefix == 'auto':
    prefix = 'TINYAGENT_VUE_GITHUB' if values.get('TINYAGENT_VUE_GITHUB_ID') and values.get('TINYAGENT_VUE_GITHUB_SECRET') else 'TINYAGENT_GITHUB'
github_id = values.get(prefix + '_ID', '')
github_secret = values.get(prefix + '_SECRET', '')
if not github_id or not github_secret:
    raise RuntimeError('missing GitHub credential pair: ' + prefix)
root = args.output_dir
root.mkdir(mode=0o700, exist_ok=True)
def write(name, content):
    if not content or '\n' in content or '\r' in content:
        raise RuntimeError('missing or invalid provider setting: ' + name)
    target = root / name
    temporary = root / (name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, target)

write('deepseek.key', values['DEEPSEEK_API_KEY'])
write('github-client-secret', github_secret)
write('github-identities.json', json.dumps({args.github_user_id: {'role': 'SRE_ADMIN', 'scopeTenant': 'default'}}))
write('providers.env', 'ANSTS_AGENT_ENABLED=true')
# Nonsecret deployment settings are held alongside the secret files and ignored by git.
target = root / 'providers.env'
with target.open('a') as output:
    output.write('\nANSTS_GITHUB_CLIENT_ID=' + github_id)
    output.write('\nANSTS_GITHUB_CLIENT_SECRET_FILE=/run/secrets/github-client-secret')
    output.write('\nANSTS_GITHUB_IDENTITIES_FILE=/run/secrets/github-identities.json\n')
    output.write('ANSTS_PUBLIC_ORIGIN=' + args.origin + '\n')
print('Imported DeepSeek key and GitHub client credentials (values hidden). GitHub user is scoped to tenant default; the application callback must be registered.')
