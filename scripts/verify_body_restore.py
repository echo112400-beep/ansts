"""Restore the current fleet snapshot to an isolated celld host; never switch the live authority."""
import json
import subprocess
import sys
from verify_body_split import ROOT, request, wait

fixture = 'ansts-body-restore-drill'
network = next(iter(json.loads(subprocess.check_output(['docker', 'inspect', '--format', '{{json .NetworkSettings.Networks}}', 'ansts-body-celld']))))
states = []
try:
    for host, port in [('body-host-01', 19501), ('body-host-02', 19502)]:
        health = request(f'http://127.0.0.1:{port}/health')[1]
        headers = {'Authorization': 'Body ' + (ROOT / f'.dev-secrets/{host}.token').read_text().strip(), 'x-ansts-body-id': host}
        states.append((port, headers, health['draining']))
        assert request(f'http://127.0.0.1:{port}/v1/daemon/drain', {'draining': True}, headers)[0] == 200
        wait(lambda: request(f'http://127.0.0.1:{port}/health')[1]['drained'])
    subprocess.run(['docker', 'run', '-d', '--name', fixture, '--network', network, '-p', '127.0.0.1:18086:8080', '--mount', f'type=bind,src={ROOT},dst=/app,readonly', '-e', 'ANSTS_BODY_AUTH_SECRET_FILE=/app/.dev-secrets/body-signing.key', '-e', 'ANSTS_SCHEDULER_URL=http://celld-brain-01:8081', '-e', 'CELLD_ESBUILD=/app/node_modules/@esbuild/linux-x64/bin/esbuild', '--entrypoint', '/usr/bin/bash', 'ghcr.io/denoland/celld:0.6.3', '/app/deploy/dev-cluster/start-celld-body.sh'], check=True, stdout=subprocess.DEVNULL)
    wait(lambda: request('http://127.0.0.1:18086/readyz')[0] == 200)
    subprocess.run([sys.executable, str(ROOT / 'scripts/migrate_body_to_fleet.py'), '--source', 'http://127.0.0.1:18082', '--target', 'http://127.0.0.1:18086', '--snapshot', str(ROOT / '.dev-secrets/body-restore-drill.json')], check=True)
    print(json.dumps({'bodySnapshotRestore': 'verified in isolated celld host', 'liveAuthority': 'unchanged', 'fixtureRemoved': True}))
finally:
    subprocess.run(['docker', 'rm', '-f', fixture], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for port, headers, was_draining in states:
        wait(lambda: request(f'http://127.0.0.1:{port}/v1/daemon/drain', {'draining': was_draining}, headers)[0] == 200)
