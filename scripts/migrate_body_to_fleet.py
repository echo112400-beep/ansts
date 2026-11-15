"""Quiesced, compare-after-import migration. Existing conflicting target state is never overwritten."""
import argparse
import json
import os
from pathlib import Path
from urllib.parse import quote
from verify_body_split import ROOT, request

parser = argparse.ArgumentParser()
parser.add_argument('--source', default='http://127.0.0.1:18085')
parser.add_argument('--target', default='http://127.0.0.1:18082')
parser.add_argument('--scheduler', default='http://127.0.0.1:18082')
parser.add_argument('--snapshot', default=str(ROOT / '.dev-secrets/body-fleet-migration.json'))
args = parser.parse_args()
operator = {'Authorization': 'Operator ' + (ROOT / '.dev-secrets/body-operator.token').read_text().strip()}
gateway = {'Authorization': 'Gateway ' + (ROOT / '.dev-secrets/gateway-control.token').read_text().strip()}
for port in (19501, 19502):
    status, health = request(f'http://127.0.0.1:{port}/health')
    assert status == 200 and health['draining'] and health['drained'], 'drain both Daemons before migration'
status, listing = request(args.scheduler + '/v1/scheduler/tasks', headers=gateway)
assert status == 200, listing
identities = ['hostId=body-host-01', 'hostId=body-host-02'] + ['taskId=' + quote(task['task_id'], safe='') for task in listing['tasks']]
snapshots = []
for identity in dict.fromkeys(identities):
    status, snapshot = request(args.source + '/v1/body/export?' + identity, headers=operator)
    assert status == 200 and snapshot.get('schema_version') == 1, snapshot
    for row in snapshot['rows']:
        if row['id'] == 'execution':
            execution = json.loads(row['payload'])
            if execution.get('schema_version') == 1:
                assert execution.get('phase') in ('COMPLETED', 'UNCERTAIN'), 'finish result delivery before migration'
    snapshots.append({'identity': identity, 'snapshot': snapshot})
target = Path(args.snapshot)
fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, 'w') as output:
    json.dump(snapshots, output)
    output.flush()
    os.fsync(output.fileno())
canonical = lambda rows: sorted((row['id'], json.dumps(json.loads(row['payload']), sort_keys=True, separators=(',', ':'))) for row in rows)
count = 0
for entry in snapshots:
    identity, snapshot = entry['identity'], entry['snapshot']
    if not snapshot['rows']:
        continue
    status, result = request(args.target + '/v1/body/import?' + identity, snapshot, operator)
    assert status == 200, result
    status, restored = request(args.target + '/v1/body/export?' + identity, headers=operator)
    assert status == 200 and canonical(restored['rows']) == canonical(snapshot['rows']), 'target conflict: keep source quiesced and investigate'
    count += 1
print(json.dumps({'bodyFleetMigration': 'verified', 'cells': count, 'sourcePreserved': True, 'snapshot': str(target)}))
if args.target == 'http://127.0.0.1:18082':
    marker = ROOT / '.dev-secrets/body-fleet-migration.verified'
    temporary = Path(str(marker) + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write('body-ha-v1:\n'); output.flush(); os.fsync(output.fileno())
    os.replace(temporary, marker)
    directory = os.open(marker.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
