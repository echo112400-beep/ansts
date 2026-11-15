"""Repeat the deployed failure matrix and persist each command's evidence."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from verify_body_split import ROOT

parser = argparse.ArgumentParser()
parser.add_argument('--rounds', type=int, default=3)
parser.add_argument('--output', default='/tmp/ansts-fault-matrix.json')
args = parser.parse_args()
assert 1 <= args.rounds <= 10
evidence = []
for index in range(args.rounds):
    commands = [
        ['node', '--test', 'dist/tests/container_failure_matrix.test.js', 'dist/tests/body_result_outbox.test.js', 'dist/tests/body_resource_ledger.test.js'],
        [sys.executable, 'scripts/verify_execution_lifecycle.py', '--rounds', '1'],
        [sys.executable, 'scripts/verify_body_drain.py'],
        [sys.executable, 'scripts/verify_daemon_crash.py'],
        [sys.executable, 'scripts/verify_control_ha.py'],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=240)
        evidence.append({'round': index + 1, 'command': command, 'exitCode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        Path(args.output).write_text(json.dumps(evidence, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError('fault matrix failed; inspect ' + args.output)
    print(json.dumps({'faultMatrixRound': index + 1, 'status': 'passed', 'scenarios': ['create-response-loss', 'cleanup-failure', 'persistent-resources', 'outbox-retry', 'normal-and-nonzero', 'timeout', 'cancel', 'tenant-isolation', 'drain-restart', 'daemon-SIGKILL', 'orphan-recovery', 'stale-fence', 'Gateway-restart', 'Brain-entry-failover', 'legacy-Body-listener-failure']}), flush=True)
