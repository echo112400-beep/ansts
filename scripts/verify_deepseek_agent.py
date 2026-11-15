"""Real DeepSeek -> restricted workspace edit -> real Engine test -> authorized artifacts."""
import base64
import json
import time
from verify_body_split import request, token, wait
from verify_execution_lifecycle import IMAGE, BASE

tenant = 'default'
headers = {'Authorization': token(tenant)}
identity = f'deepseek-agent-{time.time_ns()}'
test_source = "const assert=require('node:assert/strict');assert.equal(require('./sum')(2,3),5);assert.equal(require('./sum')(-1,1),0);console.log('sum-test-passed');"
status, submitted = request(BASE + '/v1/tasks', {
    'tenant_id': tenant, 'task_id': identity,
    'prompt': 'Fix the bug in sum.js so it adds its two inputs. Read the supplied files first. Do not change test.js. Run node test.js to verify the fix, then finish.',
    'timeout_seconds': 180,
    'resource_profile': {'think_cpu_cores': 0.5, 'act_cpu_cores': 0.5, 'act_memory_mb': 128, 'disk_mb': 32},
    'execution': {'kind': 'agent', 'image': IMAGE, 'timeoutMs': 150000, 'cpu': 0.5, 'memoryMb': 128, 'maxTurns': 8, 'maxTokens': 32768, 'files': {'sum.js': 'module.exports=(a,b)=>a-b;', 'test.js': test_source}},
}, headers)
assert status == 202, submitted
url = BASE + f'/v1/tasks/{identity}?tenantId={tenant}'
deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    status, value = request(url + '&includeResult=1', headers=headers)
    if status == 200 and value.get('phase') == 'COMPLETED':
        result = value['result']
        assert result['executionKind'] == 'agent' and result['executionOutcome'] == 'SUCCEEDED', result
        assert 'sum-test-passed' in result['logs'], result
        artifacts = {item['path']: base64.b64decode(item['contentBase64']).decode() for item in result['artifacts']}
        assert artifacts['test.js'] == test_source, 'model changed the verification test'
        assert artifacts['sum.js'] != 'module.exports=(a,b)=>a-b;', artifacts
        assert request(url + '&includeResult=1', headers={'Authorization': token('other-tenant')})[0] in (403, 404)
        print(json.dumps({'deepseekAgent': 'passed', 'taskId': identity, 'turns': len(result['agentTurns']), 'realModel': True, 'realContainerTest': True, 'originalTestPreserved': True, 'artifacts': sorted(artifacts)}))
        break
    assert value.get('phase') != 'UNCERTAIN', value
    time.sleep(1)
else:
    raise AssertionError('DeepSeek agent timed out')
