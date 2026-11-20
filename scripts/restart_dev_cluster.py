#!/usr/bin/env python3
"""Drain, rebuild and restart the real development fleet without deleting volumes."""
import re
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
from verify_m3_historical_scheduler import verify as verify_historical_scheduler

ROOT = Path(__file__).resolve().parent.parent
PRIVATE = ROOT / '.dev-secrets/redeployment'
PRIVATE.mkdir(mode=0o700, parents=True, exist_ok=True)
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def save(name, value):
    path = PRIVATE / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.flush()
        os.fsync(output.fileno())


def request(url, body=None, headers=None):
    req = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), headers={'content-type': 'application/json', **(headers or {})})
    try:
        with HTTP.open(req, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def compose_args():
    files = ['docker-compose.dev-cluster.yml', 'docker-compose.sandbox.yml']
    if (ROOT / '.dev-secrets/providers.env').exists():
        files.append('docker-compose.providers.yml')
    verified = ROOT / '.dev-secrets/body-fleet-migration.verified'
    if verified.exists():
        if verified.read_text().strip() != 'body-ha-v1:':
            raise RuntimeError('Unknown Body migration marker; preserve running authority')
        files.append('docker-compose.control-ha.yml')
    elif (ROOT / '.dev-secrets/body-fleet-migration.json').exists():
        raise RuntimeError('Body migration snapshot exists without verification; refusing authority fallback')
    return ['docker', 'compose', '-p', 'ansts-v25-dev-cluster', *[item for file in files for item in ('-f', str(ROOT / 'deploy/dev-cluster' / file))]]


def run(compose, args, label):
    print(label, flush=True)
    with (PRIVATE / 'commands.log').open('a') as output:
        os.chmod(PRIVATE / 'commands.log', 0o600)
        result = subprocess.run([*compose, *args], cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f'{label} failed; inspect private .dev-secrets/redeployment/commands.log')


def wait(predicate, label, timeout=180):
    deadline, last_progress = time.monotonic() + timeout, 0
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, urllib.error.URLError):
            pass
        if time.monotonic() - last_progress > 10:
            print(f'等待：{label}', flush=True)
            last_progress = time.monotonic()
        time.sleep(1)
    raise RuntimeError(f'Timed out: {label}')


def normalized_tasks():
    auth = 'Gateway ' + (ROOT / '.dev-secrets/gateway-control.token').read_text().strip()
    status, payload = request('http://127.0.0.1:18082/v1/scheduler/tasks', headers={'authorization': auth})
    if status != 200:
        raise RuntimeError('Cannot read the current Scheduler before maintenance')
    return {task['task_id']: {'tenant': task['tenant_id'], 'executionDigest': hashlib.sha256(json.dumps(task.get('execution'), sort_keys=True).encode()).hexdigest()} for task in payload['tasks']}


def body_auth(index):
    node = f'body-host-0{index}'
    return {'authorization': 'Body ' + (ROOT / f'.dev-secrets/{node}.token').read_text().strip(), 'x-ansts-body-id': node}


def body_health(index):
    return request(f'http://127.0.0.1:{19500 + index}/health')[1]


def drain(original):
    for index in (1, 2):
        health = body_health(index)
        original[str(index)] = health['draining']
        if not health['draining']:
            status, _ = request(f'http://127.0.0.1:{19500 + index}/v1/daemon/drain', {'draining': True}, body_auth(index))
            if status != 200:
                raise RuntimeError(f'Drain rejected for Body {index}')
    save('drain-before.json', original)
    for index in (1, 2):
        wait(lambda i=index: (h := body_health(i))['drained'] and not h.get('recoveryRequired') and h.get('activeSandboxes') == 0, f'Body {index} 任务与资源排空', 300)
    print('两台执行器已排空；没有终止用户任务。', flush=True)


def resume(original):
    for key, was_draining in original.items():
        index = int(key)
        if not was_draining:
            wait(lambda i=index: request(f'http://127.0.0.1:{19500 + i}/v1/daemon/drain', {'draining': False}, body_auth(i))[0] == 200, f'Body {index} 恢复准入')
            wait(lambda i=index: request(f'http://127.0.0.1:{19500 + i}/readyz')[0] == 200, f'Body {index} 就绪')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--build-only', action='store_true')
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--stop-only', action='store_true')
    parser.add_argument('--verify-retired-m3-shard', action='store_true')
    args = parser.parse_args()
    if (PRIVATE / 'rivet-cutover-active.json').exists() and not args.check_only:
        raise RuntimeError('Development traffic uses Rivet. Use the cutover Compose configuration to maintain the fleet.')
    compose = compose_args()
    run(compose, ['config', '--quiet'], '校验完整 Compose 配置')
    resolved = json.loads(subprocess.check_output([*compose, 'config', '--format', 'json'], cwd=ROOT))
    for index in (1, 2, 3):
        if resolved['services'][f'celld-brain-0{index}'].get('image') != 'ghcr.io/denoland/celld:0.6.3':
            raise RuntimeError('Refusing to replace the real celld Fleet with legacy simulator images')
    deployment_env = resolved['services']['celld-deploy'].get('environment', {})
    admission_mode = deployment_env.get('ANSTS_ADMISSION_MODE') or 'observe'
    def deploy_args():
        command = ['run', '--rm', '--no-deps']
        if args.verify_retired_m3_shard:
            if (deployment_env.get('ANSTS_RUNTIME_PROFILE'), deployment_env.get('CELLD_BUCKET'),
                deployment_env.get('S3_ENDPOINT')) != ('development', 's3://ansts-fleet', 'http://rustfs:9000'):
                raise RuntimeError('Historical Scheduler evidence is only valid for the local development Fleet')
            command += ['-e', 'ANSTS_DEV_VERIFIED_RETIRED_SCHEDULER=' + verify_historical_scheduler()]
        return [*command, 'celld-deploy']

    if admission_mode not in ('observe', 'shadow', 'enforce'):
        raise RuntimeError('Invalid admission mode')
    if admission_mode == 'enforce':
        pilot_shards = (deployment_env.get('ANSTS_ADMISSION_SCHEDULER_SHARDS') or '').split(',')
        pilot_tenants = (deployment_env.get('ANSTS_ADMISSION_PILOT_TENANTS') or '').split(',')
        pilot_pool = deployment_env.get('ANSTS_ADMISSION_PILOT_POOL') or ''
        pilot_estimate = deployment_env.get('ANSTS_ADMISSION_ESTIMATED_CREDIT_UNITS') or ''
        valid_ids = lambda values: all(re.fullmatch(r'[a-zA-Z0-9_-]+', value) for value in values) and len(set(values)) == len(values)
        valid_shards = all(re.fullmatch(r'[a-zA-Z0-9_-]+|id:[a-f0-9]{64}', value) for value in pilot_shards)
        if not valid_shards or len(set(pilot_shards)) != len(pilot_shards) or 'global-shard-0' not in pilot_shards or len(pilot_shards) > 32:
            raise RuntimeError('Enforce requires a unique scheduler shard roster including global-shard-0')
        if not valid_ids(pilot_tenants) or not valid_ids([pilot_pool]) or not re.fullmatch(r'[1-9][0-9]{0,77}', pilot_estimate):
            raise RuntimeError('Enforce requires pilot tenants, a capacity pool and a positive credit estimate')
    def verify_pilot_metering():
        for name in ('body-daemon-01', 'body-daemon-02'):
            env = resolved['services'][name].get('environment', {})
            metered = set((env.get('ANSTS_USAGE_TENANTS') or '').split(','))
            if env.get('ANSTS_USAGE_METERING') != 'shadow' or not set(pilot_tenants) <= metered:
                raise RuntimeError(f'{name} must enable usage metering for every enforce pilot tenant')
        token = (ROOT / '.dev-secrets/directory-service.key').read_text().strip()
        actor = {'sub': 'system:dev-pilot-preflight', 'role': 'SRE_ADMIN',
                 'scopeTenant': 'all', 'scopeRegion': 'all'}
        for tenant in pilot_tenants:
            status, value = request('http://127.0.0.1:18082/internal/v1/tenant-control/rate-read',
                                    {'actor': actor, 'tenantId': tenant}, {'Authorization': 'Bearer ' + token})
            if status != 200 or value.get('effective') is not True or not value.get('current'):
                raise RuntimeError(f'Enforce pilot tenant {tenant} requires an effective rate card')

    # Shadow admission identities belong to the Worker deployment, never the public Gateways.
    admission_files = ('ANSTS_ADMISSION_SERVICE_TOKEN_FILE', 'ANSTS_ADMISSION_SIGNING_KEY_FILE')
    for variable in admission_files:
        if not resolved['services']['celld-deploy'].get('environment', {}).get(variable):
            raise RuntimeError('Worker deployment is missing an admission credential file binding')
        for gateway in ('ansts-gateway-01', 'ansts-gateway-02'):
            if resolved['services'][gateway].get('environment', {}).get(variable):
                raise RuntimeError('Admission credential files must not be configured on public Gateways')
    admission_keys = []
    for filename in ('admission-service.key', 'admission-signing.key'):
        path = ROOT / '.dev-secrets' / filename
        if not path.is_file() or path.stat().st_mode & 0o077:
            raise RuntimeError('Admission credential files must exist with private permissions before restart')
        value = path.read_text().strip()
        if len(value) != 64 or any(character not in '0123456789abcdef' for character in value):
            raise RuntimeError('Invalid admission credential file format')
        admission_keys.append(value)
    if len(set(admission_keys)) != 2:
        raise RuntimeError('Coordinator and signing identities must be independent')
    if not (ROOT / '.dev-secrets/user-signing.key').is_file():
        raise RuntimeError('Existing identity secrets required; refusing to invent replacement keys')
    if '.dev-secrets/' not in (ROOT / '.dockerignore').read_text().splitlines():
        raise RuntimeError('Private secrets must be excluded from Docker context')
    for service in resolved['services'].values():
        for mount in service.get('volumes', []):
            if mount.get('type') != 'bind':
                continue
            source, target = Path(mount['source']), mount['target']
            if not source.exists():
                raise RuntimeError(f'Missing bind source: {source}')
            if (target.endswith(('.json', '.conf', '.yml', '.mjs', '.token', '.key')) or target.startswith('/run/secrets/')) and not source.is_file():
                raise RuntimeError(f'Bind source must be a file: {source}')
    pending = PRIVATE / 'stopped-state.json'
    resuming = pending.exists()
    if admission_mode == 'enforce' and not (args.stop_only or args.build_only or resuming):
        verify_pilot_metering()
        run(compose, [*deploy_args(), '--check-release-inputs'],
            '发布前核对共享 Scheduler 名单与 Worker 配置')
    if args.check_only:
        print('配置校验通过；未变更运行中的服务。')
        return
    if not args.skip_build and not args.stop_only:
        run(compose, ['build', 'dev-server', 'body-daemon-01', 'body-daemon-02'], '构建 Next.js 与原生执行器镜像（保留现有服务运行）')
    if args.build_only:
        return
    original = {}
    stopped = resuming
    try:
        if resuming:
            state = json.loads(pending.read_text())
            original, before = state['drain'], state['tasks']
            print('续接已有维护状态，保留最初的 Drain 意图与任务快照。', flush=True)
            for index in (1, 2):
                try:
                    health = body_health(index)
                    if not health['draining']:
                        status, _ = request(f'http://127.0.0.1:{19500 + index}/v1/daemon/drain', {'draining': True}, body_auth(index))
                        if status != 200:
                            raise RuntimeError(f'Drain rejected for Body {index}')
                    wait(lambda i=index: (h := body_health(i))['drained'] and not h.get('recoveryRequired'), f'Body {index} 续接前排空', 300)
                except urllib.error.URLError:
                    pass  # A saved maintenance snapshot permits recovery of a stopped host.
        else:
            body_health(1)
            body_health(2)
            drain(original)
            before = normalized_tasks()
            save('stopped-state.json', {'drain': original, 'tasks': before})
        if not resuming or args.stop_only:
            stopped = True
            run(compose, ['stop', '-t', '30', 'tanstack-console', 'dev-server', 'ansts-gateway-01', 'ansts-gateway-02', 'body-daemon-01', 'body-daemon-02', 'body-celld'], '停止入口和已排空的执行器')
            run(compose, ['stop', '-t', '30', 'celld-brain-01', 'celld-brain-02', 'celld-brain-03'], '停止 Fleet 实例，保留状态卷')
            run(compose, ['stop', '-t', '30', 'rustfs', 'prometheus', 'grafana'], '停止存储和观测服务，保留全部卷')
        if args.stop_only:
            print('开发集群已安全停止；未删除容器状态或持久卷。')
            return
        force = [] if resuming else ['--force-recreate']
        def up(services, label):
            run(compose, ['up', '-d', '--no-deps', *force, '--wait', '--wait-timeout', '180', *services], label)
        up(['rustfs'], '恢复对象存储')
        up(['celld-brain-01', 'celld-brain-02', 'celld-brain-03'], '恢复三节点 celld Fleet')
        if admission_mode == 'enforce':
            verify_pilot_metering()
        run(compose, deploy_args(), '部署已回归验证的 Worker，保留原命名空间')
        up(['body-celld'], '恢复 Body 兼容监听器')
        up(['body-daemon-01', 'body-daemon-02'], '恢复原生执行器与持久账本')
        up(['ansts-gateway-01', 'ansts-gateway-02', 'dev-server', 'prometheus', 'grafana'], '恢复网关、Next.js 与观测服务')
        up(['tanstack-console'], '恢复统一浏览器入口 :8088')
        after = normalized_tasks()
        missing = [key for key, value in before.items() if after.get(key) != value]
        if missing:
            raise RuntimeError(f'Persistence check failed for {len(missing)} previous tasks; private snapshot retained')
        resume(original)
        (PRIVATE / 'stopped-state.json').unlink(missing_ok=True)
        save('restart-result.json', {'completed': True, 'previousTasksPreserved': len(before), 'currentTasks': len(after), 'originalDrainRestored': original, 'completedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})
        print(f'重启完成：{len(before)} 条原任务及执行规格保留，原始 Drain 状态已恢复。', flush=True)
    except Exception:
        if not stopped and original:
            resume(original)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
