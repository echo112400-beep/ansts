#!/usr/bin/env python3
"""Read the development source fleet without changing routes or draining hosts."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
SCHEDULER_PORTS = (18082, 18083, 18084)
GATEWAY_PORTS = (18080, 18081)
BODY_WORKER_PORT = 18085


def secret(name):
    return (ROOT / ".dev-secrets" / name).read_text().strip()


def read(port, route, headers=None, body=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{route}",
        headers={"content-type": "application/json", **(headers or {})},
        data=None if body is None else json.dumps(body).encode(),
    )
    with HTTP.open(request, timeout=4) as response:
        return response.read()


def probe_worker(port, role):
    health = json.loads(read(port, "/readyz"))
    result = {"ready": health.get("healthy") is True}
    if role == "scheduler":
        headers = {"authorization": "Gateway " + secret("gateway-control.token")}
        data = json.loads(read(port, "/v1/scheduler/tasks", headers))
        result["tasks"] = len(data["tasks"])
    return result, result["ready"]


def probe_host(index):
    port = 19500 + index
    node = f"body-host-0{index}"
    health = json.loads(read(port, "/health"))
    headers = {"authorization": "Body " + secret(node + ".token"), "x-ansts-body-id": node}
    evidence = json.loads(read(port, "/v1/daemon/execution-evidence/pending", headers, {}))
    usage = json.loads(read(port, "/v1/daemon/usage/status", headers, {}))
    metrics = read(port, "/metrics", {"authorization": "Bearer " + secret("monitoring.key")}).decode()
    results = next(
        int(line.split()[1]) for line in metrics.splitlines()
        if line.startswith("ansts_body_result_outbox_pending ")
    )
    result = {
        "drained": health["drained"],
        "activeSandboxes": health["activeSandboxes"],
        "recoveryRequired": health["recoveryRequired"],
        "pendingResults": results,
        "pendingEvidence": len(evidence["observations"]),
        "pendingUsage": usage["pendingEvents"],
        "openUsageStreams": usage["openStreams"],
        "unresolvedProviderCalls": usage["unresolvedProviderCalls"],
    }
    idle = result["drained"] is True and result["recoveryRequired"] is False and all(
        value == 0 for key, value in result.items() if key not in ("drained", "recoveryRequired")
    )
    return result, idle


def main():
    probes = [
        (f"{role}:{port}", probe_worker, (port, role))
        for role, ports in (
            ("scheduler", SCHEDULER_PORTS), ("gateway", GATEWAY_PORTS), ("body-worker", (BODY_WORKER_PORT,))
        )
        for port in ports
    ]
    probes += [(f"body-host-0{index}", probe_host, (index,)) for index in (1, 2)]
    checks = {}
    passed = True
    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        futures = [(name, pool.submit(probe, *args)) for name, probe, args in probes]
        for name, future in futures:
            try:
                checks[name], ok = future.result()
            except (OSError, ValueError, KeyError, StopIteration) as error:
                checks[name] = {"error": type(error).__name__}
                ok = False
            passed &= ok
    print(json.dumps({"developmentSourceChecksPassed": passed, "checks": checks}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
