"use strict";
/**
 * @fileoverview ANSTS v2.5 TanStack Control Plane Management Console UI, BFF Client & Headless Table Engine
 * (`src/dashboard/tanstack_console_ui.ts`).
 *
 * Implements:
 * 1. `TanStackConsoleBffClient`: Real `fetch`-based BFF client binding all 5 Console Prototype tabs
 *    (`overview`, `fleet`, `harness`, `tunnels`, `wal`) + Canary Rollout & Make-Before-Break Drain mutations.
 * 2. `ConsoleDataTableEngine<T>`: Headless TanStack Table state engine supporting filtering, sorting,
 *    and pagination over `ShardMatrixRow` and `BodyFleetRow` datasets.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.TanStackConsoleBffClient = exports.ConsoleDataTableEngine = void 0;
/**
 * Headless data table state engine (TanStack Table pattern).
 */
class ConsoleDataTableEngine {
    data;
    filterText;
    constructor(data, filterText = "") {
        this.data = data;
        this.filterText = filterText;
    }
    getFilteredData(filterFn) {
        if (!this.filterText.trim())
            return this.data;
        return this.data.filter((row) => filterFn(row, this.filterText.toLowerCase()));
    }
    getSortedData(compareFn) {
        return [...this.data].sort(compareFn);
    }
}
exports.ConsoleDataTableEngine = ConsoleDataTableEngine;
/**
 * Typed TanStack Query / Mutation BFF Client for the ANSTS v2.5 Enterprise Console.
 * Connects the 5 Prototype Tabs (`overview`, `fleet`, `harness`, `tunnels`, `wal`) to live backend endpoints.
 */
class TanStackConsoleBffClient {
    baseUrl;
    constructor(baseUrl) {
        this.baseUrl = baseUrl;
    }
    async fetchOverview(region = "us-west-2") {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/overview?region=${encodeURIComponent(region)}`);
        if (!res.ok)
            throw new Error(`BFF fetchOverview failed: ${res.status}`);
        return (await res.json());
    }
    async fetchFleet() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/fleet`);
        if (!res.ok)
            throw new Error(`BFF fetchFleet failed: ${res.status}`);
        return (await res.json());
    }
    async fetchHarnessAndShards() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/harness`);
        if (!res.ok)
            throw new Error(`BFF fetchHarnessAndShards failed: ${res.status}`);
        return (await res.json());
    }
    async fetchTunnels() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/tunnels`);
        if (!res.ok)
            throw new Error(`BFF fetchTunnels failed: ${res.status}`);
        return (await res.json());
    }
    async mutateDrainGatewayOrHost(targetId, gracePeriodMs = 15000) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/tunnels/drain`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ gatewayId: targetId, gracePeriodMs }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateDrainGatewayOrHost failed: ${res.status}`);
        return (await res.json());
    }
    async mutateWalCatchupReplay(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/wal/catchup`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateWalCatchupReplay failed: ${res.status}`);
        return (await res.json());
    }
    async mutateCanaryAction(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/canary/action`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateCanaryAction failed: ${res.status}`);
        return (await res.json());
    }
    async mutateDispatchTask(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/tasks/dispatch`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateDispatchTask failed: ${res.status}`);
        return (await res.json());
    }
    async fetchShadowLab() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/shadow-lab`);
        if (!res.ok)
            throw new Error(`BFF fetchShadowLab failed: ${res.status}`);
        return (await res.json());
    }
    async mutateRunShadowEval(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/shadow-lab/run`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateRunShadowEval failed: ${res.status}`);
        return (await res.json());
    }
    async fetchSlotsHeatmap() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/slots-heatmap`);
        if (!res.ok)
            throw new Error(`BFF fetchSlotsHeatmap failed: ${res.status}`);
        return (await res.json());
    }
    async mutateMigrateSlotOnline(slotIndex, targetShardId) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/slots/migrate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ slotIndex, targetShardId }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateMigrateSlotOnline failed: ${res.status}`);
        return (await res.json());
    }
    async mutateCaptureV8FlameGraph(isolateId = "celld-brain-01") {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/profiler/capture`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ isolateId }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateCaptureV8FlameGraph failed: ${res.status}`);
        return (await res.json());
    }
    async mutateRunChaosDrills() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/chaos/run`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({}),
        });
        if (!res.ok)
            throw new Error(`BFF mutateRunChaosDrills failed: ${res.status}`);
        return (await res.json());
    }
    async mutateEvacuateSpotHost(spotHostId = "body-host-edge-02") {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/spot/evacuate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ spotHostId }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateEvacuateSpotHost failed: ${res.status}`);
        return (await res.json());
    }
    async fetchHitlAndTimeTravel(taskId = "task-prod-101") {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/hitl-timetravel?taskId=${encodeURIComponent(taskId)}`);
        if (!res.ok)
            throw new Error(`BFF fetchHitlAndTimeTravel failed: ${res.status}`);
        return (await res.json());
    }
    async mutateHitlDecision(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/hitl/decide`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateHitlDecision failed: ${res.status}`);
        return (await res.json());
    }
    async mutateTimeTravelFork(taskId, targetTurnSeq) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/timetravel/fork`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ taskId, targetTurnSeq }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateTimeTravelFork failed: ${res.status}`);
        return (await res.json());
    }
    async fetchCowAndAstMerger() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/cow-ast`);
        if (!res.ok)
            throw new Error(`BFF fetchCowAndAstMerger failed: ${res.status}`);
        return (await res.json());
    }
    async mutateForkAndAttestBranch(params) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/cow/fork-attest`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
        });
        if (!res.ok)
            throw new Error(`BFF mutateForkAndAttestBranch failed: ${res.status}`);
        return (await res.json());
    }
    async fetchFinOpsAndWdrfRadar() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/finops-wdrf`);
        if (!res.ok)
            throw new Error(`BFF fetchFinOpsAndWdrfRadar failed: ${res.status}`);
        return (await res.json());
    }
    async mutateIssueRbacToken(operatorId, role) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/rbac/token`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ operatorId, role }),
        });
        if (!res.ok)
            throw new Error(`BFF mutateIssueRbacToken failed: ${res.status}`);
        return (await res.json());
    }
    async mutateVerifyRbacPermission(token, permission) {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/rbac/verify`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ token, permission }),
        });
        const body = (await res.json());
        return { status: res.status, body };
    }
    async mutateRunV27DeepTechProbe() {
        const res = await fetch(`${this.baseUrl}/v1beta1/console/v27/deep-tech-probe`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({}),
        });
        if (!res.ok)
            throw new Error(`BFF mutateRunV27DeepTechProbe failed: ${res.status}`);
        return (await res.json());
    }
}
exports.TanStackConsoleBffClient = TanStackConsoleBffClient;
