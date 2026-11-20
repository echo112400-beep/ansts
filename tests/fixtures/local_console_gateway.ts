import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { TurnWalEngine } from "../../src/checkpoint/wal_engine.js";
import { MultiRegionGeoRouter } from "../../src/cluster/geo_routing.js";
import { SlotCanaryRolloutPlanner } from "../../src/cluster/slot_canary_rollout_planner.js";
import { LocalSchedulerMesh } from "../../src/cloudflare/local_scheduler_mesh.js";
import type { SqlStorageLike } from "../../src/cloudflare/env.js";
import { ConsoleBffController } from "../../src/dashboard/console_bff_api.js";
import { ConsoleRbacForbiddenError, type ConsolePermission } from "../../src/dashboard/console_rbac_guard.js";
import { SseEventStreamGateway } from "../../src/gateway/sse_stream.js";
import { AgentStreamQueuePublisher } from "../../src/gateway/stream_queue_publisher.js";
import { FenceToken, TaskState, TurnWalFrame } from "../../src/models.js";
import { EdgeTunnelRoutePlanner } from "../../src/runtime/edge_tunnel_ha_router.js";
import { LocalSpotEvacuation } from "../../src/runtime/local_spot_evacuation.js";

const { DatabaseSync } = createRequire(__filename)("node:sqlite") as {
  DatabaseSync: new (file: string) => {
    prepare(query: string): { all(...bindings: unknown[]): unknown[] };
    close(): void;
  };
};

function json(response: ServerResponse, value: unknown, status = 200): void {
  response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  response.end(JSON.stringify(value));
}

async function readBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const bytes = Buffer.from(chunk);
    size += bytes.byteLength;
    if (size > 1_048_576) throw new Error("INVALID_FIXTURE_BODY_SIZE");
    chunks.push(bytes);
  }
  if (!size) return {};
  const value: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("INVALID_FIXTURE_BODY");
  return { ...value };
}

function requiredString(value: unknown): string {
  if (typeof value !== "string" || !value.trim()) throw new Error("INVALID_FIXTURE_ARGUMENT");
  return value;
}

export class LocalConsoleGateway {
  private readonly root = mkdtempSync(join(tmpdir(), "ansts-legacy-console-"));
  private readonly database = new DatabaseSync(join(this.root, "fixture.sqlite"));
  private readonly bff: ConsoleBffController;
  private readonly wal: TurnWalEngine;
  private readonly server = createServer((request, response) => {
    void this.handle(request, response).catch((error: unknown) => {
      const forbidden = error instanceof ConsoleRbacForbiddenError;
      json(response, { authorized: false, error: error instanceof Error ? error.message : "FIXTURE_ERROR" }, forbidden ? 403 : 400);
    });
  });

  constructor() {
    const sql: SqlStorageLike = {
      exec: <T>(query: string, ...bindings: unknown[]) => {
        const rows = this.database.prepare(query).all(...bindings) as T[];
        return { toArray: () => rows, one: () => rows[0] ?? null };
      },
    };
    const geo = new MultiRegionGeoRouter("us-west-2");
    geo.registerRegion({ regionId: "us-west-2", gatewayEndpoint: "loopback", pingEwmaMs: 0, healthy: true, activeSessions: 0 });
    const publisher = new AgentStreamQueuePublisher(new SseEventStreamGateway(), new TurnWalEngine(sql));
    this.wal = publisher.walEngine;
    this.bff = new ConsoleBffController(
      new EdgeTunnelRoutePlanner("fixture-gateway"), geo, publisher,
      new LocalSchedulerMesh([0, 1, 2].map(index => ({ shardId: `scheduler-shard-0${index}`, hostBrainNodeId: `fixture-brain-0${index}` })), sql),
      new SlotCanaryRolloutPlanner(), new LocalSpotEvacuation(join(this.root, "spot")),
    );
    this.bff.ensureDefaultConsoleSeedData();
    for (const turn of [14, 15]) {
      this.wal.appendTurnDelta({
        taskId: "task-prod-101", turnIndex: turn, changedFiles: ["fixture.txt"],
        fencingToken: 3, highestSubmittedFencingToken: 3, sqliteWalPages: 1,
        enforceTurnMonotonicity: true, allowIdempotentRetry: true,
      });
    }
  }

  public async start(): Promise<number> {
    await new Promise<void>((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(0, "127.0.0.1", () => { this.server.off("error", reject); resolve(); });
    });
    const address = this.server.address();
    if (!address || typeof address === "string") throw new Error("FIXTURE_ADDRESS_UNAVAILABLE");
    return address.port;
  }

  public async stop(): Promise<void> {
    this.bff.ssePushGateway.closeAllSubscribers();
    await new Promise<void>((resolve, reject) => {
      this.server.close(error => error ? reject(error) : resolve());
      this.server.closeAllConnections();
    });
    this.database.close();
    rmSync(this.root, { recursive: true, force: true });
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const bff = this.bff;
    const route = `${request.method} ${url.pathname}`;
    const body = request.method === "POST" ? await readBody(request) : {};
    switch (route) {
      case "GET /v1beta1/console/overview":
        return json(response, bff.getClusterOverview({
          clientRegion: url.searchParams.get("region") ?? "us-west-2",
          activeSandboxes: bff.liveHarnessCells.size, maxSandboxes: 32,
          heapUsedMb: 32, activeCells: bff.liveHarnessCells.size,
        }));
      case "GET /v1beta1/console/harness":
        return json(response, bff.getHarnessAndShardsStatus(32));
      case "POST /v1beta1/tasks/dispatch": {
        const taskId = requiredString(body.taskId);
        const tenantId = requiredString(body.tenantId);
        this.wal.writeWalFrameIfNoneMatch(TurnWalFrame.fromPartial({
          taskId, fence: new FenceToken(1, 1, taskId),
          state: TaskState.RUNNING_THINK, diffSnapshot: JSON.stringify({ eventType: "TASK_CLAIMED" }),
        }));
        bff.recordLiveTaskExecution({
          taskId, tenantId, cellId: `fixture-${taskId}`, ownerBrainNode: "fixture-brain-00", heapUsedMb: 32,
          heapLimitMb: 128, sqliteState: "LOCAL_FIXTURE", fenceEpoch: 1, fenceTurnSeq: 1,
          routedModelPool: "DeepSeek", kvCacheHit: false, phase: "COGNITIVE_0_1C",
        });
        return json(response, { taskId, tenantId }, 201);
      }
      case "GET /v1beta1/console/shadow-lab":
        return json(response, bff.getShadowLabScorecards());
      case "POST /v1beta1/console/shadow-lab/run":
        return json(response, bff.triggerShadowReplayEval({
          sourceTaskId: requiredString(body.sourceTaskId), baselinePool: requiredString(body.baselinePool),
          candidatePool: requiredString(body.candidatePool),
          shadowTrafficSamplePct: typeof body.shadowTrafficSamplePct === "number" ? body.shadowTrafficSamplePct : undefined,
        }));
      case "GET /v1beta1/console/slots-heatmap":
        return json(response, bff.getSlotsHeatmapAndSovereignty());
      case "POST /v1beta1/console/slots/migrate":
        return json(response, bff.migrateVirtualSlotOnline(Number(body.slotIndex), requiredString(body.targetShardId)));
      case "POST /v1beta1/console/profiler/capture":
        return json(response, bff.captureLiveV8FlameGraph(requiredString(body.isolateId)));
      case "POST /v1beta1/console/chaos/run":
        return json(response, bff.runChaosEngineeringDrills());
      case "POST /v1beta1/console/spot/evacuate":
        return json(response, bff.triggerSpotHostEvacuation(requiredString(body.spotHostId)));
      case "GET /v1beta1/console/hitl-timetravel":
        return json(response, bff.getHitlAndTimeTravelState(url.searchParams.get("taskId") ?? undefined));
      case "POST /v1beta1/console/hitl/decide": {
        const decision = body.decision;
        if (decision !== "approve" && decision !== "reject" && decision !== "stage_new") throw new Error("INVALID_FIXTURE_APPROVAL");
        return json(response, bff.decideHitlStagedCommit({
          decision, taskId: requiredString(body.taskId), operatorToken: request.headers.authorization?.replace(/^Bearer /, "") ?? "",
          ticketId: typeof body.ticketId === "string" ? body.ticketId : undefined,
          toolName: typeof body.toolName === "string" ? body.toolName : undefined,
          payload: typeof body.payload === "string" ? body.payload : undefined,
        }));
      }
      case "POST /v1beta1/console/timetravel/fork":
        return json(response, bff.forkFromHistoricalTurn(requiredString(body.taskId), Number(body.targetTurnSeq)));
      case "GET /v1beta1/console/cow-ast":
        return json(response, bff.getCowBranchesAndAstMergerStatus());
      case "POST /v1beta1/console/cow/fork-attest":
        return json(response, bff.forkAndAttestSubAgentBranch({
          sandboxId: requiredString(body.sandboxId), patchFile: requiredString(body.patchFile), patchContent: requiredString(body.patchContent),
        }));
      case "GET /v1beta1/console/finops-wdrf":
        return json(response, bff.getFinOpsAndWdrfRadar());
      case "GET /v1beta1/console/events/stream":
        return bff.ssePushGateway.attachSseSubscriber(response);
      case "POST /v1beta1/console/rbac/token": {
        const role = body.role;
        if (role !== "VIEWER" && role !== "OPERATOR" && role !== "SRE_ADMIN") throw new Error("INVALID_FIXTURE_ROLE");
        return json(response, bff.issueOperatorRbacJwt(requiredString(body.operatorId), role));
      }
      case "POST /v1beta1/console/rbac/verify":
        return json(response, { authorized: true, ...bff.authorizeRbacOperation(requiredString(body.token), requiredString(body.permission) as ConsolePermission) });
      case "POST /v1beta1/console/v27/deep-tech-probe":
        return json(response, bff.runV27DeepTechSuiteProbe());
      default:
        return json(response, { error: "FIXTURE_ROUTE_NOT_FOUND" }, 404);
    }
  }
}
