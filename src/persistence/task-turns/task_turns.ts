import { FenceToken } from "../../models";
import { SnapshotBlockStore, putAndVerifyBlock, sha256Hex } from "../../checkpoint/snapshot_block_store";
import { digestJson } from "../digest";
import { CellMigration, CellOperationCommand, CellOutboxDraft, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { confirmCellWrite, withCellTransaction } from "../transactions";
import { TaskExecutionRecord, readTaskExecution, runTaskTransaction } from "../task-core/task_core";
import { TaskCompensationStatus, TaskOperationRecord, TaskOperations } from "../task-operations/task_operations";

export interface SnapshotChunkDescriptor {
  index: number;
  sha256: string;
  sizeBytes: number;
}

export interface SnapshotChunk extends SnapshotChunkDescriptor {
  objectKey: string;
}

export interface SnapshotManifest {
  version: 1;
  taskId: string;
  tenantId: string;
  turnSeq: number;
  uploadId: string;
  blocks: SnapshotChunk[];
}

export interface CommittedTaskTurn {
  taskId: string;
  tenantId: string;
  turnSeq: number;
  businessEpoch: number;
  requestDigest: string;
  commitDigest: string;
  turnId: string;
  uploadId: string;
  manifestId: string;
  manifestSha256: string;
  response: unknown;
  modelCallRefs: string[];
  settlementRefs: string[];
  progress?: unknown;
  committedAt: number;
}

export interface RestoredTaskTurn extends CommittedTaskTurn {
  manifest: SnapshotManifest;
  snapshotBlocks: Uint8Array[];
  /** Durable external effects, provider usage and compensation changes after the base Turn. */
  laterOperationOutcomes: TaskOperationRecord[];
  laterCompensations: Array<{
    operationId: string;
    turnSeq: number;
    orderSeq: number;
    toolName: string;
    args: unknown;
    status: TaskCompensationStatus;
    attempts: number;
    lastError?: string;
  }>;
  committedModelCalls: ModelCallResultRecord[];
  laterModelCalls: ModelCallResultRecord[];
  laterSettlementRefs: string[];
}

export interface ModelCallResultRecord {
  taskId: string;
  tenantId: string;
  callId: string;
  turnSeq: number;
  response: unknown;
  responseSha256: string;
  usage: unknown;
  routeRefs: unknown;
  createdAt: number;
}

export interface BeginSnapshotUploadRequest {
  taskId: string;
  tenantId: string;
  fence: FenceToken;
  uploadId: string;
  chunks: SnapshotChunkDescriptor[];
}

export interface CommitTaskTurnRequest {
  taskId: string;
  tenantId: string;
  fence: FenceToken;
  uploadId: string;
  requestDigest: string;
  response: unknown;
  modelCallRefs?: string[];
  settlementRefs?: string[];
  progress?: unknown;
  outboxEvents?: CellOutboxDraft[];
}

export interface RecordModelCallResultRequest {
  taskId: string;
  tenantId: string;
  callId: string;
  response: unknown;
  usage: unknown;
  routeRefs: unknown;
  turnSeq?: number;
}

export class TaskTurnError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "TaskTurnError";
  }
}

export class SnapshotRestoreError extends TaskTurnError {
  constructor(code: string, message: string) {
    super(code, message, 503);
    this.name = "SnapshotRestoreError";
  }
}

interface UploadRow {
  upload_id: string;
  task_id: string;
  tenant_id: string;
  turn_seq: number;
  business_epoch: number;
  expected_manifest_json: string;
  manifest_sha256: string;
  status: "UPLOADING" | "COMMITTED" | "ABORTED";
  started_at: number;
  updated_at: number;
}

interface UploadChunkRow {
  upload_id: string;
  chunk_index: number;
  sha256: string;
  size_bytes: number;
  object_key: string;
  verified_at: number | null;
}

interface TurnRow {
  task_id: string;
  tenant_id: string;
  turn_seq: number;
  business_epoch: number;
  request_digest: string;
  commit_digest: string;
  turn_id: string;
  upload_id: string;
  manifest_id: string;
  manifest_sha256: string;
  response_json: string;
  model_call_refs_json: string;
  settlement_refs_json: string;
  progress_json: string | null;
  committed_at: number;
}

interface ManifestRow {
  manifest_id: string;
  task_id: string;
  tenant_id: string;
  turn_seq: number;
  upload_id: string;
  manifest_sha256: string;
  manifest_json: string;
  created_at: number;
}

interface ReferenceRow {
  ref_id: string;
  task_id: string;
  tenant_id: string;
  turn_seq: number;
  ref_kind: "SNAPSHOT_BLOCK" | "MODEL_CALL_RESULT";
  owner_id: string;
  block_index: number;
  object_key: string;
  sha256: string;
  size_bytes: number;
  status: "ACTIVE" | "UNKNOWN" | "TOMBSTONED" | "DELETED";
  tombstoned_at: number | null;
  delete_after: number | null;
  unknown_reason: string | null;
  created_at: number;
}

interface ModelCallRow {
  task_id: string;
  tenant_id: string;
  call_id: string;
  turn_seq: number;
  response_sha256: string;
  response_size_bytes: number;
  response_chunks_json: string;
  usage_json: string;
  route_refs_json: string;
  created_at: number;
}

interface ModelCallChunk {
  index: number;
  sha256: string;
  sizeBytes: number;
  objectKey: string;
}

export const taskTurnUploadMigration: CellMigration = {
  version: 4,
  description: "resumable TaskCell snapshot upload intents and verified application chunks",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS task_turn_uploads (
      upload_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, turn_seq INTEGER NOT NULL,
      business_epoch INTEGER NOT NULL, expected_manifest_json TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
      status TEXT NOT NULL, started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      UNIQUE(task_id, turn_seq)
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_turn_uploads_task_idx ON task_turn_uploads (task_id, status, turn_seq)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_turn_upload_chunks (
      upload_id TEXT NOT NULL, chunk_index INTEGER NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL,
      object_key TEXT NOT NULL, verified_at INTEGER, PRIMARY KEY(upload_id, chunk_index)
    )`);
  },
};

export const taskTurnsMigration: CellMigration = {
  version: 5,
  description: "committed Turns, snapshot manifests/references and staged provider result journal",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS task_turns (
      task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, turn_seq INTEGER NOT NULL, business_epoch INTEGER NOT NULL,
      request_digest TEXT NOT NULL, commit_digest TEXT NOT NULL,
      turn_id TEXT NOT NULL UNIQUE, upload_id TEXT NOT NULL, manifest_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
      response_json TEXT NOT NULL, model_call_refs_json TEXT NOT NULL, settlement_refs_json TEXT NOT NULL,
      progress_json TEXT, committed_at INTEGER NOT NULL, PRIMARY KEY(task_id, turn_seq)
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_turns_latest_idx ON task_turns (task_id, turn_seq DESC)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_snapshot_manifests (
      manifest_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, turn_seq INTEGER NOT NULL,
      upload_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL, manifest_json TEXT NOT NULL, created_at INTEGER NOT NULL,
      UNIQUE(task_id, turn_seq)
    )`);
    sql.exec(`CREATE TABLE IF NOT EXISTS task_snapshot_refs (
      ref_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, turn_seq INTEGER NOT NULL,
      ref_kind TEXT NOT NULL, owner_id TEXT NOT NULL, block_index INTEGER NOT NULL, object_key TEXT NOT NULL,
      sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, status TEXT NOT NULL, tombstoned_at INTEGER,
      delete_after INTEGER, unknown_reason TEXT, created_at INTEGER NOT NULL, UNIQUE(task_id, ref_kind, owner_id, block_index)
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_snapshot_refs_gc_idx ON task_snapshot_refs (status, delete_after, task_id, turn_seq)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_model_call_results (
      task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, call_id TEXT NOT NULL, turn_seq INTEGER NOT NULL,
      response_sha256 TEXT NOT NULL, response_size_bytes INTEGER NOT NULL, response_chunks_json TEXT NOT NULL,
      usage_json TEXT NOT NULL, route_refs_json TEXT NOT NULL, created_at INTEGER NOT NULL,
      PRIMARY KEY(task_id, call_id)
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_model_call_results_turn_idx ON task_model_call_results (task_id, turn_seq, created_at)");
  },
};

export const taskTurnMigrations: CellMigration[] = [taskTurnUploadMigration, taskTurnsMigration];

const DEFAULT_MODEL_CALL_CHUNK_BYTES = 256 * 1024;

/** Durable TaskCell Turn/snapshot owner. All recovery state is reconstructed from SQL and the block store. */
export class TaskTurns {
  constructor(
    public readonly storage: PersistenceObjectStorage,
    private readonly blockStore: SnapshotBlockStore,
    private readonly operations: TaskOperations,
    private readonly options: { modelCallChunkBytes?: number; snapshotRetentionMs?: number; gcGracePeriodMs?: number } = {},
  ) {
    if (operations.storage !== storage) throw new TypeError("TaskTurns and TaskOperations must use the same TaskCell storage.");
    const chunkBytes = options.modelCallChunkBytes ?? DEFAULT_MODEL_CALL_CHUNK_BYTES;
    if (!Number.isSafeInteger(chunkBytes) || chunkBytes <= 0) throw new TypeError("modelCallChunkBytes must be a positive safe integer.");
    for (const value of [options.snapshotRetentionMs, options.gcGracePeriodMs]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) throw new TypeError("Snapshot retention and GC grace must be non-negative safe integers.");
    }
  }

  /** Register or rebind an application upload identity so a new executor can resume its chunks. */
  public async beginSnapshotUpload(request: BeginSnapshotUploadRequest): Promise<{ uploadId: string; turnSeq: number; status: string; chunks: SnapshotChunk[] }> {
    validateIdentity(request.taskId, "taskId");
    validateIdentity(request.tenantId, "tenantId");
    validateIdentity(request.uploadId, "uploadId");
    assertFence(request.taskId, request.fence);
    const chunks = normalizeDescriptors(request.chunks);
    const ownerKey = await digestJson({ tenantId: request.tenantId, taskId: request.taskId });
    const manifestBlocks: SnapshotChunk[] = chunks.map((chunk) => ({
      ...chunk,
      objectKey: `task-turn-blocks/${ownerKey.sha256}/${encodeURIComponent(request.uploadId)}/${chunk.index}-${chunk.sha256}`,
    }));
    const expectedManifest = { version: 1 as const, taskId: request.taskId, tenantId: request.tenantId,
      turnSeq: request.fence.turnSeq, uploadId: request.uploadId, blocks: manifestBlocks };
    const expectedJson = (await digestJson(expectedManifest)).canonicalJson;
    const expectedDigest = (await sha256Text(expectedJson));
    let result!: { uploadId: string; turnSeq: number; status: string; chunks: SnapshotChunk[] };

    withCellTransaction(this.storage, () => {
      const task = requireTask(this.storage.sql, request.taskId, request.tenantId);
      const byId = this.storage.sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE upload_id = ?", request.uploadId).toArray()[0];
      const byTurn = this.storage.sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE task_id = ? AND turn_seq = ?", request.taskId, request.fence.turnSeq).toArray()[0];
      const existing = byId ?? byTurn;
      if (existing) {
        if (existing.upload_id !== request.uploadId || existing.task_id !== request.taskId || existing.tenant_id !== request.tenantId ||
            Number(existing.turn_seq) !== request.fence.turnSeq || existing.manifest_sha256 !== expectedDigest || existing.expected_manifest_json !== expectedJson) {
          throw new TaskTurnError("SNAPSHOT_UPLOAD_CONFLICT", `Turn ${request.fence.turnSeq} already has a different snapshot upload identity.`);
        }
        if (existing.status === "ABORTED") throw new TaskTurnError("SNAPSHOT_UPLOAD_ABORTED", `Snapshot upload ${request.uploadId} was aborted.`);
        if (existing.status === "UPLOADING") {
          assertRunnableFence(task, request.fence);
          if (Number(existing.business_epoch) !== request.fence.epoch) {
            this.storage.sql.exec("UPDATE task_turn_uploads SET business_epoch = ?, updated_at = ? WHERE upload_id = ?",
              request.fence.epoch, Date.now(), request.uploadId);
          }
        }
        result = { uploadId: existing.upload_id, turnSeq: Number(existing.turn_seq), status: existing.status, chunks: manifestBlocks };
        return;
      }
      assertRunnableFence(task, request.fence);
      const now = Date.now();
      this.storage.sql.exec(`INSERT INTO task_turn_uploads (
        upload_id, task_id, tenant_id, turn_seq, business_epoch, expected_manifest_json, manifest_sha256, status, started_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, request.uploadId, request.taskId, request.tenantId, request.fence.turnSeq,
      request.fence.epoch, expectedJson, expectedDigest, "UPLOADING", now, now);
      for (const chunk of manifestBlocks) {
        this.storage.sql.exec(`INSERT INTO task_turn_upload_chunks (
          upload_id, chunk_index, sha256, size_bytes, object_key, verified_at
        ) VALUES (?, ?, ?, ?, ?, ?)`, request.uploadId, chunk.index, chunk.sha256, chunk.sizeBytes, chunk.objectKey, null);
      }
      result = { uploadId: request.uploadId, turnSeq: request.fence.turnSeq, status: "UPLOADING", chunks: manifestBlocks };
    });
    await confirmCellWrite(this.storage);
    return result;
  }

  /** Upload one fixed application chunk, read it back, verify SHA-256, then persist its progress. */
  public async uploadSnapshotChunk(input: { taskId: string; tenantId: string; uploadId: string; index: number; bytes: Uint8Array }): Promise<SnapshotChunk> {
    validateIdentity(input.taskId, "taskId");
    validateIdentity(input.tenantId, "tenantId");
    validateIdentity(input.uploadId, "uploadId");
    if (!Number.isSafeInteger(input.index) || input.index < 0) throw new TypeError("Chunk index must be a non-negative safe integer.");
    const session = this.readUpload(input.uploadId);
    if (!session || session.task_id !== input.taskId || session.tenant_id !== input.tenantId) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_NOT_FOUND", `Snapshot upload ${input.uploadId} was not found for this task and tenant.`, 404);
    }
    const expected = this.storage.sql.exec<UploadChunkRow>(
      "SELECT * FROM task_turn_upload_chunks WHERE upload_id = ? AND chunk_index = ?", input.uploadId, input.index,
    ).toArray()[0];
    if (!expected) throw new TaskTurnError("SNAPSHOT_CHUNK_NOT_DECLARED", `Chunk ${input.index} is not declared for upload ${input.uploadId}.`, 404);
    if (!session || session.status !== "UPLOADING") throw new TaskTurnError("SNAPSHOT_UPLOAD_NOT_OPEN", `Snapshot upload ${input.uploadId} is not open.`);
    const bytes = new Uint8Array(input.bytes);
    if (bytes.byteLength !== Number(expected.size_bytes)) throw new TaskTurnError("SNAPSHOT_CHUNK_SIZE_MISMATCH", `Chunk ${input.index} has size ${bytes.byteLength}, expected ${expected.size_bytes}.`);
    if (await sha256Hex(bytes) !== expected.sha256) throw new TaskTurnError("SNAPSHOT_CHUNK_DIGEST_MISMATCH", `Chunk ${input.index} does not match its declared SHA-256.`);
    await putAndVerifyBlock(this.blockStore, expected.object_key, bytes, expected.sha256);
    withCellTransaction(this.storage, () => {
      const fresh = this.storage.sql.exec<UploadChunkRow>(
        "SELECT * FROM task_turn_upload_chunks WHERE upload_id = ? AND chunk_index = ?", input.uploadId, input.index,
      ).toArray()[0];
      const current = this.readUpload(input.uploadId);
      if (!current || current.task_id !== input.taskId || current.tenant_id !== input.tenantId || current.status !== "UPLOADING" || !fresh || fresh.object_key !== expected.object_key ||
          fresh.sha256 !== expected.sha256 || Number(fresh.size_bytes) !== bytes.byteLength) {
        throw new TaskTurnError("SNAPSHOT_UPLOAD_CHANGED", `Snapshot upload ${input.uploadId} changed while chunk ${input.index} was stored.`);
      }
      this.storage.sql.exec("UPDATE task_turn_upload_chunks SET verified_at = ? WHERE upload_id = ? AND chunk_index = ?",
        Date.now(), input.uploadId, input.index);
      this.storage.sql.exec("UPDATE task_turn_uploads SET updated_at = ? WHERE upload_id = ?", Date.now(), input.uploadId);
    });
    await confirmCellWrite(this.storage);
    return { index: input.index, sha256: expected.sha256, sizeBytes: Number(expected.size_bytes), objectKey: expected.object_key };
  }

  /** Read-only preflight before a metered model call. `commitTurn` always repeats this verification. */
  public async assertSnapshotReady(input: { uploadId: string; taskId: string; tenantId: string; fence: FenceToken }): Promise<void> {
    assertFence(input.taskId, input.fence);
    const task = requireTask(this.storage.sql, input.taskId, input.tenantId);
    assertRunnableFence(task, input.fence);
    const upload = this.readUpload(input.uploadId);
    if (!upload || upload.task_id !== input.taskId || upload.tenant_id !== input.tenantId || Number(upload.turn_seq) !== input.fence.turnSeq) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_NOT_FOUND", `No snapshot upload matches Turn ${input.fence.turnSeq}.`, 404);
    }
    if (upload.status !== "UPLOADING" || Number(upload.business_epoch) !== input.fence.epoch) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_FENCE_MISMATCH", "The snapshot upload is not open under this execution epoch.");
    }
    const chunks = this.storage.sql.exec<UploadChunkRow>(
      "SELECT * FROM task_turn_upload_chunks WHERE upload_id = ? ORDER BY chunk_index", input.uploadId,
    ).toArray();
    if (await sha256Text(upload.expected_manifest_json) !== upload.manifest_sha256) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_MANIFEST_DIGEST_MISMATCH", `Snapshot upload ${input.uploadId} manifest failed its digest check.`);
    }
    const expected = JSON.parse(upload.expected_manifest_json) as SnapshotManifest;
    if (expected.taskId !== input.taskId || expected.tenantId !== input.tenantId || expected.turnSeq !== input.fence.turnSeq || expected.uploadId !== input.uploadId) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_MANIFEST_IDENTITY_MISMATCH", `Snapshot upload ${input.uploadId} manifest does not match its owner identity.`);
    }
    if (chunks.length !== expected.blocks.length || chunks.some((chunk) => chunk.verified_at === null)) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_INCOMPLETE", `Snapshot upload ${input.uploadId} has chunks that have not been read back and verified.`);
    }
    for (const [index, chunk] of chunks.entries()) {
      const block = expected.blocks[index];
      if (!block || block.index !== chunk.chunk_index || block.objectKey !== chunk.object_key || block.sha256 !== chunk.sha256 || block.sizeBytes !== Number(chunk.size_bytes)) {
        throw new TaskTurnError("SNAPSHOT_UPLOAD_CHANGED", `Snapshot upload ${input.uploadId} no longer matches its manifest.`);
      }
      await this.readAndVerifyBlock(chunk.object_key, chunk.sha256, Number(chunk.size_bytes), "SNAPSHOT_BLOCK");
    }
  }

  public async abortSnapshotUpload(input: { uploadId: string; taskId: string; tenantId: string }): Promise<void> {
    withCellTransaction(this.storage, () => {
      const upload = this.readUpload(input.uploadId);
      if (!upload || upload.task_id !== input.taskId || upload.tenant_id !== input.tenantId) {
        throw new TaskTurnError("SNAPSHOT_UPLOAD_NOT_FOUND", `Snapshot upload ${input.uploadId} was not found.`, 404);
      }
      if (upload.status === "COMMITTED") throw new TaskTurnError("SNAPSHOT_UPLOAD_COMMITTED", "A committed Turn upload cannot be aborted.");
      if (upload.status !== "ABORTED") this.storage.sql.exec("UPDATE task_turn_uploads SET status = ?, updated_at = ? WHERE upload_id = ?", "ABORTED", Date.now(), input.uploadId);
    });
    await confirmCellWrite(this.storage);
  }

  /** Commit response, snapshot, references, execution head and outbox in one TaskCell SQL transaction. */
  public async commitTurn(request: CommitTaskTurnRequest): Promise<CommittedTaskTurn> {
    validateIdentity(request.taskId, "taskId");
    validateIdentity(request.tenantId, "tenantId");
    validateIdentity(request.uploadId, "uploadId");
    assertFence(request.taskId, request.fence);
    if (!/^[a-f0-9]{64}$/.test(request.requestDigest)) throw new TypeError("requestDigest must be a lowercase SHA-256 digest.");
    const response = await digestJson(request.response);
    const modelCallRefs = normalizeStringRefs(request.modelCallRefs ?? [], "modelCallRefs");
    const settlementRefs = normalizeStringRefs(request.settlementRefs ?? [], "settlementRefs");
    const upload = this.readUpload(request.uploadId);
    if (!upload || upload.task_id !== request.taskId || upload.tenant_id !== request.tenantId || Number(upload.turn_seq) !== request.fence.turnSeq) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_NOT_FOUND", `No snapshot upload matches Turn ${request.fence.turnSeq}.`, 404);
    }
    if (upload.status === "ABORTED") throw new TaskTurnError("SNAPSHOT_UPLOAD_ABORTED", `Snapshot upload ${request.uploadId} was aborted.`);

    if (await sha256Text(upload.expected_manifest_json) !== upload.manifest_sha256) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_MANIFEST_DIGEST_MISMATCH", `Snapshot upload ${request.uploadId} manifest failed its digest check.`);
    }
    const manifest = JSON.parse(upload.expected_manifest_json) as SnapshotManifest;
    if (manifest.taskId !== request.taskId || manifest.tenantId !== request.tenantId ||
        manifest.turnSeq !== request.fence.turnSeq || manifest.uploadId !== request.uploadId) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_MANIFEST_IDENTITY_MISMATCH", `Snapshot upload ${request.uploadId} manifest does not match its owner identity.`);
    }
    const chunkRows = this.storage.sql.exec<UploadChunkRow>(
      "SELECT * FROM task_turn_upload_chunks WHERE upload_id = ? ORDER BY chunk_index", request.uploadId,
    ).toArray();
    if (chunkRows.length !== manifest.blocks.length || chunkRows.some((chunk) => chunk.verified_at === null)) {
      throw new TaskTurnError("SNAPSHOT_UPLOAD_INCOMPLETE", `Snapshot upload ${request.uploadId} has chunks that have not been read back and verified.`);
    }
    const verifiedBlocks: UploadChunkRow[] = [];
    for (const row of chunkRows) {
      await this.readAndVerifyBlock(row.object_key, row.sha256, Number(row.size_bytes), "SNAPSHOT_BLOCK");
      verifiedBlocks.push(row);
    }
    if (chunkRows.some((row, index) => {
      const block = manifest.blocks[index];
      return !block || row.chunk_index !== block.index || row.object_key !== block.objectKey ||
        row.sha256 !== block.sha256 || Number(row.size_bytes) !== block.sizeBytes;
    })) throw new TaskTurnError("SNAPSHOT_UPLOAD_CHANGED", `Snapshot upload ${request.uploadId} no longer matches its manifest.`);
    // A staged SQL row alone does not prove the external response blocks remain
    // recoverable. Verify every referenced model response before acknowledging a Turn.
    for (const callId of modelCallRefs) {
      const call = await this.getModelCallResult(request.taskId, callId);
      if (call.tenantId !== request.tenantId || call.turnSeq !== request.fence.turnSeq) {
        throw new TaskTurnError("MODEL_CALL_RESULT_NOT_STAGED", `Model call result ${callId} is not staged for Turn ${request.fence.turnSeq}.`);
      }
    }
    const manifestDigested = await digestJson(manifest);
    const manifestDigest = manifestDigested.sha256;
    const manifestId = stableManifestId(request.taskId, request.fence.turnSeq);
    const turnId = stableTurnId(request.taskId, request.fence.turnSeq);
    const committedAt = Date.now();
    const outboxEvents: CellOutboxDraft[] = [{
      eventId: `${turnId}:committed`, eventType: "task.turn.committed", tenantId: request.tenantId,
      payload: { taskId: request.taskId, turnSeq: request.fence.turnSeq, turnId, manifestId, manifestSha256: manifestDigest,
        modelCallRefs, settlementRefs },
    }, ...(request.outboxEvents ?? [])];
    const progressJson = request.progress === undefined ? null : (await digestJson(request.progress)).canonicalJson;
    const commitDigest = (await digestJson({
      taskId: request.taskId, tenantId: request.tenantId, turnSeq: request.fence.turnSeq,
      businessEpoch: request.fence.epoch, uploadId: request.uploadId, requestDigest: request.requestDigest,
      manifestDigest, response: request.response, modelCallRefs, settlementRefs,
      progress: request.progress ?? null, outboxEvents,
    })).sha256;
    const storedTurn = this.getTurn(request.taskId, request.fence.turnSeq);
    if (storedTurn) {
      assertSameTurnRequest(storedTurn, request, commitDigest);
      await confirmCellWrite(this.storage);
      return storedTurn;
    }
    const command = this.taskCommand(`task-turn:${turnId}:commit`, request.taskId, request.tenantId,
      "task.turn.commit", { taskId: request.taskId, tenantId: request.tenantId, turnSeq: request.fence.turnSeq,
        businessEpoch: request.fence.epoch, uploadId: request.uploadId, requestDigest: request.requestDigest,
        commitDigest, response: request.response,
        modelCallRefs, settlementRefs, progress: request.progress ?? null, manifestSha256: manifestDigest,
        outboxEvents }, "task-execution");

    const committed = await runTaskTransaction({
      storage: this.storage,
      command,
      outboxEvents,
      apply: ({ sql, assertExecutionEpoch, saveProgress }) => {
        assertExecutionEpoch(request.fence.epoch, request.fence.turnSeq);
        const currentUpload = sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE upload_id = ?", request.uploadId).toArray()[0];
        if (!currentUpload || currentUpload.status !== "UPLOADING" || Number(currentUpload.business_epoch) !== request.fence.epoch ||
            currentUpload.manifest_sha256 !== upload.manifest_sha256) {
          throw new TaskTurnError("SNAPSHOT_UPLOAD_FENCE_MISMATCH", "The snapshot upload is not owned by this current execution epoch.");
        }
        const freshChunks = sql.exec<UploadChunkRow>("SELECT * FROM task_turn_upload_chunks WHERE upload_id = ? ORDER BY chunk_index", request.uploadId).toArray();
        if (freshChunks.length !== verifiedBlocks.length || freshChunks.some((chunk, index) =>
          chunk.verified_at === null || chunk.object_key !== verifiedBlocks[index].object_key ||
          chunk.sha256 !== verifiedBlocks[index].sha256 || Number(chunk.size_bytes) !== Number(verifiedBlocks[index].size_bytes))) {
          throw new TaskTurnError("SNAPSHOT_UPLOAD_CHANGED", "Snapshot chunk declarations changed before Turn commit.");
        }
        for (const callId of modelCallRefs) {
          const call = sql.exec<ModelCallRow>("SELECT * FROM task_model_call_results WHERE task_id = ? AND call_id = ?", request.taskId, callId).toArray()[0];
          if (!call || call.tenant_id !== request.tenantId || Number(call.turn_seq) !== request.fence.turnSeq) {
            throw new TaskTurnError("MODEL_CALL_RESULT_NOT_STAGED", `Model call result ${callId} is not staged for Turn ${request.fence.turnSeq}.`);
          }
        }
        const stagedModelCalls = sql.exec<{ call_id: string }>(
          "SELECT call_id FROM task_model_call_results WHERE task_id = ? AND turn_seq = ?", request.taskId, request.fence.turnSeq,
        ).toArray();
        if (stagedModelCalls.some((call) => !modelCallRefs.includes(call.call_id))) {
          throw new TaskTurnError("MODEL_CALL_RESULT_REFERENCE_MISSING", "Every staged model-call result for this Turn must be referenced by the committed Turn.");
        }
        const existing = sql.exec<TurnRow>("SELECT * FROM task_turns WHERE task_id = ? AND turn_seq = ?", request.taskId, request.fence.turnSeq).toArray()[0];
        if (existing) {
          const mapped = mapTurn(existing);
          assertSameTurnRequest(mapped, request, commitDigest);
          return mapped;
        }
        sql.exec(`INSERT INTO task_snapshot_manifests (
          manifest_id, task_id, tenant_id, turn_seq, upload_id, manifest_sha256, manifest_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, manifestId, request.taskId, request.tenantId, request.fence.turnSeq,
        request.uploadId, manifestDigest, manifestDigested.canonicalJson, committedAt);
        for (const block of manifest.blocks) {
          sql.exec(`INSERT INTO task_snapshot_refs (
            ref_id, task_id, tenant_id, turn_seq, ref_kind, owner_id, block_index, object_key, sha256, size_bytes,
            status, tombstoned_at, delete_after, unknown_reason, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          stableSnapshotRefId(turnId, block.index), request.taskId, request.tenantId, request.fence.turnSeq,
          "SNAPSHOT_BLOCK", turnId, block.index, block.objectKey, block.sha256, block.sizeBytes,
          "ACTIVE", null, null, null, committedAt);
        }
        sql.exec(`INSERT INTO task_turns (
          task_id, tenant_id, turn_seq, business_epoch, request_digest, commit_digest, turn_id, upload_id, manifest_id, manifest_sha256,
          response_json, model_call_refs_json, settlement_refs_json, progress_json, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, request.taskId, request.tenantId, request.fence.turnSeq,
        request.fence.epoch, request.requestDigest, commitDigest, turnId, request.uploadId, manifestId, manifestDigest, response.canonicalJson,
        JSON.stringify(modelCallRefs), JSON.stringify(settlementRefs), progressJson, committedAt);
        sql.exec("UPDATE task_turn_uploads SET status = ?, updated_at = ? WHERE upload_id = ?", "COMMITTED", committedAt, request.uploadId);
        sql.exec("UPDATE task_execution SET last_turn_seq = ?, heartbeat_at = ?, updated_at = ? WHERE task_id = ?",
          request.fence.turnSeq, committedAt, committedAt, request.taskId);
        if (request.progress !== undefined) saveProgress(request.progress);
        return {
          taskId: request.taskId, tenantId: request.tenantId, turnSeq: request.fence.turnSeq,
          businessEpoch: request.fence.epoch, requestDigest: request.requestDigest, commitDigest,
          turnId, uploadId: request.uploadId, manifestId,
          manifestSha256: manifestDigest, response: JSON.parse(response.canonicalJson), modelCallRefs,
          settlementRefs, ...(request.progress === undefined ? {} : { progress: request.progress }), committedAt,
        } satisfies CommittedTaskTurn;
      },
    });
    return committed.result;
  }

  /** Store provider output before completing its quota record so retries can recover without another call. */
  public async recordModelCallResult(request: RecordModelCallResultRequest): Promise<ModelCallResultRecord> {
    validateIdentity(request.taskId, "taskId");
    validateIdentity(request.tenantId, "tenantId");
    validateIdentity(request.callId, "callId");
    const [response, usage, routeRefs, owner] = await Promise.all([
      digestJson(request.response), digestJson(request.usage), digestJson(request.routeRefs),
      digestJson({ tenantId: request.tenantId, taskId: request.taskId }),
    ]);
    const existingRow = this.storage.sql.exec<ModelCallRow>("SELECT * FROM task_model_call_results WHERE task_id = ? AND call_id = ?", request.taskId, request.callId).toArray()[0];
    if (existingRow) {
      assertSameModelCall(existingRow, request, response.sha256, usage.canonicalJson, routeRefs.canonicalJson);
      await confirmCellWrite(this.storage);
      return this.getModelCallResult(request.taskId, request.callId);
    }
    const existingTask = requireTask(this.storage.sql, request.taskId, request.tenantId);
    const turnSeq = request.turnSeq ?? (Number(existingTask.last_turn_seq) + 1);
    if (!Number.isSafeInteger(turnSeq) || turnSeq < 1) throw new TaskTurnError("MODEL_CALL_TURN_INVALID", "Model call turnSeq must be a positive safe integer.");
    if (turnSeq > Number(existingTask.last_turn_seq) + 1) throw new TaskTurnError("MODEL_CALL_TURN_GAP", `Model call Turn ${turnSeq} is ahead of the next TaskCell Turn.`);
    const bytes = new TextEncoder().encode(response.canonicalJson);
    const chunks = await this.putModelResponseBlocks(request, bytes, owner.sha256);
    const now = Date.now();
    withCellTransaction(this.storage, () => {
      const task = requireTask(this.storage.sql, request.taskId, request.tenantId);
      if (turnSeq > Number(task.last_turn_seq) + 1) throw new TaskTurnError("MODEL_CALL_TURN_GAP", `Model call Turn ${turnSeq} is ahead of the next TaskCell Turn.`);
      const existing = this.storage.sql.exec<ModelCallRow>("SELECT * FROM task_model_call_results WHERE task_id = ? AND call_id = ?", request.taskId, request.callId).toArray()[0];
      if (existing) {
        assertSameModelCall(existing, request, response.sha256, usage.canonicalJson, routeRefs.canonicalJson);
        return;
      }
      this.storage.sql.exec(`INSERT INTO task_model_call_results (
        task_id, tenant_id, call_id, turn_seq, response_sha256, response_size_bytes, response_chunks_json,
        usage_json, route_refs_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, request.taskId, request.tenantId, request.callId, turnSeq,
      response.sha256, bytes.byteLength, JSON.stringify(chunks), usage.canonicalJson, routeRefs.canonicalJson, now);
      for (const chunk of chunks) {
        this.storage.sql.exec(`INSERT INTO task_snapshot_refs (
          ref_id, task_id, tenant_id, turn_seq, ref_kind, owner_id, block_index, object_key, sha256, size_bytes,
          status, tombstoned_at, delete_after, unknown_reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        stableModelCallRefId(request.taskId, request.callId, chunk.index), request.taskId, request.tenantId,
        turnSeq, "MODEL_CALL_RESULT", request.callId, chunk.index, chunk.objectKey, chunk.sha256, chunk.sizeBytes,
        "ACTIVE", null, null, null, now);
      }
    });
    await confirmCellWrite(this.storage);
    return this.getModelCallResult(request.taskId, request.callId);
  }

  public async getModelCallResult(taskId: string, callId: string): Promise<ModelCallResultRecord> {
    const row = this.storage.sql.exec<ModelCallRow>("SELECT * FROM task_model_call_results WHERE task_id = ? AND call_id = ?", taskId, callId).toArray()[0];
    if (!row) throw new SnapshotRestoreError("MODEL_CALL_RESULT_MISSING", `Model call result ${callId} is missing for task ${taskId}.`);
    if (row.task_id !== taskId || row.call_id !== callId || readTaskExecution(this.storage.sql, taskId)?.tenant_id !== row.tenant_id) {
      throw new SnapshotRestoreError("MODEL_CALL_RESULT_OWNER_MISMATCH", `Model call result ${callId} does not match its TaskCell owner.`);
    }
    const chunks = JSON.parse(row.response_chunks_json) as ModelCallChunk[];
    const restored: Uint8Array[] = [];
    for (const chunk of chunks) {
      const ref = this.storage.sql.exec<ReferenceRow>("SELECT * FROM task_snapshot_refs WHERE task_id = ? AND ref_kind = ? AND owner_id = ? AND block_index = ?",
        taskId, "MODEL_CALL_RESULT", callId, chunk.index).toArray()[0];
      if (!ref || ref.status === "DELETED" || ref.status === "TOMBSTONED") {
        throw new SnapshotRestoreError("MODEL_CALL_RESULT_REFERENCE_REVOKED", `Model call result ${callId} block ${chunk.index} is not retained.`);
      }
      restored.push(await this.readAndVerifyBlock(chunk.objectKey, chunk.sha256, chunk.sizeBytes, "MODEL_CALL_RESULT_BLOCK"));
    }
    const bytes = concatBytes(restored, Number(row.response_size_bytes));
    if (await sha256Hex(bytes) !== row.response_sha256) throw new SnapshotRestoreError("MODEL_CALL_RESULT_DIGEST_MISMATCH", `Model call result ${callId} failed its whole-response digest check.`);
    let response: unknown;
    try { response = JSON.parse(new TextDecoder().decode(bytes)); }
    catch { throw new SnapshotRestoreError("MODEL_CALL_RESULT_INVALID_JSON", `Model call result ${callId} is not valid JSON.`); }
    return {
      taskId: row.task_id, tenantId: row.tenant_id, callId: row.call_id, turnSeq: Number(row.turn_seq),
      response, responseSha256: row.response_sha256, usage: JSON.parse(row.usage_json),
      routeRefs: JSON.parse(row.route_refs_json), createdAt: Number(row.created_at),
    };
  }

  /** Confirm the staged result before an external quota/settlement effect relies on it. */
  public async getConfirmedModelCallResult(taskId: string, callId: string): Promise<ModelCallResultRecord> {
    const result = await this.getModelCallResult(taskId, callId);
    await confirmCellWrite(this.storage);
    return result;
  }

  /** Read the latest committed Turn. A damaged latest snapshot blocks recovery visibly; it never falls back silently. */
  public async recoverLatest(taskId: string): Promise<RestoredTaskTurn> {
    const row = this.storage.sql.exec<TurnRow>("SELECT * FROM task_turns WHERE task_id = ? ORDER BY turn_seq DESC LIMIT 1", taskId).toArray()[0];
    if (!row) throw new SnapshotRestoreError("NO_COMMITTED_TURN", `Task ${taskId} has no fully committed Turn to recover.`);
    const turn = mapTurn(row);
    const manifestRow = this.storage.sql.exec<ManifestRow>("SELECT * FROM task_snapshot_manifests WHERE manifest_id = ?", turn.manifestId).toArray()[0];
    if (!manifestRow) throw new SnapshotRestoreError("SNAPSHOT_MANIFEST_MISSING", `Committed Turn ${turn.turnSeq} is missing its snapshot manifest.`);
    if (manifestRow.task_id !== taskId || manifestRow.tenant_id !== turn.tenantId || Number(manifestRow.turn_seq) !== turn.turnSeq ||
        manifestRow.upload_id !== turn.uploadId || manifestRow.manifest_sha256 !== turn.manifestSha256 ||
        await sha256Text(manifestRow.manifest_json) !== turn.manifestSha256) {
      throw new SnapshotRestoreError("SNAPSHOT_MANIFEST_DIGEST_MISMATCH", `Committed Turn ${turn.turnSeq} manifest failed SHA-256 verification.`);
    }
    const manifest = JSON.parse(manifestRow.manifest_json) as SnapshotManifest;
    if (manifest.taskId !== taskId || manifest.tenantId !== turn.tenantId || manifest.turnSeq !== turn.turnSeq || manifest.uploadId !== turn.uploadId) {
      throw new SnapshotRestoreError("SNAPSHOT_MANIFEST_IDENTITY_MISMATCH", `Committed Turn ${turn.turnSeq} manifest identity does not match its Turn.`);
    }
    const refRows = this.storage.sql.exec<ReferenceRow>(
      "SELECT * FROM task_snapshot_refs WHERE task_id = ? AND ref_kind = ? AND owner_id = ? ORDER BY block_index",
      taskId, "SNAPSHOT_BLOCK", turn.turnId,
    ).toArray();
    if (refRows.length !== manifest.blocks.length) throw new SnapshotRestoreError("SNAPSHOT_REFERENCE_MISSING", `Committed Turn ${turn.turnSeq} has incomplete snapshot references.`);
    const snapshotBlocks: Uint8Array[] = [];
    for (let index = 0; index < manifest.blocks.length; index += 1) {
      const block = manifest.blocks[index];
      const ref = refRows[index];
      if (ref.status !== "ACTIVE" || ref.object_key !== block.objectKey || ref.sha256 !== block.sha256 || Number(ref.size_bytes) !== block.sizeBytes) {
        throw new SnapshotRestoreError("SNAPSHOT_REFERENCE_INVALID", `Committed Turn ${turn.turnSeq} block ${block.index} is missing or revoked.`);
      }
      snapshotBlocks.push(await this.readAndVerifyBlock(block.objectKey, block.sha256, block.sizeBytes, "SNAPSHOT_BLOCK"));
    }
    // Include the base Turn's operation sequence because an in-flight result may have arrived after its commit.
    const laterOperationOutcomes = this.operations.listOperations(taskId).filter((operation) => operation.turnSeq >= turn.turnSeq);
    const laterCompensations = this.operations.listCompensations(taskId, turn.turnSeq - 1);
    const committedModelCalls = await Promise.all(turn.modelCallRefs.map((callId) => this.getModelCallResult(taskId, callId)));
    if (committedModelCalls.some((call) => call.tenantId !== turn.tenantId || call.turnSeq !== turn.turnSeq)) {
      throw new SnapshotRestoreError("MODEL_CALL_RESULT_TURN_MISMATCH", `Committed Turn ${turn.turnSeq} references a model result from another tenant or Turn.`);
    }
    const committedModelCallIds = new Set(turn.modelCallRefs);
    const modelRows = this.storage.sql.exec<ModelCallRow>(
      "SELECT * FROM task_model_call_results WHERE task_id = ? AND turn_seq >= ? ORDER BY turn_seq, created_at, call_id", taskId, turn.turnSeq,
    ).toArray().filter((call) => !committedModelCallIds.has(call.call_id));
    const laterModelCalls = await Promise.all(modelRows.map((call) => this.getModelCallResult(taskId, call.call_id)));
    const laterSettlementRefs = uniqueStrings(laterModelCalls.flatMap((call) => [
      ...settlementRefsFromUsage(call.usage), ...settlementRefsFromUsage(call.routeRefs),
    ]));
    return { ...turn, manifest, snapshotBlocks, laterOperationOutcomes, laterCompensations,
      committedModelCalls, laterModelCalls, laterSettlementRefs };
  }

  /** Mark an owner reference as uncertain; GC will keep it until a separate reconciliation resolves it. */
  public async markReferenceUnknown(refId: string, reason: string): Promise<void> {
    validateIdentity(refId, "refId");
    validateIdentity(reason, "reason");
    withCellTransaction(this.storage, () => {
      const ref = this.storage.sql.exec<ReferenceRow>("SELECT * FROM task_snapshot_refs WHERE ref_id = ?", refId).toArray()[0];
      if (!ref || ref.status === "DELETED") throw new TaskTurnError("SNAPSHOT_REFERENCE_NOT_FOUND", `Snapshot reference ${refId} was not found.`, 404);
      this.storage.sql.exec("UPDATE task_snapshot_refs SET status = ?, tombstoned_at = ?, delete_after = ?, unknown_reason = ? WHERE ref_id = ?",
        "UNKNOWN", null, null, reason, refId);
    });
    await confirmCellWrite(this.storage);
  }

  /** Tombstone first; physically delete only after grace, while task and effect ledgers prove the ref is safe. */
  public async collectGarbage(now = Date.now()): Promise<{ tombstoned: number; deleted: number; retained: number; failures: string[] }> {
    if (!Number.isSafeInteger(now) || now < 0) throw new TypeError("now must be a non-negative safe integer.");
    const retention = this.options.snapshotRetentionMs ?? 30 * 24 * 60 * 60 * 1000;
    const grace = this.options.gcGracePeriodMs ?? 24 * 60 * 60 * 1000;
    const allRefs = this.storage.sql.exec<ReferenceRow>("SELECT * FROM task_snapshot_refs ORDER BY task_id, turn_seq")
      .toArray().filter((ref) => ref.status === "ACTIVE" || ref.status === "TOMBSTONED");
    const tasks = Array.from(new Set(allRefs.map((ref) => ref.task_id)));
    const safety = new Map<string, { safe: boolean; latestTurn: number; latestModelCalls: Set<string> }>();
    for (const taskId of tasks) safety.set(taskId, this.garbageSafety(taskId, now, retention));

    let tombstoned = 0;
    withCellTransaction(this.storage, () => {
      for (const ref of allRefs) {
        if (ref.status !== "ACTIVE") continue;
        const state = safety.get(ref.task_id)!;
        if (!state.safe || isCurrentRecoveryReference(ref, state.latestTurn, state.latestModelCalls) ||
            Number(ref.created_at) > now - retention || Number(ref.turn_seq) >= state.latestTurn) continue;
        this.storage.sql.exec("UPDATE task_snapshot_refs SET status = ?, tombstoned_at = ?, delete_after = ? WHERE ref_id = ? AND status = ?",
          "TOMBSTONED", now, now + grace, ref.ref_id, "ACTIVE");
        tombstoned += 1;
      }
    });
    if (tombstoned > 0) await confirmCellWrite(this.storage);

    const dueRefs = this.storage.sql.exec<ReferenceRow>("SELECT * FROM task_snapshot_refs WHERE status = ? AND delete_after <= ? ORDER BY task_id, turn_seq",
      "TOMBSTONED", now).toArray();
    const failures: string[] = [];
    let deleted = 0;
    let retained = 0;
    for (const ref of dueRefs) {
      const state = this.garbageSafety(ref.task_id, now, retention);
      if (!state.safe || isCurrentRecoveryReference(ref, state.latestTurn, state.latestModelCalls) ||
          Number(ref.turn_seq) >= state.latestTurn || Number(ref.created_at) > now - retention) {
        withCellTransaction(this.storage, () => {
          this.storage.sql.exec("UPDATE task_snapshot_refs SET status = ?, tombstoned_at = ?, delete_after = ? WHERE ref_id = ? AND status = ?",
            "ACTIVE", null, null, ref.ref_id, "TOMBSTONED");
        });
        await confirmCellWrite(this.storage);
        retained += 1;
        continue;
      }
      try {
        // An earlier attempt may have committed the tombstone locally but lost its
        // bucket acknowledgment. Reconfirm revocation before every physical retry.
        await confirmCellWrite(this.storage);
        const current = this.storage.sql.exec<ReferenceRow>("SELECT * FROM task_snapshot_refs WHERE ref_id = ?", ref.ref_id).toArray()[0];
        if (!current || current.status !== "TOMBSTONED" || !this.garbageSafety(ref.task_id, now, retention).safe) {
          retained += 1;
          continue;
        }
        await this.blockStore.delete(ref.object_key);
        withCellTransaction(this.storage, () => {
          this.storage.sql.exec("UPDATE task_snapshot_refs SET status = ?, tombstoned_at = ?, delete_after = ? WHERE ref_id = ? AND status = ?",
            "DELETED", ref.tombstoned_at, ref.delete_after, ref.ref_id, "TOMBSTONED");
        });
        await confirmCellWrite(this.storage);
        deleted += 1;
      } catch (error) {
        failures.push(`${ref.ref_id}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    const openUploads = this.storage.sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE status = ?", "UPLOADING").toArray();
    const orphanUploads = openUploads.filter((upload) => {
      const state = this.garbageSafety(upload.task_id, now, retention);
      return state.safe && Number(upload.updated_at) <= now - retention;
    });
    if (orphanUploads.length > 0) {
      withCellTransaction(this.storage, () => {
        for (const upload of orphanUploads) {
          this.storage.sql.exec("UPDATE task_turn_uploads SET status = ?, updated_at = ? WHERE upload_id = ? AND status = ?",
            "ABORTED", now, upload.upload_id, "UPLOADING");
        }
      });
      await confirmCellWrite(this.storage);
    }

    const abortedUploads = this.storage.sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE status = ?", "ABORTED")
      .toArray().filter((upload) => Number(upload.updated_at) <= now - grace);
    for (const upload of abortedUploads) {
      const chunks = this.storage.sql.exec<UploadChunkRow>("SELECT * FROM task_turn_upload_chunks WHERE upload_id = ?", upload.upload_id).toArray();
      for (const chunk of chunks) {
        try {
          await confirmCellWrite(this.storage);
          if (this.readUpload(upload.upload_id)?.status !== "ABORTED") { retained += 1; continue; }
          await this.blockStore.delete(chunk.object_key);
          withCellTransaction(this.storage, () => {
            this.storage.sql.exec("DELETE FROM task_turn_upload_chunks WHERE upload_id = ? AND chunk_index = ?", chunk.upload_id, chunk.chunk_index);
          });
          await confirmCellWrite(this.storage);
          deleted += 1;
        } catch (error) { failures.push(`${chunk.object_key}: ${error instanceof Error ? error.message : String(error)}`); }
      }
    }
    return { tombstoned, deleted, retained, failures };
  }

  private async putModelResponseBlocks(request: RecordModelCallResultRequest, bytes: Uint8Array, ownerHash: string): Promise<ModelCallChunk[]> {
    const max = this.options.modelCallChunkBytes ?? DEFAULT_MODEL_CALL_CHUNK_BYTES;
    const chunks: ModelCallChunk[] = [];
    for (let offset = 0, index = 0; offset < bytes.byteLength; offset += max, index += 1) {
      const part = bytes.slice(offset, Math.min(offset + max, bytes.byteLength));
      const sha256 = await sha256Hex(part);
      const objectKey = `task-model-call-results/${ownerHash}/${encodeURIComponent(request.callId)}/${index}-${sha256}`;
      await putAndVerifyBlock(this.blockStore, objectKey, part, sha256);
      chunks.push({ index, sha256, sizeBytes: part.byteLength, objectKey });
    }
    return chunks;
  }

  private async readAndVerifyBlock(key: string, digest: string, sizeBytes: number, label: string): Promise<Uint8Array> {
    let bytes: Uint8Array | null;
    try { bytes = await this.blockStore.get(key); }
    catch (error) { throw new SnapshotRestoreError(`${label}_READ_FAILED`, `Could not read ${label.toLowerCase()} ${key}: ${message(error)}`); }
    if (bytes === null) throw new SnapshotRestoreError(`${label}_MISSING`, `${label.toLowerCase()} ${key} is missing.`);
    if (bytes.byteLength !== sizeBytes) throw new SnapshotRestoreError(`${label}_SIZE_MISMATCH`, `${label.toLowerCase()} ${key} has the wrong byte length.`);
    if (await sha256Hex(bytes) !== digest) throw new SnapshotRestoreError(`${label}_DIGEST_MISMATCH`, `${label.toLowerCase()} ${key} failed SHA-256 verification.`);
    return new Uint8Array(bytes);
  }

  private readUpload(uploadId: string): UploadRow | null {
    return this.storage.sql.exec<UploadRow>("SELECT * FROM task_turn_uploads WHERE upload_id = ?", uploadId).toArray()[0] ?? null;
  }

  private getTurn(taskId: string, turnSeq: number): CommittedTaskTurn | null {
    const row = this.storage.sql.exec<TurnRow>("SELECT * FROM task_turns WHERE task_id = ? AND turn_seq = ?", taskId, turnSeq).toArray()[0];
    return row ? mapTurn(row) : null;
  }

  private taskCommand(operationId: string, taskId: string, tenantId: string, operationType: string, payload: unknown, aggregateType: string): CellOperationCommand {
    const existing = this.storage.sql.exec<{ expected_version: number }>("SELECT expected_version FROM cell_operations WHERE operation_id = ?", operationId).toArray()[0];
    let expectedVersion = existing ? Number(existing.expected_version) : 0;
    if (!existing) {
      const aggregate = this.storage.sql.exec<{ version: number }>(
        "SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?", aggregateType, taskId,
      ).toArray()[0];
      if (aggregate) expectedVersion = Number(aggregate.version);
    }
    return { operationId, operationType, resourceId: taskId, tenantId, payload, aggregateType, aggregateId: taskId,
      expectedVersion };
  }

  private garbageSafety(taskId: string, now: number, retentionMs: number): { safe: boolean; latestTurn: number; latestModelCalls: Set<string> } {
    const task = readTaskExecution(this.storage.sql, taskId);
    const latest = this.storage.sql.exec<{ turn_seq: number; committed_at: number }>(
      "SELECT turn_seq, committed_at FROM task_turns WHERE task_id = ? ORDER BY turn_seq DESC LIMIT 1", taskId,
    ).toArray()[0];
    if (!task || !latest) return { safe: false, latestTurn: Number.MAX_SAFE_INTEGER, latestModelCalls: new Set() };
    const terminal = task.lifecycle_state === "SUCCEEDED" || task.lifecycle_state === "FAILED";
    const operations = this.operations.listOperations(taskId);
    const compensations = this.operations.listCompensations(taskId, 0);
    const unresolvedOperation = operations.some((operation) => ["READY", "RUNNING", "OUTCOME_UNKNOWN", "APPROVAL_REQUIRED"].includes(operation.status));
    const unresolvedCompensation = compensations.some((compensation) => ["PENDING", "RUNNING", "COMPENSATION_FAILED", "OUTCOME_UNKNOWN"].includes(compensation.status));
    const latestTurn = this.storage.sql.exec<TurnRow>("SELECT * FROM task_turns WHERE task_id = ? AND turn_seq = ?", taskId, latest.turn_seq).toArray()[0];
    const latestModelCalls = new Set<string>(latestTurn ? JSON.parse(latestTurn.model_call_refs_json) as string[] : []);
    const unreferencedModelCall = this.storage.sql.exec<ModelCallRow>(
      "SELECT * FROM task_model_call_results WHERE task_id = ? AND turn_seq >= ?", taskId, Number(latest.turn_seq),
    ).toArray().some((call) => !latestModelCalls.has(call.call_id));
    const oldEnough = Number(latest.committed_at) <= now - retentionMs;
    return { safe: terminal && !unresolvedOperation && !unresolvedCompensation && !unreferencedModelCall && oldEnough,
      latestTurn: Number(latest.turn_seq), latestModelCalls };
  }
}

function normalizeDescriptors(chunks: SnapshotChunkDescriptor[]): SnapshotChunkDescriptor[] {
  if (!Array.isArray(chunks)) throw new TypeError("chunks must be an array.");
  const sorted = chunks.map((chunk) => {
    if (!Number.isSafeInteger(chunk.index) || chunk.index < 0) throw new TypeError("Chunk indexes must be non-negative safe integers.");
    if (!Number.isSafeInteger(chunk.sizeBytes) || chunk.sizeBytes < 0) throw new TypeError("Chunk sizes must be non-negative safe integers.");
    if (!/^[a-f0-9]{64}$/.test(chunk.sha256)) throw new TypeError("Chunk SHA-256 must be 64 lowercase hexadecimal characters.");
    return { index: chunk.index, sha256: chunk.sha256, sizeBytes: chunk.sizeBytes };
  }).sort((a, b) => a.index - b.index);
  if (sorted.some((chunk, index) => chunk.index !== index)) throw new TypeError("Chunk indexes must be contiguous from zero.");
  return sorted;
}

function normalizeStringRefs(values: string[], label: string): string[] {
  if (!Array.isArray(values)) throw new TypeError(`${label} must be an array.`);
  const refs = values.map((value) => { validateIdentity(value, `${label} entry`); return value; });
  if (new Set(refs).size !== refs.length) throw new TypeError(`${label} cannot contain duplicates.`);
  return refs;
}

function assertFence(taskId: string, fence: FenceToken): void {
  if (!fence || fence.taskId !== taskId || !Number.isSafeInteger(fence.epoch) || fence.epoch < 1 ||
      !Number.isSafeInteger(fence.turnSeq) || fence.turnSeq < 1) throw new TaskTurnError("TASK_TURN_FENCE_INVALID", "Turn fence must name the task and use positive epoch and turn sequence.");
}

function assertRunnableFence(task: TaskExecutionRecord, fence: FenceToken): void {
  if (Number(task.business_epoch) !== fence.epoch) throw new TaskTurnError("STALE_EXECUTION_EPOCH", `Task ${task.task_id} is at business epoch ${task.business_epoch}.`);
  if (task.lifecycle_state !== "RUNNING") throw new TaskTurnError("TASK_NOT_EXECUTING", `Task ${task.task_id} is ${task.lifecycle_state}.`);
  if (Number(task.last_turn_seq) + 1 !== fence.turnSeq) throw new TaskTurnError("TASK_TURN_SEQUENCE_CONFLICT", `Task ${task.task_id} expects Turn ${Number(task.last_turn_seq) + 1}.`);
}

function requireTask(sql: PersistenceSqlStorage, taskId: string, tenantId: string): TaskExecutionRecord {
  const task = readTaskExecution(sql, taskId);
  if (!task) throw new TaskTurnError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
  if (task.tenant_id !== tenantId) throw new TaskTurnError("TASK_TENANT_CONFLICT", `Task ${taskId} belongs to another tenant.`);
  return task;
}

function mapTurn(row: TurnRow): CommittedTaskTurn {
  return {
    taskId: row.task_id, tenantId: row.tenant_id, turnSeq: Number(row.turn_seq), businessEpoch: Number(row.business_epoch),
    requestDigest: row.request_digest, commitDigest: row.commit_digest,
    turnId: row.turn_id, uploadId: row.upload_id, manifestId: row.manifest_id, manifestSha256: row.manifest_sha256,
    response: JSON.parse(row.response_json), modelCallRefs: JSON.parse(row.model_call_refs_json),
    settlementRefs: JSON.parse(row.settlement_refs_json), ...(row.progress_json === null ? {} : { progress: JSON.parse(row.progress_json) }),
    committedAt: Number(row.committed_at),
  };
}

function assertSameTurnRequest(turn: CommittedTaskTurn, request: CommitTaskTurnRequest, commitDigest: string): void {
  if (turn.tenantId !== request.tenantId || turn.uploadId !== request.uploadId || turn.businessEpoch !== request.fence.epoch ||
      turn.requestDigest !== request.requestDigest || turn.commitDigest !== commitDigest) {
    throw new TaskTurnError("TASK_TURN_CONFLICT", `Turn ${request.fence.turnSeq} is already committed with different content.`);
  }
}

function assertSameModelCall(row: ModelCallRow, request: RecordModelCallResultRequest, responseSha256: string, usageJson: string, routeRefsJson: string): void {
  if (row.tenant_id !== request.tenantId || Number(row.turn_seq) !== (request.turnSeq ?? Number(row.turn_seq)) ||
      row.response_sha256 !== responseSha256 || row.usage_json !== usageJson || row.route_refs_json !== routeRefsJson) {
    throw new TaskTurnError("MODEL_CALL_RESULT_CONFLICT", `Model call ${request.callId} already has a different durable result.`);
  }
}

function isCurrentRecoveryReference(ref: ReferenceRow, latestTurn: number, latestModelCalls: Set<string>): boolean {
  return (ref.ref_kind === "SNAPSHOT_BLOCK" && Number(ref.turn_seq) === latestTurn) ||
    (ref.ref_kind === "MODEL_CALL_RESULT" && latestModelCalls.has(ref.owner_id));
}

function settlementRefsFromUsage(usage: unknown): string[] {
  if (!usage || typeof usage !== "object") return [];
  const value = usage as Record<string, unknown>;
  const refs = value.settlementRefs ?? value.settlement_refs ?? value.settlementId ?? value.settlement_id;
  if (typeof refs === "string") return [refs];
  if (Array.isArray(refs)) return refs.filter((entry): entry is string => typeof entry === "string");
  return [];
}

function uniqueStrings(values: string[]): string[] { return Array.from(new Set(values)); }
function stableTurnId(taskId: string, turnSeq: number): string { return `task-turn:${taskId.length}:${taskId}:${turnSeq}`; }
function stableManifestId(taskId: string, turnSeq: number): string { return `task-turn-manifest:${taskId.length}:${taskId}:${turnSeq}`; }
function stableSnapshotRefId(turnId: string, index: number): string { return `${turnId}:snapshot-block:${index}`; }
function stableModelCallRefId(taskId: string, callId: string, index: number): string { return `task-model-call:${taskId.length}:${taskId}:${callId.length}:${callId}:${index}`; }
function validateIdentity(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0 || /[\u0000-\u001f\u007f]/.test(value)) throw new TypeError(`${label} must be a non-empty string without control characters.`);
}
async function sha256Text(value: string): Promise<string> { return sha256Hex(new TextEncoder().encode(value)); }
function concatBytes(parts: Uint8Array[], expectedLength: number): Uint8Array {
  const actual = parts.reduce((sum, part) => sum + part.byteLength, 0);
  if (actual !== expectedLength) throw new SnapshotRestoreError("MODEL_CALL_RESULT_SIZE_MISMATCH", "Model call response blocks do not match the stored total size.");
  const joined = new Uint8Array(actual);
  let offset = 0;
  for (const part of parts) { joined.set(part, offset); offset += part.byteLength; }
  return joined;
}
function message(error: unknown): string { return error instanceof Error ? error.message : String(error); }
