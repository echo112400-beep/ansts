import { CellOperationConflictError, CellOperationResult, executeCellOperation } from "../operations";
import { digestJson } from "../digest";
import { initializeCellProtocol } from "../schema";
import { CellOutboxDraft, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { CellDurabilityUnavailableError } from "../transactions";
import { TaskSpecValidationError, TaskSpecValidator } from "../../gateway/validator";
import { CloudflareCelldEnv, DurableObjectState } from "../../cloudflare/env";

const MAX_PENDING_ADMISSIONS = 10_000;
const IDEMPOTENCY_WINDOW_MS = 24 * 60 * 60 * 1000;

const tenantLedgerMigration = {
  version: 2,
  description: "tenant admission, queue, budget and allowance ledger",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS tenant_admissions (
        tenant_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        operation_id TEXT NOT NULL UNIQUE,
        request_digest TEXT NOT NULL,
        request_json TEXT NOT NULL,
        task_spec_json TEXT NOT NULL,
        status TEXT NOT NULL,
        runnable_status TEXT NOT NULL,
        accepted_at INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, task_id),
        UNIQUE (tenant_id, idempotency_key)
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS admission_queue (
        tenant_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        queue_name TEXT NOT NULL,
        priority_class TEXT NOT NULL,
        priority_score INTEGER NOT NULL,
        queued_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (tenant_id, task_id)
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS request_dedup (
        tenant_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        task_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        unresolved INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, idempotency_key)
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS admission_queue_pending_idx ON admission_queue (status, queued_at)");
    sql.exec("CREATE INDEX IF NOT EXISTS request_dedup_expiry_idx ON request_dedup (expires_at, unresolved)");
    sql.exec(`
      CREATE TABLE IF NOT EXISTS tenant_budget_periods (
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        currency TEXT NOT NULL,
        hard_limit_minor INTEGER NOT NULL,
        price_version TEXT NOT NULL,
        policy_version INTEGER NOT NULL,
        allocated_minor INTEGER NOT NULL,
        reserved_minor INTEGER NOT NULL,
        spent_minor INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, period_key)
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS task_allowances (
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        task_id TEXT NOT NULL,
        available_minor INTEGER NOT NULL,
        reserved_minor INTEGER NOT NULL,
        spent_minor INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, period_key, task_id)
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS budget_grants (
        grant_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        task_id TEXT NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS budget_holds (
        hold_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        task_id TEXT NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency TEXT NOT NULL,
        price_version TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        unknown_reason TEXT,
        settlement_id TEXT
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS budget_settlements (
        settlement_id TEXT PRIMARY KEY,
        hold_id TEXT NOT NULL UNIQUE,
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        task_id TEXT NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency TEXT NOT NULL,
        usage_reference TEXT NOT NULL,
        settled_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS quota_transfers (
        transfer_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        period_key TEXT NOT NULL,
        source_task_id TEXT NOT NULL,
        target_task_id TEXT NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);
  },
};

interface AdmissionRow {
  tenant_id: string;
  task_id: string;
  idempotency_key: string;
  operation_id: string;
  request_digest: string;
  request_json: string;
  task_spec_json: string;
  status: string;
  runnable_status: string;
  accepted_at: number;
}

interface DedupRow {
  task_id: string;
  request_digest: string;
  operation_id: string;
  expires_at: number;
  unresolved: number;
}

interface BudgetPeriodRow {
  tenant_id: string;
  period_key: string;
  currency: string;
  hard_limit_minor: number;
  price_version: string;
  policy_version: number;
  allocated_minor: number;
  reserved_minor: number;
  spent_minor: number;
}

interface TaskAllowanceRow {
  tenant_id: string;
  period_key: string;
  task_id: string;
  available_minor: number;
  reserved_minor: number;
  spent_minor: number;
}

interface BudgetHoldRow {
  hold_id: string;
  tenant_id: string;
  period_key: string;
  task_id: string;
  amount_minor: number;
  currency: string;
  price_version: string;
  status: string;
  created_at: number;
  unknown_reason: string | null;
  settlement_id: string | null;
}

export interface TenantAdmissionResult {
  accepted: true;
  tenant_id: string;
  task_id: string;
  idempotency_key: string;
  operation_id: string;
  status: "ACCEPTED";
  runnable_status: "PENDING_DISPATCH";
  accepted_at: number;
}

export class TenantLedgerError extends Error {
  public readonly code: string;

  constructor(
    code: string,
    message: string,
    public readonly status: number,
  ) {
    super(message);
    this.name = "TenantLedgerError";
    this.code = code;
  }
}

/** Per-tenant durable admission and budget authority. The Cell name is the tenantId. */
export class TenantLedgerCell {
  private readonly tenantId: string;
  private readonly validator = new TaskSpecValidator();

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    this.tenantId = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(ctx.storage.sql, { type: "tenant-ledger", key: this.tenantId }, [tenantLedgerMigration]);
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/admissions") {
        return await this.admit(request);
      }
      const budgetPeriodMatch = url.pathname.match(/^\/budget-periods\/([^/]+)$/);
      if (budgetPeriodMatch && request.method === "PUT") {
        return await this.configureBudgetPeriod(decodeURIComponent(budgetPeriodMatch[1]), request);
      }
      if (budgetPeriodMatch && request.method === "GET") {
        return this.getBudgetPeriod(decodeURIComponent(budgetPeriodMatch[1]), url.searchParams.get("tenant_id"));
      }
      if (url.pathname === "/budget-grants" && request.method === "POST") {
        return await this.grantBudget(request);
      }
      const budgetAccountMatch = url.pathname.match(/^\/budget-accounts\/([^/]+)$/);
      if (budgetAccountMatch && request.method === "GET") {
        return this.getBudgetAccount(
          decodeURIComponent(budgetAccountMatch[1]),
          url.searchParams.get("period_key"),
          url.searchParams.get("tenant_id"),
        );
      }
      if (url.pathname === "/budget-holds" && request.method === "POST") {
        return await this.createBudgetHold(request);
      }
      if (url.pathname === "/budget-holds" && request.method === "GET") {
        return this.listBudgetHolds(
          url.searchParams.get("period_key"),
          url.searchParams.get("status"),
          url.searchParams.get("tenant_id"),
        );
      }
      const holdUnknownMatch = url.pathname.match(/^\/budget-holds\/([^/]+)\/unknown$/);
      if (holdUnknownMatch && request.method === "POST") {
        return await this.markHoldUnknown(decodeURIComponent(holdUnknownMatch[1]), request);
      }
      const holdSettlementMatch = url.pathname.match(/^\/budget-holds\/([^/]+)\/settlements$/);
      if (holdSettlementMatch && request.method === "POST") {
        return await this.settleBudgetHold(decodeURIComponent(holdSettlementMatch[1]), request);
      }
      if (url.pathname === "/allowance-transfers" && request.method === "POST") {
        return await this.transferAllowance(request);
      }
      if (url.pathname === "/tasks" && request.method === "GET") {
        return this.listTasks(url.searchParams.get("tenant_id"), Number(url.searchParams.get("limit") ?? 100));
      }
      const taskMatch = url.pathname.match(/^\/tasks\/([^/]+)$/);
      if (request.method === "GET" && taskMatch) {
        return this.getTask(decodeURIComponent(taskMatch[1]), url.searchParams.get("tenant_id"));
      }
      const admissionMatch = url.pathname.match(/^\/admissions\/by-key\/([^/]+)$/);
      if (request.method === "GET" && admissionMatch) {
        return this.getAdmission(decodeURIComponent(admissionMatch[1]), url.searchParams.get("tenant_id"));
      }
      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      return this.errorResponse(error);
    }
  }

  private async admit(request: Request): Promise<Response> {
    let rawSpec: Record<string, any>;
    try {
      rawSpec = (await request.json()) as Record<string, any>;
    } catch {
      throw new TenantLedgerError("INVALID_JSON", "TaskSpec must be valid JSON.", 400);
    }

    const taskSpec = this.validator.validateAndNormalize(rawSpec);
    const metadata = taskSpec.metadata;
    this.assertTenant(metadata.tenant_id);
    const taskId = requireText(metadata.task_id, "metadata.task_id");
    const idempotencyKey = requireText(metadata.idempotency_key, "metadata.idempotency_key");
    if (idempotencyKey.length > 512) {
      throw new TenantLedgerError("INVALID_IDEMPOTENCY_KEY", "Idempotency key must be at most 512 characters.", 400);
    }

    const digest = await digestJson(rawSpec);
    const operationId = `admission:${idempotencyKey}`;
    const now = Date.now();
    const result = await this.applyOperation(
      {
        operationId,
        operationType: "tenant.task.admit",
        resourceId: taskId,
        tenantId: this.tenantId,
        payload: rawSpec,
        aggregateType: "tenant-admission",
        aggregateId: idempotencyKey,
        expectedVersion: 0,
      },
      (sql) => {
        const existingTask = sql
          .exec<AdmissionRow>(
            "SELECT * FROM tenant_admissions WHERE tenant_id = ? AND task_id = ?",
            this.tenantId,
            taskId,
          )
          .toArray()[0];
        if (existingTask) {
          throw new TenantLedgerError("TASK_ID_CONFLICT", "Task ID is already in use for this tenant.", 409);
        }
        const pending = sql
          .exec<{ task_id: string }>(
            "SELECT task_id FROM admission_queue WHERE status = ? LIMIT ?",
            "PENDING",
            MAX_PENDING_ADMISSIONS,
          )
          .toArray();
        if (pending.length >= MAX_PENDING_ADMISSIONS) {
          throw new TenantLedgerError("TENANT_BACKPRESSURE", "Tenant admission queue is full; retry later.", 429);
        }

        const acceptedAt = now;
        const response: TenantAdmissionResult = {
          accepted: true,
          tenant_id: this.tenantId,
          task_id: taskId,
          idempotency_key: idempotencyKey,
          operation_id: operationId,
          status: "ACCEPTED",
          runnable_status: "PENDING_DISPATCH",
          accepted_at: acceptedAt,
        };
        sql.exec(
          `INSERT INTO tenant_admissions (
            tenant_id, task_id, idempotency_key, operation_id, request_digest, request_json,
            task_spec_json, status, runnable_status, accepted_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          this.tenantId,
          taskId,
          idempotencyKey,
          operationId,
          digest.sha256,
          digest.canonicalJson,
          JSON.stringify(taskSpec),
          response.status,
          response.runnable_status,
          acceptedAt,
        );
        sql.exec(
          `INSERT INTO request_dedup (
            tenant_id, idempotency_key, task_id, operation_id, request_digest, created_at, expires_at, unresolved
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          this.tenantId,
          idempotencyKey,
          taskId,
          operationId,
          digest.sha256,
          acceptedAt,
          acceptedAt + IDEMPOTENCY_WINDOW_MS,
          1,
        );
        sql.exec(
          `INSERT INTO admission_queue (
            tenant_id, task_id, queue_name, priority_class, priority_score, queued_at, status
          ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
          this.tenantId,
          taskId,
          String(taskSpec.spec.scheduling_policy.queue_name),
          String(metadata.priority_class),
          Number(metadata.priority_score),
          acceptedAt,
          "PENDING",
        );
        return response;
      },
      [
        {
          eventId: `task-admitted:${idempotencyKey}`,
          eventType: "task.admitted",
          tenantId: this.tenantId,
          payload: {
            task_id: taskId,
            tenant_id: this.tenantId,
            idempotency_key: idempotencyKey,
            request_digest: digest.sha256,
          },
        },
      ],
    );

    const body = { ...result.result, deduplicated: result.outcome === "REPLAYED" };
    return json(
      body,
      result.outcome === "REPLAYED" ? 200 : 202,
      result.outcome === "REPLAYED" ? { "X-Idempotent-Replay": "true" } : {},
    );
  }

  private async configureBudgetPeriod(periodKey: string, request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const currency = requireCurrency(body.currency);
    const hardLimit = requireMinorUnits(body.hard_limit_minor, "hard_limit_minor", true);
    const priceVersion = requireText(body.price_version, "price_version");
    const expectedVersion = body.expected_policy_version === undefined
      ? 0
      : requireMinorUnits(body.expected_policy_version, "expected_policy_version", false);
    const operationId = `budget-period:${periodKey}:v${expectedVersion + 1}`;
    const payload = { tenant_id: this.tenantId, period_key: periodKey, currency, hard_limit_minor: hardLimit, price_version: priceVersion, expected_policy_version: expectedVersion };

    return this.runBudgetOperation(operationId, "tenant.budget.period.configure", periodKey, payload, "tenant.budget.period.configured", (sql) => {
      const current = sql
        .exec<BudgetPeriodRow>("SELECT * FROM tenant_budget_periods WHERE tenant_id = ? AND period_key = ?", this.tenantId, periodKey)
        .toArray()[0];
      if (!current) {
        if (expectedVersion !== 0) throw new TenantLedgerError("BUDGET_POLICY_VERSION_CONFLICT", "Budget period does not exist at the expected policy version.", 409);
        const now = Date.now();
        sql.exec(
          `INSERT INTO tenant_budget_periods (
            tenant_id, period_key, currency, hard_limit_minor, price_version, policy_version,
            allocated_minor, reserved_minor, spent_minor, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          this.tenantId, periodKey, currency, hardLimit, priceVersion, 1, 0, 0, 0, now,
        );
        return this.periodResult({
          tenant_id: this.tenantId, period_key: periodKey, currency, hard_limit_minor: hardLimit,
          price_version: priceVersion, policy_version: 1, allocated_minor: 0, reserved_minor: 0, spent_minor: 0,
        });
      }
      if (Number(current.policy_version) !== expectedVersion) {
        throw new TenantLedgerError("BUDGET_POLICY_VERSION_CONFLICT", "Budget period policy changed; read it and retry with the current version.", 409);
      }
      if (current.currency !== currency && (current.allocated_minor || current.reserved_minor || current.spent_minor)) {
        throw new TenantLedgerError("BUDGET_CURRENCY_IMMUTABLE", "Currency cannot change after allowance has been allocated or consumed.", 409);
      }
      if (current.price_version !== priceVersion && (current.allocated_minor || current.reserved_minor || current.spent_minor)) {
        throw new TenantLedgerError("PRICE_VERSION_IMMUTABLE", "Price version cannot change after allowance has been allocated or consumed in this period.", 409);
      }
      if (hardLimit < Number(current.allocated_minor)) {
        throw new TenantLedgerError("BUDGET_LIMIT_BELOW_ALLOCATED", "Hard limit cannot be lower than allowance already allocated.", 409);
      }
      const policyVersion = Number(current.policy_version) + 1;
      sql.exec(
        `UPDATE tenant_budget_periods SET currency = ?, hard_limit_minor = ?, price_version = ?,
         policy_version = ?, updated_at = ? WHERE tenant_id = ? AND period_key = ?`,
        currency, hardLimit, priceVersion, policyVersion, Date.now(), this.tenantId, periodKey,
      );
      return this.periodResult({ ...current, currency, hard_limit_minor: hardLimit, price_version: priceVersion, policy_version: policyVersion });
    });
  }

  private getBudgetPeriod(periodKey: string, requestedTenant: string | null): Response {
    this.assertTenant(requestedTenant);
    const row = this.ctx.storage.sql
      .exec<BudgetPeriodRow>("SELECT * FROM tenant_budget_periods WHERE tenant_id = ? AND period_key = ?", this.tenantId, periodKey)
      .toArray()[0];
    if (!row) return json({ error: "BUDGET_PERIOD_NOT_FOUND", period_key: periodKey }, 404);
    return json(this.periodResult(row));
  }

  private async grantBudget(request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const periodKey = requireText(body.period_key, "period_key");
    const taskId = requireText(body.task_id, "task_id");
    const grantId = requireText(body.grant_id, "grant_id");
    const amount = requireMinorUnits(body.amount_minor, "amount_minor", true);
    const currency = requireCurrency(body.currency);
    const payload = { tenant_id: this.tenantId, period_key: periodKey, task_id: taskId, grant_id: grantId, amount_minor: amount, currency };
    return this.runBudgetOperation(`budget-grant:${grantId}`, "tenant.budget.grant", taskId, payload, "tenant.budget.granted", (sql) => {
      const period = this.requireBudgetPeriod(sql, periodKey);
      if (period.currency !== currency) throw new TenantLedgerError("BUDGET_CURRENCY_MISMATCH", "Grant currency does not match the billing period.", 409);
      const nextAllocated = Number(period.allocated_minor) + amount;
      if (nextAllocated > Number(period.hard_limit_minor)) {
        throw new TenantLedgerError("BUDGET_EXCEEDED", "Grant would exceed the tenant hard budget limit.", 402);
      }
      const account = this.findAllowance(sql, periodKey, taskId);
      if (!account) {
        sql.exec(
          `INSERT INTO task_allowances (tenant_id, period_key, task_id, available_minor, reserved_minor, spent_minor)
           VALUES (?, ?, ?, ?, ?, ?)`,
          this.tenantId, periodKey, taskId, amount, 0, 0,
        );
      } else {
        sql.exec("UPDATE task_allowances SET available_minor = ? WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
          Number(account.available_minor) + amount, this.tenantId, periodKey, taskId);
      }
      sql.exec("UPDATE tenant_budget_periods SET allocated_minor = ?, updated_at = ? WHERE tenant_id = ? AND period_key = ?",
        nextAllocated, Date.now(), this.tenantId, periodKey);
      sql.exec(
        `INSERT INTO budget_grants (grant_id, tenant_id, period_key, task_id, amount_minor, currency, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`, grantId, this.tenantId, periodKey, taskId, amount, currency, Date.now(),
      );
      return { ...payload, available_minor: (account ? Number(account.available_minor) : 0) + amount, allocated_minor: nextAllocated };
    });
  }

  private getBudgetAccount(taskId: string, periodKey: string | null, requestedTenant: string | null): Response {
    this.assertTenant(requestedTenant);
    if (!periodKey) throw new TenantLedgerError("PERIOD_KEY_REQUIRED", "period_key is required.", 400);
    const account = this.findAllowance(this.ctx.storage.sql, periodKey, taskId);
    if (!account) return json({ tenant_id: this.tenantId, period_key: periodKey, task_id: taskId, available_minor: 0, reserved_minor: 0, spent_minor: 0 });
    return json(this.allowanceResult(account));
  }

  private async createBudgetHold(request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const periodKey = requireText(body.period_key, "period_key");
    const taskId = requireText(body.task_id, "task_id");
    const holdId = requireText(body.hold_id, "hold_id");
    const amount = requireMinorUnits(body.amount_minor, "amount_minor", true);
    const costBound = body.cost_bound_minor === undefined ? null : requireMinorUnits(body.cost_bound_minor, "cost_bound_minor", true);
    if (costBound === null) {
      throw new TenantLedgerError("COST_BOUND_REQUIRED", "Strict hard-budget calls require a credible integer cost_bound_minor.", 422);
    }
    if (costBound !== null && costBound !== amount) {
      throw new TenantLedgerError("HOLD_MUST_MATCH_COST_BOUND", "The held amount must equal the conservative cost bound.", 400);
    }
    const currency = requireCurrency(body.currency);
    const payload = { tenant_id: this.tenantId, period_key: periodKey, task_id: taskId, hold_id: holdId, amount_minor: amount, currency, cost_bound_minor: costBound };
    return this.runBudgetOperation(`budget-hold:${holdId}`, "tenant.budget.hold", taskId, payload, "tenant.budget.held", (sql) => {
      const period = this.requireBudgetPeriod(sql, periodKey);
      if (period.currency !== currency) throw new TenantLedgerError("BUDGET_CURRENCY_MISMATCH", "Hold currency does not match the billing period.", 409);
      const account = this.findAllowance(sql, periodKey, taskId);
      if (!account || Number(account.available_minor) < amount) {
        throw new TenantLedgerError("ALLOWANCE_INSUFFICIENT", "Task allowance is insufficient for this hold.", 402);
      }
      sql.exec("UPDATE task_allowances SET available_minor = ?, reserved_minor = ? WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
        Number(account.available_minor) - amount, Number(account.reserved_minor) + amount, this.tenantId, periodKey, taskId);
      sql.exec("UPDATE tenant_budget_periods SET reserved_minor = ?, updated_at = ? WHERE tenant_id = ? AND period_key = ?",
        Number(period.reserved_minor) + amount, Date.now(), this.tenantId, periodKey);
      sql.exec(
        `INSERT INTO budget_holds (hold_id, tenant_id, period_key, task_id, amount_minor, currency, price_version, status, created_at, unknown_reason, settlement_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        holdId, this.tenantId, periodKey, taskId, amount, currency, period.price_version, "HELD", Date.now(), null, null,
      );
      return { ...payload, price_version: period.price_version, status: "HELD" };
    });
  }

  private async markHoldUnknown(holdId: string, request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const reason = requireText(body.reason, "reason");
    const payload = { tenant_id: this.tenantId, hold_id: holdId, reason };
    return this.runBudgetOperation(`budget-hold-unknown:${holdId}`, "tenant.budget.hold.unknown", holdId, payload, "tenant.budget.hold_unknown", (sql) => {
      const hold = this.findHold(sql, holdId);
      if (!hold) throw new TenantLedgerError("BUDGET_HOLD_NOT_FOUND", "Budget hold was not found.", 404);
      if (hold.status === "SETTLED") throw new TenantLedgerError("BUDGET_HOLD_ALREADY_SETTLED", "Settled budget holds cannot become unknown.", 409);
      sql.exec("UPDATE budget_holds SET status = ?, unknown_reason = ? WHERE hold_id = ? AND tenant_id = ?", "UNKNOWN", reason, holdId, this.tenantId);
      return { ...this.holdResult({ ...hold, status: "UNKNOWN", unknown_reason: reason }), reserved_minor: Number(hold.amount_minor) };
    });
  }

  private async settleBudgetHold(holdId: string, request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const settlementId = requireText(body.settlement_id, "settlement_id");
    const actual = requireMinorUnits(body.actual_minor, "actual_minor", false);
    const currency = requireCurrency(body.currency);
    const usageReference = requireText(body.usage_reference, "usage_reference");
    const payload = { tenant_id: this.tenantId, hold_id: holdId, settlement_id: settlementId, actual_minor: actual, currency, usage_reference: usageReference };
    return this.runBudgetOperation(`budget-settlement:${settlementId}`, "tenant.budget.settle", holdId, payload, "tenant.budget.settled", (sql) => {
      const hold = this.findHold(sql, holdId);
      if (!hold) throw new TenantLedgerError("BUDGET_HOLD_NOT_FOUND", "Budget hold was not found.", 404);
      if (hold.status === "SETTLED") throw new TenantLedgerError("BUDGET_HOLD_ALREADY_SETTLED", "This hold already has a settlement.", 409);
      if (hold.currency !== currency) throw new TenantLedgerError("BUDGET_CURRENCY_MISMATCH", "Settlement currency does not match the hold.", 409);
      if (actual > Number(hold.amount_minor)) throw new TenantLedgerError("SETTLEMENT_EXCEEDS_HOLD", "Actual usage exceeds the reserved amount; obtain a separate hold before the provider call.", 409);
      const account = this.findAllowance(sql, hold.period_key, hold.task_id);
      const period = this.requireBudgetPeriod(sql, hold.period_key);
      if (!account) throw new TenantLedgerError("BUDGET_ACCOUNT_MISSING", "Task budget account is missing for the held operation.", 500);
      sql.exec("UPDATE task_allowances SET available_minor = ?, reserved_minor = ?, spent_minor = ? WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
        Number(account.available_minor) + Number(hold.amount_minor) - actual,
        Number(account.reserved_minor) - Number(hold.amount_minor), Number(account.spent_minor) + actual,
        this.tenantId, hold.period_key, hold.task_id);
      sql.exec("UPDATE tenant_budget_periods SET reserved_minor = ?, spent_minor = ?, updated_at = ? WHERE tenant_id = ? AND period_key = ?",
        Number(period.reserved_minor) - Number(hold.amount_minor), Number(period.spent_minor) + actual,
        Date.now(), this.tenantId, hold.period_key);
      sql.exec("UPDATE budget_holds SET status = ?, settlement_id = ? WHERE hold_id = ? AND tenant_id = ?", "SETTLED", settlementId, holdId, this.tenantId);
      sql.exec(
        `INSERT INTO budget_settlements (settlement_id, hold_id, tenant_id, period_key, task_id, amount_minor, currency, usage_reference, settled_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        settlementId, holdId, this.tenantId, hold.period_key, hold.task_id, actual, currency, usageReference, Date.now(),
      );
      return { ...payload, status: "SETTLED", released_minor: Number(hold.amount_minor) - actual, price_version: hold.price_version };
    });
  }

  private async transferAllowance(request: Request): Promise<Response> {
    const body = await readJsonRecord(request);
    this.assertBodyTenant(body.tenant_id);
    const periodKey = requireText(body.period_key, "period_key");
    const transferId = requireText(body.transfer_id, "transfer_id");
    const sourceTaskId = requireText(body.source_task_id, "source_task_id");
    const targetTaskId = requireText(body.target_task_id, "target_task_id");
    const amount = requireMinorUnits(body.amount_minor, "amount_minor", true);
    const currency = requireCurrency(body.currency);
    if (sourceTaskId === targetTaskId) throw new TenantLedgerError("INVALID_ALLOWANCE_TRANSFER", "Source and target tasks must differ.", 400);
    const payload = { tenant_id: this.tenantId, period_key: periodKey, transfer_id: transferId, source_task_id: sourceTaskId, target_task_id: targetTaskId, amount_minor: amount, currency };
    return this.runBudgetOperation(`allowance-transfer:${transferId}`, "tenant.budget.transfer", transferId, payload, "tenant.budget.transferred", (sql) => {
      const period = this.requireBudgetPeriod(sql, periodKey);
      if (period.currency !== currency) throw new TenantLedgerError("BUDGET_CURRENCY_MISMATCH", "Transfer currency does not match the billing period.", 409);
      const source = this.findAllowance(sql, periodKey, sourceTaskId);
      if (!source || Number(source.available_minor) < amount) {
        throw new TenantLedgerError("ALLOWANCE_INSUFFICIENT", "Source task has insufficient unreserved allowance.", 402);
      }
      const target = this.findAllowance(sql, periodKey, targetTaskId);
      sql.exec("UPDATE task_allowances SET available_minor = ? WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
        Number(source.available_minor) - amount, this.tenantId, periodKey, sourceTaskId);
      if (target) {
        sql.exec("UPDATE task_allowances SET available_minor = ? WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
          Number(target.available_minor) + amount, this.tenantId, periodKey, targetTaskId);
      } else {
        sql.exec(
          `INSERT INTO task_allowances (tenant_id, period_key, task_id, available_minor, reserved_minor, spent_minor)
           VALUES (?, ?, ?, ?, ?, ?)`, this.tenantId, periodKey, targetTaskId, amount, 0, 0,
        );
      }
      sql.exec(
        `INSERT INTO quota_transfers (transfer_id, tenant_id, period_key, source_task_id, target_task_id, amount_minor, currency, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        transferId, this.tenantId, periodKey, sourceTaskId, targetTaskId, amount, currency, Date.now(),
      );
      return { ...payload, status: "TRANSFERRED" };
    });
  }

  private listBudgetHolds(periodKey: string | null, status: string | null, requestedTenant: string | null): Response {
    this.assertTenant(requestedTenant);
    if (!periodKey) throw new TenantLedgerError("PERIOD_KEY_REQUIRED", "period_key is required.", 400);
    const normalizedStatus = status?.toUpperCase();
    const rows = normalizedStatus
      ? this.ctx.storage.sql.exec<BudgetHoldRow>(
          "SELECT * FROM budget_holds WHERE tenant_id = ? AND period_key = ? AND status = ? ORDER BY created_at LIMIT 1000",
          this.tenantId, periodKey, normalizedStatus,
        ).toArray()
      : this.ctx.storage.sql.exec<BudgetHoldRow>(
          "SELECT * FROM budget_holds WHERE tenant_id = ? AND period_key = ? ORDER BY created_at LIMIT 1000",
          this.tenantId, periodKey,
        ).toArray();
    return json({ tenant_id: this.tenantId, period_key: periodKey, holds: rows.map((row) => this.holdResult(row)) });
  }

  private async runBudgetOperation<T>(
    operationId: string,
    operationType: string,
    resourceId: string,
    payload: unknown,
    eventType: string,
    apply: (sql: PersistenceSqlStorage) => T,
  ): Promise<Response> {
    const result = await this.applyOperation(
      {
        operationId,
        operationType,
        resourceId,
        tenantId: this.tenantId,
        payload,
        aggregateType: "tenant-budget-operation",
        aggregateId: operationId,
        expectedVersion: 0,
      },
      apply,
      [{ eventId: `event:${operationId}`, eventType, tenantId: this.tenantId, payload }],
    );
    return json({ ...result.result, deduplicated: result.outcome === "REPLAYED" }, result.outcome === "REPLAYED" ? 200 : 201);
  }

  private requireBudgetPeriod(sql: PersistenceSqlStorage, periodKey: string): BudgetPeriodRow {
    const row = sql.exec<BudgetPeriodRow>(
      "SELECT * FROM tenant_budget_periods WHERE tenant_id = ? AND period_key = ?", this.tenantId, periodKey,
    ).toArray()[0];
    if (!row) throw new TenantLedgerError("BUDGET_PERIOD_NOT_FOUND", "Configure the tenant billing period before changing allowance.", 404);
    return row;
  }

  private findAllowance(sql: PersistenceSqlStorage, periodKey: string, taskId: string): TaskAllowanceRow | undefined {
    return sql.exec<TaskAllowanceRow>(
      "SELECT * FROM task_allowances WHERE tenant_id = ? AND period_key = ? AND task_id = ?",
      this.tenantId, periodKey, taskId,
    ).toArray()[0];
  }

  private findHold(sql: PersistenceSqlStorage, holdId: string): BudgetHoldRow | undefined {
    return sql.exec<BudgetHoldRow>(
      "SELECT * FROM budget_holds WHERE tenant_id = ? AND hold_id = ?", this.tenantId, holdId,
    ).toArray()[0];
  }

  private periodResult(row: BudgetPeriodRow): Record<string, unknown> {
    const available = Number(row.allocated_minor) - Number(row.reserved_minor) - Number(row.spent_minor);
    return {
      tenant_id: this.tenantId,
      period_key: row.period_key,
      currency: row.currency,
      hard_limit_minor: Number(row.hard_limit_minor),
      price_version: row.price_version,
      policy_version: Number(row.policy_version),
      allocated_minor: Number(row.allocated_minor),
      available_minor: available,
      reserved_minor: Number(row.reserved_minor),
      spent_minor: Number(row.spent_minor),
    };
  }

  private allowanceResult(row: TaskAllowanceRow): Record<string, unknown> {
    return {
      tenant_id: row.tenant_id,
      period_key: row.period_key,
      task_id: row.task_id,
      available_minor: Number(row.available_minor),
      reserved_minor: Number(row.reserved_minor),
      spent_minor: Number(row.spent_minor),
    };
  }

  private holdResult(row: BudgetHoldRow): Record<string, unknown> {
    return {
      hold_id: row.hold_id,
      tenant_id: row.tenant_id,
      period_key: row.period_key,
      task_id: row.task_id,
      amount_minor: Number(row.amount_minor),
      currency: row.currency,
      price_version: row.price_version,
      status: row.status,
      created_at: Number(row.created_at),
      unknown_reason: row.unknown_reason,
      settlement_id: row.settlement_id,
    };
  }

  private assertBodyTenant(bodyTenant: unknown): void {
    if (bodyTenant !== undefined) this.assertTenant(bodyTenant);
  }

  private getTask(taskId: string, requestedTenant: string | null): Response {
    this.assertTenant(requestedTenant);
    const row = this.ctx.storage.sql
      .exec<AdmissionRow>(
        "SELECT * FROM tenant_admissions WHERE tenant_id = ? AND task_id = ?",
        this.tenantId,
        taskId,
      )
      .toArray()[0];
    if (!row) return json({ error: "NotFound", task_id: taskId }, 404);
    return json({
      task_id: row.task_id,
      tenant_id: row.tenant_id,
      idempotency_key: row.idempotency_key,
      operation_id: row.operation_id,
      status: row.status,
      runnable_status: row.runnable_status,
      task_spec: JSON.parse(row.task_spec_json),
      accepted_at: Number(row.accepted_at),
    });
  }

  private listTasks(requestedTenant: string | null, limit: number): Response {
    this.assertTenant(requestedTenant);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) {
      throw new TenantLedgerError("TASK_LIST_LIMIT_INVALID", "limit must be an integer from 1 through 500.", 400);
    }
    const rows = this.ctx.storage.sql.exec<AdmissionRow>(
      "SELECT * FROM tenant_admissions WHERE tenant_id = ? ORDER BY accepted_at DESC LIMIT ?",
      this.tenantId,
      limit,
    ).toArray();
    return json({ tenant_id: this.tenantId, tasks: rows.map((row) => ({ task_id: row.task_id,
      tenant_id: row.tenant_id, status: row.status, runnable_status: row.runnable_status,
      accepted_at: Number(row.accepted_at), operation_id: row.operation_id })) });
  }

  private getAdmission(idempotencyKey: string, requestedTenant: string | null): Response {
    this.assertTenant(requestedTenant);
    const row = this.ctx.storage.sql
      .exec<DedupRow>(
        "SELECT task_id, request_digest, operation_id, expires_at, unresolved FROM request_dedup WHERE tenant_id = ? AND idempotency_key = ?",
        this.tenantId,
        idempotencyKey,
      )
      .toArray()[0];
    if (!row) {
      // A missing row is not proof that a timed-out write was absent.
      return json({ outcome: "UNKNOWN", idempotency_key: idempotencyKey }, 202);
    }
    return json({
      outcome: "ACCEPTED",
      task_id: row.task_id,
      idempotency_key: idempotencyKey,
      operation_id: row.operation_id,
      request_digest: row.request_digest,
      expires_at: Number(row.expires_at),
      unresolved: Boolean(row.unresolved),
    });
  }

  private assertTenant(requestedTenant: unknown): asserts requestedTenant is string {
    if (typeof requestedTenant !== "string" || requestedTenant.length === 0) {
      throw new TenantLedgerError("TENANT_REQUIRED", "tenant_id is required.", 400);
    }
    if (requestedTenant !== this.tenantId) {
      throw new TenantLedgerError("TENANT_SCOPE_MISMATCH", "Tenant identity does not match this ledger.", 404);
    }
  }

  private async applyOperation<T>(
    command: Parameters<typeof executeCellOperation<T>>[1],
    apply: (sql: PersistenceSqlStorage) => T,
    events: CellOutboxDraft[],
  ): Promise<CellOperationResult<T>> {
    try {
      return await executeCellOperation(this.ctx.storage as PersistenceObjectStorage, command, apply, events);
    } catch (error) {
      if (
        error instanceof TenantLedgerError ||
        error instanceof CellOperationConflictError ||
        (error instanceof Error && error.name === "CellVersionConflictError")
      ) {
        throw error;
      }
      if (error instanceof TypeError) {
        throw new TenantLedgerError("INVALID_OPERATION", error.message, 400);
      }
      const operationId = command.operationId;
      const message = error instanceof Error ? error.message : "durable confirmation did not complete";
      throw new TenantLedgerError("OUTCOME_UNKNOWN", `${message}; query or retry operation ${operationId}.`, 503);
    }
  }

  private errorResponse(error: unknown): Response {
    if (error instanceof TenantLedgerError) {
      return json({ error: error.code, message: error.message,
        ...(error.code === "OUTCOME_UNKNOWN" ? { outcome: "UNKNOWN" } : {}) }, error.status);
    }
    if (error instanceof TaskSpecValidationError) {
      return json({ error: "INVALID_TASK_SPEC", field: error.fieldPath, message: error.message }, 400);
    }
    if (error instanceof CellOperationConflictError) {
      return json({ error: "IDEMPOTENCY_CONFLICT", message: error.message }, 409);
    }
    if (error instanceof CellDurabilityUnavailableError) {
      return json({ error: "OUTCOME_UNKNOWN", outcome: "UNKNOWN", message: error.message }, 503);
    }
    return json({ error: "TENANT_LEDGER_FAILURE", message: "Tenant ledger operation failed." }, 500);
  }
}

function requireText(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TenantLedgerError("INVALID_TASK_SPEC", `${field} must be a non-empty string.`, 400);
  }
  return value;
}

async function readJsonRecord(request: Request): Promise<Record<string, any>> {
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    throw new TenantLedgerError("INVALID_JSON", "Request body must be valid JSON.", 400);
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TenantLedgerError("INVALID_REQUEST", "Request body must be a JSON object.", 400);
  }
  return value as Record<string, any>;
}

function requireMinorUnits(value: unknown, field: string, requirePositive: boolean): number {
  if (!Number.isSafeInteger(value) || (requirePositive ? Number(value) <= 0 : Number(value) < 0)) {
    throw new TenantLedgerError(
      "INVALID_MONEY_AMOUNT",
      `${field} must be a ${requirePositive ? "positive" : "non-negative"} safe integer in minor currency units.`,
      400,
    );
  }
  return Number(value);
}

function requireCurrency(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Z]{3}$/.test(value)) {
    throw new TenantLedgerError("INVALID_CURRENCY", "currency must be a three-letter uppercase code.", 400);
  }
  return value;
}

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}
