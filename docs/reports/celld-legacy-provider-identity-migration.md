# Issue #5 acceptance continuation

Branch: `impl/issue-5-acceptance`; owned implementation commit `711ab21`; merged integration through `2454d3e` in `176a676`.
Owned files: `src/router/provider_call.ts`, `src/router/cell_authorities.ts`, `tests/provider_call_identity.test.ts`.

## Reproduced and fixed

- Provider idempotency formerly used task-local `callId`. A controlled provider that caches by its idempotency header returned another tenant's prior result. The provider identity now hashes the canonical tenant/task/call tuple; logical retries remain stable.
- TenantLedger settlement/release IDs formerly used only task-local call IDs. Two tasks sharing a tenant could collide on settlement operations. Settlement/release identities now use the tenant/task/call-scoped hold identity.
- Raw colon concatenation aliased provider quota request IDs for separator-containing tenant/task components. Components are URI-encoded before joining.
- Existing allocated billing periods already reject pricing rewrites. Verified via HTTP and verified matching retries preserve original price/charge.
- Legacy recorded quota IDs that differ from the canonical encoded identity now require explicit reconciliation before a new grant/attempt can be created. A started legacy grant is never silently re-issued.
- When a hold is already settled under an older settlement ID, the budget HTTP client queries authoritative settled holds in the requested tenant/period, reuses only the matching recorded settlement ID, and resubmits the original amount/currency/usage. TenantLedger's immutable operation replay verifies those details. Mismatched evidence rejects rather than inventing a charge/outcome.

## Upgrade and migration caveats for #10

- Do not bulk rewrite old raw-concatenated quota/request identities. They are ambiguous when components contain separators; list them in the unresolved migration manifest and reconcile with recorded provider/ledger evidence. The new guard returns `PROVIDER_CALL_IDENTITY_RECONCILIATION_REQUIRED` before a provider call when a differing legacy grant exists.
- Legacy old routing pins without a quota grant have not sent a provider request yet, but a migration should retain/reconcile their original route/price pin before admitting new work under canonical identities. This patch does not claim a full legacy identity migration.
- Recorded settled hold identity recovery is deliberately conservative. The existing TenantLedger settled-hold list is bounded to 1000 rows; if the exact hold/settlement identity cannot be found/verified, the client returns `BUDGET_SETTLEMENT_RECONCILIATION_REQUIRED`. A direct per-hold lookup or paginated migration inventory is a future API improvement; absence from a bounded list is never treated as proof no settlement happened.
- No unknown provider request is retried solely because an identity format changed. No old usage evidence or already lost state is synthesized.

## Verification

6 dedicated HTTP/provider identity tests pass. Full build/lint and 123/123 integration/chaos tests pass, zero skipped, after merging the native root-storage transaction API fix and #6 acceptance fixes. `git diff --check` passes. Runtime/bucket DR remains #10; these tests use owner HTTP clients and controlled provider bindings.

Worktree has only an untracked `node_modules` dependency symlink after commit; do not commit it, and unlink only the symlink before eventual worktree cleanup.
