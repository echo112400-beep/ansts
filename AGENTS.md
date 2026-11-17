# Repository Guidelines

## Project Structure & Module Organization

- `src/cloudflare/` contains celld/Workers entry points, Scheduler cells, and durable control objects.
- `src/runtime/` contains native Body daemons, sandbox execution, and recovery journals; `src/control/` and `src/finops/` contain administration and quota contracts.
- `ui/console/` is the Next.js console; `ui/src/` contains additional UI and hrui components.
- `tests/` contains TypeScript suites, with focused control-plane tests under `tests/control/`.
- `scripts/` contains verification and deployment utilities; `deploy/` contains Compose and celld configuration; `proto/` contains RPC definitions.
- `docs/admin-platform-plan/` holds specifications and task status; `docs/validation/` holds acceptance evidence. `dist/` contains generated JavaScript.

## Build, Test, and Development Commands

- `npm ci`: install root dependencies.
- `npm run lint`: run the repository’s custom static checks.
- `npm run build && npm test`: compile CommonJS output, then run compiled suites with Node’s test runner. Rebuild first to avoid stale tests.
- `npm run test:control`: run strict control-plane type checking and focused tests.
- `npm run typecheck`: run the broader TypeScript check; distinguish existing failures from regressions.
- `npm --prefix ui/console ci`, then `npm --prefix ui/console run dev`: install and start the console. Its `typecheck` and `build` scripts validate frontend changes.
- `python3 scripts/restart_dev_cluster.py --check-only`: validate development deployment configuration before restarting services.

## Coding Style & Naming Conventions

Use TypeScript, match surrounding indentation (commonly two spaces), and retain `.js` extensions in relative source imports. Use PascalCase for classes/interfaces and camelCase for functions/variables; follow nearby underscore-based filenames. Edit source rather than generated `dist/` files. Use the custom lint script; preserve local formatting conventions.

## Testing Guidelines

Name suites `*.test.ts`. Cover authorization boundaries, idempotent replay, version conflicts, transaction rollback, and restart recovery when changing control or execution code. Keep integration fixtures isolated from development tasks. Record verification scope accurately; successful transpilation does not establish type safety.

## Commit & Pull Request Guidelines

History uses scoped subjects such as `feat(m97): …` and `feat(console-m95): …`. Follow that structure with a relevant type and scope. PRs should describe behavior, link the issue or plan, list validation, and include screenshots for UI changes. Explain storage migrations and deployment effects.

## Security & Agent Workflow

Keep credentials in ignored `.dev-secrets/` files; exclude values from logs and commits. Preserve persistent volumes and task identities during deployment. For structural code exploration, use the installed `codebase-memory` skill and verify cited-path coverage.

@/home/tiktok/.codex/RTK.md

## Agent skills

### Issue tracker

Use GitHub Issues in `echo112400-beep/ansts`. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the existing five canonical triage labels. See `docs/agents/triage-labels.md`.

### Domain docs

Use the single-context root `GLOSSARY.md` and `docs/adr/` layout. See `docs/agents/domain.md`.
