# ANSTS v2.2 — Agent-Native Sandbox Task Scheduler (`denoland/celld` + Cloudflare Workers + RustFS)

Production-grade TypeScript implementation of **ANSTS v2.2** across Milestones **M1 – M15 (`TASK-01 ~ TASK-74`)** following Google TypeScript Engineering Practices.

## Quick Start & Zero-Config CI Verification
```bash
npm run lint    # Stage 1: Google TypeScript Static & AST Guard
npm run build   # Stage 2: TypeScript Compilation (52 src + 14 test suites)
npm test        # Stage 3: Execute all 14 suites (55/55 unit & E2E tests)
npm run ci      # Run full 3-stage CI pipeline
```
