# Complete application admission recovery — 2026-11-01

Result: **PASS for the admission-only scenario**, not full application disaster recovery.

- Runtime: Celld 0.6.3, `CELLD_DURABILITY=bucket`, RustFS 1.0.4.
- Application: archive of integration commit `2454d3e`; deployed version `8bfe680bfeb4f07f` with all eight SQLite Cell classes and the normal Worker entry point.
- Isolation: new Compose project `celld-acceptance-1731f040`, bucket `ansts-celld-acceptance-1731f040`, S3/console/Worker ports 29000/29001/29876. Existing services and buckets were untouched.
- State and executable test controllers: `/tmp/ansts-full-app-smoke-20261101`. Only generated local test credentials were used; repository `.env` was not read. Local HTTP requests explicitly disabled proxy routing.

The Worker returned **202** from `POST /v1beta1/tasks` for a complete TaskSpec. Public task and admission-by-key queries then returned the stored normalized TaskSpec and original acceptance identity. The controller saved the complete task response as its comparison baseline.

The recovery controller started an owned Celld process, verified that baseline through the public Worker API, sent SIGKILL to its exact subprocess handle, and verified exit code **-9**. With that process stopped, it deleted only the marked compute directory, leaving the bucket volume intact. A fresh Celld node restored a task response exactly equal to the saved baseline, including the complete TaskSpec. Admission-by-key still returned `ACCEPTED`; retrying the original submission returned **200**, `deduplicated=true`, and the original task ID.

Evidence: `control/app-admission-recovery-report.json`, `control/task-before.json`, `control/admission.json`, and the per-node logs under the external state directory. `verify-recovery.py` contains the exact process control and public HTTP assertions; `deploy-app.py` stages runtime commands with the generated local configuration. These are external acceptance artifacts, not a new supported deployment command.

Both owned Celld processes were stopped and the newly created RustFS service was stopped after verification; its bucket volume was retained. Compute and storage still share one physical host. No model call, budget settlement, external operation, committed snapshot, independent backup, migration, or capacity/RTO measurement was tested in this scenario. Those remain #10 acceptance work.
