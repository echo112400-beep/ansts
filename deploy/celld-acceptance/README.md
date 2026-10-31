# Celld and bucket acceptance harness

This isolated harness checks a real Celld node against a local RustFS S3 API. It deploys the small Worker in `fixture/`, writes to a SQLite-backed Durable Object, explicitly awaits `storage.sync()`, and checks the value after process restarts and deletion of Celld's compute working directory.

The fixture is a runtime acceptance probe. It does not exercise the ANSTS Worker, D1, KV, R2, queues, schedulers, task flows, or microservice recovery. Full application-level DR acceptance depends on issue #10.

## Requirements

- Celld 0.6.3 or later on `PATH`.
- Docker with Docker Compose v2.
- A Bash shell on Linux or WSL, plus `curl`, `sha256sum`, `realpath`, `od`, `awk`, `ps`, `stat`, `cmp`, `seq`, `env`, and standard coreutils. `celld deploy` also needs `esbuild`; the script checks `CELLD_ESBUILD`, the repository's `node_modules/.bin/esbuild`, then `PATH`.
- Local access to the pinned images `rustfs/rustfs:1.0.4` and `amazon/aws-cli:2.39.3`.

No production credentials are needed. `init` generates isolated local test credentials under the chosen state directory with file mode `0600`. Compose receives only that generated `control/compose.env`; the script never reads `.env`, never invokes `celld dev`, and runs `celld deploy` from this harness directory.

If esbuild is installed outside the current checkout, pass its executable explicitly, for example `CELLD_ESBUILD=/path/to/esbuild runtime.sh deploy`.

## Run the full check

Choose a new absolute directory outside the repository. The three optional port arguments are S3, RustFS console, and Worker HTTP ports; defaults are `19000`, `19001`, and `19876`.

```sh
deploy/celld-acceptance/runtime.sh init /tmp/ansts-celld-acceptance
export CELLD_ACCEPTANCE_STATE_DIR=/tmp/ansts-celld-acceptance
deploy/celld-acceptance/runtime.sh verify
```

`verify` starts RustFS, creates a uniquely named bucket, deploys the fixture, and starts a single-node Celld process using `CELLD_DURABILITY=bucket`. It then:

1. Uploads a small owned object with the AWS S3 API, requests `Range: bytes=6-15`, and compares the downloaded body byte-for-byte with `0123456789`; then it removes that test object.
2. Writes through the fixture's public HTTP endpoint. The Durable Object finishes the SQL write cursor, awaits `storage.sync()`, and returns the value.
3. Gracefully stops and restarts Celld, then reads the value.
4. Stops Celld, deletes only the marked state's `compute/` directory, starts a fresh process, and reads the value from the surviving bucket.
5. Stops RustFS briefly and attempts another write. A 2xx response before RustFS is restored fails the check; an error or an unanswered request is acceptable. It restores RustFS, retries the write, and reads the value.
6. Runs `celld diagnose` against the same bucket with its writable conditional-write probe. If the just-stopped single-node processes leave unexpired local leases, it retries only direct peer-reachability failures for up to 24 seconds while those leases expire; other diagnostic failures stop the run. All attempts remain in the private state directory.

The result and diagnostic output are kept in `control/acceptance-report.md`, `control/diagnose.jsonl`, and `control/celld.log` under the state directory. Test credentials and all runtime files stay there too.

## Individual controls

After `init`, export `CELLD_ACCEPTANCE_STATE_DIR` as above. The controls are deliberately separate so restart and loss scenarios can be inspected step by step.

```sh
deploy/celld-acceptance/runtime.sh bucket-up
deploy/celld-acceptance/runtime.sh deploy
deploy/celld-acceptance/runtime.sh node-start
deploy/celld-acceptance/runtime.sh node-stop
deploy/celld-acceptance/runtime.sh node-crash
deploy/celld-acceptance/runtime.sh compute-discard
deploy/celld-acceptance/runtime.sh simulate-compute-loss
deploy/celld-acceptance/runtime.sh diagnose
deploy/celld-acceptance/runtime.sh bucket-down
```

`compute-discard` refuses to run while the marked Celld PID is alive, checks that the compute path is not a symlink, and removes only `$CELLD_ACCEPTANCE_STATE_DIR/compute`. `node-crash` intentionally sends SIGKILL to the Celld process named in this harness's PID file. `bucket-down` stops only the RustFS container. The harness does not provide a command to remove the named volumes, and it never runs `docker compose down -v`; the local bucket remains available across compute loss and harness process restarts. To remove the disposable bucket manually after review, inspect the project name and volumes with `docker volume ls` and remove only the volumes prefixed `celld-acceptance-<state-id>_`.

## What this can establish

- The current Celld binary can build and serve this minimal SQLite Durable Object from a bucket-backed deployment.
- For this local single-node setup, the explicitly awaited `storage.sync()` and successful write response are followed by readable state after process restart and deletion of the local compute directory.
- A write made while the local object store is stopped does not receive a 2xx response during the observation window; the retry succeeds after RustFS comes back.
- Celld's writable conditional-write diagnostic passes for the local RustFS configuration, when the harness reports success.

The compute directory and the Docker named volume share one physical host and may share its disk, power, kernel, and operator domain. This demonstrates that the bucket volume is distinct from Celld's removable work directory; it does not demonstrate independent hardware, rack, region, or operator failure domains. Local RustFS passing the Celld diagnostic is not production object-store qualification. The intended production bucket still needs the documented conditional create/overwrite, read-after-write, and exact ranged-read checks in its real environment.

## Executed evidence from this workspace

On 2026-10-31, the harness passed against Celld 0.6.3 and `rustfs/rustfs:1.0.4` using `CELLD_DURABILITY=bucket`. The run verified an S3 `put-object` followed by `get-object --range bytes=6-15` returning exactly `0123456789`; a SQLite Durable Object write with an awaited `storage.sync()`; reads after graceful process restart and deletion of Celld's compute directory; and a write against stopped RustFS that returned HTTP 500, followed by a successful retry and read after the bucket returned. `celld diagnose` then passed its writable conditional-write checks after one retry for an unexpired peer lease left by the single-node restart sequence. The credential/output self-test passed as well.

The detailed per-run report, diagnostic attempts, and node log are in the external state directory's mode-`0600` control files. This evidence is for the minimal harness fixture and local RustFS only. It does not establish independent physical failure domains or full ANSTS application-level DR; that application-level acceptance depends on issue #10.
