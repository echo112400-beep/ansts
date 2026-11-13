/** @fileoverview Cancellable process-group execution, with completion only after the worker exits. */
import { fork } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BodyWorkerInput } from "./body_execution_worker.js";
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";

export interface BodyWorkerOutput {
  result: ReturnType<LayerFsWorkspaceDriver["executeEphemeralBashWorkspace"]>;
  bundle: ReturnType<LayerFsWorkspaceDriver["exportBranchBundle"]>;
}
export function runBodyExecution(input: BodyWorkerInput, signal: AbortSignal): Promise<BodyWorkerOutput> {
  if (signal.aborted) return Promise.reject(new Error("BODY_EXECUTION_CANCELLED"));
  return new Promise((resolve, reject) => {
    const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-body-worker-"));
    const child = fork(require.resolve("./body_execution_worker.js"), [], { detached: process.platform !== "win32", stdio: ["ignore", "ignore", "ignore", "ipc"], execArgv: [], env: { ...process.env, TMPDIR: temporary, TEMP: temporary, TMP: temporary } });
    let output: BodyWorkerOutput | undefined;
    let failure: Error | undefined;
    const abort = () => {
      failure = new Error("BODY_EXECUTION_CANCELLED");
      try {
        if (process.platform !== "win32" && child.pid) process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") failure = new Error(`BODY_STOP_UNCONFIRMED: ${String(error)}`); }
    };
    signal.addEventListener("abort", abort, { once: true });
    child.on("message", (message: BodyWorkerOutput & { error?: string }) => {
      if (message.error) failure = new Error(message.error);
      else output = message;
    });
    child.on("error", error => { failure = error; });
    child.once("close", code => {
      signal.removeEventListener("abort", abort);
      fs.rmSync(temporary, { recursive: true, force: true });
      if (!failure && code === 0 && output && !signal.aborted) resolve(output);
      else reject(failure ?? new Error(`BODY_WORKER_EXIT: ${code}`));
    });
    child.send(input, error => { if (error) { failure = error; abort(); } });
    if (signal.aborted) abort();
  });
}
