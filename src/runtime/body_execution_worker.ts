/** @fileoverview Disposable native execution worker; parent owns commit publication and cancellation. */
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";

export interface BodyWorkerInput {
  bundle: ReturnType<LayerFsWorkspaceDriver["exportBranchBundle"]>;
  branchName: string;
  tenantId: string;
  stepId: string;
  prompt: string;
  delayMs?: number;
}
process.once("message", async (input: BodyWorkerInput) => {
  try {
    if (input.delayMs) await new Promise(resolve => setTimeout(resolve, input.delayMs));
    const driver = new LayerFsWorkspaceDriver();
    driver.importBranchBundle(input.bundle);
    // Encode the trace as data, so shell substitutions in user prompts cannot execute.
    const encoded = Buffer.from(input.prompt + "\n").toString("base64");
    const result = driver.executeEphemeralBashWorkspace({
      branchName: input.branchName, agentId: input.tenantId, stepId: input.stepId,
      command: `printf '%s' '${encoded}' | base64 -d > .ansts_task_trace.txt`,
    });
    process.send?.({ result, bundle: driver.exportBranchBundle(input.branchName) }, () => process.exit(0));
  } catch (error) { process.send?.({ error: String(error) }, () => process.exit(1)); }
});
process.once("disconnect", () => {
  if (process.platform !== "win32") { try { process.kill(-process.pid, "SIGKILL"); } catch {} }
  process.exit(1);
});
