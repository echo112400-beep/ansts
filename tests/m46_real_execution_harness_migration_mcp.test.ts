/**
 * @fileoverview M46 End-to-End Verification: Real OS Subprocess CoW Git Bisect,
 * Full 5-Stage AgentHarness Turn Execution (THINK->ACT Cgroup + CognitiveRouter + VelocityBreaker + Real Bash LayerFS),
 * Real Zlib-Compressed LayerFS Bundle Live Migration Across Body Daemons over HTTP,
 * Dynamic WAL/Facet-Derived Epoch GC Audit & Dynamic MCP Tool Registry + Saga LIFO Rollback.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M46 Deep Real-Execution Hardening & Zero-Stubbing Elimination Suite", () => {
  it("executes real OS subprocess CoW bisect, full AgentHarness + real bash turn stepping, cross-Body HTTP LayerFS bundle migration with zlib/SHA-256, dynamic Epoch GC audit, and MCP/Saga LIFO rollback", async () => {
    const tmpDir = `/tmp/ansts-m46-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    // Spin up a real target Body daemon (body-host-03) and a Gateway node
    const body3 = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "body-host-03",
      nodeRole: "body",
      storageDir: `${tmpDir}/body-03`,
    });
    const { apiPort: b3Port } = await body3.start(0, 0);

    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: `${tmpDir}/gw-01`,
      peerBodyUrls: {
        "body-host-03": `http://127.0.0.1:${b3Port}`,
      },
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. [M46-1] Real OS Subprocess CoW Git Bisect (`executeLiveLayerFsBisect`)
      const bisectResp = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "run_cow_bisect", actor: "chen", role: "SRE_ADMIN" }),
      });
      assert.equal(bisectResp.status, 200);
      const bisectData = (await bisectResp.json()) as {
        firstBadCommit: string;
        probesExecuted: number;
        probedCommits: Array<{ commitId: string; passed: boolean; branchName: string }>;
      };
      assert.ok(bisectData.firstBadCommit);
      assert.ok(bisectData.probesExecuted >= 2);
      assert.equal(bisectData.probedCommits.length, bisectData.probesExecuted);
      // Verify at least one probe failed on the regression bug commit via real `grep -q 'STATUS=OK'`
      assert.ok(bisectData.probedCommits.some((p) => p.passed === false));

      // 2. [M46-1] Full 5-Stage AgentHarness Turn Step with Real Linux Bash Execution (`executeRealBash: true`)
      const stepResp = await fetch(`${baseUrl}/v1beta1/management/tasks/step`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-ns-8841",
          toolName: "shell_exec",
          shellCommand: "printf 'export const realBashVerified = 46;\\n' > src/m46_real.ts",
          executeRealBash: true,
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(stepResp.status, 200);
      const stepData = (await stepResp.json()) as {
        turnIndex: number;
        crc32Hex: string;
        commitId: string;
        harnessExecution: { endpointId: string; turnIndex: number };
        cgroupTransition: { thinkQuotaUs: number; actQuotaUs: number };
      };
      assert.equal(stepData.turnIndex, 6);
      assert.equal(stepData.harnessExecution.endpointId, "ep-qwen-38");
      assert.equal(stepData.cgroupTransition.thinkQuotaUs, 25000);
      assert.equal(stepData.cgroupTransition.actQuotaUs, 400000);

      // Verify the file written by real bash in the materialized workspace exists in LayerFS blame
      const blameResp = await fetch(
        `${baseUrl}/v1beta1/management/layerfs/blame?taskId=task-ns-8841&filePath=src/m46_real.ts`
      );
      assert.equal(blameResp.status, 200);
      const blameData = (await blameResp.json()) as { lines: Array<{ content: string }> };
      assert.ok(blameData.lines.some((l) => l.content.includes("realBashVerified = 46")));

      // 3. [M46-2] Real Zlib-Compressed LayerFS Bundle Live Migration + Cross-Body HTTP Import (`body-host-02 -> body-host-03`)
      const migResp = await fetch(`${baseUrl}/v1beta1/management/tasks/migrate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-ns-8841",
          targetNodeId: "body-host-03",
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(migResp.status, 200);
      const migData = (await migResp.json()) as {
        sourceNodeId: string;
        targetNodeId: string;
        newFencingToken: number;
        targetRestored: boolean;
        digestVerified: boolean;
        sha256Digest: string;
        logicalByteLength: number;
        compressedByteLength: number;
        remoteBodyImported: boolean;
      };
      assert.equal(migData.sourceNodeId, "body-host-02");
      assert.equal(migData.targetNodeId, "body-host-03");
      assert.equal(migData.newFencingToken, 13);
      assert.equal(migData.targetRestored, true);
      assert.equal(migData.digestVerified, true);
      assert.equal(migData.sha256Digest.length, 64);
      assert.ok(migData.compressedByteLength > 0 && migData.compressedByteLength < migData.logicalByteLength);
      assert.equal(migData.remoteBodyImported, true);

      // Verify body3 genuinely imported the migrated `feat-ns-auth` branch including `src/m46_real.ts`!
      const body3Head = body3.layerfs.getBranchHeadCommit("feat-ns-auth");
      assert.ok(body3Head.filesTree.has("src/m46_real.ts"));

      // 4. [M46-3] Dynamic WAL/Facet-Derived `celld` Epoch GC Audit & Dynamic MCP Tool Registry + Saga LIFO Rollback
      // Bump fence once more so epoch 12 is strictly < currentFence - 1 (12 < 14 - 1) and appears in dynamic Epoch GC audit
      await fetch(`${baseUrl}/v1beta1/management/tasks/migrate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-ns-8841",
          targetNodeId: "body-host-02",
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });

      const epochAudit = await (
        await fetch(`${baseUrl}/v1beta1/management/storage/gc-sweep`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "epoch_gc_audit", graceSecs: 3600, actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.ok(epochAudit.deletableScopes.includes("TaskCell:task-ns-8841@epoch-12"));

      // Register, invoke, and LIFO rollback a dynamic MCP tool via `/v1beta1/management/mcp` & `anstsctl`
      const regMcp = await (
        await fetch(`${baseUrl}/v1beta1/management/mcp`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            action: "register_mcp_tool",
            serverName: "mcp-redis-cluster",
            toolName: "mcp_redis_flush_slot",
            description: "Flush Redis hash slot with reversible snapshot",
            actor: "chen",
            role: "SRE_ADMIN",
          }),
        })
      ).json();
      assert.equal(regMcp.toolName, "mcp_redis_flush_slot");

      const invCli = await runAnstsCtlRemote(
        ["mgmt-mcp", "invoke_mcp_tool", "mcp_redis_flush_slot", "task-ns-8841"],
        baseUrl
      );
      assert.equal(invCli.exitCode, 0);

      const rbCli = await runAnstsCtlRemote(
        ["mgmt-mcp", "saga_rollback_task", "mcp_redis_flush_slot", "task-ns-8841"],
        baseUrl
      );
      assert.equal(rbCli.exitCode, 0);
      const rbData = JSON.parse(rbCli.stdout) as { compensatedCount: number };
      assert.ok(rbData.compensatedCount >= 1);
    } finally {
      await server.stop();
      await body3.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
