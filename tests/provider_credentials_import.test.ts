import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawnSync } from "node:child_process";

it("imports the web OAuth client ID and its matching secret when both GitHub credential pairs exist", context => {
  if (spawnSync("python3", ["--version"]).status !== 0) { context.skip("python3 required"); return; }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "provider-import-"));
  try {
    const fixture = path.join(directory, "fixture.env"), output = path.join(directory, "secrets");
    fs.writeFileSync(fixture, 'TINYAGENT_GITHUB_ID=Iv1.main\nTINYAGENT_GITHUB_SECRET=main-secret\nTINYAGENT_VUE_GITHUB_ID=Ov23.web\nTINYAGENT_VUE_GITHUB_SECRET=web-secret\nDEEPSEEK_API_KEY=model-key\n');
    const result = spawnSync("python3", [path.resolve("scripts/import_provider_credentials.py"), "--env-file", fixture, "--github-user-id", "123", "--output-dir", output], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    assert.match(fs.readFileSync(path.join(output, "providers.env"), "utf8"), /^ANSTS_GITHUB_CLIENT_ID=Ov23\.web$/m);
    assert.equal(fs.readFileSync(path.join(output, "github-client-secret"), "utf8"), "web-secret");
    assert.equal(fs.statSync(path.join(output, "github-client-secret")).mode & 0o777, 0o600);
    assert.ok(!result.stdout.includes("web-secret"));
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
