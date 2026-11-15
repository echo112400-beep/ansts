/** Read-only browser acceptance against the deployed topology.
 * Requires PLAYWRIGHT_MODULE, CHROME_PATH, ANSTS_SYSTEM_TOKEN_FILE (private SRE JWT).
 * Optional ANSTS_SYSTEM_VIEWER_TOKEN_FILE verifies the restricted view.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
for (const name of [
  "PLAYWRIGHT_MODULE",
  "CHROME_PATH",
  "ANSTS_SYSTEM_TOKEN_FILE",
])
  if (!process.env[name]) throw new Error(`Missing ${name}`);
const { chromium } = await import(
  pathToFileURL(process.env.PLAYWRIGHT_MODULE).href
);
const origin = process.env.ANSTS_CONSOLE_URL ?? "http://127.0.0.1:8088";
const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH,
  headless: true,
  args: ["--no-sandbox", "--no-proxy-server"],
});
const errors = [],
  checks = [];
async function contextFor(file) {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
  });
  await context.addCookies([
    {
      name: "ansts_dev_session",
      value: readFileSync(file, "utf8")
        .trim()
        .replace(/^Bearer /, ""),
      domain: new URL(origin).hostname,
      path: "/",
      httpOnly: true,
      sameSite: "Strict",
    },
  ]);
  return context;
}
try {
  const context = await contextFor(process.env.ANSTS_SYSTEM_TOKEN_FILE),
    page = await context.newPage();
  page.on("pageerror", (e) => errors.push(e.message));
  let requests = 0;
  page.on("request", (r) => {
    if (new URL(r.url()).pathname === "/v1/console/system") requests++;
  });
  const responsePromise = page.waitForResponse(
    (r) => new URL(r.url()).pathname === "/v1/console/system",
  );
  await page.goto(origin + "/system");
  const response = await responsePromise;
  assert.equal(response.status(), 200);
  const data = await response.json();
  await page.locator(".system-summary").waitFor();
  assert.equal(
    await page.locator(".system-map .system-node").count(),
    data.nodes.length,
  );
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
    false,
  );
  checks.push(
    "authenticated-deep-link",
    "all-configured-nodes",
    "desktop-no-overflow",
  );
  await page
    .getByRole("button", { name: "查看 Prometheus", exact: true })
    .click();
  const panel = page.locator(".system-inspector");
  await panel
    .getByRole("heading", { name: "Prometheus", exact: true })
    .waitFor();
  await panel.locator(".inspector-targets").waitFor();
  assert.equal(
    await panel.locator(".inspector-targets>div").count(),
    data.nodes.find((n) => n.kind === "prometheus").targets.length,
  );
  assert.ok((await page.locator(".system-dependency-lines>path").count()) > 0);
  checks.push("scrape-evidence-and-directed-dependencies");
  await page
    .getByRole("button", { name: "查看 body-host-01", exact: true })
    .click();
  await panel.getByText("可分配 CPU", { exact: true }).waitFor();
  await panel.getByText("预留内存", { exact: true }).waitFor();
  await page
    .getByRole("button", { name: "查看 celld-node-01", exact: true })
    .click();
  await panel.getByText("内存工作集", { exact: true }).waitFor();
  checks.push("executor-and-fleet-telemetry");
  await page.getByRole("button", { name: "执行主链", exact: true }).click();
  assert.equal(
    await page
      .getByRole("button", { name: "查看 Prometheus", exact: true })
      .count(),
    0,
  );
  await page.getByRole("button", { name: "全景拓扑", exact: true }).click();
  assert.equal(
    await page.locator(".system-map .system-node").count(),
    data.nodes.length,
  );
  checks.push("topology-view-switch");
  await page
    .getByRole("textbox", { name: "搜索系统组件" })
    .fill("no-match-component");
  await page.getByText("没有匹配的组件", { exact: true }).waitFor();
  await page.getByRole("button", { name: "清除筛选", exact: true }).click();
  await page.getByRole("button", { name: "仅看异常", exact: true }).click();
  assert.equal(
    await page.locator(".system-inventory tbody tr").count(),
    data.nodes.filter((n) => ["degraded", "unavailable"].includes(n.status))
      .length,
  );
  await page.getByRole("button", { name: "仅看异常", exact: false }).click();
  checks.push("inventory-search-and-issue-filter");
  await page.getByRole("button", { name: "暂停自动刷新", exact: true }).click();
  await page.getByText("自动刷新已暂停", { exact: true }).waitFor();
  await page.waitForTimeout(500);
  const beforePause = requests;
  await page.waitForTimeout(15500);
  assert.equal(requests, beforePause);
  await page.getByRole("button", { name: "刷新状态", exact: true }).click();
  await page.getByRole("button", { name: "刷新状态", exact: true }).waitFor();
  assert.ok(requests > beforePause);
  checks.push("pause-stops-polling-manual-refresh-works");
  const updated = await page.locator(".system-updated").innerText();
  await page.route("**/v1/console/system", (r) =>
    r.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ error: "TEST_UNAVAILABLE" }),
    }),
  );
  await page.getByRole("button", { name: "刷新状态", exact: true }).click();
  await page
    .getByRole("alert")
    .filter({ hasText: "正在显示上次成功快照" })
    .waitFor();
  assert.equal(
    await page.locator(".system-map .system-node").count(),
    data.nodes.length,
  );
  assert.equal(await page.locator(".system-updated").innerText(), updated);
  await page.unroute("**/v1/console/system");
  await page.getByRole("button", { name: "重试", exact: true }).click();
  await page.locator(".system-page .banner.error").waitFor({ state: "hidden" });
  checks.push("failed-refresh-retains-timestamp-and-data", "retry-recovers");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForTimeout(350);
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
    false,
  );
  await page.getByRole("button", { name: "查看异常", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "节点详情", exact: true });
  await dialog.waitFor();
  await dialog
    .getByRole("heading", { name: "Prometheus", exact: true })
    .waitFor();
  await dialog
    .locator(".inspector-relations")
    .getByRole("button")
    .filter({ hasText: "Gateway 01" })
    .click();
  await dialog
    .getByRole("heading", { name: "Gateway 01", exact: true })
    .waitFor();
  assert.equal(await dialog.evaluate((el) => el.scrollTop), 0);
  const heading = await dialog
    .getByRole("heading", { name: "Gateway 01", exact: true })
    .boundingBox();
  assert.ok(heading.y >= 0 && heading.y < 300);
  await dialog.getByRole("button", { name: "关闭节点详情" }).click();
  checks.push(
    "mobile-overflow",
    "mobile-exception-drilldown",
    "dependency-switch-resets-scroll-and-shows-title",
  );
  await page
    .getByRole("button", { name: "查看 body-host-01", exact: true })
    .click();
  await dialog
    .getByRole("heading", { name: "body-host-01", exact: true })
    .waitFor();
  await dialog.locator(".inspector-relations").scrollIntoViewIfNeeded();
  const close = await dialog
    .getByRole("button", { name: "关闭节点详情" })
    .boundingBox();
  assert.ok(close.y >= 0 && close.y < 100);
  await page.keyboard.press("Escape");
  await dialog.waitFor({ state: "hidden" });
  checks.push("sticky-close-and-Escape");
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.getByRole("button", { name: "总览", exact: true }).click();
  await page.getByRole("heading", { name: /工作区总览/ }).waitFor();
  assert.equal(new URL(page.url()).pathname, "/");
  await page.getByRole("button", { name: "系统状态", exact: true }).click();
  await page.locator(".system-summary").waitFor();
  assert.equal(new URL(page.url()).pathname, "/system");
  await page.goBack();
  await page.getByRole("heading", { name: /工作区总览/ }).waitFor();
  checks.push("navigation-and-back-preserve-session");
  if (process.env.ANSTS_SYSTEM_VIEWER_TOKEN_FILE) {
    const viewer = await contextFor(process.env.ANSTS_SYSTEM_VIEWER_TOKEN_FILE);
    const restricted = await viewer.newPage();
    restricted.on("pageerror", (e) => errors.push(e.message));
    let topologyRequests = 0;
    restricted.on("request", (r) => {
      if (new URL(r.url()).pathname === "/v1/console/system")
        topologyRequests++;
    });
    await restricted.goto(origin + "/system");
    await restricted
      .getByRole("heading", { name: "控制平面连接", exact: true })
      .waitFor();
    await restricted
      .getByRole("heading", { name: "全局拓扑需要 SRE 管理权限", exact: true })
      .waitFor();
    assert.equal(topologyRequests, 0);
    assert.equal(await restricted.locator(".system-map").count(), 0);
    const forbidden = await viewer.request.get(origin + "/v1/console/system");
    assert.equal(forbidden.status(), 403);
    await viewer.close();
    checks.push(
      "viewer-has-basic-status-only",
      "server-rejects-viewer-topology",
    );
  }
  assert.deepEqual(errors, []);
  checks.push("no-browser-errors");
  console.log(
    JSON.stringify(
      {
        passed: true,
        checks,
        observed: {
          nodes: data.nodes.length,
          edges: data.edges.length,
          status: data.status,
        },
      },
      null,
      2,
    ),
  );
} finally {
  await browser.close();
}
