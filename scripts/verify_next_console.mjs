/** Real browser acceptance; credentials remain in a private file and browser memory.
 * PLAYWRIGHT_MODULE=/absolute/path/to/playwright/index.mjs
 * CHROME_PATH=/absolute/path/to/chrome
 * ANSTS_CONSOLE_TOKEN_FILE=.dev-secrets/console-ui-operator.jwt
 * node scripts/verify_next_console.mjs
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
const { chromium } = await import(
  pathToFileURL(process.env.PLAYWRIGHT_MODULE).href
);
const token = readFileSync(process.env.ANSTS_CONSOLE_TOKEN_FILE, "utf8").trim();
const origin = process.env.ANSTS_CONSOLE_URL ?? "http://127.0.0.1:8088";
const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH,
  headless: true,
  args: ["--no-sandbox", "--no-proxy-server"],
});
const errors = [];
try {
  const page = await browser.newPage({
    viewport: { width: 1440, height: 900 },
  });
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(origin);
  await page.getByText("使用短期开发凭据").click();
  await page.locator("input[type=password]").fill(token);
  await page.getByRole("button", { name: "连接工作区" }).click();
  await page.getByRole("heading", { name: "工作区总览" }).waitFor();
  await page
    .getByRole("button", { name: "创建任务", exact: true })
    .first()
    .click();
  const id = `console-browser-${Date.now()}`;
  await page.locator("[name=id]").fill(id);
  await page
    .locator("[name=prompt]")
    .fill("Next.js console browser acceptance");
  await page.locator("[name=kind]").selectOption("container");
  await page
    .locator("[name=argv]")
    .fill(
      JSON.stringify([
        "node",
        "-e",
        "require('fs').writeFileSync('/workspace/console-result.txt','console-artifact-ok');console.log('console-browser-ok')",
      ]),
    );
  await page.getByRole("button", { name: "提交任务", exact: true }).click();
  await page.getByRole("dialog").waitFor({ state: "hidden" });
  await page.getByRole("status").filter({ hasText: id }).waitFor();
  await page.getByRole("textbox", { name: "搜索任务或租户" }).fill(id);
  const row = page
    .locator("tbody tr")
    .filter({ has: page.getByText(id, { exact: true }) });
  await row.waitFor();
  await page.waitForFunction(
    (id) => {
      const rows = [...document.querySelectorAll("tbody tr")];
      return rows.some(
        (row) =>
          row.textContent.includes(id) && row.textContent.includes("已完成"),
      );
    },
    id,
    { timeout: 120000 },
  );
  await row.locator(".task-link").click();
  await page
    .locator(".logs")
    .filter({ hasText: "console-browser-ok" })
    .waitFor({ timeout: 30000 });
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: /console-result.txt/ }).click();
  const download = await downloadPromise;
  assert.equal(download.suggestedFilename(), "console-result.txt");
  const stream = await download.createReadStream();
  let content = "";
  for await (const chunk of stream) content += chunk.toString();
  assert.equal(content, "console-artifact-ok");
  await page.getByRole("button", { name: "关闭面板" }).click();
  // A failed refresh must retain the last successful rows and expose retry.
  await page.route("**/v1/console/tasks?**", (route) => route.abort());
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.locator(".banner.error").waitFor();
  assert.equal(await row.count(), 1);
  await page.unroute("**/v1/console/tasks?**");
  await page.getByRole("button", { name: "重试", exact: true }).click();
  await page.locator(".banner.error").waitFor({ state: "hidden" });
  // Cancel only a task created by this acceptance run.
  await page
    .getByRole("button", { name: "创建任务", exact: true })
    .first()
    .click();
  const cancelId = id + "-cancel";
  await page.locator("[name=id]").fill(cancelId);
  await page.locator("[name=prompt]").fill("Cancel acceptance task");
  await page.locator("[name=kind]").selectOption("container");
  await page
    .locator("[name=argv]")
    .fill(JSON.stringify(["node", "-e", "setTimeout(()=>{},45000)"]));
  await page.getByRole("button", { name: "提交任务", exact: true }).click();
  await page.getByRole("dialog").waitFor({ state: "hidden" });
  await page.getByRole("textbox", { name: "搜索任务或租户" }).fill(cancelId);
  const cancelRow = page.locator("tbody tr").filter({ hasText: cancelId });
  await cancelRow.waitFor();
  await cancelRow.locator(".task-link").click();
  page.once("dialog", (dialog) =>
    dialog.accept("browser acceptance cancellation"),
  );
  await page.getByRole("button", { name: "请求取消任务" }).click();
  await page
    .locator(".detail-grid dd")
    .filter({ hasText: "已取消" })
    .waitFor({ timeout: 120000 });
  assert.equal(
    await page.getByRole("button", { name: "请求取消任务" }).count(),
    0,
  );
  await page.getByRole("button", { name: "关闭面板" }).click();
  await page.waitForFunction(
    (id) =>
      [...document.querySelectorAll("tbody tr")].some(
        (row) =>
          row.textContent.includes(id) && row.textContent.includes("已取消"),
      ),
    cancelId,
    { timeout: 120000 },
  );
  await page
    .getByRole("textbox", { name: "搜索任务或租户" })
    .fill("no-match-" + Date.now());
  await page.getByRole("heading", { name: "没有匹配的任务" }).waitFor();
  await page.getByRole("button", { name: "清除筛选" }).click();
  await page.getByRole("textbox", { name: "搜索任务或租户" }).fill(id);
  await row.waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForTimeout(350);
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
    false,
  );
  await page.getByRole("button", { name: "打开导航" }).click();
  await page.getByRole("button", { name: "系统状态", exact: true }).click();
  await page.getByRole("heading", { name: "控制平面连接" }).waitFor();
  await page
    .locator(".health-row .badge")
    .filter({ hasText: "已连接" })
    .first()
    .waitFor();
  await page.getByRole("button", { name: "打开导航" }).click();
  await page.getByRole("button", { name: "退出登录" }).click();
  await page.getByRole("heading", { name: /每一次执行/ }).waitFor();
  assert.equal(await page.locator("tbody tr").count(), 0);
  assert.deepEqual(errors, []);
  console.log(
    JSON.stringify(
      {
        passed: true,
        checks: [
          "login",
          "container-submit",
          "scope-search",
          "completion",
          "logs",
          "artifact-download",
          "refresh-failure-retains-data",
          "cancel-own-task",
          "empty-filter-recovery",
          "mobile-overflow",
          "navigation-health",
          "logout-clear",
          "no-page-errors",
        ],
        taskId: id,
      },
      null,
      2,
    ),
  );
} finally {
  await browser.close();
}
