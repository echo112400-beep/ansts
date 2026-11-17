/** Minimal live console. Unsupported legacy management screens remain hidden. */
(() => {
  'use strict';
  const auth = window.AnstsConsoleAuth;
  let root, status, rows, total, tenant, task, prompt, submit, form, refresh, updated;
  let busy = false;
  const cell = (row, value) => { const td = document.createElement('td'); td.textContent = value ?? '—'; row.append(td); };
  async function api(path, body) {
    const version = auth.version;
    const response = await fetch(path, { method: body ? 'POST' : 'GET', headers: { authorization: auth.authorization(), 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined, cache: 'no-store', redirect: 'error', signal: auth.signal() });
    const data = await response.json();
    if (version !== auth.version) throw new Error('SESSION_CHANGED');
    if (response.status === 401) { auth.logout('凭据已失效，请重新登录。'); throw new Error('USER_AUTH_REQUIRED'); }
    if (!response.ok) throw new Error(response.status === 403 ? '当前身份无权执行此操作。' : '请求失败，请稍后重试。');
    return data;
  }
  async function load() {
    if (!auth.isAuthenticated() || busy) return;
    busy = true; refresh.disabled = true;
    const version = auth.version;
    status.textContent = '正在读取任务…';
    try {
      const result = await api('/v1/console/tasks');
      if (version !== auth.version) return;
      rows.replaceChildren();
      for (const item of result.tasks) {
        const row = document.createElement('tr');
        cell(row, item.task_id); cell(row, item.tenant_id); cell(row, item.state); cell(row, item.assigned_node_id);
        rows.append(row);
      }
      if (!result.tasks.length) { const row = document.createElement('tr'); const td = document.createElement('td'); td.colSpan = 4; td.textContent = '当前租户暂无任务。'; row.append(td); rows.append(row); }
      total.textContent = String(result.total);
      updated.textContent = `更新于 ${new Date(result.refreshedAt).toLocaleTimeString()} · 显示最近 ${result.tasks.length} 条`;
      status.textContent = '已连接 · 仅显示当前身份可访问的任务';
    } catch (error) {
      if (version === auth.version && auth.isAuthenticated()) status.textContent = `读取失败，保留上次数据。${error.message}`;
    } finally { if (version === auth.version) { busy = false; refresh.disabled = false; } }
  }
  function reset() {
    busy = false; rows.replaceChildren(); total.textContent = '—'; updated.textContent = ''; status.textContent = '';
    form.reset();
    root.querySelector('#console-live-submit-status').textContent = '';
    const profile = auth.profile();
    tenant.value = profile?.scopeTenant === 'all' ? '' : profile?.scopeTenant ?? '';
    tenant.readOnly = profile?.scopeTenant !== 'all';
    submit.disabled = !profile?.canWrite;
    form.hidden = !profile?.canWrite;
    if (profile) load();
  }
  function init() {
    root = document.createElement('main'); root.id = 'console-live-workspace'; root.tabIndex = -1;
    root.innerHTML = '<header><div><div class="console-wordmark">ANSTS</div><h1>任务工作区</h1><p>查看当前租户任务，或提交新的执行请求。</p></div><button type="button" id="console-live-refresh">刷新任务</button></header><section aria-label="任务概览"><div class="console-total"><span>可访问任务</span><strong id="console-live-total">—</strong></div><div><p id="console-live-status" role="status"></p><small id="console-live-updated"></small></div></section><form id="console-live-create"><h2>提交任务</h2><div class="console-fields"><label>租户<input id="console-live-tenant" required maxlength="128"></label><label>任务 ID<input id="console-live-task" required maxlength="128"></label></div><label>任务说明<textarea id="console-live-prompt" required maxlength="10000" rows="3"></textarea></label><button type="submit">提交执行</button><p id="console-live-submit-status" role="status"></p></form><section class="console-task-section"><h2>最近任务</h2><div class="console-table-wrap"><table><thead><tr><th>任务 ID</th><th>租户</th><th>状态</th><th>执行节点</th></tr></thead><tbody id="console-live-rows"></tbody></table></div></section>';
    document.querySelector('.shell').append(root);
    rows = root.querySelector('tbody'); total = root.querySelector('#console-live-total'); status = root.querySelector('#console-live-status'); updated = root.querySelector('#console-live-updated');
    form = root.querySelector('form'); tenant = root.querySelector('#console-live-tenant'); task = root.querySelector('#console-live-task'); prompt = root.querySelector('textarea'); submit = form.querySelector('button'); refresh = root.querySelector('#console-live-refresh');
    refresh.onclick = load;
    form.onsubmit = async event => {
      event.preventDefault(); if (submit.disabled || !auth.isAuthenticated()) return;
      const version = auth.version; submit.disabled = true;
      const feedback = root.querySelector('#console-live-submit-status'); feedback.textContent = '正在提交…';
      try {
        const result = await api('/v1beta1/tasks/dispatch', { tenantId: tenant.value.trim(), taskId: task.value.trim(), promptText: prompt.value });
        if (version !== auth.version) return;
        feedback.textContent = `已提交 ${result.taskId}，状态 ${result.state}。`; task.value = ''; prompt.value = ''; await load();
      } catch (error) { if (version === auth.version) feedback.textContent = error.message; }
      finally { if (version === auth.version) submit.disabled = !auth.profile()?.canWrite; }
    };
    auth.onChange(reset); reset();
    setInterval(load, 5000);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
