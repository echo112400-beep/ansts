/** Credential login: server-verified profile, memory-only bearer, expiry and local logout. */
(() => {
  'use strict';
  let bearer = '';
  let user = null;
  let expiresAt = 0;
  let generation = 0;
  let expiryTimer;
  let requests = new AbortController();
  const listeners = new Set();
  let dialog, error, input, submit, close, identity;
  const notify = () => listeners.forEach(listener => listener());
  function lock(message = '请使用有效的短期凭据登录。') {
    bearer = ''; user = null; expiresAt = 0; generation++;
    clearTimeout(expiryTimer); requests.abort(); requests = new AbortController();
    document.documentElement.dataset.authState = 'locked';
    if (input) input.value = '';
    if (error) error.textContent = message;
    notify();
    open();
  }
  function authenticated() {
    if (bearer && Date.now() >= expiresAt) lock('凭据已过期，请重新登录。');
    return !!bearer;
  }
  function open() {
    if (!dialog) return;
    close.hidden = !authenticated();
    if (!dialog.open) dialog.showModal();
    input.focus();
  }
  window.AnstsConsoleAuth = {
    open, logout: lock,
    isAuthenticated: authenticated,
    authorization: () => authenticated() ? `Bearer ${bearer}` : '',
    profile: () => authenticated() ? { ...user } : null,
    signal: () => requests.signal,
    onChange: listener => listeners.add(listener),
    get version() { return generation; },
  };
  document.documentElement.dataset.authState = 'locked';
  function init() {
    dialog = document.createElement('dialog');
    dialog.id = 'console-login';
    dialog.setAttribute('aria-labelledby', 'console-login-title');
    dialog.innerHTML = '<form><h1 id="console-login-title">登录 ANSTS 控制台</h1><p>使用管理员签发的短期访问凭据。身份和租户范围由服务端验证，凭据仅保存在当前页面内存中。</p><label for="console-login-token">访问凭据</label><input id="console-login-token" type="password" autocomplete="off" spellcheck="false" maxlength="8192" required><div id="console-login-error" role="alert" aria-live="polite"></div><footer><button type="button" id="console-login-close" hidden>取消</button><button type="submit">验证并登录</button></footer></form>';
    document.body.append(dialog);
    input = dialog.querySelector('input'); error = dialog.querySelector('#console-login-error');
    submit = dialog.querySelector('[type="submit"]'); close = dialog.querySelector('#console-login-close');
    close.onclick = () => { if (authenticated()) dialog.close(); };
    dialog.addEventListener('cancel', event => { if (!authenticated()) event.preventDefault(); });
    const bar = document.createElement('div'); bar.className = 'auth-session-bar';
    bar.innerHTML = '<span id="console-auth-identity"></span><button type="button">退出登录</button>';
    bar.querySelector('button').onclick = () => lock('已退出登录，当前页面凭据和已载入数据已清除。');
    document.querySelector('.shell')?.prepend(bar);
    identity = bar.querySelector('span');
    for (const id of ['identity', 'reset-demo']) {
      const element = document.getElementById(id); if (element) { element.disabled = true; element.hidden = true; }
    }
    const identityButton = document.getElementById('identity-open');
    if (identityButton) identityButton.setAttribute('aria-label', '重新登录');
    dialog.querySelector('form').addEventListener('submit', async event => {
      event.preventDefault();
      if (submit.disabled) return;
      const attemptVersion = generation;
      const candidate = input.value.trim().replace(/^Bearer\s+/i, '');
      input.value = ''; error.textContent = ''; submit.disabled = true;
      try {
        const response = await fetch('/v1/auth/me', { headers: { authorization: `Bearer ${candidate}` }, cache: 'no-store', credentials: 'omit', redirect: 'error', signal: AbortSignal.timeout(10000) });
        if (!response.ok) throw new Error(response.status === 401 ? '凭据无效或已过期，请重新获取。' : '暂时无法验证身份，请稍后重试。');
        const result = await response.json();
        if (attemptVersion !== generation) throw new Error('登录状态已变化，请重新提交凭据。');
        if (!result.authenticated || !result.user || !Number.isFinite(result.expiresAt) || result.expiresAt <= Date.now()) throw new Error('服务端未返回有效身份。');
        requests.abort(); requests = new AbortController(); generation++;
        bearer = candidate; user = result.user; expiresAt = result.expiresAt;
        identity.textContent = `${user.sub} · ${user.role} · 租户 ${user.scopeTenant}`;
        document.documentElement.dataset.authState = 'authenticated';
        clearTimeout(expiryTimer);
        expiryTimer = setTimeout(() => lock('凭据已过期，请重新登录。'), Math.min(2147483647, expiresAt - Date.now()));
        dialog.close(); notify();
      } catch (failure) { error.textContent = failure instanceof Error ? failure.message : '无法登录，请重试。'; input.focus(); }
      finally { submit.disabled = false; }
    });
    open();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
