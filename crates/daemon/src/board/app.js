'use strict';
let token = '';
let timer;
let active = false;
let mutating = false;
const el = id => document.getElementById(id);
const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value ?? '';
  if (className) node.className = className;
  return node;
};
function disconnect() {
  token = '';
  clearInterval(timer);
  el('token').value = '';
  el('tasks').replaceChildren();
  el('toolbar').hidden = true;
  el('dispatch').hidden = true;
  el('queue').replaceChildren();
  el('plan').value = '';
  el('login').hidden = false;
  el('connection').textContent = 'Disconnected';
}
async function refresh() {
  if (active || mutating || !token) return;
  active = true;
  const currentToken = token;
  try {
    const response = await fetch('/api/tasks', {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    if (response.status === 401) { disconnect(); el('message').textContent = 'Authentication failed. Check the operator token.'; return; }
    if (!response.ok) throw new Error(`Service unavailable (${response.status}).`);
    const data = await response.json();
    if (currentToken !== token) return;
    if (data.type !== 'tasks' || !Array.isArray(data.tasks)) throw new Error('Unexpected service response.');
    el('tasks').replaceChildren(...data.tasks.map(renderTask));
    el('summary').textContent = `${data.tasks.length} task${data.tasks.length === 1 ? '' : 's'}`;
    el('message').textContent = data.tasks.length ? '' : 'No tasks yet. Submit a native workflow from the operator CLI.';
    el('connection').textContent = `Connected · ${new Date().toLocaleTimeString()}`;
    el('login').hidden = true;
    el('toolbar').hidden = false;
    el('dispatch').hidden = false;
    await refreshQueue(currentToken);
  } catch (error) {
    if (currentToken === token) { el('message').textContent = error.message; el('connection').textContent = 'Connection lost · displayed data may be stale'; }
  } finally { active = false; }
}
function renderTask(item) {
  const task = item.aggregate;
  const card = text('article', '', 'task');
  const heading = text('div', '', 'task-heading');
  heading.append(text('h2', task.id), text('span', task.state, `state state-${task.state}`));
  card.append(heading, text('p', item.objective));
  const details = text('dl', '', 'details');
  const field = (label, value) => { details.append(text('dt', label), text('dd', value ?? '—')); };
  field('Worker', task.attempt?.worker);
  field('Attempt', task.attempt?.id);
  field('Verifier', task.verification?.verdict?.verifier);
  field('Decision', task.verification?.verdict ? (task.verification.verdict.accepted ? 'Accepted' : 'Rejected') : 'Pending');
  field('Source', task.verification?.source);
  card.append(details);
  const results = new Map((task.verification?.checks || []).map(check => [check.check_id, check]));
  const checks = text('ul', '', 'checks');
  for (const check of task.required_checks || []) {
    const result = results.get(check.id);
    checks.append(text('li', `${result ? (result.passed ? 'Passed' : 'Failed') : 'Pending'} · ${check.id}`));
  }
  card.append(checks);
  if (task.blocker) card.append(text('p', task.blocker.detail, 'blocker'));
  const controls = text('div', '', 'controls');
  const actions = [];
  if (task.state === 'backlog') actions.push(['ready', 'Mark ready']);
  if (['ready', 'claimed', 'running', 'verifying'].includes(task.state)) actions.push(['pause', 'Pause']);
  if (['paused', 'blocked'].includes(task.state)) actions.push(['resume', 'Resume to ready']);
  if (!['completed', 'cancelled'].includes(task.state)) actions.push(['cancel', 'Cancel task']);
  for (const [action, label] of actions) {
    const button = text('button', label);
    button.addEventListener('click', () => control(task, action));
    controls.append(button);
  }
  card.append(controls);
  return card;
}
el('connect').addEventListener('click', () => { token = el('token').value; el('token').value = ''; clearInterval(timer); refresh(); timer = setInterval(refresh, 3000); });
el('refresh').addEventListener('click', refresh);
el('disconnect').addEventListener('click', disconnect);
el('token').addEventListener('keydown', event => { if (event.key === 'Enter') el('connect').click(); });

async function control(task, action) {
  if (mutating || !token) return;
  if (action === 'cancel' && !window.confirm(`Cancel task ${task.id}? Retained artifacts will be preserved.`)) return;
  mutating = true;
  const currentToken = token;
  document.querySelectorAll('#tasks button').forEach(button => { button.disabled = true; });
  let message;
  try {
    const response = await fetch('/api/control', {
      method: 'POST', headers: {Authorization: `Bearer ${currentToken}`, 'Content-Type': 'application/json'},
      body: JSON.stringify({task_id: task.id, expected_version: task.version, idempotency_key: crypto.randomUUID(), action}),
      signal: AbortSignal.timeout(5000)
    });
    if (currentToken !== token) return;
    if (response.status === 401) { disconnect(); message = 'Authentication failed.'; }
    else if (response.status === 409) message = 'Task changed or action is not allowed. Refreshed state; inspect it before retrying.';
    else if (!response.ok) message = 'Action was not confirmed. Inspect refreshed state before retrying.';
    else message = ['ready', 'resume'].includes(action) ? 'Task is ready. Automatic native scheduling is not enabled yet.' : 'Action applied. Active execution stops after ownership revocation is observed.';
  } catch (_) {
    message = 'Action outcome unknown. Inspect refreshed state before retrying.';
  } finally {
    mutating = false;
    if (currentToken === token || !token) {
      await refresh();
      if (message) el('message').textContent = message;
      document.querySelectorAll('#tasks button').forEach(button => { button.disabled = false; });
    }
  }
}

async function refreshQueue(currentToken) {
  const response = await fetch('/api/workflows', {headers:{Authorization:`Bearer ${currentToken}`},cache:'no-store',signal:AbortSignal.timeout(5000)});
  if (!response.ok) throw new Error('Workflow queue unavailable.');
  const data = await response.json();
  if (currentToken !== token) return;
  if (data.type !== 'workflows' || !Array.isArray(data.entries)) throw new Error('Invalid queue response.');
  el('queue').replaceChildren(...data.entries.map(item => text('li', `${item.task_id} · ${item.status}${item.outcome_json ? ' · ' + item.outcome_json : ''}`)));
}
el('submit-plan').addEventListener('click', async () => {
  const file = el('plan').files[0];
  if (!file || !token || file.size > 131072) { el('message').textContent='Select a plan JSON file up to 128 KiB.'; return; }
  if (!window.confirm('Submit this approved plan? An enabled dispatcher can start model work immediately.')) return;
  const currentToken = token;
  el('submit-plan').disabled = true;
  try {
    const body = await file.text();
    if (currentToken !== token) return;
    const response = await fetch('/api/workflows', {method:'POST',headers:{Authorization:`Bearer ${currentToken}`,'Content-Type':'application/json'},body,signal:AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    el('message').textContent=response.ok ? 'Plan persisted. Queue execution requires an enabled dispatcher.' : 'Submission not confirmed. Inspect queue before retrying the same plan.';
    if(response.ok) el('plan').value='';
    await refreshQueue(currentToken);
  } catch (_) { if(currentToken===token) el('message').textContent='Submission outcome unknown. Inspect queue before retrying.'; }
  finally { el('submit-plan').disabled=false; }
});
