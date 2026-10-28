'use strict';
let token = '';
let timer;
let active = false;
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
  el('login').hidden = false;
  el('connection').textContent = 'Disconnected';
}
async function refresh() {
  if (active || !token) return;
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
  return card;
}
el('connect').addEventListener('click', () => { token = el('token').value; el('token').value = ''; clearInterval(timer); refresh(); timer = setInterval(refresh, 3000); });
el('refresh').addEventListener('click', refresh);
el('disconnect').addEventListener('click', disconnect);
el('token').addEventListener('keydown', event => { if (event.key === 'Enter') el('connect').click(); });
