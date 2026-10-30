'use strict';
let token = '';
let timer;
let active = false;
let mutating = false;
let taskVersions = new Map();
let taskAfter = null;
let taskNext = null;
const el = id => document.getElementById(id);
const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value ?? '';
  if (className) node.className = className;
  return node;
};
function disconnect() {
  if (typeof clearChat === 'function') clearChat();
  token = '';
  el('diagnostics').hidden = true;
  el('diagnostics-entries').replaceChildren();
  el('diagnostics-message').textContent = '';
  clearInventory();
  closeHistory();
  closeEvidence();
  closeCheckpoint();
  closeReassignment();
  clearInterval(timer);
  el('token').value = '';
  el('tasks').replaceChildren();
  taskVersions.clear();
  taskAfter = null;
  taskNext = null;
  el('tasks-next').hidden = true;
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
    const response = await fetch('/api/tasks' + (taskAfter === null ? '' : '?' + new URLSearchParams({after: taskAfter})), {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    if (response.status === 401) { disconnect(); el('message').textContent = 'Authentication failed. Check the operator token.'; return; }
    if (!response.ok) throw new Error(`Service unavailable (${response.status}).`);
    const data = await response.json();
    if (currentToken !== token) return;
    if (data.type !== 'task_page' || !Array.isArray(data.tasks)) throw new Error('Unexpected service response.');
    taskNext = data.next_after;
    el('tasks-next').hidden = !taskNext;
    taskVersions = new Map(data.tasks.map(item=>[item.aggregate.id,item.aggregate]));
    el('tasks').replaceChildren(...data.tasks.map(renderTask));
    el('summary').textContent = `${data.tasks.length} tasks on this page${taskNext ? ' · more available' : ''}`;
    el('message').textContent = data.tasks.length ? '' : (taskAfter ? 'No tasks on this page. Return to the first page.' : 'No tasks yet. Submit a native workflow from the operator CLI.');
    el('connection').textContent = `Connected · ${new Date().toLocaleTimeString()}`;
    el('login').hidden = true;
    el('toolbar').hidden = false;
    const firstInventory = el('inventory').hidden;
    el('inventory').hidden = false;
    if (firstInventory) loadInventory();
    el('dispatch').hidden = false;
    const healthResponse = await fetch('/api/health', {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    if (!healthResponse.ok) throw new Error('Service health unavailable.');
    const health = await healthResponse.json();
    if (currentToken !== token) return;
    if (health.type !== 'health') throw new Error('Unexpected health response.');
    el('connection').textContent = `${health.status === 'ready' ? 'Connected' : 'Degraded'} · Watcher: ${health.repository_watcher ?? 'unknown'} · ${new Date().toLocaleTimeString()}`;
    await refreshDiagnostics(currentToken);
    await refreshQueue(currentToken);
    if (typeof refreshChat === 'function') await refreshChat(currentToken);
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
  field('Project', item.project_id);
  field('Priority', item.priority);
  field('Worker', task.attempt?.worker);
  field('Attempt', task.attempt?.id);
  if (task.attempt) {
    const lease = task.attempt.lease;
    const observation = item.lease_observation;
    let label = 'Observation unavailable · refresh required';
    if (observation) {
      if (lease.status === 'ended') label = 'Ended';
      else if (!observation.boot_matches) label = 'Prior daemon boot · worker mutation refused';
      else if (lease.status === 'recovery_pending') label = 'Recovery pending · worker mutation refused';
      else label = observation.accepts_worker_mutation ? 'Worker lease valid at observation' : 'Worker lease expired';
    }
    field('Lease', label);
    field('Lease epoch', lease.epoch);
    field('Lease / daemon boot', `${lease.boot_generation} / ${observation?.daemon_boot_generation ?? 'unknown'}`);
    field('Worker / owner remaining at observation', observation ? `${observation.worker_remaining_ms ?? 'n/a'} / ${observation.owner_remaining_ms ?? 'n/a'} ms` : 'Unavailable');
    field('Lease observed', observation ? new Date(observation.observed_at_ms).toISOString() : 'Unavailable');
  }
  field('Verifier', task.verification?.verdict?.verifier);
  field('Review verdict', task.verification?.verdict ? (task.verification.verdict.accepted ? 'Accepted' : 'Rejected') : 'Pending');
  field('Source', task.verification?.source);
  field('Review source', task.verification?.verdict?.source);
  card.append(details);
  if (task.attempt) card.append(text('p', 'Lease validity is sampled by the daemon; it does not prove the process is live or authorize an action. Refresh after connection loss.', 'hint'));
  const results = new Map((task.verification?.checks || []).map(check => [check.check_id, check]));
  const checks = text('ul', '', 'checks');
  for (const check of task.required_checks || []) {
    const result = results.get(check.id);
    const row = text('li', `${result ? (result.passed ? 'Passed' : 'Failed') : 'Pending'} · ${check.id}`);
    if (result) row.append(text('p', `Recorded by ${result.recorded_by} · Source ${result.source}`, 'hint'));
    checks.append(row);
  }
  card.append(checks);
  if (task.blocker) {
    card.append(text('p', task.blocker.detail, 'blocker'));
    card.append(text('p', `Resume when: ${task.blocker.resume_when}`, 'blocker'));
  }
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
  const editableDecisions = ['backlog', 'ready', 'paused', 'blocked'].includes(task.state);
  const decisions = task.decisions || [];
  const decisionBox = text('section', '', 'decisions');
  decisionBox.append(text('h3', `Task decisions · ${decisions.filter(d => !d.resolution).length} open`));
  decisionBox.append(text('p', 'Open decisions prevent completion. Pause active work before changing decisions; continue retained work afterward to deliver the new context.', 'hint'));
  for (const decision of decisions) {
    const row = text('div', '', 'decision');
    row.append(text('p', `${decision.resolution ? 'Resolved' : 'Open'} · ${decision.id} · ${decision.question}`));
    row.append(text('p', `Opened by ${decision.opened_by} · ${new Date(decision.opened_at_ms).toISOString()}`, 'hint'));
    if (decision.resolution) {
      row.append(text('p', decision.resolution.rationale));
      row.append(text('p', `Resolved by ${decision.resolution.resolved_by} · ${new Date(decision.resolution.resolved_at_ms).toISOString()}`, 'hint'));
    } else if (editableDecisions) {
      const resolve = text('button', 'Resolve decision');
      resolve.addEventListener('click', () => {
        const rationale = window.prompt('Resolution and rationale (maximum 2048 bytes). This records a decision; it does not grant new capabilities.');
        if (rationale?.trim()) control(task, 'resolve_decision', {decision_id: decision.id, rationale});
      });
      row.append(resolve);
    }
    decisionBox.append(row);
  }
  if (editableDecisions && decisions.length < 32) {
    const open = text('button', 'Open decision');
    open.addEventListener('click', () => {
      const question = window.prompt('Question that must be resolved before completion (maximum 1024 bytes):');
      if (question?.trim()) control(task, 'open_decision', {decision_id: crypto.randomUUID(), question});
    });
    controls.append(open);
  }
  if (decisions.length) card.append(decisionBox);
  const checkpoint = text('button', 'View checkpoint');
  checkpoint.addEventListener('click', () => loadCheckpoint(task.id));
  controls.append(checkpoint);
  const history = text('button', 'View history');
  history.addEventListener('click', () => loadHistory(task.id));
  controls.append(history);
  const evidence = text('button', 'View evidence');
  evidence.addEventListener('click', () => loadEvidence(task.id));
  controls.append(evidence);
  card.append(controls);
  return card;
}
el('connect').addEventListener('click', () => { token = el('token').value; el('token').value = ''; clearInterval(timer); refresh(); timer = setInterval(refresh, 3000); });
el('refresh').addEventListener('click', refresh);
el('disconnect').addEventListener('click', disconnect);
el('token').addEventListener('keydown', event => { if (event.key === 'Enter') el('connect').click(); });

async function control(task, action, fields = {}) {
  if (mutating || !token) return;
  if (action === 'cancel' && !window.confirm(`Cancel task ${task.id}? Retained artifacts will be preserved.`)) return;
  mutating = true;
  const currentToken = token;
  document.querySelectorAll('#tasks button').forEach(button => { button.disabled = true; });
  let message;
  try {
    const response = await fetch('/api/control', {
      method: 'POST', headers: {Authorization: `Bearer ${currentToken}`, 'Content-Type': 'application/json'},
      body: JSON.stringify({task_id: task.id, expected_version: task.version, idempotency_key: crypto.randomUUID(), action, ...fields}),
      signal: AbortSignal.timeout(5000)
    });
    if (currentToken !== token) return;
    if (response.status === 401) { disconnect(); message = 'Authentication failed.'; }
    else if (response.status === 409) message = 'Task changed or action is not allowed. Refreshed state; inspect it before retrying.';
    else if (!response.ok) message = 'Action was not confirmed. Inspect refreshed state before retrying.';
    else if (['open_decision', 'resolve_decision'].includes(action)) message = 'Decision recorded. Existing instruction pins and capability limits remain authoritative.';
    else message = ['ready', 'resume'].includes(action) ? 'Task is ready. Use the workflow queue to start or continue approved work.' : 'Action applied. Worker shutdown is not confirmed; inspect execution evidence before restarting work.';
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
  el('queue').replaceChildren(...data.entries.map(item => {
    const row=text('li', `${item.task_id} · ${item.waiting_for_dependencies ? 'waiting for dependency admission' : (item.waiting_for_event ? 'waiting for eligible subscribed event' : item.status)}${item.outcome_json ? ' · ' + item.outcome_json : ''}`);
    if (item.waiting_for_event && item.pending_generation_limited_events > 0) row.append(text('p', `${item.pending_generation_limited_events} pending event(s) reached the causal generation limit and cannot start this workflow.`, 'hint'));
    for (const dependency of item.dependencies || []) {
      row.append(text('p', `${dependency.predecessor_task_id} · ${dependency.accepted_source || 'not yet admitted'}`, 'hint'));
    }
    if(item.selected_worker) row.append(text('p', `Selected worker: ${item.selected_worker}`, 'hint'));
    const task=taskVersions.get(item.task_id);
    if (task && !['completed','cancelled'].includes(task.state) && ['finished','needs_recovery'].includes(item.status)) {
      const button=text('button','Continue retained work');
      button.addEventListener('click',()=>continueWorkflow(task,button)); row.append(button);
      const reassign=text('button','Reassign retained work');
      reassign.addEventListener('click',()=>{closeReassignment();reassignmentTask=task;el('reassignment-title').textContent=`Reassign · ${task.id}`;el('reassignment').hidden=false;});row.append(reassign);
    }
    return row;
  }));
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
    const healthResponse = await fetch('/api/health', {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    if (!healthResponse.ok) throw new Error('Service health unavailable.');
    const health = await healthResponse.json();
    if (currentToken !== token) return;
    if (health.type !== 'health') throw new Error('Unexpected health response.');
    el('connection').textContent = `${health.status === 'ready' ? 'Connected' : 'Degraded'} · Watcher: ${health.repository_watcher ?? 'unknown'} · ${new Date().toLocaleTimeString()}`;
    await refreshDiagnostics(currentToken);
    await refreshQueue(currentToken);
  } catch (_) { if(currentToken===token) el('message').textContent='Submission outcome unknown. Inspect queue before retrying.'; }
  finally { el('submit-plan').disabled=false; }
});

async function continueWorkflow(task,button) {
  if(!token || !window.confirm(`Continue retained work for ${task.id}? New model work may start. Unverified write fences will be refused.`))return;
  const currentToken=token; button.disabled=true;
  try {
    const response=await fetch('/api/workflows/continue',{method:'POST',headers:{Authorization:`Bearer ${currentToken}`,'Content-Type':'application/json'},body:JSON.stringify({task_id:task.id,expected_version:task.version}),signal:AbortSignal.timeout(5000)});
    if(currentToken!==token)return;
    await refresh();
    el('message').textContent=response.ok?'Continuation queued from retained work.':'Continuation refused or unconfirmed. Inspect task version and execution fence before retrying.';
  } catch(_) {if(currentToken===token)el('message').textContent='Continuation outcome unknown. Refresh before retrying.';}
  finally {button.disabled=false;}
}


let historyGeneration = 0;
let historyTask = null;
let historyBefore = null;
function closeHistory() {
  historyGeneration++;
  historyTask = null;
  historyBefore = null;
  el('history').hidden = true;
  el('history-entries').replaceChildren();
  el('history-message').textContent = '';
  el('history-title').textContent = 'Task history';
}
async function loadHistory(taskId, before = null) {
  if (!token) return;
  const generation = ++historyGeneration;
  const currentToken = token;
  historyTask = taskId;
  historyBefore = null;
  el('history').hidden = false;
  el('history-title').textContent = `History · ${taskId}`;
  el('history-entries').replaceChildren();
  el('history-older').hidden = true;
  el('history-message').textContent = 'Loading history…';
  const params = new URLSearchParams({task_id: taskId});
  if (before !== null) params.set('before', before);
  try {
    const response = await fetch(`/api/history?${params}`, {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (generation !== historyGeneration || currentToken !== token) return;
    if (response.status === 401) { disconnect(); el('message').textContent = 'Authentication failed.'; return; }
    if (!response.ok) throw new Error('History unavailable. Retry with Newest entries.');
    const data = await response.json();
    if (generation !== historyGeneration || currentToken !== token) return;
    if (data.type !== 'task_history' || !Array.isArray(data.entries)) throw new Error('Invalid history response.');
    el('history-entries').replaceChildren(...data.entries.map(entry => {
      const row = text('li', '');
      row.append(text('strong', entry.kind), text('p', `${new Date(entry.occurred_at_ms).toLocaleString()} · Actor: ${entry.actor_id}`), text('p', `Sequence: ${entry.sequence} · Correlation: ${entry.correlation_id}`, 'hint'));
      return row;
    }));
    historyBefore = data.next_before;
    el('history-older').hidden = !historyBefore;
    el('history-message').textContent = data.entries.length ? `${data.entries.length} entries${historyBefore ? ' · older entries available' : ' · end of history'}` : 'No audit entries.';
  } catch (error) {
    if (generation === historyGeneration && currentToken === token) el('history-message').textContent = error.message;
  }
}
el('history-refresh').addEventListener('click', () => { if (historyTask) loadHistory(historyTask); });
el('history-older').addEventListener('click', () => { if (historyTask && historyBefore) loadHistory(historyTask, historyBefore); });
el('history-close').addEventListener('click', closeHistory);

el('tasks-first').addEventListener('click', () => { if (!active && !mutating) { taskAfter = null; refresh(); } });
el('tasks-next').addEventListener('click', () => { if (!active && !mutating && taskNext) { taskAfter = taskNext; refresh(); } });

let checkpointTask = null;
let checkpointRequest = 0;
function closeCheckpoint() {
  checkpointTask = null;
  checkpointRequest++;
  el('checkpoint').hidden = true;
  el('checkpoint-body').replaceChildren();
  el('checkpoint-message').textContent = '';
}
async function loadCheckpoint(taskId) {
  if (!token) return;
  checkpointTask = taskId;
  const request = ++checkpointRequest;
  const currentToken = token;
  el('checkpoint').hidden = false;
  el('checkpoint-title').textContent = `Checkpoint · ${taskId}`;
  el('checkpoint-body').replaceChildren();
  el('checkpoint-message').textContent = 'Loading recorded claims…';
  try {
    const response = await fetch('/api/checkpoint?' + new URLSearchParams({task_id: taskId}), {headers:{Authorization:`Bearer ${currentToken}`},cache:'no-store',signal:AbortSignal.timeout(5000)});
    if (request !== checkpointRequest || currentToken !== token) return;
    if (response.status === 401) { disconnect(); return; }
    if (!response.ok) throw new Error('Checkpoint unavailable. Refresh after inspecting task state.');
    const data = await response.json();
    if (request !== checkpointRequest || currentToken !== token) return;
    if (data.type !== 'task_checkpoint') throw new Error('Unexpected checkpoint response.');
    const view = data.view, record = view.checkpoint;
    el('checkpoint-message').textContent = record ? (view.inherited ? 'Inherited from an earlier fenced attempt; recheck against retained work.' : 'Last accepted observation; claims may be stale.') : 'No accepted structured checkpoint. The worker may not have published a valid checkpoint yet. Retained files are separate evidence.';
    if (!record) return;
    const claims = record.claims, body = el('checkpoint-body');
    body.append(text('p', view.trust, 'hint'));
    body.append(text('p', `Source attempt: ${claims.attempt_id} · Epoch ${claims.lease_epoch} · Sequence ${claims.sequence}`));
    body.append(text('p', `Observed: ${new Date(record.observed_at_ms).toISOString()}`));
    for (const [label, value] of [['Completed work (claim)',claims.completed_work],['Current state (claim)',claims.current_state],['Next action (claim)',claims.next_action]]) {
      body.append(text('h3', label), text('p', value));
    }
    for (const [label, values] of [['Open decisions (claims)',claims.open_decisions],['Running commands (claims, not live process inventory)',claims.running_commands],['Changed artifacts (claims)',claims.changed_artifacts]]) {
      body.append(text('h3',label)); const list=text('ul','');
      for (const value of values) list.append(text('li',value));
      if (!values.length) list.append(text('li','None claimed'));
      body.append(list);
    }
  } catch (error) {
    if (request === checkpointRequest && currentToken === token) el('checkpoint-message').textContent = error.message;
  }
}
el('checkpoint-close').addEventListener('click',closeCheckpoint);
el('checkpoint-refresh').addEventListener('click',()=>{if(checkpointTask)loadCheckpoint(checkpointTask);});

let reassignmentTask=null;
let reassignmentGeneration=0;
function closeReassignment(){
  reassignmentTask=null;reassignmentGeneration++;
  el('reassignment').hidden=true;el('successor-id').value='';el('successor-pin').value='';
}
el('reassignment-close').addEventListener('click',closeReassignment);
el('reassignment-submit').addEventListener('click',async()=>{
  const task=reassignmentTask,currentToken=token,generation=reassignmentGeneration;
  const worker=el('successor-id').value.trim(),file=el('successor-pin').files[0];
  if(!task||!currentToken||!worker||!file||file.size>4096){el('message').textContent='Choose a registered agent and approved profile up to 4 KiB.';return;}
  if(!window.confirm(`Reassign ${task.id} to ${worker}? New model work may start.`))return;
  el('reassignment-submit').disabled=true;
  try{
    const pin=JSON.parse(await file.text());
    if(currentToken!==token||generation!==reassignmentGeneration)return;
    const response=await fetch('/api/workflows/reassign',{method:'POST',headers:{Authorization:`Bearer ${currentToken}`,'Content-Type':'application/json'},body:JSON.stringify({task_id:task.id,expected_version:task.version,worker_id:worker,pin}),signal:AbortSignal.timeout(5000)});
    if(currentToken!==token||generation!==reassignmentGeneration)return;
    if(response.ok)closeReassignment();
    await refresh();
    if(currentToken!==token)return;
    el('message').textContent=response.ok?'Reassignment queued; previous evidence retained.':'Reassignment refused or unconfirmed. Inspect current ownership, profile and fence before retrying.';
  }catch(_){if(currentToken===token&&generation===reassignmentGeneration)el('message').textContent='Reassignment outcome unknown. Refresh before retrying.';}
  finally{el('reassignment-submit').disabled=false;}
});

let inventoryRevision = 0;
let inventoryNext = null;
function clearInventory() {
  inventoryRevision++;
  inventoryNext = null;
  el('inventory').hidden = true;
  el('inventory-entries').replaceChildren();
  el('inventory-message').textContent = '';
  el('inventory-next').hidden = true;
}
async function loadInventory(after = null) {
  const revision = ++inventoryRevision;
  const currentToken = token;
  const resource = el('inventory-resource').value;
  inventoryNext = null;
  el('inventory-next').hidden = true;
  el('inventory-entries').replaceChildren();
  if (!currentToken) return;
  el('inventory-message').textContent = 'Loading…';
  const params = new URLSearchParams({resource});
  if (after !== null) params.set('after', after);
  try {
    const response = await fetch('/api/inventory?' + params, {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (revision !== inventoryRevision || currentToken !== token) return;
    if (response.status === 401) { disconnect(); return; }
    if (!response.ok) throw new Error(`Inventory unavailable (${response.status}).`);
    const data = await response.json();
    if (revision !== inventoryRevision || currentToken !== token) return;
    if (data.type !== 'inventory' || data.resource !== resource || !Array.isArray(data.entries)) throw new Error('Unexpected inventory response.');
    el('inventory-entries').replaceChildren(...data.entries.map(entry => {
      const card = text('article', '', 'task');
      card.append(text('h3', entry.id));
      if (resource === 'usage') card.append(text('p', 'Adapter-reported metadata, not a bill or account total. Missing reports are unknown, not zero. Token categories are not summed.', 'hint'));
      const details = text('dl', '', 'details');
      for (const [name, value] of Object.entries(entry)) {
        if (name === 'id') continue;
        if (value && typeof value === 'object') {
          for (const [key, nested] of Object.entries(value)) {
            details.append(text('dt', `${name} · ${key.replaceAll('_', ' ')}`), text('dd', nested === null ? 'Not reported' : typeof nested === 'object' ? JSON.stringify(nested) : String(nested)));
          }
        } else details.append(text('dt', name.replaceAll('_', ' ')), text('dd', value === null ? '—' : String(value)));
      }
      card.append(details);
      return card;
    }));
    inventoryNext = data.next_after;
    el('inventory-next').hidden = !inventoryNext;
    el('inventory-message').textContent = `${data.entries.length} ${resource} on this page · Snapshot ${new Date().toLocaleTimeString()}${inventoryNext ? ' · more available' : ''}`;
  } catch (error) {
    if (revision === inventoryRevision && currentToken === token) el('inventory-message').textContent = error.message;
  }
}
el('inventory-resource').addEventListener('change', () => loadInventory());
el('inventory-first').addEventListener('click', () => loadInventory());
el('inventory-next').addEventListener('click', () => { if (inventoryNext) loadInventory(inventoryNext); });

let evidenceGeneration = 0;
let evidenceTask = null;
let evidenceBefore = null;
function closeEvidence() {
  evidenceGeneration++;
  evidenceTask = null;
  evidenceBefore = null;
  el('evidence').hidden = true;
  el('evidence-entries').replaceChildren();
  el('evidence-message').textContent = '';
  el('evidence-title').textContent = 'Task evidence';
}
async function loadEvidence(taskId, before = null) {
  if (!token) return;
  const generation = ++evidenceGeneration;
  const currentToken = token;
  evidenceTask = taskId;
  evidenceBefore = null;
  el('evidence').hidden = false;
  el('evidence-title').textContent = `Evidence · ${taskId}`;
  el('evidence-entries').replaceChildren();
  el('evidence-older').hidden = true;
  el('evidence-message').textContent = 'Loading evidence…';
  const params = new URLSearchParams({task_id: taskId});
  if (before !== null) params.set('before', before);
  try {
    const response = await fetch(`/api/evidence?${params}`, {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (generation !== evidenceGeneration || currentToken !== token) return;
    if (response.status === 401) { disconnect(); el('message').textContent = 'Authentication failed.'; return; }
    if (!response.ok) throw new Error('Evidence unavailable. Retry with Newest entries.');
    const data = await response.json();
    if (generation !== evidenceGeneration || currentToken !== token) return;
    if (data.type !== 'task_evidence' || !Array.isArray(data.entries)) throw new Error('Invalid evidence response.');
    el('evidence-entries').replaceChildren(...data.entries.map(renderEvidence));
    evidenceBefore = data.next_before;
    el('evidence-older').hidden = !evidenceBefore;
    el('evidence-message').textContent = data.entries.length ? `${data.entries.length} entries${evidenceBefore ? ' · older entries available' : ' · end of evidence'}` : 'No audit entries.';
  } catch (error) {
    if (generation === evidenceGeneration && currentToken === token) el('evidence-message').textContent = error.message;
  }
}
el('evidence-refresh').addEventListener('click', () => { if (evidenceTask) loadEvidence(evidenceTask); });
el('evidence-older').addEventListener('click', () => { if (evidenceTask && evidenceBefore) loadEvidence(evidenceTask, evidenceBefore); });
el('evidence-close').addEventListener('click', closeEvidence);
function renderEvidence(entry) {
  const row = text('li', '');
  const labels = {'execution.prepared':'Execution prepared','execution.started':'Execution started','execution.exited':'Execution exit recorded','execution.fenced':'Writer fence recorded','execution.blocked':'Execution blocked',begin_verification:'Verification started',bind_verification_source:'Verification source',record_check:'Check result',record_verdict:'Review verdict',complete:'Completion recorded'};
  row.append(text('strong', labels[entry.kind] ?? entry.kind), text('p', `${new Date(entry.occurred_at_ms).toLocaleString()} · Recorded by ${entry.actor_id} · Audit #${entry.sequence}`));
  const d = entry.details;
  if (d.availability !== 'recorded') {
    row.append(text('p', 'Structured evidence unavailable: missing, oversized, malformed or inconsistent record.', 'hint'));
    return row;
  }
  const fields = text('dl', '', 'details');
  const field = (label, value) => { fields.append(text('dt', label), text('dd', value ?? 'Unavailable')); };
  if (d.current_execution_snapshot) {
    const e = d.current_execution_snapshot;
    field('Attempt / lease epoch', `${e.attempt_id} / ${e.lease_epoch}`);
    field('Packet SHA-256', e.packet_sha256);
    field('Runner', e.runner_id);
    field('Current recorded execution', `${e.current_recorded_state} · Revision ${e.current_revision}`);
    field('Recorded exit code', e.recorded_exit_code);
    field('Execution record updated', new Date(e.updated_at_ms).toLocaleString());
    row.append(text('p', 'Execution fields describe the latest persisted record, which can be newer than this audit event. They do not prove current process liveness.', 'hint'));
  }
  if (d.source !== undefined) field('Recorded source', d.source);
  if (d.task_version_before !== undefined) field('Task version before event', d.task_version_before);
  if (d.attempt_id !== undefined) field('Attempt / lease epoch', `${d.attempt_id} / ${d.lease_epoch}`);
  if (d.check_id !== undefined) {
    field('Check', d.check_id);
    field('Recorded check result', d.passed ? 'Passed' : 'Failed');
    field('Structured runtime receipt', d.receipt_status === 'recorded_runtime_receipt' ? 'Recorded and internally source-bound' : 'Unavailable');
  }
  if (d.runtime_receipt) {
    const r = d.runtime_receipt;
    field('Check outcome / exit code', `${r.outcome} / ${r.exit_code ?? 'Unavailable'}`);
    field('Source SHA-256', r.source_sha256);
    field('Check definition SHA-256', r.definition_sha256);
    field('Standard output SHA-256', r.stdout_sha256);
    field('Standard error SHA-256', r.stderr_sha256);
  }
  if (d.accepted !== undefined) {
    field('Recorded review verdict', d.accepted ? 'Accepted' : 'Rejected');
    field('Review output SHA-256', d.review_output_sha256);
  }
  if (d.recorded_completion) field('Lifecycle result', 'Completion recorded');
  row.append(fields);
  return row;
}

async function refreshDiagnostics(currentToken) {
  if (currentToken !== token) return;
  try {
    const response = await fetch('/api/invariants', {headers: {Authorization: `Bearer ${currentToken}`}, cache: 'no-store', signal: AbortSignal.timeout(5000)});
    if (currentToken !== token) return;
    if (!response.ok) throw new Error('Diagnostics unavailable');
    const data = await response.json();
    if (currentToken !== token) return;
    if (data.type !== 'invariants' || !Array.isArray(data.violations)) throw new Error('Invalid diagnostics');
    renderDiagnostics(data);
  } catch (_) {
    if (currentToken !== token) return;
    el('diagnostics').hidden = false;
    el('diagnostics-entries').replaceChildren();
    el('diagnostics-message').textContent = 'Recovery diagnostics unavailable. Refresh before acting; no recovery has been authorized.';
  }
}
function renderDiagnostics(data) {
  el('diagnostics').hidden = data.violations.length === 0;
  el('diagnostics-message').textContent = data.violations.length > 128 ? 'Showing the first 128 diagnostics. Use check-invariants for the full report.' : 'Preserve task state and evidence while recovery is unresolved.';
  el('diagnostics-entries').replaceChildren(...data.violations.slice(0,128).map(item => {
    const row = text('li', `${String(item.code).slice(0,128)}: ${String(item.summary).slice(0,1024)}`);
    if (item.code === 'auxiliary_recovery_pending' && Array.isArray(item.context?.invocations)) {
      const details = text('ul', '');
      for (const invocation of item.context.invocations.slice(0,128)) {
        details.append(text('li', `Task ${String(invocation.task_id).slice(0,256)} · invocation ${String(invocation.invocation_id).slice(0,256)} · generation ${invocation.generation}`));
      }
      row.append(text('p', `${item.context.count} unresolved invocation(s); showing ${Math.min(item.context.invocations.length,128)}.`), details);
    }
    return row;
  }));
}
