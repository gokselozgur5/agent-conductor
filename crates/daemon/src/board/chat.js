'use strict';
let chatId = '';
let chatAfter = 0;
let chatNext = null;
let chatReady = false;
let chatBusy = false;
let chatGeneration = 0;
let chatPending = null;
let chatCreation = null;
let chatListAfter = '';

async function chatRpc(payload, credential = token) {
  const response = await fetch('/api/rpc', {
    method: 'POST', headers: {Authorization: `Bearer ${credential}`, 'Content-Type': 'application/json'},
    body: JSON.stringify({protocol: 'agent-conductor.local', version: 1,
      message_id: crypto.randomUUID(), correlation_id: crypto.randomUUID(), payload}),
    cache: 'no-store', signal: AbortSignal.timeout(5000)
  });
  if (credential !== token) throw new Error('Connection changed.');
  if (!response.ok) throw new Error(`Conversation request failed (${response.status}).`);
  const envelope = await response.json();
  if (credential !== token) throw new Error('Connection changed.');
  if (envelope.result?.status !== 'ok') throw new Error(envelope.result?.payload?.message || 'Conversation request was refused.');
  return envelope.result.payload;
}
function clearChat() {
  chatGeneration++;
  chatId = ''; chatAfter = 0; chatNext = null; chatReady = false; chatBusy = false;
  chatPending = null; chatCreation = null; chatListAfter = '';
  el('chat').hidden = true;
  el('chat-messages').replaceChildren();
  el('chat-conversations').replaceChildren();
  el('chat-agent').replaceChildren();
  el('chat-input').value = ''; el('chat-input').disabled = false;
  el('chat-project').value = '';
  el('chat-status').textContent = ''; el('chat-send').textContent = 'Send';
}
async function loadChatList(credential, after = '') {
  const generation = chatGeneration;
  const data = await chatRpc({type: 'list_conversations', after}, credential);
  if (generation !== chatGeneration) return;
  if (data.type !== 'conversations' || !Array.isArray(data.entries)) throw new Error('Unexpected conversation list.');
  if (!after) el('chat-conversations').replaceChildren();
  for (const entry of data.entries) {
    const option = text('option', `${entry.project_path} · ${entry.id}`);
    option.value = entry.id; el('chat-conversations').append(option);
  }
  chatListAfter = data.entries.at(-1)?.id || '';
  el('chat-list-more').hidden = data.entries.length < 20;
  if (!chatId && data.entries.length) chatId = data.entries[0].id;
  el('chat-conversations').value = chatId;
}
async function refreshChat(credential) {
  if (!credential || credential !== token) return;
  const generation = chatGeneration;
  if (chatBusy) return;
  try {
    if (!chatReady) {
      const data = await chatRpc({type: 'conversation_agents'}, credential);
      if (generation !== chatGeneration) return;
      if (data.type !== 'conversation_agents' || !Array.isArray(data.agents)) throw new Error('Unexpected agent list.');
      el('chat-agent').replaceChildren(...data.agents.map(agent => {
        const option = text('option', `${agent.id} · ${agent.vendor} · ${agent.account}`);
        option.value = agent.id; return option;
      }));
      await loadChatList(credential);
      if (generation !== chatGeneration) return;
      chatReady = true;
      el('chat').hidden = false;
      el('chat-send').disabled = data.agents.length === 0;
      if (!data.agents.length) el('chat-status').textContent = 'No conversation agents configured.';
    }
    if (!chatId) return;
    const selected = chatId;
    const data = await chatRpc({type: 'conversation_messages', conversation_id: selected, after: chatAfter}, credential);
    if (generation !== chatGeneration || selected !== chatId) return;
    if (data.type !== 'conversation_messages' || !Array.isArray(data.messages)) throw new Error('Unexpected message history.');
    chatNext = data.next_after;
    el('chat-more').hidden = chatNext == null;
    const history = el('chat-messages');
    const follow = history.children.length === 0 || history.scrollHeight - history.scrollTop - history.clientHeight < 12;
    history.replaceChildren(...data.messages.map(renderChatMessage));
    if (follow) history.scrollTop = history.scrollHeight;
  } catch (error) {
    if (credential === token && generation === chatGeneration) el('chat-status').textContent = error.message;
  }
}
function renderChatMessage(message) {
  const row = text('article', '', 'chat-message');
  const states = {saved: 'Saved, not queued', queued: 'Queued', launch_intent: 'Started or outcome pending', replied: 'Replied', blocked: 'Blocked', unknown: 'Outcome unknown · no automatic retry'};
  row.append(text('strong', message.sender === 'agent' ? message.agent_id : 'You'));
  row.append(text('p', message.body, 'chat-body'));
  row.append(text('small', states[message.status] || message.status || 'Recorded'));
  if (message.sender === 'operator' && message.status === 'saved') {
    const button = text('button', 'Send saved message to selected agent');
    button.addEventListener('click', async () => {
      if (!token || !el('chat-agent').value) return;
      button.disabled = true;
      try {
        await chatRpc({type: 'queue_conversation_turn', message_id: message.id, agent_id: el('chat-agent').value});
        await refreshChat(token);
      } catch (error) { el('chat-status').textContent = error.message; }
      finally { button.disabled = false; }
    });
    row.append(button);
  }
  return row;
}
async function createChat() {
  if (chatBusy || chatPending || !token) return;
  if (!chatCreation && !el('chat-project').value.trim().startsWith('/')) {
    el('chat-status').textContent = 'Enter an absolute project folder path.'; return;
  }
  const credential = token;
  const generation = chatGeneration;
  chatCreation ||= {id: crypto.randomUUID(), project_path: el('chat-project').value.trim()};
  chatBusy = true;
  try {
    await chatRpc({type: 'create_conversation', ...chatCreation}, credential);
    if (generation !== chatGeneration) return;
    chatId = chatCreation.id; chatAfter = 0;
    const option = text('option', `${chatCreation.project_path} · ${chatId}`);
    option.value = chatId; el('chat-conversations').append(option);
    el('chat-conversations').value = chatId;
    chatCreation = null; el('chat-status').textContent = 'Conversation created.';
  } catch (error) { if (credential === token && generation === chatGeneration) el('chat-status').textContent = `${error.message} Retry keeps the same conversation ID.`; }
  finally { if (credential === token && generation === chatGeneration) chatBusy = false; }
  await refreshChat(credential);
}
async function sendChat() {
  if (chatBusy || !token || !chatId || !el('chat-agent').value) return;
  if (!chatPending && !el('chat-input').value.trim()) return;
  const credential = token;
  const generation = chatGeneration;
  chatPending ||= {message_id: crypto.randomUUID(), conversation_id: chatId,
    body: el('chat-input').value, agent_id: el('chat-agent').value};
  const pending = chatPending;
  chatBusy = true; el('chat-input').disabled = true; el('chat-send').disabled = true;
  try {
    await chatRpc({type: 'send_conversation_message', message_id: pending.message_id,
      conversation_id: pending.conversation_id, body: pending.body}, credential);
    if (generation !== chatGeneration) return;
    await chatRpc({type: 'queue_conversation_turn', message_id: pending.message_id, agent_id: pending.agent_id}, credential);
    if (generation !== chatGeneration) return;
    chatPending = null; el('chat-input').value = ''; el('chat-input').disabled = false;
    el('chat-send').textContent = 'Send'; el('chat-status').textContent = 'Queued for the selected agent.';
  } catch (error) {
    if (credential === token && generation === chatGeneration) { el('chat-status').textContent = `${error.message} Retry uses the same message and agent.`; el('chat-send').textContent = 'Retry message'; }
  } finally { if (credential === token && generation === chatGeneration) { chatBusy = false; el('chat-send').disabled = false; } }
  await refreshChat(credential);
}
el('chat-create').addEventListener('click', createChat);
el('chat-send').addEventListener('click', sendChat);
el('chat-conversations').addEventListener('change', () => {
  if (chatPending || chatBusy) { el('chat-conversations').value = chatId; return; }
  chatId = el('chat-conversations').value; chatAfter = 0; chatGeneration++;
  refreshChat(token);
});
el('chat-first').addEventListener('click', () => { chatAfter = 0; chatGeneration++; refreshChat(token); });
el('chat-more').addEventListener('click', () => { if (chatNext != null) { chatAfter = chatNext; chatGeneration++; refreshChat(token); } });
el('chat-list-more').addEventListener('click', () => loadChatList(token, chatListAfter).catch(error => { el('chat-status').textContent = error.message; }));
