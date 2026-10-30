'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {randomUUID} = require('node:crypto');
const vm = require('node:vm');
const path = require('node:path');
function fixture() {
  const nodes = new Map();
  const node = () => ({hidden:false,textContent:'',children:[],value:'',disabled:false,
    addEventListener(){},replaceChildren(...children){this.children=children;},append(...children){this.children.push(...children);},
    set innerHTML(_){throw new Error('HTML injection');}});
  const get = id => {if(!nodes.has(id)) nodes.set(id,node());return nodes.get(id);};
  const context = vm.createContext({document:{getElementById:get,createElement:node},clearInterval(){},AbortSignal,crypto:{randomUUID},fetch:async()=>{throw new Error('offline');}});
  for (const file of ['app.js','chat.js']) vm.runInContext(readFileSync(path.join(__dirname,'../crates/daemon/src/board',file),'utf8'),context);
  return {context,get,run:code=>vm.runInContext(code,context)};
}
const ok = payload => ({ok:true,json:async()=>({result:{status:'ok',payload}})});
test('native reply text is never interpreted as markup or task completion',()=>{
  const f=fixture();
  const row=f.run(`renderChatMessage({sender:'agent',agent_id:'claude',body:'<img src=x onerror=steal()>',status:'replied'})`);
  assert.equal(row.children[1].textContent,'<img src=x onerror=steal()>');
  assert.equal(row.children[2].textContent,'Replied');
  const unknown=f.run(`renderChatMessage({sender:'operator',body:'work',status:'unknown'})`);
  assert.equal(unknown.children.length,3);
  assert.match(unknown.children[2].textContent,/no automatic retry/);
});
test('retry after queue transport loss preserves message identity and selected agent',async()=>{
  const f=fixture();const calls=[];let fail=true;
  f.run("token='credential';chatId='conversation';chatReady=true");
  f.get('chat-agent').value='codex';f.get('chat-input').value='Please work';
  f.context.fetch=async(_,options)=>{
    const p=JSON.parse(options.body).payload;calls.push(p);
    if(p.type==='queue_conversation_turn'&&fail){fail=false;throw new Error('transport lost');}
    if(p.type==='conversation_messages')return ok({type:'conversation_messages',messages:[],next_after:null});
    return ok({type:'accepted'});
  };
  await f.run('sendChat()');
  assert.equal(f.get('chat-send').textContent,'Retry message');
  f.get('chat-agent').value='claude';
  await f.run('sendChat()');
  const writes=calls.filter(p=>p.type==='send_conversation_message');
  const queues=calls.filter(p=>p.type==='queue_conversation_turn');
  assert.equal(writes.length,2);assert.equal(writes[0].message_id,writes[1].message_id);
  assert.equal(queues[0].agent_id,'codex');assert.equal(queues[1].agent_id,'codex');
  assert.equal(f.get('chat-input').value,'');
});
test('disconnect discards a late history response and clears displayed conversation',async()=>{
  const f=fixture();let resolve;
  f.run("token='credential';chatId='conversation';chatReady=true");
  f.context.fetch=()=>new Promise(r=>{resolve=r;});
  const pending=f.run("refreshChat('credential')");
  f.run('disconnect()');
  resolve(ok({type:'conversation_messages',messages:[{sender:'agent',body:'late private reply'}],next_after:null}));
  await pending;
  assert.equal(f.get('chat').hidden,true);assert.equal(f.get('chat-messages').children.length,0);
});
