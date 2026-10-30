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
test('each message explains its recipients and decision outcome as text',()=>{
  const f=fixture();
  const row=f.run(`renderChatMessage({sender:'agent',agent_id:'lead',body:'{}',status:'replied',
    routing:[{agent_id:'<b>w</b>',kind:'delegated',reason:'delegated_by_supervisor',state:'blocked'},
             {agent_id:'lead',kind:'information',reason:'report_to_supervisor',state:'received'}],
    examination:{outcome:'rejected',detail:'correction round budget exhausted'}})`);
  const list=row.children[3];
  assert.equal(list.children[0].textContent,'Delegated <b>w</b> · delegated by supervisor · blocked');
  assert.equal(list.children[1].textContent,'Informed lead · report to supervisor · received');
  assert.equal(row.children[4].textContent,'Delegation decision refused: correction round budget exhausted');
  const plain=f.run(`renderChatMessage({sender:'operator',body:'work',status:'queued',routing:[]})`);
  assert.equal(plain.children.length,3);
});
test('an authorized decision shows its assignments and a refused one keeps its body',()=>{
  const f=fixture();
  const body=JSON.stringify({schema:'conductor.delegation.v1',assignments:[
    {agent_id:'worker',instruction:'Write <b>fib.py</b>'},{agent_id:'reviewer',instruction:'Check it'}]});
  const row=f.run(`renderChatMessage({sender:'agent',agent_id:'lead',body:${JSON.stringify(body)},status:'replied',
    examination:{outcome:'authorized'}})`);
  const list=row.children[1];
  assert.equal(list.children.length,2);
  assert.equal(list.children[0].textContent,'worker: Write <b>fib.py</b>');
  assert.equal(list.children[1].textContent,'reviewer: Check it');
  const refused=f.run(`renderChatMessage({sender:'agent',agent_id:'lead',body:${JSON.stringify(body)},status:'replied',
    examination:{outcome:'rejected',detail:'not a direct report'}})`);
  assert.equal(refused.children[1].textContent,body);
});
test('retry after queue transport loss preserves message identity and selected agent',async()=>{
  const f=fixture();const calls=[];let fail=true;
  f.run("token='credential';chatId='conversation';chatRoomFor='conversation';chatReady=true");
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
  f.run("token='credential';chatId='conversation';chatRoomFor='conversation';chatReady=true");
  f.context.fetch=()=>new Promise(r=>{resolve=r;});
  const pending=f.run("refreshChat('credential')");
  f.run('disconnect()');
  resolve(ok({type:'conversation_messages',messages:[{sender:'agent',body:'late private reply'}],next_after:null}));
  await pending;
  assert.equal(f.get('chat').hidden,true);assert.equal(f.get('chat-messages').children.length,0);
});
test('shared-room retry preserves all recipients after a lost routing response', async()=>{
  const f=fixture(), calls=[];let fail=true;
  f.run("token='credential';chatId='conversation';chatRoomFor='conversation';chatReady=true;chatRoom={coordinator:'codex',members:[{agent_id:'codex'},{agent_id:'claude'}]}");
  f.get('chat-agent').value='@everyone';f.get('chat-input').value='Coordinate';
  f.context.fetch=async(_,options)=>{
    const p=JSON.parse(options.body).payload;calls.push(p);
    if(p.type==='route_conversation_message'&&fail){fail=false;throw new Error('response lost');}
    if(p.type==='conversation_messages')return ok({type:'conversation_messages',messages:[],next_after:null});
    return ok({type:'accepted'});
  };
  await f.run('sendChat()');f.get('chat-agent').value='claude';await f.run('sendChat()');
  const routes=calls.filter(p=>p.type==='route_conversation_message');
  assert.equal(routes.length,2);assert.deepEqual(routes[0],routes[1]);
  assert.equal(routes[0].everyone,true);assert.deepEqual(routes[0].recipients,[]);
  assert.equal(calls.some(p=>p.type==='queue_conversation_turn'),false);
});
test('room setup transport retry cannot silently change coordinator',async()=>{
  const f=fixture(),configs=[];let fail=true;
  f.run("token='credential';chatId='conversation';chatRoomFor='conversation';chatReady=true;chatAgents=[{id:'codex'},{id:'claude'}]");
  f.get('chat-agent').value='codex';
  f.context.fetch=async(_,options)=>{
    const p=JSON.parse(options.body).payload;
    if(p.type==='configure_conversation_room'){
      configs.push(p.configuration);if(fail){fail=false;throw new Error('response lost');}
    }
    if(p.type==='conversation_room')return ok({type:'conversation_room',configuration:configs[0]});
    if(p.type==='conversation_messages')return ok({type:'conversation_messages',messages:[],next_after:null});
    return ok({type:'accepted'});
  };
  await f.run('configureChatRoom()');f.get('chat-agent').value='claude';
  await f.run('configureChatRoom()');
  assert.equal(configs.length,2);assert.deepEqual(configs[0],configs[1]);
  assert.equal(f.get('chat-agent').value,'@coordinator');
  assert.match(f.get('chat-room-summary').textContent,/claude reports to codex/);
});
