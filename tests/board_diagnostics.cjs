'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
function fixture() {
  const nodes = new Map();
  const node = () => ({hidden:false, textContent:'', children:[], value:'',
    addEventListener(){}, replaceChildren(...children){this.children=children;},
    append(...children){this.children.push(...children);},
    set innerHTML(_){throw new Error('HTML injection');}});
  const get = id => {if(!nodes.has(id))nodes.set(id,node());return nodes.get(id);};
  const context=vm.createContext({document:{getElementById:get,createElement:node},
    clearInterval(){},AbortSignal,fetch:async()=>{throw new Error('offline');}});
  vm.runInContext(readFileSync(path.join(__dirname,'../crates/daemon/src/board/app.js'),'utf8'),context);
  return {context,get,run:code=>vm.runInContext(code,context)};
}
test('diagnostics render text and bounded identifiers without raw context',()=>{
  const f=fixture();
  f.run(`renderDiagnostics({violations:[{code:'auxiliary_recovery_pending',summary:'<img onerror=bad()>',context:{count:130,secret:'DO_NOT_RENDER',invocations:Array.from({length:130},()=>({task_id:'<script>bad</script>',invocation_id:'id',generation:1}))}}]})`);
  assert.equal(f.get('diagnostics').hidden,false);
  const row=f.get('diagnostics-entries').children[0];
  assert.match(row.textContent,/<img/);
  assert.equal(row.children[1].children.length,128);
  assert.match(row.children[1].children[0].textContent,/<script>/);
  assert.doesNotMatch(JSON.stringify(row),/DO_NOT_RENDER/);
  f.run('renderDiagnostics({violations:[]})');
  assert.equal(f.get('diagnostics').hidden,true);
  assert.equal(f.get('diagnostics-entries').children.length,0);
});
test('offline refresh clears stale diagnostics and disconnect discards late replies',async()=>{
  const f=fixture();
  f.run("token='current'; renderDiagnostics({violations:[{code:'old',summary:'stale'}]})");
  await f.run("refreshDiagnostics('current')");
  assert.equal(f.get('diagnostics-entries').children.length,0);
  assert.match(f.get('diagnostics-message').textContent,/unavailable/);
  let resolve;
  f.context.fetch=()=>new Promise(r=>{resolve=r;});
  const pending=f.run("refreshDiagnostics('current')");
  f.run('disconnect()');
  resolve({ok:true,json:async()=>({type:'invariants',violations:[{code:'late',summary:'stale'}]})});
  await pending;
  assert.equal(f.get('diagnostics').hidden,true);
  assert.equal(f.get('diagnostics-entries').children.length,0);
});
