"""Actual wrapper escape counterexample; not native writer admission."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))


@unittest.skipUnless(os.environ.get('CONDUCTOR_TEST_BIN_DIR'), 'operator binaries required')
class EscapedRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def test_group_stop_does_not_hide_an_escaped_writer(self):
        from pathlib import Path
        import subprocess,json,sys,time,select,sqlite3,hashlib
        root=Path(self.temp.name);workspace=root/'work';workspace.mkdir()

        from native_observation_guard import NativeObservationGuard,Violation
        from disaster_recovery import host_epoch
        from process_birth import observe
        bins=Path(os.environ['CONDUCTOR_TEST_BIN_DIR']);gate=root/'release-write';ready=root/'ready.json';done=root/'done'
        child_code="import os,sys,time,json\nfrom pathlib import Path\nready,gate,target,done=map(Path,sys.argv[1:])\nready.write_text(json.dumps(dict(pid=os.getpid(),pgid=os.getpgrp(),parent=os.getppid())))\ndeadline=time.monotonic()+8\nwhile time.monotonic()<deadline:\n if gate.exists():\n  target.write_text('written after wrapper stopped');done.write_text('done');break\n time.sleep(.01)\n"
        (root/'escaped.py').write_text(child_code)
        leader_code="import subprocess,sys,time,os;from pathlib import Path;Path(sys.argv[1]).write_text(str(os.getpid()));subprocess.Popen(sys.argv[2:],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True);time.sleep(20)"
        packet=dict(contract=dict(version=1,task_id='task',attempt_id='attempt',lease_epoch=1,worker_id='fake',adapter=dict(kind='fake',version='v1',executable_sha256='a'*64,conformance_sha256='b'*64),workspace=str(workspace),starting_head='c'*40,authority=dict(read_roots=[str(workspace)],write_roots=[str(workspace)],network='denied',allow_workspace_commands=True),instructions=[],checks=[],max_runtime_ms=5000),objective='Owned escaping-child acceptance control',checkpoint=None,untrusted_context=[])
        launch=dict(packet=packet,executable=sys.executable,arguments=['-c',leader_code,str(root/'leader.pid'),sys.executable,str(root/'escaped.py'),str(ready),str(gate),str(workspace/'escaped-write'),str(done)],environment={},output_limit=1024,heartbeat_timeout_ms=1000)
        runner=subprocess.Popen([str(bins/'conductor-runner')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
        child_pid=None;birth=None

        def send(value):runner.stdin.write(json.dumps(value)+'\n');runner.stdin.flush()
        def event():
         assert select.select([runner.stdout],[],[],4)[0],'runner event deadline'
         return json.loads(runner.stdout.readline())
        try:
         send(launch);ack=event();assert ack['type']=='ready';digest=ack['acknowledgement']['packet_sha256']
         send(dict(type='release',packet_sha256=digest,continuous_deadline_ms=2**62))
         deadline=time.monotonic()+3
         while not ready.exists():
          assert time.monotonic()<deadline
          send(dict(type='renew',packet_sha256=digest,continuous_deadline_ms=2**62));time.sleep(.02)
         identity=json.loads(ready.read_text());child_pid=identity['pid'];birth=observe(child_pid);assert birth
         leader=int((root/'leader.pid').read_text());assert identity['pgid']==child_pid and identity['pgid']!=leader
         send(dict(type='cancel'));terminal=event();assert terminal['type']=='stopped' and terminal['reason']=='cancelled'
         assert runner.wait(timeout=4)==0
         assert observe(child_pid)==birth and not (workspace/'escaped-write').exists()
         database=root/'state.sqlite3'
         with sqlite3.connect(database) as db:
          db.executescript('create table recovery_authority(singleton,authority_id);create table runtime_metadata(singleton,boot_generation);create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256,process_json,expected_wrapper_identity);create table leases(task_id,status);')
          db.execute('insert into recovery_authority values(1,?)',('a'*64,));db.execute('insert into runtime_metadata values(1,1)')
         guard=NativeObservationGuard(database,host_epoch(bins/'conductor-daemon'),[workspace])
         assert guard.sample({'healthy':True})['healthy']
         gate.write_text('go');deadline=time.monotonic()+3
         while not done.exists():assert time.monotonic()<deadline;time.sleep(.01)
         assert (workspace/'escaped-write').read_text()=='written after wrapper stopped'
         try:guard.sample({'healthy':True});raise AssertionError('escaped write missed')
         except Violation as error:assert str(error)=='protected_surface_changed'
         receipt=dict(actual_runner_cancelled=True,escaped_new_session=True,escaped_alive_after_wrapper_exit=True,post_stop_write_observed=True,protected_surface_guard_failed=True,models_invoked=False,runner_sha256=hashlib.sha256((bins/'conductor-runner').read_bytes()).hexdigest(),limits='Controlled fake-adapter launch through actual runner, not native vendor admission. Shows process-group stop is not escaped-descendant fencing; scoped protected-surface detection uses synthetic DB/invariant input.')
        finally:
         if runner.poll() is None:runner.kill();runner.wait(timeout=4)
         runner.stdin.close();runner.stdout.close()
         if child_pid is not None:
          deadline=time.monotonic()+10
          while observe(child_pid)==birth:
           state=subprocess.run(['/bin/ps','-p',str(child_pid),'-o','state='],capture_output=True,text=True,timeout=2)
           if state.returncode or state.stdout.strip().startswith('Z'):break
           assert time.monotonic()<deadline,'owned bounded child did not settle';time.sleep(.02)
        receipt['owned_children_settled']=True
        self.assertTrue(receipt['post_stop_write_observed'])
        self.assertTrue(receipt['protected_surface_guard_failed'])


if __name__=='__main__':unittest.main()
