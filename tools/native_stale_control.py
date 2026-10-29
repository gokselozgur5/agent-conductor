"""Explicitly owned synthetic lease control for isolated native acceptance runs."""
import json
from pathlib import Path
import uuid

from native_observation_guard import require


def validate_refusal(before, after, response):
    require(response.get('code')=='conflict' and 'attempt, worker, or epoch is stale' in response.get('message',''), 'stale_control_wrong_refusal')
    require(after==before, 'stale_control_mutated_task')


class NativeStaleControl:
    """Only mutates the fresh synthetic task created by setup, never native work."""
    def __init__(self, request, project_root):
        self.request=request
        self.root=Path(project_root)
        self.prefix='stale-'+uuid.uuid4().hex
        self.worker=self.prefix+':worker'
        self.task_id=self.prefix+':task'
        self.old=None
        self.current=None
        self.samples=0
        self.failed=False

    def task(self):
        return self.request('get-task',self.task_id)['aggregate']

    def command(self, task, kind, role='operator', label='control'):
        return dict(actor=dict(id=self.worker if role=='worker' else 'operator',role=role),
                    expected_version=task['version'],correlation_id=self.prefix,
                    idempotency_key=self.prefix+':'+label+':'+str(task['version']),
                    reason='Owned synthetic stale-lease acceptance control',kind=kind)

    def apply(self, kind, role='operator'):
        task=self.task()
        self.request('apply',self.task_id,json.dumps(self.command(task,kind,role)))

    def setup(self):
        require(self.old is None and not self.failed, 'stale_control_already_initialized')
        try:
            self.root.mkdir(mode=0o700)
            project=self.prefix+':project'
            self.request('register-project','operator',project,project,str(self.root))
            self.request('register-agent','operator',self.worker,self.worker,'synthetic','stale-control-v1','no-execution','read_write')
            self.request('create-task','operator',self.task_id,self.task_id,project,'Synthetic lease control; never dispatch an executable worker')
            self.apply(dict(type='mark_ready'))
            for number in [1,2]:
                attempt=self.prefix+':'+str(number)
                self.apply(dict(type='claim',attempt_id=attempt,worker=self.worker,lease_duration_ms=60000,worker_margin_ms=1000,recovery_grace_ms=1000),'worker')
                claimed=self.task()['attempt']
                self.apply(dict(type='start',attempt_id=attempt,lease_epoch=claimed['lease']['epoch']),'worker')
                if number==1:
                    self.old=self.task()['attempt']
                    self.apply(dict(type='pause'));self.apply(dict(type='resume'))
                else:
                    self.current=self.task()['attempt']
            require(self.current['lease']['epoch']>self.old['lease']['epoch'], 'stale_control_epoch_not_advanced')
        except Exception:
            self.failed=True
            raise

    def sample(self):
        require(not self.failed and self.current is not None, 'stale_control_unavailable')
        try:
            before=self.task()
            require(before['state']=='running' and before['attempt']['id']==self.current['id'] and before['attempt']['lease']['epoch']==self.current['lease']['epoch'], 'stale_control_owner_changed')
            kind=dict(type='heartbeat',attempt_id=self.old['id'],lease_epoch=self.old['lease']['epoch'],lease_duration_ms=60000,worker_margin_ms=1000)
            command=self.command(before,kind,'worker','stale')
            response=self.request('apply',self.task_id,json.dumps(command),ok=False)
            validate_refusal(before,self.task(),response)
            kind.update(attempt_id=self.current['id'],lease_epoch=self.current['lease']['epoch'])
            self.request('apply',self.task_id,json.dumps(self.command(before,kind,'worker','renew')))
            after=self.task()
            require(after['version']==before['version']+1 and after['attempt']['id']==self.current['id'], 'stale_control_renewal_failed')
            self.samples+=1
            return dict(stale_refused=True,aggregate_unchanged_on_refusal=True,valid_renewal_accepted=True,sample=self.samples)
        except Exception:
            self.failed=True
            raise

    def finish(self):
        # Explicit cleanup of only this control's synthetic task.
        self.apply(dict(type='cancel'))
        require(self.task()['state']=='cancelled', 'stale_control_cleanup_failed')
