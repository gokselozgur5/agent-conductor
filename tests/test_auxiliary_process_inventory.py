import json
from pathlib import Path
import sqlite3
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import auxiliary_process_inventory as aux
import inspect_runner_processes as inventory


class AuxiliaryInventoryTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('create table auxiliary_invocations(invocation_id,task_id,execution_token,boot_generation,state,release_recorded,preparation_json,launch_binding_json,process_json); create table auxiliary_host_evidence(invocation_id,host_json);')
        self.authority = 'a'*64
        self.invocation = '00000000-0000-4000-8000-000000000001'
        self.digest = 'c'*64
        self.host = dict(platform='linux', host_id='d'*64, boot_id='e'*64)
        self.process = dict(self.host, pid=42, start_marker='linux-start-ticks:123')
        self.prep = dict(invocation_id=self.invocation, task_id='task', execution_token='private-token', boot_generation=1)
        self.tag = f'--conductor-aux-owner=v1:{self.authority}:{self.invocation}:{self.digest}'
        self.command = f'/renamed-runner --auxiliary {self.invocation} {self.tag}'
        self.db.execute('insert into auxiliary_invocations values(?,?,?,?,?,?,?,?,?)', (self.invocation, 'task', 'private-token', 1, 'pending', 1, json.dumps(self.prep), json.dumps(dict(authority_id=self.authority, launch_sha256=self.digest, private='PRIVATE')), json.dumps(self.process)))
        self.db.execute('insert into auxiliary_host_evidence values(?,?)', (self.invocation, json.dumps(dict(self.host, authority_id=self.authority, generation=1))))

    def read(self, observed=(), birth=None, ps=None):
        return aux.inventory(self.db, self.authority, self.host, observed, time.monotonic()+5,
                             birth=birth or (lambda pid: self.process['start_marker']),
                             ps=ps or (lambda *args: (0, self.command)), binding=inventory.process_binding)

    def live(self):
        return [(42, aux.parse_tag(self.command), self.process['start_marker'])]

    def test_tag_is_strict_and_renamed_process_is_discovered(self):
        self.assertEqual(aux.parse_tag(self.command), (self.authority, self.invocation, self.digest))
        self.assertEqual(inventory.discover_runner_pids('', '42 '+self.command), [42])
        for command in [self.command+' '+self.tag, self.command.replace('--auxiliary ', '--bad '), self.command.replace(self.invocation, 'bad', 1), self.command+' --auxiliary bad']:
            self.assertIsNone(aux.parse_tag(command))

    def test_matching_live_and_recorded_inventory_without_private_fields(self):
        for observed in [(), self.live()]:
            result = self.read(observed)
            self.assertEqual(result['invalid_records'], 0)
            self.assertEqual([i['classification'] for i in result['observations']], ['matching_record'])
            for secret in ['PRIVATE', 'private-token', 'preparation_json', 'raw_process', '/renamed-runner']:
                self.assertNotIn(secret, json.dumps(result))

    def test_orphan_and_foreign_authority_are_distinct(self):
        self.db.execute('delete from auxiliary_invocations')
        self.assertEqual(self.read(self.live())['observations'][0]['classification'], 'no_matching_record')
        foreign = [(42, ('b'*64, self.invocation, self.digest), self.process['start_marker'])]
        self.assertEqual(self.read(foreign)['observations'], [])

    def test_pre_ack_and_launch_mismatch(self):
        self.db.execute('update auxiliary_invocations set process_json=NULL,release_recorded=0')
        self.assertTrue(all(i['classification']=='awaiting_ack' for i in self.read(self.live())['observations']))
        wrong = [(42, (self.authority, self.invocation, 'f'*64), self.process['start_marker'])]
        self.assertEqual(self.read(wrong)['observations'][0]['classification'], 'launch_binding_mismatch')

    def test_pending_disappearance_is_not_settlement(self):
        for birth in [lambda _: None, lambda _: 'linux-start-ticks:456']:
            self.assertIn(self.read(birth=birth)['observations'][0]['classification'], ('pending_process_unavailable', 'pending_pid_reused_or_marker_mismatch'))
            self.db.execute("update auxiliary_invocations set state='settled'")
            self.assertIn(self.read(birth=birth)['observations'][0]['classification'], ('historical_process_unavailable', 'historical_pid_reused_or_marker_mismatch'))
            self.db.execute("update auxiliary_invocations set state='pending'")

    def test_tag_loss_and_recorded_birth_race(self):
        self.assertEqual(self.read(ps=lambda *_: (0, 'PRIVATE unrelated'))['observations'][0]['classification'], 'tag_absent_or_mismatch')
        self.assertEqual(self.read(ps=lambda *_: (1, ''))['observations'][0]['classification'], 'arguments_unavailable')
        with mock.patch.object(aux.time, 'monotonic', return_value=1):
            result = self.read(birth=mock.Mock(side_effect=[self.process['start_marker'], None]))
        self.assertEqual(result['observations'][0]['classification'], 'changed_during_observation')
        self.assertEqual(self.read(self.live(), birth=lambda _: None)['observations'][0]['classification'], 'changed_during_observation')

    def test_settled_but_still_observed_is_not_healthy(self):
        self.db.execute("update auxiliary_invocations set state='settled'")
        self.assertEqual(self.read(self.live())['observations'][0]['classification'], 'settled_process_observed')

    def test_malformed_oversized_or_contradictory_records_are_counted(self):
        for column, value in [('process_json', '[]'), ('process_json', 'x'*1025), ('preparation_json', '{}'), ('launch_binding_json', '{}')]:
            self.db.execute('savepoint negative')
            self.db.execute('update auxiliary_invocations set '+column+'=?', (value,))
            self.assertEqual(self.read()['invalid_records'], 1)
            self.db.execute('rollback to negative'); self.db.execute('release negative')
        self.db.execute("update auxiliary_host_evidence set host_json='{}'")
        self.assertEqual(self.read()['invalid_records'], 1)

    def test_legacy_schema_and_inventory_limit_refuse(self):
        self.db.execute('drop table auxiliary_host_evidence')
        self.assertEqual(self.read()['status'], 'legacy_unavailable')
        self.db.execute('create table auxiliary_host_evidence(invocation_id,host_json)')
        self.db.executemany('insert into auxiliary_invocations select * from auxiliary_invocations limit 1', [()]*4096)
        with self.assertRaisesRegex(ValueError, 'too_many_auxiliary_records'):
            self.read()
