"""Bounded auxiliary process observations; no signaling or lifecycle authority."""
import json
import re
import time

UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TAG = re.compile(r'--conductor-aux-owner=v1:([0-9a-f]{64}):(' + UUID + r'):([0-9a-f]{64})\Z')


def parse_tag(command):
    words = command.split()
    candidates = [w for w in words if w.startswith('--conductor-aux-owner=')]
    if len(candidates) != 1 or words.count('--auxiliary') != 1:
        return None
    match = TAG.fullmatch(candidates[0])
    index = words.index('--auxiliary') + 1
    if not match or index >= len(words) or words[index] != match[2]:
        return None
    return match.groups()


def inventory(db, authority, host, observed, deadline, *, birth, ps, binding):
    """Read the same SQLite snapshot as writer observations. Never expose JSON."""
    from inspect_runner_processes import bounded_column
    tables = {r[0] for r in db.execute("select name from sqlite_master where type='table'")}
    if not {'auxiliary_invocations', 'auxiliary_host_evidence'} <= tables:
        return dict(status='legacy_unavailable', invalid_records=0, observations=[])
    columns = [bounded_column('a.invocation_id', 36), bounded_column('a.task_id', 256), bounded_column('a.execution_token', 256), 'a.boot_generation',
               'a.state', 'a.release_recorded']
    columns += [bounded_column(name, limit) for name, limit in [
        ('a.preparation_json', 16384), ('a.launch_binding_json', 16384),
        ('a.process_json', 1024), ('h.host_json', 1024)]]
    rows = db.execute('select '+','.join(columns)+' from auxiliary_invocations a '
                      'left join auxiliary_host_evidence h on h.invocation_id=a.invocation_id limit 4097').fetchall()
    if len(rows) > 4096:
        raise ValueError('too_many_auxiliary_records')
    records = {}
    invalid = 0
    for invocation, task, execution, generation, state, released, raw, launch, process, origin in rows:
        try:
            prep = json.loads(raw)
            if not isinstance(invocation, str) or not re.fullmatch(UUID, invocation) or invocation == '00000000-0000-0000-0000-000000000000':
                raise ValueError()
            if not isinstance(prep, dict) or any(prep.get(k) != v for k, v in [
                    ('invocation_id', invocation), ('task_id', task), ('execution_token', execution),
                    ('boot_generation', generation)]):
                raise ValueError()
            if type(generation) is not int or generation <= 0 or state not in ('pending', 'settled') or released not in (0, 1):
                raise ValueError()
            origin = json.loads(origin)
            if not isinstance(origin, dict) or origin.get('authority_id') != authority or origin.get('generation') != generation:
                raise ValueError()
            if origin.get('platform') not in ('linux', 'macos') or any(not isinstance(origin.get(k), str) or not re.fullmatch('[0-9a-f]{64}', origin[k]) for k in ('host_id', 'boot_id')):
                raise ValueError()
            launch = None if launch is None else json.loads(launch)
            if launch is not None and (not isinstance(launch, dict) or launch.get('authority_id') != authority or not isinstance(launch.get('launch_sha256'), str) or not re.fullmatch('[0-9a-f]{64}', launch['launch_sha256'])):
                raise ValueError()
            value = None if process is None else json.loads(process)
            if value is not None:
                if not isinstance(value, dict) or launch is None:
                    raise ValueError()
                pid, start = value.get('pid'), value.get('start_marker')
                pattern = r'linux-start-ticks:[0-9]{1,20}' if value.get('platform') == 'linux' else r'macos-start-us:[0-9]{1,20}:[0-9]{1,6}'
                if binding(process, pid, start, origin) != 'matching_observation' or not isinstance(start, str) or not re.fullmatch(pattern, start):
                    raise ValueError()
            elif released:
                raise ValueError()
            records[invocation] = dict(state=state, launch=launch, process=value, raw_process=process, origin=origin)
        except (ValueError, TypeError, KeyError):
            invalid += 1
    result = []
    seen = set()
    for pid, tag, start in observed:
        if tag[0] != authority:
            continue
        item = dict(pid=pid, invocation_id=tag[1], start_marker=start)
        record = records.get(tag[1])
        if birth(pid) != start:
            item['classification'] = 'changed_during_observation'
        elif record is None:
            item['classification'] = 'no_matching_record'
        elif record['launch'] is None or record['launch']['launch_sha256'] != tag[2]:
            item['classification'] = 'launch_binding_mismatch'
        elif record['process'] is None:
            item['classification'] = 'awaiting_ack'
        elif binding(record['raw_process'], pid, start, host) != 'matching_observation':
            item['classification'] = 'process_binding_mismatch'
        else:
            item['classification'] = 'settled_process_observed' if record['state'] == 'settled' else 'matching_record'
            seen.add(tag[1])
        result.append(item)
    candidates = [(key, r) for key, r in records.items() if r['process'] is not None]
    if len(candidates) > 128:
        raise ValueError('too_many_auxiliary_candidates')
    for invocation, record in candidates:
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise ValueError('inventory_deadline')
        value = record['process']
        if host is None or any(value[k] != host.get(k) for k in ('platform', 'host_id', 'boot_id')):
            result.append(dict(invocation_id=invocation, classification=('historical_other_host_boot' if host is not None and record['state'] == 'settled' else 'host_boot_unchecked_or_mismatch')))
            continue
        if invocation in seen:
            continue
        pid, start = value['pid'], value['start_marker']
        before = birth(pid)
        if before is None or before != start:
            classification = ('historical_' if record['state'] == 'settled' else 'pending_') + ('process_unavailable' if before is None else 'pid_reused_or_marker_mismatch')
        else:
            code, command = ps(['-ww', '-p', str(pid), '-o', 'args='], min(5, remaining))
            after = birth(pid)
            expected = (authority, invocation, record['launch']['launch_sha256'])
            classification = ('changed_during_observation' if before != after else
                              'arguments_unavailable' if code else
                              'tag_absent_or_mismatch' if parse_tag(command) != expected else
                              'settled_process_observed' if record['state'] == 'settled' else 'matching_record')
        result.append(dict(invocation_id=invocation, pid=pid, start_marker=start, classification=classification))
    for invocation, record in records.items():
        if record['process'] is None and record['launch'] is not None and record['state'] == 'pending':
            result.append(dict(invocation_id=invocation, classification='awaiting_ack'))
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    return dict(status='observed', invalid_records=invalid, observations=result)
