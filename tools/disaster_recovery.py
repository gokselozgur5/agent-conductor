"""Quarantined same-host recovery after a native kernel boot transition."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import subprocess
import uuid
import operator_backup as snapshot
import operator_restore as publication
import recovery_authority as authority
import recovery_dependencies


def canonical_state(state):
    state = state.expanduser().absolute()
    if '..' in state.parts or state.parent != state.parent.resolve(strict=True) or state.is_symlink():
        raise ValueError('recovery requires the original canonical state path')
    if state.exists():
        snapshot.private_root(state)
    return state


def identity(path):
    info = path.lstat()
    return [info.st_dev, info.st_ino]


def valid_identity(value):
    return isinstance(value, list) and len(value) == 2 and all(type(part) is int and part >= 0 for part in value)


def host_epoch(reader):
    result = subprocess.run([str(reader), '--host-epoch'], capture_output=True, text=True, timeout=12, check=True)
    if len(result.stdout) > 4096:
        raise ValueError('host epoch output exceeds bound')
    return authority.validate_host(json.loads(result.stdout))


def same_host(left, right):
    return all(left[key] == right[key] for key in ('platform', 'host_id'))


def copy_reader(source, target, expected):
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(source_fd, 'rb') as reader:
        if not stat.S_ISREG(os.fstat(reader.fileno()).st_mode):
            raise ValueError('host reader must be a regular executable')
        out = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o700)
        with os.fdopen(out, 'wb') as writer:
            total = 0
            for data in iter(lambda: reader.read(1024 * 1024), b''):
                total += len(data)
                if total > 256 * 1024 * 1024:
                    raise ValueError('host reader exceeds bound')
                writer.write(data)
            writer.flush(); os.fsync(writer.fileno())
    if snapshot.digest(target) != expected:
        raise ValueError('host reader changed during preparation')
    publication.sync_directory(target.parent)
    publication.sync_directory(target.parent.parent)


def read_manifest(source, expected=None):
    snapshot.verify(source)
    digest = snapshot.digest(source / 'manifest.json')
    if expected is not None and digest != expected:
        raise ValueError('prepared snapshot manifest changed')
    return json.loads((source / 'manifest.json').read_text()), digest


def prepared_result(ticket, current):
    return {'prepared': True, 'recovery_id': ticket['id'], 'snapshot': ticket['snapshot'],
            'retained': ticket['retained'], 'daemon_start_blocked': True,
            'host_restart_required': current['boot_id'] == ticket['prepared_host']['boot_id'],
            'phase': ticket['phase']}


def ticket_host(state, ticket):
    reader = authority.root_for(state) / ('recovery-' + ticket['id']) / 'host-reader'
    fd = authority.private_file(reader)
    os.close(fd)
    if snapshot.digest(reader) != ticket['reader_sha256']:
        raise ValueError('prepared host reader changed')
    current = host_epoch(reader)
    if not same_host(ticket['prepared_host'], current):
        raise ValueError('recovery belongs to another host')
    return current


def prepare(state, source, retained):
    state = canonical_state(state)
    source, retained = source.expanduser().absolute(), retained.expanduser().absolute()
    if (retained.parent != state.parent or retained == state or '..' in retained.parts
            or source.is_relative_to(state) or state.is_relative_to(source)
            or source.is_relative_to(retained) or retained.is_relative_to(source)):
        raise ValueError('snapshot and retained state must be disjoint canonical siblings/outside paths')
    with authority.hold_external(state, allow_recovery=True) as record:
        if record['version'] != 2:
            raise ValueError('recovery requires a previously host-bound authority')
        if record['generation_high_water'] >= 9223372036854775806:
            raise ValueError('runtime generation cannot safely advance after recovery')
        manifest, digest = read_manifest(source)
        if manifest['state_root'] != str(state):
            raise ValueError('recovery refuses relocation')
        if record.get('recovery') is not None:
            ticket = validate_ticket(state, record['recovery'])
            if (ticket['snapshot'] != str(source) or ticket['retained'] != str(retained)
                    or ticket['manifest_sha256'] != digest):
                raise ValueError('another recovery is already prepared')
            return prepared_result(ticket, ticket_host(state, ticket))
        if retained.exists() or retained.is_symlink():
            raise ValueError('retained state path already exists')
        snapshot.verify(source, external=True)
        if authority.binding(source / 'state') != record['authority_id']:
            raise ValueError('snapshot belongs to another authority')
        if publication.generation(source / 'state/state.sqlite3') > record['generation_high_water']:
            raise ValueError('authority history is older than the snapshot')
        config = json.loads((source / 'state/operator.json').read_text())
        original_reader = Path(config['bin_dir']) / 'conductor-daemon'
        expected = next((item['sha256'] for item in manifest['dependencies']
                         if item['kind'] == 'file' and item['path'] == str(original_reader)), None)
        if expected is None:
            raise ValueError('snapshot has no pinned native host reader')
        recovery_id = uuid.uuid4().hex
        directory = authority.root_for(state) / ('recovery-' + recovery_id)
        directory.mkdir(mode=0o700)
        copy_reader(original_reader, directory / 'host-reader', expected)
        current = host_epoch(directory / 'host-reader')
        if not same_host(record['host'], current):
            raise ValueError('authority belongs to another host')
        ticket = {'version': 1, 'id': recovery_id, 'snapshot': str(source), 'manifest_sha256': digest,
                  'retained': str(retained), 'prepared_host': current, 'reader_sha256': expected,
                  'generation_floor': record['generation_high_water'], 'phase': 'prepared',
                  'stage': str(state.parent / (state.name + '.recover-' + recovery_id)),
                  'original_identity': identity(state) if state.exists() else None}
        record['recovery'] = ticket
        record['completed_recovery'] = None
        authority.persist(state, record)
        return prepared_result(ticket, current)


def validate_ticket(state, ticket):
    if (not isinstance(ticket, dict) or ticket.get('version') != 1
            or ticket.get('phase') not in ('prepared', 'staged')
            or not isinstance(ticket.get('id'), str) or len(ticket['id']) != 32
            or any(c not in '0123456789abcdef' for c in ticket['id'])):
        raise ValueError('invalid prepared recovery ticket')
    if (type(ticket.get('generation_floor')) is not int or not 1 <= ticket['generation_floor'] < 9223372036854775806
            or any(not isinstance(ticket.get(key), str) or len(ticket[key]) != 64
                   or any(c not in '0123456789abcdef' for c in ticket[key]) for key in ('manifest_sha256', 'reader_sha256'))
            or ticket.get('original_identity') is not None and not valid_identity(ticket['original_identity'])
            or ticket['phase'] == 'staged' and (not valid_identity(ticket.get('staged_identity'))
                or not isinstance(ticket.get('staged_sha256'), str) or len(ticket['staged_sha256']) != 64)):
        raise ValueError('invalid recovery publication evidence')
    authority.validate_host(ticket['prepared_host'])
    retained, stage, source = map(Path, (ticket['retained'], ticket['stage'], ticket['snapshot']))
    if (not source.is_absolute() or '..' in source.parts
            or retained.parent != state.parent or stage.parent != state.parent or retained == state
            or stage in (retained, state) or not stage.name.startswith(state.name + '.recover-')
            or source.is_relative_to(state) or state.is_relative_to(source)
            or source.is_relative_to(retained) or retained.is_relative_to(source)):
        raise ValueError('invalid recovery publication paths')
    return ticket


def tree_digest(path):
    return hashlib.sha256(json.dumps(snapshot.inventory(path), sort_keys=True).encode()).hexdigest()


def verify_external_at(manifest, state, candidate):
    dependencies = []
    for original in manifest['dependencies']:
        item = dict(original)
        path = Path(item['path'])
        if item['kind'] == 'file' and path.is_relative_to(state):
            item['path'] = str(candidate / path.relative_to(state))
        dependencies.append(item)
    recovery_dependencies.verify(dependencies, snapshot.digest)


def stage_payload(state, source, ticket, record, manifest, current):
    stage = Path(ticket['stage'])
    if stage.exists() or stage.is_symlink():
        # Preserve an incomplete candidate; never guess that a partial copy is valid.
        stage = state.parent / (state.name + '.recover-' + uuid.uuid4().hex)
        ticket['stage'] = str(stage)
        authority.persist(state, record)
    snapshot.verify(source, external=True)
    stage.mkdir(mode=0o700)
    publication.populate_stage(state, source, stage, manifest)
    with closing(sqlite3.connect(stage / 'state.sqlite3')) as db:
        db.execute('update runtime_metadata set boot_generation=?', (record['generation_high_water'],))
        db.commit()
    with (stage / 'board-token').open('w') as stream:
        stream.write(secrets.token_urlsafe(32) + '\n'); stream.flush(); os.fsync(stream.fileno())
    (stage / 'board-token').chmod(0o600)
    (stage / 'runtime.lock').touch(mode=0o600)
    with (stage / 'runtime.lock').open('rb') as lock: os.fsync(lock.fileno())
    receipt = {'recovery_id': ticket['id'], 'snapshot_manifest_sha256': ticket['manifest_sha256'],
               'prepared_host': ticket['prepared_host'], 'recovered_host': current,
               'generation_floor': record['generation_high_water'], 'token_rotated': True, 'dispatch_started': False}
    publication.write_record(stage / ('recovery-' + ticket['id'] + '.json'), receipt)
    snapshot.database(stage / 'state.sqlite3', state)
    snapshot.flush_directories(stage)
    publication.sync_directory(stage.parent)
    ticket.update(phase='staged', staged_identity=identity(stage), staged_sha256=tree_digest(stage))
    authority.persist(state, record)


def publish(state, ticket, manifest):
    stage, retained = Path(ticket['stage']), Path(ticket['retained'])
    published = state.exists() and identity(state) == ticket['staged_identity']
    candidate = state if published else stage
    snapshot.private_root(candidate)
    if identity(candidate) != ticket['staged_identity'] or tree_digest(candidate) != ticket['staged_sha256']:
        raise ValueError('prepared recovery candidate changed')
    verify_external_at(manifest, state, candidate)
    original = ticket['original_identity']
    if original is not None:
        old = retained if published or not state.exists() else state
        snapshot.private_root(old)
        if identity(old) != original:
            raise ValueError('retained original directory changed')
        if old == state:
            if retained.exists() or retained.is_symlink():
                raise ValueError('retained destination became occupied')
            marker = old / 'ARCHIVED_STATE.json'
            expected = {'version': 3, 'dispatch_forbidden': True, 'recovery_id': ticket['id'], 'restored_to': str(state)}
            if marker.exists():
                from restore_publication import read_record
                existing = read_record(marker)
                if not isinstance(existing, dict) or existing.get('dispatch_forbidden') is not True:
                    raise ValueError('original archive marker changed')
            else:
                publication.write_record(marker, expected)
            os.rename(state, retained)
            publication.sync_directory(state.parent)
    elif not published and (state.exists() or state.is_symlink()):
        raise ValueError('missing original path became occupied')
    if not published:
        if state.exists() or state.is_symlink():
            raise ValueError('recovery publication destination became occupied')
        os.rename(stage, state)
        publication.sync_directory(state.parent)
    snapshot.database(state / 'state.sqlite3', state)
    return {'recovered': True, 'recovery_id': ticket['id'], 'state': str(state),
            'retained': str(retained) if original is not None else None,
            'state_identity': identity(state), 'dispatch_started': False}


def finish(state):
    state = canonical_state(state)
    with authority.hold_external(state, allow_recovery=True) as record:
        if record.get('recovery') is None:
            previous = record.get('completed_recovery')
            if (not isinstance(previous, dict) or previous.get('recovered') is not True
                    or previous.get('dispatch_started') is not False or previous.get('state') != str(state)
                    or not valid_identity(previous.get('state_identity'))
                    or not state.exists() or identity(state) != previous['state_identity']):
                raise ValueError('no prepared recovery; prepare a new request')
            return dict(previous, already_completed=True)
        ticket = validate_ticket(state, record['recovery'])
        current = ticket_host(state, ticket)
        if current['boot_id'] == ticket['prepared_host']['boot_id']:
            raise ValueError('host restart required; daemon restart is not a writer fence')
        if record['generation_high_water'] < ticket['generation_floor']:
            raise ValueError('authority generation history moved backward')
        if record['generation_high_water'] >= 9223372036854775806:
            raise ValueError('runtime generation cannot safely advance after recovery')
        source = Path(ticket['snapshot'])
        manifest, _ = read_manifest(source, ticket['manifest_sha256'])
        if ticket['phase'] == 'prepared':
            present = identity(state) if state.exists() else None
            if present != ticket['original_identity']:
                raise ValueError('original directory changed since preparation')
            stage_payload(state, source, ticket, record, manifest, current)
        result = publish(state, ticket, manifest)
        record.update(host=current, recovery=None, completed_recovery=result)
        authority.persist(state, record)
        return result


def cancel(state):
    state = canonical_state(state)
    with authority.hold_external(state, allow_recovery=True) as record:
        ticket = validate_ticket(state, record.get('recovery'))
        if ticket['phase'] != 'prepared':
            raise ValueError('publication has been staged; finish recovery instead')
        ticket_host(state, ticket)
        record['recovery'] = None
        authority.persist(state, record)
        return {'cancelled': True, 'recovery_id': ticket['id'], 'state_changed': False, 'dispatch_started': False}
