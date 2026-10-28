"""Independent runtime-generation authority; never infer orphan fencing here."""
from contextlib import closing, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import uuid


def root_for(state):
    database = state / 'state.sqlite3'
    return state.parent / ('.conductor-authority-' + hashlib.sha256(os.fsencode(database)).hexdigest())


def private_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or info.st_mode & 0o077):
            raise ValueError('invalid private authority file')
        return fd
    except BaseException:
        os.close(fd)
        raise


def validate_host(host):
    if (not isinstance(host, dict) or set(host) != {'platform', 'host_id', 'boot_id'}
            or host['platform'] not in ('linux', 'macos')
            or any(not isinstance(host[key], str) or len(host[key]) != 64
                   or any(c not in '0123456789abcdef' for c in host[key]) for key in ('host_id', 'boot_id'))):
        raise ValueError('invalid native host epoch')
    return host


def read(state):
    root = root_for(state)
    info = root.lstat()
    if (root != root.resolve(strict=True) or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise ValueError('invalid recovery authority root')
    with os.fdopen(private_file(root / 'authority.json'), 'rb') as stream:
        data = stream.read(16385)
    if len(data) > 16384:
        raise ValueError('authority record exceeds bound')
    record = json.loads(data)
    if (not isinstance(record, dict)
            or set(record) - {'version', 'database', 'authority_id', 'generation_high_water', 'host', 'recovery', 'completed_recovery'}
            or record.get('version') not in (1, 2) or record.get('database') != str(state / 'state.sqlite3')
            or not isinstance(record.get('authority_id'), str) or len(record['authority_id']) != 64
            or any(c not in '0123456789abcdef' for c in record['authority_id'])
            or type(record.get('generation_high_water')) is not int
            or not 1 <= record['generation_high_water'] <= 9223372036854775807):
        raise ValueError('recovery authority does not match the database')
    if record['version'] == 2:
        validate_host(record.get('host'))
    elif any(key in record for key in ('host', 'recovery', 'completed_recovery')):
        raise ValueError('legacy authority contains unsupported recovery fields')
    return record


def inspect(state, authority_id):
    record = read(state)
    if record['authority_id'] != authority_id:
        raise ValueError('recovery authority identity changed')
    root = root_for(state)
    info = root.stat()
    with os.fdopen(private_file(root / 'ownership.lock'), 'rb') as lock:
        lock_info = os.fstat(lock.fileno())
    result = {'kind': 'authority', 'path': str(root), 'state_root': str(state),
              'identity': [info.st_dev, info.st_ino], 'lock_identity': [lock_info.st_dev, lock_info.st_ino],
              'authority_id': authority_id}
    if record['version'] == 2:
        result.update(authority_version=2, platform=record['host']['platform'], host_id=record['host']['host_id'])
    return result


def persist(state, record):
    """Caller holds ownership.lock; never roll back the independent high-water mark."""
    before = read(state)
    if (record['authority_id'] != before['authority_id'] or record['database'] != before['database']
            or record['generation_high_water'] < before['generation_high_water'] or record['version'] != 2):
        raise ValueError('invalid authority update')
    validate_host(record['host'])
    if before['version'] == 2 and any(record['host'][key] != before['host'][key] for key in ('platform', 'host_id')):
        raise ValueError('authority belongs to another host')
    data = json.dumps(record, sort_keys=True, indent=2).encode() + b'\n'
    if len(data) > 16384:
        raise ValueError('authority record exceeds bound')
    root = root_for(state)
    temporary = root / ('pending-' + uuid.uuid4().hex + '.json')
    out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(out, 'wb') as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, root / 'authority.json')
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)


def binding(state):
    path = state / 'state.sqlite3'
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
        raise ValueError('invalid authority database')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        rows = db.execute('select authority_id from recovery_authority where singleton=1').fetchall()
    if len(rows) != 1:
        raise ValueError('missing database authority binding')
    return rows[0][0]


@contextmanager
def hold_external(state, allow_recovery=False):
    record = read(state)
    before = inspect(state, record['authority_id'])
    with os.fdopen(private_file(Path(before['path']) / 'ownership.lock'), 'rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held = os.fstat(lock.fileno())
        if [held.st_dev, held.st_ino] != before['lock_identity'] or inspect(state, record['authority_id']) != before:
            raise ValueError('recovery authority changed while locking')
        record = read(state)
        if record.get('recovery') is not None and not allow_recovery:
            raise ValueError('runtime quarantined for prepared recovery')
        yield record


@contextmanager
def hold(state, logical_root=None):
    authority_id = binding(state)
    if authority_id is None:
        yield
        return
    with hold_external(logical_root or state) as record:
        if record['authority_id'] != authority_id:
            raise ValueError('database recovery authority binding changed')
        yield
