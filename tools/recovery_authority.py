"""Independent runtime-generation authority; never infer orphan fencing here."""
from contextlib import closing, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat


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


def inspect(state, authority_id):
    root = root_for(state)
    info = root.lstat()
    if (root != root.resolve(strict=True) or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise ValueError('invalid recovery authority root')
    with os.fdopen(private_file(root / 'authority.json'), 'rb') as stream:
        record = json.loads(stream.read(16385))
        if stream.tell() > 16384:
            raise ValueError('authority record exceeds bound')
    if (record.get('version') != 1 or record.get('database') != str(state / 'state.sqlite3')
            or record.get('authority_id') != authority_id
            or type(record.get('generation_high_water')) is not int
            or not 1 <= record['generation_high_water'] <= 9223372036854775807):
        raise ValueError('recovery authority does not match the database')
    with os.fdopen(private_file(root / 'ownership.lock'), 'rb') as lock:
        lock_info = os.fstat(lock.fileno())
    return {'kind': 'authority', 'path': str(root), 'state_root': str(state),
            'identity': [info.st_dev, info.st_ino], 'lock_identity': [lock_info.st_dev, lock_info.st_ino],
            'authority_id': authority_id}


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
def hold(state, logical_root=None):
    authority_id = binding(state)
    if authority_id is None:
        yield
        return
    logical_root = logical_root or state
    before = inspect(logical_root, authority_id)
    with os.fdopen(private_file(Path(before['path']) / 'ownership.lock'), 'rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held = os.fstat(lock.fileno())
        if [held.st_dev, held.st_ino] != before['lock_identity'] or inspect(logical_root, authority_id) != before:
            raise ValueError('recovery authority changed while locking')
        yield
