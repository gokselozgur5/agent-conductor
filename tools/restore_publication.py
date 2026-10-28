"""Repair a recorded restore publication using surviving directory/lock identities.

This is not recovery from an absent authoritative database or an arbitrary archive.
All paths belong to a trusted local operator; records are not signed attestations.
"""
import fcntl
import json
import os
from pathlib import Path
import stat
import operator_backup as snapshot
import recovery_authority
from operator_restore import sync_directory, write_record

MARKER = 'ARCHIVED_STATE.json'


def identity(path):
    info = path.lstat()
    return [info.st_dev, info.st_ino]


def read_record(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > 16384):
            raise ValueError('invalid private publication record')
        return json.loads(stream.read(16385))


def repair(state, retained):
    state, retained = state.expanduser().absolute(), retained.expanduser().absolute()
    if (state.parent != state.parent.resolve(strict=True) or retained.parent != state.parent
            or state == retained or '..' in state.parts or '..' in retained.parts):
        raise ValueError('repair requires canonical sibling paths')
    # Before the first rename (or after repair's rename), the original is at state.
    original = state if (state / MARKER).exists() else retained
    snapshot.private_root(original)
    record = read_record(original / MARKER)
    if (record.get('version') != 2 or record.get('restored_to') != str(state)
            or record.get('retained') != str(retained) or record.get('dispatch_forbidden') is not True):
        raise ValueError('publication record does not match this repair')
    stage = Path(record['staged'])
    if (stage.parent != state.parent or stage in (state, retained)
            or not stage.name.startswith(state.name + '.restore-')):
        raise ValueError('invalid recorded staging path')
    if identity(original) != record['original_identity']:
        raise ValueError('original directory was replaced')
    published = original == retained and state.exists()
    replacement = state if published else stage
    snapshot.private_root(replacement)
    if identity(replacement) != record['staged_identity']:
        raise ValueError('staged directory was replaced')
    if published and stage.exists():
        raise ValueError('unexpected staging path after publication')
    # A dangling symlink must never be mistaken for an absent destination.
    if state.is_symlink() or retained.is_symlink():
        raise ValueError('repair refuses linked state paths')
    lock_path = (state if published else original) / 'runtime.lock'
    fd = os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as lock, recovery_authority.hold(original, logical_root=state):
        info = os.fstat(lock.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or [info.st_dev, info.st_ino] != record['lock_identity']):
            raise ValueError('runtime lock was replaced')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        extra = (original if published else replacement) / 'runtime.lock'
        extra_exists = extra.exists() or extra.is_symlink()
        if extra_exists and (extra.is_symlink() or identity(extra) != record['lock_identity']):
            raise ValueError('publication lock links disagree')
        if os.fstat(lock.fileno()).st_nlink != (2 if extra_exists else 1):
            raise ValueError('unexpected publication lock link count')
        # Recheck all evidence after acquiring ownership, before any mutations.
        if (read_record(original / MARKER) != record
                or identity(original) != record['original_identity']
                or identity(replacement) != record['staged_identity']):
            raise ValueError('publication evidence changed')
        snapshot.database(original / 'state.sqlite3', state)
        if published:
            snapshot.database(state / 'state.sqlite3', state)
        else:
            # Preserve the abandoned candidate and prevent launching it under its
            # temporary path. Persist the guard before releasing its lock link.
            candidate_guard = replacement / MARKER
            guard = {'dispatch_forbidden': True, 'abandoned_restore_for': str(state)}
            if candidate_guard.exists():
                if read_record(candidate_guard) != guard:
                    raise ValueError('unexpected staging archive marker')
            else:
                write_record(candidate_guard, guard)
        if extra_exists:
            extra.unlink()
            sync_directory(extra.parent)
        if not published:
            if original != state:
                if state.exists() or state.is_symlink():
                    raise ValueError('original state destination became occupied')
                os.rename(original, state)
                sync_directory(state.parent)
            (state / MARKER).unlink()
            sync_directory(state)
        sync_directory(state.parent)
    return {'repaired': True, 'outcome': 'published_restore_retained' if published else 'original_state_restored',
            'state': str(state), 'preserved_directory': str(retained if published else stage),
            'dispatch_started': False}
