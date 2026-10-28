"""Same-path rollback with a retained current state and continuous lock ownership."""
from contextlib import closing
import fcntl
import json
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import time
import uuid
import operator_backup as snapshot
import recovery_dependencies
import recovery_authority


def generation(path):
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        rows = db.execute('select boot_generation from runtime_metadata').fetchall()
        if len(rows) != 1 or not isinstance(rows[0][0], int) or rows[0][0] < 0:
            raise ValueError('invalid runtime generation')
        return rows[0][0]


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_record(path, value):
    """Publish complete private JSON while the caller holds the runtime lock."""
    if path.exists() or path.is_symlink():
        raise ValueError('publication record already exists')
    temporary = path.with_name(path.name + '.pending-' + uuid.uuid4().hex)
    out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(out, 'w') as stream:
        inode = os.fstat(stream.fileno()).st_ino
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.rename(temporary, path)
    sync_directory(path.parent)
    return inode


def restore(state, source, retained):
    with recovery_authority.hold(state):
        return _restore(state, source, retained)


def _restore(state, source, retained):
    snapshot.private_root(state)
    source = source.expanduser().absolute()
    retained = retained.expanduser().absolute()
    if retained.parent != state.parent or retained.exists() or retained.is_symlink():
        raise ValueError('retained state must be a new sibling directory')
    if source.is_relative_to(state) or state.is_relative_to(source) or retained.is_relative_to(source) or source == retained:
        raise ValueError('snapshot must be outside the runtime and retained state')
    snapshot.verify(source, external=True)
    manifest_bytes = (source / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest['state_root'] != str(state):
        raise ValueError('restore refuses relocation or another runtime root')
    fd = os.open(state / 'runtime.lock', os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError('invalid current runtime lock')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A missing/corrupt current database cannot prove absence of orphan writes.
        snapshot.database(state / 'state.sqlite3', state)
        high = max(generation(state / 'state.sqlite3'), generation(source / 'state/state.sqlite3'))
        if high >= 9223372036854775806:
            raise ValueError('runtime generation cannot safely advance')
        stage = state.parent / (state.name + '.restore-' + uuid.uuid4().hex)
        stage.mkdir(mode=0o700)
        snapshot.copy_inventory(source / 'state', stage, manifest['entries'])
        if snapshot.inventory(stage) != manifest['entries']:
            raise ValueError('staged restore differs from the verified snapshot')
        # Preserve excluded executables still located beneath this runtime root.
        # Registered project directories inside state cannot retain their inode
        # identity through copying, so that unsupported layout is refused.
        for item in manifest['dependencies']:
            path = Path(item['path'])
            if path.is_relative_to(state):
                relative = path.relative_to(state)
                if item['kind'] == 'directory':
                    raise ValueError('external directory dependency overlaps restored state')
                target = stage / relative
                if not target.exists():
                    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    record = snapshot.inventory(path.parent).get(path.name)
                    if not record or record['kind'] != 'file':
                        raise ValueError('required retained executable unavailable')
                    snapshot.copy_inventory(path.parent, target.parent, {path.name: record})
        with closing(sqlite3.connect(stage / 'state.sqlite3')) as db:
            db.execute('update runtime_metadata set boot_generation=?', (high,))
            db.commit()
        # A restored historical API token must not revive old browser sessions.
        with (stage / 'board-token').open('w') as token:
            token.write(secrets.token_urlsafe(32) + '\n')
            token.flush()
            os.fsync(token.fileno())
        (stage / 'board-token').chmod(0o600)
        receipt = {'version': 1, 'restored_at_ms': int(time.time() * 1000), 'snapshot': str(source), 'retained_state': str(retained),
                   'generation_floor': high, 'token_rotated': True,
                   'dispatch_started': False, 'snapshot_manifest_sha256': manifest_digest}
        marker = stage / ('restoration-' + uuid.uuid4().hex + '.json')
        out = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(out, 'w') as stream:
            json.dump(receipt, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        snapshot.database(stage / 'state.sqlite3', state)
        snapshot.verify(source, external=True)
        recovery_dependencies.verify(manifest['dependencies'], snapshot.digest)
        if snapshot.digest(source / 'manifest.json') != manifest_digest:
            raise ValueError('snapshot manifest changed during restore')
        # Link the same lock inode into the new tree: no replacement lock can
        # admit a concurrent daemon during the directory switch.
        linked = False
        reserved = False
        published = False
        archive_marker = state / 'ARCHIVED_STATE.json'
        marker_inode = None
        try:
            retained.mkdir(mode=0o700)
            reserved = True
            marker_inode = write_record(archive_marker, {
                'version': 2, 'restored_to': str(state), 'dispatch_forbidden': True,
                'retained': str(retained), 'staged': str(stage),
                'original_identity': [state.stat().st_dev, state.stat().st_ino],
                'staged_identity': [stage.stat().st_dev, stage.stat().st_ino],
                'lock_identity': [info.st_dev, info.st_ino]})
            os.link(state / 'runtime.lock', stage / 'runtime.lock', follow_symlinks=False)
            linked = True
            snapshot.flush_directories(stage)
            sync_directory(state.parent)
            os.rename(state, retained)
            try:
                sync_directory(state.parent)
                os.rename(stage, state)
            except BaseException:
                if not state.exists():
                    os.rename(retained, state)
                raise
            published = True
            (retained / 'runtime.lock').unlink()
            sync_directory(retained)
            sync_directory(state.parent)
            return receipt
        except BaseException as error:
            if published:
                raise ValueError('restored state was published; inspect its receipt and retained archive before retrying') from error
            raise
        finally:
            if not published and marker_inode is not None and archive_marker.exists() and archive_marker.stat().st_ino == marker_inode:
                archive_marker.unlink()
                sync_directory(state)
            if linked and (stage / 'runtime.lock').exists():
                (stage / 'runtime.lock').unlink()
            if reserved and retained.exists() and not any(retained.iterdir()):
                retained.rmdir()
