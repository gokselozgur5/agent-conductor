"""Cold operator-state backup. No lifecycle transitions or worker execution."""
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import recovery_dependencies
import recovery_authority

MAX_FILES = 100000
MAX_BYTES = 10 * 1024 ** 3
MIGRATIONS = Path(__file__).resolve().parents[1] / 'crates/store/migrations'
SKIP = {'runtime.lock', 'control.sock', 'state.sqlite3-wal', 'state.sqlite3-shm', 'bin'}


def digest(path):
    value = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError('file exceeds backup bound')
            value.update(chunk)
    return value.hexdigest()


def scan_error(error):
    raise error


def inventory(root, exclude=()):
    result = {}
    total = 0
    for directory, folders, files in os.walk(root, followlinks=False, onerror=scan_error):
        base = Path(directory)
        if base == root:
            folders[:] = [name for name in folders if name not in exclude]
            files = [name for name in files if name not in exclude]
        for name in sorted(folders + files):
            path = base / name
            info = path.lstat()
            if info.st_uid != os.getuid() or stat.S_ISLNK(info.st_mode) or info.st_mode & 0o7000:
                raise ValueError('backup refuses unowned entries and symlinks')
            relative = str(path.relative_to(root))
            if stat.S_ISDIR(info.st_mode):
                result[relative] = {'kind': 'directory', 'mode': stat.S_IMODE(info.st_mode)}
            elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                total += info.st_size
                if total > MAX_BYTES:
                    raise ValueError('backup exceeds its 10 GiB bound')
                result[relative] = {'kind': 'file', 'mode': stat.S_IMODE(info.st_mode), 'size': info.st_size, 'sha256': digest(path)}
            else:
                raise ValueError('backup refuses special files and hard links')
            if len(result) > MAX_FILES:
                raise ValueError('backup exceeds its entry bound')
    return result


def private_root(path):
    if path != path.resolve(strict=True):
        raise ValueError('backup roots must be canonical, without symlink aliases')
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('backup root must be private and operator-owned')


def inside(path, state):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or not path.is_relative_to(state):
        raise ValueError('external attempt artifacts require a separate recovery procedure')


def database(path, state):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
        raise ValueError('database must be an owned regular file without links')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        if db.execute('pragma page_count').fetchone()[0] * db.execute('pragma page_size').fetchone()[0] > MAX_BYTES:
            raise ValueError('database exceeds backup bound')
        if db.execute('pragma integrity_check').fetchall() != [('ok',)]:
            raise ValueError('database integrity check failed')
        migrations = db.execute('select version, checksum, success from _sqlx_migrations order by version').fetchall()
        expected = [(int(file.name.split('_')[0]), hashlib.sha384(file.read_bytes()).digest(), 1) for file in sorted(MIGRATIONS.glob('*.sql'))]
        if not expected or migrations != expected:
            raise ValueError('unsupported or incomplete database migration')
        if db.execute("select exists(select 1 from file_brokers where state != 'closed') or exists(select 1 from file_operations where completed_at_ms is null) or exists(select 1 from file_broker_launches where state != 'closed')").fetchone()[0]:
            raise ValueError('file broker ownership unresolved; verified closure required before backup')
        if db.execute("select count(*) from executions where state != 'fenced'").fetchone()[0]:
            raise ValueError('execution without a verified write fence; reconcile before backup')
        if db.execute('select exists(select 1 from conversation_turns)').fetchone()[0]:
            raise ValueError('native conversation execution has no verified write fence; separate recovery required before backup')
        if db.execute('select count(*) from legacy_auxiliary_ownership').fetchone()[0]:
            raise ValueError('legacy auxiliary ownership unresolved; verified recovery required before backup')
        if db.execute("select count(*) from auxiliary_invocations where state != 'settled'").fetchone()[0]:
            raise ValueError('unresolved auxiliary invocation; reconcile before backup')
        if db.execute("select count(*) from leases where status in ('active','recovery_pending')").fetchone()[0]:
            raise ValueError('live or recovering lease; reconcile before backup')
        if db.execute("select count(*) from effect_intents where status not in ('done','abandoned')").fetchone()[0]:
            raise ValueError('unresolved effect intent; reconcile before backup')
        if db.execute("select count(*) from tasks where state in ('claimed','running','verifying')").fetchone()[0]:
            raise ValueError('active task; pause and reconcile before backup')
        if db.execute("select count(*) from workflow_dispatch where status = 'running'").fetchone()[0]:
            raise ValueError('interrupted dispatch; reconcile before backup')
        for (encoded,) in db.execute('select plan_json from workflow_dispatch'):
            inside(json.loads(encoded)['artifacts'], state)
        for (encoded,) in db.execute('select packet_json from executions'):
            packet = json.loads(encoded)
            inside(packet['contract']['workspace'], state)
        return [{'version': v, 'checksum': bytes(c).hex()} for v, c, _ in migrations]


def copy_inventory(source, target, entries):
    for name, record in sorted(entries.items(), key=lambda pair: (len(Path(pair[0]).parts), pair[0])):
        original, destination = source / name, target / name
        if record['kind'] == 'directory':
            destination.mkdir(mode=0o700)
            continue
        fd = os.open(original, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as reader:
            info = os.fstat(reader.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise ValueError('source changed during backup')
            out = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(out, 'wb') as writer:
                copied = 0
                for chunk in iter(lambda: reader.read(1024 * 1024), b''):
                    copied += len(chunk)
                    if copied > record['size']:
                        raise ValueError('source grew during backup')
                    writer.write(chunk)
                writer.flush()
                os.fchmod(writer.fileno(), record['mode'])
                os.fsync(writer.fileno())
    for name, record in sorted(entries.items(), reverse=True):
        if record['kind'] == 'directory':
            (target / name).chmod(record['mode'])


def flush_directories(root):
    directories = [Path(path) for path, _, _ in os.walk(root, followlinks=False, onerror=scan_error)]
    for directory in reversed(directories):
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def backup(state, destination):
    with recovery_authority.hold(state):
        return _backup(state, destination)


def _backup(state, destination):
    private_root(state)
    if (state / 'ARCHIVED_STATE.json').exists():
        raise ValueError('retained archive is not a live operator state')
    destination = destination.expanduser().absolute()
    parent = destination.parent.resolve(strict=True)
    if destination.parent != parent or destination.is_relative_to(state):
        raise ValueError('backup destination must be outside state under a canonical parent')
    fd = os.open(state / 'runtime.lock', os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise ValueError('invalid runtime lock')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        migrations = database(state / 'state.sqlite3', state)
        entries = inventory(state, SKIP | {'state.sqlite3'})
        for name in ['operator.json', 'board-token']:
            if entries.get(name, {}).get('kind') != 'file' or entries[name]['mode'] & 0o077:
                raise ValueError('required private operator configuration is missing')
        for name in ['plans', 'artifacts']:
            if entries.get(name, {}).get('kind') != 'directory':
                raise ValueError('required operator directory is missing')
        with closing(sqlite3.connect((state / 'state.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
            database_bytes = db.execute('pragma page_count').fetchone()[0] * db.execute('pragma page_size').fetchone()[0]
        if database_bytes + sum(item.get('size', 0) for item in entries.values()) > MAX_BYTES:
            raise ValueError('combined state exceeds backup bound')
        dependencies = recovery_dependencies.collect(state, digest)
        destination.mkdir(mode=0o700)
        payload = destination / 'state'
        payload.mkdir(mode=0o700)
        copy_inventory(state, payload, entries)
        with closing(sqlite3.connect((state / 'state.sqlite3').as_uri() + '?mode=ro', uri=True)) as source:
            with closing(sqlite3.connect(payload / 'state.sqlite3')) as target:
                source.backup(target)
                # Export is one self-contained database, not a WAL sidecar set.
                if target.execute("pragma journal_mode=delete").fetchone() != ("delete",):
                    raise ValueError("could not seal the backup database")
        # Some SQLite builds leave a now-unused SHM file after WAL -> DELETE.
        # Only remove our export's sidecars after the destination is closed.
        wal = payload / 'state.sqlite3-wal'
        if wal.exists() and wal.stat().st_size:
            raise ValueError('backup still has WAL content')
        for suffix in ['-wal', '-shm']:
            sidecar = payload / ('state.sqlite3' + suffix)
            if sidecar.exists():
                sidecar.unlink()
        (payload / 'state.sqlite3').chmod(0o600)
        if database(payload / 'state.sqlite3', state) != migrations or inventory(state, SKIP | {'state.sqlite3'}) != entries:
            raise ValueError('source changed during backup')
        if inventory(payload, {'state.sqlite3'}) != entries:
            raise ValueError('copied evidence differs from source')
        if recovery_dependencies.collect(state, digest) != dependencies:
            raise ValueError('external prerequisites changed during backup')
        manifest = {'version': 2, 'dependencies': dependencies, 'state_root': str(state), 'migrations': migrations,
                    'entries': inventory(payload), 'exclusions': sorted(SKIP),
                    'limits': 'Cold same-host snapshot. External dependencies are recorded, not included. Restore requires existing stopped, quiescent original state; no relocation or missing-state recovery.'}
        flush_directories(payload)
        # Publication marker is written last. Partial directories remain inspectable.
        marker = os.open(destination / 'manifest.pending', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(marker, 'w') as stream:
            json.dump(manifest, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(destination / 'manifest.pending', destination / 'manifest.json')
        for directory in [payload, destination, parent]:
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return {'backup': str(destination), 'entries': len(manifest['entries']), 'schema': migrations[-1]['version'], 'limits': manifest['limits']}


def verify(destination, external=False):
    destination = destination.expanduser().absolute()
    private_root(destination)
    path = destination / 'manifest.json'
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077:
            raise ValueError('manifest must be a private owned regular file')
        data = stream.read(32 * 1024 * 1024 + 1)
        if len(data) > 32 * 1024 * 1024:
            raise ValueError('manifest exceeds its bound')
    manifest = json.loads(data)
    state = Path(manifest['state_root'])
    if manifest['version'] not in (1, 2) or not state.is_absolute() or '..' in state.parts:
        raise ValueError('unsupported manifest or original root')
    payload = destination / 'state'
    private_root(payload)
    if inventory(payload) != manifest['entries']:
        raise ValueError('backup evidence hash, mode or inventory mismatch')
    if database(payload / 'state.sqlite3', state) != manifest['migrations']:
        raise ValueError('backup migration mismatch')
    if external:
        if manifest['version'] != 2:
            raise ValueError('legacy snapshot has no external prerequisite inventory')
        recovery_dependencies.verify(manifest['dependencies'], digest)
        if recovery_dependencies.collect(payload, digest, logical_root=state) != manifest['dependencies']:
            raise ValueError('external prerequisite manifest is incomplete or inconsistent')
    return {'external_verified': external, 'backup': str(destination), 'state_root': str(state), 'verified': True,
            'entries': len(manifest['entries']), 'limits': manifest['limits']}
