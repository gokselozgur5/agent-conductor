"""External prerequisite identities for recovery; no model execution."""
import json
from pathlib import Path
import sqlite3
from contextlib import closing
import subprocess
import recovery_authority


def identity(path):
    path = Path(path)
    if path != path.resolve(strict=True) or not path.is_dir():
        raise ValueError('external directory identity is unavailable or aliased')
    info = path.stat()
    return f'unix:{info.st_dev}:{info.st_ino}'


def collect(state, fingerprint, logical_root=None):
    logical_root = logical_root or state
    config = json.loads((state / 'operator.json').read_text())
    dependencies = {}
    authority_id = recovery_authority.binding(state)
    if authority_id is not None:
        descriptor = recovery_authority.inspect(logical_root, authority_id)
        dependencies[('authority', descriptor['path'])] = descriptor

    def executable(path, expected=None):
        path = Path(path)
        if not path.is_absolute() or path != path.resolve(strict=True) or not path.is_file():
            raise ValueError('required external executable is missing or aliased')
        known = dependencies.get(('file', str(path)))
        actual = known['sha256'] if known else fingerprint(path)
        if not known and len(dependencies) >= 10000:
            raise ValueError('external dependency inventory exceeds its bound')
        if expected is not None and expected != actual:
            raise ValueError('required external pin changed')
        dependencies[('file', str(path))] = {'kind': 'file', 'path': str(path), 'sha256': actual}

    for name in ['conductor-daemon', 'conductorctl', 'conductor-runner']:
        executable(Path(config['bin_dir']) / name)
    with closing(sqlite3.connect((state / 'state.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
        for path, expected, git_expected in db.execute('select canonical_path,filesystem_identity,git_common_dir_identity from projects'):
            if Path(path).is_relative_to(logical_root):
                raise ValueError('registered project inside state cannot preserve its directory identity on restore')
            if len(dependencies) >= 9999:
                raise ValueError('external dependency inventory exceeds its bound')
            if identity(path) != expected:
                raise ValueError('registered external project identity changed')
            dependencies[('directory', path)] = {'kind': 'directory', 'path': path, 'identity': expected}
            if git_expected:
                result = subprocess.run(['git', '-C', path, 'rev-parse', '--path-format=absolute', '--git-common-dir'], capture_output=True, text=True, timeout=10, check=True)
                common = str(Path(result.stdout.strip()).resolve(strict=True))
                if identity(common) != git_expected:
                    raise ValueError('registered Git common-directory identity changed')
                dependencies[('directory', common)] = {'kind': 'directory', 'path': common, 'identity': git_expected}
        # Historical completed plans do not require old vendor installations to
        # preserve their recorded results. Nonterminal and queued plans do.
        plans = db.execute("select w.plan_json from workflow_dispatch w left join tasks t on t.id=w.task_id where t.id is null or t.state not in ('completed','cancelled')")
        for index, (encoded,) in enumerate(plans):
            if index >= 10000:
                raise ValueError('pending plan inventory exceeds its bound')
            plan = json.loads(encoded)
            for role in ['worker', 'verifier']:
                pin = plan[role]
                executable(pin['executable'], pin['executable_sha256'])
            executable(plan['wrapper'])
            for pin in plan.get('instructions', []):
                executable(pin['path'], pin['sha256'])
            for path in plan.get('toolchain_roots', []):
                dependencies[('directory', path)] = {'kind': 'directory', 'path': path, 'identity': identity(path)}
    if len(dependencies) > 10000:
        raise ValueError('external dependency inventory exceeds its bound')
    return [dependencies[key] for key in sorted(dependencies)]


def verify(dependencies, fingerprint):
    if not isinstance(dependencies, list) or len(dependencies) > 10000:
        raise ValueError('invalid external dependency inventory')
    for item in dependencies:
        path = Path(item['path'])
        if item['kind'] == 'authority':
            if recovery_authority.inspect(Path(item['state_root']), item['authority_id']) != item:
                raise ValueError('external recovery authority changed')
        elif item['kind'] == 'directory':
            if identity(path) != item['identity']:
                raise ValueError('external directory identity changed')
        elif item['kind'] == 'file':
            if path != path.resolve(strict=True) or not path.is_file() or fingerprint(path) != item['sha256']:
                raise ValueError('external file is missing or changed')
        else:
            raise ValueError('unknown external dependency kind')
