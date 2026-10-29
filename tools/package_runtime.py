#!/usr/bin/env python3
"""Package native runtime artifacts without user state or vendor credentials."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile

from package_local import BINARIES

REPO = Path(__file__).resolve().parents[1]


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verify(directory):
    """Check an extracted kit's complete file set and content hashes."""
    directory = directory.resolve(strict=True)
    manifest_path = directory / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError('runtime manifest must be a regular file')
    with manifest_path.open('rb') as stream:
        data = stream.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError('runtime manifest exceeds its bound')
    manifest = json.loads(data)
    expected = manifest['files']
    actual = set()
    for path in directory.rglob('*'):
        if path.is_symlink():
            raise ValueError('runtime kit must not contain symlinks')
        if path.is_file():
            actual.add(str(path.relative_to(directory)))
    if actual != set(expected) | {'manifest.json'}:
        raise ValueError('runtime kit file set differs from its manifest')
    for name, expected_hash in expected.items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or digest(directory / path) != expected_hash:
            raise ValueError('runtime kit content differs from its manifest')
    for name in BINARIES:
        if not os.access(directory / 'bin' / name, os.X_OK):
            raise ValueError('runtime binary lost its executable permission')
    return dict(source_commit=manifest['source_commit'], files_verified=len(expected))


def package(bin_dir, destination):
    """Package a clean checkout and caller-built binaries; CI supplies build proof."""
    def git(*args):
        return subprocess.check_output(['git', '-C', str(REPO), *args])

    if git('status', '--porcelain'):
        raise ValueError('runtime distribution requires a clean checkout')
    commit = git('rev-parse', 'HEAD').decode().strip()
    epoch = int(git('show', '-s', '--format=%ct', 'HEAD').decode())
    sources = []
    for name in git('ls-files', '-z').decode().split('\0'):
        if not name:
            continue
        if (name.startswith(('tools/', 'docs/', 'crates/store/migrations/'))
                or name in ('README.md', 'LICENSE', 'LICENSE.md', 'Cargo.lock', 'rust-toolchain.toml')):
            source = REPO / name
            if source.is_symlink() or not source.is_file():
                raise ValueError('distribution source must be a regular tracked file')
            sources.append((source, name))
    for name in BINARIES:
        source = bin_dir / name
        if source.is_symlink() or not source.is_file() or not os.access(source, os.X_OK):
            raise ValueError('build every native runtime binary before packaging')
        sources.append((source, 'bin/' + name))
    destination = destination.absolute()
    archive = Path(str(destination) + '.tar.gz')
    if destination.exists() or archive.exists():
        raise ValueError('distribution destination already exists')
    destination.mkdir(mode=0o700, parents=True)
    for source, name in sources:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(0o755 if name.startswith('bin/') else 0o644)
    (destination / 'README.txt').write_text('''Conductor native runtime kit

Use a Python 3.11+ interpreter explicitly; no Rust build is needed for this kit.
From this directory, initialize an existing committed Git project:
  python3 -E -s tools/conductor --state /short/private/state init --project /absolute/project --bin-dir ./bin --port 8787
Start the coordinator:
  python3 -E -s tools/conductor --state /short/private/state run --dispatch

Vendor binaries, accepted adapter pins, account configuration and logins are not
included. Use a locally accepted plan for your actual worker and independent
reviewer, including full_capability/account_worker when selecting another account:
  python3 -E -s tools/conductor account-check --from-plan /absolute/accepted-plan.json --role worker
  ./bin/conductor-runner --account-worker /absolute/worker.sock COORDINATOR_UID
Run the receiver as its selected account, with that account's own HOME/login.
Check its endpoint from the coordinator using account-check --role coordinator.
The full-capability worker retains its normal shell, tools and network access.

Prepare and submit a task after selecting real project checks:
  python3 -E -s tools/conductor --state /short/private/state plan --from-plan /absolute/accepted-plan.json --task first --objective-file /absolute/objective.txt --checks-file /absolute/checks.json
  python3 -E -s tools/conductor --state /short/private/state submit /short/private/state/plans/first.json

See docs/FULL_CAPABILITY_EXECUTION.md for account setup, docs/LINUX_CHECKS.md for
Linux dependencies, and docs/DELIVERY_ACCEPTANCE.md for remaining acceptance gaps.
This kit is not a vendor login/conformance certificate or a complete v1 release.
Never replace binaries in a running installation. Keep the verified archive and
manifest; use a separate directory when preparing an upgrade.
''')
    files = {str(p.relative_to(destination)): digest(p)
             for p in sorted(destination.rglob('*')) if p.is_file()}
    manifest = dict(schema_version=1, source_commit=commit, source_epoch=epoch,
                    system=platform.system(), machine=platform.machine(),
                    vendor_pins_included=False, files=files)
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    with archive.open('xb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as tar:
            for path in sorted(destination.rglob('*')):
                info = tar.gettarinfo(str(path), arcname=str(Path(destination.name) / path.relative_to(destination)))
                info.uid = info.gid = 0
                info.uname = info.gname = ''
                info.mtime = epoch
                info.mode = 0o755 if path.is_dir() or path.parent == destination / 'bin' else 0o644
                if path.is_file():
                    with path.open('rb') as stream:
                        tar.addfile(info, stream)
                else:
                    tar.addfile(info)
    return dict(archive=str(archive), archive_sha256=digest(archive),
                source_commit=commit, files=len(files))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path)
    parser.add_argument('--destination', type=Path)
    parser.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.verify:
        if args.bin_dir or args.destination:
            parser.error('--verify cannot be combined with packaging arguments')
        result = verify(args.verify)
    else:
        if not args.bin_dir or not args.destination:
            parser.error('--bin-dir and --destination are required for packaging')
        result = package(args.bin_dir.resolve(strict=True), args.destination)
    print(json.dumps(result, indent=2))
