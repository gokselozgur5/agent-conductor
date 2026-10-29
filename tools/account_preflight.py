"""Read-only account setup checks; never authenticate, connect or launch a vendor."""
import os
from pathlib import Path
import pwd
import stat

from adapter_pins import accepted_pin


def check(template, role):
    """Check only evidence available to the selected account at this instant."""
    if role not in ('worker', 'coordinator'):
        raise ValueError('unknown account check role')
    profile = template.get('full_capability')
    target = template.get('account_worker')
    if not isinstance(profile, dict) or not isinstance(target, dict):
        raise ValueError('account check requires full_capability and account_worker')
    account = profile['account']
    selected = pwd.getpwnam(account['name'])
    actual = dict(name=selected.pw_name, uid=selected.pw_uid,
                  gid=selected.pw_gid, home=selected.pw_dir)
    if account != actual:
        raise ValueError('selected account metadata differs from the OS account database')
    path = profile['path']
    if not isinstance(path, str) or not path or any(
            not Path(entry).is_absolute() for entry in path.split(os.pathsep)):
        raise ValueError('worker PATH must contain only absolute entries')
    endpoint = Path(target['socket'])
    artifacts = Path(target['artifacts'])
    if (not endpoint.is_absolute() or '..' in endpoint.parts
            or len(os.fsencode(endpoint)) > 100 or not artifacts.is_absolute()
            or '..' in artifacts.parts):
        raise ValueError('account endpoint or artifact path is invalid')
    parent = endpoint.parent
    info = parent.stat()
    if (parent.resolve(strict=True) != parent or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != selected.pw_uid or info.st_mode & 0o022):
        raise ValueError('endpoint parent must be canonical, worker-owned and not writable by others')
    if not os.access(parent, os.X_OK):
        raise ValueError('current account cannot traverse the endpoint parent')
    if role == 'worker':
        if ((os.getuid(), os.geteuid()) != (selected.pw_uid,) * 2
                or (os.getgid(), os.getegid()) != (selected.pw_gid,) * 2):
            raise ValueError('run the worker check as the selected OS account')
        if template['worker']['kind'] not in ('codex', 'claude'):
            raise ValueError('full-capability worker must use Codex or Claude')
        accepted_pin(template['worker'])
        info = artifacts.stat()
        if (artifacts.resolve(strict=True) != artifacts or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != selected.pw_uid
                or not os.access(artifacts, os.W_OK | os.X_OK)):
            raise ValueError('worker artifact directory must be canonical, owned and writable')
        if not os.access(parent, os.W_OK):
            raise ValueError('worker cannot create an endpoint in the selected parent')
    else:
        accepted_pin(template['verifier'])
    try:
        info = endpoint.lstat()
    except FileNotFoundError:
        if role == 'coordinator':
            raise ValueError('worker endpoint is absent; start the selected receiver first') from None
        endpoint_state = 'absent'
    else:
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != selected.pw_uid:
            raise ValueError('existing endpoint is not a socket owned by the selected worker')
        if role == 'coordinator' and not os.access(endpoint, os.W_OK):
            raise ValueError('coordinator cannot access the worker socket')
        endpoint_state = 'present_unverified'
    return dict(role=role, local_checks='passed', worker_account=actual,
                endpoint=endpoint_state, connection_attempted=False,
                vendor_login='not_checked', peer_authentication='not_checked',
                endpoint_path_absent=endpoint_state == 'absent',
                note='Runtime revalidates identity and pins before execution. An existing socket may be stale; never replace it based on this check.')
