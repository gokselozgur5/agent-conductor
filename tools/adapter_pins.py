"""Keep receiver-owned executable pins separate from locally verified binaries."""
import hashlib
import os
from pathlib import Path
import re


def accepted_pin(pin, *, receiver_owned=False):
    executable = Path(pin['executable'])
    if receiver_owned:
        if (not executable.is_absolute() or '..' in executable.parts
                or not executable.name
                or any(re.fullmatch('[0-9a-f]{64}', pin[key]) is None
                       for key in ('executable_sha256', 'conformance_sha256'))):
            raise ValueError('invalid receiving-account adapter pin')
        # Actual bytes and account identity are checked by the receiver before ACK.
    else:
        executable = executable.resolve(strict=True)
        if (not os.access(executable, os.X_OK)
                or hashlib.sha256(executable.read_bytes()).hexdigest()
                != pin['executable_sha256']):
            raise ValueError('accepted adapter binary changed; conformance must be revalidated')
    result = {key: pin[key] for key in
              ('kind', 'version', 'executable_sha256', 'conformance_sha256')}
    result['executable'] = str(executable)
    return result


def accepted_pins(template):
    remote = template.get('account_worker') is not None
    if remote and template.get('full_capability') is None:
        raise ValueError('account worker requires full capability profile')
    return {role: accepted_pin(template[role], receiver_owned=remote and role == 'worker')
            for role in ('worker', 'verifier')}
