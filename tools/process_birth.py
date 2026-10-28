"""Kernel start observations for diagnostics, never permission to signal a PID."""
import ctypes
from pathlib import Path
import sys


class BsdInfo(ctypes.Structure):
    # macOS SDK sys/proc_info.h: PROC_PIDTBSDINFO, struct proc_bsdinfo.
    _fields_ = ([(name, ctypes.c_uint32) for name in (
        'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid', 'ruid',
        'rgid', 'svuid', 'svgid', 'reserved')]
        + [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
        + [(name, ctypes.c_uint32) for name in ('nfiles', 'pgid', 'jobc', 'tdev', 'tpgid')]
        + [('nice', ctypes.c_int32), ('start_sec', ctypes.c_uint64),
           ('start_usec', ctypes.c_uint64)])


def linux_start(raw, pid):
    """Field 22 follows a parenthesized comm which may itself contain ')'."""
    prefix, separator, suffix = raw.rpartition(b') ')
    if not separator or not prefix.startswith(str(pid).encode() + b' ('):
        return None
    fields = suffix.split()
    if len(fields) < 20 or not fields[19].isdigit() or len(fields[19]) > 20:
        return None
    value = int(fields[19])
    if value > 2**64 - 1:
        return None
    return 'linux-start-ticks:' + str(value)


def observe(pid):
    """Return an opaque boot-local start marker, or None on any uncertainty.

    Start timestamps have finite resolution; they are not handles that pin a
    process, nor proof of ownership, executable identity or current liveness.
    """
    if type(pid) is not int or not 0 < pid <= 2**31 - 1:
        return None
    try:
        if sys.platform == 'linux':
            with (Path('/proc') / str(pid) / 'stat').open('rb') as source:
                raw = source.read(65537)
            return linux_start(raw, pid) if len(raw) <= 65536 else None
        if sys.platform == 'darwin':
            if ctypes.sizeof(BsdInfo) != 136:
                return None
            library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
            call = library.proc_pidinfo
            call.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                             ctypes.c_void_p, ctypes.c_int]
            call.restype = ctypes.c_int
            info = BsdInfo()
            count = call(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
            if count != ctypes.sizeof(info) or info.pid != pid or info.start_sec == 0 or info.start_usec >= 1000000:
                return None
            return f'macos-start-us:{info.start_sec}:{info.start_usec}'
    except (OSError, AttributeError, ValueError):
        return None
    return None
