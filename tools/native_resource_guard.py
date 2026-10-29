"""Read-only sampled limits for one explicitly selected native test daemon."""
from pathlib import Path
import sqlite3
import stat
import time

from inspect_runner_processes import ps
from native_observation_guard import require
from process_birth import observe


class NativeResourceGuard:
    def __init__(self, database, daemon_pid, log_files=(), *, max_rss_kib=524288,
                 growth_kib=65536, max_state_bytes=2*1024**3,
                 max_pending_deliveries=256, max_unfinished_dispatch=256):
        require(type(daemon_pid) is int and 0 < daemon_pid <= 2**31-1, 'resource_pid_invalid')
        self.database = Path(database).resolve(strict=True)
        self.pid = daemon_pid
        self.birth = observe(self.pid)
        require(self.birth is not None, 'resource_process_unavailable')
        self.limits = dict(rss_kib=max_rss_kib, growth_kib=growth_kib,
                           state_bytes=max_state_bytes, pending_deliveries=max_pending_deliveries,
                           unfinished_dispatch=max_unfinished_dispatch)
        require(all(type(v) is int and 0 <= v <= 2**40 for v in self.limits.values()), 'resource_limits_invalid')
        require(max_rss_kib > 0 and max_state_bytes > 0, 'resource_limits_invalid')
        require(len(log_files) <= 16, 'resource_file_limit')
        self.files = [self.database, Path(str(self.database)+'-wal'), Path(str(self.database)+'-shm'), *map(Path,log_files)]
        require(all(p.is_absolute() for p in self.files), 'resource_path_invalid')
        self.baseline = None
        self.failed = False

    def sample(self):
        require(not self.failed, 'resource_monitor_already_failed')
        try:
            return self._sample()
        except Exception:
            self.failed = True
            raise

    def _sample(self):
        deadline = time.monotonic()+5
        require(observe(self.pid)==self.birth, 'resource_process_identity_changed')
        code, text = ps(['-p',str(self.pid),'-o','rss='], timeout=2)
        text = text.strip()
        require(code==0 and text.isascii() and text.isdigit() and len(text)<=12, 'resource_rss_unavailable')
        rss = int(text)
        if self.baseline is None:
            self.baseline = rss
        require(rss <= self.limits['rss_kib'], 'resource_rss_limit')
        require(rss <= self.baseline+self.limits['growth_kib'], 'resource_rss_growth')
        size = 0
        for path in self.files:
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                require(path != self.database, 'resource_database_missing')
                continue
            require(stat.S_ISREG(metadata.st_mode), 'resource_special_file')
            size += metadata.st_size
        require(size <= self.limits['state_bytes'], 'resource_state_limit')
        with sqlite3.connect(self.database.as_uri()+'?mode=ro',uri=True,timeout=2) as db:
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            db.execute('begin')
            pending = db.execute("select count(*) from event_deliveries where status in ('pending','leased')").fetchone()[0]
            dispatch = db.execute("select count(*) from workflow_dispatch where status!='finished'").fetchone()[0]
        require(pending <= self.limits['pending_deliveries'], 'resource_delivery_limit')
        require(dispatch <= self.limits['unfinished_dispatch'], 'resource_dispatch_limit')
        require(observe(self.pid)==self.birth, 'resource_process_identity_changed')
        require(time.monotonic() <= deadline, 'resource_observation_deadline')
        return dict(rss_kib=rss, baseline_rss_kib=self.baseline, state_bytes=size,
                    pending_deliveries=pending, unfinished_dispatch=dispatch)
