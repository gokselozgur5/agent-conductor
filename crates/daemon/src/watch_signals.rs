//! Native events request re-observation; they never establish repository truth.
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
const RECONCILE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Signals {
    dirty: AtomicBool,
    failed: AtomicBool,
    roots: RwLock<Vec<PathBuf>>,
}
impl Signals {
    fn event(&self, result: notify::Result<Event>) {
        let Ok(event) = result else {
            self.failed.store(true, Ordering::Release);
            return;
        };
        if event.need_rescan() {
            self.dirty.store(true, Ordering::Release);
            return;
        }
        match event.kind {
            EventKind::Access(notify::event::AccessKind::Close(
                notify::event::AccessMode::Write,
            )) => {}
            EventKind::Access(_)
            | EventKind::Modify(notify::event::ModifyKind::Metadata(
                notify::event::MetadataKind::AccessTime,
            )) => return,
            _ => {}
        }
        let Ok(roots) = self.roots.read() else {
            self.failed.store(true, Ordering::Release);
            return;
        };
        if event.paths.is_empty()
            || event.paths.iter().any(|path| {
                roots
                    .iter()
                    .any(|root| path.starts_with(root) || root.starts_with(path))
            })
        {
            self.dirty.store(true, Ordering::Release);
        }
    }
}

pub struct ScanGate {
    signals: Arc<Signals>,
    watcher: RecommendedWatcher,
    watched: BTreeMap<PathBuf, bool>,
    last_scan: Option<Duration>,
}
impl ScanGate {
    pub fn new(roots: Vec<PathBuf>) -> Result<Self, &'static str> {
        let signals = Arc::new(Signals::default());
        let callback = signals.clone();
        let watcher = RecommendedWatcher::new(
            move |event| callback.event(event),
            notify::Config::default().with_follow_symlinks(false),
        )
        .map_err(|_| "native watcher unavailable")?;
        let mut gate = Self {
            signals,
            watcher,
            watched: BTreeMap::new(),
            last_scan: None,
        };
        gate.refresh(roots)?;
        Ok(gate)
    }
    pub fn refresh(&mut self, roots: Vec<PathBuf>) -> Result<(), &'static str> {
        if roots.is_empty() || roots.len() > 3 || roots.iter().any(|p| !p.is_absolute()) {
            return Err("invalid native observation roots");
        }
        let all = roots.clone();
        let roots = roots
            .into_iter()
            .filter(|path| {
                !all.iter()
                    .any(|other| other != path && path.starts_with(other))
            })
            .collect::<Vec<_>>();
        let mut wanted = BTreeMap::new();
        for root in &roots {
            wanted.insert(root.clone(), true);
            if let Some(parent) = root.parent() {
                wanted.entry(parent.to_path_buf()).or_insert(false);
            }
        }
        if wanted == self.watched {
            return Ok(());
        }
        *self
            .signals
            .roots
            .write()
            .map_err(|_| "native scope unavailable")? = roots;
        for (path, recursive) in &wanted {
            if self.watched.get(path) != Some(recursive) {
                self.watcher
                    .watch(
                        path,
                        if *recursive {
                            RecursiveMode::Recursive
                        } else {
                            RecursiveMode::NonRecursive
                        },
                    )
                    .map_err(|_| "native watch registration failed")?;
            }
        }
        for path in self
            .watched
            .keys()
            .filter(|path| !wanted.contains_key(*path))
        {
            self.watcher
                .unwatch(path)
                .map_err(|_| "native watch removal failed")?;
        }
        self.watched = wanted;
        self.signals.dirty.store(true, Ordering::Release);
        Ok(())
    }
    pub fn begin_scan(&mut self, now: Duration, settling: bool) -> Result<bool, &'static str> {
        scan_due(&mut self.last_scan, &self.signals, now, settling)
    }
}

fn scan_due(
    last_scan: &mut Option<Duration>,
    signals: &Signals,
    now: Duration,
    settling: bool,
) -> Result<bool, &'static str> {
    if signals.failed.load(Ordering::Acquire) {
        return Err("native watcher failed");
    }
    let due = last_scan
        .as_ref()
        .is_none_or(|last| now.saturating_sub(*last) >= RECONCILE)
        || settling
        || signals.dirty.load(Ordering::Acquire);
    if due {
        // Clear before observation so notifications arriving during I/O remain pending.
        signals.dirty.store(false, Ordering::Release);
        *last_scan = Some(now);
    }
    Ok(due)
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, DataChange, Flag, ModifyKind};

    #[test]
    fn dirty_hints_settling_and_lost_notifications_keep_bounded_scan_scheduling() {
        let root = PathBuf::from("/fixture");
        let signals = Signals::default();
        *signals.roots.write().unwrap() = vec![root.clone()];
        let mut last_scan = None;
        assert!(scan_due(&mut last_scan, &signals, Duration::ZERO, false).unwrap());
        assert!(!scan_due(&mut last_scan, &signals, Duration::from_millis(250), false).unwrap());
        signals.event(Ok(Event::new(EventKind::Modify(ModifyKind::Data(
            DataChange::Content,
        )))
        .add_path(root.join("file"))));
        assert!(scan_due(&mut last_scan, &signals, Duration::from_millis(500), false).unwrap());
        // An event arriving after the scan starts must remain for the next pass.
        signals.event(Ok(Event::new(EventKind::Any).add_path(root.join("later"))));
        assert!(scan_due(&mut last_scan, &signals, Duration::from_millis(750), false).unwrap());
        assert!(scan_due(&mut last_scan, &signals, Duration::from_secs(1), true).unwrap());
        assert!(!scan_due(&mut last_scan, &signals, Duration::from_millis(1250), false).unwrap());
        assert!(!scan_due(&mut last_scan, &signals, Duration::from_millis(5999), false).unwrap());
        assert!(scan_due(&mut last_scan, &signals, Duration::from_secs(6), false).unwrap());
        signals.event(Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)));
        assert!(scan_due(&mut last_scan, &signals, Duration::from_millis(6250), false).unwrap());
        signals.event(Err(notify::Error::generic("injected")));
        assert!(scan_due(&mut last_scan, &signals, Duration::from_millis(6500), false).is_err());
    }

    #[test]
    fn read_noise_and_siblings_do_not_invalidate_but_root_loss_does() {
        let signals = Signals::default();
        *signals.roots.write().unwrap() = vec![PathBuf::from("/parent/project")];
        signals
            .event(Ok(Event::new(EventKind::Access(AccessKind::Read))
                .add_path("/parent/project/file".into())));
        signals.event(Ok(
            Event::new(EventKind::Any).add_path("/parent/other/file".into())
        ));
        assert!(!signals.dirty.load(Ordering::Acquire));
        signals.event(Ok(
            Event::new(EventKind::Any).add_path("/parent/project".into())
        ));
        assert!(signals.dirty.load(Ordering::Acquire));
    }

    #[test]
    fn native_changes_arrive_before_periodic_reconciliation() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut gate = ScanGate::new(vec![root.clone()]).unwrap();
        gate.begin_scan(Duration::ZERO, false).unwrap();
        std::thread::sleep(Duration::from_millis(200));
        gate.signals.dirty.store(false, Ordering::Release);
        std::fs::create_dir(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/new.txt"), "change").unwrap();
        let start = std::time::Instant::now();
        while !gate.signals.dirty.load(Ordering::Acquire)
            && start.elapsed() < Duration::from_secs(3)
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            gate.signals.dirty.load(Ordering::Acquire),
            "native notification missing before fallback"
        );
        assert!(gate.begin_scan(Duration::from_secs(3), false).unwrap());
        assert!(!gate.signals.failed.load(Ordering::Acquire));
    }
}
