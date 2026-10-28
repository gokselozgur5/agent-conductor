//! External boot high-water mark. This does not itself prove orphan fencing.
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    database: PathBuf,
    authority_id: String,
    generation_high_water: u64,
}

pub struct Authority {
    root: PathBuf,
    database: PathBuf,
    record: Option<Record>,
    _lock: File,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn private(metadata: &fs::Metadata, directory: bool) -> io::Result<()> {
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(invalid(
            "authority entry must be private, owned and without links",
        ));
    }
    Ok(())
}

impl Authority {
    pub fn acquire(database: &Path) -> io::Result<Self> {
        let parent = database
            .parent()
            .ok_or_else(|| invalid("database parent missing"))?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
        .canonicalize()?;
        let database = parent.join(
            database
                .file_name()
                .ok_or_else(|| invalid("database filename missing"))?,
        );
        let name = format!(
            ".conductor-authority-{:x}",
            Sha256::digest(database.as_os_str().as_encoded_bytes())
        );
        let root = parent
            .parent()
            .ok_or_else(|| invalid("authority parent missing"))?
            .join(name);
        let created = match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(error),
        };
        private(&fs::symlink_metadata(&root)?, true)?;
        if root.canonicalize()? != root {
            return Err(invalid("authority root has a symlink alias"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(created)
            .truncate(false)
            .mode(0o600)
            .custom_flags(nix::fcntl::OFlag::O_NOFOLLOW.bits())
            .open(root.join("ownership.lock"))?;
        private(&lock.metadata()?, false)?;
        lock.try_lock_exclusive()?;
        let record = match OpenOptions::new()
            .read(true)
            .custom_flags(nix::fcntl::OFlag::O_NOFOLLOW.bits())
            .open(root.join("authority.json"))
        {
            Ok(file) => {
                private(&file.metadata()?, false)?;
                let mut bytes = Vec::new();
                file.take(16385).read_to_end(&mut bytes)?;
                if bytes.len() > 16384 {
                    return Err(invalid("authority record exceeds bound"));
                }
                let record: Record = serde_json::from_slice(&bytes)?;
                if record.version != 1
                    || record.database != database
                    || record.authority_id.len() != 64
                    || !record
                        .authority_id
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    || record.generation_high_water == 0
                    || record.generation_high_water > i64::MAX.unsigned_abs()
                {
                    return Err(invalid("authority record does not match this runtime"));
                }
                Some(record)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && created => None,
            Err(error) => return Err(error),
        };
        match fs::symlink_metadata(&database) {
            Ok(info)
                if info.is_file()
                    && info.nlink() == 1
                    && info.uid() == nix::unistd::geteuid().as_raw() => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound && record.is_none() => {}
            _ => {
                return Err(invalid(
                    "recorded database is missing, replaced by a link, or unowned; recovery required",
                ));
            }
        }
        Ok(Self {
            root,
            database,
            record,
            _lock: lock,
        })
    }

    pub fn reserve(
        &mut self,
        binding: Option<&str>,
        database_generation: u64,
    ) -> io::Result<(String, u64)> {
        if binding.is_some_and(|identity| {
            self.record
                .as_ref()
                .is_none_or(|record| record.authority_id != identity)
        }) {
            return Err(invalid(
                "database authority binding is missing or different",
            ));
        }
        let mut record = self.record.clone().unwrap_or_else(|| Record {
            version: 1,
            database: self.database.clone(),
            authority_id: format!("{:x}", Sha256::digest(uuid::Uuid::now_v7().as_bytes())),
            generation_high_water: 0,
        });
        record.generation_high_water = record
            .generation_high_water
            .max(database_generation)
            .checked_add(1)
            .filter(|generation| *generation <= i64::MAX.unsigned_abs())
            .ok_or_else(|| invalid("authority generation cannot advance"))?;
        let temporary = self
            .root
            .join(format!("pending-{}.json", uuid::Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(&record)?)?;
        file.sync_all()?;
        fs::rename(temporary, self.root.join("authority.json"))?;
        File::open(&self.root)?.sync_all()?;
        File::open(
            self.root
                .parent()
                .ok_or_else(|| invalid("authority parent missing"))?,
        )?
        .sync_all()?;
        let result = (record.authority_id.clone(), record.generation_high_water);
        self.record = Some(record);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        fs::create_dir(&state).unwrap();
        let database = state.join("state.sqlite3");
        File::create(&database).unwrap();
        (root, database)
    }

    #[test]
    fn reservation_survives_database_rollback_and_unused_reservations() {
        let (_root, database) = fixture();
        let mut authority = Authority::acquire(&database).unwrap();
        let (id, first) = authority.reserve(None, 20).unwrap();
        assert_eq!(first, 21);
        assert!(Authority::acquire(&database).is_err());
        drop(authority);
        let mut authority = Authority::acquire(&database).unwrap();
        // No database boot consumed 21; its external reservation still cannot repeat.
        assert_eq!(authority.reserve(Some(&id), 3).unwrap(), (id.clone(), 22));
        assert_eq!(authority.reserve(Some(&id), 40).unwrap(), (id, 41));
    }

    #[test]
    fn missing_database_or_record_never_creates_fresh_authority() {
        let (_root, database) = fixture();
        let mut authority = Authority::acquire(&database).unwrap();
        authority.reserve(None, 1).unwrap();
        let record = authority.root.join("authority.json");
        drop(authority);
        fs::rename(&database, database.with_extension("retained")).unwrap();
        assert!(Authority::acquire(&database).is_err());
        assert!(!database.exists());
        fs::rename(database.with_extension("retained"), &database).unwrap();
        fs::remove_file(record).unwrap();
        assert!(Authority::acquire(&database).is_err());
    }

    #[test]
    fn changed_binding_and_overflow_preserve_record_bytes() {
        let (_root, database) = fixture();
        let mut authority = Authority::acquire(&database).unwrap();
        let (id, _) = authority.reserve(None, 1).unwrap();
        let path = authority.root.join("authority.json");
        let before = fs::read(&path).unwrap();
        assert!(authority.reserve(Some(&"x".repeat(64)), 2).is_err());
        assert!(
            authority
                .reserve(Some(&id), i64::MAX.unsigned_abs())
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn record_links_permissions_and_corruption_are_refused() {
        let (_root, database) = fixture();
        let mut authority = Authority::acquire(&database).unwrap();
        authority.reserve(None, 1).unwrap();
        let path = authority.root.join("authority.json");
        drop(authority);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Authority::acquire(&database).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let other = path.with_extension("retained");
        fs::hard_link(&path, &other).unwrap();
        assert!(Authority::acquire(&database).is_err());
        fs::remove_file(&other).unwrap();
        fs::write(&path, b"corrupt").unwrap();
        assert!(Authority::acquire(&database).is_err());
    }
}
