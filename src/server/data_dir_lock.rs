//! Single-owner lock for a session's persisted data directory.
//!
//! The socket paths and the persistence paths resolve independently: a socket
//! override (`HERDR_SOCKET_PATH`) moves the sockets but not `session.json`.
//! The `AddrInUse` check on the socket therefore cannot tell that another live
//! server already restores from and saves to the same data directory. This
//! lock makes the data directory itself the single-instance authority, so a
//! second server never restores a copy of a running session and then
//! overwrites it.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) const LOCK_FILE_NAME: &str = "server.lock";

/// Exclusive ownership of a session data directory. The lock is released when
/// the guard drops or the owning process exits, including on a crash, so a
/// stale file never blocks the next start.
#[derive(Debug)]
pub(crate) struct DataDirLock {
    path: PathBuf,
    _file: File,
}

impl DataDirLock {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// Why the lock could not be taken right now.
#[derive(Debug)]
pub(crate) enum DataDirLockError {
    /// Another live process holds the lock.
    Held {
        path: PathBuf,
    },
    Io(io::Error),
}

impl std::fmt::Display for DataDirLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held { path } => write!(
                f,
                "another herdr server already owns the session data at {}",
                path.display()
            ),
            Self::Io(err) => write!(f, "could not lock the session data directory: {err}"),
        }
    }
}

impl From<io::Error> for DataDirLockError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub(crate) fn lock_path(data_dir: &Path) -> PathBuf {
    data_dir.join(LOCK_FILE_NAME)
}

/// Try once to take exclusive ownership of `data_dir`.
pub(crate) fn try_acquire(data_dir: &Path) -> Result<DataDirLock, DataDirLockError> {
    std::fs::create_dir_all(data_dir)?;
    let path = lock_path(data_dir);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(DataDirLock { path, _file: file }),
        Err(TryLockError::WouldBlock) => Err(DataDirLockError::Held { path }),
        Err(TryLockError::Error(err)) => Err(DataDirLockError::Io(err)),
    }
}

/// Retry `try_acquire` until `timeout` elapses. Used when the previous owner
/// is known to be exiting, such as the old server after a live handoff commit.
pub(crate) fn acquire_within(
    data_dir: &Path,
    timeout: Duration,
) -> Result<DataDirLock, DataDirLockError> {
    let deadline = Instant::now() + timeout;
    loop {
        match try_acquire(data_dir) {
            Err(DataDirLockError::Held { .. }) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_data_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "herdr-data-dir-lock-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn second_acquire_fails_while_the_first_lock_is_held() {
        let dir = temp_data_dir("held");
        let first = try_acquire(&dir).expect("first acquire");
        assert_eq!(first.path(), lock_path(&dir));
        match try_acquire(&dir) {
            Err(DataDirLockError::Held { path }) => assert_eq!(path, lock_path(&dir)),
            other => panic!("expected held lock, got {other:?}"),
        }
        drop(first);
        try_acquire(&dir).expect("acquire after release");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn acquire_within_waits_for_the_previous_owner_to_release() {
        let dir = temp_data_dir("wait");
        let first = try_acquire(&dir).expect("first acquire");
        let releaser = {
            let dir = dir.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(150));
                drop(first);
                dir
            })
        };
        let lock = acquire_within(&dir, Duration::from_secs(5)).expect("acquire after release");
        assert_eq!(lock.path(), lock_path(&dir));
        releaser.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn acquire_within_reports_held_after_the_timeout() {
        let dir = temp_data_dir("timeout");
        let _first = try_acquire(&dir).expect("first acquire");
        let started = Instant::now();
        match acquire_within(&dir, Duration::from_millis(100)) {
            Err(DataDirLockError::Held { .. }) => {}
            other => panic!("expected held lock, got {other:?}"),
        }
        assert!(started.elapsed() >= Duration::from_millis(100));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_data_dir_is_created() {
        let dir = temp_data_dir("create").join("nested").join("session");
        let lock = try_acquire(&dir).expect("acquire creates the directory");
        assert!(lock.path().exists());
        let _ = std::fs::remove_dir_all(dir.parent().unwrap().parent().unwrap());
    }
}
