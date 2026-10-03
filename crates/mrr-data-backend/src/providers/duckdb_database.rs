//! One native database handle per canonical local file, with weak ownership.
use crate::BackendError;
use duckdb::Connection;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};
pub(super) type SharedDatabase = Arc<Mutex<Connection>>;
type Registry = HashMap<PathBuf, Weak<Mutex<Connection>>>;
static DATABASES: OnceLock<Mutex<Registry>> = OnceLock::new();

fn canonical(path: &Path) -> Result<PathBuf, BackendError> {
    if path.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if path
                .metadata()
                .map_err(|_| BackendError::Unavailable)?
                .nlink()
                != 1
            {
                return Err(BackendError::InvalidConfiguration);
            }
        }
        return path.canonicalize().map_err(|_| BackendError::Unavailable);
    }
    if path
        .symlink_metadata()
        .is_ok_and(|m| m.file_type().is_symlink())
    {
        return Err(BackendError::InvalidConfiguration);
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path.file_name().ok_or(BackendError::InvalidConfiguration)?;
    Ok(parent
        .canonicalize()
        .map_err(|_| BackendError::Unavailable)?
        .join(name))
}
pub(super) fn open(
    path: &Path,
    initialize: impl FnOnce(&Connection, bool) -> Result<(), BackendError>,
) -> Result<SharedDatabase, BackendError> {
    let path = canonical(path)?;
    let mut registry = DATABASES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| BackendError::Unavailable)?;
    registry.retain(|_, weak| weak.strong_count() != 0);
    if let Some(database) = registry.get(&path).and_then(Weak::upgrade) {
        return Ok(database);
    }
    let existed = path.exists();
    let connection = Connection::open(&path).map_err(|_| BackendError::Unavailable)?;
    initialize(&connection, existed)?;
    let database = Arc::new(Mutex::new(connection));
    registry.insert(path, Arc::downgrade(&database));
    Ok(database)
}
pub(super) fn close(database: SharedDatabase) -> Result<(), BackendError> {
    // Keep reopen from racing the final native database teardown/checkpoint.
    let _registry = DATABASES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| BackendError::Unavailable)?;
    if let Ok(database) = Arc::try_unwrap(database) {
        database
            .into_inner()
            .map_err(|_| BackendError::Unavailable)?
            .close()
            .map_err(|_| BackendError::Unavailable)?;
    }
    Ok(())
}
