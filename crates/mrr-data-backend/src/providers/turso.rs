//! Optional Turso native engine. This module owns its physical database dialect;
//! the shared backend consumes only bounded opaque transaction records.
use super::{MetadataTransaction, ProviderResult, TransactionProvider};
use crate::BackendError;
use mrr_data_content::ConditionalCommitPortError as PortError;
use std::{path::PathBuf, sync::Mutex, time::Duration};
use tokio::runtime::Handle;
const SCHEMA: &str = r#"("mrr.backend.store.v1" "turso" "scheme")"#;
/// Local Turso connections using the Host executor. No global allocator, remote
/// replica, automatic runtime or process signal handler is enabled.
pub struct TursoProvider {
    path: PathBuf,
    runtime: Handle,
    writer: Mutex<Option<turso::Connection>>,
    reader: Mutex<Option<turso::Connection>>,
    database: Mutex<Option<turso::Database>>,
}
impl TursoProvider {
    #[must_use]
    pub fn new(path: PathBuf, runtime: Handle) -> Self {
        Self {
            path,
            runtime,
            writer: Mutex::new(None),
            reader: Mutex::new(None),
            database: Mutex::new(None),
        }
    }
}
fn failure(_: turso::Error) -> BackendError {
    BackendError::Unavailable
}
fn before(e: BackendError) -> PortError<BackendError, ()> {
    PortError::BeforeCommit(e)
}
fn read(
    conn: &turso::Connection,
    runtime: &Handle,
    key: &str,
) -> Result<Option<Vec<u8>>, BackendError> {
    runtime.block_on(async {
        let mut rows = conn
            .query(
                "SELECT CASE WHEN length(value)<=65536 THEN value ELSE NULL END FROM mrr_backend_kv WHERE key=?1",
                [key],
            )
            .await
            .map_err(failure)?;
        let Some(row) = rows.next().await.map_err(failure)? else {
            return Ok(None);
        };
        match row.get_value(0).map_err(failure)? {
            turso::Value::Blob(bytes) => Ok(Some(bytes)),
            _ => Err(BackendError::Corrupt),
        }
    })
}
struct Transaction<'a> {
    conn: &'a turso::Connection,
    runtime: &'a Handle,
    finished: bool,
}
impl MetadataTransaction for Transaction<'_> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        read(self.conn, self.runtime, key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        if key.len() > 8192 || value.len() > 65536 {
            return Err(BackendError::Limit);
        }
        self.runtime
            .block_on(self.conn.execute(
                "INSERT INTO mrr_backend_kv VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [turso::Value::Text(key.into()), turso::Value::Blob(value.into())],
            ))
            .map_err(failure)?;
        Ok(())
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.runtime.block_on(self.conn.execute("ROLLBACK", ()));
        }
    }
}
impl TransactionProvider for TursoProvider {
    fn open_storage(&self) -> Result<(), BackendError> {
        let path = self
            .path
            .to_str()
            .filter(|p| !p.is_empty() && *p != ":memory:")
            .ok_or(BackendError::InvalidConfiguration)?;
        let mut writer = self.writer.lock().map_err(|_| BackendError::Unavailable)?;
        if writer.is_some() {
            return Err(BackendError::InvalidConfiguration);
        }
        let existed = self.path.exists();
        let db = self
            .runtime
            .block_on(turso::Builder::new_local(path).build())
            .map_err(failure)?;
        let conn = db.connect().map_err(failure)?;
        conn.busy_timeout(Duration::from_millis(250))
            .map_err(failure)?;
        self.runtime
            .block_on(conn.execute("PRAGMA synchronous=FULL", ()))
            .map_err(failure)?;
        self.runtime.block_on(async {
            let mut rows = conn
                .query("PRAGMA synchronous", ())
                .await
                .map_err(failure)?;
            let row = rows
                .next()
                .await
                .map_err(failure)?
                .ok_or(BackendError::Corrupt)?;
            if row.get::<i64>(0).map_err(failure)? != 2 {
                return Err(BackendError::UnsupportedCapabilities);
            }
            Ok(())
        })?;
        initialize(&conn, &self.runtime, existed)?;
        let reader = db.connect().map_err(failure)?;
        reader
            .busy_timeout(Duration::from_millis(250))
            .map_err(failure)?;
        *self.reader.lock().map_err(|_| BackendError::Unavailable)? = Some(reader);
        *self
            .database
            .lock()
            .map_err(|_| BackendError::Unavailable)? = Some(db);
        *writer = Some(conn);
        Ok(())
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let guard = self.reader.lock().map_err(|_| BackendError::Unavailable)?;
        read(
            guard.as_ref().ok_or(BackendError::NotReady)?,
            &self.runtime,
            key,
        )
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        let guard = self
            .writer
            .lock()
            .map_err(|_| before(BackendError::Unavailable))?;
        let conn = guard
            .as_ref()
            .ok_or_else(|| before(BackendError::NotReady))?;
        self.runtime
            .block_on(conn.execute("BEGIN IMMEDIATE", ()))
            .map_err(failure)
            .map_err(before)?;
        let mut tx = Transaction {
            conn,
            runtime: &self.runtime,
            finished: false,
        };
        run(&mut tx)?;
        self.runtime
            .block_on(conn.execute("COMMIT", ()))
            .map_err(failure)
            .map_err(PortError::Unknown)?;
        tx.finished = true;
        Ok(())
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        self.reader
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take();
        self.writer
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take();
        self.database
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take();
        Ok(())
    }
}
fn initialize(
    conn: &turso::Connection,
    runtime: &Handle,
    existed: bool,
) -> Result<(), BackendError> {
    runtime
        .block_on(conn.execute("BEGIN IMMEDIATE", ()))
        .map_err(failure)?;
    let mut tx = Transaction {
        conn,
        runtime,
        finished: false,
    };
    let tables = runtime.block_on(async {
        let mut rows = conn
            .query(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                (),
            )
            .await
            .map_err(failure)?;
        rows.next()
            .await
            .map_err(failure)?
            .ok_or(BackendError::Corrupt)?
            .get::<i64>(0)
            .map_err(failure)
    })?;
    if tables == 0 {
        if existed {
            return Err(BackendError::Corrupt);
        }
        runtime
            .block_on(conn.execute(
                "CREATE TABLE mrr_backend_kv (key TEXT PRIMARY KEY NOT NULL, value BLOB NOT NULL)",
                (),
            ))
            .map_err(failure)?;
        tx.put("mrr.backend.schema", SCHEMA.as_bytes())?;
    } else if tx
        .get("mrr.backend.schema")
        .map_err(|_| BackendError::Corrupt)?
        .as_deref()
        != Some(SCHEMA.as_bytes())
    {
        return Err(BackendError::Corrupt);
    }
    runtime
        .block_on(conn.execute("COMMIT", ()))
        .map_err(failure)?;
    tx.finished = true;
    Ok(())
}
