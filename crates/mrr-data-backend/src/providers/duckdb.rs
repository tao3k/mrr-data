//! Optional `DuckDB` native provider; physical schema/query details stay here.
use super::{MetadataTransaction, ProviderResult, TransactionProvider, duckdb_database};
use crate::BackendError;
use duckdb::{Connection, OptionalExt, params};
use mrr_data_content::ConditionalCommitPortError as PortError;
use mrr_data_profile::BACKEND_DUCKDB_SCHEMA;
use std::{path::PathBuf, sync::Mutex};
/// Local `DuckDB` metadata capability. Deploy within a single writer process;
/// this adapter does not declare a distributed/multi-process write service.
pub struct DuckDbProvider {
    path: PathBuf,
    writer: Mutex<Option<Connection>>,
    reader: Mutex<Option<Connection>>,
    database: Mutex<Option<duckdb_database::SharedDatabase>>,
}
impl DuckDbProvider {
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            writer: Mutex::new(None),
            reader: Mutex::new(None),
            database: Mutex::new(None),
        }
    }
}
fn failure(_: duckdb::Error) -> BackendError {
    BackendError::Unavailable
}
fn before(e: BackendError) -> PortError<BackendError, ()> {
    PortError::BeforeCommit(e)
}
fn read(conn: &Connection, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
    conn.query_row(
        "SELECT CASE WHEN octet_length(value)<=65536 THEN value ELSE NULL END FROM mrr_backend_kv WHERE key=?1",
        [key],
        |row| row.get(0),
    )
    .optional()
    .map_err(failure)
}
struct Transaction<'a> {
    conn: &'a Connection,
    finished: bool,
}
impl MetadataTransaction for Transaction<'_> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        read(self.conn, key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        if key.len() > 8192 || value.len() > 65536 {
            return Err(BackendError::Limit);
        }
        self.conn
            .execute(
                "INSERT INTO mrr_backend_kv VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(failure)?;
        Ok(())
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}
impl TransactionProvider for DuckDbProvider {
    fn open_storage(&self) -> Result<(), BackendError> {
        if self.path.as_os_str().is_empty() || self.path == std::path::Path::new(":memory:") {
            return Err(BackendError::InvalidConfiguration);
        }
        let mut writer = self.writer.lock().map_err(|_| BackendError::Unavailable)?;
        if writer.is_some() {
            return Err(BackendError::InvalidConfiguration);
        }
        let database = duckdb_database::open(&self.path, initialize)?;
        let anchor = database.lock().map_err(|_| BackendError::Unavailable)?;
        let conn = anchor.try_clone().map_err(failure)?;
        let reader = anchor.try_clone().map_err(failure)?;
        drop(anchor);
        *self.reader.lock().map_err(|_| BackendError::Unavailable)? = Some(reader);
        *self
            .database
            .lock()
            .map_err(|_| BackendError::Unavailable)? = Some(database);
        *writer = Some(conn);
        Ok(())
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let guard = self.reader.lock().map_err(|_| BackendError::Unavailable)?;
        read(guard.as_ref().ok_or(BackendError::NotReady)?, key)
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
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(failure)
            .map_err(before)?;
        let mut tx = Transaction {
            conn,
            finished: false,
        };
        run(&mut tx)?;
        conn.execute_batch("COMMIT")
            .map_err(failure)
            .map_err(PortError::Unknown)?;
        tx.finished = true;
        Ok(())
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        if let Some(reader) = self
            .reader
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take()
        {
            reader.close().map_err(|_| BackendError::Unavailable)?;
        }
        if let Some(writer) = self
            .writer
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take()
        {
            writer.close().map_err(|_| BackendError::Unavailable)?;
        }
        if let Some(database) = self
            .database
            .lock()
            .map_err(|_| BackendError::Unavailable)?
            .take()
        {
            duckdb_database::close(database)?;
        }
        Ok(())
    }
}
fn initialize(conn: &Connection, existed: bool) -> Result<(), BackendError> {
    conn.execute_batch("BEGIN TRANSACTION").map_err(failure)?;
    let mut tx = Transaction {
        conn,
        finished: false,
    };
    let tables: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='main'",
            [],
            |r| r.get(0),
        )
        .map_err(failure)?;
    if tables == 0 {
        if existed {
            return Err(BackendError::Corrupt);
        }
        conn.execute_batch(
            "CREATE TABLE mrr_backend_kv (key VARCHAR PRIMARY KEY NOT NULL, value BLOB NOT NULL)",
        )
        .map_err(failure)?;
        tx.put(
            "mrr.backend.schema",
            &crate::scheme_record::schema_marker(BACKEND_DUCKDB_SCHEMA),
        )?;
    } else if tx
        .get("mrr.backend.schema")
        .map_err(|_| BackendError::Corrupt)?
        .as_deref()
        != Some(crate::scheme_record::schema_marker(BACKEND_DUCKDB_SCHEMA).as_slice())
    {
        return Err(BackendError::Corrupt);
    }
    conn.execute_batch("COMMIT").map_err(failure)?;
    tx.finished = true;
    Ok(())
}
