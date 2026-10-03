//! Local `SQLite` WAL/FULL metadata transaction provider. The Host protects the
//! database directory and supplies truthful block publication ACKs independently.
//! This provider does not detect rollback of a replaced database/backup.
use super::ProviderResult;
use crate::{
    BackendError, MetadataProvider, ProviderCapabilities, StoredOutcome, StoredRevision,
    StoredWrite,
};
use mrr_data_content::{
    ConditionalCommitDisposition, ConditionalCommitPortError as PortError,
    ConditionalContentReceipt, ContentRevision, PublishReceipt,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{path::PathBuf, sync::Mutex, time::Duration};
/// Separate writer and historical reader connections share the same database.
/// One writer per instance is bounded by the shared engine; `SQLite` also protects
/// transactions across independently opened processes/instances on the same file.
pub struct SqliteProvider {
    path: PathBuf,
    writer: Mutex<Option<Connection>>,
    reader: Mutex<Option<Connection>>,
}
impl SqliteProvider {
    /// Configuration only; directory ownership and local filesystem suitability
    /// are Host responsibilities. Network filesystems are outside qualification.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            writer: Mutex::new(None),
            reader: Mutex::new(None),
        }
    }
}
fn unavailable(_: rusqlite::Error) -> BackendError {
    BackendError::Unavailable
}
fn before(error: BackendError) -> PortError<BackendError, ()> {
    PortError::BeforeCommit(error)
}
fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, BackendError> {
    serde_json::to_vec(value).map_err(|_| BackendError::Corrupt)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, BackendError> {
    if bytes.len() > 16384 {
        return Err(BackendError::Corrupt);
    }
    serde_json::from_slice(bytes).map_err(|_| BackendError::Corrupt)
}
fn lookup(conn: &Connection, write: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
    let row: Option<(Vec<u8>, Vec<u8>)> = conn.query_row("SELECT CASE WHEN length(body)<=16384 THEN body END, CASE WHEN length(committed)<=16384 THEN committed END FROM mrr_operations WHERE profile=?1 AND namespace=?2 AND scope=?3 AND operation_id=?4", params![write.profile, write.namespace, write.scope, write.operation_id], |r| Ok((r.get(0)?, r.get(1)?))).optional().map_err(unavailable).map_err(before)?;
    let Some((body, committed)) = row else {
        return Ok(None);
    };
    let stored: StoredWrite = decode(&body).map_err(before)?;
    let committed: StoredRevision = decode(&committed).map_err(before)?;
    let receipt = ConditionalContentReceipt {
        write: stored.content_write(),
        committed: committed.into(),
    };
    write
        .content_write()
        .recover_receipt(Some(&receipt))
        .map_err(PortError::Protocol)?;
    if stored.profile != write.profile || stored.namespace != write.namespace {
        return Err(before(BackendError::Corrupt));
    }
    Ok(Some(committed))
}
impl MetadataProvider for SqliteProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            atomic_head_operation: true,
            durable_commit: true,
            historical_lookup: true,
        }
    }
    fn open(&self) -> Result<(), BackendError> {
        if self.path.as_os_str().is_empty() || self.path == std::path::Path::new(":memory:") {
            return Err(BackendError::InvalidConfiguration);
        }
        let mut writer = self.writer.lock().map_err(|_| BackendError::Unavailable)?;
        if writer.is_some() {
            return Err(BackendError::InvalidConfiguration);
        }
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(unavailable)?;
        conn.busy_timeout(Duration::from_millis(250))
            .map_err(unavailable)?;
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .map_err(unavailable)?;
        conn.execute_batch("PRAGMA synchronous=FULL;")
            .map_err(unavailable)?;
        let sync: i64 = conn
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(unavailable)?;
        if mode != "wal" || sync != 2 {
            return Err(BackendError::UnsupportedCapabilities);
        }
        initialize(&conn)?;
        let reader = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(unavailable)?;
        reader
            .busy_timeout(Duration::from_millis(250))
            .map_err(unavailable)?;
        *self.reader.lock().map_err(|_| BackendError::Unavailable)? = Some(reader);
        *writer = Some(conn);
        Ok(())
    }
    fn commit(
        &self,
        write: &StoredWrite,
        physical: Option<&PublishReceipt>,
        validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        let mut guard = self
            .writer
            .lock()
            .map_err(|_| before(BackendError::Unavailable))?;
        let conn = guard
            .as_mut()
            .ok_or_else(|| before(BackendError::NotReady))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)
            .map_err(before)?;
        if let Some(committed) = lookup(&tx, write)? {
            return Ok(StoredOutcome {
                committed,
                replayed: true,
            });
        }
        let head: Option<Vec<u8>> = tx
            .query_row(
                "SELECT CASE WHEN length(head)<=16384 THEN head END FROM mrr_heads WHERE profile=?1 AND namespace=?2 AND scope=?3",
                params![write.profile, write.namespace, write.scope],
                |r| r.get(0),
            )
            .optional()
            .map_err(unavailable)
            .map_err(before)?;
        let current: Option<StoredRevision> =
            head.as_deref().map(decode).transpose().map_err(before)?;
        let disposition = write
            .content_write()
            .decide_commit(current.map(Into::into), physical, None)
            .map_err(PortError::Protocol)?;
        let ConditionalCommitDisposition::Apply(next) = disposition else {
            return Err(before(BackendError::Corrupt));
        };
        if !validate(current.map(Into::into)) {
            return Err(PortError::Validation(()));
        }
        let committed = StoredRevision::from(next);
        let head = encode(&committed).map_err(before)?;
        let body = encode(write).map_err(before)?;
        tx.execute(
            "INSERT INTO mrr_operations VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                write.profile,
                write.namespace,
                write.scope,
                write.operation_id,
                body,
                head
            ],
        )
        .map_err(unavailable)
        .map_err(before)?;
        tx.execute("INSERT INTO mrr_heads VALUES (?1,?2,?3,?4) ON CONFLICT(profile,namespace,scope) DO UPDATE SET head=excluded.head", params![write.profile, write.namespace, write.scope, head]).map_err(unavailable).map_err(before)?;
        tx.commit()
            .map_err(unavailable)
            .map_err(PortError::Unknown)?;
        Ok(StoredOutcome {
            committed,
            replayed: false,
        })
    }
    fn recover(&self, write: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        // Validate the query even when the ledger has no row.
        write
            .content_write()
            .recover_receipt(None)
            .map_err(PortError::Protocol)?;
        let guard = self
            .reader
            .lock()
            .map_err(|_| before(BackendError::Unavailable))?;
        lookup(
            guard
                .as_ref()
                .ok_or_else(|| before(BackendError::NotReady))?,
            write,
        )
    }
    fn close(&self) -> Result<(), BackendError> {
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
        // FULL commits already crossed the WAL durability barrier. Closing does
        // not require truncating a WAL that another live backend may still use.
        Ok(())
    }
}

fn initialize(conn: &Connection) -> Result<(), BackendError> {
    let tx = rusqlite::Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
        .map_err(unavailable)?;
    let version: i64 = tx
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(unavailable)?;
    let application: i64 = tx
        .query_row("PRAGMA application_id", [], |r| r.get(0))
        .map_err(unavailable)?;
    if version == 0 && application == 0 {
        let tables: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .map_err(unavailable)?;
        if tables != 0 {
            return Err(BackendError::Corrupt);
        }
        tx.execute_batch("CREATE TABLE mrr_heads (profile TEXT NOT NULL, namespace TEXT NOT NULL, scope TEXT NOT NULL, head BLOB NOT NULL, PRIMARY KEY(profile,namespace,scope)) WITHOUT ROWID;
            CREATE TABLE mrr_operations (profile TEXT NOT NULL, namespace TEXT NOT NULL, scope TEXT NOT NULL, operation_id TEXT NOT NULL, body BLOB NOT NULL, committed BLOB NOT NULL, PRIMARY KEY(profile,namespace,scope,operation_id)) WITHOUT ROWID;
            PRAGMA application_id=1297240642;
            PRAGMA user_version=1;").map_err(unavailable)?;
    } else if version != 1 || application != 1_297_240_642 {
        return Err(BackendError::Corrupt);
    }
    // Never recreate missing authority tables from an existing database version.
    // Each live head must have an exactly matching completion row. This checks
    // structural continuity, not authenticity/anti-rollback of replaced files.
    let orphan: i64 = tx.query_row("SELECT count(*) FROM mrr_heads h WHERE NOT EXISTS (SELECT 1 FROM mrr_operations o WHERE o.profile=h.profile AND o.namespace=h.namespace AND o.scope=h.scope AND o.committed=h.head)", [], |r| r.get(0)).map_err(|_| BackendError::Corrupt)?;
    let unheaded: i64 = tx.query_row("SELECT count(*) FROM mrr_operations o WHERE NOT EXISTS (SELECT 1 FROM mrr_heads h WHERE h.profile=o.profile AND h.namespace=o.namespace AND h.scope=o.scope)", [], |r| r.get(0)).map_err(|_| BackendError::Corrupt)?;
    if orphan != 0 || unheaded != 0 {
        return Err(BackendError::Corrupt);
    }
    let integrity: String = tx
        .query_row("PRAGMA quick_check", [], |r| r.get(0))
        .map_err(unavailable)?;
    if integrity != "ok" {
        return Err(BackendError::Corrupt);
    }
    tx.commit().map_err(unavailable)
}
