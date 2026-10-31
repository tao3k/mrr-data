//! Private verified physical connection configuration.
use crate::BackendError;
use tokio::runtime::Handle;
pub(super) fn failure(_: turso::Error) -> BackendError {
    BackendError::Unavailable
}
pub(super) fn full_writer(conn: &turso::Connection, runtime: &Handle) -> Result<(), BackendError> {
    runtime.block_on(async {
        conn.execute("PRAGMA synchronous=FULL", ())
            .await
            .map_err(failure)?;
        let mut rows = conn
            .query("PRAGMA synchronous", ())
            .await
            .map_err(failure)?;
        let row = rows
            .next()
            .await
            .map_err(failure)?
            .ok_or(BackendError::Corrupt)?;
        match row.get::<i64>(0).map_err(failure)? {
            2 => Ok(()),
            _ => Err(BackendError::UnsupportedCapabilities),
        }
    })
}
