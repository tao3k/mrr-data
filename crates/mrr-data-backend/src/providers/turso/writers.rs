//! Exclusive, bounded physical writers. No semantic authority is cached here.
use super::connection::{failure, full_writer};
use crate::BackendError;
use std::{sync::Mutex, time::Duration};
use tokio::runtime::Handle;

#[derive(Default)]
pub(super) struct Writers(Mutex<State>);
#[derive(Default)]
struct State {
    database: Option<turso::Database>,
    idle: Vec<turso::Connection>,
    leased: usize,
    capacity: Option<usize>,
    phase: Phase,
}
#[derive(Default, PartialEq)]
enum Phase {
    #[default]
    Fresh,
    Open,
    Closing,
    Closed,
}
impl Writers {
    pub(super) fn ensure_fresh(&self) -> Result<(), BackendError> {
        let state = self.0.lock().map_err(|_| BackendError::Unavailable)?;
        (state.phase == Phase::Fresh)
            .then_some(())
            .ok_or(BackendError::InvalidConfiguration)
    }
    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        let state = self.0.lock().unwrap();
        (state.leased, state.idle.len())
    }
    pub(super) fn open(
        &self,
        database: turso::Database,
        capacity: Option<usize>,
    ) -> Result<(), BackendError> {
        let mut state = self.0.lock().map_err(|_| BackendError::Unavailable)?;
        if state.phase != Phase::Fresh || state.database.is_some() || state.leased != 0 {
            return Err(BackendError::InvalidConfiguration);
        }
        state.database = Some(database);
        state.capacity = capacity;
        state.phase = Phase::Open;
        Ok(())
    }
    pub(super) fn checkout(&self, runtime: &Handle) -> Result<Writer<'_>, BackendError> {
        let (database, conn) = {
            let mut state = self.0.lock().map_err(|_| BackendError::Unavailable)?;
            if state.phase != Phase::Open {
                return Err(BackendError::NotReady);
            }
            let database = state.database.clone().ok_or(BackendError::NotReady)?;
            if state.capacity.is_some_and(|cap| state.leased >= cap) {
                return Err(BackendError::Saturated);
            }
            state.leased = state.leased.checked_add(1).ok_or(BackendError::Limit)?;
            (database, state.idle.pop())
        };
        // Own the slot before fallible configuration; Drop returns it on failure.
        let mut writer = Writer {
            owner: self,
            conn,
            healthy: false,
        };
        if writer.conn.is_none() {
            let conn = database.connect().map_err(failure)?;
            conn.busy_timeout(Duration::from_millis(250))
                .map_err(failure)?;
            full_writer(&conn, runtime)?;
            writer.conn = Some(conn);
        }
        Ok(writer)
    }
    pub(super) fn close(&self) -> Result<(), BackendError> {
        let mut state = self.0.lock().map_err(|_| BackendError::Unavailable)?;
        state.phase = Phase::Closing;
        state.idle.clear();
        if state.leased != 0 {
            return Err(BackendError::NotReady);
        }
        state.database.take();
        state.phase = Phase::Closed;
        Ok(())
    }
}
pub(super) struct Writer<'a> {
    owner: &'a Writers,
    conn: Option<turso::Connection>,
    healthy: bool,
}
impl Writer<'_> {
    pub(super) fn connection(&self) -> Result<&turso::Connection, BackendError> {
        self.conn.as_ref().ok_or(BackendError::NotReady)
    }
    pub(super) fn committed(&mut self) {
        self.healthy = self
            .conn
            .as_ref()
            .is_some_and(|c| matches!(c.is_autocommit(), Ok(true)));
    }
}
impl Drop for Writer<'_> {
    fn drop(&mut self) {
        let conn = self.conn.take();
        if self.healthy
            && let Ok(mut state) = self.owner.0.lock()
            && state.phase == Phase::Open
            && state.capacity.is_some()
        {
            state.idle.extend(conn);
            state.leased -= 1;
            return;
        }
        // Discard/finish physical cleanup before making capacity available.
        drop(conn);
        if let Ok(mut state) = self.owner.0.lock() {
            state.leased -= 1;
        }
    }
}
