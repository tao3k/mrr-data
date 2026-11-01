//! Test-only physical transactions; logical authority/CAS/replay stays in Backend.
//! State survives reopening this object, not process exit or power loss.
use mrr_data_backend::{
    BackendError,
    providers::{MetadataTransaction, ProviderResult, TransactionProvider},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Default)]
pub(super) struct SimulatedMetadata {
    committed: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    writer: Arc<Mutex<()>>,
}
struct Working(BTreeMap<String, Vec<u8>>);
impl MetadataTransaction for Working {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.0.get(key).cloned())
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        if value.len() > 65_536 {
            return Err(BackendError::Limit);
        }
        self.0.insert(key.into(), value.to_vec());
        Ok(())
    }
}
impl TransactionProvider for SimulatedMetadata {
    fn open_storage(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.committed.lock().unwrap().get(key).cloned())
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        let _writer = self.writer.lock().unwrap();
        let mut working = Working(self.committed.lock().unwrap().clone());
        run(&mut working)?;
        *self.committed.lock().unwrap() = working.0;
        Ok(())
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        Ok(())
    }
}
