use cid::Cid;
use mrr_data_content::{ContentBlock, RemoteContentStore, RemoteError, RemoteFuture};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Default)]
pub struct Remote {
    pub blocks: Mutex<BTreeMap<Cid, Vec<u8>>>,
    pub writes: Mutex<Vec<Cid>>,
    pub fail: Mutex<Option<Cid>>,
    pub lost_ack: Mutex<Option<Cid>>,
    pub read_bytes: AtomicU64,
    pub read_blocks: AtomicU64,
}
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let blocks = self.blocks.lock().unwrap();
            if blocks.get(cid).is_some_and(|b| b.len() > max_bytes) {
                return Err(RemoteError::TooLarge);
            }
            let result = blocks.get(cid).cloned();
            if let Some(bytes) = &result {
                self.read_blocks.fetch_add(1, Ordering::Relaxed);
                self.read_bytes
                    .fetch_add(u64::try_from(bytes.len()).unwrap(), Ordering::Relaxed);
            }
            Ok(result)
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.writes.lock().unwrap().push(block.cid());
            if *self.fail.lock().unwrap() == Some(block.cid()) {
                return Err(RemoteError::Unavailable);
            }
            self.blocks
                .lock()
                .unwrap()
                .insert(block.cid(), block.bytes().to_vec());
            if *self.lost_ack.lock().unwrap() == Some(block.cid()) {
                return Err(RemoteError::Unavailable);
            }
            Ok(())
        })
    }
}
