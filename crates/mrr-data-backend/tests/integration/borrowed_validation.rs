//! Actual native completion must not move a borrowed caller guard or grant twice.
use super::{AuthorityStatus, Fixture, Outcome, PausePoint, PortError, port, run};
use mrr_data_content::ConditionalContentCommitPort;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
struct ScopeGuard<'a>(&'a AtomicBool);
impl ScopeGuard<'_> {
    fn check(&self) {
        assert!(self.0.load(Ordering::Acquire));
    }
}
impl Drop for ScopeGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
#[test]
fn borrowed_guard_stays_on_caller_until_original_native_completion() {
    run(async {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::open(dir.path(), PausePoint::BeforeCommit).await;
        f.pause.arm();
        let live = AtomicBool::new(true);
        let guard = ScopeGuard(&live);
        let caller_thread = std::thread::current().id();
        let p = port(&f.backend, f.authority);
        {
            let pending = p.commit(f.consume(), Some(&f.physical), |_| {
                guard.check(); // Intentionally borrow stack state; never 'static.
                assert_eq!(std::thread::current().id(), caller_thread);
                Ok::<_, ()>(())
            });
            tokio::pin!(pending);
            tokio::select! {
                () = f.pause.entered() => {},
                result = &mut pending => panic!("native commit finished before release: {result:?}"),
            }
            guard.check();
            assert_eq!(f.backend.status().active_writes, 1);
            f.pause.release();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), pending)
                    .await
                    .unwrap()
                    .unwrap(),
                Outcome::Committed(_)
            ));
        }
        guard.check();
        drop(guard);
        assert!(!live.load(Ordering::Acquire));
        f.backend.shutdown().await.unwrap();
        f.recovered(dir.path(), true, AuthorityStatus::Active).await;
    });
}
#[test]
fn typed_validator_rejection_preserves_original_unconsumed_obligation() {
    run(async {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::open(dir.path(), PausePoint::BeforeDecision).await;
        let p = port(&f.backend, f.authority);
        assert!(matches!(
            p.commit(f.consume(), Some(&f.physical), |_| Err("denied"))
                .await,
            Err(PortError::Validation("denied"))
        ));
        tokio::time::timeout(Duration::from_secs(3), f.backend.shutdown())
            .await
            .unwrap()
            .unwrap();
        f.recovered(dir.path(), false, AuthorityStatus::Active)
            .await;
    });
}
#[test]
fn validator_panic_closes_pending_answer_and_drains_without_consumption() {
    run(async {
        let dir = tempfile::tempdir().unwrap();
        let f = Arc::new(Fixture::open(dir.path(), PausePoint::BeforeDecision).await);
        let work = f.clone();
        let caller = tokio::spawn(async move {
            port(&work.backend, work.authority)
                .commit(
                    work.consume(),
                    Some(&work.physical),
                    |_| -> Result<(), ()> { panic!("deliberate validator panic") },
                )
                .await
                .map(|_| ())
        });
        assert!(caller.await.unwrap_err().is_panic());
        tokio::time::timeout(Duration::from_secs(3), f.backend.shutdown())
            .await
            .unwrap()
            .unwrap();
        f.recovered(dir.path(), false, AuthorityStatus::Active)
            .await;
    });
}
