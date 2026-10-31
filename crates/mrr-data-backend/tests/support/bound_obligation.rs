//! Representative Host content validation, using real Data integrity and authority APIs.
use mrr_data_backend::{AuthorityExpectation, AuthorityProposal, AuthorityStatus, ProfilePort};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentRevision, ContentStore, FilesystemContentStore,
};
use std::{path::Path, sync::Arc, time::Instant};

#[derive(Default, serde::Serialize)]
pub struct ValidationCost {
    pub read_integrity_ns: u128,
    pub decode_compare_ns: u128,
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Body {
    scope: String,
    claim: String,
    reservation: String,
    unknown_exposure: String,
    history: String,
    held: u64,
    reserved: u64,
    burned: bool,
    payload: String,
}
pub struct BoundObligation {
    store: FilesystemContentStore,
    body: Body,
    root: cid::Cid,
    guards: Vec<AuthorityExpectation>,
}
impl BoundObligation {
    pub async fn prepare(port: ProfilePort, path: &Path, scope: &str) -> (ProfilePort, Arc<Self>) {
        let body = Body {
            scope: scope.into(),
            claim: format!("original-claim:{scope}"),
            reservation: format!("original-reservation:{scope}"),
            unknown_exposure: format!("original-exposure:{scope}"),
            history: format!("original-history:{scope}"),
            held: 4,
            reserved: 4,
            burned: true,
            payload: "x".repeat(8192),
        };
        let store = FilesystemContentStore::open(path).unwrap();
        let bytes = serde_json::to_vec(&body).unwrap();
        let root = store
            .put(ContentBlock::new(ContentCodec::Raw, &bytes))
            .unwrap();
        let state = port
            .advance_authority(
                scope,
                AuthorityProposal {
                    authority_id: "bound-profile".into(),
                    expected: None,
                    replacement: root,
                    status: AuthorityStatus::Active,
                },
            )
            .await
            .unwrap();
        let guards = vec![AuthorityExpectation {
            authority_id: "bound-profile".into(),
            state,
        }];
        let port = port.with_authorities(&guards).unwrap();
        (
            port,
            Arc::new(Self {
                store,
                body,
                root,
                guards,
            }),
        )
    }
    pub fn guards(&self) -> &[AuthorityExpectation] {
        &self.guards
    }
    pub fn root(&self) -> cid::Cid {
        self.root
    }
    pub fn validate(
        &self,
        current: Option<ContentRevision>,
        expected: Option<ContentRevision>,
    ) -> Result<(), ()> {
        self.validate_timed(current, expected).map(|_| ())
    }
    pub fn validate_timed(
        &self,
        current: Option<ContentRevision>,
        expected: Option<ContentRevision>,
    ) -> Result<ValidationCost, ()> {
        let start = Instant::now();
        let bytes = self.store.get_bounded(&self.root, 16384).map_err(|_| ())?;
        let read_integrity_ns = start.elapsed().as_nanos();
        let start = Instant::now();
        let body: Body = serde_json::from_slice(&bytes).map_err(|_| ())?;
        (body == self.body && current == expected)
            .then(|| ValidationCost {
                read_integrity_ns,
                decode_compare_ns: start.elapsed().as_nanos(),
            })
            .ok_or(())
    }
}
