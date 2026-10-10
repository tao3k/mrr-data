//! Consumer tests for the extracted upstream entry transport.
use super::contracts::{DataPolicy, config, get, key, put};
use anyhow::{Result, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Method, Request, Response, StatusCode},
    routing::any,
};
use cid::Cid;
use kache_remote::{
    config::{FilesystemRemoteConfig, RemoteBackendConfig, RemoteConfig, S3RemoteConfig},
    remote_backend::create_backend,
    remote_layout::RemoteLayout,
};
use kache_store::ArtifactStore;
use mrr_data_content::{ContentBlock, ContentCodec};
use std::{
    collections::BTreeMap,
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;

// All transport, packing, compression and extraction is upstream code. This
// consumer boundary only coordinates publication and verifies the MRR CID.
async fn fetch(
    store: &ArtifactStore<DataPolicy>,
    remote: &RemoteLayout<'_>,
    cid: &Cid,
) -> Result<Option<Vec<u8>>> {
    if let Some(bytes) = get(store, cid)? {
        return Ok(Some(bytes));
    }
    let cache_key = key(cid);
    let _lock = store
        .try_lock(&cache_key)?
        .ok_or_else(|| anyhow::anyhow!("cache key busy"))?;
    if let Some(bytes) = get(store, cid)? {
        return Ok(Some(bytes));
    }
    if !remote.exists_entry(&cache_key, "mrr-data").await? {
        return Ok(None);
    }
    let staging = tempfile::tempdir_in(store.cache_dir())?;
    let entry = staging.path().join(&cache_key);
    remote
        .download_entry_until(&cache_key, "mrr-data", &entry, &store.blobs_dir(), None)
        .await?;
    let bytes = fs::read(entry.join("block"))?;
    // The test transports CAR archives as raw blocks; CID checks happen before
    // registration, and CAR admission subsequently checks snapshot closure.
    ensure!(
        ContentBlock::new(ContentCodec::Raw, &bytes).cid() == *cid,
        "MRR CID mismatch"
    );
    fs::rename(entry, store.entry_dir(&cache_key))?;
    store.import_restored_entry(&cache_key)?;
    get(store, cid)
}

async fn upload(
    store: &ArtifactStore<DataPolicy>,
    remote: &RemoteLayout<'_>,
    cid: &Cid,
) -> Result<()> {
    let cache_key = key(cid);
    let _lock = store
        .try_lock(&cache_key)?
        .ok_or_else(|| anyhow::anyhow!("cache key busy"))?;
    // Hold upstream GC ownership until both remote PUTs have completed.
    let _gc = store.acquire_gc_lock()?;
    remote
        .upload_entry_until(
            &cache_key,
            "mrr-data",
            &store.entry_dir(&cache_key),
            &store.blobs_dir(),
            3,
            None,
        )
        .await?;
    Ok(())
}

#[derive(Default)]
struct Objects {
    bytes: Mutex<BTreeMap<String, Vec<u8>>>,
    gets: AtomicUsize,
    reject_manifest: AtomicBool,
}

struct S3Wire {
    objects: Arc<Objects>,
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for S3Wire {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl S3Wire {
    async fn start() -> Result<Self> {
        let objects = Arc::new(Objects::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let app = Router::new()
            .fallback(any(object_request))
            .with_state(Arc::clone(&objects));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            objects,
            endpoint,
            task,
        })
    }
    fn config(&self) -> RemoteConfig {
        RemoteConfig {
            prefix: "mrr-probe".into(),
            backend: RemoteBackendConfig::S3(S3RemoteConfig {
                bucket: "probe".into(),
                endpoint: Some(self.endpoint.clone()),
                region: "us-east-1".into(),
                profile: None,
                user_agent: None,
            }),
        }
    }
}

async fn object_request(
    State(objects): State<Arc<Objects>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_owned();
    // Wire test only: require the real client's SigV4 header, without pretending
    // to validate credentials or reproduce a hosted provider's authentication.
    if !request
        .headers()
        .get("authorization")
        .is_some_and(|h| h.as_bytes().starts_with(b"AWS4-HMAC-SHA256"))
    {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .unwrap();
    }
    let method = request.method().clone();
    if method == Method::PUT {
        if path.contains("/manifests/") && objects.reject_manifest.load(Ordering::SeqCst) {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::empty())
                .unwrap();
        }
        let bytes = to_bytes(request.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap();
        objects.bytes.lock().unwrap().insert(path, bytes.to_vec());
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", "\"probe\"")
            .body(Body::empty())
            .unwrap();
    }
    if method == Method::GET {
        objects.gets.fetch_add(1, Ordering::SeqCst);
    }
    let Some(bytes) = objects.bytes.lock().unwrap().get(&path).cloned() else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    let length = bytes.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(bytes)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", length)
        .header("etag", "\"probe\"")
        .body(body)
        .unwrap()
}

#[tokio::test]
async fn filesystem_remote_restores_into_an_empty_cache() -> Result<()> {
    let a = tempdir()?;
    let b = tempdir()?;
    let remote_root = tempdir()?;
    let remote_config = RemoteConfig {
        prefix: "mrr".into(),
        backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
            root: remote_root.path().join("objects"),
            atomic_write_dir: remote_root.path().join("staging"),
        }),
    };
    let backend = create_backend(&remote_config, 30).await?;
    let remote = RemoteLayout::new(backend.as_ref(), &remote_config);
    let source = ArtifactStore::<DataPolicy>::open(config(a.path(), 1_000_000))?;
    let target = ArtifactStore::<DataPolicy>::open(config(b.path(), 1_000_000))?;
    let cid = put(
        &source,
        ContentBlock::new(ContentCodec::Raw, b"filesystem roundtrip"),
    )?;
    assert!(get(&target, &cid)?.is_none());
    upload(&source, &remote, &cid).await?;
    assert_eq!(
        fetch(&target, &remote, &cid).await?,
        Some(b"filesystem roundtrip".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn s3_wire_restores_car_and_warm_reads_do_not_download_again() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote_config = wire.config();
    let backend = create_backend(&remote_config, 30).await?;
    let remote = RemoteLayout::new(backend.as_ref(), &remote_config);
    let a = tempdir()?;
    let b = tempdir()?;
    let source = ArtifactStore::<DataPolicy>::open(config(a.path(), 1_000_000))?;
    let target = ArtifactStore::<DataPolicy>::open(config(b.path(), 1_000_000))?;
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let archive = mrr_data_content::encode_snapshot_car(&snapshot, &children)?;
    let cid = put(&source, ContentBlock::new(ContentCodec::Raw, &archive))?;
    upload(&source, &remote, &cid).await?;
    let downloaded = fetch(&target, &remote, &cid).await?.unwrap();
    let memory = mrr_data_content::MemoryContentStore::default();
    let imported = mrr_data_content::import_snapshot_car(
        &downloaded,
        mrr_data_content::CarImportLimits::new(1_000_000, 10, 1_000_000, 1_000_000),
        &relations,
        &entities,
        &memory,
    )?;
    assert_eq!(imported.root(), snapshot.cid());
    assert_eq!(imported.manifest(), snapshot.manifest());
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), 1);
    assert_eq!(fetch(&target, &remote, &cid).await?, Some(archive));
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), 1);
    drop(target);
    let reopened = ArtifactStore::<DataPolicy>::open(config(b.path(), 1_000_000))?;
    assert!(fetch(&reopened, &remote, &cid).await?.is_some());
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_manifest_publication_is_not_reported_as_uploaded_or_fetchable() -> Result<()> {
    let wire = S3Wire::start().await?;
    wire.objects.reject_manifest.store(true, Ordering::SeqCst);
    let remote_config = wire.config();
    let backend = create_backend(&remote_config, 30).await?;
    let remote = RemoteLayout::new(backend.as_ref(), &remote_config);
    let a = tempdir()?;
    let b = tempdir()?;
    let source = ArtifactStore::<DataPolicy>::open(config(a.path(), 1_000_000))?;
    let target = ArtifactStore::<DataPolicy>::open(config(b.path(), 1_000_000))?;
    let cid = put(
        &source,
        ContentBlock::new(ContentCodec::Raw, b"not published"),
    )?;
    assert!(upload(&source, &remote, &cid).await.is_err());
    assert!(
        wire.objects
            .bytes
            .lock()
            .unwrap()
            .keys()
            .any(|key| key.contains("/packs/"))
    );
    assert!(fetch(&target, &remote, &cid).await?.is_none());
    assert_eq!(target.entry_count()?, 0);
    // The caller can explicitly retry through the same upstream entry API.
    wire.objects.reject_manifest.store(false, Ordering::SeqCst);
    upload(&source, &remote, &cid).await?;
    assert!(fetch(&target, &remote, &cid).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn valid_pack_for_another_cid_is_rejected_before_cache_registration() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote_config = wire.config();
    let backend = create_backend(&remote_config, 30).await?;
    let remote = RemoteLayout::new(backend.as_ref(), &remote_config);
    let a = tempdir()?;
    let b = tempdir()?;
    let source = ArtifactStore::<DataPolicy>::open(config(a.path(), 1_000_000))?;
    let target = ArtifactStore::<DataPolicy>::open(config(b.path(), 1_000_000))?;
    let expected = put(&source, ContentBlock::new(ContentCodec::Raw, b"expected"))?;
    let other = put(&source, ContentBlock::new(ContentCodec::Raw, b"different"))?;
    upload(&source, &remote, &expected).await?;
    upload(&source, &remote, &other).await?;
    let pack_path = |cid: &Cid| format!("/probe/mrr-probe/v3/packs/mrr-data/{}.tar.zst", key(cid));
    {
        let mut objects = wire.objects.bytes.lock().unwrap();
        let wrong = objects[&pack_path(&other)].clone();
        objects.insert(pack_path(&expected), wrong);
    }
    let error = fetch(&target, &remote, &expected).await.unwrap_err();
    assert!(error.to_string().contains("MRR CID mismatch"));
    assert_eq!(target.entry_count()?, 0);
    assert!(!target.entry_dir(&key(&expected)).exists());
    Ok(())
}

// A real MRR manifest and CAR closure; its physical batch is an opaque fixture.
type SnapshotFixture = (
    mrr_data_core::SnapshotBlock,
    Vec<ContentBlock<'static>>,
    meta_relational_reasoning::RelationCatalog,
    meta_relational_reasoning::EntityCatalog,
);
fn snapshot_fixture() -> SnapshotFixture {
    use meta_relational_reasoning::{
        EntityCatalog, EntityId, EntitySchema, ExternalRevisionIdentity, GenerationId,
        RelationCatalog, RelationField, RelationId, RelationSchema, RevisionBinding,
        SemanticSnapshot, ValueSchema,
    };
    use mrr_data_core::{
        BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
        SnapshotManifest, SnapshotManifestRequest, raw_cid,
    };
    let relation = RelationId::from_canonical_bytes("probe:relation").unwrap();
    let relations = RelationCatalog::admit(vec![
        RelationSchema::new(
            relation,
            "probe",
            vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let entities = EntityCatalog::admit(vec![
        EntitySchema::new(
            EntityId::from_canonical_bytes("probe:entity").unwrap(),
            "Probe",
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let generation = GenerationId::from_canonical_bytes("probe:generation").unwrap();
    let semantic = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "probe", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let data = b"physical-batch";
    let coverage = b"coverage";
    let manifest = SnapshotManifest::admit(SnapshotManifestRequest::new(
        semantic,
        &relations,
        &entities,
        vec![
            RelationDescriptor::new(
                relation,
                1,
                vec![BatchDescriptor::new(raw_cid(data), 1, data.len() as u64).unwrap()],
            )
            .unwrap(),
        ],
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(coverage)).unwrap(),
    ))
    .unwrap();
    (
        SnapshotBlock::encode(manifest).unwrap(),
        vec![
            ContentBlock::new(ContentCodec::Raw, data),
            ContentBlock::new(ContentCodec::Raw, coverage),
        ],
        relations,
        entities,
    )
}
