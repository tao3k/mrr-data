//! Concrete content protocol adapter.
use crate::{HttpClient, S3Config};
use cid::Cid;
use futures::TryStreamExt;
use mrr_data_content::{ContentBlock, RemoteContentStore, RemoteError, RemoteFuture};
use opendal::{ErrorKind, HttpTransporter, OperationContext, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use std::time::Duration;

/// Creates a TLS-enabled client builder, installing ring only if the process
/// has not selected a rustls provider. Certificate validation remains enabled.
/// Callers can configure CA certificates and client identity before `build`.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    let _ = rustls::crypto::ring::default_provider().install_default();
    HttpClient::builder()
}

/// S3-compatible immutable CID objects, using `OpenDAL` signing and transport.
/// Configure bucket/root/endpoint/credentials through `S3Config`; customize CA
/// certificates or client identity through the supplied `HttpClient`.
#[derive(Clone)]
pub struct S3ContentStore {
    operator: Operator,
    deadline: Duration,
}

fn remote_error(error: &opendal::Error) -> RemoteError {
    match error.kind() {
        ErrorKind::PermissionDenied => RemoteError::PermissionDenied,
        ErrorKind::ConfigInvalid | ErrorKind::Unsupported => RemoteError::InvalidConfiguration,
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists => RemoteError::Conflict,
        _ => RemoteError::Unavailable,
    }
}
fn key(cid: &Cid) -> String {
    format!("blocks/{cid}")
}

impl S3ContentStore {
    /// Creates an adapter with a total deadline for each operation, including
    /// response body streaming. No network requests are made during construction.
    /// # Errors
    /// Returns invalid configuration for a zero deadline or invalid S3 builder.
    pub fn new(
        config: S3Config,
        client: HttpClient,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        if deadline.is_zero() {
            return Err(RemoteError::InvalidConfiguration);
        }
        let context = OperationContext::new()
            .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));
        let operator = Operator::new(config)
            .map_err(|e| remote_error(&e))?
            .with_context(context);
        Ok(Self { operator, deadline })
    }

    async fn read(&self, cid: &Cid, max_bytes: usize) -> Result<Option<Vec<u8>>, RemoteError> {
        let codec =
            mrr_data_content::ContentCodec::from_cid(cid).map_err(|_| RemoteError::Corrupt)?;
        let reader = match self.operator.reader(&key(cid)).await {
            Ok(reader) => reader,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(remote_error(&e)),
        };
        let mut stream = match reader.into_stream(..).await {
            Ok(stream) => stream,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(remote_error(&e)),
        };
        let mut bytes = Vec::new();
        loop {
            let chunk = match stream.try_next().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) if e.kind() == ErrorKind::NotFound && bytes.is_empty() => return Ok(None),
                Err(e) => return Err(remote_error(&e)),
            };
            if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
                return Err(RemoteError::TooLarge);
            }
            for part in chunk {
                bytes.extend_from_slice(&part);
            }
        }
        if ContentBlock::new(codec, &bytes).cid() != *cid {
            return Err(RemoteError::Corrupt);
        }
        Ok(Some(bytes))
    }
}

impl RemoteContentStore for S3ContentStore {
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            tokio::time::timeout(self.deadline, self.read(cid, max_bytes))
                .await
                .map_err(|_| RemoteError::DeadlineExceeded)?
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::timeout(self.deadline, async {
                let cid = block.cid();
                match self
                    .operator
                    .write_with(&key(&cid), block.bytes().to_vec())
                    .if_not_exists(true)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e)
                        if matches!(
                            e.kind(),
                            ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
                        ) =>
                    {
                        match self.read(&cid, block.bytes().len()).await? {
                            Some(bytes) if bytes == block.bytes() => Ok(()),
                            _ => Err(RemoteError::Conflict),
                        }
                    }
                    Err(e) => Err(remote_error(&e)),
                }
            })
            .await
            .map_err(|_| RemoteError::DeadlineExceeded)?
        })
    }
}
