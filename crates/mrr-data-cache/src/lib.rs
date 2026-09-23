//! Concrete adapters for the MRR content protocol. Cache mechanisms are Kache's;
//! S3 transport, credentials and signing are `OpenDAL`'s.
#![forbid(unsafe_code)]

#[cfg(feature = "kache")]
mod local;
#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "kache")]
pub use local::KacheContentStore;
#[cfg(feature = "s3")]
pub use opendal_service_s3::S3 as S3Config;
#[cfg(feature = "s3")]
pub use reqwest::Client as HttpClient;
#[cfg(feature = "s3")]
pub use s3::{S3ContentStore, http_client_builder};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;

#[cfg(feature = "blocking")]
mod blocking;
#[cfg(feature = "blocking")]
pub use blocking::BlockingContentStore;
