//! Native closure publication, cold restore and direct-IR result acceptance.
mod acceptance;
mod fixture;
mod remote;

#[cfg(feature = "backend")]
mod backend;

#[cfg(feature = "backend")]
mod content;
#[cfg(feature = "backend")]
mod content_store;
