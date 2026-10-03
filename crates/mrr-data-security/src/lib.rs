//! Provider-neutral security bindings over immutable MRR Data snapshots.
#![forbid(unsafe_code)]

pub mod data_protection;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
