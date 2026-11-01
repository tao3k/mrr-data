//! Turso provider interface and private physical writer ownership.
mod connection;
mod provider;
#[cfg(test)]
#[path = "../../../tests/unit/native_read_timing.rs"]
pub(crate) mod read_timing;
mod writers;
pub use provider::TursoProvider;

#[cfg(test)]
#[path = "../../../tests/unit/turso_writers.rs"]
mod tests;
