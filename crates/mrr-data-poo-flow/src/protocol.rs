//! Bounded data envelopes for the fixed static-edge resource; no query syntax.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const PROFILE: &str = "poo-flow.static-edges.v1";
pub(crate) const MAX_INPUT: usize = 1024 * 1024;
pub(crate) const MAX_EDGES: usize = 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) profile: String,
    pub(crate) source: String,
    pub(crate) revision: String,
    pub(crate) operation: Operation,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Protect { edges: Vec<[String; 2]> },
    Publish { edges: Vec<[String; 2]> },
    Sync { root: String },
    Query { root: String },
}
#[derive(Serialize)]
pub(crate) struct Receipt {
    pub(crate) profile: &'static str,
    pub(crate) producer: &'static str,
    pub(crate) request_sha256: String,
    pub(crate) source: String,
    pub(crate) revision: String,
    pub(crate) root: String,
    pub(crate) generation: String,
    pub(crate) result: Outcome,
    pub(crate) remote_operations: usize,
    pub(crate) charged_bytes: usize,
    pub(crate) elapsed_micros: u128,
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Outcome {
    Protected,
    Published,
    Admitted {
        rows: Vec<[String; 2]>,
        admission_digest: String,
    },
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("String write cannot fail");
        out
    })
}
pub(crate) fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
pub(crate) fn text(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
