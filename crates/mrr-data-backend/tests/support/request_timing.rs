//! Bounded per-request clocks; no callbacks, scheduler or storage replacement.
use super::bound_obligation::ValidationCost;
#[derive(Default, serde::Serialize)]
pub struct RequestCost {
    pub lane: usize,
    pub attempt: usize,
    pub home_wait_ns: u128,
    pub api_ns: u128,
    pub to_validator_ns: u128,
    pub validator_ns: u128,
    pub validation: ValidationCost,
}
