mod asp_rust_gate;
mod projection;
#[cfg(feature = "native-graphar")]
mod query_parity;
mod scenarios;
#[cfg(feature = "native-graphar")]
mod writer;

#[cfg(feature = "file-inventory")]
mod inventory;

#[cfg(feature = "native-graphar")]
mod snapshot;

#[cfg(feature = "selective-graphar")]
mod selective;

#[cfg(feature = "native-graphar")]
#[path = "../support/binary_entity.rs"]
mod binary_entity;

#[cfg(feature = "native-graphar")]
pub(crate) use binary_entity::{fact as binary_fact, schema as binary_schema};

#[cfg(feature = "native-graphar")]
mod entity_properties;
