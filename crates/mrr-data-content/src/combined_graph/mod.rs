//! Complete native topology/property closure, with snapshot root acknowledged last.
mod api;
mod prepare;
mod publish;
mod restore;
pub use api::{CombinedGraphInputs, PreparedCombinedGraph};
pub use prepare::{prepare_combined_graph, prepare_combined_graph_checked};
pub use publish::publish_combined_graph;
pub use restore::restore_combined_graph;
