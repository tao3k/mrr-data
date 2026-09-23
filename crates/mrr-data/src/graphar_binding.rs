//! Fail-closed composition between verified snapshot bindings and `GraphAr` sources.

use std::{fmt, fs, io, path::PathBuf};

use mrr_data_core::{
    BoundDataQuery, DataGraphSourceBindingError, admit_graph_projection_source, raw_cid,
};
use mrr_data_graphar::GraphArQuerySource;

/// Reasons a database-neutral `GraphAr` source cannot serve a bound query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphArQuerySourceBindingError {
    Binding(DataGraphSourceBindingError),
    GraphInfoRead { path: PathBuf, kind: io::ErrorKind },
}

impl fmt::Display for GraphArQuerySourceBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => write!(formatter, "GraphAr source binding: {error}"),
            Self::GraphInfoRead { path, kind } => write!(
                formatter,
                "cannot read GraphAr metadata `{}`: {kind}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for GraphArQuerySourceBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Binding(error) => Some(error),
            Self::GraphInfoRead { .. } => None,
        }
    }
}

impl From<DataGraphSourceBindingError> for GraphArQuerySourceBindingError {
    fn from(error: DataGraphSourceBindingError) -> Self {
        Self::Binding(error)
    }
}

/// Admits one immutable `GraphAr` source for an already bound physical query.
///
/// The operation does not create an engine, database connection, mutable
/// catalog, or execution lifecycle. It verifies that the source relation is
/// required by the MRR-owned query and that the current `GraphAr` metadata bytes
/// still hash to the exact raw manifest CID carried by [`BoundDataQuery`]. A
/// downstream adapter may register the returned source with its native engine.
///
/// # Errors
///
/// Returns [`GraphArQuerySourceBindingError`] when the physical binding has no
/// `GraphAr` projection, the query does not reference this source relation, the
/// metadata cannot be read, or its content identity has drifted.
pub fn admit_graphar_query_source<'source>(
    query: &BoundDataQuery,
    source: &'source GraphArQuerySource,
) -> Result<&'source GraphArQuerySource, GraphArQuerySourceBindingError> {
    let graph_info_path = source.graph_info_path();
    let graph_info = fs::read(&graph_info_path).map_err(|error| {
        GraphArQuerySourceBindingError::GraphInfoRead {
            path: graph_info_path,
            kind: error.kind(),
        }
    })?;
    admit_graph_projection_source(query, source.relation_id(), &raw_cid(&graph_info))?;
    Ok(source)
}
