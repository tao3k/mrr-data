use meta_relational_reasoning::Binding;
#[cfg(feature = "duckgql-graphar")]
use meta_relational_reasoning::QueryResultValue;
#[cfg(feature = "duckgql-graphar")]
use mrr_data_core::PhysicalQueryOutput;
use mrr_data_core::{BoundDataQuery, DataEngineProfile};
use mrr_data_graphar::{BinaryEntityHop, BinaryEntityProjection, EntityEndpoint, EntityHopError};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DuckGqlError {
    Shape(EntityHopError),
    Schema,
    Artifact,
    SourceMismatch,
    Native,
    Cleanup,
    CorruptOutput,
    Limit(&'static str),
    Cancelled,
    Deadline,
    #[cfg(feature = "backend-worker")]
    Backend(mrr_data_backend::BackendError),
}
impl std::fmt::Display for DuckGqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DuckGQL: {self:?}")
    }
}
impl std::error::Error for DuckGqlError {}

/// Exact physical capability profile; version fields live in the program Schema.
/// # Errors
/// Refuses invalid static profile declarations.
pub fn duckgql_graphar_engine_profile() -> Result<DataEngineProfile, DuckGqlError> {
    DataEngineProfile::new("duckgql-graphar-single-hop", true, []).map_err(|_| DuckGqlError::Schema)
}
pub(crate) fn schema() -> Result<Value, DuckGqlError> {
    serde_json::from_str(include_str!("../schema.json")).map_err(|_| DuckGqlError::Schema)
}

/// A checked program for the plugin's registered typed table function.
#[derive(Clone, Debug)]
pub struct DuckGqlSingleHopProgram {
    hop: BinaryEntityHop,
    version: u8,
    parameters: [String; 4],
}
impl DuckGqlSingleHopProgram {
    /// Compile a bound query without parsing GQL or rebinding semantics.
    /// # Errors
    /// Refuses profile/source drift, unsupported shape or invalid program Schema.
    pub fn compile(
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
    ) -> Result<Self, DuckGqlError> {
        let hop = BinaryEntityHop::admit(query, projection, &duckgql_graphar_engine_profile()?)
            .map_err(DuckGqlError::Shape)?;
        let schema = schema()?;
        let version = schema["properties"]["program_version"]["const"]
            .as_u64()
            .and_then(|v| u8::try_from(v).ok())
            .ok_or(DuckGqlError::Schema)?;
        if schema["properties"]["argument_count"]["const"] != 27 {
            return Err(DuckGqlError::Schema);
        }
        let mut expressions: Vec<_> = hop
            .columns()
            .iter()
            .map(|column| {
                property_expression(
                    match column {
                        EntityEndpoint::Source(_) => 0,
                        EntityEndpoint::Target(_) => 2,
                    },
                    8,
                    "mrr_entity",
                )
            })
            .collect();
        // Hidden edge ordinal ensures source Fact order independent of ID text
        // collation, while duplicate endpoint pairs remain distinct native edges.
        expressions.push(property_expression(1, 9, "mrr_order"));
        let names: Vec<_> = (0..expressions.len())
            .map(|i| format!("column_{i}"))
            .collect();
        let parameters = [
            json!(["node", "edge", "node"]).to_string(),
            json!(expressions).to_string(),
            json!(names).to_string(),
            json!([hop.outputs().len()]).to_string(),
        ];
        Ok(Self {
            hop,
            version,
            parameters,
        })
    }
    #[must_use]
    pub fn statement(&self) -> &'static str {
        include_str!("typed-match.sql")
    }
    #[must_use]
    pub fn parameters(&self) -> &[String; 4] {
        &self.parameters
    }
    #[must_use]
    pub const fn program_version(&self) -> u8 {
        self.version
    }
    #[must_use]
    pub fn outputs(&self) -> &[Binding] {
        self.hop.outputs()
    }
    #[cfg(feature = "duckgql-graphar")]
    pub(crate) fn decode(&self, values: &[String]) -> Result<Vec<QueryResultValue>, DuckGqlError> {
        if values.len() != self.hop.columns().len() {
            return Err(DuckGqlError::CorruptOutput);
        }
        values
            .iter()
            .zip(self.hop.columns())
            .map(|(text, column)| {
                text.parse()
                    .map(|entity| QueryResultValue::node(entity, column.entity_type()))
                    .map_err(|_| DuckGqlError::CorruptOutput)
            })
            .collect()
    }
    #[cfg(feature = "duckgql-graphar")]
    pub(crate) fn output(&self, rows: Vec<Vec<QueryResultValue>>) -> PhysicalQueryOutput {
        PhysicalQueryOutput::new(self.outputs().to_vec(), rows)
    }
}
fn property_expression(index: u64, binding_type: u8, name: &str) -> Value {
    json!({"node_types":[2,1],"result_types":[if name == "mrr_order" {3} else {7},binding_type],"binding_indices":[index,index],"operators":[0,0],"values":["",""],"properties":[name,""],"child_counts":[0,0],"aggregate":[false,false],"distinct":[false,false]})
}
