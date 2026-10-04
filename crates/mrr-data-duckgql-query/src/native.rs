//! Query connections and cursors close before output; extension code is process resident.
use crate::{DuckGqlError, DuckGqlSingleHopProgram};
use duckdb::{Connection, params};
use meta_relational_reasoning::{EntityId, FactId};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use mrr_data_graphar::{BinaryEntityProjection, CapturedGraphArSnapshot};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Read, path::Path, sync::Arc};

/// Host-prepared immutable artifact, shared across invocations and clones.
/// `DuckDB` retains loaded code for the process lifetime. Reuse one artifact
/// owner to reuse the same mapped module; query cleanup does not unload it.
#[derive(Clone, Debug)]
pub struct DuckGqlArtifact {
    directory: Arc<tempfile::TempDir>,
    sha256: [u8; 32],
    byte_len: usize,
    allow_unsigned: bool,
}

/// Application bounds and native settings; `DuckDB` memory_limit is not an RSS cap.
#[derive(Clone, Debug)]
pub struct DuckGqlLimits {
    pub max_input_rows: usize,
    pub max_input_bytes: usize,
    pub max_output_rows: usize,
    pub max_output_cells: usize,
    pub native_memory_limit: String,
    pub native_threads: usize,
}
struct Image {
    vertices: BTreeMap<EntityId, u64>,
    edges: Vec<(FactId, u64, u64)>,
}
fn prepare_image(
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: &DuckGqlLimits,
    checkpoint: &impl Fn() -> Result<(), DuckGqlError>,
) -> Result<Image, DuckGqlError> {
    if [
        limits.max_input_rows,
        limits.max_input_bytes,
        limits.max_output_rows,
        limits.max_output_cells,
        limits.native_threads,
    ]
    .contains(&0)
    {
        return Err(DuckGqlError::Limit("zero query budget"));
    }
    if source.binding().relation() != projection.relation_id() {
        return Err(DuckGqlError::SourceMismatch);
    }
    let facts = source
        .facts(query)
        .map_err(|_| DuckGqlError::SourceMismatch)?;
    if facts.len() > limits.max_input_rows {
        return Err(DuckGqlError::Limit("input rows"));
    }
    let mut image = Image {
        vertices: BTreeMap::new(),
        edges: Vec::with_capacity(facts.len()),
    };
    let mut previous = None;
    let mut bytes = 0usize;
    for fact in facts {
        checkpoint()?;
        let edge = projection
            .project(fact)
            .map_err(|_| DuckGqlError::SourceMismatch)?;
        if edge.generation_id() != query.query().generation()
            || previous.is_some_and(|last| last >= edge.fact_id())
        {
            return Err(DuckGqlError::SourceMismatch);
        }
        previous = Some(edge.fact_id());
        bytes = [
            edge.fact_id().to_string(),
            edge.relation_id().to_string(),
            edge.generation_id().to_string(),
            edge.source().to_string(),
            edge.destination().to_string(),
        ]
        .iter()
        .try_fold(bytes, |total, text| total.checked_add(text.len()))
        .ok_or(DuckGqlError::Limit("input bytes"))?;
        if bytes > limits.max_input_bytes {
            return Err(DuckGqlError::Limit("input bytes"));
        }
        let mut keys = [0; 2];
        for (index, entity) in [edge.source(), edge.destination()].into_iter().enumerate() {
            let next = u64::try_from(image.vertices.len())
                .map_err(|_| DuckGqlError::Limit("vertex index"))?;
            keys[index] = *image.vertices.entry(entity).or_insert(next);
        }
        image.edges.push((edge.fact_id(), keys[0], keys[1]));
    }
    Ok(image)
}
impl DuckGqlArtifact {
    /// Copy and authenticate an installed artifact once on an accounted Host
    /// setup worker. Queries and clones share its private immutable load path.
    /// The Host accounts for the resident module separately from request scratch.
    /// # Errors
    /// Refuses unreadable, oversized or digest-mismatched artifacts.
    pub fn capture(
        path: impl AsRef<Path>,
        sha256: [u8; 32],
        max_bytes: usize,
        allow_unsigned: bool,
    ) -> Result<Self, DuckGqlError> {
        let limit = u64::try_from(max_bytes).map_err(|_| DuckGqlError::Artifact)?;
        if limit == 0 {
            return Err(DuckGqlError::Artifact);
        }
        let input = std::fs::File::open(path).map_err(|_| DuckGqlError::Artifact)?;
        let mut bytes = Vec::new();
        input
            .take(limit.checked_add(1).ok_or(DuckGqlError::Artifact)?)
            .read_to_end(&mut bytes)
            .map_err(|_| DuckGqlError::Artifact)?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() > max_bytes || digest != sha256 {
            return Err(DuckGqlError::Artifact);
        }
        let directory = tempfile::tempdir().map_err(|_| DuckGqlError::Artifact)?;
        std::fs::write(directory.path().join("duckgql.duckdb_extension"), &bytes)
            .map_err(|_| DuckGqlError::Artifact)?;
        Ok(Self {
            directory: Arc::new(directory),
            sha256,
            byte_len: bytes.len(),
            allow_unsigned,
        })
    }
    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
    #[must_use]
    pub const fn byte_len(&self) -> usize {
        self.byte_len
    }
}

fn checked<T>(
    result: duckdb::Result<T>,
    checkpoint: &impl Fn() -> Result<(), DuckGqlError>,
) -> Result<T, DuckGqlError> {
    checkpoint()?;
    result.map_err(|_| DuckGqlError::Native)
}

/// Execute through a checked artifact on one privately owned `DuckDB` connection.
/// The Host calls this on an accounted worker. No source-language read is parsed.
/// # Errors
/// Refuses unsupported queries, source/artifact drift, limits and native errors.
pub fn execute_duckgql_graphar_single_hop(
    artifact: &DuckGqlArtifact,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: &DuckGqlLimits,
) -> Result<PhysicalQueryOutput, DuckGqlError> {
    execute_checked(
        artifact,
        query,
        source,
        projection,
        limits,
        &|| Ok(()),
        &|_| Ok(()),
    )
}
pub(crate) fn execute_checked<G>(
    artifact: &DuckGqlArtifact,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: &DuckGqlLimits,
    checkpoint: &impl Fn() -> Result<(), DuckGqlError>,
    monitor: &impl Fn(&Connection) -> Result<G, DuckGqlError>,
) -> Result<PhysicalQueryOutput, DuckGqlError> {
    checkpoint()?;
    let program = DuckGqlSingleHopProgram::compile(query, projection)?;
    if program.outputs().len() > limits.max_output_cells {
        return Err(DuckGqlError::Limit("output width"));
    }
    let image = prepare_image(query, source, projection, limits, checkpoint)?;
    checkpoint()?;
    let mut config = duckdb::Config::default()
        .enable_autoload_extension(false)
        .map_err(|_| DuckGqlError::Native)?;
    let threads =
        i64::try_from(limits.native_threads).map_err(|_| DuckGqlError::Limit("native threads"))?;
    config = config
        .max_memory(&limits.native_memory_limit)
        .and_then(|config| config.threads(threads))
        .map_err(|_| DuckGqlError::Limit("native settings"))?;
    if artifact.allow_unsigned {
        config = config
            .allow_unsigned_extensions()
            .map_err(|_| DuckGqlError::Artifact)?;
    }
    let connection =
        Connection::open_in_memory_with_flags(config).map_err(|_| DuckGqlError::Native)?;
    let guard = monitor(&connection)?;
    let result = execute_native(&connection, artifact, &program, image, limits, checkpoint);
    drop(guard);
    // A private connection owns its graph catalog and all derived tables. Close
    // before returning any successful output or cancellation refusal.
    connection.close().map_err(|_| DuckGqlError::Cleanup)?;
    checkpoint()?;
    result
}
fn execute_native(
    connection: &Connection,
    artifact: &DuckGqlArtifact,
    program: &DuckGqlSingleHopProgram,
    image: Image,
    limits: &DuckGqlLimits,
    checkpoint: &impl Fn() -> Result<(), DuckGqlError>,
) -> Result<PhysicalQueryOutput, DuckGqlError> {
    let schema = crate::program::schema()?;
    let version: String = checked(
        connection.query_row("SELECT version()", [], |row| row.get(0)),
        checkpoint,
    )?;
    if Some(version.as_str()) != schema["properties"]["rust_engine_version"]["const"].as_str() {
        return Err(DuckGqlError::Artifact);
    }
    let path = artifact.directory.path().join("duckgql.duckdb_extension");
    let quoted = path
        .to_str()
        .ok_or(DuckGqlError::Artifact)?
        .replace('\'', "''");
    checked(
        connection.execute_batch(&format!("LOAD json; LOAD '{quoted}'")),
        checkpoint,
    )?;
    let extension: String = checked(connection.query_row("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='duckgql' AND loaded", [], |row| row.get(0)), checkpoint)?;
    if Some(extension.as_str()) != schema["properties"]["extension_version"]["const"].as_str() {
        return Err(DuckGqlError::Artifact);
    }
    let row_count = image.edges.len();
    load_image(connection, image, checkpoint)?;
    checked(
        connection.execute_batch(include_str!("register.sql")),
        checkpoint,
    )?;
    let mut statement = checked(connection.prepare(program.statement()), checkpoint)?;
    let bindings = program.parameters();
    let mut cursor = checked(
        statement.query(params![
            program.program_version(),
            &bindings[0],
            &bindings[1],
            &bindings[2],
            &bindings[3]
        ]),
        checkpoint,
    )?;
    let mut output = Vec::new();
    while let Some(row) = checked(cursor.next(), checkpoint)? {
        if output.len() >= limits.max_output_rows {
            return Err(DuckGqlError::Limit("output rows"));
        }
        let cells = (output.len() + 1)
            .checked_mul(program.outputs().len())
            .ok_or(DuckGqlError::Limit("output cells"))?;
        if cells > limits.max_output_cells {
            return Err(DuckGqlError::Limit("output cells"));
        }
        let ordinal: u64 = row
            .get(program.outputs().len())
            .map_err(|_| DuckGqlError::CorruptOutput)?;
        if usize::try_from(ordinal).ok() != Some(output.len()) {
            return Err(DuckGqlError::CorruptOutput);
        }
        let values = (0..program.outputs().len())
            .map(|index| {
                row.get::<_, String>(index)
                    .map_err(|_| DuckGqlError::CorruptOutput)
            })
            .collect::<Result<Vec<_>, _>>()?;
        output.push(program.decode(&values)?);
    }
    if output.len() != row_count {
        return Err(DuckGqlError::CorruptOutput);
    }
    Ok(program.output(output))
}
fn load_image(
    connection: &Connection,
    image: Image,
    checkpoint: &impl Fn() -> Result<(), DuckGqlError>,
) -> Result<(), DuckGqlError> {
    checked(connection.execute_batch("BEGIN; CREATE TABLE mrr_vertices(vertex_key UBIGINT PRIMARY KEY, mrr_entity VARCHAR NOT NULL); CREATE TABLE mrr_edges(edge_key UBIGINT PRIMARY KEY, source UBIGINT NOT NULL, target UBIGINT NOT NULL, mrr_fact VARCHAR NOT NULL, mrr_order BIGINT NOT NULL);"), checkpoint)?;
    {
        let mut insert = checked(
            connection.prepare("INSERT INTO mrr_vertices VALUES (?, ?)"),
            checkpoint,
        )?;
        for (entity, key) in image.vertices {
            checkpoint()?;
            checked(insert.execute(params![key, entity.to_string()]), checkpoint)?;
        }
    }
    {
        let mut insert = checked(
            connection.prepare("INSERT INTO mrr_edges VALUES (?, ?, ?, ?, ?)"),
            checkpoint,
        )?;
        for (index, (fact, source, target)) in image.edges.into_iter().enumerate() {
            checkpoint()?;
            let ordinal = i64::try_from(index).map_err(|_| DuckGqlError::Limit("edge index"))?;
            checked(
                insert.execute(params![ordinal, source, target, fact.to_string(), ordinal]),
                checkpoint,
            )?;
        }
    }
    checked(connection.execute_batch("COMMIT"), checkpoint)
}
