-- Native interface fixture only. Graph registration is fixed setup; the read
-- below uses the plugin's typed program and contains no GQL MATCH source.
CREATE TABLE audit_vertices(source_key BIGINT, mrr_entity VARCHAR);
INSERT INTO audit_vertices VALUES (1, 'alice'), (2, 'bob'), (3, 'zoe'), (4, 'yan');
CREATE TABLE audit_edges(source_key BIGINT, source BIGINT, target BIGINT, mrr_fact VARCHAR);
INSERT INTO audit_edges VALUES (11, 1, 2, 'edge-b'), (12, 3, 4, 'edge-a'), (13, 1, 2, 'edge-c');
CREATE GRAPH mrr_audit TYPED {
  (Node :Node {mrr_entity STRING}),
  (Node)-[:knows {mrr_fact STRING}]->(Node)
} FROM TABLES (
  VERTEX TABLE memory.main.audit_vertices MAP TO NODE TYPE Node KEY (source_key)
    PROPERTIES (mrr_entity AS mrr_entity),
  EDGE TABLE memory.main.audit_edges MAP TO EDGE TYPE knows KEY (source_key)
    SOURCE (source) REFERENCES NODE TYPE Node
    DESTINATION (target) REFERENCES NODE TYPE Node
    PROPERTIES (mrr_fact AS mrr_fact)
) OPTIONS (SNAPSHOT_POLICY 'LIVE', ACCESS_MODE 'READ_ONLY', VALIDATE TRUE);
SESSION SET GRAPH mrr_audit;
SELECT * FROM gql_match_relational(
  __PROGRAM_VERSION__::UTINYINT, -- Schema-managed program version
  [3]::UBIGINT[],
  [0, 1, 0]::UTINYINT[],
  [0, 1, 2]::UBIGINT[],
  -- Unquoted registration identifiers are canonicalized in the plugin catalog.
  -- The physical binding uses catalog labels, not original source spelling.
  ['node', 'knows', 'node']::VARCHAR[],
  [false, false, false]::BOOLEAN[],
  [false, false, false]::BOOLEAN[],
  [false, false, false]::BOOLEAN[],
  [1, 1, 1]::UBIGINT[],
  [1, 1, 1]::UBIGINT[],
  [1]::UBIGINT[],
  [1]::UTINYINT[],
  [18446744073709551615]::UBIGINT[],
  [18446744073709551615]::UBIGINT[],
  [0]::UBIGINT[],
  0::UBIGINT,
  [
    struct_pack(node_types := [2, 1]::UTINYINT[], result_types := [7, 8]::UTINYINT[],
      binding_indices := [0, 0]::UBIGINT[], operators := [0, 0]::UTINYINT[],
      "values" := ['', '']::VARCHAR[], properties := ['mrr_entity', '']::VARCHAR[],
      child_counts := [0, 0]::UTINYINT[], "aggregate" := [false, false]::BOOLEAN[], "distinct" := [false, false]::BOOLEAN[]),
    struct_pack(node_types := [2, 1]::UTINYINT[], result_types := [7, 8]::UTINYINT[],
      binding_indices := [2, 2]::UBIGINT[], operators := [0, 0]::UTINYINT[],
      "values" := ['', '']::VARCHAR[], properties := ['mrr_entity', '']::VARCHAR[],
      child_counts := [0, 0]::UTINYINT[], "aggregate" := [false, false]::BOOLEAN[], "distinct" := [false, false]::BOOLEAN[]),
    struct_pack(node_types := [2, 1]::UTINYINT[], result_types := [7, 9]::UTINYINT[],
      binding_indices := [1, 1]::UBIGINT[], operators := [0, 0]::UTINYINT[],
      "values" := ['', '']::VARCHAR[], properties := ['mrr_fact', '']::VARCHAR[],
      child_counts := [0, 0]::UTINYINT[], "aggregate" := [false, false]::BOOLEAN[], "distinct" := [false, false]::BOOLEAN[])
  ],
  ['source', 'target', 'fact']::VARCHAR[],
  []::STRUCT(node_types UTINYINT[], result_types UTINYINT[], binding_indices UBIGINT[],
    operators UTINYINT[], "values" VARCHAR[], properties VARCHAR[], child_counts UTINYINT[],
    "aggregate" BOOLEAN[], "distinct" BOOLEAN[])[],
  false,
  [2]::UBIGINT[],
  [false]::BOOLEAN[],
  [0]::UTINYINT[],
  false, 0::UBIGINT,
  false, 0::UBIGINT
);
