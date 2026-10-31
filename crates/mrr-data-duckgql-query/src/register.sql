-- Fixed administration over the private derived image. No read query is parsed.
CREATE GRAPH mrr_query TYPED {
  (Node :Node {mrr_entity STRING}),
  (Node)-[:Edge {mrr_fact STRING, mrr_order INT64}]->(Node)
} FROM TABLES (
  VERTEX TABLE memory.main.mrr_vertices MAP TO NODE TYPE Node KEY (vertex_key)
    PROPERTIES (mrr_entity AS mrr_entity),
  EDGE TABLE memory.main.mrr_edges MAP TO EDGE TYPE Edge KEY (edge_key)
    SOURCE (source) REFERENCES NODE TYPE Node
    DESTINATION (target) REFERENCES NODE TYPE Node
    PROPERTIES (mrr_fact AS mrr_fact, mrr_order AS mrr_order)
) OPTIONS (SNAPSHOT_POLICY 'LIVE', ACCESS_MODE 'READ_ONLY', VALIDATE TRUE);
SESSION SET GRAPH mrr_query;
