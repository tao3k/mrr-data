-- Fixed typed plugin invocation. Values use native prepared scalar bindings.
SELECT * FROM gql_match_relational(
  $1::UTINYINT, -- Version parameter supplied from the Schema
  [3]::UBIGINT[],
  [0, 1, 0]::UTINYINT[],
  [0, 1, 2]::UBIGINT[],
  -- Unquoted registration identifiers are canonicalized in the plugin catalog.
  -- The physical binding uses catalog labels, not original source spelling.
  CAST(CAST($2 AS JSON) AS VARCHAR[]),
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
  CAST(CAST($3 AS JSON) AS STRUCT(node_types UTINYINT[], result_types UTINYINT[], binding_indices UBIGINT[], operators UTINYINT[], "values" VARCHAR[], properties VARCHAR[], child_counts UTINYINT[], "aggregate" BOOLEAN[], "distinct" BOOLEAN[])[]),
  CAST(CAST($4 AS JSON) AS VARCHAR[]),
  []::STRUCT(node_types UTINYINT[], result_types UTINYINT[], binding_indices UBIGINT[],
    operators UTINYINT[], "values" VARCHAR[], properties VARCHAR[], child_counts UTINYINT[],
    "aggregate" BOOLEAN[], "distinct" BOOLEAN[])[],
  false,
  CAST(CAST($5 AS JSON) AS UBIGINT[]),
  [false]::BOOLEAN[],
  [0]::UTINYINT[],
  false, 0::UBIGINT,
  false, 0::UBIGINT
);
