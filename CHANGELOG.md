# Changelog

## Unreleased

- Guarded inserts (a captured query predicate or tenant scope) are admitted
  on storage semantics (`typed_insert_admission`, `typed_insert_precision`):
  after the portable candidate check, the INSERT runs in the write
  transaction and the row is re-read by its primary key (the assigned key, or
  LAST_INSERT_ID only when the server generated one) under the predicate, so
  DECIMAL rounding and collations decide; no matching row is
  `query_rule_violation` and rolls back. A session without strict sql_mode, a
  table with an INSERT trigger, an unknown key, or a strict-mode conversion
  failure (1264, 1265, 1292, 1366) is `query_rule_not_evaluable`.
- ORDER BY places NULLs as PostgreSQL does: last for an ascending and first
  for a descending sort, through a leading `(expr IS NULL)` term
  (`:postgresql_null_order`). An explicit NULLS FIRST/LAST still wins, and
  per-parent collection ordering uses the same placement as its keyset
  cursor predicate. Grouped, set-operation, rollup, window and JSON orderings
  follow it too (see the Selecto changelog). This is a behaviour change: NULL
  keys move in existing queries.
- Read-only server-side cursor streams (`stream/4`, `supports?(:stream)`):
  a producer checks the connection out with `DBConnection.run/3`, issues no
  transaction control, fetches one acknowledged chunk at a time, reports the
  columns before enumeration and releases an abandoned cursor after
  `:receive_timeout` (a later read then errors instead of truncating).
- Computed-value casts and `json_text` (`render_computed_value/2`); a decimal
  cast is `DECIMAL(65, 10)` and there is no boolean cast.
- Array predicates over JSON arrays (`array_contains`, `array_overlap`,
  `array_contained`; NULL stays unknown, non-arrays never match) and ordered
  `unnest` with ordinality through `JSON_TABLE`, and json_each-style JSON
  rowsets (`:json_table_rowset`; only `$`, `.member` and `[n]` paths).
- Governed prefix lookups (`text_search` `:prefix`, `governed_lookup?`):
  normalized `word:*` text binds as one parameter and becomes `+word*` in
  BOOLEAN MODE; the rank is a field selector.
- Ordered and per-parent-limited collection JSON aggregates use
  `JSON_ARRAYAGG(... ORDER BY ... LIMIT n)` (`:json_agg_limit`). MariaDB has
  no LATERAL and no GROUPING(), so neither is advertised.
- `JSON_TABLE` array elements are their unquoted scalar text.
- `execute_write/3` refuses a write without the `Selecto.Write.Authorization`
  the governed entry point (`SelectoUpdato`) issued for exactly that command,
  batch or graph, with `:ungoverned_write` and no row changed. The raw
  implementation moved to `execute_write_unsafe/3`, for trusted tooling and
  adapter tests only.

## 0.5.0 - 2026-08-14

- Removed renderer aliases for the retired `json_extract_path`,
  `json_extract_path_text`, and `json_insert` core operations.
- Normalize the custom O'Saasy Hex metadata to `LicenseRef-O-Saasy`; the
  packaged license text and licensing terms are unchanged.
- Raised the Selecto baseline to `0.5.0` and implemented the explicit runtime,
  normalized result/error/type, and typed dialect-fragment ports.
- Unsupported PostgreSQL-shaped features now fail with structured capability
  evidence instead of inheriting core fallback SQL.
- MariaDB now owns portable datetime-format and case-insensitive comparison
  rendering and explicitly rejects unsupported timezone/epoch conversion.

## 0.2.0 - 2026-08-12

- Added versioned portable flat writes and atomic batches through MyXQL.
- Added the public Selecto transaction callback used by the advertised
  transaction capability.
- Added parameterized guarded mutations and native `ON DUPLICATE KEY UPDATE`
  upserts with physical 1/2-to-one logical affected-row normalization while
  preserving zero for a failed atomic reference guard.
- Fail closed unless the domain declares exactly one matching conflict target;
  MariaDB cannot select among multiple unique constraints in this syntax.
- Fails capability preflight for arbitrary returning and generated-key graphs
  rather than simulating unsupported semantics.
- Added opt-in live MariaDB coverage for tenant-scoped flat writes, atomic
  reference guards, insert/update/no-op upsert row counts, delete, and complete
  batch rollback.
- Normalized MyXQL command results with `columns: nil` to an empty portable
  column list instead of crashing during non-query execution.
