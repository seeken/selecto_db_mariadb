# Changelog

## Unreleased

- Live test: a host predicate on a computed value's CASE conditions (eq, IN,
  nested computed predicate) and on a joined schema's computed predicate
  compares exactly.

- Live test: a required filter on a computed predicate compares its body
  exactly, and association `where` policies confine joins, collections,
  through bridges and retargets exactly ('S1' and 's1 ' never match a
  's1' pin; a retarget onto a where-confined association returns only the
  policy's rows), with text and unrecognised policy column types.

- Live test: DECIMAL and DATETIME(3) columns declared with custom type
  names that contain "char" or "text" (`charge_amount`, `context_ts`) stay
  guarded in reads and writes. `char varying` and `national varchar` stay
  exact.


- Write predicates, re-reads and reference guards follow Selecto's
  three-way type classification. Columns that are undeclared, of unknown
  type or untyped (a command without field types) use the guarded form
  `(col = ? AND (COLLATION(col) = 'binary' OR <bytes>))`, so a host `<>` on
  an undeclared DATETIME or DECIMAL column keeps PostgreSQL's semantics.
  Live tests cover host predicates of every shape, temporal and undeclared
  columns, parameterised and custom text types, and a refused through scope
  key.


- Every statement the adapter sends (reads, streams, writes, re-reads,
  probes, the session check) runs as `SET STATEMENT
  optimizer_switch='subquery_cache=off' FOR ...`. MariaDB's subquery cache
  keys a correlated subquery's outer references under the column's
  collation, so an outer `'T7'` (or `'t7 '`) reused the result for `'t7'`
  whatever exact comparison the SQL used. This leaked through-association
  joins on the default optimizer and scope-key retargets with semijoin=off.
  The setting applies per statement and never changes the host's session.
  Savepoint statements cannot carry it and hold no subquery.


- UUID columns are text (CHAR(36), or MariaDB's native UUID type), as
  `type_name/1` maps them. `:uuid_text_storage` makes filters bind the string
  form, so text-stored UUID tenants are readable and compare exactly. BINARY(16)
  UUID storage is not supported on these adapters.
- Live tests: retargets onto scope-key-confined relations (both strategies,
  one and two hops, engine tenant and required filter, including a
  case-variant scope).


- UUID tenant and site columns (`:uuid`, `:binary_id`) in write predicates,
  guarded-insert re-reads, upsert targets and reference guards compare by
  their stored bytes, unconverted. That is exact for CHAR(36)/VARCHAR and
  BINARY(16) storage alike. Live tests now cover through-association scope
  pairs (INT and VARCHAR roots, bridges and targets, declared and undeclared
  bridges, both scope namings) and UUID tenants.


- Upserts no longer use `ON DUPLICATE KEY UPDATE`. It fires on the primary
  key and every unique index, so an upsert whose assigned id belonged to
  another row overwrote that row. The declared conflict target must now be
  exactly one whole-column unique index of an InnoDB table, or the upsert is
  refused (`:write_capability_missing`, code
  `:unsupported_upsert_conflict_target`). In the write's transaction the
  adapter probes the target. If the row exists, it is locked with FOR UPDATE
  and updated by the target. If not, the row is inserted inside a savepoint.
  On a duplicate, the savepoint is rolled back and the target's row is locked
  and updated. If no such row exists, the typed unique violation stands
  (`SelectoDBMariaDB.Upsert`). A failed savepoint rollback never replaces the original error, a
  deadlock included. Tenant and site columns of the target compare exactly.
  The upsert preview shows the INSERT branch.

- A write preview reports its logical parameters, each value once, as
  `params` (see the README's "Write previews"). The exact tenant compare binds its value a second time. When that
  happens, the statement's placeholders in order are in `bind_params`.
  Execution always binds every placeholder.


- Tenant and site columns compare exactly (`:exact_tenant_compare`). Reads
  are covered by Selecto. Write predicates on `metadata.tenant_fields`, the
  guarded-insert re-read and reference-guard tenants compare the column's
  utf8mb4 bytes as well, so tenant `'t1'` never updates, deletes, admits or
  references a `'T1'` or `'t1 '` row. The same holds for utf16 and latin1
  columns. A column typed as a non-text value keeps the plain comparison.
- Every read, stream and write first checks the session on the connection it
  uses, once per call. The time zone must be UTC (`UTC`, `+00:00`, `Etc/UTC`,
  or `SYSTEM` on a UTC server, never `GMT`). The client, connection and
  results character sets must be utf8mb4. sql_mode must be strict. Otherwise
  the call is refused before any statement runs: reads and streams get a
  `:connection_error` with category `:invalid_session`, writes get
  `:invalid_connection`. Only the failing setting is named.
- Server errors 1264, 1265, 1292, 1366, 1367, 1406 and 1690 are category
  `:data_violation`, 1213 is `:deadlock_detected` and 1205 is
  `:lock_timeout`. All were `:database_error` before. A deadlock victim keeps
  its category inside a host transaction.
- A read or stream that returns a zero date, a zero month or day, or year 0
  (storable outside strict mode) fails as a query error with SQLSTATE
  `22008` and category `:datetime_field_overflow`, instead of handing back an
  invalid date.
- An empty input to a rollup returns PostgreSQL's grand-total row
  (`:rollup_without_empty_total`).


- Guarded inserts (a captured query predicate or tenant scope) are admitted
  on storage semantics (`typed_insert_admission`, `typed_insert_precision`):
  after the portable candidate check, the INSERT runs in the write
  transaction and the row is re-read by its declared single-column primary
  key under the predicate, so DECIMAL rounding and collations decide; no
  matching row is `query_rule_violation` and rolls back. The key is the
  assigned value compared in the column's own type, or LAST_INSERT_ID only
  when that key column is the table's AUTO_INCREMENT column; exactly one row
  must carry it. A session without strict sql_mode, a non-InnoDB table, a
  table with an INSERT trigger, a composite or unknown key, or a strict-mode
  conversion failure (1264, 1265, 1292, 1366) is `query_rule_not_evaluable`.
  information_schema shows only objects the writing user holds a privilege
  on, so trigger detection assumes the writer has its normal table
  privileges.
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
