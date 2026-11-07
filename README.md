# SelectoDBMariaDB

MariaDB adapter package for the Selecto ecosystem.

This package provides `SelectoDBMariaDB.Adapter`, an external adapter module for
using Selecto against MariaDB via `myxql`.

## Installation

```elixir
def deps do
  [
    {:selecto, ">= 0.5.0 and < 0.6.0"},
    {:selecto_db_mariadb, "~> 0.2"}
  ]
end
```

## Usage

Pass the adapter explicitly when configuring Selecto:

```elixir
selecto =
  Selecto.configure(domain, mariadb_opts,
    adapter: SelectoDBMariaDB.Adapter
  )
```

## Notes

- Placeholder style is `?`.
- Identifier quoting uses backticks.
- Streaming is not currently supported.
- Portable flat writes and atomic batches are supported, including governed
  predicates, reference guards and upserts. An upsert needs exactly one
  domain-declared conflict target, and that target must be one whole-column
  unique index of an InnoDB table. It never uses `ON DUPLICATE KEY UPDATE`.
- Arbitrary DML returning and generated-key graphs are not advertised by this
  release; requests requiring them fail before adapter dispatch.

## Governed writes

Applications write through `SelectoUpdato`, which validates every command,
batch, and graph against the domain's `writes` contract and hands this adapter
a single-use `Selecto.Write.Authorization` for exactly that payload.
`execute_write/3` refuses a write without one with `:ungoverned_write` before
any statement runs, leaving every row unchanged. `execute_write_unsafe/3` skips
that check; it exists for trusted tooling and this package's own tests, never
for application code.

### Post-image check and its limits

A write bound to a tenant or trusted scope (`metadata.post_image`, set by
`SelectoUpdato`) runs in its own transaction (a savepoint inside the
caller's), after a shared metadata lock on the table, and its written rows
are re-read by key under the scope before it ends. A row that left the scope
rolls the write back (`query_rule_violation`); what the check cannot see
through is refused (`query_rule_not_evaluable`): non-InnoDB tables, views,
keys that are not the whole single-column primary key, guarded inserts on a
table with a BEFORE INSERT trigger, and scoped upserts on a table with any
INSERT or UPDATE trigger. Every governed write needs an InnoDB table and is
refused up front otherwise: MEMORY, MyISAM and Aria cannot roll back a
refused or failing write. A TEMPORARY table shadowing the target is refused
for every write: information_schema does not list it, so no engine, key or
trigger check could see it.

**Row-level triggers that modify other rows are allowed, and their effects
on those rows are the schema owner's responsibility.** The check proves the
rows the statement itself wrote. A row-level trigger (AFTER INSERT, AFTER
UPDATE or AFTER DELETE) that updates or deletes *other* rows, another
tenant's included, runs and is not checked. MySQL has no statement-level
triggers or rules.

## Session and tenant semantics

- Each read, stream and write first checks its connection's session. The
  time zone must be UTC (`UTC`, `+00:00`, `Etc/UTC`, or `SYSTEM` on a UTC
  server; never `GMT`). The client, connection and results character sets
  must be utf8mb4, and sql_mode must include `STRICT_TRANS_TABLES` or
  `STRICT_ALL_TABLES`, and `system_versioning_asof` must be `DEFAULT`: set, a
  SELECT reads the past while UPDATE and DELETE hit current rows, so a scope or
  post-image guard would prove a row that is no longer there. `sql_select_limit`
  must have no limit, or reads and a write's capture come back short, and
  `div_precision_increment` must be the default, or an AVG changes. MariaDB
  10.3.4 is the floor: the check reads `system_versioning_asof`. If any check
  fails, the call is refused before any statement runs. Set these with MyXQL's
  `after_connect` if the server defaults differ. Text wholly the host's is not
  held to this: it reads the past the host asked for.
- A statement Selecto built pins, for that statement only, what would cut its
  result: `group_concat_max_len` at the server's limit, and
  `max_recursive_iterations` at the depth it declares, so a chain past the
  server's 1000 runs in full and a cycle stops at `max_depth`. A CTE that declares
  no depth keeps the session's bound. A result the server still cuts, or sends
  as NULL over `max_allowed_packet`, is an error (`query_rule_not_evaluable`,
  `reason: :result_cut`). The pins need no privilege.
- Only a BASE TABLE or SYSTEM VERSIONED table is a write target: a write to a
  SEQUENCE (`reason: :sequence_target`, an INSERT would reset it) or a view
  (`reason: :unsupported_write_target`) refuses with `query_rule_not_evaluable`.
- The time zone and sql_mode are compared again after every read, after
  every fetched stream batch (before its rows are handed out) and after
  every write statement: a stored function a domain view calls, or a row
  trigger, can `SET time_zone` mid-statement and leave it changed. A change
  restores the values seen before (session variables are not
  transactional), discards the rows and refuses
  (`query_rule_violation`, `reason: :session_state_changed`); a write's
  transaction or savepoint rolls back. A switch made and undone inside one
  statement cannot be seen that way, so host, trusted, required and tenant
  comparisons, and association `where` pins, on a column stored as
  TIMESTAMP compare `UNIX_TIMESTAMP(col)` with an epoch computed in Elixir
  (the storage types are read before each read and each write). DATETIME
  columns compare as before.
- Scope comparisons (host, trusted, required, engine tenant, and query
  member filters, never the caller's own) follow each column's storage
  type, read per relation and column: a TIMESTAMP compares by epoch, a
  character column by utf8mb4 bytes (an integer-declared column stored as
  VARCHAR included), and a computed value, pattern or date part over a
  TIMESTAMP is refused (`reason: :timestamp_operator`). Relation names are
  matched exactly as the server resolves them: with
  `lower_case_table_names = 0` the lookup compares database and table names
  by bytes (`events` and `Events` are two tables), otherwise as the server
  folds them; a domain name that matches more than one table refuses
  (`reason: :relation_ambiguous`). A temporal column of a VIEW keeps its
  reported type only when the view definition takes it as is from a base
  table column (a TIMESTAMP source then compares by epoch); one derived by an
  expression (`ts + INTERVAL 0 SECOND` reports DATETIME but follows the
  session time zone), an alias or a nested view refuses scope compares
  (`reason: :column_unresolved`).
- A Selecto read refuses a relation a TEMPORARY table shadows
  (`query_rule_not_evaluable`, `reason: :temporary_shadow`); a row trigger
  can create one for the rest of the session. Unrelated temporary tables
  keep working.
- A checked-out connection with `autocommit=0` is inside the host's implicit
  transaction even before its first statement: a write there takes a
  savepoint, never BEGIN/COMMIT (which would commit the host's work). A
  managed write on a connection left with `autocommit=0` is refused
  (`:leaked_transaction`).
- Every transaction Selecto ends, it ends with `COMMIT AND NO CHAIN NO RELEASE`
  or `ROLLBACK AND NO CHAIN NO RELEASE`: `completion_type` is the host's and
  neither chains a new transaction nor closes the connection under Selecto.
- A read on an idle connection at `autocommit=1` is its own
  `START TRANSACTION READ ONLY`, ended
  by that ROLLBACK, so a view that writes refuses (`reason: :read_only_read`)
  and nothing survives the read. Read-only state lives in that transaction;
  no session setting is touched. If the ending itself fails it is retried,
  then the connection closes.
- A read inside a host transaction runs as written: no savepoint, no anchor,
  no read-only mode. A writing view reached there is the host's to undo, and
  a write it made to a non-transactional table cannot be undone by anyone.
  `autocommit=0` is a host transaction from its first statement: a read there
  opens what the host's own SELECT would, and its snapshot stays until the host
  ends it. A pool whose connections are left at `autocommit=0` therefore keeps
  each connection's snapshot from one checkout to the next.
- Text wholly the host's (no compile built it) runs as written, in the host's
  session: no `time_zone` or `sql_mode` refusal, no TEMPORARY guard; only the
  encoding refusal stays. It must be a read by its first word (SELECT, WITH, VALUES,
  TABLE, SHOW, EXPLAIN, DESCRIBE, ANALYZE of a select, or
  `reason: :hand_built_not_a_read`). MariaDB's parser takes no
  WITH ... UPDATE, so the first word is the whole check; it never EXPLAINs, since
  that evaluates a table-less CTE and would run the read's functions twice.
  That check stops honest mistakes, not evasion.
- A statement Selecto built that carries a host fragment stays generated: its
  relations, filters and tenant scope keep every guard, and only the fragment is
  verbatim. Tenant scope depends on a `raw_sql_filter` being well-formed: one that
  closes its own parenthesis escapes the scope.
- **Never build hand-built text or a raw fragment from end-user input.** It is the
  host's own code.
- A hand-built row-locking read (`FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE MODE`)
  runs as the driver's would: in a host transaction it runs as written and the
  lock holds to the host's end; alone it ends with the read. The check here is
  for writes, not locks. On that idle path, or for EXPLAIN, R55 is met by the
  ROLLBACK that ends the read: a function that writes there is undone, but a write
  to a non-transactional table persists.
- A read under `LOCK TABLES` or `FLUSH TABLES t WITH READ LOCK` runs as
  written too, because START TRANSACTION would release those locks: a writing
  view reached there at `autocommit=1` commits, as it would for the host's own
  SELECT. Reads keep the host's connection, variables and locks. A write or
  `transaction/3` under such locks refuses (`reason: :host_table_locks`).
- When no probe of a name can tell whether table locks are held (no default
  database, table-only grants) a table the role can see answers; if that cannot
  settle it either, a read runs as written and a write or `transaction/3` refuses.
- Selecto sends no ROLLBACK or COMMIT for a transaction it does not own. A write
  whose savepoint cannot be undone in a transaction begun with text on a held
  connection closes that connection, so the holder's next statement fails and no
  COMMIT reports a false OK. A transaction begun with text on a pool pid is host
  misuse: a pool can reconnect between statements with or without Selecto. Use a
  transaction handle.
- A write or `transaction/3` on a pid or pool whose connection is in a
  transaction refuses without sending a statement
  (`reason: :unowned_transaction`). Pass the connection from
  `DBConnection.run/3` to write inside a transaction you hold.
- On an AUTO_INCREMENT key only a canonical positive integer names the
  inserted row for the re-read: every value MySQL reads as zero (`0`, `'0'`,
  `'00'`, `'+0'`, `0.0`, FALSE) generates a new id, so a guarded insert
  assigning one is not evaluable.
- UUID columns (`:uuid`, `:binary_id`) are stored as text: CHAR(36), or
  MariaDB's native UUID type. Filters and writes bind the string form.
  BINARY(16) UUID storage is not supported.
- Tenant and site columns compare by their utf8mb4 bytes, whatever their
  collation or character set, so tenant `'t1'` never reaches `'T1'` or
  `'t1 '`. You do not need a `_bin` collation.
- Strict-mode value errors are `:data_violation`. Deadlocks and lock wait
  timeouts are `:deadlock_detected` and `:lock_timeout`, and the caller may
  retry them. Zero or out-of-range stored dates fail the read as a query
  error with SQLSTATE `22008` (category `:datetime_field_overflow`).

## Write previews

A write preview's `params` are the logical parameters: each value once, as
the command states it. An exact tenant/site compare binds its value a second
time. When it does, `bind_params` holds the statement's placeholder binds in
order. Run the preview's text with `bind_params` when it is present. An
upsert preview shows only the INSERT branch. The adapter probes, locks and
updates by the declared target around it (see the changelog).

## Local Workspace Development

For local multi-repo development against vendored ecosystem packages, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

When enabled, this package resolves `{:selecto, path: "../selecto"}`.

## Live Release Verification

Live tests are excluded from the default suite. Point the adapter at an
isolated MariaDB service and run:

```bash
SELECTO_MARIADB_PASSWORD='...' \
SELECTO_MARIADB_HOST=127.0.0.1 \
SELECTO_MARIADB_PORT=3306 \
mix test test/selecto_db_mariadb/write_execution_integration_test.exs \
  --include requires_db
```

The suite creates a uniquely named database, verifies governed write and
rollback behavior, and drops the database before disconnecting.
