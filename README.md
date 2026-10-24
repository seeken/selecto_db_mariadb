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
INSERT or UPDATE trigger.

**Triggers that modify other rows are outside the guarantee.** The check
proves the rows the statement wrote; a row-level trigger (an AFTER DELETE,
AFTER INSERT or AFTER UPDATE trigger) that updates or deletes *other* rows,
another tenant's included, is not detected. Do not attach such triggers to
tables written under a bound scope.

## Session and tenant semantics

- Each read, stream and write first checks its connection's session. The
  time zone must be UTC (`UTC`, `+00:00`, `Etc/UTC`, or `SYSTEM` on a UTC
  server; never `GMT`). The client, connection and results character sets
  must be utf8mb4, and sql_mode must include `STRICT_TRANS_TABLES` or
  `STRICT_ALL_TABLES`. If any check fails, the call is refused before any
  statement runs. Set these with MyXQL's `after_connect` if the server
  defaults differ.
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
