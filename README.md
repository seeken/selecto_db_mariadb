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
  predicates, reference guards, and `ON DUPLICATE KEY UPDATE`. Upsert requires
  exactly one domain-declared conflict target because MariaDB cannot name a
  particular unique constraint in that statement.
- Arbitrary DML returning and generated-key graphs are not advertised by this
  release; requests requiring them fail before adapter dispatch.

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
