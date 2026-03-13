# SelectoDBMariaDB

MariaDB adapter package for the Selecto ecosystem.

This package provides `SelectoDBMariaDB.Adapter`, an external adapter module for
using Selecto against MariaDB via `myxql`.

## Installation

```elixir
def deps do
  [
    {:selecto, "~> 0.3.16"},
    {:selecto_db_adapter, "~> 0.1"},
    {:selecto_db_mariadb, "~> 0.1"}
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

## Local Workspace Development

For local multi-repo development against vendored ecosystem packages, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

When enabled, this package resolves
`{:selecto_db_adapter, path: "../selecto_db_adapter"}`.
