# Changelog

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
