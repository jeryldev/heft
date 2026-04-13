# Heft

Heft is an embedded accounting engine built in Zig on top of SQLite.

The design goal is to give applications a small, deterministic, transactional
ledger core with double-entry posting, accounting periods, reporting,
subledgers, dimensions, budgets, and a C ABI for embedding.

## What Heft is

- An embedded ledger engine
- A SQLite-backed accounting core
- A Zig library with a C ABI surface
- A systems-oriented codebase that favors integer money, transactional updates,
  bounded report sizes, and explicit audit logging

## What Heft is not

- A networked accounting server
- A multi-writer distributed database
- A UI or complete end-user accounting application

## Core model

- Books define the accounting entity and base currency
- Accounts define the chart of accounts
- Periods define posting windows and close state
- Entries and lines hold journal activity
- Balance cache stores per-account, per-period aggregates
- Audit log records all mutations
- Subledger/open items support AR/AP style workflows
- Dimensions and budgets support analysis and planning

## Build

Requirements:

- Zig `0.15.2`

Common commands:

```bash
zig build
zig build test
zig build check
zig build bench
```

Notes:

- `zig build test` runs both the Zig module tests and the `main.zig` C ABI test
  surface.
- `zig build bench` runs a lightweight in-memory benchmark harness for the core read, statement, comparative, export, and lifecycle surfaces. The harness now reports setup time separately from
  operation time so fixture generation does not get mistaken for engine cost.
- The C ABI tests create real `.ledger` files, so they need a writable working
  directory.
- For sandboxed or read-only environments, the in-memory engine suite can be
  run directly with:

```bash
zig test src/root.zig vendor/sqlite3.c -lc -I vendor
```

Benchmark examples:

```bash
zig build bench
zig build bench -- --scenario trial_balance --read-iterations 50
zig build bench -- --scenario close_period --write-iterations 5 --close-entries 2000
zig build bench -- --scenario statement_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario comparative_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario export_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario seed_report --write-iterations 5 --report-entries 5000 --counterparties 128
zig build bench -- --scenario query_scale --read-iterations 10 --report-entries 2000 --counterparties 128
```

Useful benchmark groups:

- `trial_balance`, `general_ledger`, `aged_subledger`
  Read benchmarks against one seeded report workload, with setup shown separately
- `statement_suite`
  Covers income statement, trial balance movement, balance sheet, and projected retained earnings balance sheet
- `comparative_suite`
  Covers comparative reporting and equity changes
- `export_suite`
  Covers chart of accounts, journal entries, audit trail, periods, subledger, and book metadata exports
- `budget_suite`
  Covers budget vs actual reporting on generated budget lines
- `open_item_suite`
  Covers open-item listing for active-only and mixed-status views
- `classification_suite`
  Covers classified balance sheet, income statement, trial balance, and direct/indirect cash flow
- `dimension_suite`
  Covers dimension summary, rollup, and dimension metadata listing
- `abi_buffer_suite`
  Covers key C ABI buffer endpoints and one ABI export path
- `seed_report`, `seed_close`, `seed_revalue`
  Measures fixture generation cost explicitly
- `close_period`, `close_direct`, `close_income_summary`, `close_allocated`
  Measures close paths and prints internal close-phase timings
- `recalculate_stale`, `post_generated_entry`, `generate_opening_entry`
  Isolates common close sub-operations
- `query_scale`
  Runs the main read/report surfaces at multiple dataset sizes

## Public surfaces

Heft has two main entry points:

- Zig API via `src/root.zig`
- C ABI via [include/heft.h](include/heft.h)

The C ABI entry implementation lives in [src/main.zig](src/main.zig).

## Docs

- [Architecture](docs/architecture.md)
- [Book Designations](docs/designations.md)
- [Accounting Lifecycle](docs/lifecycle.md)
- [Embedding Guide](docs/embedding.md)
- [Benchmark Guide](docs/benchmarks.md)
- [Performance Matrix](docs/performance-matrix.md)
- [Stability Statement](docs/stability.md)
- [v0.1.1 Release Notes](docs/releases/v0.1.1.md)
- [Release Checklist](RELEASE.md)

## Release posture

The repo is currently positioned for `v0.1.1`.

- Version: `0.1.1`
- Stability: pre-`1.0`, with strong core behavior but still-evolving API packaging
- Local release gate: `bash scripts/quality-gate.sh`

## Important conventions

- Amounts are stored as `i64` scaled by `10^8`
- FX rates are stored as `i64` scaled by `10^10`
- Dates are `YYYY-MM-DD`
- Timestamps are UTC ISO 8601 text
- One `LedgerDB` handle per thread; handles are not thread-safe

## Testing strategy

The test suite includes:

- low-level SQLite wrapper tests
- schema and migration tests
- entity-level unit tests
- accounting lifecycle tests
- characterization tests
- C ABI integration tests

This is intentional: most regressions in accounting engines show up at boundary
surfaces and lifecycle seams, not just in isolated units.

## Contract sync

The repo now includes sync checks for:

- package version vs `ledger_version()`
- schema version vs `HEFT_SCHEMA_VERSION`
- selected header error codes vs the public `mapError()` contract

These tests are meant to catch drift between Zig internals and the public ABI.

## Current priorities

The codebase is strongest in:

- accounting invariants
- transactional integrity
- test coverage

The main maintainability hotspots are:

- [src/query_impl.zig](src/query_impl.zig)
- [src/report_impl.zig](src/report_impl.zig)
- [src/schema.zig](src/schema.zig)

The current ABI root has already been split into focused modules:

- [src/abi_common.zig](src/abi_common.zig)
- [src/abi_core.zig](src/abi_core.zig)
- [src/abi_buffers.zig](src/abi_buffers.zig)
- [src/abi_reports.zig](src/abi_reports.zig)
- [src/abi_integration_test.zig](src/abi_integration_test.zig)

## License

`AGPL-3.0-or-later`
