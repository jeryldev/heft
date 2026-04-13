# Heft Module Boundary Inventory

Status: Draft

## Purpose

This document classifies the current Heft modules by architectural weight:

- domain-heavy
- mixed domain/storage
- storage-heavy
- boundary/adapters

The goal is not to pretend the codebase is already cleanly layered.

The goal is to make the next storage-port and OBLE-alignment moves more
deliberate by naming where the current seams actually are.

## Categories

### Domain-heavy

Modules where the primary value is accounting semantics, lifecycle rules, and
invariants.

### Mixed domain/storage

Modules where the accounting logic is real, but tightly intertwined with SQL,
persistence shape, or materialization concerns.

### Storage-heavy

Modules where the primary value is schema, persistence mechanics, queries, or
database-facing infrastructure.

### Boundary/adapters

Modules whose main job is import/export, ABI bridging, reporting boundaries, or
other interoperability surfaces.

## Inventory

### Domain-heavy

- `entry.zig`
  Posting, reversing, voiding, and line-level accounting invariants live here.
- `close.zig`
  Close and opening-carry semantics are primarily lifecycle logic.
- `revaluation.zig`
  Multi-currency economic semantics and revaluation behavior are core logic.
- `open_item.zig`
  Settlement and open-item correctness is mostly domain behavior.
- `verify.zig`
  Ledger integrity checks are domain assertions over stored state.

### Mixed domain/storage

- `book.zig`
  Strong policy semantics, but directly coupled to persistence and audit writes.
- `account.zig`
  Mostly domain rules plus direct SQL operations.
- `period.zig`
  Period lifecycle semantics plus direct state transitions in storage.
- `subledger.zig`
  Counterparty/subledger semantics with persistence entwined.
- `classification.zig`
  Reporting semantics, hierarchy logic, and direct query/storage behavior are
  blended.
- `dimension.zig`
  Analytical semantics plus storage-facing summaries/rollups.
- `budget.zig`
  Workflow semantics plus direct persistence.
- `batch.zig`
  Workflow orchestration over entry persistence.

### Storage-heavy

- `db.zig`
  SQLite wrapper and statement lifecycle.
- `schema.zig`
  DDL, migrations, indexes, and database structure.
- `cache.zig`
  Balance materialization and stale-state recomputation.
- `query_impl.zig`
  Read/query orchestration over persisted state.
- `report_impl.zig`
  Heavier reporting query/materialization layer.
- `export.zig`
  Output formatting over queried state.

### Boundary/adapters

- `main.zig`
  C ABI root wiring.
- `abi_common.zig`
  ABI error/handle utilities.
- `abi_core.zig`
  C wrapper boundary for mutations/workflows.
- `abi_buffers.zig`
  Buffer/query/export C boundary.
- `abi_reports.zig`
  Report/classified-result C boundary.
- `oble_export.zig`
  OBLE serialization boundary.
- `oble_import.zig`
  OBLE import/interchange boundary.
- `query.zig`
  Public read facade.
- `report.zig`
  Public report facade.

## What this implies

### Best candidates for future storage ports

These are the narrowest, highest-value starting points:

- audit append
- entry/line persistence
- period state transition persistence
- open-item persistence
- report/cache read interfaces

### Best candidates to keep SQLite-first longest

- `schema.zig`
- `db.zig`
- `cache.zig`
- large parts of `query_impl.zig`
- large parts of `report_impl.zig`

These are the most SQLite-shaped surfaces in the repo today.

### Best candidates for further OBLE alignment

- `entry.zig`
- `close.zig`
- `revaluation.zig`
- `open_item.zig`
- `oble_export.zig`
- `oble_import.zig`

These are where the universal semantics are most visible.

## Immediate next step

Use this inventory before any storage-port refactor.

The first question should not be:

- "how do we abstract SQLite?"

It should be:

- "which mixed modules need a clearer semantic seam first?"
