# Heft to OBLE Module Map

Status: Draft

## Purpose

This document labels the current Heft codebase by semantic role:

- `OBLE Core`
- `OBLE Profiles`
- `Heft Runtime`
- `Cross-cutting / Transitional`

The goal is not to claim the codebase is already perfectly separated.

The goal is to make the current direction explicit:

- which parts of Heft are becoming more clearly OBLE-aligned
- which parts represent profile-level accounting behavior
- which parts are engine/runtime implementation

## Classification rules

### OBLE Core

Modules whose primary responsibility is universal ledger semantics:

- books, accounts, periods, entries, lines
- posting/balancing rules
- core lifecycle semantics
- exact-amount and currency/base-amount behavior

### OBLE Profiles

Modules whose primary responsibility is richer but still portable accounting
behavior that sits above the irreducible core:

- counterparties and subledgers
- open items and settlements
- close/reopen profile behavior
- revaluation profile behavior
- policy/designation profiles

### Heft Runtime

Modules whose primary responsibility is implementation, persistence,
materialization, ABI transport, formatting, or performance:

- SQLite wrapper and schema
- caches
- query materialization
- C ABI wrappers
- buffer/export plumbing
- benchmark harnesses

### Cross-cutting / Transitional

Modules that currently mix semantic and runtime concerns heavily enough that
they should not yet be treated as cleanly belonging to only one layer.

These are the places most likely to need future refactoring.

## Current map

### OBLE Core

- `src/book.zig`
  Book identity, fiscal-year anchoring, and core book invariants.
- `src/account.zig`
  Chart-of-accounts identity and account-level posting constraints.
- `src/period.zig`
  Period lifecycle and period-state enforcement.
- `src/entry.zig`
  Draft/post/void/reverse mechanics, line semantics, and balance invariants.
- `src/money.zig`
  Exact arithmetic and base-amount derivation behavior.
- `src/oble_core.zig`
  Explicit OBLE-core packet boundary for the canonical `Book`, `Account[]`,
  `Period[]`, and `Entry` packet set.

These modules are the strongest candidates for explicit OBLE-core alignment in
future refactors.

### OBLE Profiles

- `src/oble_profile_counterparty.zig`
  Explicit profile boundary for counterparties, open items, and the current
  counterparty/open-item bundle surface.
- `src/oble_profile_policy.zig`
  Explicit profile boundary for policy/designation import-export plus the
  current close/reopen and revaluation lifecycle packet surface.
- `src/subledger.zig`
  Counterparty-account semantics and control-account relationships.
- `src/open_item.zig`
  Open-item creation, remaining-balance tracking, and settlement lifecycle.
- `src/close.zig`
  Close/opening-entry generation and close-profile behavior.
- `src/revaluation.zig`
  FX-position and revaluation profile behavior.
- `src/classification.zig`
  Classification-driven statement profiles, especially cash-flow/classified
  reporting semantics.
- `src/dimension.zig`
  Analytical tagging and rollup profile behavior.
- `src/budget.zig`
  Budget workflow and budget-vs-actual profile behavior.

These are good candidates for future “OBLE profile” naming and conformance
boundaries rather than forcing them into the smallest common core.

### Heft Runtime

- `src/db.zig`
  SQLite wrapper and statement lifecycle.
- `src/schema.zig`
  DDL, migrations, and index strategy.
- `src/cache.zig`
  Balance materialization and stale recomputation.
- `src/export.zig`
  CSV/JSON formatting helpers.
- `src/query_common.zig`
- `src/query_books.zig`
- `src/query_entries.zig`
- `src/query_subledger.zig`
- `src/query_impl.zig`
  Read/query shaping, output formatting, and database-facing retrieval logic.
- `src/report_common.zig`
- `src/report_ledger.zig`
- `src/report_statements.zig`
- `src/report_compare.zig`
- `src/report_impl.zig`
  Report materialization and output-facing report assembly.
- `src/abi_common.zig`
- `src/abi_core.zig`
- `src/abi_buffers.zig`
- `src/abi_reports.zig`
- `src/main.zig`
  Public C ABI boundary and buffer/result transport.
- `src/benchmark.zig`
- `src/bench_feature_suites.zig`
  Benchmark/runtime instrumentation.

### Cross-cutting / Transitional

- `src/verify.zig`
  Domain integrity semantics over runtime-derived state.
- `src/batch.zig`
  Workflow orchestration over core mutation APIs.
- `src/oble_export.zig`
  Boundary layer, but semantically close to both OBLE and Heft internals.
- `src/oble_import.zig`
  Same: portability boundary that depends on both OBLE packet semantics and
  live Heft runtime behavior.
- `src/oble_conformance_test.zig`
  Standards-facing verification over runtime and semantic boundaries at once.

## What should become more explicitly OBLE-shaped?

The best near-term candidates are:

- `book.zig`
- `account.zig`
- `period.zig`
- `entry.zig`
- `money.zig`
- `oble_export.zig`
- `oble_import.zig`

That does not mean large rewrites.

It means:

- clearer packet semantics
- clearer naming around core versus profile behavior
- stronger conformance boundaries
- fewer runtime details leaking into the semantic story

## What should stay Heft-specific?

These are valuable Heft traits and do not need to be flattened into OBLE core:

- designation-driven book policy
- SQLite-oriented storage choices
- balance cache implementation
- ABI/result transport details
- benchmark and performance harnesses
- some workflow-specific policy behavior

## Immediate engineering guidance

When touching a module, ask:

1. Is this code expressing universal ledger semantics?
2. Is it expressing a portable accounting profile?
3. Is it expressing runtime/storage mechanics?

If the answers are mixed, that module is a candidate for future seam cleanup.

## Near-term next step

Use this map together with:

- [Heft vs OBLE](heft-vs-oble.md)
- [Core, Policy, and Runtime](core-policy-runtime.md)
- [Heft Module Boundary Inventory](module-boundary-inventory.md)

to decide which APIs or modules should be labeled next as:

- `OBLE Core`
- `OBLE Profile`
- `Heft Runtime`
