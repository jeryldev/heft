# Heft vs OBLE

Status: Draft

## Purpose

This document explains the relationship between:

- `Heft` as an embedded accounting engine
- `OBLE` as an open ledger semantics and exchange standard

This distinction matters because users will reasonably ask:

- should I build against Heft or OBLE?
- which API do I call?
- is Heft being replaced by OBLE?

The short answer is:

- use `Heft` as the engine
- use `OBLE` as the common language

## The simple model

`Heft` is the accounting engine.

`OBLE` is the interoperability layer.

That means:

- applications typically call Heft APIs to do work
- systems, tools, import/export pipelines, and agents use OBLE-shaped payloads
  when they need a portable ledger language

## What Heft provides

Heft provides operational behavior:

- ledger storage
- posting rules
- period lifecycle
- close and reopen behavior
- revaluation
- subledgers and open items
- reporting
- audit logging
- embedding-oriented Zig and C surfaces

These are runtime and workflow capabilities.

## What OBLE provides

OBLE provides semantic portability:

- canonical ledger concepts
- lifecycle expectations
- packet shapes
- conformance vocabulary
- a portable language for exchange and automation

OBLE is not meant to replace a real engine.

It is meant to give multiple engines and tools a shared ledger boundary.

## Which API should a user call?

Most users should call `Heft` APIs.

Examples:

- `ledger_create_draft(...)`
- `ledger_add_line(...)`
- `ledger_post_entry(...)`
- `ledger_close_period(...)`
- `ledger_trial_balance(...)`

These are operational engine APIs.

Users should call `OBLE`-facing APIs when they need:

- import/export
- migration
- conformance
- portable interchange
- agent workflows
- cross-system integration

Examples:

- `ledger_oble_export_book(...)`
- `ledger_oble_export_entry(...)`
- `ledger_oble_export_counterparty_open_item(...)`
- `ledger_oble_import_session_open(...)`
- `ledger_oble_import_core_bundle(...)`

So the practical split is:

- `Heft API` for running the engine
- `OBLE API` for moving meaning across boundaries

## Is Heft being replaced by OBLE?

No.

OBLE is not a replacement for Heft.

The intended direction is:

- Heft becomes more explicitly aligned with OBLE core semantics
- while still remaining a richer engine with policy, lifecycle, and runtime
  behavior beyond the minimum standard

## The intended architecture split

Over time, Heft should become easier to understand in three layers:

1. `OBLE Core`
2. `Heft Policy Layer`
3. `Heft Runtime and Storage Layer`

### 1. OBLE Core

This is the universal ledger layer:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`
- lifecycle semantics
- balancing invariants
- multi-currency line semantics

This layer should be the most portable and standard-aligned part of Heft.

### 2. Heft Policy Layer

This is where Heft adds accounting power beyond bare core semantics:

- designations
- policy profiles
- close/reopen strategy
- subledger and open-item workflows
- approval behavior
- dimensions
- budgets

Some of this may map to OBLE profiles and extensions.

Some of it may remain implementation-specific.

### 3. Heft Runtime and Storage Layer

This is the implementation layer:

- SQLite
- cache materialization
- query optimization
- ABI wrappers
- benchmark surfaces
- storage-specific behavior

This layer is essential to Heft, but it is not part of OBLE core.

## What should migrate toward OBLE core?

The plan is not to rewrite the whole engine around spec documents.

The plan is to make the most universal parts of Heft more explicitly OBLE-core
aligned.

That includes:

- canonical export/import boundaries
- clearer core packet semantics
- conformance tests
- machine-checkable schemas
- internal module boundaries that better separate universal ledger semantics
  from policy and storage details

## What should remain Heft-specific?

Heft should keep the parts that make it a serious engine:

- designation-driven book policy
- close/reopen implementation choices
- runtime optimizations
- cache behavior
- SQLite-specific performance choices
- richer workflows above the core protocol

The goal is not to flatten Heft into a lowest-common-denominator spec engine.

The goal is to let Heft be both:

- a strong accounting engine
- a strong reference implementation of OBLE-aligned behavior

## Developer guidance

If you are embedding Heft:

- use Heft APIs for application behavior
- use OBLE exports/imports when you need portability
- for the richest OBLE import sequencing, prefer the Zig import-session boundary
  rather than ad-hoc one-shot packet calls
- if you are embedding from C, use the C ABI import-session handle for the
  stable packet set rather than inventing your own row-ID mapping
- use the import session's logical-ID resolution instead of assuming imported
  row IDs

If you are integrating multiple systems:

- prefer OBLE packet boundaries over product-specific table exports

If you are building future tooling or agents:

- treat OBLE as the ledger language
- treat Heft as one engine that speaks it

## Current direction

The repo is already moving in this direction:

- OBLE draft packet and schemas exist
- an explicit `oble_core.zig` boundary now exists for the core packet set
- an explicit `oble_profile_counterparty.zig` boundary now exists for the
  first profile-level packet family
- an explicit `oble_profile_fx.zig` boundary now exists for the emerging
  multi-currency and revaluation packet family
- an explicit `oble_profile_policy.zig` boundary now exists for policy and
  lifecycle-oriented profile packets
- Heft-to-OBLE conformance is documented
- canonical OBLE export exists for several packet families
- canonical OBLE import exists for the implemented core and extension packets
- C ABI export exposure is growing for the same packet set

The next work is to keep expanding that boundary carefully without forcing the
entire engine to become spec-shaped internally all at once.

## Repository strategy

The likely long-term shape is:

- `Heft` remains the engine and reference implementation
- `OBLE` moves into its own standards repo

That split should happen once the current draft packet, schemas, examples, and
profile matrix are stable enough to stand on their own.

When that happens:

- the `oble` repo should become the source of truth for the standard
- the `Heft` repo should keep the implementation code and conformance tests
- `Heft` should consume OBLE artifacts for validation, not treat OBLE as a
  runtime dependency

For the detailed split plan, see
[OBLE Repo Split Plan](oble-repo-split-plan.md).

Related docs:

- [Heft to OBLE Module Map](heft-oble-module-map.md)
- [Core, Policy, and Runtime](core-policy-runtime.md)
