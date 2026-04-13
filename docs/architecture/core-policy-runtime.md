# Heft Core, Policy, and Runtime

Status: Draft

## Purpose

This document describes a practical long-term module split for Heft.

The goal is to make it easier to evolve Heft toward:

- clearer OBLE alignment
- cleaner internal boundaries
- eventual storage-port readiness

without weakening the current engine.

## Three-layer model

Heft should gradually be understood as three layers:

1. `Core`
2. `Policy`
3. `Runtime`

This is a design target, not a claim that the codebase is already perfectly
separated today.

## 1. Core

The `Core` layer is where the universal ledger semantics live.

Examples:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`
- posting invariants
- debit/credit balancing
- reverse and void semantics
- base-currency balancing

This is the part that should align most closely with OBLE core.

### Core qualities

The core should be:

- implementation-neutral where reasonable
- explicit about invariants
- usable as the semantic center of exporters, importers, and conformance tests

## 2. Policy

The `Policy` layer adds behavior that is still accounting logic, but not part
of the irreducible core.

Examples:

- designations
- policy profiles
- entity-type behavior
- close strategy
- suspense rules
- subledger policies
- open-item settlement rules
- approval requirements
- dimensions and budgets

These are real accounting behaviors, but they are not all universal enough to
belong to the smallest common protocol core.

### Policy qualities

The policy layer should be:

- explicit
- auditable
- configurable at the book/profile level
- increasingly expressible through OBLE profiles where appropriate

## 3. Runtime

The `Runtime` layer is how the engine is actually implemented.

Examples:

- SQLite persistence
- balance cache tables
- query planning and indexing
- export and ABI buffer plumbing
- test harnesses
- benchmarks
- performance-oriented internal utilities

This layer matters enormously for production use, but it is not the same thing
as ledger semantics.

### Runtime qualities

The runtime should be:

- fast
- deterministic
- bounded
- explicit about tradeoffs

## Why this split matters

Without this split, different kinds of decisions blur together:

- protocol decisions
- accounting-policy decisions
- storage decisions

That makes it harder to:

- reason about OBLE conformance
- expose clean import/export boundaries
- add future storage ports safely
- explain the system to users and contributors

## How this maps to OBLE

Roughly:

- `Core` maps to `OBLE Core`
- parts of `Policy` map to OBLE profiles and extensions
- `Runtime` is mostly outside OBLE

This is why the long-term direction is not "make all of Heft into OBLE."

The better framing is:

- make the core more explicitly OBLE-aligned
- make the policy layer more explicitly profile-driven
- keep runtime details separate from both

## Current examples in Heft

### Core-heavy

- entry creation and posting invariants
- period status transitions
- balance equation enforcement
- line currency/base amount semantics

### Policy-heavy

- retained earnings designation
- income summary designation
- suspense enforcement
- approval requirement
- counterparties and open items
- close/reopen strategy

### Runtime-heavy

- SQLite schema and migrations
- ledger account balance cache
- ABI buffer exports
- benchmark harnesses
- query/index tuning

## What should happen next

### Near-term

- keep expanding OBLE export/import/conformance boundaries
- make profile-oriented packet shapes clearer
- document which behaviors are core versus policy

### Mid-term

- identify modules that mix core, policy, and runtime concerns too heavily
- refactor around those seams
- avoid premature generic-storage abstraction

### Long-term

- extract narrow storage ports only after the semantic seams are clear
- allow Heft to remain SQLite-first even if ports exist

## Design rule

When adding a new feature, ask:

1. Is this universal ledger semantics?
2. Is this policy behavior?
3. Is this runtime/storage implementation?

The answer should shape where the code and the docs go.
