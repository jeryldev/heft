# Heft Storage Port Plan

Status: Draft

## Purpose

This document explains how Heft could evolve toward storage pluggability without
damaging the accounting invariants that currently benefit from a SQLite-first
design.

The central position of this plan is:

- storage abstraction is reasonable
- but it should come after semantic boundary work, not before it

## Why not abstract the database immediately

SQLite is not just a replaceable implementation detail in current Heft.

It currently provides:

- transactional atomicity
- relational constraints
- deterministic embedded deployment
- strong OLTP behavior
- simple and explicit query power

If Heft is abstracted too early around a generic storage layer, likely failure
modes include:

- weaker invariants
- leaky abstractions
- lowest-common-denominator design
- extra complexity before the stable seams are known

## Principle

Extract domain ports after the semantic boundary is clear.

That means:

1. define the accounting contract
2. expose the OBLE contract
3. identify stable storage-facing seams
4. only then design the port layer

## What should become a port eventually

Not every SQL query needs its own abstraction.

The right storage ports are likely broader domain capabilities such as:

- transaction boundary management
- entity persistence for books, accounts, periods, entries, and lines
- audit append
- open-item persistence
- report/materialization reads
- cache or balance materialization

These should remain narrow and purpose-built.

## Suggested architecture

### Domain layer

Pure accounting semantics and lifecycle rules.

Examples:

- posting invariants
- close/reopen rules
- reverse/void semantics
- counterparty/open-item invariants

### Storage port layer

Interfaces for persistence and retrieval needed by the domain layer.

Examples:

- load entry for posting
- append audit record
- persist lines
- recalculate or query balance materialization

### SQLite adapter layer

The current implementation, moved behind the storage ports over time.

## Phased plan

### Phase 1: Identify seams

Map current modules into:

- pure domain logic
- SQL-coupled persistence logic
- mixed modules that need refactoring

### Phase 2: Introduce narrow ports

Create small interfaces around the most stable requirements first.

Good early candidates:

- audit persistence
- entry/line persistence
- period state transitions

### Phase 3: Keep SQLite as the only adapter

Do not chase a second backend immediately.

The first goal is not optionality for its own sake.

The first goal is cleaner boundaries while preserving behavior.

### Phase 4: Add one secondary adapter only if justified

Potential candidates:

- in-memory adapter for tests
- PostgreSQL adapter for service-hosted use
- another embedded adapter if a compelling use case appears

## Important constraint

Heft should remain free to be optimized for SQLite even if a port layer exists.

That means:

- do not erase assumptions that are essential to current correctness
- do not forbid relational enforcement where it materially helps
- do not require every backend to support identical performance profiles

## Relationship to OBLE

OBLE and storage ports solve different problems.

OBLE solves:

- semantic portability
- exchange portability
- conformance clarity

Storage ports solve:

- implementation portability
- backend optionality
- cleaner domain boundaries

OBLE should come first because it clarifies what the engine means.

Storage ports should come later because they clarify how the engine is
implemented.

## Immediate next steps

1. Finish the Heft-to-OBLE conformance matrix
2. Add canonical OBLE export/import boundaries
3. Annotate existing modules as domain-heavy vs storage-heavy
   See: [module-boundary-inventory.md](module-boundary-inventory.md)
4. Only then design the first narrow storage ports
