# Heft OBLE Alignment Plan

Status: Draft

## Purpose

This document defines the plan for bringing Heft into explicit alignment with
OBLE as an open ledger standard.

The key idea is:

- Heft already embodies much of the semantics
- but it does not yet expose those semantics as a clean standards boundary

So the work is not primarily "invent accounting semantics."

The work is:

- extract
- formalize
- validate
- expose

## Current state

Heft already aligns strongly with the current OBLE drafts in:

- core ledger model
- entry lifecycle
- period-aware behavior
- reverse and void semantics
- multi-currency accounting
- counterparties, subledgers, and open items
- close and reopen behavior
- designation-driven book policy

Current gaps are mostly at the boundary layer:

- no canonical OBLE exporter
- no canonical OBLE importer
- no fixture-driven conformance test suite
- no formal "Heft claims X OBLE profiles" matrix wired to tests

## Guiding principle

Make Heft OBLE-pluggable before making Heft database-pluggable.

This matters because the higher-leverage gap today is semantic portability, not
storage portability.

## Phase 1: Declare and measure conformance

### Goal

Move from "Heft appears aligned" to "Heft can justify concrete OBLE claims."

### Work

- create `docs/oble/heft-conformance.md`
- classify each OBLE draft area as:
  - confirmed
  - partial
  - draft-dependent
  - not yet implemented
- map those claims to concrete code and test surfaces

### Output

A transparent Heft-to-OBLE conformance matrix.

## Phase 2: Add canonical OBLE export

### Goal

Make Heft capable of emitting canonical OBLE payloads directly.

### Scope

Start with read/export only:

- `Book`
- `Account`
- `Period`
- `Entry`
- reversal pair
- counterparty/open item extension payloads

### Work

- define one canonical OBLE export module
- serialize using the current OBLE example/schema shapes
- validate exported payloads against the OBLE draft schemas

### Output

A real `Heft -> OBLE` bridge.

## Phase 3: Add canonical OBLE import

### Goal

Prove that OBLE is not just a documentation format but an interchange layer.

### Scope

Start with safe import targets:

- books
- accounts
- periods
- simple entries

Delay more sensitive imports until the rules are clear:

- close-derived state
- policy profiles
- revaluation artifacts

### Output

A minimal `OBLE -> Heft` import path.

## Phase 4: Fixture-driven conformance

### Goal

Move from example documentation to executable standards validation.

### Work

- publish fixture bundles
- validate shape with schemas
- validate semantics with engine-level tests
- record which OBLE profiles Heft actually passes

### Output

The first serious OBLE conformance story.

## Phase 5: Public standards boundary

### Goal

Make OBLE feel like an actual external contract, not an internal doc set.

### Work

- stabilize canonical payload shapes
- consider moving OBLE to its own repository
- keep Heft as a reference implementation

## Things not to do too early

- do not rewrite Heft around generic storage traits first
- do not try to standardize all policy and jurisdiction detail immediately
- do not tie OBLE too tightly to Heft table names or ABI conventions

## Success criteria

This plan is working if:

- Heft can export OBLE payloads directly
- those payloads validate against OBLE schemas
- lifecycle fixtures prove Heft's semantics, not just its shape
- OBLE becomes implementable by another engine without copying Heft internals

## Immediate next steps

1. Write `docs/oble/heft-conformance.md`
2. Define canonical `Heft -> OBLE` export targets
3. Decide where those exporters should live in the codebase
4. Add first schema-validated export tests
