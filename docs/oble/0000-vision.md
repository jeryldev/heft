# OBLE-0000 Vision

Status: Draft

## Name

OBLE stands for Open Bookkeeping Ledger Exchange.

## Problem

Accounting data is routinely trapped inside application-specific schemas.

That creates three recurring problems:

- migration between systems is expensive and lossy
- reconciliation across multiple systems becomes a spreadsheet exercise
- agents and automation have no common ledger language to target

Most accounting systems implement the same deep semantics:

- books
- charts of accounts
- accounting periods
- double-entry journal entries
- posting lifecycle transitions
- audit expectations

But they expose those semantics through incompatible data models and APIs.

## Goal

OBLE is intended to define an open, implementation-independent standard for
representing and exchanging double-entry ledger state and lifecycle operations.

The first goal is not to standardize all of accounting.

The first goal is to standardize the smallest useful common ledger core so that:

- engines can exchange entries and balances with less ambiguity
- applications can migrate financial state more predictably
- automation and AI agents can target a stable accounting protocol
- multiple implementations can claim conformance to the same semantics

## Non-goals

OBLE is not initially trying to standardize:

- every jurisdiction-specific tax rule
- payroll or inventory behavior
- every report format
- every ERP workflow
- UI conventions
- one required transport protocol

Those areas may be addressed later through extensions or profiles, but they are
not part of the minimum viable core.

## Core thesis

The double-entry journal entry should be treated as a protocol primitive.

In the same way that HTTP standardizes request and response semantics without
requiring one implementation language, OBLE should standardize ledger semantics
without requiring one database, one wire transport, or one programming model.

## Initial scope

The first OBLE drafts should cover:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`
- posting lifecycle transitions
- accounting invariants
- extension points

The first extension layer should likely cover:

- counterparties
- subledgers
- open items

## Relationship to Heft

Heft is a reference implementation candidate, not the definition of OBLE.

Heft is useful because it demonstrates:

- a coherent ledger lifecycle
- explicit accounting invariants
- embeddable engine design
- a real implementation boundary between core and extensions

But OBLE should remain neutral enough that another engine in another language
can implement the same semantics without inheriting Heft-specific structure.

## Standardization strategy

OBLE should grow in small drafts:

1. Vision and scope
2. Core model
3. Lifecycle and invariants
4. Counterparty and subledger extension
5. Canonical serialization and conformance levels

## Success criteria

OBLE is succeeding if:

- two independent systems can exchange ledger data predictably
- a migration tool can target OBLE instead of one vendor schema
- a conformance test suite can validate core behavior
- Heft can export or consume OBLE without redefining its internals

## Failure modes to avoid

OBLE should avoid becoming:

- a renamed copy of one engine's private schema
- an overgrown ERP meta-spec
- a format that standardizes reports before semantics
- a spec whose required core is too large to implement

## Working principle

Start with universal ledger semantics.

Push jurisdiction, policy, and workflow-specific behavior into extensions unless
they are truly part of the irreducible double-entry core.
