# OBLE-0009 Classifications and Report Structures

Status: Draft

## Purpose

This draft defines an optional OBLE profile for portable report structures that
classify ledger accounts into reusable hierarchies.

The goal is not to standardize every report layout.

The goal is to standardize enough structure that engines and tools can
exchange:

- classification trees
- node identity
- report intent
- account-to-node membership
- ordering and hierarchy

without confusing these structures with the chart of accounts itself.

## Why this belongs outside the core

The OBLE core already covers:

- books
- accounts
- periods
- entries
- lines

Classification structures are useful, but they sit above that irreducible
core.

They are best treated as a profile because:

- different systems structure reports differently
- classifications are not required for valid double-entry semantics
- classification logic should stay separate from raw posting identity

## Core ideas

This profile introduces:

- `Classification`
- `ClassificationNode`
- `ClassificationAccountBinding`

These structures describe how an implementation organizes accounts for a
specific reporting purpose.

## Classification

A `Classification` represents a named reporting structure.

Suggested fields:

- `id`
- `book_id`
- `name`
- `report_type`
- `status`

### `report_type`

Examples:

- `balance_sheet`
- `income_statement`
- `cash_flow`
- `trial_balance`
- `custom`

The profile should allow custom report types, but implementations may also
support more constrained enums.

## Classification nodes

A `ClassificationNode` represents one node inside the hierarchy.

Suggested fields:

- `id`
- `classification_id`
- `node_type`
- `label`
- `parent_id`
- `position`

### `node_type`

The minimum useful values are:

- `group`
- `account`

`group` nodes structure the hierarchy.

`account` nodes bind an account into the hierarchy while preserving the fact
that the chart of accounts remains the primary ledger identity.

## Account binding semantics

An account-binding node should preserve:

- the referenced account identifier
- the position of the node within the hierarchy
- the parent grouping relationship

This keeps report structure separate from account identity.

## Invariants

The profile should preserve at least these invariants:

- nodes belong to exactly one classification
- the hierarchy must be acyclic
- account-binding nodes must reference accounts in the same book
- sibling order must be explicit
- deleting or deactivating a classification should not change ledger history

## Relationship to reports

This profile standardizes report structure, not full report outputs.

That means:

- the hierarchy itself is portable
- generated report numbers are still derived from ledger state
- report rendering may remain implementation-specific

This is intentional.

OBLE should standardize portable accounting meaning first, then consider
whether more rendered report shapes should also become portable.

## Suggested packet shapes

### `classification`

A single classification definition.

### `classification-nodes`

An ordered list of nodes for one classification.

### `classification-profile-bundle`

A grouped bundle containing:

- classification metadata
- ordered node set
- account bindings

## What this profile is not

This profile is not:

- a chart of accounts replacement
- a rendered report file format
- a universal reporting engine contract

It is a portable structure for how accounts are grouped and ordered for
reporting purposes.

## Relationship to Heft

Heft already has rich classification support:

- classification trees
- grouped and account-bound nodes
- classified reports
- cash-flow structures
- classified trial-balance support

So Heft is a strong candidate reference implementation for this profile once
the OBLE packet shapes are defined more concretely.

## Current draft posture

This profile should currently be treated as:

- useful
- strongly motivated by real engine behavior
- not yet implemented as a stable OBLE packet set

## Immediate next steps

1. define example payloads for a minimal balance-sheet classification
2. define whether account binding lives inside node payloads or beside them
3. decide whether rendered classified reports belong in this profile or in a later one
