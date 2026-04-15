# OBLE-0011 Budgets and Planning

Status: Draft

## Purpose

This draft defines an optional OBLE profile for budgets and budget line
planning.

The goal is to make budget structures portable enough for planning workflows
without pretending that all planning systems are the same.

## Why this is a profile

Budgets are important, but they are not part of the irreducible ledger core.

They belong in a profile because:

- a ledger can be valid without any budget layer
- planning semantics vary across implementations
- budget structures should remain distinct from posted financial history

## Core ideas

This profile introduces:

- `Budget`
- `BudgetLine`
- optional derived `BudgetActualComparison`

The minimum objective is to preserve the user-authored planning layer:

- which budget exists
- which period/account combinations it targets
- what amounts were planned

## Budget

A `Budget` defines one named planning set for a book.

Suggested fields:

- `id`
- `book_id`
- `name`
- `fiscal_year`
- `status`

### `status`

Examples:

- `draft`
- `approved`
- `closed`

An implementation may support additional statuses, but these are a useful
starting minimum.

## Budget line

A `BudgetLine` binds a planned amount to:

- one budget
- one account
- one period

Suggested fields:

- `budget_id`
- `account_id`
- `period_id`
- `amount`

## Invariants

At minimum:

- budget lines must reference an existing budget
- budget lines must reference accounts and periods in the same book
- planning amounts must not mutate posted journal history
- budget lifecycle must be distinct from entry lifecycle

## Relationship to actuals

This draft separates:

- user-authored planning state
- derived comparison outputs

That means the portable minimum should be:

- budget definitions
- budget line amounts

While `budget vs actual` style outputs may remain:

- implementation-specific at first
- or a later extension once enough convergence appears

## Suggested packet shapes

### `budget`

A single budget definition.

### `budget-lines`

Planned line items for one budget.

### `budget-profile-bundle`

A grouped bundle containing:

- budget metadata
- budget lines

## What this profile is not

This profile is not:

- a complete planning platform contract
- a forecasting standard
- a promise that all comparative budget reports will be portable

It is a portable baseline for budget structures and planned amounts.

## Relationship to Heft

Heft already has:

- budgets
- budget lines
- budget lifecycle transitions
- budget-vs-actual reporting

So Heft is a strong implementation candidate once the packet shapes are
defined and exported/imported explicitly.

## Current draft posture

This profile should currently be treated as:

- an important planning-oriented extension
- useful, but less foundational than the current implemented profiles
- now represented by an initial Zig-first profile bundle in Heft, but still
  missing broader examples, schemas, and wider public-surface exposure

## Immediate next steps

1. define a minimal budget example over one fiscal year and a few periods
2. decide whether budget comparisons belong in this profile or a later reporting profile
3. clarify how approval/closure semantics should be represented for planning layers
