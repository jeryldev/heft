# OBLE-0001 Core Model

Status: Draft

## Purpose

This document defines the minimum core data model for OBLE.

The core model is intentionally narrow. It should be implementable by embedded
engines, service-backed systems, and plain-text tooling without requiring one
storage model or one transport protocol.

## Design rules

The core model should satisfy these constraints:

- every posted financial effect is represented by journal lines
- entries are balanced in aggregate
- the book is the policy and isolation boundary
- periods are first-class lifecycle boundaries
- amounts are exact, not floating-point
- mutation history must be recoverable or auditable

## Core entities

### Book

A `Book` is the top-level accounting boundary.

Minimum responsibilities:

- defines the accounting entity boundary
- defines the base currency
- defines decimal precision for presentation and fixed-point interpretation
- owns accounts, periods, and entries

Required fields:

- `id`
- `name`
- `base_currency`
- `decimal_places`

Recommended fields:

- `status`
- `fiscal_year_start`
- `metadata`

### Account

An `Account` is a classified ledger bucket inside one book.

Required fields:

- `id`
- `book_id`
- `code` or `number`
- `name`
- `account_type`
- `normal_balance`

Recommended fields:

- `status`
- `is_contra`
- `parent_account_id`
- `metadata`

Minimum account type taxonomy:

- `asset`
- `liability`
- `equity`
- `revenue`
- `expense`

Minimum normal balance values:

- `debit`
- `credit`

### Period

A `Period` is a lifecycle and reporting boundary for a book.

Required fields:

- `id`
- `book_id`
- `name`
- `start_date`
- `end_date`
- `status`

Recommended fields:

- `period_number`
- `year`
- `period_type`
- `metadata`

Minimum status taxonomy:

- `open`
- `soft_closed`
- `closed`
- `locked`

### Entry

An `Entry` is the unit of journal intent and posting lifecycle.

An entry contains one or more lines and becomes economically effective only
when it reaches a posted state according to the implementation's lifecycle
rules.

Required fields:

- `id`
- `book_id`
- `period_id`
- `status`
- `transaction_date`
- `posting_date`

Recommended fields:

- `document_number`
- `description`
- `entry_type`
- `reverses_entry_id`
- `created_at`
- `updated_at`
- `posted_at`
- `posted_by`
- `metadata`

Minimum status taxonomy:

- `draft`
- `posted`
- `void`
- `reversed`

### Line

A `Line` is the atomic financial effect inside an entry.

Required fields:

- `id`
- `entry_id`
- `line_number`
- `account_id`
- `debit_amount`
- `credit_amount`

Recommended fields:

- `transaction_currency`
- `fx_rate`
- `base_debit_amount`
- `base_credit_amount`
- `description`
- `metadata`

## Core relationships

- one `Book` has many `Accounts`
- one `Book` has many `Periods`
- one `Book` has many `Entries`
- one `Period` belongs to one `Book`
- one `Entry` belongs to one `Book`
- one `Entry` belongs to one `Period`
- one `Line` belongs to one `Entry`
- one `Line` references one `Account`

## Amount model

OBLE requires exact decimal semantics.

Allowed implementation strategies:

- scaled integers
- arbitrary precision decimals
- any exact fixed-point representation

OBLE should not require floating-point storage or arithmetic.

At the model level:

- `debit_amount` and `credit_amount` are exact values
- both cannot be economically positive at the same time for one line
- line orientation must be explicit, not inferred from sign alone

## Identity model

Every core entity must have a stable identifier within its book or global
implementation scope.

OBLE does not yet require one universal identifier format.

Allowed examples:

- 64-bit integers
- UUIDs
- ULIDs
- implementation-defined opaque IDs

The serialization profile may later place stronger requirements on identifier
encoding.

## Minimum invariants

The core model assumes these invariants:

1. Every `Account`, `Period`, and `Entry` belongs to exactly one `Book`.
2. Every `Line` belongs to exactly one `Entry`.
3. Every `Line.account_id` references an account in the same `Book` as the entry.
4. Every posted entry has at least two lines.
5. The total debits and total credits of a posted entry are equal.
6. `account_type` and `normal_balance` are explicit, not inferred from usage.
7. `posting_date` and `period_id` are both first-class lifecycle references.

## Deliberate exclusions from the core

These concepts matter, but are not part of the smallest required OBLE core:

- counterparties
- open items
- subledger groups
- dimensions
- budgets
- classifications
- approval workflows
- jurisdiction-specific tax semantics

They should be modeled in extension drafts unless later evidence shows they are
part of the irreducible core.

## Mapping note for Heft

Heft already contains a close mapping to this model:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`

But Heft also includes policy-bearing designations and extension surfaces that
should not automatically be promoted into the OBLE core without separate
justification.
