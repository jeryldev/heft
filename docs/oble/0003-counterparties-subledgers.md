# OBLE-0003 Counterparties and Subledgers

Status: Draft

## Purpose

This document defines the first extension layer above the OBLE core for
counterparties, subledgers, and open-item style settlement workflows.

These concepts matter in real accounting systems, but they are not part of the
irreducible double-entry minimum.

They are standardized here as an extension so that implementations can share
AR/AP semantics without forcing every conforming ledger to model them.

## Extension principle

The core ledger remains valid without counterparties.

This extension adds standardized semantics for cases where an implementation
needs to represent:

- customers
- suppliers
- members
- employees
- investors
- borrowers
- any other named external or internal settlement party

## Counterparty

A `Counterparty` is a ledger-adjacent entity that can participate in lines,
open items, and subledger workflows.

Required fields:

- `id`
- `book_id`
- `name`

Recommended fields:

- `number` or `code`
- `role`
- `status`
- `metadata`

### Role model

OBLE should not hardcode only `customer` and `supplier`.

Minimum recommended role values:

- `customer`
- `supplier`
- `employee`
- `investor`
- `other`

For the current draft-0 profile, the canonical schema carries one primary
`role` string per exported counterparty.

Implementations may still support richer native models internally:

- one legal entity may have more than one role
- an engine may expose a combined role such as `both`
- future drafts may widen the schema if a portable multi-role model proves
  necessary

For now, the portable minimum is one explicit primary role in the packet.

## Line-level counterparty linkage

A ledger line may optionally reference a `Counterparty`.

Recommended field:

- `counterparty_id`

Semantics:

- the line remains a normal journal line
- the counterparty reference adds subledger meaning
- the counterparty does not replace the account reference

This is important because subledger accounting is still ledger accounting.

The account remains the posting bucket. The counterparty refines who that line
is about.

## Subledger group

A `SubledgerGroup` associates a set of counterparties with a controlling ledger
account.

Required fields:

- `id`
- `book_id`
- `name`
- `group_type`
- `control_account_id`

Recommended fields:

- `number`
- `metadata`

Typical examples:

- Accounts receivable customers
- Accounts payable suppliers
- Employee advances
- Investor capital accounts

## Control account semantics

If a subledger group is linked to a control account:

- lines using counterparties in that group should post through that control
  account according to implementation policy
- the aggregate subledger balance should reconcile to the control account

OBLE does not require one exact enforcement mechanism, but it does define the
expected accounting relationship.

## Open item

An `OpenItem` represents an unsettled amount tied to a source line.

Required fields:

- `id`
- `book_id`
- `entry_line_id`
- `counterparty_id`
- `original_amount`
- `remaining_amount`
- `status`

Recommended fields:

- `due_date`
- `metadata`

Minimum status taxonomy:

- `open`
- `partial`
- `closed`

## Open-item invariants

Minimum extension invariants:

1. An open item references one source line.
2. The source line and counterparty belong to the same book.
3. The open-item amount must not exceed the economically relevant source line
   amount.
4. Remaining amount must never be negative.
5. Closed items have zero remaining amount.

## Settlement or allocation

A settlement or allocation operation reduces the remaining amount of an open
item.

Minimum semantics:

- allocation amount must be positive
- allocation amount must not exceed remaining amount
- allocation transitions status consistently
- allocation must be auditable

This draft does not yet standardize the full many-to-many settlement graph.

For the first extension level, it is enough to standardize the meaning of
reducing an open item's outstanding balance.

## Reconciliation expectations

If an implementation exposes subledger reconciliation:

- the sum of active open items or subledger balances should reconcile to the
  related control account according to the implementation's published rules
- differences should be detectable, not silently ignored

## Import sequencing guidance

Implementations that support import should expect this extension layer to carry
real lifecycle dependencies.

Recommended sequencing:

1. create or resolve the `Counterparty`
2. import the `Entry` and its `Line` objects so the source line exists
3. create the `OpenItem` against the imported source line
4. apply any settlement or allocation state

This ordering matters because open items are not free-floating records. They
derive their meaning from a source line, a counterparty, and a shared book
boundary.

## Customer and supplier naming

OBLE should describe these as roles, not separate primitive entity types.

Why:

- not every system uses separate customer and supplier tables
- one legal entity may be both
- many domains need more roles than just AR and AP

So the preferred pattern is:

- generic `Counterparty`
- role tags
- optional `SubledgerGroup`
- optional `OpenItem`

The current draft keeps the packet shape simple by using one primary `role`
string, while allowing implementations to document richer native role models.

## What remains out of scope

This extension still does not standardize:

- tax engines
- payment rail integration
- collection workflows
- inventory costing
- payroll deductions
- fund waterfall allocations

Those may need later extensions or domain-specific profiles.

## Mapping note for Heft

Heft already maps closely to this extension model:

- subledger groups
- subledger accounts acting as counterparties
- line counterparty references
- open items
- allocation
- reconciliation and aging reports

The main adjustment for OBLE is naming and generalization: the spec should talk
in terms of generic counterparties and roles so that the extension remains
portable beyond one engine's terminology.
