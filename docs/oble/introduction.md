# Introducing OBLE

OBLE stands for Open Bookkeeping Ledger Exchange.

It is an emerging open standard for representing and exchanging double-entry
ledger state and lifecycle semantics across accounting engines, applications,
automation, and agents.

## Why OBLE exists

Today, accounting data is usually trapped inside product-specific schemas.

That makes a few things much harder than they should be:

- migrating between systems
- reconciling financial state across multiple tools
- building automation that can speak a stable ledger language
- letting AI agents interact with accounting systems safely at scale

Underneath those incompatible schemas, most systems are still doing the same
deep work:

- books
- accounts
- periods
- entries
- lines
- posting lifecycle
- auditability

OBLE exists to standardize that common semantic layer.

## What OBLE is

OBLE is:

- a semantics standard
- a lifecycle standard
- an exchange standard
- a conformance story

It starts with the smallest useful common core instead of trying to standardize
all of accounting at once.

## What OBLE is not

OBLE is not:

- a full ERP specification
- a jurisdiction-specific tax standard
- a payroll or inventory standard
- one required database schema
- one required transport protocol

## Why the journal entry matters

The deepest idea behind OBLE is that the double-entry journal entry should be
treated as a protocol primitive.

That means:

- entries are not just app records
- they are the common semantic unit different systems can exchange

In the same way SQL standardized a common way to express relational operations,
OBLE aims to standardize a common way to express ledger state and lifecycle.

## Relationship to Heft

Heft is the running engine that made these drafts possible.

It is useful because it already implements:

- exact double-entry posting
- period-aware lifecycle rules
- void and reverse semantics
- close and reopen behavior
- multi-currency accounting
- counterparties, subledgers, and open items
- designation-driven book policy

But OBLE is not just Heft renamed.

Heft is a reference implementation candidate.
OBLE is the extracted semantics layer.

## Current OBLE shape

The current draft set covers:

- vision
- core model
- lifecycle and invariants
- counterparties and subledgers
- serialization and conformance
- multi-currency semantics
- close and reopen profile
- designations and policy profiles

## What comes next

OBLE will become more useful as it grows in three directions:

1. clearer examples and validation artifacts
2. fixture-driven conformance testing
3. independent implementations or adapters

That is how it stops being a good idea and starts becoming a real standard.
