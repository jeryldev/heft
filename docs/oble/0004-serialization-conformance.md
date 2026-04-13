# OBLE-0004 Serialization and Conformance

Status: Draft

## Purpose

This document defines an initial serialization profile and a first pass at
conformance levels for OBLE.

The goal is not to freeze one perfect wire format on day one.

The goal is to give implementations a common exchange surface and a way to make
credible interoperability claims without requiring every optional feature.

## Serialization goals

The first OBLE serialization profile should be:

- text-based
- deterministic enough for interchange
- simple to generate from most systems
- simple to validate
- extensible without breaking the core

## Initial format choice

The initial canonical serialization should be JSON.

Why JSON first:

- nearly every language and platform can produce and consume it
- it is readable enough for debugging and migration work
- it is a practical fit for agents, automation, and APIs
- it keeps the first OBLE profile small

This does not prevent future profiles such as:

- newline-delimited event streams
- binary snapshots
- SQL-friendly tabular encodings

## Core document shapes

The serialization profile should define canonical document shapes for:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`

Recommended exchange bundles:

- `book_snapshot`
- `entry_batch`
- `period_snapshot`

## Core encoding requirements

### Identifiers

Identifiers may remain implementation-defined, but serialized IDs must be
stable within the exchange payload.

Allowed examples:

- integer IDs
- UUID strings
- ULID strings

The profile should not require one identifier scheme in the first draft.

### Dates and timestamps

Recommended date encoding:

- `YYYY-MM-DD`

Recommended timestamp encoding:

- RFC 3339 / ISO 8601 UTC timestamps where timestamps are present

### Amounts

Amounts must not be serialized as ambiguous floating-point values.

Recommended approaches:

- decimal string
- integer fixed-point plus declared scale

For the first OBLE JSON draft, the safest recommendation is:

- exact decimal values encoded as strings

Examples:

- `"100.00"`
- `"565.0000000000"`

This avoids cross-language float drift and makes conformance easier.

### Enums and status values

Status, type, and role values should be serialized as lowercase strings.

Examples:

- `posted`
- `reversed`
- `asset`
- `customer`

## Core extensibility rule

OBLE documents should allow extension fields.

Requirements:

- core required fields must preserve their semantics
- extension fields must not silently redefine core meaning
- implementations should ignore unknown extension fields unless the profile
  explicitly requires failure

Recommended convention:

- reserve `extensions` or profile-specific namespaces for non-core fields

## Minimal JSON examples

### Entry

```json
{
  "id": "entry-001",
  "book_id": "book-001",
  "period_id": "2026-01",
  "status": "posted",
  "transaction_date": "2026-01-15",
  "posting_date": "2026-01-15",
  "document_number": "JE-001",
  "lines": [
    {
      "id": "line-001",
      "line_number": 1,
      "account_id": "1000",
      "debit_amount": "1000.00",
      "credit_amount": "0.00"
    },
    {
      "id": "line-002",
      "line_number": 2,
      "account_id": "3000",
      "debit_amount": "0.00",
      "credit_amount": "1000.00"
    }
  ]
}
```

### Counterparty extension example

```json
{
  "id": "line-003",
  "line_number": 1,
  "account_id": "1100",
  "debit_amount": "500.00",
  "credit_amount": "0.00",
  "counterparty_id": "cust-001"
}
```

## Conformance model

OBLE should support layered conformance rather than a single all-or-nothing
claim.

### Core conformance

A `Core` conforming implementation supports:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`
- core lifecycle semantics
- core invariants
- canonical JSON serialization for core entities

### Period-aware conformance

A `Period-Aware` implementation additionally supports:

- explicit period state handling
- close-aware lifecycle behavior

### Reversible conformance

A `Reversible` implementation additionally supports:

- explicit reverse semantics
- recoverable original-to-reversal linkage

### Counterparty/Subledger conformance

A `Counterparty/Subledger` implementation additionally supports:

- counterparties
- subledger groups
- open items or equivalent unsettled-balance constructs

### Multi-currency conformance

A `Multi-Currency` implementation additionally supports:

- transaction currency separate from base currency
- explicit FX rate semantics
- exact base-amount derivation rules

## Conformance claims

An implementation should not simply say "OBLE compliant."

It should say something closer to:

- `OBLE Core`
- `OBLE Core + Period-Aware + Reversible`
- `OBLE Core + Counterparty/Subledger + Multi-Currency`

That makes adoption more realistic and avoids false equivalence between minimal
and feature-rich engines.

## Validation expectations

The OBLE project should eventually publish:

- JSON schemas or equivalent validators
- reference fixtures
- conformance tests

The first draft does not need a full test suite yet, but the serialization
profile should be written so that a conformance suite can emerge naturally from
it.

## Mapping note for Heft

Heft likely targets at least:

- `OBLE Core`
- `Period-Aware`
- `Reversible`
- `Counterparty/Subledger`
- `Multi-Currency`

But those claims should be earned through explicit fixture-driven conformance
tests rather than assumed from implementation intent.
