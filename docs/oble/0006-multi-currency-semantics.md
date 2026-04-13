# OBLE-0006 Multi-Currency Semantics

Status: Draft

## Purpose

This document defines the minimum semantics for representing and exchanging
multi-currency ledger activity in OBLE.

The goal is not to standardize every FX accounting policy in the first draft.

The goal is to standardize the irreducible meanings of:

- transaction currency
- base currency
- exchange rate
- exact base amount derivation

## Why this matters

Multi-currency support is one of the first places where accounting systems stop
being portable.

Two systems may both claim to support FX, yet differ on:

- whether they store transaction amounts, base amounts, or both
- how they encode exchange rates
- when they round
- how they treat later revaluation

If OBLE leaves these semantics implicit, interoperable exchange will be weak
even when the rest of the core model is aligned.

## Core concepts

### Base currency

Every `Book` has one base currency.

The base currency is the unit in which book-level balances and canonical ledger
totals are ultimately expressed.

### Transaction currency

A `Line` may be denominated in a transaction currency that differs from the
book's base currency.

Recommended field:

- `transaction_currency`

If omitted, the transaction currency is assumed to be the same as the book's
base currency.

### Exchange rate

If a line's transaction currency differs from the book's base currency, the
exchange rate used for that line must be explicit or derivable.

Recommended field:

- `fx_rate`

## Minimum model requirements

For a multi-currency-capable implementation, a line should expose enough data
to recover both:

- the transaction-side amount
- the base-currency economic effect

Recommended fields:

- `transaction_currency`
- `fx_rate`
- `base_debit_amount`
- `base_credit_amount`

## Exactness requirement

OBLE multi-currency semantics require exact arithmetic, not floating-point
approximation.

Allowed implementation approaches:

- scaled integers
- decimal fixed-point
- arbitrary precision decimal

Not recommended:

- binary floating-point as the primary accounting representation

## Base amount derivation

If an implementation stores both transaction amounts and base amounts, the
relationship between them must be deterministic.

That means:

- the exchange-rate interpretation is explicit
- the rounding point is explicit
- the resulting base amount is not ambiguous

OBLE does not yet mandate one global rate scale, but a conforming
implementation must publish the scale or exact decimal interpretation it uses.

## Same-currency case

If `transaction_currency == book.base_currency`:

- base amount and transaction amount are economically identical
- the implementation may still store both explicitly
- the serialization should not imply a different FX conversion took place

## Line orientation rule

Multi-currency does not change line orientation semantics.

The same debit/credit invariants still apply:

- one line must not carry two positive economic directions at once
- base amounts and transaction amounts must preserve the same orientation

## Entry balancing rule

For a posted multi-currency entry, balancing must hold in the book's economic
measurement domain.

At minimum, this means:

- total base debits equal total base credits

An implementation may also require transaction-side balance constraints, but
the base-currency balance invariant is the minimum interoperable rule.

## Revaluation

OBLE recognizes revaluation as a real accounting concern, but it does not place
full revaluation mechanics in the minimum multi-currency core.

For now, the standard only assumes:

- original posted FX-denominated lines preserve their original exchange context
- later revaluation is a separate lifecycle event or derived accounting process
- revaluation must not erase the original transaction meaning

Detailed revaluation profiles can come later.

Implementations may still expose a revaluation packet or profile for exchange.
Heft already does this at the exporter boundary, but OBLE does not yet treat
that packet as fully normative core behavior.

## Serialization guidance

Recommended JSON representation for a foreign-currency line:

```json
{
  "id": "line-200",
  "line_number": 1,
  "account_id": "acct-1001",
  "transaction_currency": "USD",
  "debit_amount": "1000.00",
  "credit_amount": "0.00",
  "fx_rate": "56.5000000000",
  "base_debit_amount": "56500.00",
  "base_credit_amount": "0.00"
}
```

Recommended JSON representation for a same-currency line:

```json
{
  "id": "line-201",
  "line_number": 2,
  "account_id": "acct-1000",
  "transaction_currency": "PHP",
  "debit_amount": "0.00",
  "credit_amount": "56500.00",
  "fx_rate": "1.0000000000",
  "base_debit_amount": "0.00",
  "base_credit_amount": "56500.00"
}
```

## Conformance expectations

An implementation may claim `OBLE Multi-Currency` if:

- it distinguishes transaction currency from base currency
- it represents FX rate semantics explicitly
- it derives or stores exact base amounts deterministically
- posted entries remain base-balanced

## Mapping note for Heft

Heft is a strong source for this draft because it already models:

- transaction amounts
- base amounts
- explicit FX rates
- exact integer arithmetic
- revaluation as a separate accounting flow

That makes Heft especially useful for clarifying what should be protocol
semantics versus implementation detail in OBLE's multi-currency story.
