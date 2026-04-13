# OBLE Glossary

Status: Draft

This glossary defines the working meanings of key OBLE terms.

It is intentionally short and practical. The goal is consistency across drafts,
not encyclopedic coverage.

## Account

A ledger bucket classified by account type and normal balance.

Examples:

- asset
- liability
- equity
- revenue
- expense

## Base amount

The economic amount of a line measured in the book's base currency.

## Base currency

The primary currency of a book. Canonical ledger totals are ultimately measured
in this currency.

## Book

The top-level accounting boundary that owns accounts, periods, and entries.

## Close

A lifecycle event that marks a period boundary and may create derived
accounting state such as closing entries or opening carry-forward.

## Conformance

A declared level of support for OBLE semantics or profiles.

Examples:

- `OBLE Core`
- `OBLE Core + Multi-Currency`
- `OBLE Close/Reopen Profile`

## Control account

A general-ledger account whose balance is expected to reconcile to a related
subledger domain.

Examples:

- accounts receivable
- accounts payable

## Counterparty

A named entity that can be referenced by ledger lines, open items, or subledger
workflows.

Examples:

- customer
- supplier
- employee
- investor

## Debit / Credit orientation

The explicit economic direction of a line.

One line must not carry two positive directions at once.

## Designation

A book-level policy binding that gives special semantic meaning to an account or
role.

Examples:

- retained earnings account
- income summary account
- suspense account

## Draft

An entry state that is not yet economically effective.

## Entry

The unit of journal intent and posting lifecycle. An entry contains one or more
lines.

## Exchange rate

The explicit conversion relationship between a transaction currency and the
book's base currency for a given line or valuation context.

## Lifecycle

The set of allowed state transitions and invariants governing accounting
entities such as entries and periods.

## Line

The atomic financial effect inside an entry.

## Multi-currency

The condition where transaction amounts may be denominated in a currency
different from the book's base currency.

## Normal balance

The expected balance direction of an account.

Allowed core values:

- debit
- credit

## OBLE

Open Bookkeeping Ledger Exchange.

An emerging open standard for representing and exchanging double-entry ledger
state and lifecycle semantics.

## Open item

An unsettled amount tied to a source line and counterparty.

## Period

A reporting and lifecycle boundary for a book.

## Policy profile

A declared set of policy-bearing accounting behaviors or designation
requirements attached to a book or implementation.

## Posted

An entry state that is economically effective and contributes to ledger totals.

## Reopen

An explicit lifecycle event that reactivates a previously closed period and may
invalidate stale derived close state.

## Reverse

To offset a posted entry using a separate reversing entry while keeping the
original audit trail visible.

## Serialization profile

A defined way to encode OBLE entities for exchange, such as the current draft
JSON profile.

## Subledger

A structured accounting domain linked to counterparties and a control account.

## Suspense

A policy-bearing account or balance domain used to hold unresolved accounting
differences until they are properly classified.

## Transaction currency

The currency in which a line was originally denominated.

## Void

To cancel a posted entry in place while preserving its identity and audit
history.
