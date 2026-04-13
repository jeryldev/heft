# OBLE Conformance Checklist

Status: Draft

This checklist is a practical companion to the OBLE draft set.

It is intentionally lightweight. The goal is to help an implementation state
what it supports before a formal fixture-driven conformance suite exists.

## Core

An implementation can claim `OBLE Core` if all of the following are true:

- It models `Book`, `Account`, `Period`, `Entry`, and `Line`.
- A posted entry belongs to exactly one book and one period.
- Each line references an account in the same book as the entry.
- Posted entries require at least two lines.
- Posted entries balance exactly.
- The implementation distinguishes `draft` from `posted`.
- Amounts are exact, not floating-point-only.
- The implementation can serialize the core model to the OBLE JSON profile.

## Period-Aware

An implementation can additionally claim `Period-Aware` if:

- Period state is explicit.
- Period state influences posting behavior.
- The implementation has a notion of close or equivalent period boundary.

## Reversible

An implementation can additionally claim `Reversible` if:

- It supports explicit reversal behavior.
- The original entry and reversal relationship is recoverable.
- Reverse semantics preserve ledger consistency.

## Counterparty/Subledger

An implementation can additionally claim `Counterparty/Subledger` if:

- It supports generic counterparties or an equivalent entity.
- It supports line-level counterparty linkage.
- It supports subledger grouping or equivalent control-account semantics.
- It supports open-item or unsettled-balance tracking.

## Multi-Currency

An implementation can additionally claim `Multi-Currency` if:

- It distinguishes transaction currency from base currency.
- It stores or derives exact base amounts deterministically.
- It exposes explicit FX-rate semantics.

## Current Heft assessment

Based on the current engine design, Heft appears to target at least:

- `OBLE Core`
- `Period-Aware`
- `Reversible`
- `Counterparty/Subledger`
- `Multi-Currency`

That is an implementation assessment, not yet a formal certification result.

Formal conformance should eventually be based on published fixtures and test
cases, not just a checklist declaration.
