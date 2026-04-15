# OBLE-0015 Core Statement Result Packets

Status: Draft

## Purpose

This draft defines export-first OBLE packets for core financial statement
results that are derived from ledger state but are not themselves source
ledger facts.

These packets let implementations exchange rendered statement meaning
portably without confusing that meaning with posted-entry history.

## Covered statement families

Suggested `packet_kind` values:

- `trial_balance`
- `trial_balance_movement`
- `income_statement`
- `balance_sheet`

Implementations may support a subset first.

## Core fields

Suggested top-level fields:

- `packet_kind`
- `book_id`
- reporting boundary fields such as `as_of_date` or `start_date` / `end_date`
- `decimal_places`
- `total_debits`
- `total_credits`
- `rows`

`balance_sheet` packets may also include an implementation note such as
`projected_retained_earnings` when the exported view includes a synthetic
presentation-layer retained-earnings row.

## Row fields

Suggested row fields:

- `account_id`
- `account_number`
- `account_name`
- `account_type`
- `debit`
- `credit`

Amounts should be rendered as canonical decimal strings at the book's declared
precision.

## Invariants

The packet should preserve:

- the reporting boundary used to derive the statement
- row ordering
- account identity
- debit/credit orientation as rendered by the statement
- explicit totals at the same rendered precision

## Import posture

These packets are intentionally export-first.

They are useful for:

- interoperability
- review
- external rendering
- regression testing
- browser sandboxes

They should not be treated as authoritative source state for replay.

## Relationship to Heft

Heft already produces:

- trial balance outputs
- trial balance movement outputs
- income statements
- balance sheets

So Heft is a natural reference implementation candidate for the first result
packet shapes in this area.
