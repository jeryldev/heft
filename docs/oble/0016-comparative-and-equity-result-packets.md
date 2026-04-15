# OBLE-0016 Comparative and Equity Result Packets

Status: Draft

## Purpose

This draft defines export-first OBLE packets for comparative statement outputs
and equity-change views.

These packets represent derived presentation-layer meaning across two reporting
boundaries or across a period of equity movement.

## Covered packet kinds

Suggested `packet_kind` values:

- `trial_balance_comparative`
- `trial_balance_movement_comparative`
- `income_statement_comparative`
- `balance_sheet_comparative`
- `equity_changes`

Implementations may support a subset first.

## Comparative packet fields

Suggested comparative top-level fields:

- `packet_kind`
- `book_id`
- current and prior reporting boundary fields
- `decimal_places`
- `current_total_debits`
- `current_total_credits`
- `prior_total_debits`
- `prior_total_credits`
- `rows`

Suggested row fields:

- `account_id`
- `account_number`
- `account_name`
- `account_type`
- `current_debit`
- `current_credit`
- `prior_debit`
- `prior_credit`
- `variance_debit`
- `variance_credit`

## Equity-change packet fields

Suggested equity top-level fields:

- `packet_kind`
- `book_id`
- `start_date`
- `end_date`
- `fy_start_date`
- `decimal_places`
- `net_income`
- `total_opening`
- `total_closing`
- `rows`

Suggested row fields:

- `account_id`
- `account_number`
- `account_name`
- `opening_balance`
- `period_activity`
- `closing_balance`

## Import posture

These packets are intentionally export-first.

They are useful for:

- interoperability
- review
- period-over-period comparisons
- browser sandboxes
- regression testing

They should not be treated as authoritative source state for replay.

## Relationship to Heft

Heft already produces:

- comparative trial balances
- comparative income statements
- comparative balance sheets
- comparative movement statements
- equity changes outputs

So Heft is a natural reference implementation candidate for this packet family
too.
