# OBLE-0012 Classified Result Packets

Status: Draft

## Purpose

This draft defines export-first OBLE packets for derived classification-based
report outputs.

The goal is to make derived report meaning portable without pretending that
rendered reports are core ledger state.

These packets are for:

- classified balance-sheet style outputs
- classified trial-balance outputs
- direct cash-flow classification outputs

## Why this is not the same as classification structure

`OBLE-0009` standardizes the reusable reporting structure:

- classifications
- nodes
- account bindings

This draft standardizes derived outputs produced from that structure at a
particular reporting boundary.

That means:

- structure is still the reusable profile layer
- report packets are derived exports
- import should remain conservative or unnecessary

## Packet kinds

Suggested `packet_kind` values:

- `classified_report`
- `classified_trial_balance`
- `cash_flow_statement`

Implementations may support a subset first.

## Core fields

Suggested top-level fields:

- `packet_kind`
- `classification_id`
- `book_id`
- `report_type`
- reporting boundary fields such as `as_of_date` or `start_date` / `end_date`
- `decimal_places`
- `total_debits`
- `total_credits`
- `unclassified_debits`
- `unclassified_credits`
- `rows`

## Row fields

Suggested row fields:

- `node_id`
- `node_type`
- `depth`
- `position`
- `label`
- `account_id`
- `debit`
- `credit`

`account_id` may be `null` for grouping rows.

Amounts should be rendered as canonical decimal strings at the book's declared
precision.

## Invariants

The packet should preserve:

- the classification identity used to derive the report
- the reporting boundary (date or date range)
- row ordering
- hierarchy depth
- whether a row is a grouping node or an account-binding node
- explicit unclassified balances when present

## Import posture

This draft is intentionally export-first.

These packets represent derived report meaning at a point in time. They are
useful for:

- interoperability
- review
- testing
- comparison
- browser playbooks and sandboxes

They should not be treated as authoritative source state for replay.

## Relationship to Heft

Heft already produces:

- classified reports
- classified trial balances
- cash-flow statement outputs

So Heft is a natural reference implementation candidate for the first result
packet shapes in this area.
