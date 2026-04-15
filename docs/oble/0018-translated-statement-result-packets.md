# OBLE-0018: Translated Statement Result Packets

Status: Draft

## Purpose

This draft defines export-first result packets for statement outputs that have
been translated into a target presentation currency.

These packets are intended for:

- presentation-currency reporting
- cross-system display
- conformance and demo tooling

They are not a substitute for base-currency ledger history.

## Packet family

Recommended packet kinds:

- `translated_trial_balance`
- `translated_income_statement`
- `translated_balance_sheet`

Each packet should include:

- `book_id`
- source statement boundary fields
- `source_currency`
- `target_currency`
- `closing_rate`
- `average_rate`
- translated totals
- translated rows

## Boundary posture

Translated statement packets are derived outputs.

They should be treated as export-first packets. Engines may reconstruct them
from base-currency results rather than importing them as primary accounting
state.
