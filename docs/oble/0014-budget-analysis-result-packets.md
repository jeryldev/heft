# OBLE-0014 Budget Analysis Result Packets

Status: Draft

## Purpose

This draft defines export-first OBLE packets for derived planning analysis,
starting with budget-versus-actual outputs.

The goal is to exchange planning analysis without confusing it with either:

- planning source state from `OBLE-0011`
- posted ledger history from the core

## Covered result families

The first useful family is:

- `budget_vs_actual`

Later drafts may define richer planning outputs such as:

- scenario comparisons
- multi-version variance views
- departmental or dimensional planning overlays

## Core fields

Suggested top-level fields:

- `packet_kind`
- `book_id`
- `budget_id`
- `start_date`
- `end_date`
- `decimal_places`
- `rows`

## Row fields

Suggested row fields:

- `account_id`
- `account_number`
- `account_name`
- `budget`
- `actual_debit`
- `actual_credit`
- `actual_net`
- `variance`

Amounts should be rendered as canonical decimal strings at the book's declared
precision.

## Invariants

The packet should preserve:

- the planning artifact identity (`budget_id`)
- the reporting boundary
- the account identity used for each comparison row
- the distinction between gross actual debit/credit values and derived net
  actual behavior
- the derived variance

## Import posture

This draft is export-first.

Budget analysis packets are derived views. They are useful for:

- review
- reporting interchange
- sandbox and browser tooling
- comparisons between planning systems

They should not be treated as source-of-truth planning state.

## Relationship to Heft

Heft already supports:

- budgets
- budget lines
- budget lifecycle state
- budget-versus-actual reporting

So Heft is a natural candidate reference implementation for this packet family.
