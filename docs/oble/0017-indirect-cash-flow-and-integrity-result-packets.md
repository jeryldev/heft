# OBLE-0017: Indirect Cash Flow and Integrity Result Packets

Status: Draft

## Purpose

This draft defines export-first result packets for:

- indirect cash flow summaries
- ledger integrity verification summaries

These packets are derived outputs. They are meant for inspection,
interoperability, conformance reporting, and browser tooling, not for replay
into another ledger as source-of-truth history.

## Indirect Cash Flow Result

The indirect cash flow result packet expresses:

- the classification used to derive the result
- the reporting date range
- net income
- operating, investing, and financing totals
- net cash change
- the classified adjustment rows used by the engine

Recommended packet kind:

- `cash_flow_indirect`

## Integrity Summary Result

The integrity summary result packet expresses:

- whether verification passed
- error count
- warning count
- how many entries, accounts, and periods were checked

Recommended packet kind:

- `integrity_summary`

## Boundary posture

These packets are export-first.

They describe derived outcomes from a ledger engine and should not be imported
as authoritative ledger state.
