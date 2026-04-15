# OBLE-0013 Dimension Summary Result Packets

Status: Draft

## Purpose

This draft defines export-first OBLE packets for derived analytical outputs
based on dimensions and dimension values.

The goal is to exchange analytical summaries without collapsing them into core
ledger state.

## Covered result families

The first useful result families are:

- flat dimension summaries
- roll-up dimension summaries

## Core fields

Suggested top-level fields:

- `packet_kind`
- `book_id`
- `dimension_id`
- `dimension_type`
- `start_date`
- `end_date`
- `rollup`
- `decimal_places`
- `rows`

Suggested `packet_kind` values:

- `dimension_summary`
- `dimension_summary_rollup`

## Row fields

Suggested row fields:

- `dimension_value_id`
- `parent_value_id`
- `code`
- `label`
- `total_debits`
- `total_credits`
- `net`

The flat form may omit `parent_value_id` or set it to `null`.

The roll-up form should preserve parent-child meaning when the source
implementation has hierarchical dimension values.

## Invariants

The packet should preserve:

- the dimension and reporting boundary used to derive the result
- the value identity used for each row
- whether the result is rolled up
- canonical decimal rendering for numeric values

## Import posture

This draft is export-first.

These packets are analytical outputs, not source-of-truth ledger state.

They are best used for:

- reporting interchange
- browser and dashboard views
- conformance checks
- comparisons across implementations

## Relationship to Heft

Heft already supports:

- dimensions
- dimension values
- line-level assignments
- flat summaries
- hierarchical rollups

So Heft is a strong candidate reference implementation for this result layer.
