# Heft to OBLE Conformance

Status: Draft

## Purpose

This document records how Heft currently maps to the OBLE drafts at a
conformance level.

It is intentionally practical.

The goal is to answer:

- what Heft clearly satisfies today
- what Heft partially satisfies
- what depends on draft stabilization
- what is not yet implemented as a standards boundary

## Status meanings

### Confirmed

The semantics are present in Heft today and are backed by running code and test
coverage, even if the OBLE-facing export/import boundary is not finished yet.

### Partial

The underlying semantics mostly exist, but the OBLE boundary is incomplete or
the exact draft shape is not fully exposed yet.

### Draft-dependent

Heft likely aligns, but the OBLE draft area is still too unsettled to claim
strong conformance cleanly.

### Not yet implemented

The relevant OBLE-facing behavior does not yet exist in Heft as an explicit
standard boundary or testable conformance surface.

## Conformance summary

| OBLE area | Heft status | Notes |
| --- | --- | --- |
| OBLE Core | Confirmed | Heft has `Book`, `Account`, `Period`, `Entry`, and `Line` with exact balancing and book/period boundaries. |
| Lifecycle and Invariants | Confirmed | Draft/post/void/reverse states, period state enforcement, and audit-backed lifecycle semantics are present. |
| Counterparties and Subledgers | Confirmed | Heft supports subledger groups/accounts, line-level counterparty linkage, open items, allocation, aging, and reconciliation. |
| Serialization and Conformance | Partial | Draft schemas and examples exist, but Heft does not yet export canonical OBLE payloads directly. |
| Heft Mapping | Confirmed | The mapping is documented explicitly in the OBLE docs. |
| Multi-Currency Semantics | Confirmed | Heft stores transaction amounts, base amounts, and FX rates with exact integer arithmetic and revaluation flows. |
| Close and Reopen Profile | Confirmed | Heft implements close-generated state, opening carry-forward, reopen cascades, and stale derived-state invalidation. |
| Designations and Policy Profiles | Confirmed | Heft already uses designation-driven book policy heavily. |
| Example payload validation | Partial | The OBLE schemas validate draft payloads, but they are not yet produced directly from live Heft objects. |
| Fixture-driven OBLE conformance | Not yet implemented | There is no dedicated `Heft -> OBLE` fixture suite yet. |
| Canonical `Heft -> OBLE` export | Not yet implemented | This is the biggest missing standards boundary. |
| Canonical `OBLE -> Heft` import | Not yet implemented | Import is planned but not yet present. |

## Detail by draft area

## OBLE Core

Status: `Confirmed`

Heft clearly implements the current OBLE core model:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`

It also enforces the most important core invariants:

- entries belong to books and periods
- lines belong to entries
- lines reference accounts in the same book
- posted entries balance exactly
- exact arithmetic is used for accounting amounts

## OBLE-0002 Lifecycle and Invariants

Status: `Confirmed`

Heft strongly aligns with the lifecycle draft:

- draft and posted distinction
- explicit void semantics
- explicit reverse semantics
- period state enforcement
- audit-backed lifecycle mutation

This is one of the strongest areas of alignment.

## OBLE-0003 Counterparties and Subledgers

Status: `Confirmed`

Heft models this through:

- subledger groups
- subledger accounts
- line-level counterparty references
- open items
- allocation
- aged subledger and reconciliation surfaces

The terminology differs slightly from the generic OBLE language, but the
underlying semantics are clearly present.

## OBLE-0004 Serialization and Conformance

Status: `Partial`

Heft is not yet fully conformant here because the current OBLE serialization
layer mostly exists as:

- draft schemas
- draft examples
- validation docs

What is still missing:

- a canonical exporter from live Heft objects into OBLE payloads
- a standards-facing validation workflow over those exported payloads

## OBLE-0006 Multi-Currency

Status: `Confirmed`

Heft already supports the main semantics assumed by the current draft:

- explicit transaction currency
- explicit FX rate
- explicit base amounts
- exact integer arithmetic
- revaluation as a separate accounting flow

## OBLE-0007 Close and Reopen Profile

Status: `Confirmed`

Heft is particularly strong here:

- close is a real lifecycle event
- close can generate derived state
- opening balances can be materialized
- reopen invalidates stale derived close/opening artifacts
- close/reopen behavior is audit-aware

## OBLE-0008 Designations and Policy Profiles

Status: `Confirmed`

Heft is one of the clearest real implementations of this draft area.

Examples already present in Heft include:

- retained earnings designation
- income summary designation
- FX gain/loss designation
- suspense designation
- opening balance designation
- dividends/drawings designation

## Biggest remaining gaps

The most important gaps are not in ledger semantics.

They are in the standards boundary:

1. canonical OBLE export from Heft
2. canonical OBLE import into Heft
3. fixture-driven conformance tests
4. explicit profile claim validation

## Current practical claim

The strongest honest claim today is:

Heft appears to satisfy:

- `OBLE Core`
- `Period-Aware`
- `Reversible`
- `Counterparty/Subledger`
- `Multi-Currency`
- `Close/Reopen Profile`
- `Designations and Policy Profiles`

But those claims are not yet formally certified through OBLE-native export and
fixture-driven conformance tests.

## Immediate next steps

1. define the first canonical `Heft -> OBLE` export targets
2. export real Heft objects into OBLE JSON
3. validate those exports against the OBLE draft schemas
4. turn these prose claims into executable conformance checks
