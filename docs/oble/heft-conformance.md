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
| Serialization and Conformance | Confirmed | Heft now exports canonical OBLE JSON for the implemented core and extension packets, imports those packets back into live ledgers, and runs lightweight draft-bundle validation in the repo today. |
| Heft Mapping | Confirmed | The mapping is documented explicitly in the OBLE docs. |
| Multi-Currency Semantics | Confirmed | Heft stores transaction amounts, base amounts, and FX rates with exact integer arithmetic and revaluation flows. |
| Close and Reopen Profile | Confirmed | Heft implements close-generated state, opening carry-forward, reopen cascades, and stale derived-state invalidation. |
| Designations and Policy Profiles | Confirmed | Heft already uses designation-driven book policy heavily. |
| Classifications and Report Structures | Confirmed | Heft now has a classification profile bundle with public Zig and C export surfaces plus session-oriented Zig import and logical-ID resolution, and it now exports classified, statement, comparative-statement, and equity result packets as distinct derived-output layers. |
| Dimensions and Analytics | Confirmed | Heft now has a dimension profile bundle with public Zig and C export surfaces plus session-oriented Zig import and logical-ID resolution, and it now exports summary and rollup result packets as a distinct derived-output layer. |
| Budgets and Planning | Confirmed | Heft now has a budget profile bundle with public Zig and C export surfaces plus session-oriented Zig import and logical-ID resolution, and it now exports budget-analysis result packets as a distinct derived-output layer. |
| Indirect Cash Flow and Integrity Result Packets | Confirmed | Heft now exports indirect cash-flow and integrity-summary result packets as explicit derived-output layers with public Zig and C surfaces plus conformance coverage. |
| Translated Statement Result Packets | Confirmed | Heft now exports translated trial balance, translated income statement, and translated balance sheet packets as explicit derived-output layers with public Zig and C surfaces plus conformance coverage. |
| Audit Trail and Provenance Result Packets | Confirmed | Heft now exports audit-trail result packets with immutable records and hash-chain visibility as an explicit derived-output layer with public Zig and C surfaces plus conformance coverage. |
| Example payload validation | Confirmed | The published OBLE examples map to draft schemas, and Heft's implemented packet shapes follow the same canonical JSON conventions. |
| Fixture-driven OBLE conformance | Confirmed | Heft now has dedicated conformance tests plus executable round-trip tests for the implemented OBLE packets and profiles. |
| Canonical `Heft -> OBLE` export | Confirmed | Heft exports canonical OBLE JSON for `Book`, `Account[]`, `Period[]`, `Entry`, `BookSnapshot`, `Counterparty[]`, `ReversalPair`, `CounterpartyOpenItem`, `PolicyProfile`, `CloseReopenProfile`, and `RevaluationPacket`. |
| Canonical `OBLE -> Heft` import | Partial | Heft imports the implemented core, book snapshot, reversal, counterparty/open-item, and policy-profile packets and round-trips the safe user-authored layers successfully. Importer support is strongest in Zig, and Heft now also exposes a minimal C ABI import-session boundary for the stable packet set with explicit logical-ID mapping and packet-order semantics. Zig import sessions can additionally import the safe user-authored portions of FX and policy/lifecycle bundles while reporting the presence of derived packets that still require engine reconstruction. Not every lifecycle-derived packet is imported directly. |
| Semantic equivalence verification | Confirmed | Heft now includes a dedicated semantic-verification layer that compares canonical exported semantics and trial-balance meaning across source and target ledgers after OBLE interchange. |

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

Status: `Confirmed`

Heft now implements the first real OBLE serialization boundary in code:

- canonical JSON export from live Heft objects
- canonical JSON import for the implemented packet set
- a dedicated OBLE conformance test suite for the implemented profiles
- round-trip tests for core and extension packets
- schema/example validation guidance and machine-readable example mapping
- bundle-level exchange via `book_snapshot`

What is still missing is breadth, not the existence of the boundary:

- wider profile coverage beyond the currently implemented packets
- full draft-2020-12 schema validation of live exports in CI
- broader import coverage for lifecycle-derived profile packets where replaying the packet directly is safe and semantically honest
- fuller ABI-level failure-path coverage as more import packets are exposed

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

Heft now also imports and round-trips the exported policy-profile packet for the
safe user-authored configuration layer:

- entity type
- fiscal-year start month
- approval requirement
- designation bindings

## OBLE-0009 Classifications and Report Structures

Status: `Confirmed`

Heft already has:

- classification trees
- group and account nodes
- classified report behavior
- cash-flow and classified trial-balance semantics

Heft now has:

- a classification profile bundle
- public Zig export
- public C export
- session-oriented Zig import
- logical-ID resolution through the Zig import session

What is still missing:

- broader canonical examples and schemas beyond the first structure and result packets
- wider public import posture beyond Zig-first workflows
- any later classified-report packet families if OBLE wants to standardize them

## OBLE-0010 Dimensions and Analytics

Status: `Confirmed`

Heft already has:

- dimensions
- dimension values
- line assignments
- dimension summaries and rollups

Heft now has:

- a dimension profile bundle
- public Zig export
- public C export
- session-oriented Zig import
- logical-ID resolution through the Zig import session

What is still missing:

- broader canonical examples and schemas beyond the first profile and result packets
- wider public import posture beyond Zig-first workflows
- any later summary/rollup packet families if OBLE wants to standardize them

## OBLE-0011 Budgets and Planning

Status: `Confirmed`

Heft already has:

- budgets
- budget lines
- budget status transitions
- budget-vs-actual reporting

Heft now has:

- a budget profile bundle
- public Zig export
- public C export
- session-oriented Zig import
- logical-ID resolution through the Zig import session

What is still missing:

- broader canonical examples and schemas beyond the first profile and result packets
- wider public import posture beyond Zig-first workflows
- a decision on whether budget-vs-actual belongs in this profile or a later
  reporting-oriented extension, now that the first result packet exists

## OBLE-0017 Indirect Cash Flow and Integrity Result Packets

Status: `Confirmed`

Heft now has:

- indirect cash-flow result packet export
- integrity-summary result packet export
- public Zig and C export surfaces
- direct packet tests
- dedicated conformance coverage

These packets are intentionally export-first derived outputs rather than a new
import-heavy lifecycle boundary.

## OBLE-0018 Translated Statement Result Packets

Status: `Confirmed`

Heft now has:

- translated trial balance result packet export
- translated income statement result packet export
- translated balance sheet result packet export
- public Zig and C export surfaces
- direct packet tests
- dedicated conformance coverage

These packets give the multi-currency/reporting layer a real portable
presentation-currency output boundary.

## OBLE-0019 Audit Trail and Provenance Result Packets

Status: `Confirmed`

Heft now has:

- audit-trail result packet export
- immutable audit record exposure
- hash-chain visibility in the portable packet
- public Zig and C export surfaces
- direct packet tests
- dedicated conformance coverage

This packet is intentionally export-first. It exposes the portable provenance
surface without pretending every native engine audit mechanism is identical.

## Biggest remaining gaps

The most important gaps are now about completeness, not first principles.

1. broaden packet coverage for close/reopen bundles and richer multi-currency examples
2. decide whether richer audit-log or provenance variants should extend beyond the first audit-trail packet
3. automated schema validation of exported payloads
4. broader public-surface exposure beyond the current Zig bridge and minimal C import session
5. fuller live-export validation against a bundled draft-2020-12 schema validator once one is adopted

## Current practical claim

The strongest honest claim today is:

Heft currently satisfies:

- `OBLE Core`
- `Period-Aware`
- `Reversible`
- `Counterparty/Subledger`
- `Multi-Currency`
- `Close/Reopen Profile`
- `Designations and Policy Profiles`

For the implemented packets, those claims are now backed by OBLE-native export,
import, and round-trip tests. What remains is broadening that proof to more of
the draft set and more integration boundaries.

## Immediate next steps

1. broaden the exporter/importer packet set to more OBLE profiles
2. automate schema validation for canonical exports
3. turn profile claims into fuller executable conformance checks
4. decide how much more of the import surface should be frozen in the C ABI
