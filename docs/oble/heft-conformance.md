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
| Classifications and Report Structures | Partial | Heft now has an initial Zig-first classification profile bundle for metadata and ordered nodes, but broader packet coverage, public-surface exposure, and canonical OBLE examples/schemas are still missing. |
| Dimensions and Analytics | Partial | Heft now has an initial Zig-first dimension profile bundle for definitions, values, and line assignments, but broader packet coverage, public-surface exposure, and canonical OBLE examples/schemas are still missing. |
| Budgets and Planning | Draft-dependent | Heft has budget workflow semantics, but no stable OBLE packet/profile boundary yet. |
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

Status: `Partial`

Heft already has:

- classification trees
- group and account nodes
- classified report behavior
- cash-flow and classified trial-balance semantics

Heft now also has an initial Zig-first classification profile bundle boundary
for:

- classification metadata
- ordered nodes
- account-binding nodes

What is still missing:

- broader packet breadth
- public ABI exposure
- canonical OBLE examples and schemas for this profile
- a clearer decision on whether rendered classified reports should also become OBLE packets

## OBLE-0010 Dimensions and Analytics

Status: `Partial`

Heft already has:

- dimensions
- dimension values
- line assignments
- dimension summaries and rollups

Heft now also has an initial Zig-first dimension profile bundle boundary for:

- dimension definitions
- dimension values
- line-level assignments

What is still missing:

- broader packet breadth
- public ABI exposure
- canonical OBLE examples and schemas for this profile
- a clearer decision on whether summaries and rollups should become OBLE packet families

## OBLE-0011 Budgets and Planning

Status: `Draft-dependent`

Heft already has:

- budgets
- budget lines
- budget status transitions
- budget-vs-actual reporting

But this remains a Heft-native capability rather than an implemented OBLE packet/profile boundary.

## Biggest remaining gaps

The most important gaps are now about completeness, not first principles.

1. broaden the new classification and dimension boundaries into examples, schemas, and wider packet coverage
2. implement packet/profile boundaries for budgets
3. broaden packet coverage for close/reopen bundles and richer multi-currency examples
4. automated schema validation of exported payloads
5. broader public-surface exposure beyond the current Zig bridge and minimal C import session
6. fuller live-export validation against a bundled draft-2020-12 schema validator once one is adopted

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

1. broaden the classification exporter/importer into a fuller standards surface
2. implement dimensions and budgets as the next OBLE profiles
3. automate schema validation for canonical exports
4. turn profile claims into fuller executable conformance checks
5. decide how much more of the import surface should be frozen in the C ABI
