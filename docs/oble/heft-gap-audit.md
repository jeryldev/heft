# Heft to OBLE Gap Audit

Status: Draft

## Purpose

This document audits the remaining gap between Heft's current feature surface
and the current OBLE draft set.

It is not a general code-quality review.

It is specifically meant to answer:

- which Heft capabilities already have real OBLE packet/profile coverage
- which capabilities are only partially represented
- which capabilities are still outside the OBLE boundary
- which capabilities should probably remain Heft-specific

## Status meanings

### Confirmed

The feature has a real OBLE-facing surface in Heft today and is backed by
export/import/conformance or reconstruction-aware interchange behavior.

### Partial

The feature clearly exists in Heft and has some OBLE shape, but coverage is
still asymmetric, reconstruction-oriented, or not yet broad enough to feel
finished.

### Missing

The feature exists in Heft but does not yet have a credible OBLE packet,
profile, or conformance boundary.

### Heft-only

The feature is valuable, but it is currently better treated as engine/runtime
behavior rather than something OBLE should necessarily standardize.

## Audit summary

| Heft area | Current OBLE status | Notes |
| --- | --- | --- |
| Core ledger model | Confirmed | `Book`, `Account`, `Period`, `Entry`, and `Line` have real OBLE export/import/bundle support. |
| Entry lifecycle | Confirmed | draft/post/void/reverse semantics are represented and tested. |
| Counterparties and open items | Confirmed | Counterparty/open-item packets and profile bundles exist and round-trip. |
| Policy and designations | Confirmed | Policy profile export/import is real for safe user-authored settings. |
| Multi-currency entry semantics | Confirmed | FX entry semantics export/import through the FX profile boundary. |
| Revaluation | Partial | Revaluation packets export cleanly, but public import remains reconstruction-oriented rather than direct replay. |
| Close and reopen | Partial | Close/reopen profile export exists, and Zig reconstruction helpers exist, but direct portable replay is intentionally conservative. |
| Book snapshot / grouped export | Confirmed | Snapshot-style grouped export exists and is one of the strongest current interchange surfaces. |
| Semantic verification | Confirmed | Heft can verify semantic equivalence across source and imported ledgers. |
| Reports and statements | Partial | Heft now has the first export-first OBLE result-packet layer for classified outputs, dimension summaries, and budget analysis, but broader report/result coverage is still incomplete. |
| Classifications | Confirmed | Heft now has a real OBLE classification profile boundary plus export-first classified result packets. |
| Dimensions | Confirmed | Heft now has a real OBLE dimension profile boundary plus export-first summary and rollup result packets. |
| Budgets | Confirmed | Heft now has a real OBLE budget profile boundary plus an export-first budget-analysis result packet. |
| Audit trail export | Partial | Audit/provenance exists strongly in Heft, but OBLE does not yet have a mature portable audit packet set here. |
| Verification / integrity checks | Partial | Heft exposes verification, but OBLE does not yet define a conformance-facing integrity-result packet. |
| Batch workflows | Heft-only | Batch post/void orchestration is useful operational behavior, but not obviously a standards concern today. |
| Query/report transport formats | Heft-only | CSV/JSON report formatting is a Heft boundary, not an OBLE interoperability layer. |
| Cache/materialization/runtime details | Heft-only | Cache tables, stale recomputation, ABI buffer mechanics, and SQLite details are outside OBLE scope. |

## Detailed audit

## 1. Core ledger semantics

Status: `Confirmed`

These are in strong shape already:

- book export/import
- account export/import
- period export/import
- entry export/import
- core bundle export/import
- book snapshot export/import
- semantic verification after interchange

This is the most mature OBLE area in Heft.

## 2. Lifecycle semantics

Status: `Confirmed`

Heft already has strong OBLE-aligned coverage for:

- draft versus posted state
- reverse semantics
- reversal-pair packet export/import
- void semantics
- period-aware posting constraints

The remaining gap is not the lifecycle model itself.
It is the broader representation of derived lifecycle state.

## 3. Counterparties, subledgers, and open items

Status: `Confirmed`

This is another strong area:

- counterparties export/import
- line-level counterparty linkage
- open-item packet export/import
- counterparty profile bundle export/import
- book snapshot inclusion

This profile is already credible enough to talk about as a real implementation.

## 4. Policy and designations

Status: `Confirmed`

Heft now exports and imports safe user-authored policy state such as:

- entity type
- fiscal-year start month
- approval requirement
- designation bindings

This is one of Heft's clearest contributions to the OBLE direction.

## 5. Multi-currency and revaluation

Status: `Partial`

What is strong today:

- foreign-currency line semantics
- explicit transaction currency
- explicit FX rate
- explicit base amounts
- FX profile bundle export
- Zig import of the safe user-authored foreign-currency entry layer

What is still partial:

- revaluation packets are exportable
- Zig import reports revaluation presence honestly
- reconstruction helpers exist
- but direct portable replay of revaluation remains intentionally conservative

This is a good profile, but still not a fully frozen interchange story.

## 6. Close and reopen

Status: `Partial`

What is already real:

- close/reopen profile export
- policy/lifecycle bundle export
- reconstruction helpers for imported periods
- semantic guidance around reconstruction instead of naive replay

What is still partial:

- close/opening-entry state is heavily lifecycle-derived
- direct import replay is intentionally conservative
- the public surface is strongest in Zig, not in the minimal C import session

This is one of the most important remaining “honest partial” areas.

## 7. Reports and statements

Status: `Partial`

Heft has strong report surfaces:

- trial balance
- income statement
- balance sheet
- ledger views
- comparative reports
- cash flow variants

The result layer is better now:

- classified result packets exist
- dimension summary result packets exist
- budget analysis result packets exist

But these surfaces are still currently:

- Heft report APIs
- Heft C ABI surfaces
- Heft-native comparative and statement outputs for many other report families

That means report semantics are no longer entirely outside OBLE, but the
portable result-packet layer is still incomplete.

What is still missing:

- trial balance / income statement / balance sheet result packets
- comparative result packets
- cash-flow indirect result packets
- a clearer standards posture for which report outputs should remain Heft-only
  versus becoming OBLE packets

## 8. Classifications / Report Structures

Status: `Confirmed`

Heft already has:

- classification trees
- account-node and group-node modeling
- classified reports
- cash flow statement generation
- classified trial balance support

OBLE now has an explicit draft profile for this area:

- `OBLE-0009 Classifications and Report Structures`

Heft now has an implemented boundary for this area:

- classification profile bundle export/import
- classified report result packet export
- classified trial-balance result packet export
- cash-flow statement result packet export

What is still missing:

- broader packet breadth beyond the first structure and result packets
- wider result coverage for indirect or comparative variants
- any future decision on whether even more classified report families belong in
  OBLE

This is now one of the stronger non-core OBLE areas in Heft.

## 9. Dimensions / Analytics

Status: `Confirmed`

Heft already has:

- dimension definitions
- dimension values
- line-dimension assignment
- dimension summaries
- rollups

OBLE now has an explicit draft profile for this area:

- `OBLE-0010 Dimensions and Analytics`

Heft now has an implemented boundary for this area:

- dimension profile bundle export/import
- dimension summary result packet export
- dimension rollup result packet export

What is still missing:

- broader packet breadth beyond the first profile and result packets
- any future analytics packets beyond summaries and rollups

This area is now materially aligned instead of merely aspirational.

## 10. Budgets / Planning

Status: `Confirmed`

Heft already has:

- budgets
- budget lines
- budget status transitions
- budget-vs-actual reporting

OBLE now has an explicit draft profile for this area:

- `OBLE-0011 Budgets and Planning`

Heft now also has an implemented OBLE result layer for:

- budget metadata
- budget lifecycle state
- budget lines
- budget-vs-actual result export

What is still missing:

- broader planning-result families beyond the first budget-analysis packet
- any decision on whether multi-version or scenario planning belongs in later
  OBLE drafts

This profile is now real, even if it is still early.

## 11. Audit and provenance

Status: `Partial`

Heft is strong here internally:

- audit log writes on mutations
- per-book hash chains
- audit export through native Heft surfaces

But OBLE does not yet have a mature portable audit profile that matches this
depth.

So provenance semantics exist, but the standards boundary is still incomplete.

## 12. Verification and conformance reporting

Status: `Partial`

Heft already has:

- engine-level verify routines
- semantic verification across imported ledgers
- OBLE conformance tests

What is still missing is a cleaner standards-facing packet or profile for
reporting integrity/conformance results in a portable way.

## 13. Batch workflows

Status: `Heft-only`

Batch post/void support is useful application behavior, but it is currently
better treated as an operational engine API than as an OBLE standard surface.

This may stay out of OBLE entirely unless a real multi-entry workflow protocol
becomes necessary.

## 14. Runtime, cache, query, and ABI mechanics

Status: `Heft-only`

These should remain outside OBLE:

- SQLite schema details
- cache tables and recomputation
- benchmark harnesses
- buffer management
- ABI return conventions
- Heft CSV/JSON transport quirks

These are important for the engine, but they are not protocol semantics.

## Priority gaps

The most important remaining gaps are:

1. reports and statement semantics
2. budgets/planning examples and schema coverage
3. portable audit/provenance shapes
4. broader standards coverage for classifications and dimensions
5. portable audit/conformance result shapes
6. stronger public posture for lifecycle-derived import replay

## Recommended next sequence

1. Broaden `Classifications / Report Structures` from the first bundle boundary into a fuller packet/example/schema set.
2. Broaden `Dimensions / Analytics` from the first bundle boundary into examples, schemas, and wider packet coverage.
3. Broaden `Budgets / Planning` from the first bundle boundary into examples,
   schemas, and any later comparison-oriented packet design.
4. Define a narrow OBLE audit/provenance profile only if there is real cross-engine need.
5. Keep `Close/Reopen` and `Revaluation` honest as reconstruction-oriented until replay semantics are truly stable.

## Practical conclusion

Heft is already strongly aligned with the current OBLE core and named profiles.

The remaining gap is no longer “does OBLE exist in Heft?”

The remaining gap is:

- how much more of Heft should become standardized
- which features deserve new OBLE profiles
- which features should remain valuable Heft-native behavior
