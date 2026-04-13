# Accounting Lifecycle

This document describes the normal accounting lifecycle in Heft.

## 1. Book setup

Create a book first. A book defines:

- base currency
- decimal places
- fiscal policy and lifecycle designations

After book creation, create:

- chart of accounts
- periods
- optional subledger groups/accounts
- optional classifications
- optional dimensions
- optional budgets

Then assign the designations needed by your workflow.

## 2. Draft entry lifecycle

Entries begin as drafts.

While draft:

- lines can be added
- lines can be edited
- lines can be removed
- draft header fields can be edited

Drafts are where applications assemble accounting intent before the engine
accepts it into the ledger.

## 3. Post

Posting is the core lifecycle boundary.

At post time, Heft validates:

- entry state
- period state
- approval requirements
- debit/credit balance
- counterparty/control-account rules
- account activity and book ownership
- FX/base amount logic

When valid, posting:

- computes or validates base amounts
- updates per-period balance cache
- writes audit log entries
- changes entry status from draft to posted

## 4. Read and report

After posting, data becomes available to:

- financial statement reports
- ledger and journal reports
- subledger views
- aging and reconciliation
- budget comparison
- exports and buffer queries

## 5. Void and reverse

Posted entries are not edited like drafts.

Instead, Heft supports:

- void
- reverse

These are controlled lifecycle actions with period and invariant checks.

Heft also protects related workflows such as:

- open-item integrity
- reversal-period rules
- cache and future-period stale handling

## 6. Period states

Periods are not just date buckets. They are lifecycle states.

Supported states include:

- open
- soft_closed
- closed
- locked

Operationally:

- open means normal posting allowed
- soft-closed means the period is operationally closed but still recoverable
- closed means regular posting is disallowed
- locked means immutable, including lifecycle mutation that would change status

## 7. Close

Close behavior depends on book policy.

Possible close models:

- direct close to equity close target
- income-summary close
- allocation-based close for partnership/LLC style entities

Close also performs or coordinates:

- stale-cache refresh
- period balance checks
- suspense enforcement
- year-end dividend/drawing sweep when applicable
- opening-entry generation for the next period
- period-state transitions

## 8. Opening carry-forward

Heft carries forward balance-sheet balances using a real opening journal entry.

This matters because Heft treats accounting lifecycle actions as journalized,
auditable events rather than hidden internal state mutations.

The opening entry is:

- posted in the next period
- marked as an opening entry
- handled specially by reporting/cache logic

## 9. Reopen cascade

Reopening a period is not only a status toggle.

If closing/opening entries were generated from that period state, reopening has
to invalidate those derived lifecycle artifacts.

That is why Heft uses cascade reopen behavior:

- stale closing entries are voided
- the next period’s opening entry can be voided
- later closed periods may block reopen until handled in order

## 10. Revaluation

Foreign-currency revaluation is treated as a proper lifecycle operation.

It depends on:

- monetary account designation/behavior
- FX gain/loss designation
- period and posting rules

Revaluation creates real journal entries rather than hidden computed values.

## 11. Open items and subledger lifecycle

AR/AP workflows are layered on top of journal activity through:

- subledger groups
- subledger accounts
- open items
- payment allocation

This means the engine can support:

- customer and supplier ledgers
- aging
- reconciliation
- open-item aware posting/void/reversal rules

## 12. Verification

Verification is a lifecycle safety tool, not just a test helper.

It checks whether the stored accounting state remains consistent with:

- journal truth
- cache truth
- lifecycle invariants

That is especially important in an engine that intentionally uses derived cache
tables for performance.
