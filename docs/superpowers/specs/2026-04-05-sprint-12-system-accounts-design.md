# Sprint 12: Book Setup & System Account Infrastructure

**Date:** 2026-04-05
**Branch:** heft-sprint-12 (from heft-sprint-11)
**Lifecycle Phase:** Phase 0 — Book Setup (before first transaction)
**Reference:** .research/11-accounting-lifecycle-checklist.md, Phase 0

---

## Goal

Enable full book configuration with system account designation slots so that every downstream accounting procedure (period close, FX revaluation, opening balance migration) has its prerequisites available. Fix C ABI parameter gaps that block daily operations.

After this sprint, an accountant can:
1. Create a book with a complete chart of accounts
2. Designate all system accounts (retained earnings, income summary, FX gain/loss, opening balance equity, suspense)
3. Migrate opening balances from a prior system
4. Use the full C ABI for daily operations (line descriptions, draft metadata, cross-period reversals)

---

## Architecture

No new files. All changes are additions to existing modules following established patterns:
- book.zig: 5 new set*Account functions (following setRoundingAccount pattern exactly)
- schema.zig: 5 new nullable columns on ledger_books, schema version 3
- error.zig: 5 new compliance gate errors
- main.zig: 5 new C ABI exports for designation + fixes to existing exports
- entry.zig: no changes (closing entries are Sprint 14)

The designation model: application creates accounts of any number/name, then tells the engine which account serves which role. The engine validates the account type is appropriate and stores the designation. Features that need a designation check at point-of-use and return a clear error if not set.

---

## 12A: System Account Designation Slots

### Schema Change (schema.zig)

Add 5 new nullable INTEGER columns to ledger_books, after rounding_account_id:

```sql
fx_gain_loss_account_id INTEGER,
retained_earnings_account_id INTEGER,
income_summary_account_id INTEGER,
opening_balance_account_id INTEGER,
suspense_account_id INTEGER
```

No FK REFERENCES on any of these (same pattern as rounding_account_id — Zig validates at designation time, avoids forward-reference DDL issues).

Increment PRAGMA user_version from 2 to 3.
Update SCHEMA_VERSION in main.zig from 2 to 3.
Update schema test "creates N indexes" and "sets user_version" assertions.

### Error Variants (error.zig)

Add 4 new compliance gate errors:

```
RetainedEarningsAccountRequired
FxGainLossAccountRequired
OpeningBalanceAccountRequired
IncomeSummaryAccountRequired
```

Note: IncomeSummaryAccountRequired is included for completeness but no current feature requires it — it will only be used if the application explicitly requests two-step close and hasn't designated the account. Direct close works without it.

### Type Validation Rules

Each system account role requires specific account types. The set*Account function validates:

| Role | Allowed account_type | Allowed is_contra | Rationale |
|------|---------------------|-------------------|-----------|
| rounding | any | any | FX rounding can be expense or revenue |
| fx_gain_loss | revenue or expense | any | Gains are revenue, losses are expense; single account handles both via debit/credit direction |
| retained_earnings | equity | false only | RE is credit-normal equity, never contra |
| income_summary | equity | false only | Temporary equity account for two-step close |
| opening_balance | equity | false only | Opening balance equity is credit-normal |
| suspense | asset or liability | any | Catch-all for unresolved items |

### Designation Functions (book.zig)

5 new public functions, each following the exact setRoundingAccount pattern:

**Book.setFxGainLossAccount(database, book_id, account_id, performed_by) !void**
- Validates: book exists, not archived (error.BookArchived)
- Validates: account exists, is active, belongs to same book (error.AccountInactive, error.CrossBookViolation)
- Validates: account_type is 'revenue' or 'expense' (error.InvalidInput)
- Executes: UPDATE ledger_books SET fx_gain_loss_account_id = ? WHERE id = ?
- Audit: action="update", field="fx_gain_loss_account_id", old=null, new=account_id

**Book.setRetainedEarningsAccount(database, book_id, account_id, performed_by) !void**
- Same validation pattern
- Validates: account_type is 'equity' AND is_contra = 0 (error.InvalidInput)
- Audit: field="retained_earnings_account_id"

**Book.setIncomeSummaryAccount(database, book_id, account_id, performed_by) !void**
- Same validation pattern
- Validates: account_type is 'equity' AND is_contra = 0 (error.InvalidInput)
- Audit: field="income_summary_account_id"

**Book.setOpeningBalanceAccount(database, book_id, account_id, performed_by) !void**
- Same validation pattern
- Validates: account_type is 'equity' AND is_contra = 0 (error.InvalidInput)
- Audit: field="opening_balance_account_id"

**Book.setSuspenseAccount(database, book_id, account_id, performed_by) !void**
- Same validation pattern
- Validates: account_type is 'asset' or 'liability' (error.InvalidInput)
- Audit: field="suspense_account_id"

### C ABI Exports (main.zig)

5 new exports following the ledger_set_rounding_account pattern:

```
ledger_set_fx_gain_loss_account(handle, book_id, account_id, performed_by) bool
ledger_set_retained_earnings_account(handle, book_id, account_id, performed_by) bool
ledger_set_income_summary_account(handle, book_id, account_id, performed_by) bool
ledger_set_opening_balance_account(handle, book_id, account_id, performed_by) bool
ledger_set_suspense_account(handle, book_id, account_id, performed_by) bool
```

Add error code mappings in mapError:
```
error.RetainedEarningsAccountRequired => 26,
error.FxGainLossAccountRequired => 27,
error.OpeningBalanceAccountRequired => 28,
error.IncomeSummaryAccountRequired => 29,
```

Update error code comment block.

### Query Updates (query.zig)

Update getBook to include the 5 new columns in output:
- CSV: add fx_gain_loss_account_id, retained_earnings_account_id, income_summary_account_id, opening_balance_account_id, suspense_account_id
- JSON: same fields

### Tests

For EACH of the 5 new set*Account functions:
1. Happy path: create book, create account of correct type, designate, verify via getBook
2. Wrong account type: try to set retained_earnings to an asset account, expect error.InvalidInput
3. Contra rejected (where applicable): try equity contra for retained_earnings, expect error.InvalidInput
4. Archived book: archive book, try designation, expect error.BookArchived
5. Inactive account: deactivate account, try designation, expect error.AccountInactive
6. Cross-book: create account in different book, try designation, expect error.CrossBookViolation
7. Nonexistent book: expect error.NotFound
8. Nonexistent account: expect error.NotFound
9. Audit entry verified: check audit_log for the designation record

C ABI tests:
10. Null handle returns false
11. Happy path through C ABI
12. Error code set correctly on failure

---

## 12B: C ABI Completeness

### Expose addLine description parameter

Current: ledger_add_line passes description=null to Entry.addLine
Fix: Add description parameter to C ABI export

```
pub export fn ledger_add_line(
    handle: ?*LedgerDB,
    entry_id: i64,
    line_number: i32,
    debit_amount: i64,
    credit_amount: i64,
    transaction_currency: [*:0]const u8,
    fx_rate: i64,
    account_id: i64,
    counterparty_id: i64,
    description: ?[*:0]const u8,  // NEW — was not exposed
    performed_by: [*:0]const u8,
) i64
```

This is a C ABI BREAKING CHANGE. Since Heft is v0.0.1 and not yet released, this is acceptable. Document in changelog.

### Expose createDraft description and metadata parameters

Current: ledger_create_draft passes description=null, metadata=null
Fix: Add both parameters

```
pub export fn ledger_create_draft(
    handle: ?*LedgerDB,
    book_id: i64,
    document_number: [*:0]const u8,
    transaction_date: [*:0]const u8,
    posting_date: [*:0]const u8,
    description: ?[*:0]const u8,   // NEW
    period_id: i64,
    metadata: ?[*:0]const u8,      // NEW
    performed_by: [*:0]const u8,
) i64
```

### Expose reverse target_period_id parameter

Current: ledger_reverse_entry passes target_period_id=null (always reverses in original period)
Fix: Add target_period_id parameter (0 = same period as original, >0 = specific period)

```
pub export fn ledger_reverse_entry(
    handle: ?*LedgerDB,
    entry_id: i64,
    reason: [*:0]const u8,
    reversal_date: [*:0]const u8,
    target_period_id: i64,  // NEW — 0 = use original period
    performed_by: [*:0]const u8,
) i64
```

### Tests

1. addLine with description: create draft, add line with description, verify via listEntryLines
2. addLine with null description: backward compatible behavior
3. createDraft with description and metadata: create, verify via getEntry
4. createDraft with null description/metadata: backward compatible
5. reverse with target_period_id: reverse into a different period, verify reversal entry's period
6. reverse with target_period_id=0: same as current behavior (original period)

### Update existing tests

All existing tests that call ledger_create_draft and ledger_add_line must be updated with the new parameter (pass null for description/metadata to preserve existing behavior).

---

## 12C: Opening Balance Migration

### Function (book.zig or entry.zig — TBD)

This is a convenience function, not a new entity. It creates a regular journal entry with a specific pattern.

**Book.validateOpeningBalanceMigration(database, book_id) !void**
- Compliance gate: opening_balance_account_id MUST be designated (error.OpeningBalanceAccountRequired)
- Validates: book exists, not archived

The actual migration is done by the application using existing entry functions:
1. Application calls validateOpeningBalanceMigration to check prerequisites
2. Application creates a draft entry with document_number like "OB-001"
3. Application adds lines for each account's opening balance
4. Application adds an offset line to the opening_balance_account_id
5. Application posts the entry

The engine verifies the balance equation at post time (debits = credits). The offset to Opening Balance Equity ensures this.

This is the simplest correct approach — no new entry type, no new posting logic. The application composes existing primitives. Rule 16.

### Tests

1. validateOpeningBalanceMigration without designation: expect error.OpeningBalanceAccountRequired
2. validateOpeningBalanceMigration with designation: succeeds
3. Full migration flow: designate OB account, create entry with balances, post, verify all account balances via trial balance

---

## Compliance Gate Summary

After Sprint 12, the engine has these gates ready for future sprints:

| Gate Error | Checked By | Sprint |
|-----------|-----------|--------|
| error.RetainedEarningsAccountRequired | closePeriod() | S14 |
| error.FxGainLossAccountRequired | revalueForexBalances() | S15 |
| error.OpeningBalanceAccountRequired | validateOpeningBalanceMigration() | S12 (this sprint) |
| error.IncomeSummaryAccountRequired | closePeriod() with two-step | S14 (only if app requests two-step and IS not set) |

The errors are defined now (Sprint 12) so they're available when the features land in later sprints.

---

## What This Sprint Does NOT Include

- Closing entries (Sprint 14 — needs reports infrastructure from Sprint 13)
- FX revaluation (Sprint 15 — needs closing infrastructure from Sprint 14)
- Stale cache recalculation (Sprint 13 — separate concern)
- Comparative reports (Sprint 13 — separate concern)
- Dimensions/tagging (Sprint 17 — independent workstream)
- Budget entity (Sprint 18 — independent workstream)

---

## Success Criteria

1. All 5 system account designation functions work correctly with full validation
2. All 5 compliance gate errors are defined and tested
3. C ABI exposes addLine description, createDraft description/metadata, reverse target_period_id
4. Opening balance migration flow works end-to-end
5. All existing 783 tests still pass
6. New tests bring total to ~840+
7. `zig build test` passes, `zig fmt --check` passes, `zig build` produces static+shared libs
8. Schema version incremented to 3
