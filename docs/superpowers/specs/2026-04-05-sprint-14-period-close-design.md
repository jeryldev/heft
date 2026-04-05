# Sprint 14: Period Close & Year-End

**Date:** 2026-04-05
**Branch:** heft-sprint-14 (from heft-sprint-13)
**Lifecycle Phase:** Phase 6 — Annual/Year-End Close
**Reference:** .research/11-accounting-lifecycle-checklist.md, Phase 6 (steps 6.4-6.8)
**Depends on:** Sprint 12 (system accounts), Sprint 13 (correct reports for verification)

---

## Goal

Implement the period closing cycle that zeroes out temporary accounts (revenue/expense) and transfers net income to retained earnings via real journal entries (Rule 1). Support both direct close (industry standard) and two-step close via Income Summary (France/China). After this sprint, an accountant can perform a full year-end close.

---

## Architecture

New file:
- close.zig: closePeriod function + helpers

Changes to existing files:
- root.zig: add close.zig to re-exports
- main.zig: C ABI export for closePeriod
- verify.zig: add check that closed periods have zero R/E balances

---

## 14A: closePeriod Function

### New File: close.zig

**`pub fn closePeriod(database, book_id, period_id, performed_by) !void`**

### Compliance Gates (checked first)

1. retained_earnings_account_id MUST be designated → error.RetainedEarningsAccountRequired
2. Period must be open or soft_closed → error.PeriodClosed or error.PeriodLocked
3. No draft entries in this period → error.InvalidInput (drafts must be posted or deleted first)

### Determine Close Method

```zig
const re_account_id = // query retained_earnings_account_id from book
const is_account_id = // query income_summary_account_id from book (nullable)
```

If is_account_id is null → direct close
If is_account_id is not null → two-step close via Income Summary

### Direct Close (when income_summary not designated)

Single compound journal entry:

1. Query all accounts with type 'revenue' or 'expense' that have non-zero base balance for this period:
   ```sql
   SELECT ab.account_id, a.account_type, a.normal_balance, ab.debit_sum, ab.credit_sum
   FROM ledger_account_balances ab
   JOIN ledger_accounts a ON a.id = ab.account_id
   WHERE ab.book_id = ? AND ab.period_id = ? AND a.account_type IN ('revenue', 'expense')
     AND (ab.debit_sum != 0 OR ab.credit_sum != 0)
   ```

2. Create draft entry:
   - document_number: "CLOSE-P{period_number}-FY{year}"
   - posting_date: period end_date
   - metadata: {"closing_entry": true, "method": "direct"}

3. For each revenue account (credit-normal) with net credit balance:
   - Line: Debit the revenue account (zeroing it), Credit Retained Earnings
   - Amount = credit_sum - debit_sum (the net balance)

4. For each revenue account with abnormal debit balance:
   - Line: Credit the revenue account, Debit Retained Earnings

5. For each expense account (debit-normal) with net debit balance:
   - Line: Credit the expense account (zeroing it), Debit Retained Earnings
   - Amount = debit_sum - credit_sum

6. For each expense account with abnormal credit balance:
   - Line: Debit the expense account, Credit Retained Earnings

7. Post the entry via Entry.post() — full audit trail, balance verification, cache update

8. Transition period to "closed" via Period.transition()

### Two-Step Close (when income_summary IS designated)

Three separate journal entries:

**Entry 1: Close revenues to Income Summary**
- document_number: "CLOSE-REV-P{n}-FY{year}"
- metadata: {"closing_entry": true, "method": "income_summary", "step": 1}
- For each revenue account with balance:
  - Debit Revenue (net balance), Credit Income Summary
- Post via Entry.post()

**Entry 2: Close expenses to Income Summary**
- document_number: "CLOSE-EXP-P{n}-FY{year}"
- metadata: {"closing_entry": true, "method": "income_summary", "step": 2}
- For each expense account with balance:
  - Debit Income Summary, Credit Expense (net balance)
- Post via Entry.post()

**Entry 3: Close Income Summary to Retained Earnings**
- document_number: "CLOSE-IS-P{n}-FY{year}"
- metadata: {"closing_entry": true, "method": "income_summary", "step": 3}
- Income Summary now has the net income balance
- If net income (credit balance): Debit Income Summary, Credit Retained Earnings
- If net loss (debit balance): Debit Retained Earnings, Credit Income Summary
- Post via Entry.post()

Transition period to "closed" via Period.transition()

### Post-Close State

After successful closePeriod:
- All revenue accounts: zero balance for this period
- All expense accounts: zero balance for this period
- Income Summary (if used): zero balance
- Retained Earnings: increased by net income (credit) or decreased by net loss (debit)
- Period status: closed
- All closing entries: posted, audited, with metadata identifying them as closing entries
- Balance cache: updated for all affected accounts

### Contra Account Handling

Contra revenue accounts (is_contra=true, account_type='revenue'):
- These have debit-normal balance (opposite of regular revenue)
- Close the same way: zero the account, contra to RE (or IS)
- The sign naturally handles itself because we use the actual debit_sum/credit_sum, not a computed "net income"

Contra expense accounts (is_contra=true, account_type='expense'):
- These have credit-normal balance
- Same: zero the account, contra to RE (or IS)

No special-casing needed — the algorithm uses actual balances, not assumptions about normal balance direction.

### C ABI

```
ledger_close_period(handle, book_id, period_id, performed_by) bool
```

### Tests

**Direct close:**
1. Post revenue 10000 and expense 6000 in a period, closePeriod, verify:
   - Revenue account balance = 0
   - Expense account balance = 0
   - Retained Earnings increased by 4000
   - One closing entry created with correct metadata
   - Period status = closed

2. Net loss: revenue 3000, expense 5000, closePeriod, verify:
   - RE decreased by 2000

3. Multiple revenue and expense accounts: verify all zeroed

4. Contra accounts: contra revenue with debit balance, verify correctly closed

**Two-step close:**
5. Same as test 1 but with income_summary designated, verify:
   - Three closing entries created (step 1, 2, 3)
   - Income Summary balance = 0 after all three
   - RE increased by net income
   - Period status = closed

**Compliance gates:**
6. No retained_earnings designated: expect RetainedEarningsAccountRequired
7. Draft entries exist in period: expect InvalidInput
8. Period already closed: expect PeriodClosed
9. Period locked: expect PeriodLocked

**Edge cases:**
10. Period with zero revenue and expense: no closing entries created, period still transitions to closed
11. Close then verify: run ledger_verify, expect all checks pass
12. Close then BS: verify RE has correct balance, no phantom net income row

**Reopen and re-close:**
13. Close period, reopen (closed → open), reverse closing entries, post adjustment, re-close
    Verify: new closing entries reflect adjusted amounts

---

## 14B: Carry-Forward Verification

### Addition to verify.zig

Add Check 9: closed periods should have zero revenue/expense balances

```zig
// Check 9: Closed period R/E account balances should be zero
{
    var stmt = try database.prepare(
        \\SELECT COUNT(*) FROM ledger_account_balances ab
        \\JOIN ledger_accounts a ON a.id = ab.account_id
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.status IN ('closed', 'locked')
        \\  AND a.account_type IN ('revenue', 'expense')
        \\  AND (ab.debit_sum != 0 OR ab.credit_sum != 0);
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    if (stmt.columnInt(0) > 0) result.warnings += 1;
}
```

This catches periods that were "closed" via period transition without running closePeriod (accounting error — period status says closed but temporary accounts still have balances).

### Tests

1. Close period properly via closePeriod: verify passes
2. Transition period to closed WITHOUT closing entries: verify warns

---

## What This Sprint Does NOT Include

- Dividends/drawings closing (application creates regular JE — Rule 16)
- OCI closing to AOCI (future sprint if needed)
- Automatic reversal of closing entries on reopen (application uses existing reverse())
- Batch close of multiple periods (application loops closePeriod)

---

## Success Criteria

1. closePeriod creates correct closing entries (direct and two-step)
2. All compliance gates enforced
3. All 10 account types handled correctly (5 base + 5 contra)
4. verify() detects improperly closed periods
5. Full reopen-adjust-reclose cycle works
6. All existing tests pass + new tests bring total to ~910+
