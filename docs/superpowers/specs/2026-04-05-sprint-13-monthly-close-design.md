# Sprint 13: Daily Operations & Monthly Close Foundation

**Date:** 2026-04-05
**Branch:** heft-sprint-13 (from heft-sprint-12)
**Lifecycle Phase:** Phase 1 (Daily Ops) + Phase 3 (Monthly Close)
**Reference:** .research/11-accounting-lifecycle-checklist.md, Phases 1 and 3
**Depends on:** Sprint 12 (system accounts for BS retained earnings row)

---

## Goal

Make the monthly close process produce correct, trustworthy financial statements. After this sprint, an accountant can run trial balance, income statement, and balance sheet at month-end and trust the numbers — stale cache is automatically recalculated, the BS shows a proper retained earnings + net income presentation, and comparative reports show current vs prior period.

---

## Architecture

Changes to existing files only:
- report.zig: stale cache recalculation, BS retained earnings row, TB movement, comparative result types
- entry.zig: recalculateStale helper (called by reports)
- main.zig: new C ABI exports for new reports + comparative variants
- query.zig: updated getBook output (system account columns from Sprint 12)

New internal function in a new file:
- cache.zig: recalculateStale function (extracted from entry.zig's posting logic pattern)

---

## 13A: Stale Cache Recalculation

### The Problem
When an entry is posted in period 1, all future periods are marked is_stale=1. Reports for those periods currently serve stale data silently.

### The Solution
Each report function calls recalculateStale for the periods it needs before querying the cache.

### New File: cache.zig

```
pub fn recalculateStale(database, book_id, period_ids: []const i64) !u32
```

For each period_id in the list:
1. Query all stale cache rows: `SELECT account_id FROM ledger_account_balances WHERE book_id = ? AND period_id = ? AND is_stale = 1`
2. For each stale (account, period) pair, recompute from posted entry lines:
   ```sql
   SELECT COALESCE(SUM(el.base_debit_amount), 0), COALESCE(SUM(el.base_credit_amount), 0), COUNT(*)
   FROM ledger_entry_lines el
   JOIN ledger_entries e ON e.id = el.entry_id
   WHERE e.book_id = ? AND e.period_id = ? AND e.status = 'posted' AND el.account_id = ?
   ```
3. Update the cache row: `UPDATE ledger_account_balances SET debit_sum = ?, credit_sum = ?, balance = ? - ?, entry_count = ?, is_stale = 0, last_recalculated_at = now WHERE account_id = ? AND period_id = ?`
4. Return count of rows recalculated

All recalculation happens in a single transaction (atomic — reports see consistent state).

### Full-book variant

```
pub fn recalculateAllStale(database, book_id) !u32
```

Same logic but queries ALL stale rows for the book, not a specific period list. Exposed via C ABI for manual trigger.

### Integration with reports

Each report function determines which periods it needs, then calls recalculateStale before the report query:

| Report | Periods needed |
|--------|---------------|
| trialBalance(as_of_date) | All periods with end_date <= as_of_date |
| incomeStatement(start, end) | Periods with start_date >= start AND end_date <= end |
| balanceSheet(as_of_date, fy_start) | All periods with end_date <= as_of_date |
| trialBalanceMovement(start, end) | Periods in the date range |
| generalLedger(start, end) | No cache dependency (reads from view) — skip |
| accountLedger(start, end) | Opening balance periods (end_date < start) + detail periods |
| journalRegister(start, end) | No cache dependency — skip |

The period lookup query:
```sql
SELECT id FROM ledger_periods WHERE book_id = ? AND end_date <= ? AND EXISTS (
    SELECT 1 FROM ledger_account_balances WHERE book_id = ? AND period_id = ledger_periods.id AND is_stale = 1
)
```

This returns only periods that actually have stale data — if nothing is stale, the recalculation is a no-op.

### C ABI

```
ledger_recalculate_balances(handle, book_id) i32
    Returns: count of rows recalculated, or -1 on error
```

### root.zig

Add cache.zig to module re-exports.

### Tests

1. Post in period 1, verify period 2 is stale, run TB for period 2, verify stale auto-recalculated
2. Post in period 1, run TB for period 1 only, verify period 2 still stale (scoped recalc)
3. No stale data: run TB, verify no recalculation happens (performance — no unnecessary work)
4. Manual trigger: ledger_recalculate_balances returns correct count
5. Concurrent safety: recalculate is atomic (single transaction)

---

## 13B: Balance Sheet Retained Earnings Row

### The Problem
Currently, net income is injected into BS total_debits/total_credits only — not as a visible row. The accountant can't see "Net Income — Current Period" on the BS.

### The Solution
If retained_earnings_account_id is designated on the book:
1. Retained Earnings account appears naturally in BS rows (it's an equity account with posted balance from prior closing entries)
2. Compute "Net Income — Current Period" from unclosed revenue/expense activity
3. Inject as a synthetic row in result.rows AFTER all equity accounts

### Logic in balanceSheet function

After building the normal BS rows and BEFORE computing totals:

```zig
// Compute current-period net income from unclosed revenue/expense
const re_account_id = getRetainedEarningsAccountId(database, book_id);
if (re_account_id) |re_id| {
    // Query revenue/expense activity for the current fiscal year that hasn't been closed
    // "Unclosed" = revenue/expense accounts with non-zero balance for periods in the FY
    var net_income: i64 = 0;
    // Sum all revenue credits - revenue debits (revenue is credit-normal)
    // Sum all expense debits - expense credits (expense is debit-normal)
    // net_income = net_revenue - net_expense
    
    if (net_income != 0) {
        // Add synthetic row
        var ni_row: ReportRow = undefined;
        ni_row.account_id = re_id;
        // Use retained earnings account number/name for identification
        // But mark with a flag or special convention that this is computed, not a real account balance
        ni_row.debit_balance = if (net_income < 0) -net_income else 0;
        ni_row.credit_balance = if (net_income > 0) net_income else 0;
        try rows.append(allocator, ni_row);
    }
}
```

### How it works across the fiscal year

| Scenario | RE Account Balance | Net Income Row | Total Equity |
|----------|-------------------|----------------|-------------|
| Mid-year (no close yet) | 0 (or prior year RE) | Current YTD net income | RE + NI |
| After year-end close | RE includes closed net income | 0 (R/E accounts zeroed) | RE only |
| Partial close (some periods closed) | RE includes closed periods | Unclosed periods' net income | RE + NI |

This prevents double-counting because:
- Closed periods: R/E accounts are zero → net income computation returns 0
- Open periods: R/E accounts have balances → net income computation returns the unclosed amount
- RE account: has actual posted balance from closing entries

### balanceSheet signature change

Add fy_start_date parameter (already exists in current signature — verify it's used correctly for the net income computation window).

### Tests

1. Mid-year BS: post revenue 1000, expense 600, verify net income row = 400 credit
2. After close: close the period, verify net income row = 0 (R/E accounts zeroed), RE account has 400
3. Net loss: revenue 500, expense 800, verify net income row = 300 debit
4. No RE designated: no net income row (backward compatible, totals-only injection)
5. Multiple periods: revenue in P1 and P2, verify cumulative net income for the FY

---

## 13C: Trial Balance Movement Report

### New function

```
pub fn trialBalanceMovement(database, book_id, start_date, end_date) !*ReportResult
```

Pattern B (period activity) applied to ALL 5 account types:
```sql
SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
    SUM(ab.debit_sum), SUM(ab.credit_sum)
FROM ledger_account_balances ab
JOIN ledger_accounts a ON a.id = ab.account_id
JOIN ledger_periods p ON p.id = ab.period_id
WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
GROUP BY a.id
ORDER BY a.number ASC
```

Uses buildReportResult — same debit/credit column assignment based on normal_balance.

This is identical to incomeStatement but without the account_type filter. Shows period activity for assets, liabilities, equity, revenue, AND expense.

### C ABI

```
ledger_trial_balance_movement(handle, book_id, start_date, end_date) ?*ReportResult
```

Free via existing ledger_free_result.

### Tests

1. Post entries for A/L/E/R/E accounts, verify all 5 types appear in movement report
2. Movement for a period with no activity: empty result
3. Movement correctly shows only the period's activity (not cumulative)

---

## 13D: Comparative Reports

### New Result Type

```zig
pub const ComparativeReportRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    account_type: [16]u8,
    account_type_len: usize,
    current_debit: i64,
    current_credit: i64,
    prior_debit: i64,
    prior_credit: i64,
    variance_debit: i64,
    variance_credit: i64,
};

pub const ComparativeReportResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []ComparativeReportRow,
    current_total_debits: i64,
    current_total_credits: i64,
    prior_total_debits: i64,
    prior_total_credits: i64,
    
    pub fn deinit(self: *ComparativeReportResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};
```

### Implementation approach

Each comparative function:
1. Run the current-period query (existing logic)
2. Run the prior-period query (same logic, different dates)
3. Merge results by account_id into ComparativeReportRow
4. Compute variance = current - prior for each account

Accounts that appear in current but not prior: prior columns = 0.
Accounts that appear in prior but not current: current columns = 0.

### Comparative Functions

```
trialBalanceComparative(database, book_id, current_date, prior_date) !*ComparativeReportResult
incomeStatementComparative(database, book_id, cur_start, cur_end, prior_start, prior_end) !*ComparativeReportResult
balanceSheetComparative(database, book_id, current_date, prior_date, fy_start) !*ComparativeReportResult
trialBalanceMovementComparative(database, book_id, cur_start, cur_end, prior_start, prior_end) !*ComparativeReportResult
```

### C ABI Exports

```
ledger_trial_balance_comparative(handle, book_id, current_date, prior_date) ?*ComparativeReportResult
ledger_income_statement_comparative(handle, book_id, cur_start, cur_end, prior_start, prior_end) ?*ComparativeReportResult
ledger_balance_sheet_comparative(handle, book_id, current_date, prior_date, fy_start) ?*ComparativeReportResult
ledger_trial_balance_movement_comparative(handle, book_id, cur_start, cur_end, prior_start, prior_end) ?*ComparativeReportResult
ledger_free_comparative_result(result) void
```

### Tests

1. TB comparative: create two fiscal years of data, verify both columns populated
2. IS comparative: Q1 current vs Q1 prior, verify variance calculation
3. BS comparative: year-end current vs year-end prior
4. Account exists in current but not prior: prior = 0
5. Account exists in prior but not current: current = 0
6. Variance computation: verified for positive and negative differences

---

## What This Sprint Does NOT Include

- Closing entries (Sprint 14)
- FX revaluation (Sprint 15)
- Cash flow statement (Sprint 16)
- Statement of changes in equity (Sprint 16)
- Dimensions/tagging (Sprint 17)

---

## Success Criteria

1. Reports never serve stale cache data — automatic scoped recalculation
2. Balance sheet shows "Net Income — Current Period" row when RE account designated
3. TB Movement report available for management reporting
4. All 4 comparative reports produce correct current + prior + variance
5. All existing tests pass + new tests bring total to ~880+
6. `zig build test` passes, `zig fmt --check` passes
