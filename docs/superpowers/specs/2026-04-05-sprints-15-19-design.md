# Sprints 15-19: FX Revaluation through Batch Operations

**Date:** 2026-04-05
**Reference:** .research/10-lifecycle-driven-sprint-plan.md, .research/11-accounting-lifecycle-checklist.md

---

## Sprint 15: FX Revaluation & Multi-Currency Close

**Lifecycle Phase:** Phase 3 step 3.5 + Phase 6 step 6.2
**Depends on:** Sprint 12 (fx_gain_loss_account_id), Sprint 14 (period close)

### Goal
IAS 21 / ASC 830 compliance. Period-end revaluation of foreign currency balances with automatic gain/loss journal entries.

### 15A: revalueForexBalances

**New file: revaluation.zig**

```
pub fn revalueForexBalances(database, book_id, period_id, rates: []const CurrencyRate, performed_by) !i64
```

Where CurrencyRate = struct { currency: []const u8, new_rate: i64 }.

**Compliance gate:** fx_gain_loss_account_id MUST be designated → error.FxGainLossAccountRequired

**Process:**
1. For each account in the book with foreign currency entry lines in the period:
   a. Compute current base amount: SUM(transaction_amount) * new_rate / FX_RATE_SCALE
   b. Compute existing base amount: SUM(base_debit_amount - base_credit_amount) from posted lines
   c. Difference = unrealized gain or loss
2. Create a single compound revaluation journal entry:
   - For each account with a difference:
     - If gain (new base > old base): Debit account, Credit FX Gain/Loss
     - If loss (new base < old base): Debit FX Gain/Loss, Credit account
   - document_number: "REVAL-P{n}-FY{year}"
   - metadata: {"revaluation": true, "rates": {currency: rate, ...}}
3. Post via Entry.post()
4. Return: revaluation entry id

**C ABI:**
```
ledger_revalue_forex_balances(handle, book_id, period_id, rates_json, performed_by) i64
    rates_json: JSON string like [{"currency":"USD","rate":56500000000}]
    Returns: entry_id or -1
```

Parsing JSON for rates in Zig: use a simple manual parser for the fixed structure (array of {currency, rate} objects). No external JSON library needed — the format is constrained.

### 15B: Revaluation Reversal Pattern

Common practice: reverse revaluation entries at the start of next period. Application uses existing reverse() on the revaluation entry. No new engine feature needed — the metadata {"revaluation": true} identifies them.

### Tests
1. Revalue USD balance when PHP rate changes: verify gain/loss entry
2. Multiple currencies in single revaluation call
3. No foreign currency balances: no entry created (no-op)
4. Missing fx_gain_loss_account: expect FxGainLossAccountRequired
5. Verify audit trail on revaluation entry

---

## Sprint 16: Cash Flow Statement & Equity Statement

**Lifecycle Phase:** Phase 6 step 6.6 (annual statements), Phase 5 (semi-annual)
**Depends on:** Sprint 13 (reports infrastructure), existing classification module

### Goal
Complete the IFRS/GAAP financial statement package: Cash Flow (IAS 7/ASC 230) and Changes in Equity (IAS 1).

### 16A: Cash Flow Statement

**Classification-based approach** — reuses existing classification tree infrastructure.

New report_type value added to schema CHECK: "cash_flow"

Application creates a classification with report_type = "cash_flow" and builds the tree:
- Operating Activities (group node)
  - Cash from customers (group or account nodes)
  - Cash paid to suppliers
  - Cash paid for operating expenses
  - Interest paid/received
  - Tax paid
- Investing Activities (group node)
  - Purchase of equipment
  - Sale of investments
- Financing Activities (group node)
  - Proceeds from borrowing
  - Repayment of debt
  - Dividends paid

**New report function:**
```
pub fn cashFlowStatement(database, classification_id, start_date, end_date) !*ClassifiedResult
```

Uses the existing classifiedReport infrastructure but filtered to cash accounts (accounts in the classification tree). The classification tree structure IS the cash flow structure.

This is the indirect method: the tree groups cash movements by activity type. The direct method (starting from net income and adjusting) would require a different approach — but the indirect method is what most companies use and what the classification tree naturally supports.

**C ABI:**
```
ledger_cash_flow_statement(handle, classification_id, start_date, end_date) ?*ClassifiedResult
```

Free via existing ledger_free_classified_result.

### 16B: Statement of Changes in Equity

**New result type:**
```zig
pub const EquityRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    opening_balance: i64,
    period_activity: i64,
    closing_balance: i64,
};

pub const EquityResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []EquityRow,
    net_income: i64,
    total_opening: i64,
    total_closing: i64,
    
    pub fn deinit(self: *EquityResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};
```

**New report function:**
```
pub fn equityChanges(database, book_id, start_date, end_date, fy_start_date) !*EquityResult
```

For each equity account:
1. Opening balance: SUM from periods with end_date < start_date
2. Period activity: SUM from periods within start_date..end_date
3. Closing balance: opening + activity

Plus a computed net_income row (revenue - expenses for the period).

Components of equity change:
- Capital / Share capital: equity account movements
- Retained Earnings: actual RE account balance change
- Net Income: computed from R/E (for unclosed periods)
- Dividends: contra equity movements
- OCI: if AOCI account exists

**C ABI:**
```
ledger_equity_changes(handle, book_id, start_date, end_date, fy_start_date) ?*EquityResult
ledger_free_equity_result(result) void
```

### Tests
1. Cash flow: create classification, assign cash accounts, post entries, verify O/I/F grouping
2. Equity changes: post capital, revenue, expense, dividends, verify all components
3. Equity changes after period close: verify RE reflects closed net income

---

## Sprint 17: Dimensions & Tagging Infrastructure

**Lifecycle Phase:** All — enables any industry/jurisdiction tagging
**Depends on:** Sprint 12 (schema version management)

### Goal
Structured metadata on entry lines for tax codes, cost centers, departments, projects, segments. The universal infrastructure that replaces ad-hoc JSON metadata with queryable, typed dimensions.

### Schema

```sql
CREATE TABLE IF NOT EXISTS ledger_dimensions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    dimension_type TEXT NOT NULL
        CHECK (dimension_type IN ('tax_code', 'cost_center', 'department', 'project', 'segment', 'custom')),
    book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    UNIQUE (book_id, name)
);

CREATE TABLE IF NOT EXISTS ledger_dimension_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL CHECK (length(code) BETWEEN 1 AND 50),
    label TEXT NOT NULL CHECK (length(label) BETWEEN 1 AND 255),
    dimension_id INTEGER NOT NULL REFERENCES ledger_dimensions(id),
    UNIQUE (dimension_id, code)
);

CREATE TABLE IF NOT EXISTS ledger_line_dimensions (
    line_id INTEGER NOT NULL REFERENCES ledger_entry_lines(id),
    dimension_value_id INTEGER NOT NULL REFERENCES ledger_dimension_values(id),
    PRIMARY KEY (line_id, dimension_value_id)
);
```

### Functions

```
Dimension.create(database, book_id, name, dimension_type, performed_by) !i64
Dimension.delete(database, dimension_id, performed_by) !void
DimensionValue.create(database, dimension_id, code, label, performed_by) !i64
DimensionValue.delete(database, value_id, performed_by) !void
LineDimension.assign(database, line_id, dimension_value_id, performed_by) !void
LineDimension.remove(database, line_id, dimension_value_id, performed_by) !void
```

### Query Integration

Add optional dimension_value_id filter to:
- trialBalance, incomeStatement, balanceSheet (filter accounts by dimension)
- All list functions (filter by dimension values on entry lines)

New query:
```
dimensionSummary(database, dimension_id, start_date, end_date, buf, format) ![]u8
    Returns: debit/credit totals grouped by dimension value
    Example: VAT summary by tax code, expense by department
```

### C ABI

Full CRUD for dimensions + values + assignments.
Dimension-filtered report variants.

---

## Sprint 18: Reconciliation & Budget

**Lifecycle Phase:** Phase 3 steps 3.7-3.8, Phase 5 step 5.4
**Depends on:** Sprint 13 (reports)

### 18A: GL-SL Reconciliation

Add to verify.zig Check 10:
```sql
-- For each subledger group: SUM(subledger balances) should = GL control account balance
SELECT sg.id, sg.name, sg.gl_account_id,
    ab.debit_sum - ab.credit_sum AS gl_balance,
    COALESCE(sl_totals.sl_balance, 0) AS sl_balance
FROM ledger_subledger_groups sg
JOIN ledger_account_balances ab ON ab.account_id = sg.gl_account_id AND ab.period_id = ?
LEFT JOIN (...subledger sum subquery...) sl_totals ON sl_totals.group_id = sg.id
WHERE sg.book_id = ? AND gl_balance != sl_balance
```

Also new standalone report:
```
reconcileSubledger(database, group_id, period_id) !ReconciliationResult
```

### 18B: Budget Entity

New tables: ledger_budgets, ledger_budget_lines
CRUD functions for budgets and budget lines
budgetVsActual report comparing budget to actual per account

---

## Sprint 19: Batch Operations & Performance

**Lifecycle Phase:** Phase 1 (daily high-volume)
**Depends on:** Core posting (existing)

### Features

```
batchPost(database, entry_ids: []const i64, performed_by) !void
    All-or-nothing: single transaction, all entries posted or none
    
batchVoid(database, entry_ids: []const i64, reason, performed_by) !void
    Same atomic pattern

exportBook(database, book_id, buf, format) ![]u8
    Full book to JSON (schema, accounts, periods, entries, lines, cache, audit)
```

---

## Sprint Delivery Timeline

| Sprint | Scope | Est. Complexity |
|--------|-------|----------------|
| 12 | System accounts + C ABI + OB migration | Medium |
| 13 | Stale cache + BS RE row + TB movement + comparative | Large |
| 14 | Period close (direct + two-step) | Medium |
| 15 | FX revaluation | Medium |
| 16 | Cash flow + equity changes | Medium |
| 17 | Dimensions (3 new tables + query integration) | Large |
| 18 | Reconciliation + budget | Medium |
| 19 | Batch ops + export | Small |

## Lifecycle Coverage After All Sprints

Every procedure from the accounting lifecycle checklist will be supported:
- Daily operations: BUILT (Sprint 1-11)
- Monthly close: Sprint 13
- Period/year-end close: Sprint 14
- FX revaluation: Sprint 15
- Full financial statements: Sprint 16
- Tax/dimension tagging: Sprint 17
- Reconciliation/budget: Sprint 18
- High-volume operations: Sprint 19
- Audit support: BUILT (Sprint 1-11)
