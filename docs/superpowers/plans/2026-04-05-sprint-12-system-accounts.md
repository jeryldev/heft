# Sprint 12: System Accounts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add 5 system account designation slots, fix C ABI parameter gaps, enable opening balance migration.

**Architecture:** Extend ledger_books with 5 nullable columns, add 5 set*Account functions in book.zig following setRoundingAccount pattern, add 4 compliance gate errors, fix C ABI exports for addLine/createDraft/reverse parameters.

**Tech Stack:** Zig 0.15.2, SQLite, C ABI

---

### Task 1: Error variants + Schema columns

**Files:**
- Modify: `src/error.zig`
- Modify: `src/schema.zig`
- Modify: `src/main.zig` (SCHEMA_VERSION)

- [ ] Add 4 compliance gate errors to error.zig: `FxGainLossAccountRequired`, `IncomeSummaryAccountRequired`, `OpeningBalanceAccountRequired`, `RetainedEarningsAccountRequired`
- [ ] Add 5 columns to ledger_books DDL in schema.zig after rounding_account_id: `fx_gain_loss_account_id INTEGER`, `retained_earnings_account_id INTEGER`, `income_summary_account_id INTEGER`, `opening_balance_account_id INTEGER`, `suspense_account_id INTEGER`
- [ ] Change PRAGMA user_version from 2 to 3
- [ ] Change SCHEMA_VERSION in main.zig from 2 to 3
- [ ] Update schema tests: user_version assertion to 3
- [ ] Run: `zig build test` — all existing tests pass

### Task 2: Five set*Account functions in book.zig

**Files:**
- Modify: `src/book.zig`

Each function follows the exact setRoundingAccount pattern (lines 56-99). For each of the 5:

- [ ] **setFxGainLossAccount**: validates account_type IN ('revenue','expense'). UPDATE SET fx_gain_loss_account_id. Audit field="fx_gain_loss_account_id".
- [ ] **setRetainedEarningsAccount**: validates account_type = 'equity' AND is_contra = 0. UPDATE SET retained_earnings_account_id. Audit field="retained_earnings_account_id".
- [ ] **setIncomeSummaryAccount**: validates account_type = 'equity' AND is_contra = 0. UPDATE SET income_summary_account_id. Audit field="income_summary_account_id".
- [ ] **setOpeningBalanceAccount**: validates account_type = 'equity' AND is_contra = 0. UPDATE SET opening_balance_account_id. Audit field="opening_balance_account_id".
- [ ] **setSuspenseAccount**: validates account_type IN ('asset','liability'). UPDATE SET suspense_account_id. Audit field="suspense_account_id".

Type validation pattern — read account_type and is_contra from ledger_accounts:
```zig
const acct_type = stmt.columnText(0).?;
const is_contra = stmt.columnInt(2);
// For retained_earnings/income_summary/opening_balance:
if (!std.mem.eql(u8, acct_type, "equity") or is_contra != 0) return error.InvalidInput;
// For fx_gain_loss:
if (!std.mem.eql(u8, acct_type, "revenue") and !std.mem.eql(u8, acct_type, "expense")) return error.InvalidInput;
// For suspense:
if (!std.mem.eql(u8, acct_type, "asset") and !std.mem.eql(u8, acct_type, "liability")) return error.InvalidInput;
```

- [ ] Add tests for each function: happy path, wrong type, contra rejected, archived book, inactive account, cross-book, not found
- [ ] Run: `zig build test`

### Task 3: C ABI exports for 5 designations + error mapping

**Files:**
- Modify: `src/main.zig`

- [ ] Add 5 exports following ledger_set_rounding_account pattern (line 130-137):
  ```
  ledger_set_fx_gain_loss_account(handle, book_id, account_id, performed_by) bool
  ledger_set_retained_earnings_account(handle, book_id, account_id, performed_by) bool
  ledger_set_income_summary_account(handle, book_id, account_id, performed_by) bool
  ledger_set_opening_balance_account(handle, book_id, account_id, performed_by) bool
  ledger_set_suspense_account(handle, book_id, account_id, performed_by) bool
  ```
- [ ] Add error mappings in mapError: RetainedEarningsAccountRequired => 26, FxGainLossAccountRequired => 27, OpeningBalanceAccountRequired => 28, IncomeSummaryAccountRequired => 29
- [ ] Update error code comment block
- [ ] Add null-handle tests for all 5 exports
- [ ] Run: `zig build test`

### Task 4: C ABI parameter fixes (addLine, createDraft, reverse)

**Files:**
- Modify: `src/main.zig`

- [ ] **ledger_add_line**: Add `description: ?[*:0]const u8` parameter before `performed_by`. Convert with `if (description) |d| std.mem.span(d) else null`. Pass to Entry.addLine.
- [ ] **ledger_create_draft**: Add `description: ?[*:0]const u8` after `posting_date` and `metadata: ?[*:0]const u8` after `period_id`. Convert and pass to Entry.createDraft.
- [ ] **ledger_reverse_entry**: Add `target_period_id: i64` parameter. Convert: `const tp: ?i64 = if (target_period_id > 0) target_period_id else null;`. Pass to Entry.reverse.
- [ ] Update ALL existing tests that call these 3 functions to include the new parameters (pass null/0 for backward-compatible behavior)
- [ ] Add new tests: addLine with description, createDraft with desc+metadata, reverse with target_period_id
- [ ] Run: `zig build test`

### Task 5: Query updates — getBook includes new columns

**Files:**
- Modify: `src/query.zig`

- [ ] Update getBook SELECT to include the 5 new columns
- [ ] Update CSV output: add 5 columns to header and data row
- [ ] Update JSON output: add 5 fields (output as integer, 0 for null)
- [ ] Update listBooks SELECT if it shows these columns (check current query)
- [ ] Update existing getBook tests to verify new columns appear
- [ ] Run: `zig build test`

### Task 6: Opening balance migration validation

**Files:**
- Modify: `src/book.zig`

- [ ] Add `validateOpeningBalanceMigration(database, book_id) !void`:
  - Query opening_balance_account_id from book
  - If null: return error.OpeningBalanceAccountRequired
  - Verify book exists, not archived
- [ ] Add test: call without designation → expect OpeningBalanceAccountRequired
- [ ] Add test: call with designation → succeeds
- [ ] Add integration test: full migration flow — designate OB account, create entry with account balances, add offset line to OB account, post, verify via trial balance
- [ ] Run: `zig build test`

### Task 7: Final verification

- [ ] Run: `zig fmt --check src/*.zig build.zig`
- [ ] Run: `zig build test` — all tests pass
- [ ] Run: `zig build` — static + shared libs compile
- [ ] Verify test count increased from 783
