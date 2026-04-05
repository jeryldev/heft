# Sprint 11: Hardening — Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address all 76 findings (16 HIGH, 37 MEDIUM, 23 LOW) from the line-by-line code audit across all 17 source files.

**Architecture:** Fix-in-place across existing modules. No new files. Changes grouped by dependency order: foundation (error.zig, schema.zig) first, then safety fixes, entity TOCTOU, reports/verify, classification/subledger, query/export, build, tests.

**Tech Stack:** Zig 0.15.2, SQLite (vendored), C ABI

---

## Task 1: error.zig — Add specific error variants (M3)

**Files:** Modify: `src/error.zig`

- [ ] Add new error variants to replace overloaded InvalidInput:
  - BookArchived, CrossBookViolation, InvalidFxRate, InvalidDecimalPlaces
- [ ] Add BufferTooSmall for query.zig buffer overflow distinction
- [ ] Add SqliteError as catch-all for SQLite infrastructure errors

## Task 2: schema.zig — Constraints, indexes, CHECKs (M9-M13, M10-M11, L18-L19)

**Files:** Modify: `src/schema.zig`

- [ ] Add FK REFERENCES for rounding_account_id (M9)
- [ ] Add CHECK reverses_entry_id != id (M12)
- [ ] Add CHECK for classification_nodes group/account invariant (M11)
- [ ] Strengthen date CHECKs with date() validation (M10)
- [ ] Add CHECK number_range_start <= number_range_end for subledger_groups (L18)
- [ ] Add 3 missing indexes: entries(book_id,period_id,status), account_balances(period_id,account_id), entries(reverses_entry_id) (M13)
- [ ] Increment SCHEMA_VERSION to 2

## Task 3: money.zig — Safety guards (H6, M15, L5, L6)

**Files:** Modify: `src/money.zig`

- [ ] Guard computeBaseAmount against fx_rate <= 0 → error.InvalidFxRate (L5, L6)
- [ ] Guard formatDecimal against decimal_places > 8 → error.InvalidDecimalPlaces (H6)
- [ ] Replace bare frac_part *= 10 with std.math.mul (M15)
- [ ] Add tests: fx_rate=0, fx_rate negative, decimal_places=9, ".5", "1.", "-0", scale=1, buffer too small, negative dp=0

## Task 4: db.zig — bindText range check (M36)

**Files:** Modify: `src/db.zig`

- [ ] Guard @intCast(value.len) with range check against maxInt(c_int) in bindText

## Task 5: entry.zig — Overflow, audit, TOCTOU, comments (H1-H3, M1, M5-M7)

**Files:** Modify: `src/entry.zig`

- [ ] H1: Replace bare += with std.math.add for total_base_debits/credits accumulation
- [ ] H2: Fix abs_diff using unsigned arithmetic for minInt(i64) safety
- [ ] H3: Add audit.log for auto-posted rounding line (capture lastInsertRowId)
- [ ] M1: Move pre-validation reads inside BEGIN IMMEDIATE for createDraft, addLine, editLine, removeLine, deleteDraft, post, voidEntry, reverse, editDraft, editPosted
- [ ] M5: Fix counterparty audit to pass null instead of "0" when removing
- [ ] M6: Change mark-stale to only mark future periods (WHERE start_date > current period end_date)
- [ ] M7: Fix step numbering in post() comments
- [ ] Add tests: overflow accumulation, abs_diff minInt, rounding audit, stale-marking, counterparty null audit

## Task 6: main.zig — Null guards, @intCast, error mapping (H4-H5, M21-M24, L13-L14)

**Files:** Modify: `src/main.zig`

- [ ] H4: Change ledger_open path to ?[*:0]const u8 with orelse guard
- [ ] H5: Change ledger_verify out params to ?*u32 with orelse guards
- [ ] M21: Guard all @intCast(usize->i32) with saturation
- [ ] M22: Map 5 SQLite errors to distinct codes (90-94) in mapError
- [ ] M23: Validate ExportFormat and SortOrder integer values
- [ ] M24: Mark ledger_edit_line as deprecated (comment)
- [ ] L13: Replace setError(2) with setError(mapError(error.InvalidInput)) at 4 sites
- [ ] L14: Update error code comment to include codes 11-20, 90-94
- [ ] Add tests: null path, null out params, invalid format values, exact error codes

## Task 7: book.zig — TOCTOU, enum usage (M1, M2)

**Files:** Modify: `src/book.zig`

- [ ] Move pre-validation inside transaction for setRoundingAccount, updateName, archive
- [ ] Replace string comparisons with BookStatus.fromString
- [ ] Fix test comment at line 527

## Task 8: account.zig — TOCTOU, enum, error handling (M1, M2, M8)

**Files:** Modify: `src/account.zig`

- [ ] Move pre-validation inside transaction for create, updateName, updateStatus
- [ ] Replace string comparisons with enum-based checks
- [ ] M8: Replace catch-all DuplicateNumber with proper error checking

## Task 9: period.zig — TOCTOU, overlap detection, type casts (M1, M2, M8, M34, L22)

**Files:** Modify: `src/period.zig`

- [ ] Move pre-validation inside transaction for create
- [ ] Replace string comparisons with enum-based checks
- [ ] M8: Replace catch-all DuplicateNumber
- [ ] M34: Add overlap detection to bulkCreate
- [ ] L22: Simplify isLeapYear type cast chain
- [ ] M33: Add bulkCreate tests for all 12 start months + quarterly/semi-annual leap year

## Task 10: report.zig — Buffer sizes, overflow, journal register (H7, M16-M18)

**Files:** Modify: `src/report.zig`

- [ ] H7: Change TransactionRow.description to [1001]u8
- [ ] M16: Change ReportRow.account_type to [16]u8
- [ ] M17: Create separate jr_sql for journalRegister with ORDER BY document_number
- [ ] M18: Add checked arithmetic for running balance and totals
- [ ] Fix TB test assertion (>= 4 to == 5), add AL opening_balance assertion

## Task 11: verify.zig — Strengthen all checks (H8-H10, M19-M20)

**Files:** Modify: `src/verify.zig`

- [ ] H8: Include reversed entries in balance equation check
- [ ] H9: Add orphaned cache entries and orphaned entry lines checks
- [ ] H10: Strengthen audit check to verify complete trail per status
- [ ] M19: Convert Check 3 from count-only to actual period integrity checks (overlaps, gaps)
- [ ] M20: Add stale cache warning, reversed-entry-has-reversal check, duplicate line numbers check
- [ ] Add tests for all new checks

## Task 12: classification.zig — Move bugs, book-active, audit (H11-H12, M27-M28)

**Files:** Modify: `src/classification.zig`

- [ ] H11: Add same-classification constraint check in move()
- [ ] H12: Recursive UPDATE of children depths after move
- [ ] M27: Audit each descendant in recursive delete
- [ ] M28: Add book-active check to getBookId helper (affects addGroup, addAccount, move, updateLabel, delete)
- [ ] Add tests: cross-classification move, children depth, self-parent, max-depth, cascade delete audit

## Task 13: subledger.zig — Validation, book-active (H13, M29)

**Files:** Modify: `src/subledger.zig`

- [ ] H13: Add group existence check and cross-book validation in SubledgerAccount.create
- [ ] M29: Add book-active check to SubledgerGroup.updateName and SubledgerAccount.updateName
- [ ] Move pre-validation inside transactions
- [ ] Add tests: invalid group_id, cross-book group, updateName happy+error, delete with accounts

## Task 14: query.zig — Fix logic bugs, add missing columns (H14-H15, M30-M31)

**Files:** Modify: `src/query.zig`

- [ ] H14: Fix counterpartyLedger to accept/derive normal_balance direction
- [ ] H15: Fix subledgerReconciliation to use transaction-level for both GL and subledger
- [ ] M30: Add granularity to listPeriods, metadata to getEntry/listEntries
- [ ] M31: Add book_id parameter to getEntry for isolation

## Task 15: export.zig — Wire formatAmount (M32)

**Files:** Modify: `src/export.zig`

- [ ] Wire formatAmount into reportToCsv, reportToJson, ledgerResultToCsv, ledgerResultToJson, classifiedResultToCsv, classifiedResultToJson

## Task 16: build.zig — Reuse module (M25)

**Files:** Modify: `build.zig`

- [ ] Reuse main_mod for shared lib instead of creating duplicate inline module

## Task 17: audit.zig — Add logWithStmt tests (M35)

**Files:** Modify: `src/audit.zig`

- [ ] Add test for logWithStmt with 3 sequential entries
- [ ] Add test for logWithStmt reset/clearBindings contract

## Task 18: Test coverage — Untested functions and edge cases (H16, remaining)

**Files:** Modify: `src/query.zig`, `src/main.zig`, `src/entry.zig`

- [ ] H16: Add tests for counterpartyLedger, listTransactions, subledgerReport, subledgerReconciliation, agedSubledger
- [ ] Add main.zig tests: ledger_verify happy+failure, ledger_edit_line_full happy path, subledger query exports happy path
- [ ] Add entry.zig edge cases: void when cache deleted, reverse 96-char doc, editDraft on void entry
- [ ] Add classification.zig tests: max_depth CircularReference

## Task 19: root.zig — Cleanup (L17)

**Files:** Modify: `src/root.zig`

- [ ] Remove redundant @import("error.zig") in comptime block

## Task 20: Compile and test

- [ ] Run `zig build test` — all tests pass
- [ ] Run `zig build` — static and shared libs compile
- [ ] Verify no warnings
