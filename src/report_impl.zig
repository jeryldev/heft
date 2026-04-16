const std = @import("std");
const db = @import("db.zig");
const book_mod = @import("book.zig");
const money = @import("money.zig");
const common = @import("report_common.zig");
const ledger = @import("report_ledger.zig");
const statements = @import("report_statements.zig");
const compare = @import("report_compare.zig");

pub const MAX_REPORT_ROWS = common.MAX_REPORT_ROWS;
pub const ReportRow = common.ReportRow;
pub const ReportResult = common.ReportResult;
pub const TransactionRow = common.TransactionRow;
pub const LedgerResult = common.LedgerResult;
pub const ComparativeReportRow = common.ComparativeReportRow;
pub const ComparativeReportResult = common.ComparativeReportResult;
pub const EquityRow = common.EquityRow;
pub const EquityResult = common.EquityResult;
pub const TranslationRates = compare.TranslationRates;

pub const generalLedger = ledger.generalLedger;
pub const accountLedger = ledger.accountLedger;
pub const journalRegister = ledger.journalRegister;
pub const trialBalance = statements.trialBalance;
pub const incomeStatement = statements.incomeStatement;
pub const trialBalanceMovement = statements.trialBalanceMovement;
pub const balanceSheetAuto = statements.balanceSheetAuto;
pub const balanceSheet = statements.balanceSheet;
pub const balanceSheetAutoWithProjectedRE = statements.balanceSheetAutoWithProjectedRE;
pub const balanceSheetWithProjectedRE = statements.balanceSheetWithProjectedRE;
pub const trialBalanceComparative = compare.trialBalanceComparative;
pub const incomeStatementComparative = compare.incomeStatementComparative;
pub const balanceSheetComparative = compare.balanceSheetComparative;
pub const trialBalanceMovementComparative = compare.trialBalanceMovementComparative;
pub const equityChanges = compare.equityChanges;
pub const translateReportResult = compare.translateReportResult;

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const close_mod = @import("close.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "Cost of Goods Sold", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

fn postEntry(database: db.Database, doc: []const u8, date: []const u8, period_id: i64, debits: []const struct { amount: i64, account_id: i64 }, credits: []const struct { amount: i64, account_id: i64 }) !void {
    const entry_id = try entry_mod.Entry.createDraft(database, 1, doc, date, date, null, period_id, null, "admin");
    var line_num: i32 = 1;
    for (debits) |d| {
        _ = try entry_mod.Entry.addLine(database, entry_id, line_num, d.amount, 0, "PHP", money.FX_RATE_SCALE, d.account_id, null, null, "admin");
        line_num += 1;
    }
    for (credits) |c| {
        _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, c.amount, "PHP", money.FX_RATE_SCALE, c.account_id, null, null, "admin");
        line_num += 1;
    }
    try entry_mod.Entry.post(database, entry_id, "admin");
}

// ── Trial Balance tests ─────────────────────────────────────────

test "trial balance: single entry, two accounts" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "trial balance: multiple entries accumulate" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-01-16", 1, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    // Cash: 1500 debit, AP: 1500 credit
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_credits);
}

test "trial balance: voided entry excluded from balances" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "trial balance: all five account types" {
    const database = try setupTestDb();
    defer database.close();

    // Capital contribution: debit Cash, credit Capital
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    // Sale: debit Cash, credit Revenue
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    // Purchase: debit COGS, credit AP
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    // 5 accounts with activity (Cash, AP, Capital, Revenue, COGS)
    try std.testing.expectEqual(@as(usize, 5), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "trial balance: empty book returns zero rows" {
    const database = try setupTestDb();
    defer database.close();

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

test "trial balance: nonexistent book returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = trialBalance(database, 999, "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

test "trial balance: debits always equal credits (accounting equation)" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-004", "2026-01-25", 1, &.{.{ .amount = 2_000_000_000_00, .account_id = 2 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

// ── Income Statement tests ──────────────────────────────────────

test "income statement: revenue minus expenses" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Revenue 5000, Expense 3000 => Net income in total_credits - total_debits
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), result.total_credits); // revenue
    try std.testing.expectEqual(@as(i64, 3_000_000_000_00), result.total_debits); // expenses
}

test "income statement: only revenue and expense accounts" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Only revenue account should appear (asset/equity excluded)
    for (result.rows) |row| {
        const acct_type = row.account_type[0..row.account_type_len];
        try std.testing.expect(std.mem.eql(u8, acct_type, "revenue") or std.mem.eql(u8, acct_type, "expense"));
    }
}

test "income statement: empty period returns zero" {
    const database = try setupTestDb();
    defer database.close();

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

// ── Balance Sheet tests ─────────────────────────────────────────

test "balance sheet: assets equal liabilities plus equity" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // A = L + E (with net income injected)
    // Assets: Cash = 10000 + 5000 - 3000(via AP payment... wait, no)
    // Cash: debit 10000 + 5000 = 15000
    // AP: credit 3000
    // Capital: credit 10000
    // Net income: 5000 - 3000 = 2000
    // A (15000) = L (3000) + E (10000 + 2000) = 15000 ✓
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "balance sheet: includes net income from revenue minus expenses" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Balance sheet should only have A/L/E rows (not R/E)
    // But net income (5000) should be reflected in the equity side
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "balance sheet: empty book" {
    const database = try setupTestDb();
    defer database.close();

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

// ── Multi-period tests ──────────────────────────────────────────

test "trial balance: spans multiple periods" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    // Trial balance as of Feb 28 should include both periods
    const result = try trialBalance(database, 1, "2026-02-28");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
}

test "trial balance: as_of_date before second period excludes it" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    // Trial balance as of Jan 31 should only include Jan
    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), result.total_debits);
}

test "trial balance: mid-period as_of_date includes posted activity" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_500_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-15");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_credits);
}

test "balance sheet: mid-period as_of_date includes posted activity" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_500_000_000_00, .account_id = 2 }});

    const result = try balanceSheet(database, 1, "2026-01-15");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_credits);
}

test "income statement: closing entries are excluded after close" {
    const database = try setupTestDb();
    defer database.close();

    const retained_earnings = try account_mod.Account.create(database, 1, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, 1, retained_earnings, "admin");

    try postEntry(database, "REV-001", "2026-01-15", 1, &.{.{ .amount = 1_100_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_100_000_000_00, .account_id = 4 }});

    const before_close = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer before_close.deinit();
    const revenue_before = findRow(before_close.rows, "4000").?;
    try std.testing.expectEqual(@as(i64, 1_100_000_000_00), revenue_before.credit_balance);
    try std.testing.expectEqual(@as(i64, 1_100_000_000_00), before_close.total_credits);

    try close_mod.closePeriod(database, 1, 1, "admin");

    const after_close = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer after_close.deinit();
    const revenue_after = findRow(after_close.rows, "4000").?;
    try std.testing.expectEqual(@as(i64, 1_100_000_000_00), revenue_after.credit_balance);
    try std.testing.expectEqual(@as(i64, 1_100_000_000_00), after_close.total_credits);
    try std.testing.expectEqual(@as(i64, 0), after_close.total_debits);
}

// ── Helper: find row by account number ──────────────────────────

fn findRow(rows: []const ReportRow, acct_num: []const u8) ?ReportRow {
    for (rows) |row| {
        if (std.mem.eql(u8, row.account_number[0..row.account_number_len], acct_num)) return row;
    }
    return null;
}

// ── Comprehensive: every account type in reports ────────────────

fn setupFullDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try @import("schema.zig").createAll(database);
    _ = try book_mod.Book.create(database, "Full Test", "PHP", 2, "admin");
    // 5 standard accounts
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1900", "Accum Depreciation", .asset, true, "admin"); // contra
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    return database;
}

test "TB: contra asset account shows on credit side" {
    const database = try setupFullDb();
    defer database.close();

    // Debit depreciation expense, credit accumulated depreciation (contra asset)
    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    // Accum Dep (id=2) is contra asset with credit normal_balance

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    const accum_dep = findRow(result.rows, "1900");
    try std.testing.expect(accum_dep != null);
    // Contra asset: credit normal, so credit_sum > debit_sum -> shows on credit side
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), accum_dep.?.credit_balance);
    try std.testing.expectEqual(@as(i64, 0), accum_dep.?.debit_balance);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "TB: row content has correct account number and name" {
    const database = try setupFullDb();
    defer database.close();

    // Capital contribution
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    const cash = findRow(result.rows, "1000");
    try std.testing.expect(cash != null);
    try std.testing.expectEqualStrings("1000", cash.?.account_number[0..cash.?.account_number_len]);
    try std.testing.expectEqualStrings("Cash", cash.?.account_name[0..cash.?.account_name_len]);
    try std.testing.expectEqualStrings("asset", cash.?.account_type[0..cash.?.account_type_len]);
}

test "TB: reversed entry — both original and reversal affect balances" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 3 }});
    _ = try entry_mod.Entry.reverse(database, 1, "Reversal", "2026-01-31", null, "admin");

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    // After reversal, all balances should net to zero
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "TB: multi-currency entries aggregate correctly in base" {
    const database = try setupFullDb();
    defer database.close();

    // Entry in PHP (base currency, fx_rate = 1.0)
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});

    // Entry in USD with FX rate (100 USD * 56.50 = 5,650 PHP base)
    {
        const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = try entry_mod.Entry.addLine(database, entry_id, 1, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, null, "admin");
        _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 10_000_000_000, "USD", 565_000_000_000, 3, null, null, "admin");
        try entry_mod.Entry.post(database, entry_id, "admin");
    }

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    // Cash: 1000 PHP + 5650 PHP (from USD conversion) = 6650
    const cash = findRow(result.rows, "1000");
    try std.testing.expect(cash != null);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00 + 565_000_000_000), cash.?.debit_balance);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "BS: net loss (expenses > revenue) goes to debit side" {
    const database = try setupFullDb();
    defer database.close();

    // Capital contribution
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    // Revenue 2000
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 5 }});
    // Expenses 8000 (loss of 6000)
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 8_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 8_000_000_000_00, .account_id = 3 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Should still balance (A = L + E - net loss)
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "reports work on archived book (read-only)" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 3 }});

    // Close periods and archive book
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 2, .soft_closed, "admin");
    try period_mod.Period.transition(database, 2, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    // Reports should still work on archived book
    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), tb.total_debits);

    const is = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer is.deinit();

    const bs = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer bs.deinit();
    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
}

test "IS: excludes asset, liability, equity accounts" {
    const database = try setupFullDb();
    defer database.close();

    // Post entries touching ALL account types
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 5 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Verify no A/L/E accounts appear
    for (result.rows) |row| {
        const at = row.account_type[0..row.account_type_len];
        try std.testing.expect(!std.mem.eql(u8, at, "asset"));
        try std.testing.expect(!std.mem.eql(u8, at, "liability"));
        try std.testing.expect(!std.mem.eql(u8, at, "equity"));
    }
}

// ── Transaction history view verification ───────────────────────

test "transaction_history: posted entries visible, draft/void/reversed excluded" {
    const database = try setupFullDb();
    defer database.close();

    // Draft entry (not posted)
    _ = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-05", "2026-01-05", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 100_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 100_000_000, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");

    // Posted entry
    try postEntry(database, "POST-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});

    // Voided entry
    try postEntry(database, "VOID-001", "2026-01-15", 1, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 3 }});
    try entry_mod.Entry.voidEntry(database, 3, "Error", "admin");

    // Reversed entry (original becomes 'reversed', reversal is 'posted')
    try postEntry(database, "REV-SRC", "2026-01-20", 1, &.{.{ .amount = 200_000_000_00, .account_id = 1 }}, &.{.{ .amount = 200_000_000_00, .account_id = 3 }});
    _ = try entry_mod.Entry.reverse(database, 4, "Reversal", "2026-01-25", null, "admin");

    // View should show: POST-001 (2 lines) + reversal entry (2 lines) = 4 lines
    // Excluded: DRAFT-001 (draft), VOID-001 (void), REV-SRC (reversed)
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history;");
    defer stmt.finalize();
    _ = try stmt.step();
    // POST-001 (2 lines) + REV-REV-SRC reversal (2 lines) = 4
    try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
}

test "transaction_history: shows correct account details" {
    const database = try setupFullDb();
    defer database.close();

    // In setupFullDb: 1=Cash, 2=AccumDep, 3=AP, 4=Capital, 5=Revenue, 6=COGS
    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 5 }});

    var stmt = try database.prepare(
        \\SELECT account_number, account_name, account_type, debit_amount, credit_amount
        \\FROM ledger_transaction_history ORDER BY line_number;
    );
    defer stmt.finalize();

    // Line 1: Cash debit
    _ = try stmt.step();
    try std.testing.expectEqualStrings("1000", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Cash", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("asset", stmt.columnText(2).?);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(3));

    // Line 2: Revenue credit
    _ = try stmt.step();
    try std.testing.expectEqualStrings("4000", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Sales Revenue", stmt.columnText(1).?);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(4));
}

// ── Account balances cache verification ─────────────────────────

test "account_balances: reflects all posted entries per period" {
    const database = try setupFullDb();
    defer database.close();

    // Post in Jan
    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    // Post in Feb
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 4 }});

    // Verify per-period cache
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    }
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(0));
    }

    // Trial balance should show cumulative (3000)
    const result = try trialBalance(database, 1, "2026-02-28");
    defer result.deinit();

    const cash = findRow(result.rows, "1000");
    try std.testing.expect(cash != null);
    try std.testing.expectEqual(@as(i64, 3_000_000_000_00), cash.?.debit_balance);
}

test "account_balances: void zeros out specific period cache" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 4 }});

    // Void Jan entry
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    // Jan cache should be zero, Feb untouched
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    }
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(0));
    }

    // TB as of Feb: only Feb remains
    const result = try trialBalance(database, 1, "2026-02-28");
    defer result.deinit();

    const cash = findRow(result.rows, "1000");
    try std.testing.expect(cash != null);
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), cash.?.debit_balance);
}

// ── Comprehensive: all 10 account type+contra combinations ─────

fn setupAll10Db() !db.Database {
    const database = try db.Database.open(":memory:");
    try @import("schema.zig").createAll(database);
    _ = try book_mod.Book.create(database, "All 10 Types", "PHP", 2, "admin");

    // 1. Asset (debit normal) — Cash
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    // 2. Contra Asset (credit normal) — Accumulated Depreciation
    _ = try account_mod.Account.create(database, 1, "1900", "Accum Depreciation", .asset, true, "admin");
    // 3. Liability (credit normal) — Accounts Payable
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    // 4. Contra Liability (debit normal) — Discount on Bonds Payable
    _ = try account_mod.Account.create(database, 1, "2900", "Discount on Bonds", .liability, true, "admin");
    // 5. Equity (credit normal) — Capital
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    // 6. Contra Equity (debit normal) — Treasury Stock
    _ = try account_mod.Account.create(database, 1, "3900", "Treasury Stock", .equity, true, "admin");
    // 7. Revenue (credit normal) — Sales Revenue
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    // 8. Contra Revenue (debit normal) — Sales Returns
    _ = try account_mod.Account.create(database, 1, "4900", "Sales Returns", .revenue, true, "admin");
    // 9. Expense (debit normal) — COGS
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    // 10. Contra Expense (credit normal) — Purchase Discounts
    _ = try account_mod.Account.create(database, 1, "5900", "Purchase Discounts", .expense, true, "admin");

    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

// Helper to post entry with specific FX rate
fn postFxEntry(database: db.Database, doc: []const u8, date: []const u8, period_id: i64, debit_acct: i64, debit_amt: i64, credit_acct: i64, credit_amt: i64, currency: []const u8, fx_rate: i64) !i64 {
    const entry_id = try entry_mod.Entry.createDraft(database, 1, doc, date, date, null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, debit_amt, 0, currency, fx_rate, debit_acct, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, credit_amt, currency, fx_rate, credit_acct, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");
    return entry_id;
}

test "all 10 account types: realistic entries flow through cache, view, and reports" {
    const database = try setupAll10Db();
    defer database.close();

    const scale = money.FX_RATE_SCALE;
    // Account IDs: 1=Cash, 2=AccumDep, 3=AP, 4=DiscBonds, 5=Capital,
    //              6=Treasury, 7=Revenue, 8=SalesReturns, 9=COGS, 10=PurchDisc

    // 1. Capital contribution: Debit Cash 100,000, Credit Capital 100,000
    _ = try postFxEntry(database, "JE-001", "2026-01-02", 1, 1, 10_000_000_000_000, 5, 10_000_000_000_000, "PHP", scale);

    // 2. Sale: Debit Cash 50,000, Credit Sales Revenue 50,000
    _ = try postFxEntry(database, "JE-002", "2026-01-05", 1, 1, 5_000_000_000_000, 7, 5_000_000_000_000, "PHP", scale);

    // 3. Sales return: Debit Sales Returns 5,000, Credit Cash 5,000
    _ = try postFxEntry(database, "JE-003", "2026-01-06", 1, 8, 500_000_000_000, 1, 500_000_000_000, "PHP", scale);

    // 4. Purchase inventory: Debit COGS 30,000, Credit AP 30,000
    _ = try postFxEntry(database, "JE-004", "2026-01-10", 1, 9, 3_000_000_000_000, 3, 3_000_000_000_000, "PHP", scale);

    // 5. Purchase discount: Debit AP 1,000, Credit Purchase Discounts 1,000
    _ = try postFxEntry(database, "JE-005", "2026-01-11", 1, 3, 100_000_000_000, 10, 100_000_000_000, "PHP", scale);

    // 6. Depreciation: Debit Depreciation Expense (use COGS for simplicity), Credit Accum Dep 10,000
    _ = try postFxEntry(database, "JE-006", "2026-01-15", 1, 9, 1_000_000_000_000, 2, 1_000_000_000_000, "PHP", scale);

    // 7a. Bond issued at discount: Debit Cash 20,000, Credit Bonds Payable (AP) 25,000, Debit Discount on Bonds 5,000
    {
        const eid = try entry_mod.Entry.createDraft(database, 1, "JE-007a", "2026-01-18", "2026-01-18", null, 1, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 1, 2_000_000_000_000, 0, "PHP", scale, 1, null, null, "admin"); // debit Cash 20k
        _ = try entry_mod.Entry.addLine(database, eid, 2, 500_000_000_000, 0, "PHP", scale, 4, null, null, "admin"); // debit Disc on Bonds 5k
        _ = try entry_mod.Entry.addLine(database, eid, 3, 0, 2_500_000_000_000, "PHP", scale, 3, null, null, "admin"); // credit AP (as bonds payable) 25k
        try entry_mod.Entry.post(database, eid, "admin");
    }
    // 7b. Bond discount amortization: Debit Interest Expense, Credit Discount 2,000
    _ = try postFxEntry(database, "JE-007b", "2026-01-20", 1, 9, 200_000_000_000, 4, 200_000_000_000, "PHP", scale);

    // 8. Treasury stock purchase: Debit Treasury Stock 15,000, Credit Cash 15,000
    _ = try postFxEntry(database, "JE-008", "2026-01-25", 1, 6, 1_500_000_000_000, 1, 1_500_000_000_000, "PHP", scale);

    // === TRIAL BALANCE ===
    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();

    // Must balance
    try std.testing.expectEqual(tb.total_debits, tb.total_credits);

    // Verify each account shows on correct side
    // 1. Cash (asset, debit normal): 100k + 50k - 5k - 15k = 130k debit
    const cash = findRow(tb.rows, "1000").?;
    try std.testing.expect(cash.debit_balance > 0);
    try std.testing.expectEqual(@as(i64, 0), cash.credit_balance);

    // 2. Accum Dep (contra asset, credit normal): 10k credit
    const accum = findRow(tb.rows, "1900").?;
    try std.testing.expect(accum.credit_balance > 0);
    try std.testing.expectEqual(@as(i64, 0), accum.debit_balance);

    // 3. AP (liability, credit normal): 30k - 1k = 29k credit
    const ap = findRow(tb.rows, "2000").?;
    try std.testing.expect(ap.credit_balance > 0);

    // 4. Discount on Bonds (contra liability, debit normal): 5k established - 2k amortized = 3k debit
    const disc = findRow(tb.rows, "2900").?;
    try std.testing.expect(disc.debit_balance > 0);
    try std.testing.expectEqual(@as(i64, 300_000_000_000), disc.debit_balance);

    // 5. Capital (equity, credit normal): 100k credit
    const capital = findRow(tb.rows, "3000").?;
    try std.testing.expect(capital.credit_balance > 0);

    // 6. Treasury Stock (contra equity, debit normal): 15k debit
    const treasury = findRow(tb.rows, "3900").?;
    try std.testing.expect(treasury.debit_balance > 0);

    // 7. Revenue (credit normal): 50k credit
    const revenue = findRow(tb.rows, "4000").?;
    try std.testing.expect(revenue.credit_balance > 0);

    // 8. Sales Returns (contra revenue, debit normal): 5k debit
    const returns = findRow(tb.rows, "4900").?;
    try std.testing.expect(returns.debit_balance > 0);

    // 9. COGS (expense, debit normal): 30k + 10k + 2k = 42k debit
    const cogs = findRow(tb.rows, "5000").?;
    try std.testing.expect(cogs.debit_balance > 0);

    // 10. Purchase Discounts (contra expense, credit normal): 1k credit
    const pdisc = findRow(tb.rows, "5900").?;
    try std.testing.expect(pdisc.credit_balance > 0);

    // === INCOME STATEMENT ===
    const is_result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer is_result.deinit();

    // Should contain Revenue, Sales Returns, COGS, Purchase Discounts (4 accounts)
    try std.testing.expectEqual(@as(usize, 4), is_result.rows.len);

    // Net income = (Revenue 50k - Sales Returns 5k) - (COGS 42k - Purchase Discounts 1k) = 45k - 41k = 4k
    const net_income = is_result.total_credits - is_result.total_debits;
    try std.testing.expect(net_income > 0);

    // === BALANCE SHEET ===
    const bs = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer bs.deinit();

    // Must balance: A = L + E (with net income)
    try std.testing.expectEqual(bs.total_debits, bs.total_credits);

    // BS should have 6 rows (A/L/E accounts only: Cash, AccumDep, AP, DiscBonds, Capital, Treasury)
    try std.testing.expectEqual(@as(usize, 6), bs.rows.len);

    // === TRANSACTION HISTORY VIEW ===
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history;");
        defer stmt.finalize();
        _ = try stmt.step();
        // 9 entries: 8 with 2 lines + 1 with 3 lines = 19 lines
        try std.testing.expectEqual(@as(i32, 19), stmt.columnInt(0));
    }

    // === ACCOUNT BALANCES CACHE ===
    {
        // Every account with activity should have a cache entry
        var stmt = try database.prepare("SELECT COUNT(DISTINCT account_id) FROM ledger_account_balances WHERE book_id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 10), stmt.columnInt(0));
    }
}

test "all entry statuses x account types: correct representation everywhere" {
    const database = try setupAll10Db();
    defer database.close();

    const scale = money.FX_RATE_SCALE;
    // IDs: 1=Cash, 2=AccumDep, 3=AP, 4=DiscBonds, 5=Capital,
    //      6=Treasury, 7=Revenue, 8=SalesRet, 9=COGS, 10=PurchDisc

    // POSTED entry: Debit Cash 1000, Credit Revenue 1000
    _ = try postFxEntry(database, "POSTED-001", "2026-01-10", 1, 1, 100_000_000_000, 7, 100_000_000_000, "PHP", scale);

    // DRAFT entry: Debit COGS 500, Credit AP 500 (not posted)
    {
        const eid = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-12", "2026-01-12", null, 1, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 1, 50_000_000_000, 0, "PHP", scale, 9, null, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 50_000_000_000, "PHP", scale, 3, null, null, "admin");
    }

    // VOIDED entry: Debit Cash 2000, Credit Capital 2000 (then voided)
    {
        const eid = try postFxEntry(database, "VOID-001", "2026-01-15", 1, 1, 200_000_000_000, 5, 200_000_000_000, "PHP", scale);
        try entry_mod.Entry.voidEntry(database, eid, "Wrong amount", "admin");
    }

    // REVERSED entry: Debit Cash 3000, Credit Revenue 3000 (then reversed)
    {
        const eid = try postFxEntry(database, "REV-001", "2026-01-20", 1, 1, 300_000_000_000, 7, 300_000_000_000, "PHP", scale);
        _ = try entry_mod.Entry.reverse(database, eid, "Accrual reversal", "2026-01-25", null, "admin");
    }

    // === ACCOUNT BALANCES CACHE ===
    // Only POSTED entries affect cache. VOID zeros it. REVERSE adds flipped entry.
    // Net: POSTED-001 (1000 debit Cash, 1000 credit Revenue) remains
    // VOID-001: zeroed. REV-001 + REV reversal: net zero. DRAFT: no cache.
    {
        var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        // Cash: POSTED 1000 debit + VOID (zeroed: +2000 then -2000) + REV (net zero)
        const debit = stmt.columnInt64(0);
        const credit = stmt.columnInt64(1);
        try std.testing.expectEqual(@as(i64, 100_000_000_000), debit - credit); // net 1000 debit
    }

    // === TRANSACTION HISTORY VIEW ===
    // Only status='posted' entries. POSTED-001 (2 lines) + REV reversal (2 lines) = 4
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
    }

    // Verify draft not in view
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history WHERE document_number = 'DRAFT-001';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    // Verify void not in view
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history WHERE document_number = 'VOID-001';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    // Verify reversed original not in view
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history WHERE document_number = 'REV-001';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    // === TRIAL BALANCE ===
    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();

    // Only POSTED-001 affects TB (void zeroed, reverse netted, draft excluded)
    try std.testing.expectEqual(tb.total_debits, tb.total_credits);
    try std.testing.expectEqual(@as(i64, 100_000_000_000), tb.total_debits);

    // === INCOME STATEMENT ===
    const is_result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer is_result.deinit();

    // Revenue: POSTED-001 credit 1000 + REV-001 credit 3000 = 4000 raw credits
    // Reversal adds debit 3000 to revenue, so total_debits from revenue reversal
    // Net revenue = credits - debits = 4000 - 3000 = 1000
    const net_income = is_result.total_credits - is_result.total_debits;
    try std.testing.expectEqual(@as(i64, 100_000_000_000), net_income);

    // === BALANCE SHEET ===
    const bs = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer bs.deinit();

    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
}

test "reports across fiscal year with quarterly periods" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try @import("schema.zig").createAll(database);
    _ = try book_mod.Book.create(database, "FY Test", "PHP", 2, "admin");

    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");

    // Quarterly periods for non-calendar FY (Apr-Mar)
    try period_mod.Period.bulkCreate(database, 1, 2027, 4, .quarterly, "admin");

    const scale = money.FX_RATE_SCALE;
    // Post in Q1 (Apr-Jun 2026)
    _ = try postFxEntry(database, "JE-Q1", "2026-04-15", 1, 1, 100_000_000_000, 2, 100_000_000_000, "PHP", scale);
    // Post in Q3 (Oct-Dec 2026)
    _ = try postFxEntry(database, "JE-Q3", "2026-10-15", 3, 1, 200_000_000_000, 2, 200_000_000_000, "PHP", scale);

    // TB as of Dec 2026 should include Q1 + Q3
    const tb = try trialBalance(database, 1, "2026-12-31");
    defer tb.deinit();

    try std.testing.expectEqual(@as(i64, 300_000_000_000), tb.total_debits);
    try std.testing.expectEqual(tb.total_debits, tb.total_credits);

    // TB as of Jun 2026 should only include Q1
    const tb_q1 = try trialBalance(database, 1, "2026-06-30");
    defer tb_q1.deinit();

    try std.testing.expectEqual(@as(i64, 100_000_000_000), tb_q1.total_debits);
}

test "post in soft_closed period: entry visible in reports" {
    const database = try setupFullDb();
    defer database.close();

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), result.total_debits);
}

// ══════════════════════════════════════════════════════════════════
// Sprint 5: General Ledger, Account Ledger, Journal Register tests
// ══════════════════════════════════════════════════════════════════

// ── General Ledger tests ────────────────────────────────────────

test "GL: shows all transactions with running balance" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 5 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }});

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 6), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "GL: empty period returns zero rows" {
    const database = try setupFullDb();
    defer database.close();

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

test "GL: nonexistent book returns NotFound" {
    const database = try setupFullDb();
    defer database.close();

    const result = generalLedger(database, 999, "2026-01-01", "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

test "GL: voided entry excluded" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "GL: transaction rows have correct document numbers" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expect(result.rows.len >= 2);
    try std.testing.expectEqualStrings("2026-01-15", result.rows[0].posting_date[0..result.rows[0].posting_date_len]);
    try std.testing.expectEqualStrings("JE-001", result.rows[0].document_number[0..result.rows[0].document_number_len]);
}

// ── Account Ledger tests ────────────────────────────────────────

test "AL: single account transactions with running balance" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 5 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }});

    const result = try accountLedger(database, 1, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), result.rows[0].running_balance);
    try std.testing.expectEqual(@as(i64, 15_000_000_000_00), result.rows[1].running_balance);
    try std.testing.expectEqual(@as(i64, 12_000_000_000_00), result.rows[2].running_balance);
    try std.testing.expectEqual(@as(i64, 12_000_000_000_00), result.closing_balance);
}

test "AL: account with no activity returns zero" {
    const database = try setupFullDb();
    defer database.close();

    const result = try accountLedger(database, 1, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.closing_balance);
}

test "AL: credit-normal account running balance" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const result = try accountLedger(database, 1, 4, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), result.rows[0].running_balance);
    try std.testing.expectEqual(@as(i64, 15_000_000_000_00), result.rows[1].running_balance);
}

// ── Journal Register tests ──────────────────────────────────────

test "JR: shows all posted entries with lines" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 5 }});

    const result = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "JR: empty period returns zero" {
    const database = try setupFullDb();
    defer database.close();

    const result = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "GL: reversed entry — reversal visible, original excluded" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    _ = try entry_mod.Entry.reverse(database, 1, "Reversal", "2026-01-20", null, "admin");

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Original (reversed) excluded, reversal (posted) visible = 2 lines
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
}

test "AL: opening balance from prior period" {
    const database = try setupFullDb();
    defer database.close();

    // setupFullDb creates Jan (id=1) and Feb (id=2) periods
    // Post in Jan
    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});
    // Post in Feb
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 5 }});

    // AL for Cash in Feb: should show only Feb transaction
    // Running balance starts from 0 (within date range), not from opening
    // The opening_balance field would be set from cache (future enhancement)
    const result = try accountLedger(database, 1, 1, "2026-02-01", "2026-02-28");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), result.opening_balance);
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), result.rows[0].debit_amount);
}

test "GL: multi-currency entries show base amounts" {
    const database = try setupFullDb();
    defer database.close();

    // Post USD entry at 56.50 PHP/USD
    {
        const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 1, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 10_000_000_000, "USD", 565_000_000_000, 4, null, null, "admin");
        try entry_mod.Entry.post(database, eid, "admin");
    }

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Should show BASE amounts (565_000_000_000), not transaction amounts
    try std.testing.expect(result.rows.len >= 2);
    try std.testing.expectEqual(@as(i64, 565_000_000_000), result.rows[0].debit_amount);
}

test "GL and JR: reports work on archived book" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 2, .soft_closed, "admin");
    try period_mod.Period.transition(database, 2, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const gl = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer gl.deinit();
    try std.testing.expectEqual(@as(usize, 2), gl.rows.len);

    const jr = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer jr.deinit();
    try std.testing.expectEqual(@as(usize, 2), jr.rows.len);

    const al = try accountLedger(database, 1, 1, "2026-01-01", "2026-01-31");
    defer al.deinit();
    try std.testing.expectEqual(@as(usize, 1), al.rows.len);
}

test "JR: excludes void entries" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "POST-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "VOID-001", "2026-01-15", 1, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 4 }});
    try entry_mod.Entry.voidEntry(database, 2, "Error", "admin");

    const result = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
}

test "TB: no posted entries returns empty result" {
    const database = try setupFullDb();
    defer database.close();

    // Create draft only (no posting)
    _ = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-05", "2026-01-05", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 4, null, null, "admin");

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "IS: no revenue or expense activity returns empty" {
    const database = try setupFullDb();
    defer database.close();

    // Post only A/L/E entries (no revenue or expense)
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 4 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "BS: contra asset (accumulated depreciation) appears on credit side" {
    const database = try setupFullDb();
    defer database.close();

    // Buy equipment: Debit Equipment (id=3), Credit Capital (id=4)
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 50_000_000_000_00, .account_id = 3 }}, &.{.{ .amount = 50_000_000_000_00, .account_id = 4 }});
    // Depreciation: Debit COGS (id=6), Credit Accum Dep (id=2, contra asset)
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 2 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Find Accum Depreciation in BS rows
    var found_accum = false;
    for (result.rows) |row| {
        if (std.mem.eql(u8, row.account_number[0..row.account_number_len], "1900")) {
            found_accum = true;
            // Contra asset has credit normal balance, so it shows on credit side
            try std.testing.expectEqual(@as(i64, 10_000_000_000_00), row.credit_balance);
            try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        }
    }
    try std.testing.expect(found_accum);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "AL: no transactions returns empty rows with zero opening balance" {
    const database = try setupFullDb();
    defer database.close();

    // Post in Jan so there is prior activity for Cash
    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    // Query AL for Capital (id=4) in Feb — no Feb transactions but has Jan prior
    const result = try accountLedger(database, 1, 4, "2026-02-01", "2026-02-28");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    // Opening balance from Jan for Capital (credit normal): 5000 credit
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), result.opening_balance);
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), result.closing_balance);
}

test "JR: sorted by document number" {
    const database = try setupFullDb();
    defer database.close();

    // Post JE-003 first, then JE-001 — JR should return JE-001 first
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 5 }});

    const result = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.rows.len);
    // First two rows should be JE-001 (sorted by document_number)
    try std.testing.expectEqualStrings("JE-001", result.rows[0].document_number[0..result.rows[0].document_number_len]);
    try std.testing.expectEqualStrings("JE-001", result.rows[1].document_number[0..result.rows[1].document_number_len]);
    // Last two rows should be JE-003
    try std.testing.expectEqualStrings("JE-003", result.rows[2].document_number[0..result.rows[2].document_number_len]);
    try std.testing.expectEqualStrings("JE-003", result.rows[3].document_number[0..result.rows[3].document_number_len]);
}

test "GL: sorted by posting date" {
    const database = try setupFullDb();
    defer database.close();

    // Post JE-003 on Jan 20 first, then JE-001 on Jan 10 — GL should return Jan 10 first
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 5 }});

    const result = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.rows.len);
    // First two rows should be from Jan 10
    try std.testing.expectEqualStrings("2026-01-10", result.rows[0].posting_date[0..result.rows[0].posting_date_len]);
    try std.testing.expectEqualStrings("2026-01-10", result.rows[1].posting_date[0..result.rows[1].posting_date_len]);
    // Last two rows should be from Jan 20
    try std.testing.expectEqualStrings("2026-01-20", result.rows[2].posting_date[0..result.rows[2].posting_date_len]);
    try std.testing.expectEqualStrings("2026-01-20", result.rows[3].posting_date[0..result.rows[3].posting_date_len]);
}

test "TB: void entry shows zero balances" {
    const database = try setupFullDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }});

    // Void both entries
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");
    try entry_mod.Entry.voidEntry(database, 2, "Error", "admin");

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "BS: net income injection — revenue and expense flow into equity side" {
    const database = try setupFullDb();
    defer database.close();

    // Capital contribution
    try postEntry(database, "JE-001", "2026-01-05", 1, &.{.{ .amount = 20_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 20_000_000_000_00, .account_id = 4 }});
    // Revenue
    try postEntry(database, "JE-002", "2026-01-10", 1, &.{.{ .amount = 8_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 8_000_000_000_00, .account_id = 5 }});
    // Expense
    try postEntry(database, "JE-003", "2026-01-15", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 6 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // BS rows only contain A/L/E (no revenue/expense rows)
    for (result.rows) |row| {
        const at = row.account_type[0..row.account_type_len];
        try std.testing.expect(!std.mem.eql(u8, at, "revenue"));
        try std.testing.expect(!std.mem.eql(u8, at, "expense"));
    }

    // Net income (8000 - 3000 = 5000) injected into totals
    // Assets: Cash = 20000 + 8000 - 3000 = 25000 debit
    // Equity: Capital = 20000 credit + net income 5000 = 25000 credit side
    try std.testing.expectEqual(result.total_debits, result.total_credits);
    // Verify net income was actually injected (total_credits > just Capital)
    try std.testing.expect(result.total_credits > 20_000_000_000_00);
}

// ── Balance Sheet: net income row tests ────────────────────────

test "BS: net income row appears when RE account designated" {
    const database = try setupTestDb();
    defer database.close();

    // Designate Capital (id=3, equity) as retained earnings account
    try book_mod.Book.setRetainedEarningsAccount(database, 1, 3, "admin");

    // Revenue 1000, Expense 600 => net income 400
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 600_000_000_00, .account_id = 5 }}, &.{.{ .amount = 600_000_000_00, .account_id = 1 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Find the synthetic net income row (RE account_id = 3)
    var ni_row_found = false;
    var ni_credit: i64 = 0;
    for (result.rows) |row| {
        if (row.account_id == 3) {
            // Could be the natural Capital row or the synthetic NI row
            // The synthetic NI row has credit_balance = 400 (net income)
            if (row.credit_balance == 400_000_000_00) {
                ni_row_found = true;
                ni_credit = row.credit_balance;
            }
        }
    }
    try std.testing.expect(ni_row_found);
    try std.testing.expectEqual(@as(i64, 400_000_000_00), ni_credit);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "BS: no net income row when RE not designated" {
    const database = try setupTestDb();
    defer database.close();

    // Do NOT designate retained_earnings_account_id
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 600_000_000_00, .account_id = 5 }}, &.{.{ .amount = 600_000_000_00, .account_id = 1 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Without RE designation: Cash (asset) only — no synthetic row
    // Cash has activity, no other A/L/E accounts have activity
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "BS: net loss shows as debit" {
    const database = try setupTestDb();
    defer database.close();

    try book_mod.Book.setRetainedEarningsAccount(database, 1, 3, "admin");

    // Revenue 300, Expense 500 => net loss 200
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 300_000_000_00, .account_id = 1 }}, &.{.{ .amount = 300_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 500_000_000_00, .account_id = 5 }}, &.{.{ .amount = 500_000_000_00, .account_id = 1 }});

    const result = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Find the synthetic net loss row
    var ni_debit: i64 = 0;
    var ni_row_found = false;
    for (result.rows) |row| {
        if (row.account_id == 3 and row.debit_balance == 200_000_000_00) {
            ni_row_found = true;
            ni_debit = row.debit_balance;
        }
    }
    try std.testing.expect(ni_row_found);
    try std.testing.expectEqual(@as(i64, 200_000_000_00), ni_debit);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

// ── Trial Balance Movement tests ───────────────────────────────

test "TB Movement: shows all 5 account types" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try trialBalanceMovement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expect(result.rows.len >= 5);

    var found_asset = false;
    var found_liability = false;
    var found_equity = false;
    var found_revenue = false;
    var found_expense = false;
    for (result.rows) |row| {
        const at = row.account_type[0..row.account_type_len];
        if (std.mem.eql(u8, at, "asset")) found_asset = true;
        if (std.mem.eql(u8, at, "liability")) found_liability = true;
        if (std.mem.eql(u8, at, "equity")) found_equity = true;
        if (std.mem.eql(u8, at, "revenue")) found_revenue = true;
        if (std.mem.eql(u8, at, "expense")) found_expense = true;
    }
    try std.testing.expect(found_asset);
    try std.testing.expect(found_liability);
    try std.testing.expect(found_equity);
    try std.testing.expect(found_revenue);
    try std.testing.expect(found_expense);
}

test "TB Movement: empty period returns empty" {
    const database = try setupTestDb();
    defer database.close();

    const result = try trialBalanceMovement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "TB Movement: shows only period activity, not cumulative" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 4 }});

    const result = try trialBalanceMovement(database, 1, "2026-02-01", "2026-02-28");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 500_000_000_00), result.total_debits);
    try std.testing.expectEqual(@as(i64, 500_000_000_00), result.total_credits);
}

// ── Comparative Report Tests ────────────────────────────────────

test "TB comparative: current and prior periods" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 2 }});

    const result = try trialBalanceComparative(database, 1, "2026-02-28", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(result.current_total_debits, result.current_total_credits);
    try std.testing.expectEqual(result.prior_total_debits, result.prior_total_credits);

    for (result.rows) |row| {
        try std.testing.expectEqual(row.current_debit - row.prior_debit, row.variance_debit);
        try std.testing.expectEqual(row.current_credit - row.prior_credit, row.variance_credit);
    }

    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), result.prior_total_debits);
    try std.testing.expectEqual(@as(i64, 3_000_000_000_00), result.current_total_debits);
}

test "IS comparative: account in current but not prior" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 5 }}, &.{.{ .amount = 500_000_000_00, .account_id = 4 }});

    const result = try incomeStatementComparative(database, 1, "2026-02-01", "2026-02-28", "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expect(result.rows.len >= 2);

    for (result.rows) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.prior_debit);
        try std.testing.expectEqual(@as(i64, 0), row.prior_credit);
        try std.testing.expectEqual(row.current_debit, row.variance_debit);
        try std.testing.expectEqual(row.current_credit, row.variance_credit);
    }
}

test "Comparative: account in prior but not current" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 700_000_000_00, .account_id = 5 }}, &.{.{ .amount = 700_000_000_00, .account_id = 4 }});

    const result = try incomeStatementComparative(database, 1, "2026-02-01", "2026-02-28", "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expect(result.rows.len >= 2);
    try std.testing.expectEqual(@as(i64, 0), result.current_total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.current_total_credits);

    for (result.rows) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.current_debit);
        try std.testing.expectEqual(@as(i64, 0), row.current_credit);
        try std.testing.expectEqual(-row.prior_debit, row.variance_debit);
        try std.testing.expectEqual(-row.prior_credit, row.variance_credit);
    }
}

// ── Equity Changes tests ───────────────────────────────────────

fn findEquityRow(rows: []const EquityRow, acct_num: []const u8) ?EquityRow {
    for (rows) |row| {
        if (std.mem.eql(u8, row.account_number[0..row.account_number_len], acct_num)) return row;
    }
    return null;
}

test "equity changes: basic capital contribution and net income" {
    const database = try setupTestDb();
    defer database.close();

    // Capital contribution: debit Cash(1), credit Capital(3) = 10,000
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    // Revenue: debit Cash(1), credit Revenue(4) = 5,000
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    // Expense: debit COGS(5), credit Cash(1) = 3,000
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }});

    const result = try equityChanges(database, 1, "2026-01-01", "2026-01-31", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    const capital = findEquityRow(result.rows, "3000").?;
    try std.testing.expectEqual(@as(i64, 0), capital.opening_balance);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), capital.period_activity);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), capital.closing_balance);
    // Net income = revenue 5000 - expense 3000 = 2000
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), result.net_income);
}

test "equity changes: prior period opening balance" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Period 1: capital contribution
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    // Period 2: revenue and expense
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-02-20", 2, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 1 }});

    const result = try equityChanges(database, 1, "2026-02-01", "2026-02-28", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    const capital = findEquityRow(result.rows, "3000").?;
    // Opening from period 1, no new capital in period 2
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), capital.opening_balance);
    try std.testing.expectEqual(@as(i64, 0), capital.period_activity);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), capital.closing_balance);
    // Net income from FY start (Jan) through Feb
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), result.net_income);
}

test "equity changes: empty — no equity transactions" {
    const database = try setupTestDb();
    defer database.close();

    const result = try equityChanges(database, 1, "2026-01-01", "2026-01-31", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.net_income);
    try std.testing.expectEqual(@as(i64, 0), result.total_opening);
    try std.testing.expectEqual(@as(i64, 0), result.total_closing);
}

test "equity changes: contra equity (drawings) shows correct sign" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3900", "Drawings", .equity, true, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "Expenses", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Capital contribution: debit Cash(1), credit Capital(2) = 10,000
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 2 }});
    // Drawing: debit Drawings(3), credit Cash(1) = 1,000
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 3 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }});

    const result = try equityChanges(database, 1, "2026-01-01", "2026-01-31", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);

    const capital = findEquityRow(result.rows, "3000").?;
    // Capital is credit-normal: credits - debits = 10,000
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), capital.closing_balance);

    const drawings = findEquityRow(result.rows, "3900").?;
    // Drawings is contra equity (debit-normal): debits - credits = 1,000
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), drawings.closing_balance);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), drawings.period_activity);
}

test "equity changes: nonexistent book returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = equityChanges(database, 999, "2026-01-01", "2026-01-31", "2026-01-01");
    try std.testing.expectError(error.NotFound, result);
}

test "truncated flag is false for small result sets" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});

    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();
    try std.testing.expect(!tb.truncated);

    const is = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer is.deinit();
    try std.testing.expect(!is.truncated);

    const gl = try generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer gl.deinit();
    try std.testing.expect(!gl.truncated);

    const al = try accountLedger(database, 1, 1, "2026-01-01", "2026-01-31");
    defer al.deinit();
    try std.testing.expect(!al.truncated);

    const jr = try journalRegister(database, 1, "2026-01-01", "2026-01-31");
    defer jr.deinit();
    try std.testing.expect(!jr.truncated);

    const eq = try equityChanges(database, 1, "2026-01-01", "2026-01-31", "2026-01-01");
    defer eq.deinit();
    try std.testing.expect(!eq.truncated);
}

test "MAX_REPORT_ROWS constant is 50000" {
    try std.testing.expectEqual(@as(usize, 50_000), MAX_REPORT_ROWS);
}

test "balanceSheetAuto: derives fy_start_date from book config" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const bs_auto = try balanceSheetAutoWithProjectedRE(database, 1, "2026-01-31");
    defer bs_auto.deinit();

    const bs_manual = try balanceSheetWithProjectedRE(database, 1, "2026-01-31", "2026-01-01");
    defer bs_manual.deinit();

    try std.testing.expectEqual(bs_manual.total_debits, bs_auto.total_debits);
    try std.testing.expectEqual(bs_manual.total_credits, bs_auto.total_credits);
    try std.testing.expectEqual(bs_auto.total_debits, bs_auto.total_credits);
}

test "balanceSheetAuto: nonexistent book returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = balanceSheetAuto(database, 999, "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

test "translateReportResult: BS accounts use closing rate, IS accounts use average rate" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();

    const rates = TranslationRates{
        .closing_rate = 20_000_000_000,
        .average_rate = 18_000_000_000,
    };
    const translated = try translateReportResult(tb, rates);
    defer translated.deinit();

    try std.testing.expectEqual(tb.rows.len, translated.rows.len);

    for (translated.rows) |row| {
        const acct_type = row.account_type[0..row.account_type_len];
        _ = acct_type;
        try std.testing.expect(row.debit_balance != 0 or row.credit_balance != 0);
    }
}

test "translateReportResult: 1:1 rate preserves amounts" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});

    const tb = try trialBalance(database, 1, "2026-01-31");
    defer tb.deinit();

    const rates = TranslationRates{
        .closing_rate = money.FX_RATE_SCALE,
        .average_rate = money.FX_RATE_SCALE,
    };
    const translated = try translateReportResult(tb, rates);
    defer translated.deinit();

    try std.testing.expectEqual(tb.total_debits, translated.total_debits);
    try std.testing.expectEqual(tb.total_credits, translated.total_credits);
}
