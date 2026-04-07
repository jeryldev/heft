// Characterization tests — the safety net for Sprint C rewrite.
//
// These tests lock down the CURRENT behavior of the engine with exact
// numerical assertions. The rewrite in Sprint C changes the cache
// semantics ("period movement" → "cumulative position") and introduces
// opening entries. If Sprint C preserves all assertions here, the
// refactor is semantically equivalent.
//
// Strategy: "measure first, assert second." Values were computed by
// hand from the deterministic 2-year scenario in
// .research/24-two-year-characterization-scenario.md, cross-checked
// against a dry run of the current engine.
//
// When adding new assertions: run the test, copy the actual value from
// the error message into the expected slot, re-run to confirm green.
//
// Scenario (per .research/24):
//   - Calendar year 2026 + 2027
//   - 8 accounts: cash, AR, equipment, AP, capital, RE, revenue, expense
//   - 11 transactions in Year 1 (Jan: 5, Feb: 3, Mar: 3)
//   - Year-end close closes all 12 periods
//   - Year 2 continues with additional transactions

const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const close_mod = @import("close.zig");
const report_mod = @import("report.zig");
const verify_mod = @import("verify.zig");
const money = @import("money.zig");

// ── Corporation scenario (single-account, direct close) ────────

const CorpScenario = struct {
    database: db.Database,
    book_id: i64,
    // accounts
    cash: i64,
    ar: i64,
    equipment: i64,
    ap: i64,
    capital: i64,
    re: i64,
    revenue: i64,
    expense: i64,
    // periods (12 for 2026)
    periods: [12]i64,
};

fn setupCorporationScenario() !CorpScenario {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Acme Corp", "PHP", 2, "admin");
    // Entity type defaults to corporation — no setEntityType needed

    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const ar = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const equipment = try account_mod.Account.create(database, book_id, "1500", "Equipment", .asset, false, "admin");
    const ap = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "admin");
    const capital = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const re = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Sales Revenue", .revenue, false, "admin");
    const expense = try account_mod.Account.create(database, book_id, "5000", "Operating Expenses", .expense, false, "admin");

    try book_mod.Book.setEquityCloseTarget(database, book_id, re, "admin");

    try period_mod.Period.bulkCreate(database, book_id, 2026, 1, .monthly, "admin");

    var periods: [12]i64 = undefined;
    {
        var stmt = try database.prepare(
            "SELECT id FROM ledger_periods WHERE book_id = ? AND year = 2026 ORDER BY period_number ASC;",
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) : (idx += 1) {
            if (idx < 12) periods[idx] = stmt.columnInt64(0);
        }
    }

    return .{
        .database = database,
        .book_id = book_id,
        .cash = cash,
        .ar = ar,
        .equipment = equipment,
        .ap = ap,
        .capital = capital,
        .re = re,
        .revenue = revenue,
        .expense = expense,
        .periods = periods,
    };
}

fn postSimple2LineEntry(database: db.Database, book_id: i64, period_id: i64, doc: []const u8, date: []const u8, dr_account: i64, cr_account: i64, amount: i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, book_id, doc, date, date, null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, amount, 0, "PHP", money.FX_RATE_SCALE, dr_account, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, amount, "PHP", money.FX_RATE_SCALE, cr_account, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
}

// Amounts (all scaled × 10^8 = ×100_000_000)
// 100,000.00 = 10_000_000_000_000
//  30,000.00 =  3_000_000_000_000
//  15,000.00 =  1_500_000_000_000
//  10,000.00 =  1_000_000_000_000
//   8,000.00 =    800_000_000_000
//  20,000.00 =  2_000_000_000_000
//   5,000.00 =    500_000_000_000
//  12,000.00 =  1_200_000_000_000
//   3,000.00 =    300_000_000_000

const AMT_100K: i64 = 10_000_000_000_000;
const AMT_30K: i64 = 3_000_000_000_000;
const AMT_15K: i64 = 1_500_000_000_000;
const AMT_10K: i64 = 1_000_000_000_000;
const AMT_8K: i64 = 800_000_000_000;
const AMT_20K: i64 = 2_000_000_000_000;
const AMT_5K: i64 = 500_000_000_000;
const AMT_12K: i64 = 1_200_000_000_000;
const AMT_3K: i64 = 300_000_000_000;

fn postYear1Transactions(s: CorpScenario) !void {
    // Y1-E1: Capital contribution — Jan 2
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-001", "2026-01-02", s.cash, s.capital, AMT_100K);
    // Y1-E2: Equipment purchase — Jan 5
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "EQP-001", "2026-01-05", s.equipment, s.cash, AMT_30K);
    // Y1-E3: Sale on credit — Jan 10
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-001", "2026-01-10", s.ar, s.revenue, AMT_15K);
    // Y1-E4: Cash sale — Jan 20
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-002", "2026-01-20", s.cash, s.revenue, AMT_10K);
    // Y1-E5: Operating expense cash — Jan 25
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "EXP-001", "2026-01-25", s.expense, s.cash, AMT_8K);
    // === Jan close point — 5 entries ===

    // Y1-E6: Customer payment on Y1-E3 — Feb 10
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "PAY-001", "2026-02-10", s.cash, s.ar, AMT_15K);
    // Y1-E7: Revenue on account — Feb 15
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "SALE-003", "2026-02-15", s.ar, s.revenue, AMT_20K);
    // Y1-E8: Expense on account — Feb 20
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "EXP-002", "2026-02-20", s.expense, s.ap, AMT_5K);
    // === Feb close point — 8 entries total ===

    // Y1-E9: Cash sale — Mar 5
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "SALE-004", "2026-03-05", s.cash, s.revenue, AMT_12K);
    // Y1-E10: Pay supplier — Mar 15
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "PAY-002", "2026-03-15", s.ap, s.cash, AMT_5K);
    // Y1-E11: Operating expense — Mar 25
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "EXP-003", "2026-03-25", s.expense, s.cash, AMT_3K);
}

fn queryCacheBalance(database: db.Database, account_id: i64, period_id: i64) !struct { debit: i64, credit: i64 } {
    var stmt = try database.prepare(
        "SELECT COALESCE(debit_sum, 0), COALESCE(credit_sum, 0) FROM ledger_account_balances WHERE account_id = ? AND period_id = ?;",
    );
    defer stmt.finalize();
    try stmt.bindInt(1, account_id);
    try stmt.bindInt(2, period_id);
    if (try stmt.step()) {
        return .{ .debit = stmt.columnInt64(0), .credit = stmt.columnInt64(1) };
    }
    return .{ .debit = 0, .credit = 0 };
}

fn findRow(rows: []const report_mod.ReportRow, account_id: i64) ?report_mod.ReportRow {
    for (rows) |row| {
        if (row.account_id == account_id) return row;
    }
    return null;
}

// ─────────────────────────────────────────────────────────────
// B.1 CORPORATION — direct close characterization
// ─────────────────────────────────────────────────────────────

test "CHAR corp: Jan cache after first entry (Y1-E1 Capital contribution)" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-001", "2026-01-02", s.cash, s.capital, AMT_100K);

    const cash_bal = try queryCacheBalance(s.database, s.cash, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_100K), cash_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), cash_bal.credit);

    const cap_bal = try queryCacheBalance(s.database, s.capital, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), cap_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_100K), cap_bal.credit);
}

test "CHAR corp: Jan cache after all 5 entries (pre-close)" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    // Cash: +100k E1, -30k E2, +10k E4, -8k E5 → 110k debit, 38k credit
    const cash_bal = try queryCacheBalance(s.database, s.cash, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 110 * 1_000_000_000_00), cash_bal.debit);
    try std.testing.expectEqual(@as(i64, 38 * 1_000_000_000_00), cash_bal.credit);

    // AR: +15k E3, only
    const ar_bal = try queryCacheBalance(s.database, s.ar, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_15K), ar_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), ar_bal.credit);

    // Equipment: +30k E2
    const eq_bal = try queryCacheBalance(s.database, s.equipment, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_30K), eq_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), eq_bal.credit);

    // Capital: +100k E1
    const cap_bal = try queryCacheBalance(s.database, s.capital, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), cap_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_100K), cap_bal.credit);

    // Revenue: +15k E3, +10k E4 → 25k credit
    const rev_bal = try queryCacheBalance(s.database, s.revenue, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.debit);
    try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), rev_bal.credit);

    // Expense: +8k E5
    const exp_bal = try queryCacheBalance(s.database, s.expense, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_8K), exp_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.credit);
}

test "CHAR corp: Jan Income Statement pre-close" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const is_report = try report_mod.incomeStatement(s.database, s.book_id, "2026-01-01", "2026-01-31");
    defer is_report.deinit();

    try std.testing.expectEqual(@as(usize, 2), is_report.rows.len);
    // Revenue 25k credit, Expense 8k debit → Net income = 17k
    try std.testing.expectEqual(@as(i64, AMT_8K), is_report.total_debits);
    try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), is_report.total_credits);
}

test "CHAR corp: Jan Trial Balance pre-close (cumulative)" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const tb = try report_mod.trialBalance(s.database, s.book_id, "2026-01-31");
    defer tb.deinit();

    // All 6 accounts with activity (Cash, AR, Equipment, Capital, Revenue, Expense — no AP, RE)
    try std.testing.expectEqual(@as(usize, 6), tb.rows.len);

    // Cash: 72k debit (110k-38k)
    if (findRow(tb.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 72 * 1_000_000_000_00), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // AR: 15k debit
    if (findRow(tb.rows, s.ar)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_15K), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Equipment: 30k debit
    if (findRow(tb.rows, s.equipment)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_30K), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Capital: 100k credit
    if (findRow(tb.rows, s.capital)) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        try std.testing.expectEqual(@as(i64, AMT_100K), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Revenue: 25k credit
    if (findRow(tb.rows, s.revenue)) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Expense: 8k debit
    if (findRow(tb.rows, s.expense)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_8K), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Totals balance: Dr 72+15+30+8 = 125k; Cr 100+25 = 125k
    try std.testing.expectEqual(@as(i64, 125 * 1_000_000_000_00), tb.total_debits);
    try std.testing.expectEqual(@as(i64, 125 * 1_000_000_000_00), tb.total_credits);
}

test "CHAR corp: Jan close zeroes revenue/expense and credits RE net income" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    // Revenue zeroed: 25k debit (from close), 25k credit (from activity)
    const rev_bal = try queryCacheBalance(s.database, s.revenue, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), rev_bal.debit);
    try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), rev_bal.credit);

    // Expense zeroed: 8k debit (from activity), 8k credit (from close)
    const exp_bal = try queryCacheBalance(s.database, s.expense, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_8K), exp_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_8K), exp_bal.credit);

    // RE receives net income (17k): 8k debit (from expense close), 25k credit (from revenue close)
    const re_bal = try queryCacheBalance(s.database, s.re, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_8K), re_bal.debit);
    try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), re_bal.credit);
    try std.testing.expectEqual(@as(i64, 17 * 1_000_000_000_00), re_bal.credit - re_bal.debit);
}

test "CHAR corp: Jan Balance Sheet after close" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    const bs = try report_mod.balanceSheet(s.database, s.book_id, "2026-01-31", "2026-01-01");
    defer bs.deinit();

    // Cash 72k
    if (findRow(bs.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 72 * 1_000_000_000_00), row.debit_balance);
    } else return error.TestUnexpectedResult;
    // AR 15k
    if (findRow(bs.rows, s.ar)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_15K), row.debit_balance);
    } else return error.TestUnexpectedResult;
    // Equipment 30k
    if (findRow(bs.rows, s.equipment)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_30K), row.debit_balance);
    } else return error.TestUnexpectedResult;
    // Capital 100k
    if (findRow(bs.rows, s.capital)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_100K), row.credit_balance);
    } else return error.TestUnexpectedResult;
    // RE 17k (net income from close)
    if (findRow(bs.rows, s.re)) |row| {
        try std.testing.expectEqual(@as(i64, 17 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // BS balances: 72+15+30 = 117k assets; 100+17 = 117k equity
    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    try std.testing.expectEqual(@as(i64, 117 * 1_000_000_000_00), bs.total_debits);
}

test "CHAR corp: Feb cache after activity (cumulative)" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    // Cash Feb activity: +15k (E6) = Feb cache: 15k debit, 0 credit
    const cash_bal = try queryCacheBalance(s.database, s.cash, s.periods[1]);
    try std.testing.expectEqual(@as(i64, AMT_15K), cash_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), cash_bal.credit);

    // AR Feb: +20k (E7), -15k (E6) = 20k debit, 15k credit
    const ar_bal = try queryCacheBalance(s.database, s.ar, s.periods[1]);
    try std.testing.expectEqual(@as(i64, AMT_20K), ar_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_15K), ar_bal.credit);

    // AP Feb: +5k (E8) = 0 debit, 5k credit
    const ap_bal = try queryCacheBalance(s.database, s.ap, s.periods[1]);
    try std.testing.expectEqual(@as(i64, 0), ap_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_5K), ap_bal.credit);

    // Revenue Feb: +20k (E7) = 0 debit, 20k credit
    const rev_bal = try queryCacheBalance(s.database, s.revenue, s.periods[1]);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_20K), rev_bal.credit);

    // Expense Feb: +5k (E8) = 5k debit, 0 credit
    const exp_bal = try queryCacheBalance(s.database, s.expense, s.periods[1]);
    try std.testing.expectEqual(@as(i64, AMT_5K), exp_bal.debit);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.credit);
}

test "CHAR corp: Trial Balance Feb 28 cumulative across Jan and Feb" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    const tb = try report_mod.trialBalance(s.database, s.book_id, "2026-02-28");
    defer tb.deinit();

    // Cumulative cash: (Jan 110k dr, 38k cr) + (Feb 15k dr) = 125k dr, 38k cr → 87k net debit
    if (findRow(tb.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 87 * 1_000_000_000_00), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Cumulative AR: (Jan 15k dr) + (Feb 20k dr, 15k cr) = 35k dr, 15k cr → 20k net debit
    if (findRow(tb.rows, s.ar)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_20K), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Cumulative AP: 5k credit
    if (findRow(tb.rows, s.ap)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_5K), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Cumulative RE: 17k credit (from Jan close, Feb not yet closed)
    if (findRow(tb.rows, s.re)) |row| {
        try std.testing.expectEqual(@as(i64, 17 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Cumulative Revenue: Jan closed zero + Feb 20k = 20k credit
    if (findRow(tb.rows, s.revenue)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_20K), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Cumulative Expense: Jan closed zero + Feb 5k = 5k debit
    if (findRow(tb.rows, s.expense)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_5K), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Totals: Dr 87+20+30+5 = 142k; Cr 5+100+17+20 = 142k
    try std.testing.expectEqual(@as(i64, 142 * 1_000_000_000_00), tb.total_debits);
    try std.testing.expectEqual(@as(i64, 142 * 1_000_000_000_00), tb.total_credits);
}

test "CHAR corp: Mar after all activity and close" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    // Feb net income = 20k Rev - 5k Exp = 15k
    // Mar net income = 12k Rev - 3k Exp = 9k
    // Cumulative RE: 17k (Jan) + 15k (Feb) + 9k (Mar) = 41k
    const tb = try report_mod.trialBalance(s.database, s.book_id, "2026-03-31");
    defer tb.deinit();

    if (findRow(tb.rows, s.re)) |row| {
        try std.testing.expectEqual(@as(i64, 41 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;

    // Cash Mar activity: +12k E9, -5k E10, -3k E11 → 12k dr, 8k cr
    // Cumulative cash: (Jan+Feb 87k dr net) + (Mar 12k dr - 8k cr = 4k net) = 91k
    if (findRow(tb.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 91 * 1_000_000_000_00), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // AR cumulative: 20k (unchanged in Mar)
    if (findRow(tb.rows, s.ar)) |row| {
        try std.testing.expectEqual(@as(i64, AMT_20K), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // AP Mar: -5k (E10 payment) → Feb 5k + Mar -5k = 0
    if (findRow(tb.rows, s.ap)) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
    } else {
        // If AP has zero balance, it may be omitted from the TB. Either
        // behavior is acceptable for characterization.
    }

    // Revenue cumulative: all closed → 0
    if (findRow(tb.rows, s.revenue)) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
    } else {
        // Zero-balance accounts may be omitted.
    }

    // TB balances
    try std.testing.expectEqual(tb.total_debits, tb.total_credits);
}

test "CHAR corp: Year-end Balance Sheet after all 3 months closed" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    const bs = try report_mod.balanceSheet(s.database, s.book_id, "2026-03-31", "2026-01-01");
    defer bs.deinit();

    // Assets: Cash 91k + AR 20k + Equipment 30k = 141k
    // Liab + Equity: AP 0 + Capital 100k + RE 41k = 141k
    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    try std.testing.expectEqual(@as(i64, 141 * 1_000_000_000_00), bs.total_debits);

    if (findRow(bs.rows, s.re)) |row| {
        try std.testing.expectEqual(@as(i64, 41 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;
}

test "CHAR corp: verify passes after 3-period close" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    const result = try verify_mod.verify(s.database, s.book_id);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.errors);
}

// ─────────────────────────────────────────────────────────────
// B.2 SOLE PROPRIETORSHIP — single-target close to Owner's Capital
// ─────────────────────────────────────────────────────────────
//
// Same transaction scenario as corporation, but:
// - entity_type = sole_proprietorship
// - Equity close target = Owner's Capital (account 3000) instead of RE
// - No separate RE account (sole props don't have one)
// - Net income increases Owner's Capital directly

const SoleScenario = struct {
    database: db.Database,
    book_id: i64,
    cash: i64,
    ar: i64,
    equipment: i64,
    ap: i64,
    owner_capital: i64,
    revenue: i64,
    expense: i64,
    periods: [12]i64,
};

fn setupSolePropScenario() !SoleScenario {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Juan dela Cruz", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .sole_proprietorship, "admin");

    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const ar = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const equipment = try account_mod.Account.create(database, book_id, "1500", "Equipment", .asset, false, "admin");
    const ap = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "admin");
    const owner_capital = try account_mod.Account.create(database, book_id, "3000", "Owner's Capital", .equity, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Sales", .revenue, false, "admin");
    const expense = try account_mod.Account.create(database, book_id, "5000", "Operating Expenses", .expense, false, "admin");

    // Generic alias works: sets retained_earnings_account_id but the
    // account is Owner's Capital, not RE
    try book_mod.Book.setEquityCloseTarget(database, book_id, owner_capital, "admin");

    try period_mod.Period.bulkCreate(database, book_id, 2026, 1, .monthly, "admin");

    var periods: [12]i64 = undefined;
    {
        var stmt = try database.prepare(
            "SELECT id FROM ledger_periods WHERE book_id = ? AND year = 2026 ORDER BY period_number ASC;",
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) : (idx += 1) {
            if (idx < 12) periods[idx] = stmt.columnInt64(0);
        }
    }

    return .{
        .database = database,
        .book_id = book_id,
        .cash = cash,
        .ar = ar,
        .equipment = equipment,
        .ap = ap,
        .owner_capital = owner_capital,
        .revenue = revenue,
        .expense = expense,
        .periods = periods,
    };
}

fn postSoleYear1Transactions(s: SoleScenario) !void {
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-001", "2026-01-02", s.cash, s.owner_capital, AMT_100K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "EQP-001", "2026-01-05", s.equipment, s.cash, AMT_30K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-001", "2026-01-10", s.ar, s.revenue, AMT_15K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-002", "2026-01-20", s.cash, s.revenue, AMT_10K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "EXP-001", "2026-01-25", s.expense, s.cash, AMT_8K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "PAY-001", "2026-02-10", s.cash, s.ar, AMT_15K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "SALE-003", "2026-02-15", s.ar, s.revenue, AMT_20K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[1], "EXP-002", "2026-02-20", s.expense, s.ap, AMT_5K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "SALE-004", "2026-03-05", s.cash, s.revenue, AMT_12K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "PAY-002", "2026-03-15", s.ap, s.cash, AMT_5K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[2], "EXP-003", "2026-03-25", s.expense, s.cash, AMT_3K);
}

test "CHAR sole: Owner's Capital starts at 100k after initial contribution" {
    const s = try setupSolePropScenario();
    defer s.database.close();

    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-001", "2026-01-02", s.cash, s.owner_capital, AMT_100K);

    const oc_bal = try queryCacheBalance(s.database, s.owner_capital, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), oc_bal.debit);
    try std.testing.expectEqual(@as(i64, AMT_100K), oc_bal.credit);
}

test "CHAR sole: Jan close increases Owner's Capital by 17k (net income)" {
    const s = try setupSolePropScenario();
    defer s.database.close();

    try postSoleYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    // Owner's Capital: initial 100k + net income 17k = 117k cumulative credit
    // Period cache: 100k (initial) + 25k (from closing revenue) - 8k (from closing expense)
    //             = 125k cr, 8k dr = net 117k
    const oc_bal = try queryCacheBalance(s.database, s.owner_capital, s.periods[0]);
    try std.testing.expectEqual(@as(i64, AMT_8K), oc_bal.debit);
    try std.testing.expectEqual(@as(i64, 125 * 1_000_000_000_00), oc_bal.credit);
    try std.testing.expectEqual(@as(i64, 117 * 1_000_000_000_00), oc_bal.credit - oc_bal.debit);
}

test "CHAR sole: Mar BS has Owner's Capital 141k (no separate RE)" {
    const s = try setupSolePropScenario();
    defer s.database.close();

    try postSoleYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    const bs = try report_mod.balanceSheet(s.database, s.book_id, "2026-03-31", "2026-01-01");
    defer bs.deinit();

    // Owner's Capital = 100k initial + 17k + 15k + 9k = 141k (everything flows to one account)
    if (findRow(bs.rows, s.owner_capital)) |row| {
        try std.testing.expectEqual(@as(i64, 141 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;

    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    try std.testing.expectEqual(@as(i64, 141 * 1_000_000_000_00), bs.total_debits);
}

test "CHAR sole: verify passes" {
    const s = try setupSolePropScenario();
    defer s.database.close();

    try postSoleYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    const result = try verify_mod.verify(s.database, s.book_id);
    try std.testing.expect(result.passed());
}

// ─────────────────────────────────────────────────────────────
// B.3 PARTNERSHIP — allocated close (3 partners, 50/30/20)
// ─────────────────────────────────────────────────────────────

const PartScenario = struct {
    database: db.Database,
    book_id: i64,
    cash: i64,
    ar: i64,
    ap: i64,
    partner_a: i64,
    partner_b: i64,
    partner_c: i64,
    revenue: i64,
    expense: i64,
    periods: [12]i64,
};

fn setupPartnershipScenario() !PartScenario {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "ABC Partnership", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .partnership, "admin");

    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const ar = try account_mod.Account.create(database, book_id, "1100", "AR", .asset, false, "admin");
    const ap = try account_mod.Account.create(database, book_id, "2000", "AP", .liability, false, "admin");
    const partner_a = try account_mod.Account.create(database, book_id, "3101", "Partner A Capital", .equity, false, "admin");
    const partner_b = try account_mod.Account.create(database, book_id, "3102", "Partner B Capital", .equity, false, "admin");
    const partner_c = try account_mod.Account.create(database, book_id, "3103", "Partner C Capital", .equity, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const expense = try account_mod.Account.create(database, book_id, "5000", "Expenses", .expense, false, "admin");

    // 50/30/20 allocation
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_a, "Partner A", 5000, "2026-01-01", "admin");
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_b, "Partner B", 3000, "2026-01-01", "admin");
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_c, "Partner C", 2000, "2026-01-01", "admin");

    try period_mod.Period.bulkCreate(database, book_id, 2026, 1, .monthly, "admin");

    var periods: [12]i64 = undefined;
    {
        var stmt = try database.prepare(
            "SELECT id FROM ledger_periods WHERE book_id = ? AND year = 2026 ORDER BY period_number ASC;",
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) : (idx += 1) {
            if (idx < 12) periods[idx] = stmt.columnInt64(0);
        }
    }

    return .{
        .database = database,
        .book_id = book_id,
        .cash = cash,
        .ar = ar,
        .ap = ap,
        .partner_a = partner_a,
        .partner_b = partner_b,
        .partner_c = partner_c,
        .revenue = revenue,
        .expense = expense,
        .periods = periods,
    };
}

fn postPartnershipYear1(s: PartScenario) !void {
    // Partners contribute initial capital (each partner contributes proportionally)
    // 50k A, 30k B, 20k C = 100k total cash
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-A", "2026-01-01", s.cash, s.partner_a, 50 * 1_000_000_000_00);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-B", "2026-01-01", s.cash, s.partner_b, 30 * 1_000_000_000_00);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "CAP-C", "2026-01-01", s.cash, s.partner_c, 20 * 1_000_000_000_00);

    // Operating activity same shape as corp scenario (but no equipment purchase)
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-001", "2026-01-10", s.ar, s.revenue, AMT_15K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "SALE-002", "2026-01-20", s.cash, s.revenue, AMT_10K);
    try postSimple2LineEntry(s.database, s.book_id, s.periods[0], "EXP-001", "2026-01-25", s.expense, s.cash, AMT_8K);
}

test "CHAR part: Jan close splits net income 50/30/20 to partners" {
    const s = try setupPartnershipScenario();
    defer s.database.close();

    try postPartnershipYear1(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    // Net income Jan: 25k revenue - 8k expense = 17k
    // Partner A (50%): 17k × 0.50 = 8,500
    // Partner B (30%): 17k × 0.30 = 5,100
    // Partner C (20%): 17k - 8,500 - 5,100 = 3,400 (residual, includes rounding)

    // Partner A cumulative (50k initial + 8,500 allocation) = 58,500
    const a_bal = try queryCacheBalance(s.database, s.partner_a, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), a_bal.debit);
    try std.testing.expectEqual(@as(i64, 585 * 1_000_000_000_0), a_bal.credit);

    // Partner B (30k + 5,100) = 35,100
    const b_bal = try queryCacheBalance(s.database, s.partner_b, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), b_bal.debit);
    try std.testing.expectEqual(@as(i64, 351 * 1_000_000_000_0), b_bal.credit);

    // Partner C (20k + 3,400) = 23,400
    const c_bal = try queryCacheBalance(s.database, s.partner_c, s.periods[0]);
    try std.testing.expectEqual(@as(i64, 0), c_bal.debit);
    try std.testing.expectEqual(@as(i64, 234 * 1_000_000_000_0), c_bal.credit);

    // Total of partner allocations = total net income exactly
    const total_allocated = (a_bal.credit - 50 * 1_000_000_000_00) + (b_bal.credit - 30 * 1_000_000_000_00) + (c_bal.credit - 20 * 1_000_000_000_00);
    try std.testing.expectEqual(@as(i64, 17 * 1_000_000_000_00), total_allocated);
}

test "CHAR part: BS after Jan close balances with partner capital" {
    const s = try setupPartnershipScenario();
    defer s.database.close();

    try postPartnershipYear1(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    const bs = try report_mod.balanceSheet(s.database, s.book_id, "2026-01-31", "2026-01-01");
    defer bs.deinit();

    // Assets: Cash (100 - 8 = 92) + AR 15 = 107k
    // Equity: Partner A 58.5 + B 35.1 + C 23.4 = 117k
    // Wait: 107 assets vs 117 equity doesn't balance!
    // Let me recompute: Cash = 50+30+20 (initial) + 10 (E4 cash sale) - 8 (E5 expense) = 102
    // AR = 15
    // Total assets = 117
    // Equity = 58.5 + 35.1 + 23.4 = 117 ✓
    if (findRow(bs.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 102 * 1_000_000_000_00), row.debit_balance);
    } else return error.TestUnexpectedResult;

    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    try std.testing.expectEqual(@as(i64, 117 * 1_000_000_000_00), bs.total_debits);
}

test "CHAR part: verify passes after close" {
    const s = try setupPartnershipScenario();
    defer s.database.close();

    try postPartnershipYear1(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");

    const result = try verify_mod.verify(s.database, s.book_id);
    try std.testing.expect(result.passed());
}

// ─────────────────────────────────────────────────────────────
// B.4 NONPROFIT — single-target close to Net Assets
// ─────────────────────────────────────────────────────────────

test "CHAR nonprofit: donation and program expense close to Net Assets" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Heft Foundation", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .nonprofit, "admin");

    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const na_without = try account_mod.Account.create(database, book_id, "3000", "Net Assets Without Donor Restrictions", .equity, false, "admin");
    const donations = try account_mod.Account.create(database, book_id, "4000", "Donations", .revenue, false, "admin");
    const programs = try account_mod.Account.create(database, book_id, "5000", "Program Expenses", .expense, false, "admin");

    try book_mod.Book.setEquityCloseTarget(database, book_id, na_without, "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Donations received: 50k
    try postSimple2LineEntry(database, book_id, period_id, "DON-001", "2026-01-10", cash, donations, 50 * 1_000_000_000_00);
    // Programs run: 20k
    try postSimple2LineEntry(database, book_id, period_id, "PROG-001", "2026-01-25", programs, cash, 20 * 1_000_000_000_00);

    try close_mod.closePeriod(database, book_id, period_id, "admin");

    // Change in net assets = 50 - 20 = 30k → closes to Net Assets Without Donor Restrictions
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = ? AND period_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, na_without);
    try stmt.bindInt(2, period_id);
    _ = try stmt.step();
    const na_debit = stmt.columnInt64(0);
    const na_credit = stmt.columnInt64(1);
    try std.testing.expectEqual(@as(i64, 30 * 1_000_000_000_00), na_credit - na_debit);

    // Donations and programs zeroed
    var d_stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = ? AND period_id = ?;");
    defer d_stmt.finalize();
    try d_stmt.bindInt(1, donations);
    try d_stmt.bindInt(2, period_id);
    _ = try d_stmt.step();
    try std.testing.expectEqual(d_stmt.columnInt64(0), d_stmt.columnInt64(1));

    const result = try verify_mod.verify(database, book_id);
    try std.testing.expect(result.passed());
}

// ─────────────────────────────────────────────────────────────
// B.5 MULTI-PERIOD REPORTS — comparative, GL, journal register, etc.
// ─────────────────────────────────────────────────────────────

test "CHAR corp: TB Movement Jan shows period activity only" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const tbm = try report_mod.trialBalanceMovement(s.database, s.book_id, "2026-01-01", "2026-01-31");
    defer tbm.deinit();

    try std.testing.expectEqual(tbm.total_debits, tbm.total_credits);

    // Cash movement in Jan: 110k dr, 38k cr → 72k net debit
    if (findRow(tbm.rows, s.cash)) |row| {
        try std.testing.expectEqual(@as(i64, 72 * 1_000_000_000_00), row.debit_balance);
    } else return error.TestUnexpectedResult;

    // Revenue movement: 25k credit
    if (findRow(tbm.rows, s.revenue)) |row| {
        try std.testing.expectEqual(@as(i64, 25 * 1_000_000_000_00), row.credit_balance);
    } else return error.TestUnexpectedResult;
}

test "CHAR corp: Income Statement Jan-Mar shows YTD totals" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    // YTD activity (before closing entries affected the cache):
    // Revenue: 25k + 20k + 12k = 57k
    // Expense: 8k + 5k + 3k = 16k
    // NOTE: After close, the closing entries also appear in the cache. This
    // test captures what the current implementation produces.
    const is_report = try report_mod.incomeStatement(s.database, s.book_id, "2026-01-01", "2026-03-31");
    defer is_report.deinit();

    // After close, revenue has dr=57 cr=57 (period net movement = 0), same for expense
    // So the IS for a closed period shows all zeros.
    // This is CURRENT BEHAVIOR — locked down for the rewrite.
    try std.testing.expectEqual(@as(i64, 0), is_report.total_debits);
    try std.testing.expectEqual(@as(i64, 0), is_report.total_credits);
}

test "CHAR corp: General Ledger Jan lists all 5 entries" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const gl = try report_mod.generalLedger(s.database, s.book_id, "2026-01-01", "2026-01-31");
    defer gl.deinit();

    // 5 entries × 2 lines each = 10 rows
    try std.testing.expectEqual(@as(usize, 10), gl.rows.len);
    try std.testing.expectEqual(gl.total_debits, gl.total_credits);
}

test "CHAR corp: Journal Register Jan totals balance" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const jr = try report_mod.journalRegister(s.database, s.book_id, "2026-01-01", "2026-01-31");
    defer jr.deinit();

    try std.testing.expectEqual(@as(usize, 10), jr.rows.len);
    try std.testing.expectEqual(jr.total_debits, jr.total_credits);
}

test "CHAR corp: Account Ledger for Cash shows running balance" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);

    const al = try report_mod.accountLedger(s.database, s.book_id, s.cash, "2026-01-01", "2026-01-31");
    defer al.deinit();

    // Cash has 4 lines in Jan (E1 +100k, E2 -30k, E4 +10k, E5 -8k)
    try std.testing.expectEqual(@as(usize, 4), al.rows.len);
    // Jan is the first period, no opening balance
    try std.testing.expectEqual(@as(i64, 0), al.opening_balance);
    // Closing balance: 100 - 30 + 10 - 8 = 72k
    try std.testing.expectEqual(@as(i64, 72 * 1_000_000_000_00), al.closing_balance);
}

test "CHAR corp: Comparative TB Feb vs Jan after both closed" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");

    const comp = try report_mod.trialBalanceComparative(s.database, s.book_id, "2026-02-28", "2026-01-31");
    defer comp.deinit();

    // Comparative report returns rows with current and prior balances.
    // Both should be balanced.
    try std.testing.expectEqual(comp.current_total_debits, comp.current_total_credits);
    try std.testing.expectEqual(comp.prior_total_debits, comp.prior_total_credits);
}

test "CHAR corp: Equity Changes Jan-Mar shows movement" {
    const s = try setupCorporationScenario();
    defer s.database.close();

    try postYear1Transactions(s);
    try close_mod.closePeriod(s.database, s.book_id, s.periods[0], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[1], "admin");
    try close_mod.closePeriod(s.database, s.book_id, s.periods[2], "admin");

    const eq = try report_mod.equityChanges(s.database, s.book_id, "2026-01-01", "2026-03-31", "2026-01-01");
    defer eq.deinit();

    try std.testing.expect(eq.rows.len > 0);
}
