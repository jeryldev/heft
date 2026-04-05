const std = @import("std");
const db = @import("db.zig");
const entry_mod = @import("entry.zig");
const period_mod = @import("period.zig");
const audit = @import("audit.zig");
const money = @import("money.zig");

pub fn closePeriod(database: db.Database, book_id: i64, period_id: i64, performed_by: []const u8) !void {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    var re_account_id: i64 = 0;
    var is_account_id: i64 = 0;
    var book_currency_buf: [4]u8 = undefined;
    var book_currency_len: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT status, retained_earnings_account_id, income_summary_account_id, base_currency
            \\FROM ledger_books WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        re_account_id = stmt.columnInt64(1);
        is_account_id = stmt.columnInt64(2);
        if (stmt.columnText(3)) |cur| {
            book_currency_len = @min(cur.len, book_currency_buf.len);
            @memcpy(book_currency_buf[0..book_currency_len], cur[0..book_currency_len]);
        }
    }

    if (re_account_id <= 0) return error.RetainedEarningsAccountRequired;

    const base_currency = book_currency_buf[0..book_currency_len];

    var period_status_buf: [16]u8 = undefined;
    var period_status_len: usize = 0;
    var period_number: i32 = 0;
    var period_year: i32 = 0;
    var end_date_buf: [11]u8 = undefined;
    var end_date_len: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT status, period_number, year, end_date FROM ledger_periods WHERE id = ? AND book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, period_id);
        try stmt.bindInt(2, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        const status = stmt.columnText(0).?;
        if (std.mem.eql(u8, status, "closed")) return error.PeriodClosed;
        if (std.mem.eql(u8, status, "locked")) return error.PeriodLocked;
        period_status_len = @min(status.len, period_status_buf.len);
        @memcpy(period_status_buf[0..period_status_len], status[0..period_status_len]);
        period_number = stmt.columnInt(1);
        period_year = stmt.columnInt(2);
        if (stmt.columnText(3)) |ed| {
            end_date_len = @min(ed.len, end_date_buf.len);
            @memcpy(end_date_buf[0..end_date_len], ed[0..end_date_len]);
        }
    }
    const end_date = end_date_buf[0..end_date_len];
    const period_status = period_status_buf[0..period_status_len];

    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND period_id = ? AND status = 'draft';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, period_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) return error.InvalidInput;
    }

    const MaxAccounts = 500;
    var account_ids: [MaxAccounts]i64 = undefined;
    var debit_sums: [MaxAccounts]i64 = undefined;
    var credit_sums: [MaxAccounts]i64 = undefined;
    var is_revenue: [MaxAccounts]bool = undefined;
    var acct_count: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT ab.account_id, a.account_type, ab.debit_sum, ab.credit_sum
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\WHERE ab.book_id = ? AND ab.period_id = ?
            \\  AND a.account_type IN ('revenue', 'expense')
            \\  AND (ab.debit_sum != 0 OR ab.credit_sum != 0);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, period_id);
        while (try stmt.step()) {
            if (acct_count >= MaxAccounts) break;
            account_ids[acct_count] = stmt.columnInt64(0);
            const acct_type = stmt.columnText(1).?;
            is_revenue[acct_count] = std.mem.eql(u8, acct_type, "revenue");
            debit_sums[acct_count] = stmt.columnInt64(2);
            credit_sums[acct_count] = stmt.columnInt64(3);
            acct_count += 1;
        }
    }

    if (acct_count == 0) {
        if (std.mem.eql(u8, period_status, "open")) {
            try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
        }
        try period_mod.Period.transition(database, period_id, .closed, performed_by);
        if (owns_txn) try database.commit();
        return;
    }

    var doc_buf: [32]u8 = undefined;

    if (is_account_id > 0) {
        try twoStepClose(database, book_id, period_id, re_account_id, is_account_id, base_currency, end_date, period_number, period_year, account_ids[0..acct_count], debit_sums[0..acct_count], credit_sums[0..acct_count], is_revenue[0..acct_count], &doc_buf, performed_by);
    } else {
        try directClose(database, book_id, period_id, re_account_id, base_currency, end_date, period_number, period_year, account_ids[0..acct_count], debit_sums[0..acct_count], credit_sums[0..acct_count], &doc_buf, performed_by);
    }

    if (std.mem.eql(u8, period_status, "open")) {
        try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
    }
    try period_mod.Period.transition(database, period_id, .closed, performed_by);

    if (owns_txn) try database.commit();
}

fn directClose(database: db.Database, book_id: i64, period_id: i64, re_account_id: i64, base_currency: []const u8, end_date: []const u8, period_number: i32, period_year: i32, account_ids: []const i64, debit_sums: []const i64, credit_sums: []const i64, doc_buf: *[32]u8, performed_by: []const u8) !void {
    const doc_number = std.fmt.bufPrint(doc_buf, "CLOSE-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, doc_number, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"direct\"}", performed_by);

    var line_num: i32 = 1;
    for (account_ids, debit_sums, credit_sums) |acct_id, ds, cs| {
        if (ds > cs) {
            const amount = ds - cs;
            _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, re_account_id, null, null, performed_by);
            line_num += 1;
            _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
            line_num += 1;
        } else if (cs > ds) {
            const amount = cs - ds;
            _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
            line_num += 1;
            _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, re_account_id, null, null, performed_by);
            line_num += 1;
        }
    }

    try entry_mod.Entry.post(database, entry_id, performed_by);
}

fn twoStepClose(database: db.Database, book_id: i64, period_id: i64, re_account_id: i64, is_account_id: i64, base_currency: []const u8, end_date: []const u8, period_number: i32, period_year: i32, account_ids: []const i64, debit_sums: []const i64, credit_sums: []const i64, is_revenue_flags: []const bool, doc_buf: *[32]u8, performed_by: []const u8) !void {
    {
        const doc = std.fmt.bufPrint(doc_buf, "CLOSE-REV-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;
        const entry_id = try entry_mod.Entry.createDraft(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"income_summary\",\"step\":1}", performed_by);
        var line_num: i32 = 1;
        var has_lines = false;
        for (account_ids, debit_sums, credit_sums, is_revenue_flags) |acct_id, ds, cs, is_rev| {
            if (!is_rev) continue;
            if (ds == cs) continue;
            has_lines = true;
            if (cs > ds) {
                const amount = cs - ds;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
                line_num += 1;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
                line_num += 1;
            } else {
                const amount = ds - cs;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
                line_num += 1;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
                line_num += 1;
            }
        }
        if (has_lines) {
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else {
            try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
        }
    }

    {
        const doc = std.fmt.bufPrint(doc_buf, "CLOSE-EXP-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;
        const entry_id = try entry_mod.Entry.createDraft(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"income_summary\",\"step\":2}", performed_by);
        var line_num: i32 = 1;
        var has_lines = false;
        for (account_ids, debit_sums, credit_sums, is_revenue_flags) |acct_id, ds, cs, is_rev| {
            if (is_rev) continue;
            if (ds == cs) continue;
            has_lines = true;
            if (ds > cs) {
                const amount = ds - cs;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
                line_num += 1;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
                line_num += 1;
            } else {
                const amount = cs - ds;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null, performed_by);
                line_num += 1;
                _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, amount, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
                line_num += 1;
            }
        }
        if (has_lines) {
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else {
            try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
        }
    }

    {
        const doc = std.fmt.bufPrint(doc_buf, "CLOSE-IS-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;
        const entry_id = try entry_mod.Entry.createDraft(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"income_summary\",\"step\":3}", performed_by);

        var is_debit: i64 = 0;
        var is_credit: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT COALESCE(SUM(debit_sum), 0), COALESCE(SUM(credit_sum), 0)
                \\FROM ledger_account_balances
                \\WHERE book_id = ? AND period_id = ? AND account_id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            try stmt.bindInt(2, period_id);
            try stmt.bindInt(3, is_account_id);
            _ = try stmt.step();
            is_debit = stmt.columnInt64(0);
            is_credit = stmt.columnInt64(1);
        }

        if (is_credit > is_debit) {
            const amount = is_credit - is_debit;
            _ = try entry_mod.Entry.addLine(database, entry_id, 1, amount, 0, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
            _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, amount, base_currency, money.FX_RATE_SCALE, re_account_id, null, null, performed_by);
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else if (is_debit > is_credit) {
            const amount = is_debit - is_credit;
            _ = try entry_mod.Entry.addLine(database, entry_id, 1, amount, 0, base_currency, money.FX_RATE_SCALE, re_account_id, null, null, performed_by);
            _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, amount, base_currency, money.FX_RATE_SCALE, is_account_id, null, null, performed_by);
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else {
            try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const verify_mod = @import("verify.zig");

fn setupCloseTestDb() !struct { database: db.Database, book_id: i64, cash_id: i64, revenue_id: i64, expense_id: i64, re_id: i64, period_id: i64 } {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const expense_id = try account_mod.Account.create(database, book_id, "5001", "Expense", .expense, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return .{ .database = database, .book_id = book_id, .cash_id = cash_id, .revenue_id = revenue_id, .expense_id = expense_id, .re_id = re_id, .period_id = period_id };
}

fn postTestEntry(database: db.Database, book_id: i64, doc: []const u8, debit_acct: i64, debit_amt: i64, credit_acct: i64, credit_amt: i64, period_id: i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, book_id, doc, "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, "PHP", money.FX_RATE_SCALE, debit_acct, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, "PHP", money.FX_RATE_SCALE, credit_acct, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
}

fn queryBalance(database: db.Database, account_id: i64, period_id: i64) !struct { debit_sum: i64, credit_sum: i64 } {
    var stmt = try database.prepare("SELECT COALESCE(debit_sum, 0), COALESCE(credit_sum, 0) FROM ledger_account_balances WHERE account_id = ? AND period_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, account_id);
    try stmt.bindInt(2, period_id);
    const has_row = try stmt.step();
    if (!has_row) return .{ .debit_sum = 0, .credit_sum = 0 };
    return .{ .debit_sum = stmt.columnInt64(0), .credit_sum = stmt.columnInt64(1) };
}

fn queryPeriodStatus(database: db.Database, period_id: i64, out_buf: []u8) ![]u8 {
    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, period_id);
    _ = try stmt.step();
    const status = stmt.columnText(0).?;
    const len = @min(status.len, out_buf.len);
    @memcpy(out_buf[0..len], status[0..len]);
    return out_buf[0..len];
}

test "closePeriod: direct close with profit" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const rev_bal = try queryBalance(s.database, s.revenue_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.credit_sum - rev_bal.debit_sum);

    const exp_bal = try queryBalance(s.database, s.expense_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.debit_sum - exp_bal.credit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const net_income: i64 = 10_000_000_000_00 - 6_000_000_000_00;
    try std.testing.expectEqual(net_income, re_bal.credit_sum - re_bal.debit_sum);

    {
        var stmt = try s.database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, s.period_id);
        _ = try stmt.step();
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(0).?, "closed"));
    }

    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND metadata LIKE '%closing_entry%';");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expect(stmt.columnInt(0) >= 1);
    }
}

test "closePeriod: direct close with loss" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 3_000_000_000_00, s.revenue_id, 3_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 5_000_000_000_00, s.cash_id, 5_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const net_loss: i64 = 5_000_000_000_00 - 3_000_000_000_00;
    try std.testing.expectEqual(net_loss, re_bal.debit_sum - re_bal.credit_sum);
}

test "closePeriod: two-step close via income summary" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const is_id = try account_mod.Account.create(s.database, s.book_id, "3200", "Income Summary", .equity, false, "admin");
    try book_mod.Book.setIncomeSummaryAccount(s.database, s.book_id, is_id, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND metadata LIKE '%income_summary%';");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }

    const is_bal = try queryBalance(s.database, is_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), is_bal.debit_sum - is_bal.credit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const net_income: i64 = 10_000_000_000_00 - 6_000_000_000_00;
    try std.testing.expectEqual(net_income, re_bal.credit_sum - re_bal.debit_sum);

    {
        var stmt = try s.database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, s.period_id);
        _ = try stmt.step();
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(0).?, "closed"));
    }
}

test "closePeriod: no retained earnings designated" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const result = closePeriod(database, book_id, period_id, "admin");
    try std.testing.expectError(error.RetainedEarningsAccountRequired, result);
}

test "closePeriod: draft entries exist" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    _ = try entry_mod.Entry.createDraft(s.database, s.book_id, "DRAFT-001", "2026-01-15", "2026-01-15", null, s.period_id, null, "admin");

    const result = closePeriod(s.database, s.book_id, s.period_id, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "closePeriod: period already closed" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const result = closePeriod(s.database, s.book_id, s.period_id, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "closePeriod: period locked" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try period_mod.Period.transition(s.database, s.period_id, .soft_closed, "admin");
    try period_mod.Period.transition(s.database, s.period_id, .closed, "admin");
    try period_mod.Period.transition(s.database, s.period_id, .locked, "admin");

    const result = closePeriod(s.database, s.book_id, s.period_id, "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

test "closePeriod: zero revenue/expense just transitions to closed" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    {
        var stmt = try s.database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, s.period_id);
        _ = try stmt.step();
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(0).?, "closed"));
    }

    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND metadata LIKE '%closing_entry%';");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }
}

test "closePeriod: multiple accounts all zeroed" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const rev2_id = try account_mod.Account.create(s.database, s.book_id, "4002", "Service Revenue", .revenue, false, "admin");
    const rev3_id = try account_mod.Account.create(s.database, s.book_id, "4003", "Interest Revenue", .revenue, false, "admin");
    const exp2_id = try account_mod.Account.create(s.database, s.book_id, "5002", "Salaries", .expense, false, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 5_000_000_000_00, s.revenue_id, 5_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "REV-002", s.cash_id, 3_000_000_000_00, rev2_id, 3_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "REV-003", s.cash_id, 1_000_000_000_00, rev3_id, 1_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 4_000_000_000_00, s.cash_id, 4_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-002", exp2_id, 2_000_000_000_00, s.cash_id, 2_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const accounts = [_]i64{ s.revenue_id, rev2_id, rev3_id, s.expense_id, exp2_id };
    for (accounts) |acct_id| {
        const bal = try queryBalance(s.database, acct_id, s.period_id);
        try std.testing.expectEqual(@as(i64, 0), bal.debit_sum - bal.credit_sum);
    }

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const expected_net: i64 = (5_000_000_000_00 + 3_000_000_000_00 + 1_000_000_000_00) - (4_000_000_000_00 + 2_000_000_000_00);
    try std.testing.expectEqual(expected_net, re_bal.credit_sum - re_bal.debit_sum);
}

test "closePeriod: verify after close passes" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const result = try verify_mod.verify(s.database, s.book_id);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.errors);
}

test "closePeriod: soft_closed period closes correctly" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try period_mod.Period.transition(s.database, s.period_id, .soft_closed, "admin");

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    {
        var stmt = try s.database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, s.period_id);
        _ = try stmt.step();
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(0).?, "closed"));
    }
}

test "closePeriod: book archived rejected" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "RE", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try period_mod.Period.transition(database, period_id, .soft_closed, "admin");
    try period_mod.Period.transition(database, period_id, .closed, "admin");
    try book_mod.Book.archive(database, book_id, "admin");

    const result = closePeriod(database, book_id, period_id, "admin");
    try std.testing.expectError(error.BookArchived, result);
}
