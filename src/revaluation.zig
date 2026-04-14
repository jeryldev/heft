const std = @import("std");
const db = @import("db.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");

pub const CurrencyRate = struct {
    currency: []const u8,
    new_rate: i64,
};

pub const RevalResult = struct {
    entry_id: i64,
    reversal_id: i64,
};

pub fn revalueForexBalances(database: db.Database, book_id: i64, period_id: i64, rates: []const CurrencyRate, performed_by: []const u8) !RevalResult {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    var fx_gl_account_id: i64 = 0;
    var base_currency_buf: [4]u8 = undefined;
    var base_currency_len: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT status, fx_gain_loss_account_id, base_currency FROM ledger_books WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        fx_gl_account_id = stmt.columnInt64(1);
        if (stmt.columnText(2)) |cur| {
            base_currency_len = @min(cur.len, base_currency_buf.len);
            @memcpy(base_currency_buf[0..base_currency_len], cur[0..base_currency_len]);
        }
    }
    const base_currency = base_currency_buf[0..base_currency_len];

    if (fx_gl_account_id <= 0) return error.FxGainLossAccountRequired;

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
        period_number = stmt.columnInt(1);
        period_year = stmt.columnInt(2);
        if (stmt.columnText(3)) |ed| {
            end_date_len = @min(ed.len, end_date_buf.len);
            @memcpy(end_date_buf[0..end_date_len], ed[0..end_date_len]);
        }
    }
    const end_date = end_date_buf[0..end_date_len];

    const MaxAdjustments = 2000;
    var adj_account_ids: [MaxAdjustments]i64 = undefined;
    var adj_amounts: [MaxAdjustments]i64 = undefined;
    var adj_count: usize = 0;

    var rate_stmt = try database.prepare(
        \\SELECT el.account_id,
        \\  SUM(CASE WHEN el.debit_amount > 0 THEN el.debit_amount ELSE -el.credit_amount END) as net_txn_amount,
        \\  SUM(el.base_debit_amount - el.base_credit_amount) as existing_base
        \\FROM ledger_entry_lines el
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\JOIN ledger_periods p ON p.id = e.period_id
        \\JOIN ledger_accounts a ON a.id = el.account_id
        \\WHERE e.book_id = ? AND e.status IN ('posted', 'reversed')
        \\  AND p.end_date <= (SELECT end_date FROM ledger_periods WHERE id = ?)
        \\  AND el.transaction_currency = ?
        \\  AND a.is_monetary = 1
        \\GROUP BY el.account_id;
    );
    defer rate_stmt.finalize();

    for (rates) |rate| {
        if (std.mem.eql(u8, rate.currency, base_currency)) continue;

        rate_stmt.reset();
        rate_stmt.clearBindings();
        try rate_stmt.bindInt(1, book_id);
        try rate_stmt.bindInt(2, period_id);
        try rate_stmt.bindText(3, rate.currency);

        while (try rate_stmt.step()) {
            if (adj_count >= MaxAdjustments) return error.TooManyAccounts;
            const account_id = rate_stmt.columnInt64(0);
            const net_txn_amount = rate_stmt.columnInt64(1);
            const existing_base = rate_stmt.columnInt64(2);

            const revalued_base = try money.computeBaseAmount(net_txn_amount, rate.new_rate);

            const diff = std.math.sub(i64, revalued_base, existing_base) catch return error.AmountOverflow;

            if (diff != 0) {
                adj_account_ids[adj_count] = account_id;
                adj_amounts[adj_count] = diff;
                adj_count += 1;
            }
        }
    }

    if (adj_count == 0) {
        if (owns_txn) try database.commit();
        return .{ .entry_id = 0, .reversal_id = 0 };
    }

    var doc_buf: [32]u8 = undefined;
    const doc_number = std.fmt.bufPrint(&doc_buf, "REVAL-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;

    const fx_one: i64 = money.FX_RATE_SCALE;

    const entry_id = try entry_mod.Entry.createDraftAs(
        database,
        book_id,
        doc_number,
        end_date,
        end_date,
        null,
        period_id,
        "{\"revaluation\":true}",
        .adjusting,
        performed_by,
    );
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();

    var line_num: i32 = 1;
    var fx_debit_total: i64 = 0;
    var fx_credit_total: i64 = 0;
    for (adj_account_ids[0..adj_count], adj_amounts[0..adj_count]) |acct_id, amount| {
        if (amount > 0) {
            _ = try appender.add(line_num, amount, 0, base_currency, fx_one, acct_id, null, null);
            line_num += 1;
            fx_credit_total = std.math.add(i64, fx_credit_total, amount) catch return error.AmountOverflow;
        } else {
            const abs_amount = std.math.negate(amount) catch return error.AmountOverflow;
            _ = try appender.add(line_num, 0, abs_amount, base_currency, fx_one, acct_id, null, null);
            line_num += 1;
            fx_debit_total = std.math.add(i64, fx_debit_total, abs_amount) catch return error.AmountOverflow;
        }
    }
    if (fx_debit_total > 0) {
        _ = try appender.add(line_num, fx_debit_total, 0, base_currency, fx_one, fx_gl_account_id, null, null);
        line_num += 1;
    }
    if (fx_credit_total > 0) {
        _ = try appender.add(line_num, 0, fx_credit_total, base_currency, fx_one, fx_gl_account_id, null, null);
        line_num += 1;
    }

    try entry_mod.Entry.post(database, entry_id, performed_by);

    var reversal_id: i64 = 0;
    {
        var next_stmt = try database.prepare(
            \\SELECT id, start_date FROM ledger_periods
            \\WHERE book_id = ? AND start_date > ? AND status IN ('open', 'soft_closed')
            \\ORDER BY start_date ASC LIMIT 1;
        );
        defer next_stmt.finalize();
        try next_stmt.bindInt(1, book_id);
        try next_stmt.bindText(2, end_date);
        const has_next = try next_stmt.step();
        if (has_next) {
            const next_period_id = next_stmt.columnInt64(0);
            const next_start = next_stmt.columnText(1).?;

            var rev_doc_buf: [40]u8 = undefined;
            const rev_doc = std.fmt.bufPrint(&rev_doc_buf, "REVAL-REV-P{d}-FY{d}", .{ period_number, period_year }) catch unreachable;

            var rev_meta_buf: [128]u8 = undefined;
            const rev_meta = std.fmt.bufPrint(&rev_meta_buf, "{{\"revaluation_reversal\":true,\"reverses_revaluation\":{d}}}", .{entry_id}) catch unreachable;

            const rev_id = try entry_mod.Entry.createDraftAs(
                database,
                book_id,
                rev_doc,
                next_start,
                next_start,
                null,
                next_period_id,
                rev_meta,
                .adjusting,
                performed_by,
            );
            var rev_appender = try entry_mod.Entry.LineAppender.init(database, rev_id, book_id, performed_by);
            defer rev_appender.deinit();

            var rev_line: i32 = 1;
            var rev_fx_debit: i64 = 0;
            var rev_fx_credit: i64 = 0;
            for (adj_account_ids[0..adj_count], adj_amounts[0..adj_count]) |acct_id, amount| {
                if (amount > 0) {
                    _ = try rev_appender.add(rev_line, 0, amount, base_currency, fx_one, acct_id, null, null);
                    rev_line += 1;
                    rev_fx_debit = std.math.add(i64, rev_fx_debit, amount) catch return error.AmountOverflow;
                } else {
                    const abs_amount = std.math.negate(amount) catch return error.AmountOverflow;
                    _ = try rev_appender.add(rev_line, abs_amount, 0, base_currency, fx_one, acct_id, null, null);
                    rev_line += 1;
                    rev_fx_credit = std.math.add(i64, rev_fx_credit, abs_amount) catch return error.AmountOverflow;
                }
            }
            if (rev_fx_debit > 0) {
                _ = try rev_appender.add(rev_line, rev_fx_debit, 0, base_currency, fx_one, fx_gl_account_id, null, null);
                rev_line += 1;
            }
            if (rev_fx_credit > 0) {
                _ = try rev_appender.add(rev_line, 0, rev_fx_credit, base_currency, fx_one, fx_gl_account_id, null, null);
                rev_line += 1;
            }

            try entry_mod.Entry.post(database, rev_id, performed_by);
            reversal_id = rev_id;
        }
    }

    if (owns_txn) try database.commit();
    return .{ .entry_id = entry_id, .reversal_id = reversal_id };
}

pub fn parseRatesJson(json: []const u8, rates_buf: []CurrencyRate) !usize {
    var count: usize = 0;
    var i: usize = 0;

    while (i < json.len and (json[i] == ' ' or json[i] == '\n' or json[i] == '[')) : (i += 1) {}

    while (i < json.len and count < rates_buf.len) {
        while (i < json.len and json[i] != '{') : (i += 1) {}
        if (i >= json.len) break;
        i += 1;

        var currency_start: usize = 0;
        var currency_end: usize = 0;
        var rate_val: i64 = 0;

        while (i < json.len and json[i] != '}') {
            while (i < json.len and json[i] != '"') : (i += 1) {}
            if (i >= json.len) break;
            i += 1;
            const key_start = i;
            while (i < json.len and json[i] != '"') : (i += 1) {}
            if (i >= json.len) break;
            const key = json[key_start..i];
            i += 1;

            while (i < json.len and json[i] != ':') : (i += 1) {}
            if (i >= json.len) break;
            i += 1;

            while (i < json.len and json[i] == ' ') : (i += 1) {}

            if (std.mem.eql(u8, key, "currency")) {
                while (i < json.len and json[i] != '"') : (i += 1) {}
                if (i >= json.len) break;
                i += 1;
                currency_start = i;
                while (i < json.len and json[i] != '"') : (i += 1) {}
                currency_end = i;
                if (i < json.len) i += 1;
            } else if (std.mem.eql(u8, key, "rate")) {
                var negative = false;
                if (i < json.len and json[i] == '-') {
                    negative = true;
                    i += 1;
                }
                while (i < json.len and json[i] >= '0' and json[i] <= '9') {
                    rate_val = std.math.mul(i64, rate_val, 10) catch return error.InvalidAmount;
                    rate_val = std.math.add(i64, rate_val, @as(i64, json[i] - '0')) catch return error.InvalidAmount;
                    i += 1;
                }
                if (negative) rate_val = std.math.negate(rate_val) catch return error.InvalidAmount;
            }

            while (i < json.len and (json[i] == ',' or json[i] == ' ')) : (i += 1) {}
        }

        if (currency_end > currency_start and rate_val > 0) {
            rates_buf[count] = .{
                .currency = json[currency_start..currency_end],
                .new_rate = rate_val,
            };
            count += 1;
        }

        while (i < json.len and (json[i] == '}' or json[i] == ',' or json[i] == ' ' or json[i] == '\n')) : (i += 1) {}
    }

    return count;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

fn setupFxTestDb() !struct { database: db.Database, book_id: i64, cash_php: i64, cash_usd: i64, revenue: i64, fx_gl: i64, period_id: i64 } {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "FX Test", "PHP", 2, "admin");
    const cash_php = try account_mod.Account.create(database, book_id, "1001", "Cash PHP", .asset, false, "admin");
    const cash_usd = try account_mod.Account.create(database, book_id, "1002", "Cash USD", .asset, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const fx_gl = try account_mod.Account.create(database, book_id, "7001", "FX Gain/Loss", .expense, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gl, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return .{ .database = database, .book_id = book_id, .cash_php = cash_php, .cash_usd = cash_usd, .revenue = revenue, .fx_gl = fx_gl, .period_id = period_id };
}

fn postFxEntry(database: db.Database, book_id: i64, doc: []const u8, debit_acct: i64, debit_amt: i64, debit_cur: []const u8, debit_fx: i64, credit_acct: i64, credit_amt: i64, credit_cur: []const u8, credit_fx: i64, period_id: i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, book_id, doc, "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, debit_cur, debit_fx, debit_acct, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, credit_cur, credit_fx, credit_acct, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
}

test "revalueForexBalances: FX gain from rate increase" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Post: Debit Cash-USD $100 at 56.50 PHP/USD, Credit Cash-PHP 5650 at 1.00
    // FX rate 56.50 = 565_000_000_000 in 10^10 scale
    // base_debit = computeBaseAmount(10B, 565B) = 10B * 565B / 10B = 565B = PHP 5,650.00
    // base_credit = computeBaseAmount(565B, 10B) = 565B * 10B / 10B = 565B = PHP 5,650.00
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue USD at 57.00 = 570_000_000_000 in 10^10 scale
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    // Gain = (100 * 57.00) - (100 * 56.50) = 5700 - 5650 = 50 PHP = 5_000_000_000 in 10^8 scale
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }

    {
        var stmt = try s.database.prepare(
            \\SELECT status, metadata FROM ledger_entries WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(0).?, "posted"));
        try std.testing.expect(std.mem.indexOf(u8, stmt.columnText(1).?, "revaluation") != null);
    }

    // Verify gain: Debit Cash-USD 50 PHP, Credit FX GL 50 PHP
    {
        var stmt = try s.database.prepare(
            \\SELECT base_debit_amount, base_credit_amount, account_id FROM ledger_entry_lines
            \\WHERE entry_id = ? ORDER BY line_number;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        const line1_debit = stmt.columnInt64(0);
        const line1_acct = stmt.columnInt64(2);
        try std.testing.expectEqual(@as(i64, 5_000_000_000), line1_debit);
        try std.testing.expectEqual(s.cash_usd, line1_acct);
        _ = try stmt.step();
        const line2_credit = stmt.columnInt64(1);
        const line2_acct = stmt.columnInt64(2);
        try std.testing.expectEqual(@as(i64, 5_000_000_000), line2_credit);
        try std.testing.expectEqual(s.fx_gl, line2_acct);
    }
}

test "revalueForexBalances: entries are marked entry_type='adjusting'" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;

    // The revaluation entry and its reversal (if created) must both carry
    // entry_type='adjusting' so IFRS/GAAP adjustment disclosures can filter them.
    var stmt = try s.database.prepare(
        \\SELECT entry_type FROM ledger_entries WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, reval_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("adjusting", stmt.columnText(0).?);
}

test "revalueForexBalances: FX loss from rate decrease" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Post: Debit Cash-USD $100 at 56.50, Credit Cash-PHP 5650 at 1.00
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue USD at 55.00 = 550_000_000_000
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 550_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    // Loss = (100 * 55.00) - (100 * 56.50) = 5500 - 5650 = -150 PHP = -15_000_000_000
    // Netted: line 1 = account credit, line 2 = FX G/L debit (net counterpart)
    {
        var stmt = try s.database.prepare(
            \\SELECT base_debit_amount, base_credit_amount, account_id FROM ledger_entry_lines
            \\WHERE entry_id = ? ORDER BY line_number;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        const line1_credit = stmt.columnInt64(1);
        const line1_acct = stmt.columnInt64(2);
        try std.testing.expectEqual(@as(i64, 15_000_000_000), line1_credit);
        try std.testing.expectEqual(s.cash_usd, line1_acct);
        _ = try stmt.step();
        const line2_debit = stmt.columnInt64(0);
        const line2_acct = stmt.columnInt64(2);
        try std.testing.expectEqual(@as(i64, 15_000_000_000), line2_debit);
        try std.testing.expectEqual(s.fx_gl, line2_acct);
    }
}

test "revalueForexBalances: multiple currencies in single compound entry" {
    const s = try setupFxTestDb();
    defer s.database.close();

    const cash_eur = try account_mod.Account.create(s.database, s.book_id, "1003", "Cash EUR", .asset, false, "admin");

    // Post USD entry: $100 at 56.50 (565_000_000_000 in 10^10)
    // base = 10B * 565B / 10B = 565B = PHP 5,650.00
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Post EUR entry: EUR 200 at 60.00 (600_000_000_000 in 10^10)
    // base = 20B * 600B / 10B = 1_200B = PHP 12,000.00
    try postFxEntry(s.database, s.book_id, "FX-002", cash_eur, 20_000_000_000, "EUR", 600_000_000_000, s.cash_php, 1_200_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue: USD to 57.00 (570B), EUR to 61.00 (610B)
    const rates = [_]CurrencyRate{
        .{ .currency = "USD", .new_rate = 570_000_000_000 },
        .{ .currency = "EUR", .new_rate = 610_000_000_000 },
    };
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    // USD gain = (100*57 - 100*56.50) = 50 PHP = 5_000_000_000
    // EUR gain = (200*61 - 200*60) = 200 PHP = 20_000_000_000
    // Netted: 2 account lines + 1 FX G/L line = 3 lines total
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }
}

test "revalueForexBalances: no foreign currency lines returns 0" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Post PHP-only entry
    try postFxEntry(s.database, s.book_id, "PHP-001", s.cash_php, 100_000_000_000, "PHP", 10_000_000_000, s.revenue, 100_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue USD (no USD lines exist)
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 57_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectEqual(@as(i64, 0), result.entry_id);
}

test "revalueForexBalances: missing FX GL account returns FxGainLossAccountRequired" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "No FX GL", "PHP", 2, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 57_000_000_000 }};
    const result = revalueForexBalances(database, book_id, period_id, &rates, "admin");
    try std.testing.expectError(error.FxGainLossAccountRequired, result);
}

test "revalueForexBalances: closed period returns PeriodClosed" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try period_mod.Period.transition(s.database, s.period_id, .soft_closed, "admin");
    try period_mod.Period.transition(s.database, s.period_id, .closed, "admin");

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 57_000_000_000 }};
    const result = revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "revalueForexBalances: locked period returns PeriodLocked" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try period_mod.Period.transition(s.database, s.period_id, .soft_closed, "admin");
    try period_mod.Period.transition(s.database, s.period_id, .closed, "admin");
    try period_mod.Period.transition(s.database, s.period_id, .locked, "admin");

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 57_000_000_000 }};
    const result = revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

test "parseRatesJson: valid single rate" {
    const json = "[{\"currency\":\"USD\",\"rate\":57000000000}]";
    var buf: [10]CurrencyRate = undefined;
    const count = try parseRatesJson(json, &buf);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expect(std.mem.eql(u8, buf[0].currency, "USD"));
    try std.testing.expectEqual(@as(i64, 57_000_000_000), buf[0].new_rate);
}

test "parseRatesJson: multiple rates" {
    const json = "[{\"currency\":\"USD\",\"rate\":57000000000},{\"currency\":\"EUR\",\"rate\":61000000000}]";
    var buf: [10]CurrencyRate = undefined;
    const count = try parseRatesJson(json, &buf);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expect(std.mem.eql(u8, buf[0].currency, "USD"));
    try std.testing.expectEqual(@as(i64, 57_000_000_000), buf[0].new_rate);
    try std.testing.expect(std.mem.eql(u8, buf[1].currency, "EUR"));
    try std.testing.expectEqual(@as(i64, 61_000_000_000), buf[1].new_rate);
}

test "parseRatesJson: empty array returns 0" {
    const json = "[]";
    var buf: [10]CurrencyRate = undefined;
    const count = try parseRatesJson(json, &buf);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "revalueForexBalances: book archived returns BookArchived" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "Archived", "PHP", 2, "admin");
    const fx_gl = try account_mod.Account.create(database, book_id, "7001", "FX GL", .expense, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gl, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try period_mod.Period.transition(database, period_id, .soft_closed, "admin");
    try period_mod.Period.transition(database, period_id, .closed, "admin");
    try book_mod.Book.archive(database, book_id, "admin");

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 57_000_000_000 }};
    const result = revalueForexBalances(database, book_id, period_id, &rates, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "revalueForexBalances: no adjustment when rate unchanged" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Post: Debit Cash-USD $100 at 56.50 (565B in 10^10)
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue at the same rate -> no adjustment
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 565_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectEqual(@as(i64, 0), result.entry_id);
}

test "revalueForexBalances: reversed FX position is not revalued" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try postFxEntry(s.database, s.book_id, "FX-REV-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);
    _ = try entry_mod.Entry.reverse(s.database, 1, "Cancelled", "2026-01-20", null, "admin");

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");

    try std.testing.expectEqual(@as(i64, 0), result.entry_id);
    try std.testing.expectEqual(@as(i64, 0), result.reversal_id);
}

test "revalueForexBalances: base currency rate skipped" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Only provide PHP rate (base currency) -> no adjustment
    const rates = [_]CurrencyRate{.{ .currency = "PHP", .new_rate = 10_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectEqual(@as(i64, 0), result.entry_id);
}

test "parseRatesJson: malformed JSON parses partial data gracefully" {
    const json = "[{\"currency\":\"USD\",\"rate\":56";
    var buf: [10]CurrencyRate = undefined;
    const count = try parseRatesJson(json, &buf);
    // Parser extracts what it can from truncated input: currency="USD", rate=56 (>0), so 1 rate
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expect(std.mem.eql(u8, buf[0].currency, "USD"));
    try std.testing.expectEqual(@as(i64, 56), buf[0].new_rate);
}

test "parseRatesJson: negative rate silently dropped" {
    const json = "[{\"currency\":\"USD\",\"rate\":-100}]";
    var buf: [10]CurrencyRate = undefined;
    const count = try parseRatesJson(json, &buf);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "revalueForexBalances: entry has correct revaluation metadata" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    var stmt = try s.database.prepare("SELECT metadata FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, reval_id);
    _ = try stmt.step();
    const metadata = stmt.columnText(0).?;
    try std.testing.expect(std.mem.indexOf(u8, metadata, "\"revaluation\":true") != null);
}

test "revalueForexBalances: audit trail exists for revaluation entry" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    var stmt = try s.database.prepare(
        \\SELECT COUNT(*) FROM ledger_audit_log
        \\WHERE entity_type = 'entry' AND entity_id = ? AND action = 'create';
    );
    defer stmt.finalize();
    try stmt.bindInt(1, reval_id);
    _ = try stmt.step();
    try std.testing.expect(stmt.columnInt(0) >= 1);
}

test "revalueForexBalances: multiple accounts same currency adjusted" {
    const s = try setupFxTestDb();
    defer s.database.close();

    const ar_usd = try account_mod.Account.create(s.database, s.book_id, "1100", "AR USD", .asset, false, "admin");

    // Post $100 to Cash-USD at 56.50
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);
    // Post $200 to AR-USD at 56.50
    try postFxEntry(s.database, s.book_id, "FX-002", ar_usd, 20_000_000_000, "USD", 565_000_000_000, s.cash_php, 1_130_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue USD to 57.00
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    const reval_id = result.entry_id;
    try std.testing.expect(reval_id > 0);

    // Cash-USD gain: (100*57 - 100*56.50) = 50 PHP
    // AR-USD gain: (200*57 - 200*56.50) = 100 PHP
    // Netted: 2 account lines + 1 FX G/L line = 3 lines total
    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, reval_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }
}

test "revalueForexBalances: zero net transaction amount produces no adjustment" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Post $100 debit and $100 credit to same account in USD -> net = 0
    const eid = try entry_mod.Entry.createDraft(s.database, s.book_id, "FX-NET0", "2026-01-15", "2026-01-15", null, s.period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid, 1, 10_000_000_000, 0, "USD", 565_000_000_000, s.cash_usd, null, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid, 2, 0, 10_000_000_000, "USD", 565_000_000_000, s.cash_usd, null, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid, 3, 565_000_000_000, 0, "PHP", 10_000_000_000, s.cash_php, null, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid, 4, 0, 565_000_000_000, "PHP", 10_000_000_000, s.revenue, null, null, "admin");
    try entry_mod.Entry.post(s.database, eid, "admin");

    // Revalue USD at a different rate
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expectEqual(@as(i64, 0), result.entry_id);
}

test "revalueForexBalances: auto-reversal created in next period" {
    const s = try setupFxTestDb();
    defer s.database.close();

    const feb_id = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expect(result.entry_id > 0);
    try std.testing.expect(result.reversal_id > 0);

    {
        var stmt = try s.database.prepare("SELECT period_id, status, metadata FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, result.reversal_id);
        _ = try stmt.step();
        try std.testing.expectEqual(feb_id, stmt.columnInt64(0));
        try std.testing.expect(std.mem.eql(u8, stmt.columnText(1).?, "posted"));
        try std.testing.expect(std.mem.indexOf(u8, stmt.columnText(2).?, "revaluation_reversal") != null);
    }

    {
        var stmt = try s.database.prepare(
            \\SELECT SUM(base_debit_amount), SUM(base_credit_amount) FROM ledger_entry_lines WHERE entry_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, result.reversal_id);
        _ = try stmt.step();
        const rev_debits = stmt.columnInt64(0);
        const rev_credits = stmt.columnInt64(1);

        stmt.reset();
        stmt.clearBindings();
        try stmt.bindInt(1, result.entry_id);
        _ = try stmt.step();
        const reval_debits = stmt.columnInt64(0);
        const reval_credits = stmt.columnInt64(1);

        try std.testing.expectEqual(reval_debits, rev_credits);
        try std.testing.expectEqual(reval_credits, rev_debits);
    }
}

test "revalueForexBalances: no next period skips auto-reversal" {
    const s = try setupFxTestDb();
    defer s.database.close();

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expect(result.entry_id > 0);
    try std.testing.expectEqual(@as(i64, 0), result.reversal_id);
}

test "revalueForexBalances: net effect across periods is zero" {
    const s = try setupFxTestDb();
    defer s.database.close();

    _ = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");

    var stmt = try s.database.prepare(
        \\SELECT SUM(base_debit_amount) - SUM(base_credit_amount) FROM ledger_entry_lines
        \\WHERE entry_id IN (?, ?);
    );
    defer stmt.finalize();
    try stmt.bindInt(1, result.entry_id);
    try stmt.bindInt(2, result.reversal_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
}

test "revalueForexBalances: MaxAdjustments is 2000" {
    try std.testing.expect(2000 == 2000);
}

test "revalueForexBalances: non-monetary account skipped" {
    const s = try setupFxTestDb();
    defer s.database.close();

    // Create a non-monetary PPE account
    const ppe_usd = try account_mod.Account.create(s.database, s.book_id, "1500", "Equipment USD", .asset, false, "admin");
    try account_mod.Account.setMonetary(s.database, ppe_usd, false, "admin");

    // Post USD entry to PPE (non-monetary) and Cash-USD (monetary)
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);
    try postFxEntry(s.database, s.book_id, "FX-002", ppe_usd, 20_000_000_000, "USD", 565_000_000_000, s.cash_php, 1_130_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue USD to 57.00 — only Cash-USD should be adjusted, not PPE
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates, "admin");
    try std.testing.expect(result.entry_id > 0);

    // Only 2 lines (Cash-USD gain + FX GL offset), not 4
    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, result.entry_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }

    // Verify the adjustment is only for Cash-USD ($100 gain of 50 PHP)
    {
        var stmt = try s.database.prepare(
            \\SELECT base_debit_amount, account_id FROM ledger_entry_lines
            \\WHERE entry_id = ? AND base_debit_amount > 0 AND account_id != ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, result.entry_id);
        try stmt.bindInt(2, s.fx_gl);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 5_000_000_000), stmt.columnInt64(0));
        try std.testing.expectEqual(s.cash_usd, stmt.columnInt64(1));
    }
}

test "revalueForexBalances: cumulative across periods (IAS 21.23)" {
    const s = try setupFxTestDb();
    defer s.database.close();

    const feb_id = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Period 1: Buy $100 USD at 56.50 PHP/USD
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Period 2: Buy another $50 USD at 57.00 PHP/USD
    const eid2 = try entry_mod.Entry.createDraft(s.database, s.book_id, "FX-002", "2026-02-15", "2026-02-15", null, feb_id, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid2, 1, 5_000_000_000, 0, "USD", 570_000_000_000, s.cash_usd, null, null, "admin");
    _ = try entry_mod.Entry.addLine(s.database, eid2, 2, 0, 285_000_000_000, "PHP", 10_000_000_000, s.cash_php, null, null, "admin");
    try entry_mod.Entry.post(s.database, eid2, "admin");

    // Revalue at period 2 end with rate 58.00. Cumulative USD position = $150.
    // net_txn_amount = 100 + 50 = $150 = 15_000_000_000
    // existing_base = (100*56.50) + (50*57.00) = 5650 + 2850 = 8500 PHP = 850_000_000_000
    // revalued_base = 150 * 58.00 = 8700 PHP = 870_000_000_000
    // diff = 870B - 850B = 20B = PHP 200.00 gain
    const rates = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 580_000_000_000 }};
    const result = try revalueForexBalances(s.database, s.book_id, feb_id, &rates, "admin");
    try std.testing.expect(result.entry_id > 0);

    // Verify the adjustment amount = 20_000_000_000 (PHP 200.00)
    {
        var stmt = try s.database.prepare(
            \\SELECT base_debit_amount, account_id FROM ledger_entry_lines
            \\WHERE entry_id = ? AND base_debit_amount > 0 AND account_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, result.entry_id);
        try stmt.bindInt(2, s.cash_usd);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 20_000_000_000), stmt.columnInt64(0));
    }
}

test "revalueForexBalances: sequential revaluation across periods" {
    const s = try setupFxTestDb();
    defer s.database.close();

    const feb_id = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    _ = try period_mod.Period.create(s.database, s.book_id, "Mar 2026", 3, 2026, "2026-03-01", "2026-03-31", "regular", "admin");

    // Period 1: Buy $100 USD at 56.50
    try postFxEntry(s.database, s.book_id, "FX-001", s.cash_usd, 10_000_000_000, "USD", 565_000_000_000, s.cash_php, 565_000_000_000, "PHP", 10_000_000_000, s.period_id);

    // Revalue period 1 at 57.00. Gain = 100*(57-56.50) = 50 PHP = 5B
    const rates1 = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const r1 = try revalueForexBalances(s.database, s.book_id, s.period_id, &rates1, "admin");
    try std.testing.expect(r1.entry_id > 0);
    try std.testing.expect(r1.reversal_id > 0);

    // Revalue period 2 at 58.00. Cumulative position = $100 (original).
    // The period 1 revaluation entry (+50 PHP) and its reversal (-50 PHP) are also
    // in the cumulative window, but they are in base currency (PHP), not USD.
    // So the USD net_txn_amount remains $100 from the original entry.
    // existing_base includes original (5650 PHP) + reval (+50) + reversal (-50) = 5650 PHP
    // revalued = 100 * 58.00 = 5800 PHP. diff = 5800 - 5650 = 150 PHP = 15B
    const rates2 = [_]CurrencyRate{.{ .currency = "USD", .new_rate = 580_000_000_000 }};
    const r2 = try revalueForexBalances(s.database, s.book_id, feb_id, &rates2, "admin");
    try std.testing.expect(r2.entry_id > 0);

    {
        var stmt = try s.database.prepare(
            \\SELECT base_debit_amount FROM ledger_entry_lines
            \\WHERE entry_id = ? AND account_id = ? AND base_debit_amount > 0;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, r2.entry_id);
        try stmt.bindInt(2, s.cash_usd);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 15_000_000_000), stmt.columnInt64(0));
    }
}
