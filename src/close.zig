const std = @import("std");
const db = @import("db.zig");
const cache = @import("cache.zig");
const entry_mod = @import("entry.zig");
const period_mod = @import("period.zig");
const money = @import("money.zig");

pub const ClosePeriodProfile = struct {
    preflight_ns: u64 = 0,
    recalculate_stale_ns: u64 = 0,
    load_accounts_ns: u64 = 0,
    close_entries_ns: u64 = 0,
    opening_entry_ns: u64 = 0,
    transitions_ns: u64 = 0,
};

pub fn closePeriod(database: db.Database, book_id: i64, period_id: i64, performed_by: []const u8) !void {
    return closePeriodInternal(database, book_id, period_id, performed_by, null);
}

pub fn closePeriodProfiled(database: db.Database, book_id: i64, period_id: i64, performed_by: []const u8, profile: *ClosePeriodProfile) !void {
    profile.* = .{};
    return closePeriodInternal(database, book_id, period_id, performed_by, profile);
}

fn closePeriodInternal(database: db.Database, book_id: i64, period_id: i64, performed_by: []const u8, profile: ?*ClosePeriodProfile) !void {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();
    var phase_timer: ?std.time.Timer = if (profile != null) try std.time.Timer.start() else null;

    var re_account_id: i64 = 0;
    var is_account_id: i64 = 0;
    var suspense_account_id: i64 = 0;
    var dividends_drawings_account_id: i64 = 0;
    var book_currency_buf: [4]u8 = undefined;
    var book_currency_len: usize = 0;
    var entity_type: book_mod.EntityType = .corporation;
    {
        var stmt = try database.prepare(
            \\SELECT status, retained_earnings_account_id, income_summary_account_id,
            \\  suspense_account_id, dividends_drawings_account_id, base_currency, entity_type
            \\FROM ledger_books WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        re_account_id = stmt.columnInt64(1);
        is_account_id = stmt.columnInt64(2);
        suspense_account_id = stmt.columnInt64(3);
        dividends_drawings_account_id = stmt.columnInt64(4);
        if (stmt.columnText(5)) |cur| {
            book_currency_len = @min(cur.len, book_currency_buf.len);
            @memcpy(book_currency_buf[0..book_currency_len], cur[0..book_currency_len]);
        }
        entity_type = book_mod.EntityType.fromString(stmt.columnText(6).?) orelse return error.InvalidInput;
    }

    // Partnership and LLC use the allocation table. All other entity types
    // require a single equity close target (retained_earnings_account_id).
    const uses_allocations = entity_type == .partnership or entity_type == .llc;
    if (!uses_allocations and re_account_id <= 0) return error.RetainedEarningsAccountRequired;

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

    if (phase_timer) |*timer| {
        profile.?.preflight_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    _ = try cache.recalculateStale(database, book_id, &.{period_id});

    if (phase_timer) |*timer| {
        profile.?.recalculate_stale_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(debit_sum), 0), COALESCE(SUM(credit_sum), 0)
            \\FROM ledger_account_balances WHERE book_id = ? AND period_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, period_id);
        _ = try stmt.step();
        if (stmt.columnInt64(0) != stmt.columnInt64(1)) return error.PeriodNotInBalance;
    }

    // Sprint F (#6): if a suspense account is designated, refuse close while
    // it carries any non-zero cumulative balance. Unresolved suspense items
    // are a control failure — they must be reclassified before period-end.
    {
        if (suspense_account_id > 0) {
            var stmt = try database.prepare(
                \\SELECT COALESCE(SUM(debit_sum) - SUM(credit_sum), 0)
                \\FROM ledger_account_balances ab
                \\JOIN ledger_periods p ON p.id = ab.period_id
                \\WHERE ab.book_id = ? AND ab.account_id = ?
                \\  AND p.end_date <= ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            try stmt.bindInt(2, suspense_account_id);
            try stmt.bindText(3, end_date);
            _ = try stmt.step();
            if (stmt.columnInt64(0) != 0) return error.SuspenseNotClear;
        }
    }

    if (phase_timer) |*timer| {
        profile.?.preflight_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    const MaxAccounts = 2000;
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
            if (acct_count >= MaxAccounts) return error.TooManyAccounts;
            account_ids[acct_count] = stmt.columnInt64(0);
            const acct_type = stmt.columnText(1).?;
            is_revenue[acct_count] = std.mem.eql(u8, acct_type, "revenue");
            debit_sums[acct_count] = stmt.columnInt64(2);
            credit_sums[acct_count] = stmt.columnInt64(3);
            acct_count += 1;
        }
    }

    if (phase_timer) |*timer| {
        profile.?.load_accounts_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    if (acct_count == 0) {
        const is_year_end_zero = try isYearEndPeriod(database, book_id, period_id, period_year, end_date);
        if (is_year_end_zero) {
            try closeDividendsDrawings(database, book_id, period_id, dividends_drawings_account_id, re_account_id, base_currency, end_date, period_number, period_year, 0, performed_by);
        }
        if (phase_timer) |*timer| {
            profile.?.close_entries_ns += timer.read();
            timer.* = try std.time.Timer.start();
        }
        // Even with no rev/exp activity, balance sheet balances must be carried
        // forward to the next period via an opening entry (Rule 1: everything
        // is a journal entry). Skipping this would orphan future periods from
        // their prior balances.
        generateOpeningEntry(database, book_id, period_id, end_date, base_currency, performed_by) catch |err| switch (err) {
            error.NoNextPeriod => {},
            else => return err,
        };
        if (phase_timer) |*timer| {
            profile.?.opening_entry_ns += timer.read();
            timer.* = try std.time.Timer.start();
        }
        if (std.mem.eql(u8, period_status, "open")) {
            try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
        }
        if (is_year_end_zero) {
            try period_mod.Period.transition(database, period_id, .closed, performed_by);
        }
        if (phase_timer) |*timer| {
            profile.?.transitions_ns += timer.read();
        }
        if (owns_txn) try database.commit();
        return;
    }

    var doc_buf: [48]u8 = undefined;

    // Re-close revision: count voided/reversed prior closing entries to derive
    // a unique suffix. First close = 0 (no suffix), re-close after reopen = 1, etc.
    const close_revision = try countPriorClosingRevisions(database, book_id, period_id);

    if (uses_allocations) {
        // Partnership/LLC: validate allocations, then split net income
        try book_mod.Book.validateEquityAllocations(database, book_id, end_date);
        try allocatedClose(database, book_id, period_id, base_currency, end_date, period_number, period_year, close_revision, account_ids[0..acct_count], debit_sums[0..acct_count], credit_sums[0..acct_count], is_revenue[0..acct_count], &doc_buf, performed_by);
    } else if (is_account_id > 0) {
        try twoStepClose(database, book_id, period_id, re_account_id, is_account_id, base_currency, end_date, period_number, period_year, close_revision, account_ids[0..acct_count], debit_sums[0..acct_count], credit_sums[0..acct_count], is_revenue[0..acct_count], &doc_buf, performed_by);
    } else {
        try directClose(database, book_id, period_id, re_account_id, base_currency, end_date, period_number, period_year, close_revision, account_ids[0..acct_count], debit_sums[0..acct_count], credit_sums[0..acct_count], &doc_buf, performed_by);
    }

    // Sprint D.1: Year-end detection determines whether this is a SOFT close
    // (mid-year monthly/quarterly) or HARD close (last period of fiscal year).
    // Soft closes transition to soft_closed (late entries allowed via reopen).
    // Hard closes additionally sweep dividends/drawings and transition to closed.
    const is_year_end = try isYearEndPeriod(database, book_id, period_id, period_year, end_date);

    if (is_year_end) {
        // Sprint D.3: Year-end dividends/drawings sweep.
        try closeDividendsDrawings(database, book_id, period_id, dividends_drawings_account_id, re_account_id, base_currency, end_date, period_number, period_year, close_revision, performed_by);
    }

    if (phase_timer) |*timer| {
        profile.?.close_entries_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    // Sprint C.1: Generate opening balance entry in the next period, if it exists.
    // Honors Rule 1 (everything is a journal entry) — the next period's opening
    // balances now appear as a real posted entry in the journal. The entry is
    // marked entry_type='opening' so post() skips cache updates and the existing
    // cumulative-query report architecture continues to work.
    generateOpeningEntry(database, book_id, period_id, end_date, base_currency, performed_by) catch |err| switch (err) {
        // If no next period exists, that's fine — application can create periods
        // later and call generateOpeningEntry manually. Any other error is a real bug.
        error.NoNextPeriod => {},
        else => return err,
    };

    if (phase_timer) |*timer| {
        profile.?.opening_entry_ns += timer.read();
        timer.* = try std.time.Timer.start();
    }

    if (std.mem.eql(u8, period_status, "open")) {
        try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
    }
    if (is_year_end) {
        try period_mod.Period.transition(database, period_id, .closed, performed_by);
    }

    if (phase_timer) |*timer| {
        profile.?.transitions_ns += timer.read();
    }

    if (owns_txn) try database.commit();
}

fn isYearEndPeriod(database: db.Database, book_id: i64, period_id: i64, period_year: i32, period_end_date: []const u8) !bool {
    var stmt = try database.prepare(
        \\SELECT COUNT(*) FROM ledger_periods
        \\WHERE book_id = ? AND year = ? AND id != ?
        \\  AND start_date > ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_year);
    try stmt.bindInt(3, period_id);
    try stmt.bindText(4, period_end_date);
    _ = try stmt.step();
    return stmt.columnInt(0) == 0;
}

fn closeDividendsDrawings(database: db.Database, book_id: i64, period_id: i64, div_account_id: i64, re_account_id: i64, base_currency: []const u8, end_date: []const u8, period_number: i32, period_year: i32, close_revision: i32, performed_by: []const u8) !void {
    if (div_account_id == 0) return;
    if (re_account_id <= 0) return;

    // Cumulative balance through end of this period (signed: +debit, -credit).
    var net: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(ab.debit_sum) - SUM(ab.credit_sum), 0)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND ab.account_id = ?
            \\  AND p.end_date <= ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, div_account_id);
        try stmt.bindText(3, end_date);
        _ = try stmt.step();
        net = stmt.columnInt64(0);
    }
    if (net == 0) return;

    var doc_buf: [48]u8 = undefined;
    const doc = formatCloseDoc(&doc_buf, "CLOSE-DIV", period_number, period_year, close_revision) catch unreachable;
    const entry_id = try entry_mod.Entry.createDraftAs(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"dividends_sweep\"}", .closing, performed_by);
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();

    if (net > 0) {
        // Dividends has a debit balance — credit it to zero, debit RE.
        _ = try appender.add(1, 0, net, base_currency, money.FX_RATE_SCALE, div_account_id, null, null);
        _ = try appender.add(2, net, 0, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
    } else {
        const abs_net = std.math.negate(net) catch return error.AmountOverflow;
        _ = try appender.add(1, abs_net, 0, base_currency, money.FX_RATE_SCALE, div_account_id, null, null);
        _ = try appender.add(2, 0, abs_net, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
    }

    try entry_mod.Entry.post(database, entry_id, performed_by);
}

fn countPriorClosingRevisions(database: db.Database, book_id: i64, period_id: i64) !i32 {
    var stmt = try database.prepare(
        \\SELECT COUNT(*) FROM ledger_entries
        \\WHERE book_id = ? AND period_id = ?
        \\  AND status IN ('void', 'reversed')
        \\  AND entry_type = 'closing';
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    _ = try stmt.step();
    return stmt.columnInt(0);
}

fn formatCloseDoc(buf: []u8, prefix: []const u8, period_number: i32, period_year: i32, revision: i32) ![]u8 {
    if (revision == 0) {
        return try std.fmt.bufPrint(buf, "{s}-P{d}-FY{d}", .{ prefix, period_number, period_year });
    }
    return try std.fmt.bufPrint(buf, "{s}-P{d}-FY{d}-R{d}", .{ prefix, period_number, period_year, revision });
}

fn directClose(database: db.Database, book_id: i64, period_id: i64, re_account_id: i64, base_currency: []const u8, end_date: []const u8, period_number: i32, period_year: i32, close_revision: i32, account_ids: []const i64, debit_sums: []const i64, credit_sums: []const i64, doc_buf: *[48]u8, performed_by: []const u8) !void {
    const doc_number = formatCloseDoc(doc_buf, "CLOSE", period_number, period_year, close_revision) catch unreachable;

    const entry_id = try entry_mod.Entry.createDraftAs(database, book_id, doc_number, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"direct\"}", .closing, performed_by);
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();

    var line_num: i32 = 1;
    var re_debit_total: i64 = 0;
    var re_credit_total: i64 = 0;

    for (account_ids, debit_sums, credit_sums) |acct_id, ds, cs| {
        if (ds > cs) {
            const amount = std.math.sub(i64, ds, cs) catch return error.AmountOverflow;
            _ = try appender.add(line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
            line_num += 1;
            re_debit_total = std.math.add(i64, re_debit_total, amount) catch return error.AmountOverflow;
        } else if (cs > ds) {
            const amount = std.math.sub(i64, cs, ds) catch return error.AmountOverflow;
            _ = try appender.add(line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
            line_num += 1;
            re_credit_total = std.math.add(i64, re_credit_total, amount) catch return error.AmountOverflow;
        }
    }

    if (re_debit_total > 0) {
        _ = try appender.add(line_num, re_debit_total, 0, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
        line_num += 1;
    }
    if (re_credit_total > 0) {
        _ = try appender.add(line_num, 0, re_credit_total, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
        line_num += 1;
    }

    try entry_mod.Entry.post(database, entry_id, performed_by);
}

/// Closes all revenue and expense accounts into the income summary account in a
/// single netted entry. Account-level close lines are preserved, while the
/// income-summary side is reduced to at most one debit and one credit line.
fn closeToIncomeSummary(database: db.Database, book_id: i64, entry_id: i64, base_currency: []const u8, is_account_id: i64, account_ids: []const i64, debit_sums: []const i64, credit_sums: []const i64, is_revenue_flags: []const bool, performed_by: []const u8) !void {
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();
    var line_num: i32 = 1;
    var has_lines = false;
    var is_debit_total: i64 = 0;
    var is_credit_total: i64 = 0;
    for (account_ids, debit_sums, credit_sums, is_revenue_flags) |acct_id, ds, cs, is_rev| {
        if (ds == cs) continue;
        has_lines = true;
        if (is_rev) {
            if (cs > ds) {
                const amount = std.math.sub(i64, cs, ds) catch return error.AmountOverflow;
                _ = try appender.add(line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                is_credit_total = std.math.add(i64, is_credit_total, amount) catch return error.AmountOverflow;
            } else {
                const amount = std.math.sub(i64, ds, cs) catch return error.AmountOverflow;
                _ = try appender.add(line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                is_debit_total = std.math.add(i64, is_debit_total, amount) catch return error.AmountOverflow;
            }
        } else {
            if (ds > cs) {
                const amount = std.math.sub(i64, ds, cs) catch return error.AmountOverflow;
                _ = try appender.add(line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                is_debit_total = std.math.add(i64, is_debit_total, amount) catch return error.AmountOverflow;
            } else {
                const amount = std.math.sub(i64, cs, ds) catch return error.AmountOverflow;
                _ = try appender.add(line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                is_credit_total = std.math.add(i64, is_credit_total, amount) catch return error.AmountOverflow;
            }
        }
    }
    if (is_debit_total > 0) {
        _ = try appender.add(line_num, is_debit_total, 0, base_currency, money.FX_RATE_SCALE, is_account_id, null, null);
        line_num += 1;
    }
    if (is_credit_total > 0) {
        _ = try appender.add(line_num, 0, is_credit_total, base_currency, money.FX_RATE_SCALE, is_account_id, null, null);
    }
    if (has_lines) {
        try entry_mod.Entry.post(database, entry_id, performed_by);
    } else {
        try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
    }
}

fn twoStepClose(database: db.Database, book_id: i64, period_id: i64, re_account_id: i64, is_account_id: i64, base_currency: []const u8, end_date: []const u8, period_number: i32, period_year: i32, close_revision: i32, account_ids: []const i64, debit_sums: []const i64, credit_sums: []const i64, is_revenue_flags: []const bool, doc_buf: *[48]u8, performed_by: []const u8) !void {
    // Step 1: Close all revenue and expense accounts to income summary.
    {
        const doc = formatCloseDoc(doc_buf, "CLOSE-ISUM", period_number, period_year, close_revision) catch unreachable;
        const entry_id = try entry_mod.Entry.createDraftAs(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"income_summary\",\"step\":1}", .closing, performed_by);
        try closeToIncomeSummary(database, book_id, entry_id, base_currency, is_account_id, account_ids, debit_sums, credit_sums, is_revenue_flags, performed_by);
    }

    // Step 2: Close income summary to retained earnings.
    {
        const doc = formatCloseDoc(doc_buf, "CLOSE-IS", period_number, period_year, close_revision) catch unreachable;
        const entry_id = try entry_mod.Entry.createDraftAs(database, book_id, doc, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"income_summary\",\"step\":2}", .closing, performed_by);
        var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
        defer appender.deinit();

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
            const amount = std.math.sub(i64, is_credit, is_debit) catch return error.AmountOverflow;
            _ = try appender.add(1, amount, 0, base_currency, money.FX_RATE_SCALE, is_account_id, null, null);
            _ = try appender.add(2, 0, amount, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else if (is_debit > is_credit) {
            const amount = std.math.sub(i64, is_debit, is_credit) catch return error.AmountOverflow;
            _ = try appender.add(1, amount, 0, base_currency, money.FX_RATE_SCALE, re_account_id, null, null);
            _ = try appender.add(2, 0, amount, base_currency, money.FX_RATE_SCALE, is_account_id, null, null);
            try entry_mod.Entry.post(database, entry_id, performed_by);
        } else {
            try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
        }
    }
}

/// Closing path for partnerships and LLCs. Computes net income across
/// all revenue and expense accounts, then allocates the net to each
/// partner/member capital account per the active equity allocations.
///
/// Line structure:
///   1..N: Close each revenue/expense account (one line per account, netted)
///   N+1..N+M: Credit/debit each partner capital account by their allocation share
///
/// The last allocation absorbs the rounding residual to ensure:
///   sum(partner_shares) == total_net_income (exactly)
fn allocatedClose(
    database: db.Database,
    book_id: i64,
    period_id: i64,
    base_currency: []const u8,
    end_date: []const u8,
    period_number: i32,
    period_year: i32,
    close_revision: i32,
    account_ids: []const i64,
    debit_sums: []const i64,
    credit_sums: []const i64,
    is_revenue_flags: []const bool,
    doc_buf: *[48]u8,
    performed_by: []const u8,
) !void {
    const doc_number = formatCloseDoc(doc_buf, "CLOSE", period_number, period_year, close_revision) catch unreachable;
    const entry_id = try entry_mod.Entry.createDraftAs(database, book_id, doc_number, end_date, end_date, null, period_id, "{\"closing_entry\":true,\"method\":\"allocated\"}", .closing, performed_by);
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();

    var line_num: i32 = 1;
    var net_income: i64 = 0; // credit side minus debit side, signed

    // First pass: close revenue/expense accounts to the entry and compute net income
    for (account_ids, debit_sums, credit_sums, is_revenue_flags) |acct_id, ds, cs, is_rev| {
        if (ds == cs) continue;
        if (is_rev) {
            // Revenue: credit balance. Close by debiting revenue.
            if (cs > ds) {
                const amount = std.math.sub(i64, cs, ds) catch return error.AmountOverflow;
                _ = try appender.add(line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                net_income = std.math.add(i64, net_income, amount) catch return error.AmountOverflow;
            } else {
                // Unusual: revenue with debit balance (e.g., sales returns net > sales). Credit it.
                const amount = std.math.sub(i64, ds, cs) catch return error.AmountOverflow;
                _ = try appender.add(line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                net_income = std.math.sub(i64, net_income, amount) catch return error.AmountOverflow;
            }
        } else {
            // Expense: debit balance. Close by crediting expense.
            if (ds > cs) {
                const amount = std.math.sub(i64, ds, cs) catch return error.AmountOverflow;
                _ = try appender.add(line_num, 0, amount, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                net_income = std.math.sub(i64, net_income, amount) catch return error.AmountOverflow;
            } else {
                // Unusual: expense with credit balance (e.g., purchase returns > purchases). Debit it.
                const amount = std.math.sub(i64, cs, ds) catch return error.AmountOverflow;
                _ = try appender.add(line_num, amount, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
                line_num += 1;
                net_income = std.math.add(i64, net_income, amount) catch return error.AmountOverflow;
            }
        }
    }

    // If no net income/loss, no allocation needed; delete or post accordingly
    if (net_income == 0) {
        if (line_num == 1) {
            // No lines added (all zero-activity accounts). Delete the draft.
            try entry_mod.Entry.deleteDraft(database, entry_id, performed_by);
            return;
        }
        // Lines exist but they net to zero. Post with no partner allocation.
        try entry_mod.Entry.post(database, entry_id, performed_by);
        return;
    }

    // Second pass: allocate net_income to partner capital accounts
    // Read active allocations at end_date
    const MaxAllocations = 64;
    var alloc_account_ids: [MaxAllocations]i64 = undefined;
    var alloc_values: [MaxAllocations]i64 = undefined;
    var alloc_count: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT account_id, allocation_value
            \\FROM ledger_equity_allocations
            \\WHERE book_id = ?
            \\  AND effective_date <= ?
            \\  AND (end_date IS NULL OR end_date >= ?)
            \\  AND allocation_type = 'percentage'
            \\ORDER BY id ASC;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, end_date);
        try stmt.bindText(3, end_date);
        while (try stmt.step()) {
            if (alloc_count >= MaxAllocations) return error.TooManyAccounts;
            alloc_account_ids[alloc_count] = stmt.columnInt64(0);
            alloc_values[alloc_count] = stmt.columnInt64(1);
            alloc_count += 1;
        }
    }

    // Distribute net_income across partners. The last partner absorbs the
    // rounding residual: instead of independently computing its share, we
    // compute (total - sum_so_far) to guarantee exact total.
    var allocated_so_far: i64 = 0;
    for (alloc_account_ids[0..alloc_count], alloc_values[0..alloc_count], 0..) |acct_id, pct, i| {
        const is_last = (i == alloc_count - 1);
        var share: i64 = undefined;
        if (is_last) {
            share = std.math.sub(i64, net_income, allocated_so_far) catch return error.AmountOverflow;
        } else {
            // share = net_income * pct / 10000 (use i128 for overflow safety)
            const wide = @as(i128, net_income) * @as(i128, pct);
            const quot = @divTrunc(wide, 10000);
            if (quot > std.math.maxInt(i64) or quot < std.math.minInt(i64)) return error.AmountOverflow;
            // quot was just range-checked against i64 bounds — @intCast cannot panic.
            share = @intCast(quot);
            allocated_so_far = std.math.add(i64, allocated_so_far, share) catch return error.AmountOverflow;
        }

        if (share == 0) continue;

        // Positive net income → credit partner capital (equity increases)
        // Negative net income (loss) → debit partner capital (equity decreases)
        if (share > 0) {
            _ = try appender.add(line_num, 0, share, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
        } else {
            const abs_share = std.math.negate(share) catch return error.AmountOverflow;
            _ = try appender.add(line_num, abs_share, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
        }
        line_num += 1;
    }

    try entry_mod.Entry.post(database, entry_id, performed_by);
}

/// Sprint C.1: Generate an opening balance entry in the period immediately
/// following the just-closed period. Brings forward all balance sheet
/// account cumulative balances as of the closed period's end.
///
/// The entry is tagged with metadata {"opening_entry":true,"source_period_id":N}
/// so post() skips the cache update and the existing cumulative-query report
/// architecture continues to work correctly. The entry exists solely for
/// audit trail completeness (Rule 1).
///
/// Returns error.NoNextPeriod if there is no period after the closed one.
/// Callers should treat this as a non-error (the application will create
/// the next period later and can call this function manually).
pub fn generateOpeningEntry(database: db.Database, book_id: i64, closed_period_id: i64, closed_period_end_date: []const u8, base_currency: []const u8, performed_by: []const u8) !void {
    // 1. Find the next period by start_date > closed_period.end_date
    var next_period_id: i64 = 0;
    var next_start_date_buf: [11]u8 = undefined;
    var next_start_date_len: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT id, start_date FROM ledger_periods
            \\WHERE book_id = ?
            \\  AND start_date > ?
            \\  AND status IN ('open', 'soft_closed')
            \\ORDER BY start_date ASC LIMIT 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, closed_period_end_date);
        const has_row = try stmt.step();
        if (!has_row) return error.NoNextPeriod;
        next_period_id = stmt.columnInt64(0);
        const sd = stmt.columnText(1).?;
        next_start_date_len = @min(sd.len, next_start_date_buf.len);
        @memcpy(next_start_date_buf[0..next_start_date_len], sd[0..next_start_date_len]);
    }
    const next_start_date = next_start_date_buf[0..next_start_date_len];

    // 2. Check if the next period already has an opening entry (shouldn't
    //    happen in normal flow but prevents duplicates on retry/reopen)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries
            \\WHERE book_id = ? AND period_id = ? AND status = 'posted'
            \\  AND entry_type = 'opening';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, next_period_id);
        _ = try stmt.step();
        // status='posted' filter is load-bearing: cascadeReopen voids
        // the prior opening entry, so this check correctly misses after
        // reopen and allows a fresh opening entry to be generated.
        if (stmt.columnInt(0) > 0) return;
    }

    // 3. Query all balance sheet accounts with non-zero cumulative balance
    //    through the end of the closed period. This uses the same cumulative
    //    SUM pattern as the trial balance, scoped to balance sheet accounts.
    const MaxAccounts = 2000;
    var bs_account_ids: [MaxAccounts]i64 = undefined;
    var bs_net_amounts: [MaxAccounts]i64 = undefined; // positive = debit balance, negative = credit balance
    var bs_count: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT a.id, SUM(ab.debit_sum) - SUM(ab.credit_sum) AS net
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ?
            \\  AND p.end_date <= ?
            \\  AND a.account_type IN ('asset', 'liability', 'equity')
            \\GROUP BY a.id
            \\HAVING SUM(ab.debit_sum) != 0 OR SUM(ab.credit_sum) != 0
            \\ORDER BY a.number;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, closed_period_end_date);
        while (try stmt.step()) {
            if (bs_count >= MaxAccounts) return error.TooManyAccounts;
            bs_account_ids[bs_count] = stmt.columnInt64(0);
            bs_net_amounts[bs_count] = stmt.columnInt64(1);
            bs_count += 1;
        }
    }

    if (bs_count == 0) return; // nothing to bring forward

    // 4. Create the opening entry
    var doc_buf: [48]u8 = undefined;
    var meta_buf: [80]u8 = undefined;
    // Count any prior opening entries (including void/reversed) for this next
    // period so re-close after reopen produces a unique document_number.
    var open_revision: i32 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries
            \\WHERE book_id = ? AND period_id = ?
            \\  AND entry_type = 'opening';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, next_period_id);
        _ = try stmt.step();
        open_revision = stmt.columnInt(0);
    }
    const doc = if (open_revision == 0)
        std.fmt.bufPrint(&doc_buf, "OPEN-P{d}", .{next_period_id}) catch unreachable
    else
        std.fmt.bufPrint(&doc_buf, "OPEN-P{d}-R{d}", .{ next_period_id, open_revision }) catch unreachable;
    const metadata = std.fmt.bufPrint(&meta_buf, "{{\"opening_entry\":true,\"source_period_id\":{d}}}", .{closed_period_id}) catch unreachable;

    const entry_id = try entry_mod.Entry.createDraftAs(
        database,
        book_id,
        doc,
        next_start_date,
        next_start_date,
        null,
        next_period_id,
        metadata,
        .opening,
        performed_by,
    );
    var appender = try entry_mod.Entry.LineAppender.init(database, entry_id, book_id, performed_by);
    defer appender.deinit();

    // 5. Add one line per balance sheet account, debit or credit based on sign
    var line_num: i32 = 1;
    for (bs_account_ids[0..bs_count], bs_net_amounts[0..bs_count]) |acct_id, net| {
        if (net == 0) continue;
        if (net > 0) {
            // Debit balance → debit the account in the opening entry
            _ = try appender.add(line_num, net, 0, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
        } else {
            // Credit balance → credit the account in the opening entry
            const abs_net = std.math.negate(net) catch return error.AmountOverflow;
            _ = try appender.add(line_num, 0, abs_net, base_currency, money.FX_RATE_SCALE, acct_id, null, null);
        }
        line_num += 1;
    }

    // 6. Post the entry. Because of the "opening_entry":true metadata tag,
    //    post() will skip the cache update and future-stale marking.
    try entry_mod.Entry.post(database, entry_id, performed_by);
}

/// Find the opening entry for a given period, if any. Returns the entry ID
/// or null if the period has no opening entry. Used by period reopening
/// cascade (Sprint C.2) to void the entry before allowing reopen.
pub fn findOpeningEntry(database: db.Database, book_id: i64, period_id: i64) !?i64 {
    var stmt = try database.prepare(
        \\SELECT id FROM ledger_entries
        \\WHERE book_id = ? AND period_id = ? AND status = 'posted'
        \\  AND entry_type = 'opening'
        \\LIMIT 1;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    const has_row = try stmt.step();
    if (!has_row) return null;
    return stmt.columnInt64(0);
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
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND entry_type = 'closing';");
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

test "closePeriod: closes dividends/drawings to RE at year-end" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    // Add Feb 2026 — Jan is now NOT year-end, Feb IS year-end (no later period in FY 2026).
    const feb_id = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Designate dividends/drawings (contra-equity, normal debit balance)
    const div_id = try account_mod.Account.create(s.database, s.book_id, "3300", "Dividends Declared", .equity, true, "admin");
    try book_mod.Book.setDividendsDrawingsAccount(s.database, s.book_id, div_id, "admin");

    // Post a dividend in Jan: debit Dividends 1000, credit Cash 1000
    try postTestEntry(s.database, s.book_id, "DIV-001", div_id, 1_000_000_000_00, s.cash_id, 1_000_000_000_00, s.period_id);

    // Close Jan — NOT year-end. Dividends should still have a non-zero cumulative balance.
    try closePeriod(s.database, s.book_id, s.period_id, "admin");
    {
        var stmt = try s.database.prepare("SELECT COALESCE(SUM(debit_sum) - SUM(credit_sum), 0) FROM ledger_account_balances WHERE account_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, div_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    }

    // Close Feb — YEAR-END. Dividends should be swept to RE.
    try closePeriod(s.database, s.book_id, feb_id, "admin");

    // After year-end close, the cumulative dividends balance must be zero
    // (closing entry credited Dividends, debited RE).
    {
        var stmt = try s.database.prepare("SELECT COALESCE(SUM(debit_sum) - SUM(credit_sum), 0) FROM ledger_account_balances WHERE account_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, div_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    }
}

test "closePeriod: re-closing reopened period uses revision suffix" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    try period_mod.Period.transitionWithReason(s.database, s.period_id, .open, "audit correction", "admin");

    // Re-close should succeed and produce a distinct doc number, not collide
    // with the original closing entry's UNIQUE document_number constraint.
    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    var stmt = try s.database.prepare(
        \\SELECT COUNT(DISTINCT document_number) FROM ledger_entries
        \\WHERE book_id = ? AND period_id = ? AND entry_type = 'closing';
    );
    defer stmt.finalize();
    try stmt.bindInt(1, s.book_id);
    try stmt.bindInt(2, s.period_id);
    _ = try stmt.step();
    try std.testing.expect(stmt.columnInt(0) >= 2);
}

test "closePeriod: zero rev/exp period still generates opening entry for next period" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const period2_id = try period_mod.Period.create(s.database, s.book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Post a balance sheet only entry: Cash debit / RE credit. No revenue/expense.
    try postTestEntry(s.database, s.book_id, "BS-001", s.cash_id, 5_000_000_000_00, s.re_id, 5_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    // Bug: zero rev/exp early-return skipped generateOpeningEntry. Period 2 must
    // still receive an opening entry bringing forward Cash and RE balances.
    const opening = try findOpeningEntry(s.database, s.book_id, period2_id);
    try std.testing.expect(opening != null);
}

test "closePeriod: rejects when suspense account has non-zero balance" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    // Designate a suspense account and post an entry that parks a balance there.
    const suspense_id = try account_mod.Account.create(s.database, s.book_id, "1999", "Suspense", .asset, false, "admin");
    try book_mod.Book.setSuspenseAccount(s.database, s.book_id, suspense_id, "admin");

    try postTestEntry(s.database, s.book_id, "SUSP-001", suspense_id, 1_000_000_000_00, s.cash_id, 1_000_000_000_00, s.period_id);

    // Close must refuse: unresolved suspense items are a control failure.
    const result = closePeriod(s.database, s.book_id, s.period_id, "admin");
    try std.testing.expectError(error.SuspenseNotClear, result);
}

test "closePeriod: accepts when suspense account has zero balance" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const suspense_id = try account_mod.Account.create(s.database, s.book_id, "1999", "Suspense", .asset, false, "admin");
    try book_mod.Book.setSuspenseAccount(s.database, s.book_id, suspense_id, "admin");

    // Park then clear: suspense ends at zero, close should succeed.
    try postTestEntry(s.database, s.book_id, "SUSP-001", suspense_id, 1_000_000_000_00, s.cash_id, 1_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "SUSP-002", s.cash_id, 1_000_000_000_00, suspense_id, 1_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");
}

test "closePeriod: rejects when cache trial balance is out of balance" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    {
        var stmt = try s.database.prepare(
            \\UPDATE ledger_account_balances SET debit_sum = debit_sum + 1
            \\WHERE book_id = ? AND period_id = ? AND account_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        try stmt.bindInt(2, s.period_id);
        try stmt.bindInt(3, s.cash_id);
        _ = try stmt.step();
    }

    {
        var stmt = try s.database.prepare("UPDATE ledger_account_balances SET is_stale = 0 WHERE book_id = ? AND period_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        try stmt.bindInt(2, s.period_id);
        _ = try stmt.step();
    }

    const result = closePeriod(s.database, s.book_id, s.period_id, "admin");
    try std.testing.expectError(error.PeriodNotInBalance, result);
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
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
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
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND entry_type = 'closing';");
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

test "closePeriod: two-step close with only revenue (no expenses)" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const is_id = try account_mod.Account.create(s.database, s.book_id, "3200", "Income Summary", .equity, false, "admin");
    try book_mod.Book.setIncomeSummaryAccount(s.database, s.book_id, is_id, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 8_000_000_000_00, s.revenue_id, 8_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND metadata LIKE '%income_summary%' AND status = 'posted';");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }

    const rev_bal = try queryBalance(s.database, s.revenue_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.credit_sum - rev_bal.debit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 8_000_000_000_00), re_bal.credit_sum - re_bal.debit_sum);
}

test "closePeriod: two-step close with only expenses (no revenue)" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const is_id = try account_mod.Account.create(s.database, s.book_id, "3200", "Income Summary", .equity, false, "admin");
    try book_mod.Book.setIncomeSummaryAccount(s.database, s.book_id, is_id, "admin");

    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 5_000_000_000_00, s.cash_id, 5_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    {
        var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND metadata LIKE '%income_summary%' AND status = 'posted';");
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }

    const exp_bal = try queryBalance(s.database, s.expense_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.debit_sum - exp_bal.credit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), re_bal.debit_sum - re_bal.credit_sum);
}

test "closePeriod: direct close with contra revenue account" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const contra_rev_id = try account_mod.Account.create(s.database, s.book_id, "4100", "Sales Returns", .revenue, true, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "RET-001", contra_rev_id, 2_000_000_000_00, s.cash_id, 2_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 3_000_000_000_00, s.cash_id, 3_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const contra_bal = try queryBalance(s.database, contra_rev_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), contra_bal.debit_sum - contra_bal.credit_sum);

    const rev_bal = try queryBalance(s.database, s.revenue_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.credit_sum - rev_bal.debit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const expected_net: i64 = (10_000_000_000_00 - 2_000_000_000_00) - 3_000_000_000_00;
    try std.testing.expectEqual(expected_net, re_bal.credit_sum - re_bal.debit_sum);
}

test "closePeriod: direct close with contra expense account" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const contra_exp_id = try account_mod.Account.create(s.database, s.book_id, "5100", "Purchase Returns", .expense, true, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 7_000_000_000_00, s.cash_id, 7_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "PR-001", s.cash_id, 1_000_000_000_00, contra_exp_id, 1_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const contra_bal = try queryBalance(s.database, contra_exp_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), contra_bal.credit_sum - contra_bal.debit_sum);

    const exp_bal = try queryBalance(s.database, s.expense_id, s.period_id);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.debit_sum - exp_bal.credit_sum);

    const re_bal = try queryBalance(s.database, s.re_id, s.period_id);
    const expected_net: i64 = 10_000_000_000_00 - (7_000_000_000_00 - 1_000_000_000_00);
    try std.testing.expectEqual(expected_net, re_bal.credit_sum - re_bal.debit_sum);
}

test "closePeriod: balance sheet shows correct RE after close" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    const report_mod = @import("report.zig");
    const result = try report_mod.balanceSheetWithProjectedRE(s.database, s.book_id, "2026-01-31", "2026-01-01");
    defer result.deinit();

    var re_credit: i64 = 0;
    var phantom_ni_count: u32 = 0;
    for (result.rows) |row| {
        if (row.account_id == s.re_id) {
            re_credit += row.credit_balance;
            re_credit -= row.debit_balance;
        }
    }
    for (result.rows) |row| {
        const name = row.account_name[0..row.account_name_len];
        if (std.mem.indexOf(u8, name, "Net Income") != null and row.account_id != s.re_id) {
            phantom_ni_count += 1;
        }
    }

    const net_income: i64 = 10_000_000_000_00 - 6_000_000_000_00;
    try std.testing.expectEqual(net_income, re_credit);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
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

test "closePeriod: direct close netted line count is N+1 for uniform direction" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const rev2_id = try account_mod.Account.create(s.database, s.book_id, "4002", "Service Revenue", .revenue, false, "admin");
    const rev3_id = try account_mod.Account.create(s.database, s.book_id, "4003", "Interest Revenue", .revenue, false, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 5_000_000_000_00, s.revenue_id, 5_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "REV-002", s.cash_id, 3_000_000_000_00, rev2_id, 3_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "REV-003", s.cash_id, 1_000_000_000_00, rev3_id, 1_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    // 3 revenue accounts + 1 netted RE line = 4 lines (not 6)
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.entry_type = 'closing';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
    }
}

test "closePeriod: direct close netted line count N+2 for mixed revenue and expense" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 10_000_000_000_00, s.revenue_id, 10_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 6_000_000_000_00, s.cash_id, 6_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    // 1 revenue + 1 expense + 2 RE lines (one debit, one credit) = 4 lines (not 4 old either, but structure is different)
    // Revenue account gets debited (close credit balance) -> RE gets credited
    // Expense account gets credited (close debit balance) -> RE gets debited
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.entry_type = 'closing';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
    }

    // Verify the RE lines are netted (not paired per account)
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.entry_type = 'closing' AND el.account_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        try stmt.bindInt(2, s.re_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }
}

test "closePeriod: two-step close netted line count" {
    const s = try setupCloseTestDb();
    defer s.database.close();

    const is_id = try account_mod.Account.create(s.database, s.book_id, "3200", "Income Summary", .equity, false, "admin");
    try book_mod.Book.setIncomeSummaryAccount(s.database, s.book_id, is_id, "admin");

    const rev2_id = try account_mod.Account.create(s.database, s.book_id, "4002", "Service Revenue", .revenue, false, "admin");

    try postTestEntry(s.database, s.book_id, "REV-001", s.cash_id, 5_000_000_000_00, s.revenue_id, 5_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "REV-002", s.cash_id, 3_000_000_000_00, rev2_id, 3_000_000_000_00, s.period_id);
    try postTestEntry(s.database, s.book_id, "EXP-001", s.expense_id, 4_000_000_000_00, s.cash_id, 4_000_000_000_00, s.period_id);

    try closePeriod(s.database, s.book_id, s.period_id, "admin");

    // Step 1 (CLOSE-ISUM): 2 revenue accounts + 1 expense account + 2 IS lines = 5 lines
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.metadata LIKE '%"step":1%';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 5), stmt.columnInt(0));
    }

    // Step 2 (CLOSE-IS): always 2 lines (IS -> RE)
    {
        var stmt = try s.database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.metadata LIKE '%"step":2%';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, s.book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }
}

// ── Sprint A.5: Entity type validation in closePeriod ─────────

test "closePeriod: partnership without allocations fails with EquityAllocationRequired" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "ABC Partners", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .partnership, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "3100", "Partner Capital", .equity, false, "admin");
    // Note: we DON'T designate retained_earnings_account_id because partnerships
    // use the allocation table instead.

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try postTestEntry(database, book_id, "REV-001", cash_id, 10_000_000_000_00, revenue_id, 10_000_000_000_00, period_id);

    const result = close_partnership_helper(database, book_id, period_id);
    try std.testing.expectError(error.EquityAllocationRequired, result);
}

fn close_partnership_helper(database: db.Database, book_id: i64, period_id: i64) !void {
    return closePeriod(database, book_id, period_id, "admin");
}

test "closePeriod: partnership with 50/30/20 allocation splits net income" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "XYZ Partners", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .partnership, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const expense_id = try account_mod.Account.create(database, book_id, "5001", "Expense", .expense, false, "admin");
    const partner_a = try account_mod.Account.create(database, book_id, "3101", "Partner A Capital", .equity, false, "admin");
    const partner_b = try account_mod.Account.create(database, book_id, "3102", "Partner B Capital", .equity, false, "admin");
    const partner_c = try account_mod.Account.create(database, book_id, "3103", "Partner C Capital", .equity, false, "admin");

    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_a, "Partner A", 5000, "2026-01-01", "admin"); // 50%
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_b, "Partner B", 3000, "2026-01-01", "admin"); // 30%
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_c, "Partner C", 2000, "2026-01-01", "admin"); // 20%

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Revenue 10,000 - Expense 4,000 = Net Income 6,000
    try postTestEntry(database, book_id, "REV-001", cash_id, 1_000_000_000_000, revenue_id, 1_000_000_000_000, period_id);
    try postTestEntry(database, book_id, "EXP-001", expense_id, 400_000_000_000, cash_id, 400_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    // Partner A: 50% × 6,000 = 3,000
    const a_bal = try queryBalance(database, partner_a, period_id);
    try std.testing.expectEqual(@as(i64, 300_000_000_000), a_bal.credit_sum - a_bal.debit_sum);

    // Partner B: 30% × 6,000 = 1,800
    const b_bal = try queryBalance(database, partner_b, period_id);
    try std.testing.expectEqual(@as(i64, 180_000_000_000), b_bal.credit_sum - b_bal.debit_sum);

    // Partner C: 20% × 6,000 = 1,200
    const c_bal = try queryBalance(database, partner_c, period_id);
    try std.testing.expectEqual(@as(i64, 120_000_000_000), c_bal.credit_sum - c_bal.debit_sum);

    // Revenue and expense zeroed
    const rev_bal = try queryBalance(database, revenue_id, period_id);
    try std.testing.expectEqual(@as(i64, 0), rev_bal.credit_sum - rev_bal.debit_sum);
    const exp_bal = try queryBalance(database, expense_id, period_id);
    try std.testing.expectEqual(@as(i64, 0), exp_bal.debit_sum - exp_bal.credit_sum);
}

test "closePeriod: partnership with 50/50 allocation on net loss splits equally" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Partners", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .partnership, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const expense_id = try account_mod.Account.create(database, book_id, "5001", "Expense", .expense, false, "admin");
    const partner_a = try account_mod.Account.create(database, book_id, "3101", "Partner A", .equity, false, "admin");
    const partner_b = try account_mod.Account.create(database, book_id, "3102", "Partner B", .equity, false, "admin");

    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_a, "Partner A", 5000, "2026-01-01", "admin");
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_b, "Partner B", 5000, "2026-01-01", "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Net loss: Revenue 3,000 - Expense 5,000 = -2,000
    try postTestEntry(database, book_id, "REV-001", cash_id, 300_000_000_000, revenue_id, 300_000_000_000, period_id);
    try postTestEntry(database, book_id, "EXP-001", expense_id, 500_000_000_000, cash_id, 500_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    // Each partner absorbs -1,000 (debit balance of 100,000,000,000)
    const a_bal = try queryBalance(database, partner_a, period_id);
    try std.testing.expectEqual(@as(i64, 100_000_000_000), a_bal.debit_sum - a_bal.credit_sum);

    const b_bal = try queryBalance(database, partner_b, period_id);
    try std.testing.expectEqual(@as(i64, 100_000_000_000), b_bal.debit_sum - b_bal.credit_sum);
}

test "closePeriod: partnership rounding residual absorbed by last partner" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Partners", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .partnership, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const partner_a = try account_mod.Account.create(database, book_id, "3101", "Partner A", .equity, false, "admin");
    const partner_b = try account_mod.Account.create(database, book_id, "3102", "Partner B", .equity, false, "admin");
    const partner_c = try account_mod.Account.create(database, book_id, "3103", "Partner C", .equity, false, "admin");

    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_a, "A", 3333, "2026-01-01", "admin");
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_b, "B", 3333, "2026-01-01", "admin");
    _ = try book_mod.Book.addEquityAllocation(database, book_id, partner_c, "C", 3334, "2026-01-01", "admin"); // last gets 33.34 to sum to 100.00

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Net income = 100 (in base units: 100_00000000 = 10,000,000,000)
    try postTestEntry(database, book_id, "REV-001", cash_id, 10_000_000_000, revenue_id, 10_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    // A: 3333/10000 × 10_000_000_000 = 3_333_000_000
    // B: 3333/10000 × 10_000_000_000 = 3_333_000_000
    // C: 10_000_000_000 - 3_333_000_000 - 3_333_000_000 = 3_334_000_000 (absorbs residual)
    const a_bal = try queryBalance(database, partner_a, period_id);
    const b_bal = try queryBalance(database, partner_b, period_id);
    const c_bal = try queryBalance(database, partner_c, period_id);

    const a_net = a_bal.credit_sum - a_bal.debit_sum;
    const b_net = b_bal.credit_sum - b_bal.debit_sum;
    const c_net = c_bal.credit_sum - c_bal.debit_sum;

    // Sum must equal the total net income exactly
    try std.testing.expectEqual(@as(i64, 10_000_000_000), a_net + b_net + c_net);
    // A and B get the proportional share
    try std.testing.expectEqual(@as(i64, 3_333_000_000), a_net);
    try std.testing.expectEqual(@as(i64, 3_333_000_000), b_net);
    // C absorbs the residual
    try std.testing.expectEqual(@as(i64, 3_334_000_000), c_net);
}

test "closePeriod: llc same allocation behavior as partnership" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "XYZ LLC", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .llc, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Revenue", .revenue, false, "admin");
    const member_1 = try account_mod.Account.create(database, book_id, "3101", "Member 1 Capital", .equity, false, "admin");
    const member_2 = try account_mod.Account.create(database, book_id, "3102", "Member 2 Capital", .equity, false, "admin");

    _ = try book_mod.Book.addEquityAllocation(database, book_id, member_1, "Member 1", 6000, "2026-01-01", "admin"); // 60%
    _ = try book_mod.Book.addEquityAllocation(database, book_id, member_2, "Member 2", 4000, "2026-01-01", "admin"); // 40%

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Net income 1,000
    try postTestEntry(database, book_id, "REV-001", cash_id, 100_000_000_000, revenue_id, 100_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    const m1_bal = try queryBalance(database, member_1, period_id);
    try std.testing.expectEqual(@as(i64, 60_000_000_000), m1_bal.credit_sum - m1_bal.debit_sum);

    const m2_bal = try queryBalance(database, member_2, period_id);
    try std.testing.expectEqual(@as(i64, 40_000_000_000), m2_bal.credit_sum - m2_bal.debit_sum);
}

test "closePeriod: sole proprietorship uses single-target close to Owner's Capital" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Juan dela Cruz", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .sole_proprietorship, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Sales", .revenue, false, "admin");
    const expense_id = try account_mod.Account.create(database, book_id, "5001", "Expenses", .expense, false, "admin");
    const owner_cap = try account_mod.Account.create(database, book_id, "3000", "Owner's Capital", .equity, false, "admin");

    try book_mod.Book.setEquityCloseTarget(database, book_id, owner_cap, "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try postTestEntry(database, book_id, "REV-001", cash_id, 500_000_000_000, revenue_id, 500_000_000_000, period_id);
    try postTestEntry(database, book_id, "EXP-001", expense_id, 200_000_000_000, cash_id, 200_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    // Owner's Capital increases by net income (300)
    const oc_bal = try queryBalance(database, owner_cap, period_id);
    try std.testing.expectEqual(@as(i64, 300_000_000_000), oc_bal.credit_sum - oc_bal.debit_sum);
}

test "closePeriod: nonprofit uses single-target close to Net Assets Without Restrictions" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Heft Foundation", "PHP", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .nonprofit, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1001", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4001", "Donations", .revenue, false, "admin");
    const expense_id = try account_mod.Account.create(database, book_id, "5001", "Programs", .expense, false, "admin");
    const na_no_restrict = try account_mod.Account.create(database, book_id, "3000", "Net Assets Without Restrictions", .equity, false, "admin");

    try book_mod.Book.setEquityCloseTarget(database, book_id, na_no_restrict, "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try postTestEntry(database, book_id, "DON-001", cash_id, 1_000_000_000_000, revenue_id, 1_000_000_000_000, period_id);
    try postTestEntry(database, book_id, "PROG-001", expense_id, 600_000_000_000, cash_id, 600_000_000_000, period_id);

    try closePeriod(database, book_id, period_id, "admin");

    // Net Assets increases by change in net assets (400)
    const na_bal = try queryBalance(database, na_no_restrict, period_id);
    try std.testing.expectEqual(@as(i64, 400_000_000_000), na_bal.credit_sum - na_bal.debit_sum);
}
