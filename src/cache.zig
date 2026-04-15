const std = @import("std");
const db = @import("db.zig");

pub fn recalculateStale(database: db.Database, book_id: i64, period_ids: []const i64) !u32 {
    var count: u32 = 0;

    // Exclude opening entries from cache recalculation — they are audit-trail
    // markers that don't contribute to computed balances (Sprint C.1 design).
    var compute_stmt = try database.prepare(
        \\SELECT COALESCE(SUM(el.base_debit_amount), 0),
        \\       COALESCE(SUM(el.base_credit_amount), 0),
        \\       COUNT(*)
        \\FROM ledger_entry_lines el
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\WHERE e.book_id = ? AND e.period_id = ? AND e.status IN ('posted', 'reversed')
        \\  AND el.account_id = ?
        \\  AND e.entry_type != 'opening';
    );
    defer compute_stmt.finalize();

    var update_stmt = try database.prepare(
        \\UPDATE ledger_account_balances
        \\SET debit_sum = ?, credit_sum = ?, balance = ? - ?,
        \\    entry_count = ?, is_stale = 0,
        \\    last_recalculated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        \\WHERE account_id = ? AND period_id = ?;
    );
    defer update_stmt.finalize();

    var stale_stmt = try database.prepare(
        \\SELECT account_id FROM ledger_account_balances
        \\WHERE book_id = ? AND period_id = ? AND is_stale = 1;
    );
    defer stale_stmt.finalize();

    for (period_ids) |period_id| {
        stale_stmt.reset();
        stale_stmt.clearBindings();
        try stale_stmt.bindInt(1, book_id);
        try stale_stmt.bindInt(2, period_id);

        while (try stale_stmt.step()) {
            const account_id = stale_stmt.columnInt64(0);

            compute_stmt.reset();
            compute_stmt.clearBindings();
            try compute_stmt.bindInt(1, book_id);
            try compute_stmt.bindInt(2, period_id);
            try compute_stmt.bindInt(3, account_id);
            _ = try compute_stmt.step();

            const real_debit = compute_stmt.columnInt64(0);
            const real_credit = compute_stmt.columnInt64(1);
            const entry_count = compute_stmt.columnInt(2);

            update_stmt.reset();
            update_stmt.clearBindings();
            try update_stmt.bindInt(1, real_debit);
            try update_stmt.bindInt(2, real_credit);
            try update_stmt.bindInt(3, real_debit);
            try update_stmt.bindInt(4, real_credit);
            try update_stmt.bindInt(5, @as(i64, @intCast(entry_count)));
            try update_stmt.bindInt(6, account_id);
            try update_stmt.bindInt(7, period_id);
            _ = try update_stmt.step();

            count += 1;
        }
    }

    return count;
}

pub fn recalculateAllStale(database: db.Database, book_id: i64) !u32 {
    var total_fixed: u32 = 0;

    while (true) {
        var period_stmt = try database.prepare(
            \\SELECT DISTINCT period_id FROM ledger_account_balances
            \\WHERE book_id = ? AND is_stale = 1
            \\LIMIT 200;
        );
        defer period_stmt.finalize();
        try period_stmt.bindInt(1, book_id);

        var period_ids: [200]i64 = undefined;
        var period_count: usize = 0;
        while (try period_stmt.step()) {
            period_ids[period_count] = period_stmt.columnInt64(0);
            period_count += 1;
        }

        if (period_count == 0) break;
        total_fixed += try recalculateStale(database, book_id, period_ids[0..period_count]);
    }

    return total_fixed;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");
const report = @import("report.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

fn postEntry(database: db.Database, doc: []const u8, date: []const u8, period_id: i64, debit_account: i64, credit_account: i64, amount: i64) !void {
    const entry_id = try entry_mod.Entry.createDraft(database, 1, doc, date, date, null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, amount, 0, "PHP", money.FX_RATE_SCALE, debit_account, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, amount, "PHP", money.FX_RATE_SCALE, credit_account, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");
}

fn markStale(database: db.Database, book_id: i64, period_id: i64) !void {
    var stmt = try database.prepare(
        \\UPDATE ledger_account_balances SET is_stale = 1
        \\WHERE book_id = ? AND period_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    _ = try stmt.step();
}

fn corruptCache(database: db.Database, book_id: i64, period_id: i64) !void {
    var stmt = try database.prepare(
        \\UPDATE ledger_account_balances
        \\SET debit_sum = 99999, credit_sum = 99999, balance = 0, is_stale = 1
        \\WHERE book_id = ? AND period_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    _ = try stmt.step();
}

fn readCacheStale(database: db.Database, book_id: i64, period_id: i64, account_id: i64) !i32 {
    var stmt = try database.prepare(
        \\SELECT is_stale FROM ledger_account_balances
        \\WHERE book_id = ? AND period_id = ? AND account_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    try stmt.bindInt(3, account_id);
    if (try stmt.step()) return stmt.columnInt(0);
    return -1;
}

fn readCacheSums(database: db.Database, book_id: i64, period_id: i64, account_id: i64) !struct { debit: i64, credit: i64, balance: i64 } {
    var stmt = try database.prepare(
        \\SELECT debit_sum, credit_sum, balance FROM ledger_account_balances
        \\WHERE book_id = ? AND period_id = ? AND account_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, period_id);
    try stmt.bindInt(3, account_id);
    if (try stmt.step()) {
        return .{
            .debit = stmt.columnInt64(0),
            .credit = stmt.columnInt64(1),
            .balance = stmt.columnInt64(2),
        };
    }
    return error.NotFound;
}

test "recalculateStale fixes stale cache" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    try markStale(database, 1, 1);
    try std.testing.expectEqual(@as(i32, 1), try readCacheStale(database, 1, 1, 1));

    const fixed = try recalculateStale(database, 1, &.{1});
    try std.testing.expectEqual(@as(u32, 2), fixed);
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, 1));
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, 2));
}

test "recalculateAllStale finds all stale periods" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);
    try postEntry(database, "JE-002", "2026-02-15", 2, 1, 2, 500_000_000_00);

    try markStale(database, 1, 1);
    try markStale(database, 1, 2);

    const fixed = try recalculateAllStale(database, 1);
    try std.testing.expectEqual(@as(u32, 4), fixed);

    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, 1));
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 2, 1));
}

test "recalculateStale no-op when nothing stale" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    const fixed = try recalculateStale(database, 1, &.{1});
    try std.testing.expectEqual(@as(u32, 0), fixed);
}

test "recalculateStale empty period_ids slice returns 0" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    const empty: []const i64 = &.{};
    const fixed = try recalculateStale(database, 1, empty);
    try std.testing.expectEqual(@as(u32, 0), fixed);
}

test "recalculateStale entry_count correct after recalculation" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);
    try postEntry(database, "JE-002", "2026-01-20", 1, 1, 2, 2_000_000_000_00);

    try markStale(database, 1, 1);
    _ = try recalculateStale(database, 1, &.{1});

    var stmt = try database.prepare(
        \\SELECT entry_count FROM ledger_account_balances
        \\WHERE book_id = 1 AND period_id = 1 AND account_id = 1;
    );
    defer stmt.finalize();
    if (try stmt.step()) {
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }
}

test "recalculateStale last_recalculated_at not null after recalculation" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    try markStale(database, 1, 1);
    _ = try recalculateStale(database, 1, &.{1});

    var stmt = try database.prepare(
        \\SELECT last_recalculated_at FROM ledger_account_balances
        \\WHERE book_id = 1 AND period_id = 1 AND account_id = 1;
    );
    defer stmt.finalize();
    if (try stmt.step()) {
        const recalc_at = stmt.columnText(0);
        try std.testing.expect(recalc_at != null);
    }
}

test "recalculateStale after void shows zero cache" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    const entry_id: i64 = 1;
    try entry_mod.Entry.voidEntry(database, entry_id, "Correction", "admin");

    try markStale(database, 1, 1);
    _ = try recalculateStale(database, 1, &.{1});

    var stmt = try database.prepare(
        \\SELECT debit_sum, credit_sum FROM ledger_account_balances
        \\WHERE book_id = 1 AND period_id = 1 AND account_id = 1;
    );
    defer stmt.finalize();
    if (try stmt.step()) {
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    }
}

test "cache lifecycle: future periods become stale and recalculate restores corrupted rows" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "FEB-001", "2026-02-15", 2, 1, 2, 500_000_000_00);
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 2, 1));

    // A backdated posting invalidates downstream cache rows because future
    // reports depend on cumulative balances, not just per-period activity.
    try postEntry(database, "JAN-001", "2026-01-20", 1, 1, 2, 1_000_000_000_00);
    try std.testing.expectEqual(@as(i32, 1), try readCacheStale(database, 1, 2, 1));
    try std.testing.expectEqual(@as(i32, 1), try readCacheStale(database, 1, 2, 2));

    try corruptCache(database, 1, 2);

    const fixed = try recalculateAllStale(database, 1);
    try std.testing.expectEqual(@as(u32, 2), fixed);
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 2, 1));
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 2, 2));

    const cash_cache = try readCacheSums(database, 1, 2, 1);
    const payable_cache = try readCacheSums(database, 1, 2, 2);
    try std.testing.expectEqual(@as(i64, 500_000_000_00), cash_cache.debit);
    try std.testing.expectEqual(@as(i64, 0), cash_cache.credit);
    try std.testing.expectEqual(@as(i64, 500_000_000_00), cash_cache.balance);
    try std.testing.expectEqual(@as(i64, 0), payable_cache.debit);
    try std.testing.expectEqual(@as(i64, 500_000_000_00), payable_cache.credit);
    try std.testing.expectEqual(@as(i64, -500_000_000_00), payable_cache.balance);

    const trial_balance = try report.trialBalance(database, 1, "2026-02-28");
    defer trial_balance.deinit();
    try std.testing.expect(trial_balance.rows.len >= 2);
}

test "recalculateStale with multiple accounts in same period" {
    const database = try setupTestDb();
    defer database.close();

    const acct3 = try account_mod.Account.create(database, 1, "3000", "Equity", .equity, false, "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);
    try postEntry(database, "JE-002", "2026-01-20", 1, 1, acct3, 500_000_000_00);

    try markStale(database, 1, 1);
    const fixed = try recalculateStale(database, 1, &.{1});
    try std.testing.expectEqual(@as(u32, 3), fixed);

    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, 1));
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, 2));
    try std.testing.expectEqual(@as(i32, 0), try readCacheStale(database, 1, 1, acct3));
}

test "recalculateStale preserves reversed originals" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);
    _ = try entry_mod.Entry.reverse(database, 1, "Correction", "2026-01-20", null, "admin");

    try corruptCache(database, 1, 1);
    const fixed = try recalculateStale(database, 1, &.{1});
    try std.testing.expectEqual(@as(u32, 2), fixed);

    var stmt = try database.prepare(
        \\SELECT debit_sum, credit_sum, balance
        \\FROM ledger_account_balances
        \\WHERE book_id = 1 AND period_id = 1 AND account_id = 1;
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(2));
}

test "recalculateAllStale returns correct total count" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);
    try postEntry(database, "JE-002", "2026-02-15", 2, 1, 2, 500_000_000_00);

    try markStale(database, 1, 1);
    try markStale(database, 1, 2);

    const fixed = try recalculateAllStale(database, 1);
    try std.testing.expectEqual(@as(u32, 4), fixed);
}

test "report auto-recalculates before querying" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, 1, 2, 1_000_000_000_00);

    try corruptCache(database, 1, 1);

    const result = try report.trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(result.total_debits, result.total_credits);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), result.total_debits);
}
