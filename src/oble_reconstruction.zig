const std = @import("std");
const db = @import("db.zig");
const close_mod = @import("close.zig");
const revaluation_mod = @import("revaluation.zig");

fn bookIdForPeriod(database: db.Database, period_id: i64) !i64 {
    var stmt = try database.prepare(
        \\SELECT book_id
        \\FROM ledger_periods
        \\WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, period_id);
    if (!try stmt.step()) return error.NotFound;
    return stmt.columnInt64(0);
}

pub fn reconstructCloseForPeriod(database: db.Database, period_id: i64, performed_by: []const u8) !void {
    const book_id = try bookIdForPeriod(database, period_id);
    try close_mod.closePeriod(database, book_id, period_id, performed_by);
}

pub fn reconstructRevaluationForPeriod(database: db.Database, period_id: i64, rates: []const revaluation_mod.CurrencyRate, performed_by: []const u8) !i64 {
    const book_id = try bookIdForPeriod(database, period_id);
    const result = try revaluation_mod.revalueForexBalances(database, book_id, period_id, rates, performed_by);
    return result.entry_id;
}

test "OBLE reconstruction: close helper resolves book from period" {
    const schema = @import("schema.zig");
    const book_mod = @import("book.zig");
    const account_mod = @import("account.zig");
    const period_mod = @import("period.zig");
    const entry_mod = @import("entry.zig");
    const money = @import("money.zig");

    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Close Recon", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const sale_entry_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_entry_id, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_entry_id, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, sale_entry_id, "admin");

    try reconstructCloseForPeriod(database, jan_id, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, jan_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("soft_closed", stmt.columnText(0).?);
}

test "OBLE reconstruction: revaluation helper resolves book from period" {
    const schema = @import("schema.zig");
    const book_mod = @import("book.zig");
    const account_mod = @import("account.zig");
    const period_mod = @import("period.zig");
    const entry_mod = @import("entry.zig");
    const money = @import("money.zig");

    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "FX Recon", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1010", "Cash USD", .asset, false, "admin");
    const payable_id = try account_mod.Account.create(database, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(database, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const rates = [_]revaluation_mod.CurrencyRate{
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const reval_entry_id = try reconstructRevaluationForPeriod(database, jan_id, &rates, "admin");
    try std.testing.expect(reval_entry_id > 0);
}
