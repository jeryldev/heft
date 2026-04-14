const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const oble_core = @import("oble_core.zig");
const oble_export = @import("oble_export.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");
const revaluation_mod = @import("revaluation.zig");

pub fn exportEntryJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_core.exportEntryJson(database, entry_id, buf);
}

pub fn exportRevaluationPacketJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportRevaluationPacketJson(database, entry_id, buf);
}

pub fn exportMultiCurrencyBundleJson(database: db.Database, entry_id: i64, revaluation_entry_id: ?i64, buf: []u8) ![]u8 {
    var entry_buf: [32 * 1024]u8 = undefined;
    const entry_json = try exportEntryJson(database, entry_id, &entry_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"foreign_currency_entry\":");
    try appendLiteral(buf, &pos, entry_json);
    try appendLiteral(buf, &pos, ",\"revaluation_packet\":");
    if (revaluation_entry_id) |reval_id| {
        var reval_buf: [32 * 1024]u8 = undefined;
        const reval_json = try exportRevaluationPacketJson(database, reval_id, &reval_buf);
        try appendLiteral(buf, &pos, reval_json);
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

test "OBLE FX profile: multi-currency bundle exports foreign entry and revaluation packet" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "FX Book", "USD", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .corporation, "admin");

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
    const result = try revaluation_mod.revalueForexBalances(database, book_id, jan_id, &rates, "admin");
    try std.testing.expect(result.entry_id > 0);

    var buf: [64 * 1024]u8 = undefined;
    const bundle_json = try exportMultiCurrencyBundleJson(database, entry_id, result.entry_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"foreign_currency_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"transaction_currency\":\"EUR\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"fx_rate\":\"1.1000000000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"revaluation_packet\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"source_period_id\":\"period-2026-01\"") != null);
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}
