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
const oble_import = @import("oble_import.zig");

pub const ImportContext = oble_import.ImportContext;

pub const MultiCurrencyImportResult = struct {
    foreign_currency_entry_id: i64,
    has_revaluation_packet: bool,
};

const MultiCurrencyBundlePayload = struct {
    foreign_currency_entry: std.json.Value,
    revaluation_packet: ?std.json.Value = null,
};

pub fn exportEntryJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_core.exportEntryJson(database, entry_id, buf);
}

pub fn exportRevaluationPacketJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportRevaluationPacketJson(database, entry_id, buf);
}

pub fn exportMultiCurrencyBundleJson(database: db.Database, entry_id: i64, revaluation_entry_id: ?i64, buf: []u8) ![]u8 {
    const entry_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(entry_buf);
    const reval_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(reval_buf);
    const entry_json = try exportEntryJson(database, entry_id, entry_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"foreign_currency_entry\":");
    try appendLiteral(buf, &pos, entry_json);
    try appendLiteral(buf, &pos, ",\"revaluation_packet\":");
    if (revaluation_entry_id) |reval_id| {
        const reval_json = try exportRevaluationPacketJson(database, reval_id, reval_buf);
        try appendLiteral(buf, &pos, reval_json);
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn importMultiCurrencyBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !MultiCurrencyImportResult {
    try oble_import.validateImportPayload(json);
    var parsed = try std.json.parseFromSlice(MultiCurrencyBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var entry_json: std.io.Writer.Allocating = .init(ctx.allocator);
    defer entry_json.deinit();
    try std.json.Stringify.value(parsed.value.foreign_currency_entry, .{}, &entry_json.writer);

    const foreign_currency_entry_id = try oble_core.importEntryJson(database, ctx, entry_json.written(), performed_by);
    return .{
        .foreign_currency_entry_id = foreign_currency_entry_id,
        .has_revaluation_packet = parsed.value.revaluation_packet != null,
    };
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

test "OBLE FX profile: Zig import imports foreign entry and flags revaluation replay" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "FX Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1010", "Cash USD", .asset, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const rates = [_]revaluation_mod.CurrencyRate{
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const revalue_result = try revaluation_mod.revalueForexBalances(source_db, book_id, jan_id, &rates, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var bundle_buf: [64 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const bundle_json = try exportMultiCurrencyBundleJson(source_db, entry_id, revalue_result.entry_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    _ = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    const import_result = try importMultiCurrencyBundleJson(target_db, &ctx, bundle_json, "admin");
    try std.testing.expect(import_result.foreign_currency_entry_id > 0);
    try std.testing.expect(import_result.has_revaluation_packet);

    var entry_buf: [32 * 1024]u8 = undefined;
    const round_entry_json = try exportEntryJson(target_db, import_result.foreign_currency_entry_id, &entry_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_entry_json, "\"transaction_currency\":\"EUR\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_entry_json, "\"fx_rate\":\"1.1000000000\"") != null);
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}
