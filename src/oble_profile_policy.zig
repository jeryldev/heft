const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const oble_core = @import("oble_core.zig");
const oble_export = @import("oble_export.zig");
const oble_import = @import("oble_import.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");
const close_mod = @import("close.zig");
const revaluation_mod = @import("revaluation.zig");

pub const ImportContext = oble_import.ImportContext;

pub const PolicyLifecycleImportResult = struct {
    book_id: i64,
    has_close_reopen_profile: bool,
    has_revaluation_packet: bool,
};

const PolicyLifecycleBundlePayload = struct {
    policy_profile: std.json.Value,
    close_reopen_profile: ?std.json.Value = null,
    revaluation_packet: ?std.json.Value = null,
};

pub fn exportPolicyProfileJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportPolicyProfileJson(database, book_id, buf);
}

pub fn importPolicyProfileJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    return oble_import.importPolicyProfileJson(database, ctx, json, performed_by);
}

pub fn exportCloseReopenProfileJson(database: db.Database, book_id: i64, period_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportCloseReopenProfileJson(database, book_id, period_id, buf);
}

pub fn exportRevaluationPacketJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportRevaluationPacketJson(database, entry_id, buf);
}

pub fn exportPolicyLifecycleBundleJson(database: db.Database, book_id: i64, period_id: i64, revaluation_entry_id: ?i64, buf: []u8) ![]u8 {
    const policy_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(policy_buf);
    const close_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(close_buf);
    const reval_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(reval_buf);

    const policy_json = try exportPolicyProfileJson(database, book_id, policy_buf);
    const close_json = try exportCloseReopenProfileJson(database, book_id, period_id, close_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"policy_profile\":");
    try appendLiteral(buf, &pos, policy_json);
    try appendLiteral(buf, &pos, ",\"close_reopen_profile\":");
    try appendLiteral(buf, &pos, close_json);
    try appendLiteral(buf, &pos, ",\"revaluation_packet\":");
    if (revaluation_entry_id) |entry_id| {
        const reval_json = try exportRevaluationPacketJson(database, entry_id, reval_buf);
        try appendLiteral(buf, &pos, reval_json);
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn importPolicyLifecycleBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !PolicyLifecycleImportResult {
    try oble_import.validateImportPayload(json);
    var parsed = try std.json.parseFromSlice(PolicyLifecycleBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var policy_json: std.io.Writer.Allocating = .init(ctx.allocator);
    defer policy_json.deinit();
    try std.json.Stringify.value(parsed.value.policy_profile, .{}, &policy_json.writer);

    const book_id = try importPolicyProfileJson(database, ctx, policy_json.written(), performed_by);
    return .{
        .book_id = book_id,
        .has_close_reopen_profile = parsed.value.close_reopen_profile != null,
        .has_revaluation_packet = parsed.value.revaluation_packet != null,
    };
}

test "OBLE policy profile: export and import policy round-trips" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Policy Source", "PHP", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const is_id = try account_mod.Account.create(source_db, book_id, "3200", "Income Summary", .equity, false, "admin");
    const ob_id = try account_mod.Account.create(source_db, book_id, "3300", "Opening Balance", .equity, false, "admin");
    const suspense_id = try account_mod.Account.create(source_db, book_id, "9999", "Suspense", .asset, false, "admin");
    const fx_id = try account_mod.Account.create(source_db, book_id, "7999", "FX Gain Loss", .revenue, false, "admin");
    const rounding_id = try account_mod.Account.create(source_db, book_id, "6999", "Rounding", .expense, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setIncomeSummaryAccount(source_db, book_id, is_id, "admin");
    try book_mod.Book.setOpeningBalanceAccount(source_db, book_id, ob_id, "admin");
    try book_mod.Book.setSuspenseAccount(source_db, book_id, suspense_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_id, "admin");
    try book_mod.Book.setRoundingAccount(source_db, book_id, rounding_id, "admin");
    try book_mod.Book.setRequireApproval(source_db, book_id, true, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var policy_buf: [16 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const policy_json = try exportPolicyProfileJson(source_db, book_id, &policy_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    _ = try importPolicyProfileJson(target_db, &ctx, policy_json, "admin");

    var round_policy_buf: [16 * 1024]u8 = undefined;
    const round_policy_json = try exportPolicyProfileJson(target_db, imported_book_id, &round_policy_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"fy_start_month\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"name\":\"income_summary_close\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"name\":\"approval_required\"") != null);
}

test "OBLE policy profile: lifecycle bundle exports close and revaluation packets" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Lifecycle Book", "USD", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .corporation, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const payable_id = try account_mod.Account.create(database, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(database, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-28", "2026-02-28", "regular", "admin");

    const sale_entry_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_entry_id, 1, 1_000_000_000_00, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_entry_id, 2, 0, 1_000_000_000_00, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, sale_entry_id, "admin");

    const fx_entry_id = try entry_mod.Entry.createDraft(database, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, fx_entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, fx_entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(database, fx_entry_id, "admin");

    const rates = [_]revaluation_mod.CurrencyRate{
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const revalue_result = try revaluation_mod.revalueForexBalances(database, book_id, jan_id, &rates, "admin");
    try std.testing.expect(revalue_result.entry_id > 0);

    try close_mod.closePeriod(database, book_id, jan_id, "admin");

    var bundle_buf: [64 * 1024]u8 = undefined;
    const bundle_json = try exportPolicyLifecycleBundleJson(database, book_id, jan_id, revalue_result.entry_id, &bundle_buf);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"policy_profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"close_reopen_profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"revaluation_packet\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bundle_json, "\"source_period_id\":\"period-2026-01\"") != null);
}

test "OBLE policy profile: Zig import imports safe policy and flags derived lifecycle packets" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Lifecycle Import Source", "USD", 2, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");

    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-28", "2026-02-28", "regular", "admin");

    const sale_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 1, 1_000_000_000_00, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 2, 0, 1_000_000_000_00, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, sale_entry_id, "admin");

    const fx_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, fx_entry_id, "admin");

    const rates = [_]revaluation_mod.CurrencyRate{
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const revalue_result = try revaluation_mod.revalueForexBalances(source_db, book_id, jan_id, &rates, "admin");
    try close_mod.closePeriod(source_db, book_id, jan_id, "admin");
    try book_mod.Book.setRequireApproval(source_db, book_id, true, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var bundle_buf: [64 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const bundle_json = try exportPolicyLifecycleBundleJson(source_db, book_id, jan_id, revalue_result.entry_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    const import_result = try importPolicyLifecycleBundleJson(target_db, &ctx, bundle_json, "admin");
    try std.testing.expectEqual(imported_book_id, import_result.book_id);
    try std.testing.expect(import_result.has_close_reopen_profile);
    try std.testing.expect(import_result.has_revaluation_packet);

    var policy_buf: [16 * 1024]u8 = undefined;
    const policy_json = try exportPolicyProfileJson(target_db, imported_book_id, &policy_buf);
    try std.testing.expect(std.mem.indexOf(u8, policy_json, "\"fy_start_month\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, policy_json, "\"name\":\"approval_required\"") != null);
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}
