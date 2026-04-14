const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const oble_core = @import("oble_core.zig");
const oble_export = @import("oble_export.zig");
const oble_import = @import("oble_import.zig");

pub const ImportContext = oble_import.ImportContext;
pub const CounterpartyOpenItemImportResult = struct {
    counterparty_id: i64,
    open_item_id: i64,
};

const CounterpartyPayload = struct {
    id: []const u8,
    book_id: []const u8,
    number: []const u8,
    name: []const u8,
    role: []const u8,
    status: ?[]const u8 = null,
    control_account_id: ?[]const u8 = null,
};

const OpenItemLinePayload = struct {
    id: []const u8,
    line_number: i32,
    account_id: []const u8,
    debit_amount: []const u8,
    credit_amount: []const u8,
    counterparty_id: ?[]const u8 = null,
};

const OpenItemPayload = struct {
    id: []const u8,
    book_id: []const u8,
    entry_line_id: []const u8,
    counterparty_id: []const u8,
    original_amount: []const u8,
    remaining_amount: []const u8,
    status: []const u8,
    due_date: ?[]const u8 = null,
};

const CounterpartyOpenItemPayload = struct {
    counterparty: CounterpartyPayload,
    line: OpenItemLinePayload,
    open_item: OpenItemPayload,
};

const CounterpartyProfileBundlePayload = struct {
    counterparties: []const CounterpartyPayload,
    open_items: []const CounterpartyOpenItemPayload,
};

pub fn exportCounterpartiesJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportCounterpartiesJson(database, book_id, buf);
}

pub fn exportCounterpartyOpenItemJson(database: db.Database, open_item_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportCounterpartyOpenItemJson(database, open_item_id, buf);
}

pub fn exportBookSnapshotJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportBookSnapshotJson(database, book_id, buf);
}

pub fn importCounterpartiesJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    return oble_import.importCounterpartiesJson(database, ctx, json, performed_by);
}

pub fn importCounterpartyOpenItemJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !CounterpartyOpenItemImportResult {
    const result = try oble_import.importCounterpartyOpenItemJson(database, ctx, json, performed_by);
    return .{
        .counterparty_id = result.counterparty_id,
        .open_item_id = result.open_item_id,
    };
}

pub fn importBookSnapshotJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    return oble_import.importBookSnapshotJson(database, ctx, json, performed_by);
}

pub fn exportCounterpartyProfileBundleJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    const counterparties_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(counterparties_buf);
    const open_item_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(open_item_buf);
    const counterparties_json = try exportCounterpartiesJson(database, book_id, counterparties_buf);

    var stmt = try database.prepare(
        \\SELECT id
        \\FROM ledger_open_items
        \\WHERE book_id = ?
        \\ORDER BY id ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"counterparties\":");
    try appendLiteral(buf, &pos, counterparties_json);
    try appendLiteral(buf, &pos, ",\"open_items\":[");

    var first = true;
    while (try stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        const open_item_json = try exportCounterpartyOpenItemJson(database, stmt.columnInt64(0), open_item_buf);
        try appendLiteral(buf, &pos, open_item_json);
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn importCounterpartyProfileBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    var parsed = try std.json.parseFromSlice(CounterpartyProfileBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    {
        var counterparties_json: std.io.Writer.Allocating = .init(ctx.allocator);
        defer counterparties_json.deinit();
        try std.json.Stringify.value(parsed.value.counterparties, .{}, &counterparties_json.writer);
        try importCounterpartiesJson(database, ctx, counterparties_json.written(), performed_by);
    }

    for (parsed.value.open_items) |payload| {
        var open_item_json: std.io.Writer.Allocating = .init(ctx.allocator);
        defer open_item_json.deinit();
        try std.json.Stringify.value(payload, .{}, &open_item_json.writer);
        _ = try importCounterpartyOpenItemJson(database, ctx, open_item_json.written(), performed_by);
    }
}

test "OBLE counterparty profile: export and import bundle round-trips" {
    const book_mod = @import("book.zig");
    const account_mod = @import("account.zig");
    const period_mod = @import("period.zig");
    const subledger_mod = @import("subledger.zig");
    const entry_mod = @import("entry.zig");
    const money = @import("money.zig");
    const open_item_mod = @import("open_item.zig");

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Profile Book", "PHP", 2, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4100", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "CUST-001", "Customer One", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", period_id, null, "admin");
    const receivable_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");
    _ = try open_item_mod.createOpenItem(source_db, receivable_line_id, customer_id, 500_00_000_000, "2026-02-15", book_id, "admin");

    var core_buf: [512 * 1024]u8 = undefined;
    var entry_buf: [128 * 1024]u8 = undefined;
    var counterparties_buf: [128 * 1024]u8 = undefined;
    var bundle_buf: [512 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const counterparties_json = try exportCounterpartiesJson(source_db, book_id, &counterparties_buf);
    const exported = try exportCounterpartyProfileBundleJson(source_db, book_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    const imported_book_id = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    try importCounterpartiesJson(target_db, &ctx, counterparties_json, "admin");
    _ = try oble_core.importEntryJson(target_db, &ctx, entry_json, "admin");
    try importCounterpartyProfileBundleJson(target_db, &ctx, exported, "admin");

    var round_buf: [512 * 1024]u8 = undefined;
    const round_trip = try exportCounterpartyProfileBundleJson(target_db, imported_book_id, &round_buf);
    try std.testing.expectEqualStrings(exported, round_trip);
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}
