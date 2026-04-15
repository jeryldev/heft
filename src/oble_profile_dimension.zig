const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const dimension_mod = @import("dimension.zig");
const oble_core = @import("oble_core.zig");
const oble_import = @import("oble_import.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");

pub const ImportContext = oble_import.ImportContext;

const DimensionPayload = struct {
    id: []const u8,
    book_id: []const u8,
    name: []const u8,
    dimension_type: []const u8,
};

const DimensionValuePayload = struct {
    id: []const u8,
    dimension_id: []const u8,
    code: []const u8,
    label: []const u8,
    parent_value_id: ?[]const u8 = null,
};

const LineDimensionAssignmentPayload = struct {
    line_id: []const u8,
    dimension_value_id: []const u8,
};

const DimensionProfileBundlePayload = struct {
    dimensions: []const DimensionPayload,
    dimension_values: []const DimensionValuePayload,
    line_dimension_assignments: []const LineDimensionAssignmentPayload,
};

pub fn exportDimensionProfileBundleJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"dimensions\":[");

    var dim_stmt = try database.prepare(
        \\SELECT id, name, dimension_type
        \\FROM ledger_dimensions
        \\WHERE book_id = ?
        \\ORDER BY name ASC, id ASC;
    );
    defer dim_stmt.finalize();
    try dim_stmt.bindInt(1, book_id);

    var first = true;
    while (try dim_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        pos += try writeDimensionJson(
            buf[pos..],
            dim_stmt.columnInt64(0),
            book_id,
            dim_stmt.columnText(1).?,
            dim_stmt.columnText(2).?,
        );
    }

    try appendLiteral(buf, &pos, "],\"dimension_values\":[");

    var value_stmt = try database.prepare(
        \\SELECT dv.id, dv.dimension_id, dv.code, dv.label, dv.parent_value_id
        \\FROM ledger_dimension_values dv
        \\JOIN ledger_dimensions d ON d.id = dv.dimension_id
        \\WHERE d.book_id = ?
        \\ORDER BY COALESCE(dv.parent_value_id, 0) ASC, dv.id ASC;
    );
    defer value_stmt.finalize();
    try value_stmt.bindInt(1, book_id);

    first = true;
    while (try value_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        pos += try writeDimensionValueJson(
            buf[pos..],
            value_stmt.columnInt64(0),
            value_stmt.columnInt64(1),
            value_stmt.columnText(2).?,
            value_stmt.columnText(3).?,
            if (value_stmt.columnText(4) != null) value_stmt.columnInt64(4) else null,
        );
    }

    try appendLiteral(buf, &pos, "],\"line_dimension_assignments\":[");

    var assign_stmt = try database.prepare(
        \\SELECT ld.line_id, ld.dimension_value_id
        \\FROM ledger_line_dimensions ld
        \\JOIN ledger_entry_lines el ON el.id = ld.line_id
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\WHERE e.book_id = ?
        \\ORDER BY ld.line_id ASC, ld.dimension_value_id ASC;
    );
    defer assign_stmt.finalize();
    try assign_stmt.bindInt(1, book_id);

    first = true;
    while (try assign_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        pos += try writeAssignmentJson(buf[pos..], assign_stmt.columnInt64(0), assign_stmt.columnInt64(1));
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn importDimensionProfileBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    var parsed = try std.json.parseFromSlice(DimensionProfileBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    for (parsed.value.dimensions) |dimension| {
        const book_id = try resolveId(&ctx.book_ids, dimension.book_id);
        const dtype = dimension_mod.DimensionType.fromString(dimension.dimension_type) orelse return error.InvalidInput;
        const dimension_id = try dimension_mod.Dimension.create(database, book_id, dimension.name, dtype, performed_by);
        try putUnique(ctx, &ctx.dimension_ids, dimension.id, dimension_id);
    }

    var remaining = parsed.value.dimension_values.len;
    while (remaining > 0) {
        var progressed = false;
        for (parsed.value.dimension_values) |value| {
            if (ctx.dimension_value_ids.contains(value.id)) continue;
            const dimension_id = try resolveId(&ctx.dimension_ids, value.dimension_id);
            const parent_value_id = if (value.parent_value_id) |parent_key|
                ctx.dimension_value_ids.get(parent_key) orelse continue
            else
                null;
            const value_id = try dimension_mod.DimensionValue.createWithParent(
                database,
                dimension_id,
                value.code,
                value.label,
                parent_value_id,
                performed_by,
            );
            try putUnique(ctx, &ctx.dimension_value_ids, value.id, value_id);
            progressed = true;
            remaining -= 1;
        }
        if (!progressed) return error.NotFound;
    }

    for (parsed.value.line_dimension_assignments) |assignment| {
        try dimension_mod.LineDimension.assign(
            database,
            try resolveId(&ctx.line_ids, assignment.line_id),
            try resolveId(&ctx.dimension_value_ids, assignment.dimension_value_id),
            performed_by,
        );
    }
}

fn putUnique(ctx: *ImportContext, map: *std.StringHashMap(i64), key: []const u8, value: i64) !void {
    const owned_key = try ctx.stableAllocator().dupe(u8, key);
    errdefer ctx.stableAllocator().free(owned_key);
    const gop = try map.getOrPut(owned_key);
    if (gop.found_existing) return error.DuplicateNumber;
    gop.value_ptr.* = value;
}

fn resolveId(map: *const std.StringHashMap(i64), key: []const u8) !i64 {
    return map.get(key) orelse error.NotFound;
}

fn writeDimensionJson(buf: []u8, dimension_id: i64, book_id: i64, name: []const u8, dimension_type: []const u8) !usize {
    var pos: usize = 0;
    const prefix = try std.fmt.bufPrint(buf[pos..], "{{\"id\":\"dimension-{d}\",\"book_id\":\"book-{d}\",\"name\":\"", .{ dimension_id, book_id });
    pos += prefix.len;
    pos += try jsonString(buf[pos..], name);
    try appendLiteral(buf, &pos, "\",\"dimension_type\":\"");
    pos += try jsonString(buf[pos..], dimension_type);
    try appendLiteral(buf, &pos, "\"}");
    return pos;
}

fn writeDimensionValueJson(buf: []u8, value_id: i64, dimension_id: i64, code: []const u8, label: []const u8, parent_value_id: ?i64) !usize {
    var pos: usize = 0;
    const prefix = try std.fmt.bufPrint(buf[pos..], "{{\"id\":\"dimension-value-{d}\",\"dimension_id\":\"dimension-{d}\",\"code\":\"", .{ value_id, dimension_id });
    pos += prefix.len;
    pos += try jsonString(buf[pos..], code);
    try appendLiteral(buf, &pos, "\",\"label\":\"");
    pos += try jsonString(buf[pos..], label);
    try appendLiteral(buf, &pos, "\",\"parent_value_id\":");
    if (parent_value_id) |parent_id| {
        const parent = try std.fmt.bufPrint(buf[pos..], "\"dimension-value-{d}\"", .{parent_id});
        pos += parent.len;
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, "}");
    return pos;
}

fn writeAssignmentJson(buf: []u8, line_id: i64, dimension_value_id: i64) !usize {
    const rendered = try std.fmt.bufPrint(buf, "{{\"line_id\":\"line-{d}\",\"dimension_value_id\":\"dimension-value-{d}\"}}", .{ line_id, dimension_value_id });
    return rendered.len;
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}

fn jsonString(buf: []u8, text: []const u8) !usize {
    var pos: usize = 0;
    for (text) |c| {
        switch (c) {
            '"', '\\' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = c;
                pos += 2;
            },
            '\n' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = 'n';
                pos += 2;
            },
            else => {
                if (pos + 1 > buf.len) return error.BufferTooSmall;
                buf[pos] = c;
                pos += 1;
            },
        }
    }
    return pos;
}

test "OBLE dimension profile: export and import bundle round-trips" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Dimension Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "SALE-001", "2026-01-10", "2026-01-10", "Sale", period_id, null, "admin");
    const cash_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 100_00_000_000, 0, "USD", 10_000_000_000, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 100_00_000_000, "USD", 10_000_000_000, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const dimension_id = try dimension_mod.Dimension.create(source_db, book_id, "Tax Code", .tax_code, "admin");
    const parent_value_id = try dimension_mod.DimensionValue.create(source_db, dimension_id, "VAT", "VAT", "admin");
    const child_value_id = try dimension_mod.DimensionValue.createWithParent(source_db, dimension_id, "VAT12", "VAT 12%", parent_value_id, "admin");
    try dimension_mod.LineDimension.assign(source_db, cash_line_id, child_value_id, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var entry_buf: [128 * 1024]u8 = undefined;
    var dimension_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const dimension_json = try exportDimensionProfileBundleJson(source_db, book_id, &dimension_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    _ = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    _ = try oble_core.importEntryJson(target_db, &ctx, entry_json, "admin");
    try importDimensionProfileBundleJson(target_db, &ctx, dimension_json, "admin");

    var round_buf: [256 * 1024]u8 = undefined;
    const round_trip = try exportDimensionProfileBundleJson(target_db, try resolveId(&ctx.book_ids, "book-1"), &round_buf);
    try std.testing.expectEqualStrings(dimension_json, round_trip);
}
