const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const classification_mod = @import("classification.zig");
const oble_core = @import("oble_core.zig");
const oble_import = @import("oble_import.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

pub const ImportContext = oble_import.ImportContext;

const ClassificationPayload = struct {
    id: []const u8,
    book_id: []const u8,
    name: []const u8,
    report_type: []const u8,
};

const ClassificationNodePayload = struct {
    id: []const u8,
    classification_id: []const u8,
    node_type: []const u8,
    label: ?[]const u8 = null,
    parent_id: ?[]const u8 = null,
    account_id: ?[]const u8 = null,
    position: i32,
};

const ClassificationProfileBundlePayload = struct {
    classification: ClassificationPayload,
    nodes: []const ClassificationNodePayload,
};

pub fn exportClassificationProfileBundleJson(database: db.Database, classification_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT c.id, c.name, c.report_type, c.book_id
        \\FROM ledger_classifications c
        \\WHERE c.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, classification_id);
    if (!try stmt.step()) return error.NotFound;

    const book_id = stmt.columnInt64(3);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"classification\":");
    pos += try writeClassificationJson(buf[pos..], classification_id, book_id, stmt.columnText(1).?, stmt.columnText(2).?);
    try appendLiteral(buf, &pos, ",\"nodes\":[");

    var node_stmt = try database.prepare(
        \\SELECT n.id,
        \\       n.node_type,
        \\       n.label,
        \\       n.parent_id,
        \\       n.account_id,
        \\       n.position
        \\FROM ledger_classification_nodes n
        \\WHERE n.classification_id = ?
        \\ORDER BY n.depth ASC, n.position ASC, n.id ASC;
    );
    defer node_stmt.finalize();
    try node_stmt.bindInt(1, classification_id);

    var first = true;
    while (try node_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        pos += try writeNodeJson(
            buf[pos..],
            classification_id,
            node_stmt.columnInt64(0),
            node_stmt.columnText(1).?,
            node_stmt.columnText(2),
            if (node_stmt.columnText(3) != null) node_stmt.columnInt64(3) else null,
            if (node_stmt.columnText(4) != null) node_stmt.columnInt64(4) else null,
            node_stmt.columnInt(5),
        );
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn importClassificationProfileBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(ClassificationProfileBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const bundle = parsed.value;
    const book_id = try resolveId(&ctx.book_ids, bundle.classification.book_id);
    const classification_id = try classification_mod.Classification.create(
        database,
        book_id,
        bundle.classification.name,
        bundle.classification.report_type,
        performed_by,
    );
    try putUnique(ctx, &ctx.classification_ids, bundle.classification.id, classification_id);

    for (bundle.nodes) |node| {
        const mapped_classification_id = try resolveId(&ctx.classification_ids, node.classification_id);
        const parent_id = if (node.parent_id) |parent_key|
            try resolveId(&ctx.classification_node_ids, parent_key)
        else
            null;

        const node_id = if (std.mem.eql(u8, node.node_type, "group"))
            try classification_mod.ClassificationNode.addGroup(
                database,
                mapped_classification_id,
                node.label orelse return error.InvalidInput,
                parent_id,
                node.position,
                performed_by,
            )
        else if (std.mem.eql(u8, node.node_type, "account"))
            try classification_mod.ClassificationNode.addAccount(
                database,
                mapped_classification_id,
                try resolveId(&ctx.account_ids, node.account_id orelse return error.InvalidInput),
                parent_id,
                node.position,
                performed_by,
            )
        else
            return error.InvalidInput;

        try putUnique(ctx, &ctx.classification_node_ids, node.id, node_id);
    }

    return classification_id;
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

fn writeClassificationJson(buf: []u8, classification_id: i64, book_id: i64, name: []const u8, report_type: []const u8) !usize {
    var pos: usize = 0;
    const prefix = try std.fmt.bufPrint(buf[pos..], "{{\"id\":\"classification-{d}\",\"book_id\":\"book-{d}\",\"name\":\"", .{ classification_id, book_id });
    pos += prefix.len;
    pos += try jsonString(buf[pos..], name);
    try appendLiteral(buf, &pos, "\",\"report_type\":\"");
    pos += try jsonString(buf[pos..], report_type);
    try appendLiteral(buf, &pos, "\"}");
    return pos;
}

fn writeNodeJson(
    buf: []u8,
    classification_id: i64,
    node_id: i64,
    node_type: []const u8,
    label: ?[]const u8,
    parent_id: ?i64,
    account_id: ?i64,
    position: i32,
) !usize {
    var pos: usize = 0;
    const prefix = try std.fmt.bufPrint(buf[pos..], "{{\"id\":\"classification-node-{d}\",\"classification_id\":\"classification-{d}\",\"node_type\":\"", .{ node_id, classification_id });
    pos += prefix.len;
    pos += try jsonString(buf[pos..], node_type);
    try appendLiteral(buf, &pos, "\",\"label\":");
    if (label) |text| {
        try appendLiteral(buf, &pos, "\"");
        pos += try jsonString(buf[pos..], text);
        try appendLiteral(buf, &pos, "\"");
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, ",\"parent_id\":");
    if (parent_id) |pid| {
        const parent = try std.fmt.bufPrint(buf[pos..], "\"classification-node-{d}\"", .{pid});
        pos += parent.len;
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    try appendLiteral(buf, &pos, ",\"account_id\":");
    if (account_id) |aid| {
        const account = try std.fmt.bufPrint(buf[pos..], "\"acct-{d}\"", .{aid});
        pos += account.len;
    } else {
        try appendLiteral(buf, &pos, "null");
    }
    const suffix = try std.fmt.bufPrint(buf[pos..], ",\"position\":{d}}}", .{position});
    pos += suffix.len;
    return pos;
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

test "OBLE classification profile: export and import bundle round-trips" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Classification Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const receivable_id = try account_mod.Account.create(source_db, book_id, "1100", "Receivables", .asset, false, "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const classification_id = try classification_mod.Classification.create(source_db, book_id, "Balance Sheet", "balance_sheet", "admin");
    const assets_group_id = try classification_mod.ClassificationNode.addGroup(source_db, classification_id, "Assets", null, 1, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(source_db, classification_id, cash_id, assets_group_id, 1, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(source_db, classification_id, receivable_id, assets_group_id, 2, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var classification_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const classification_json = try exportClassificationProfileBundleJson(source_db, classification_id, &classification_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    _ = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    const imported_classification_id = try importClassificationProfileBundleJson(target_db, &ctx, classification_json, "admin");

    var round_buf: [256 * 1024]u8 = undefined;
    const round_trip = try exportClassificationProfileBundleJson(target_db, imported_classification_id, &round_buf);
    try std.testing.expectEqualStrings(classification_json, round_trip);
}
