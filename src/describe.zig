const std = @import("std");
const db = @import("db.zig");
const export_mod = @import("export.zig");

pub fn describeSchema(database: db.Database, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var version: i32 = 0;
    {
        var stmt = try database.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        version = stmt.columnInt(0);
    }

    var pos: usize = 0;

    switch (format) {
        .json => {
            const prefix = std.fmt.bufPrint(buf[pos..], "{{\"schema_version\":{d}", .{version}) catch return error.BufferTooSmall;
            pos += prefix.len;

            const object_types = [_]struct { key: []const u8, sql_type: []const u8 }{
                .{ .key = "tables", .sql_type = "table" },
                .{ .key = "indexes", .sql_type = "index" },
                .{ .key = "views", .sql_type = "view" },
                .{ .key = "triggers", .sql_type = "trigger" },
            };

            for (object_types) |ot| {
                const arr_open = std.fmt.bufPrint(buf[pos..], ",\"{s}\":[", .{ot.key}) catch return error.BufferTooSmall;
                pos += arr_open.len;

                var stmt = try database.prepare("SELECT name, sql FROM sqlite_master WHERE type = ? AND name LIKE 'ledger_%' ORDER BY name;");
                defer stmt.finalize();
                try stmt.bindText(1, ot.sql_type);

                var first = true;
                while (try stmt.step()) {
                    const name = stmt.columnText(0) orelse continue;
                    const sql_text = stmt.columnText(1) orelse "";

                    if (!first) {
                        if (pos >= buf.len) return error.BufferTooSmall;
                        buf[pos] = ',';
                        pos += 1;
                    }
                    first = false;

                    const name_pre = std.fmt.bufPrint(buf[pos..], "{{\"name\":\"", .{}) catch return error.BufferTooSmall;
                    pos += name_pre.len;
                    pos += try export_mod.jsonString(buf[pos..], name);
                    const sql_pre = std.fmt.bufPrint(buf[pos..], "\",\"sql\":\"", .{}) catch return error.BufferTooSmall;
                    pos += sql_pre.len;
                    pos += try export_mod.jsonString(buf[pos..], sql_text);
                    if (pos + 2 > buf.len) return error.BufferTooSmall;
                    buf[pos] = '"';
                    pos += 1;
                    buf[pos] = '}';
                    pos += 1;
                }

                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ']';
                pos += 1;
            }

            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '}';
            pos += 1;
        },
        .csv => {
            const header = std.fmt.bufPrint(buf[pos..], "# schema_version={d}\ntype,name,sql\n", .{version}) catch return error.BufferTooSmall;
            pos += header.len;

            var stmt = try database.prepare("SELECT type, name, sql FROM sqlite_master WHERE name LIKE 'ledger_%' ORDER BY type, name;");
            defer stmt.finalize();

            while (try stmt.step()) {
                const obj_type = stmt.columnText(0) orelse "";
                const name = stmt.columnText(1) orelse "";
                const sql_text = stmt.columnText(2) orelse "";

                pos += try export_mod.csvField(buf[pos..], obj_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], sql_text);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
    }

    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    return database;
}

test "describeSchema returns valid JSON with all tables" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"schema_version\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"tables\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ledger_books") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ledger_entries") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ledger_accounts") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"indexes\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"views\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"triggers\":[") != null);
}

test "describeSchema version matches PRAGMA user_version" {
    const database = try setupTestDb();
    defer database.close();

    var version: i32 = 0;
    {
        var stmt = try database.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        version = stmt.columnInt(0);
    }

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    var expected_buf: [64]u8 = undefined;
    const expected = std.fmt.bufPrint(&expected_buf, "\"schema_version\":{d}", .{version}) catch unreachable;
    try std.testing.expect(std.mem.indexOf(u8, result, expected) != null);
}
