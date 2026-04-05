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

test "describeSchema CSV format contains header and table rows" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .csv);

    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "# schema_version=") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "type,name,sql\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ledger_books") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "ledger_entries") != null);
}

test "describeSchema buffer too small returns error" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [10]u8 = undefined;
    const json_result = describeSchema(database, &buf, .json);
    try std.testing.expectError(error.BufferTooSmall, json_result);

    const csv_result = describeSchema(database, &buf, .csv);
    try std.testing.expectError(error.BufferTooSmall, csv_result);
}

test "describeSchema JSON contains all 16 table names" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    const table_names = [_][]const u8{
        "ledger_books",
        "ledger_accounts",
        "ledger_periods",
        "ledger_entries",
        "ledger_entry_lines",
        "ledger_account_balances",
        "ledger_audit_log",
        "ledger_classifications",
        "ledger_classification_nodes",
        "ledger_subledger_groups",
        "ledger_subledger_accounts",
        "ledger_dimensions",
        "ledger_dimension_values",
        "ledger_line_dimensions",
        "ledger_budgets",
        "ledger_budget_lines",
    };

    for (table_names) |name| {
        try std.testing.expect(std.mem.indexOf(u8, result, name) != null);
    }
}

test "describeSchema JSON schema_version matches schema.SCHEMA_VERSION" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    var expected_buf: [64]u8 = undefined;
    const expected = std.fmt.bufPrint(&expected_buf, "\"schema_version\":{d}", .{schema.SCHEMA_VERSION}) catch unreachable;
    try std.testing.expect(std.mem.indexOf(u8, result, expected) != null);
}

test "describeSchema CSV contains multiple rows" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .csv);

    var line_count: usize = 0;
    for (result) |c| {
        if (c == '\n') line_count += 1;
    }

    try std.testing.expect(line_count > 2);
}

test "describeSchema on empty database returns minimal output" {
    const database = try db.Database.open(":memory:");
    defer database.close();

    var buf: [65536]u8 = undefined;
    const json_result = try describeSchema(database, &buf, .json);

    try std.testing.expect(json_result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"schema_version\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"tables\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"indexes\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"views\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"triggers\":[]") != null);
}

test "describeSchema large buffer works and returns reasonable length" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    try std.testing.expect(result.len > 100);
    try std.testing.expect(result.len < 65536);
}

test "describeSchema JSON has matching braces and brackets" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [65536]u8 = undefined;
    const result = try describeSchema(database, &buf, .json);

    var brace_count: i32 = 0;
    var bracket_count: i32 = 0;
    for (result) |c| {
        switch (c) {
            '{' => brace_count += 1,
            '}' => brace_count -= 1,
            '[' => bracket_count += 1,
            ']' => bracket_count -= 1,
            else => {},
        }
    }

    try std.testing.expectEqual(@as(i32, 0), brace_count);
    try std.testing.expectEqual(@as(i32, 0), bracket_count);
}
