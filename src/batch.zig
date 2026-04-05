const std = @import("std");
const db = @import("db.zig");
const entry_mod = @import("entry.zig");

pub const BatchResult = struct {
    succeeded: u32,
    failed: u32,
    first_error_index: ?u32,
};

pub fn batchPost(database: db.Database, entry_ids: []const i64, performed_by: []const u8) BatchResult {
    var succeeded: u32 = 0;
    var failed: u32 = 0;
    var first_error_index: ?u32 = null;

    for (entry_ids, 0..) |eid, i| {
        entry_mod.Entry.post(database, eid, performed_by) catch {
            failed += 1;
            if (first_error_index == null) first_error_index = @intCast(i);
            continue;
        };
        succeeded += 1;
    }

    return .{
        .succeeded = succeeded,
        .failed = failed,
        .first_error_index = first_error_index,
    };
}

pub fn batchVoid(database: db.Database, entry_ids: []const i64, reason: []const u8, performed_by: []const u8) BatchResult {
    var succeeded: u32 = 0;
    var failed: u32 = 0;
    var first_error_index: ?u32 = null;

    for (entry_ids, 0..) |eid, i| {
        entry_mod.Entry.voidEntry(database, eid, reason, performed_by) catch {
            failed += 1;
            if (first_error_index == null) first_error_index = @intCast(i);
            continue;
        };
        succeeded += 1;
    }

    return .{
        .succeeded = succeeded,
        .failed = failed,
        .first_error_index = first_error_index,
    };
}

pub fn parseIdArray(json: []const u8, buf: *[1000]i64) !usize {
    if (json.len < 2) return error.InvalidInput;
    if (json[0] != '[' or json[json.len - 1] != ']') return error.InvalidInput;

    const inner = json[1 .. json.len - 1];
    if (inner.len == 0) return 0;

    var count: usize = 0;
    var start: usize = 0;

    for (inner, 0..) |ch, i| {
        if (ch == ',' or i == inner.len - 1) {
            const end = if (ch == ',') i else i + 1;
            const token = std.mem.trim(u8, inner[start..end], " \t\n\r");
            if (token.len == 0) return error.InvalidInput;

            var value: i64 = 0;
            var negative = false;
            var digit_start: usize = 0;
            if (token[0] == '-') {
                negative = true;
                digit_start = 1;
            }
            if (digit_start >= token.len) return error.InvalidInput;

            for (token[digit_start..]) |d| {
                if (d < '0' or d > '9') return error.InvalidInput;
                value = std.math.mul(i64, value, 10) catch return error.InvalidInput;
                value = std.math.add(i64, value, @as(i64, d - '0')) catch return error.InvalidInput;
            }
            if (negative) value = -value;

            if (count >= 1000) return error.InvalidInput;
            buf[count] = value;
            count += 1;
            start = i + 1;
        }
    }

    return count;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

fn createPostableEntry(database: db.Database, doc: []const u8) !i64 {
    const eid = try entry_mod.Entry.createDraft(database, 1, doc, "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    return eid;
}

test "batchPost all 3 succeed" {
    const database = try setupTestDb();
    defer database.close();

    const e1 = try createPostableEntry(database, "JE-001");
    const e2 = try createPostableEntry(database, "JE-002");
    const e3 = try createPostableEntry(database, "JE-003");

    const ids = [_]i64{ e1, e2, e3 };
    const result = batchPost(database, &ids, "admin");

    try std.testing.expectEqual(@as(u32, 3), result.succeeded);
    try std.testing.expectEqual(@as(u32, 0), result.failed);
    try std.testing.expect(result.first_error_index == null);
}

test "batchPost partial failure" {
    const database = try setupTestDb();
    defer database.close();

    const e1 = try createPostableEntry(database, "JE-001");
    const e2 = try createPostableEntry(database, "JE-002");

    // e3 has only 1 line — will fail with TooFewLines
    const e3 = try entry_mod.Entry.createDraft(database, 1, "JE-003", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e3, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");

    const ids = [_]i64{ e1, e3, e2 };
    const result = batchPost(database, &ids, "admin");

    try std.testing.expectEqual(@as(u32, 2), result.succeeded);
    try std.testing.expectEqual(@as(u32, 1), result.failed);
    try std.testing.expectEqual(@as(u32, 1), result.first_error_index.?);
}

test "batchPost empty" {
    const database = try setupTestDb();
    defer database.close();

    const ids = [_]i64{};
    const result = batchPost(database, &ids, "admin");

    try std.testing.expectEqual(@as(u32, 0), result.succeeded);
    try std.testing.expectEqual(@as(u32, 0), result.failed);
    try std.testing.expect(result.first_error_index == null);
}

test "batchVoid all succeed" {
    const database = try setupTestDb();
    defer database.close();

    const e1 = try createPostableEntry(database, "JE-001");
    const e2 = try createPostableEntry(database, "JE-002");
    const e3 = try createPostableEntry(database, "JE-003");
    try entry_mod.Entry.post(database, e1, "admin");
    try entry_mod.Entry.post(database, e2, "admin");
    try entry_mod.Entry.post(database, e3, "admin");

    const ids = [_]i64{ e1, e2, e3 };
    const result = batchVoid(database, &ids, "Batch correction", "admin");

    try std.testing.expectEqual(@as(u32, 3), result.succeeded);
    try std.testing.expectEqual(@as(u32, 0), result.failed);
    try std.testing.expect(result.first_error_index == null);
}

test "batchVoid partial — one already voided" {
    const database = try setupTestDb();
    defer database.close();

    const e1 = try createPostableEntry(database, "JE-001");
    const e2 = try createPostableEntry(database, "JE-002");
    const e3 = try createPostableEntry(database, "JE-003");
    try entry_mod.Entry.post(database, e1, "admin");
    try entry_mod.Entry.post(database, e2, "admin");
    try entry_mod.Entry.post(database, e3, "admin");

    try entry_mod.Entry.voidEntry(database, e2, "Already voided", "admin");

    const ids = [_]i64{ e1, e2, e3 };
    const result = batchVoid(database, &ids, "Batch correction", "admin");

    try std.testing.expectEqual(@as(u32, 2), result.succeeded);
    try std.testing.expectEqual(@as(u32, 1), result.failed);
    try std.testing.expectEqual(@as(u32, 1), result.first_error_index.?);
}

test "parseIdArray valid inputs" {
    var buf: [1000]i64 = undefined;

    const count3 = try parseIdArray("[1,2,3]", &buf);
    try std.testing.expectEqual(@as(usize, 3), count3);
    try std.testing.expectEqual(@as(i64, 1), buf[0]);
    try std.testing.expectEqual(@as(i64, 2), buf[1]);
    try std.testing.expectEqual(@as(i64, 3), buf[2]);

    const count0 = try parseIdArray("[]", &buf);
    try std.testing.expectEqual(@as(usize, 0), count0);

    const count1 = try parseIdArray("[42]", &buf);
    try std.testing.expectEqual(@as(usize, 1), count1);
    try std.testing.expectEqual(@as(i64, 42), buf[0]);
}

test "parseIdArray overflow returns error" {
    var buf: [1000]i64 = undefined;
    const result = parseIdArray("[99999999999999999999]", &buf);
    try std.testing.expectError(error.InvalidInput, result);
}
