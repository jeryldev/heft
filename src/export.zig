const std = @import("std");
const report_mod = @import("report.zig");

/// Export a ReportResult (TB/IS/BS) to CSV format in a caller-provided buffer.
/// Returns the used portion of the buffer.
pub fn reportToCsv(result: *report_mod.ReportResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    // Header
    const header = "account_number,account_name,account_type,debit_balance,credit_balance\n";
    if (pos + header.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    // Rows
    for (result.rows) |row| {
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        const line = std.fmt.bufPrint(buf[pos..], "{s},{s},{s},{d},{d}\n", .{
            acct_num, acct_name, acct_type, row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += line.len;
    }

    return buf[0..pos];
}

/// Export a ReportResult to JSON format in a caller-provided buffer.
pub fn reportToJson(result: *report_mod.ReportResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const open = "{\"total_debits\":";
    if (pos + open.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;

    const td = std.fmt.bufPrint(buf[pos..], "{d}", .{result.total_debits}) catch return error.InvalidInput;
    pos += td.len;

    const mid = ",\"total_credits\":";
    @memcpy(buf[pos .. pos + mid.len], mid);
    pos += mid.len;

    const tc = std.fmt.bufPrint(buf[pos..], "{d}", .{result.total_credits}) catch return error.InvalidInput;
    pos += tc.len;

    const arr_open = ",\"rows\":[";
    @memcpy(buf[pos .. pos + arr_open.len], arr_open);
    pos += arr_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            buf[pos] = ',';
            pos += 1;
        }
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        const entry = std.fmt.bufPrint(buf[pos..], "{{\"account\":\"{s}\",\"name\":\"{s}\",\"type\":\"{s}\",\"debit\":{d},\"credit\":{d}}}", .{
            acct_num, acct_name, acct_type, row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += entry.len;
    }

    const close = "]}";
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");

fn setupAndPost() !struct { database: db.Database, result: *report_mod.ReportResult } {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const result = try report_mod.trialBalance(database, 1, "2026-01-31");
    return .{ .database = database, .result = result };
}

test "CSV export: contains header and data rows" {
    var setup = try setupAndPost();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [4096]u8 = undefined;
    const csv = try reportToCsv(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "account_number,account_name") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Cash") != null);
}

test "CSV export: correct line count" {
    var setup = try setupAndPost();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [4096]u8 = undefined;
    const csv = try reportToCsv(setup.result, &buf);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + N data rows
    try std.testing.expectEqual(@as(u32, 1 + @as(u32, @intCast(setup.result.rows.len))), line_count);
}

test "JSON export: valid structure" {
    var setup = try setupAndPost();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [4096]u8 = undefined;
    const json = try reportToJson(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_debits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_credits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
    try std.testing.expect(json[json.len - 1] == '}');
}

test "JSON export: contains account data" {
    var setup = try setupAndPost();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [4096]u8 = undefined;
    const json = try reportToJson(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"account\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Cash\"") != null);
}

test "CSV export: empty report produces header only" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    const result = try report_mod.trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const csv = try reportToCsv(result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "account_number") != null);
    // Only header line
    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "JSON export: empty report produces empty array" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    const result = try report_mod.trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const json = try reportToJson(result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

test "CSV export: buffer too small returns error" {
    var setup = try setupAndPost();
    defer setup.database.close();
    defer setup.result.deinit();

    var small_buf: [10]u8 = undefined;
    const result = reportToCsv(setup.result, &small_buf);
    try std.testing.expectError(error.InvalidInput, result);
}
