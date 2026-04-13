const std = @import("std");
const db = @import("db.zig");
const export_mod = @import("export.zig");
const money = @import("money.zig");

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}

fn appendJsonString(buf: []u8, pos: *usize, value: []const u8) !void {
    try appendLiteral(buf, pos, "\"");
    pos.* += try export_mod.jsonString(buf[pos.*..], value);
    try appendLiteral(buf, pos, "\"");
}

fn appendInt(buf: []u8, pos: *usize, value: anytype) !void {
    const rendered = std.fmt.bufPrint(buf[pos.*..], "{d}", .{value}) catch return error.BufferTooSmall;
    pos.* += rendered.len;
}

fn formatScaledDecimal(buf: []u8, value: i64, decimal_places: u8) ![]u8 {
    if (decimal_places <= 8) return money.formatDecimal(buf, value, decimal_places);

    const negative = value < 0;
    const abs_val: u64 = if (value == std.math.minInt(i64))
        @as(u64, std.math.maxInt(i64)) + 1
    else if (negative)
        @intCast(-value)
    else
        @intCast(value);

    var divisor: u64 = 1;
    var i: u8 = 0;
    while (i < decimal_places) : (i += 1) divisor *= 10;

    const int_part = abs_val / divisor;
    const frac_part = abs_val % divisor;

    var frac_buf: [24]u8 = undefined;
    var remaining = frac_part;
    var dp = decimal_places;
    while (dp > 0) : (dp -= 1) {
        frac_buf[dp - 1] = @intCast('0' + (remaining % 10));
        remaining /= 10;
    }

    var pos: usize = 0;
    if (negative) {
        if (pos >= buf.len) return error.InvalidAmount;
        buf[pos] = '-';
        pos += 1;
    }

    const int_str = std.fmt.bufPrint(buf[pos..], "{d}", .{int_part}) catch return error.InvalidAmount;
    pos += int_str.len;

    if (decimal_places == 0) return buf[0..pos];
    if (pos >= buf.len) return error.InvalidAmount;
    buf[pos] = '.';
    pos += 1;
    if (pos + decimal_places > buf.len) return error.InvalidAmount;
    @memcpy(buf[pos .. pos + decimal_places], frac_buf[0..decimal_places]);
    pos += decimal_places;
    return buf[0..pos];
}

fn appendAmount(buf: []u8, pos: *usize, amount: i64, decimal_places: u8) !void {
    var amt_buf: [64]u8 = undefined;
    const rendered = try formatScaledDecimal(&amt_buf, amount, decimal_places);
    try appendJsonString(buf, pos, rendered);
}

fn appendLineObject(
    buf: []u8,
    pos: *usize,
    line_id: i64,
    line_number: i32,
    account_id: i64,
    debit_amount: i64,
    credit_amount: i64,
    transaction_currency: []const u8,
    fx_rate: i64,
    base_debit_amount: i64,
    base_credit_amount: i64,
    counterparty_id: ?i64,
    decimal_places: u8,
) !void {
    try appendLiteral(buf, pos, "{\"id\":");
    var id_buf: [32]u8 = undefined;
    const id_text = std.fmt.bufPrint(&id_buf, "line-{d}", .{line_id}) catch unreachable;
    try appendJsonString(buf, pos, id_text);
    try appendLiteral(buf, pos, ",\"line_number\":");
    try appendInt(buf, pos, line_number);
    try appendLiteral(buf, pos, ",\"account_id\":");
    var acct_buf: [32]u8 = undefined;
    const acct_text = std.fmt.bufPrint(&acct_buf, "acct-{d}", .{account_id}) catch unreachable;
    try appendJsonString(buf, pos, acct_text);
    try appendLiteral(buf, pos, ",\"debit_amount\":");
    try appendAmount(buf, pos, debit_amount, decimal_places);
    try appendLiteral(buf, pos, ",\"credit_amount\":");
    try appendAmount(buf, pos, credit_amount, decimal_places);
    try appendLiteral(buf, pos, ",\"transaction_currency\":");
    try appendJsonString(buf, pos, transaction_currency);
    try appendLiteral(buf, pos, ",\"fx_rate\":");
    try appendAmount(buf, pos, fx_rate, 10);
    try appendLiteral(buf, pos, ",\"base_debit_amount\":");
    try appendAmount(buf, pos, base_debit_amount, decimal_places);
    try appendLiteral(buf, pos, ",\"base_credit_amount\":");
    try appendAmount(buf, pos, base_credit_amount, decimal_places);
    if (counterparty_id) |cp_id| {
        try appendLiteral(buf, pos, ",\"counterparty_id\":");
        var cp_buf: [32]u8 = undefined;
        const cp_text = std.fmt.bufPrint(&cp_buf, "cp-{d}", .{cp_id}) catch unreachable;
        try appendJsonString(buf, pos, cp_text);
    }
    try appendLiteral(buf, pos, "}");
}

pub fn exportBookJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, base_currency, decimal_places, status
        \\FROM ledger_books WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (!try stmt.step()) return error.NotFound;

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"id\":");
    var id_buf: [32]u8 = undefined;
    const id_text = std.fmt.bufPrint(&id_buf, "book-{d}", .{stmt.columnInt64(0)}) catch unreachable;
    try appendJsonString(buf, &pos, id_text);
    try appendLiteral(buf, &pos, ",\"name\":");
    try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
    try appendLiteral(buf, &pos, ",\"base_currency\":");
    try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
    try appendLiteral(buf, &pos, ",\"decimal_places\":");
    try appendInt(buf, &pos, stmt.columnInt(3));
    try appendLiteral(buf, &pos, ",\"status\":");
    try appendJsonString(buf, &pos, stmt.columnText(4) orelse "");
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn exportAccountsJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, number, name, account_type, normal_balance, status
        \\FROM ledger_accounts
        \\WHERE book_id = ?
        \\ORDER BY number ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "[");
    var first = true;
    while (try stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;

        try appendLiteral(buf, &pos, "{\"id\":");
        var id_buf: [32]u8 = undefined;
        const id_text = std.fmt.bufPrint(&id_buf, "acct-{d}", .{stmt.columnInt64(0)}) catch unreachable;
        try appendJsonString(buf, &pos, id_text);
        try appendLiteral(buf, &pos, ",\"book_id\":");
        var book_buf: [32]u8 = undefined;
        const book_text = std.fmt.bufPrint(&book_buf, "book-{d}", .{book_id}) catch unreachable;
        try appendJsonString(buf, &pos, book_text);
        try appendLiteral(buf, &pos, ",\"number\":");
        try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
        try appendLiteral(buf, &pos, ",\"name\":");
        try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
        try appendLiteral(buf, &pos, ",\"account_type\":");
        try appendJsonString(buf, &pos, stmt.columnText(3) orelse "");
        try appendLiteral(buf, &pos, ",\"normal_balance\":");
        try appendJsonString(buf, &pos, stmt.columnText(4) orelse "");
        try appendLiteral(buf, &pos, ",\"status\":");
        try appendJsonString(buf, &pos, stmt.columnText(5) orelse "");
        try appendLiteral(buf, &pos, "}");
    }
    try appendLiteral(buf, &pos, "]");
    return buf[0..pos];
}

pub fn exportPeriodsJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, start_date, end_date, status, period_number, year
        \\FROM ledger_periods
        \\WHERE book_id = ?
        \\ORDER BY year ASC, period_number ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "[");
    var first = true;
    while (try stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;

        try appendLiteral(buf, &pos, "{\"id\":");
        var id_buf: [48]u8 = undefined;
        const id_text = std.fmt.bufPrint(&id_buf, "period-{d}-{d}", .{ stmt.columnInt(6), stmt.columnInt(5) }) catch unreachable;
        try appendJsonString(buf, &pos, id_text);
        try appendLiteral(buf, &pos, ",\"book_id\":");
        var book_buf: [32]u8 = undefined;
        const book_text = std.fmt.bufPrint(&book_buf, "book-{d}", .{book_id}) catch unreachable;
        try appendJsonString(buf, &pos, book_text);
        try appendLiteral(buf, &pos, ",\"name\":");
        try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
        try appendLiteral(buf, &pos, ",\"start_date\":");
        try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
        try appendLiteral(buf, &pos, ",\"end_date\":");
        try appendJsonString(buf, &pos, stmt.columnText(3) orelse "");
        try appendLiteral(buf, &pos, ",\"status\":");
        try appendJsonString(buf, &pos, stmt.columnText(4) orelse "");
        try appendLiteral(buf, &pos, ",\"period_number\":");
        try appendInt(buf, &pos, stmt.columnInt(5));
        try appendLiteral(buf, &pos, ",\"year\":");
        try appendInt(buf, &pos, stmt.columnInt(6));
        try appendLiteral(buf, &pos, "}");
    }
    try appendLiteral(buf, &pos, "]");
    return buf[0..pos];
}

pub fn exportEntryJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    var header_stmt = try database.prepare(
        \\SELECT e.id, e.book_id, e.period_id, e.status, e.transaction_date, e.posting_date,
        \\  e.document_number, e.description, b.decimal_places
        \\FROM ledger_entries e
        \\JOIN ledger_books b ON b.id = e.book_id
        \\WHERE e.id = ?;
    );
    defer header_stmt.finalize();
    try header_stmt.bindInt(1, entry_id);
    if (!try header_stmt.step()) return error.NotFound;

    const book_id = header_stmt.columnInt64(1);
    const period_id = header_stmt.columnInt64(2);
    const decimal_places: u8 = @intCast(header_stmt.columnInt(8));

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"id\":");
    var entry_buf: [32]u8 = undefined;
    const entry_text = std.fmt.bufPrint(&entry_buf, "entry-{d}", .{header_stmt.columnInt64(0)}) catch unreachable;
    try appendJsonString(buf, &pos, entry_text);
    try appendLiteral(buf, &pos, ",\"book_id\":");
    var book_buf: [32]u8 = undefined;
    const book_text = std.fmt.bufPrint(&book_buf, "book-{d}", .{book_id}) catch unreachable;
    try appendJsonString(buf, &pos, book_text);
    try appendLiteral(buf, &pos, ",\"period_id\":");
    var period_buf: [48]u8 = undefined;
    const period_text = std.fmt.bufPrint(&period_buf, "period-{d}", .{period_id}) catch unreachable;
    try appendJsonString(buf, &pos, period_text);
    try appendLiteral(buf, &pos, ",\"status\":");
    try appendJsonString(buf, &pos, header_stmt.columnText(3) orelse "");
    try appendLiteral(buf, &pos, ",\"transaction_date\":");
    try appendJsonString(buf, &pos, header_stmt.columnText(4) orelse "");
    try appendLiteral(buf, &pos, ",\"posting_date\":");
    try appendJsonString(buf, &pos, header_stmt.columnText(5) orelse "");
    try appendLiteral(buf, &pos, ",\"document_number\":");
    try appendJsonString(buf, &pos, header_stmt.columnText(6) orelse "");
    if (header_stmt.columnText(7)) |desc| {
        try appendLiteral(buf, &pos, ",\"description\":");
        try appendJsonString(buf, &pos, desc);
    }
    try appendLiteral(buf, &pos, ",\"lines\":[");

    var line_stmt = try database.prepare(
        \\SELECT id, line_number, account_id, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, base_debit_amount, base_credit_amount, counterparty_id
        \\FROM ledger_entry_lines
        \\WHERE entry_id = ?
        \\ORDER BY line_number ASC;
    );
    defer line_stmt.finalize();
    try line_stmt.bindInt(1, entry_id);

    var first = true;
    while (try line_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        try appendLineObject(
            buf,
            &pos,
            line_stmt.columnInt64(0),
            line_stmt.columnInt(1),
            line_stmt.columnInt64(2),
            line_stmt.columnInt64(3),
            line_stmt.columnInt64(4),
            line_stmt.columnText(5) orelse "",
            line_stmt.columnInt64(6),
            line_stmt.columnInt64(7),
            line_stmt.columnInt64(8),
            if (line_stmt.columnText(9) != null) line_stmt.columnInt64(9) else null,
            decimal_places,
        );
    }
    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    return database;
}

test "OBLE export: book accounts periods and entry" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try book_mod.Book.create(database, "Example Entity", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-10", "2026-01-10", "Owner capital injection", period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [8192]u8 = undefined;

    const book_json = try exportBookJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, book_json, "\"base_currency\":\"PHP\"") != null);

    const accounts_json = try exportAccountsJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, accounts_json, "\"account_type\":\"asset\"") != null);

    const periods_json = try exportPeriodsJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, periods_json, "\"period_number\":1") != null);

    const entry_json = try exportEntryJson(database, entry_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"document_number\":\"JE-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"debit_amount\":\"1000.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"fx_rate\":\"1.0000000000\"") != null);
}
