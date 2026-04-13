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

fn appendBookId(buf: []u8, pos: *usize, book_id: i64) !void {
    var book_buf: [32]u8 = undefined;
    const book_text = std.fmt.bufPrint(&book_buf, "book-{d}", .{book_id}) catch unreachable;
    try appendJsonString(buf, pos, book_text);
}

fn appendPeriodId(buf: []u8, pos: *usize, year: i32, period_number: i32) !void {
    var period_buf: [48]u8 = undefined;
    const period_no: u32 = @intCast(period_number);
    const period_text = std.fmt.bufPrint(&period_buf, "period-{d}-{d:0>2}", .{ year, period_no }) catch unreachable;
    try appendJsonString(buf, pos, period_text);
}

fn appendEntryId(buf: []u8, pos: *usize, entry_id: i64) !void {
    var entry_buf: [32]u8 = undefined;
    const entry_text = std.fmt.bufPrint(&entry_buf, "entry-{d}", .{entry_id}) catch unreachable;
    try appendJsonString(buf, pos, entry_text);
}

fn appendCounterpartyId(buf: []u8, pos: *usize, counterparty_id: i64) !void {
    var cp_buf: [32]u8 = undefined;
    const cp_text = std.fmt.bufPrint(&cp_buf, "cp-{d}", .{counterparty_id}) catch unreachable;
    try appendJsonString(buf, pos, cp_text);
}

fn appendLineId(buf: []u8, pos: *usize, line_id: i64) !void {
    var id_buf: [32]u8 = undefined;
    const id_text = std.fmt.bufPrint(&id_buf, "line-{d}", .{line_id}) catch unreachable;
    try appendJsonString(buf, pos, id_text);
}

fn appendAccountId(buf: []u8, pos: *usize, account_id: i64) !void {
    var acct_buf: [32]u8 = undefined;
    const acct_text = std.fmt.bufPrint(&acct_buf, "acct-{d}", .{account_id}) catch unreachable;
    try appendJsonString(buf, pos, acct_text);
}

fn appendOpenItemId(buf: []u8, pos: *usize, open_item_id: i64) !void {
    var oi_buf: [32]u8 = undefined;
    const oi_text = std.fmt.bufPrint(&oi_buf, "oi-{d}", .{open_item_id}) catch unreachable;
    try appendJsonString(buf, pos, oi_text);
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
    try appendLineId(buf, pos, line_id);
    try appendLiteral(buf, pos, ",\"line_number\":");
    try appendInt(buf, pos, line_number);
    try appendLiteral(buf, pos, ",\"account_id\":");
    try appendAccountId(buf, pos, account_id);
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
        try appendCounterpartyId(buf, pos, cp_id);
    }
    try appendLiteral(buf, pos, "}");
}

fn parseSourcePeriodId(metadata: ?[]const u8) ?i64 {
    const m = metadata orelse return null;
    const needle = "\"source_period_id\":";
    const start = std.mem.indexOf(u8, m, needle) orelse return null;
    var i = start + needle.len;
    while (i < m.len and (m[i] == ' ' or m[i] == '\t')) : (i += 1) {}
    var j = i;
    while (j < m.len and std.ascii.isDigit(m[j])) : (j += 1) {}
    if (j == i) return null;
    return std.fmt.parseInt(i64, m[i..j], 10) catch null;
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
    try appendBookId(buf, &pos, stmt.columnInt64(0));
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
        try appendAccountId(buf, &pos, stmt.columnInt64(0));
        try appendLiteral(buf, &pos, ",\"book_id\":");
        try appendBookId(buf, &pos, book_id);
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
        try appendPeriodId(buf, &pos, stmt.columnInt(6), stmt.columnInt(5));
        try appendLiteral(buf, &pos, ",\"book_id\":");
        try appendBookId(buf, &pos, book_id);
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

pub fn exportCounterpartiesJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT sa.id, sa.number, sa.name, sa.type, sa.status, sg.gl_account_id
        \\FROM ledger_subledger_accounts sa
        \\JOIN ledger_subledger_groups sg ON sg.id = sa.group_id
        \\WHERE sa.book_id = ?
        \\ORDER BY sa.number ASC, sa.id ASC;
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
        try appendCounterpartyId(buf, &pos, stmt.columnInt64(0));
        try appendLiteral(buf, &pos, ",\"book_id\":");
        try appendBookId(buf, &pos, book_id);
        try appendLiteral(buf, &pos, ",\"number\":");
        try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
        try appendLiteral(buf, &pos, ",\"name\":");
        try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
        try appendLiteral(buf, &pos, ",\"role\":");
        try appendJsonString(buf, &pos, stmt.columnText(3) orelse "");
        try appendLiteral(buf, &pos, ",\"status\":");
        try appendJsonString(buf, &pos, stmt.columnText(4) orelse "");
        try appendLiteral(buf, &pos, ",\"control_account_id\":");
        try appendAccountId(buf, &pos, stmt.columnInt64(5));
        try appendLiteral(buf, &pos, "}");
    }
    try appendLiteral(buf, &pos, "]");
    return buf[0..pos];
}

pub fn exportBookSnapshotJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [16384]u8 = undefined;
    var periods_buf: [16384]u8 = undefined;
    var counterparties_buf: [16384]u8 = undefined;
    var policy_buf: [8192]u8 = undefined;

    const book_json = try exportBookJson(database, book_id, &book_buf);
    const accounts_json = try exportAccountsJson(database, book_id, &accounts_buf);
    const periods_json = try exportPeriodsJson(database, book_id, &periods_buf);
    const counterparties_json = try exportCounterpartiesJson(database, book_id, &counterparties_buf);
    const policy_json = try exportPolicyProfileJson(database, book_id, &policy_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"book\":");
    try appendLiteral(buf, &pos, book_json);
    try appendLiteral(buf, &pos, ",\"accounts\":");
    try appendLiteral(buf, &pos, accounts_json);
    try appendLiteral(buf, &pos, ",\"periods\":");
    try appendLiteral(buf, &pos, periods_json);
    try appendLiteral(buf, &pos, ",\"counterparties\":");
    try appendLiteral(buf, &pos, counterparties_json);
    try appendLiteral(buf, &pos, ",\"policy_profile\":");
    try appendLiteral(buf, &pos, policy_json);
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn exportPolicyProfileJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT entity_type, fy_start_month, require_approval,
        \\  rounding_account_id, fx_gain_loss_account_id,
        \\  retained_earnings_account_id, income_summary_account_id,
        \\  opening_balance_account_id, suspense_account_id,
        \\  dividends_drawings_account_id, current_year_earnings_account_id
        \\FROM ledger_books
        \\WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (!try stmt.step()) return error.NotFound;

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"entity_type\":");
    try appendJsonString(buf, &pos, stmt.columnText(0) orelse "");
    try appendLiteral(buf, &pos, ",\"fy_start_month\":");
    try appendInt(buf, &pos, stmt.columnInt(1));
    try appendLiteral(buf, &pos, ",\"require_approval\":");
    try appendLiteral(buf, &pos, if (stmt.columnInt(2) != 0) "true" else "false");
    try appendLiteral(buf, &pos, ",\"designations\":{");

    const designation_names = [_][]const u8{
        "rounding_account",
        "fx_gain_loss_account",
        "retained_earnings_account",
        "income_summary_account",
        "opening_balance_account",
        "suspense_account",
        "dividends_drawings_account",
        "current_year_earnings_account",
    };

    for (designation_names, 0..) |name, i| {
        if (i != 0) try appendLiteral(buf, &pos, ",");
        try appendJsonString(buf, &pos, name);
        try appendLiteral(buf, &pos, ":");
        const account_id = stmt.columnInt64(@intCast(3 + i));
        if (account_id == 0) {
            try appendLiteral(buf, &pos, "null");
        } else {
            try appendAccountId(buf, &pos, account_id);
        }
    }

    try appendLiteral(buf, &pos, "},\"policy_profiles\":[");

    const PolicyProfile = struct {
        enabled: bool,
        name: []const u8,
    };

    const profiles = [_]PolicyProfile{
        .{ .enabled = stmt.columnInt64(5) != 0, .name = "equity_close_target" },
        .{ .enabled = stmt.columnInt64(5) != 0 and stmt.columnInt64(6) != 0, .name = "income_summary_close" },
        .{ .enabled = stmt.columnInt64(4) != 0, .name = "multi_currency_revaluation" },
        .{ .enabled = stmt.columnInt64(7) != 0, .name = "opening_balance_workflow" },
        .{ .enabled = stmt.columnInt64(8) != 0, .name = "suspense_enforced" },
        .{ .enabled = stmt.columnInt64(9) != 0, .name = "dividends_drawings_tracking" },
        .{ .enabled = stmt.columnInt(2) != 0, .name = "approval_required" },
    };

    var first = true;
    for (profiles) |profile| {
        if (!profile.enabled) continue;
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        try appendLiteral(buf, &pos, "{\"name\":");
        try appendJsonString(buf, &pos, profile.name);
        try appendLiteral(buf, &pos, "}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn exportCloseReopenProfileJson(database: db.Database, book_id: i64, period_id: i64, buf: []u8) ![]u8 {
    var period_stmt = try database.prepare(
        \\SELECT status, period_number, year, start_date, end_date
        \\FROM ledger_periods
        \\WHERE id = ? AND book_id = ?;
    );
    defer period_stmt.finalize();
    try period_stmt.bindInt(1, period_id);
    try period_stmt.bindInt(2, book_id);
    if (!try period_stmt.step()) return error.NotFound;

    const period_status = period_stmt.columnText(0) orelse "";
    const period_number = period_stmt.columnInt(1);
    const period_year = period_stmt.columnInt(2);
    const period_end_date = period_stmt.columnText(4) orelse "";

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"period_id\":");
    try appendPeriodId(buf, &pos, period_year, period_number);
    try appendLiteral(buf, &pos, ",\"period_status\":");
    try appendJsonString(buf, &pos, period_status);
    try appendLiteral(buf, &pos, ",\"closing_entries\":[");

    {
        var stmt = try database.prepare(
            \\SELECT id, document_number, status
            \\FROM ledger_entries
            \\WHERE book_id = ? AND period_id = ? AND entry_type = 'closing'
            \\ORDER BY id ASC;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, period_id);

        var first = true;
        while (try stmt.step()) {
            if (!first) try appendLiteral(buf, &pos, ",");
            first = false;
            try appendLiteral(buf, &pos, "{\"id\":");
            try appendEntryId(buf, &pos, stmt.columnInt64(0));
            try appendLiteral(buf, &pos, ",\"document_number\":");
            try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
            try appendLiteral(buf, &pos, ",\"status\":");
            try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
            try appendLiteral(buf, &pos, "}");
        }
    }

    try appendLiteral(buf, &pos, "],\"next_period\":");

    var next_period_id: i64 = 0;
    var next_period_number: i32 = 0;
    var next_period_year: i32 = 0;
    var next_period_status: []const u8 = "";
    {
        var stmt = try database.prepare(
            \\SELECT id, period_number, year, status
            \\FROM ledger_periods
            \\WHERE book_id = ?
            \\  AND start_date > ?
            \\ORDER BY start_date ASC
            \\LIMIT 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, period_end_date);
        if (try stmt.step()) {
            next_period_id = stmt.columnInt64(0);
            next_period_number = stmt.columnInt(1);
            next_period_year = stmt.columnInt(2);
            next_period_status = stmt.columnText(3) orelse "";
            try appendLiteral(buf, &pos, "{\"id\":");
            try appendPeriodId(buf, &pos, next_period_year, next_period_number);
            try appendLiteral(buf, &pos, ",\"status\":");
            try appendJsonString(buf, &pos, next_period_status);
            try appendLiteral(buf, &pos, "}");
        } else {
            try appendLiteral(buf, &pos, "null");
        }
    }

    try appendLiteral(buf, &pos, ",\"next_opening_entry\":");
    if (next_period_id > 0) {
        var stmt = try database.prepare(
            \\SELECT id, document_number, status, metadata
            \\FROM ledger_entries
            \\WHERE book_id = ? AND period_id = ? AND entry_type = 'opening'
            \\ORDER BY id DESC
            \\LIMIT 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, next_period_id);
        if (try stmt.step()) {
            try appendLiteral(buf, &pos, "{\"id\":");
            try appendEntryId(buf, &pos, stmt.columnInt64(0));
            try appendLiteral(buf, &pos, ",\"document_number\":");
            try appendJsonString(buf, &pos, stmt.columnText(1) orelse "");
            try appendLiteral(buf, &pos, ",\"status\":");
            try appendJsonString(buf, &pos, stmt.columnText(2) orelse "");
            if (parseSourcePeriodId(stmt.columnText(3))) |source_period_id| {
                var src_stmt = try database.prepare(
                    \\SELECT period_number, year FROM ledger_periods WHERE id = ?;
                );
                defer src_stmt.finalize();
                try src_stmt.bindInt(1, source_period_id);
                if (try src_stmt.step()) {
                    try appendLiteral(buf, &pos, ",\"source_period_id\":");
                    try appendPeriodId(buf, &pos, src_stmt.columnInt(1), src_stmt.columnInt(0));
                }
            }
            try appendLiteral(buf, &pos, "}");
        } else {
            try appendLiteral(buf, &pos, "null");
        }
    } else {
        try appendLiteral(buf, &pos, "null");
    }

    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn exportRevaluationPacketJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    var header_stmt = try database.prepare(
        \\SELECT e.book_id, e.period_id, p.period_number, p.year, e.metadata
        \\FROM ledger_entries e
        \\JOIN ledger_periods p ON p.id = e.period_id
        \\WHERE e.id = ?;
    );
    defer header_stmt.finalize();
    try header_stmt.bindInt(1, entry_id);
    if (!try header_stmt.step()) return error.NotFound;

    const metadata = header_stmt.columnText(4) orelse return error.InvalidInput;
    if (std.mem.indexOf(u8, metadata, "\"revaluation\":true") == null) return error.InvalidInput;

    const book_id = header_stmt.columnInt64(0);
    const source_period_id = header_stmt.columnInt64(1);
    const source_period_number = header_stmt.columnInt(2);
    const source_period_year = header_stmt.columnInt(3);

    var reval_buf: [16384]u8 = undefined;
    const reval_json = try exportEntryJson(database, entry_id, &reval_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"source_period_id\":");
    try appendPeriodId(buf, &pos, source_period_year, source_period_number);
    try appendLiteral(buf, &pos, ",\"revaluation_entry\":");
    try appendLiteral(buf, &pos, reval_json);

    var reversal_stmt = try database.prepare(
        \\SELECT id
        \\FROM ledger_entries
        \\WHERE metadata LIKE ?
        \\ORDER BY id ASC
        \\LIMIT 1;
    );
    defer reversal_stmt.finalize();
    var pattern_buf: [96]u8 = undefined;
    const pattern = std.fmt.bufPrint(&pattern_buf, "%\"reverses_revaluation\":{d}%", .{entry_id}) catch unreachable;
    try reversal_stmt.bindText(1, pattern);

    try appendLiteral(buf, &pos, ",\"reversal_entry\":");
    if (try reversal_stmt.step()) {
        var reversal_buf: [16384]u8 = undefined;
        const reversal_json = try exportEntryJson(database, reversal_stmt.columnInt64(0), &reversal_buf);
        try appendLiteral(buf, &pos, reversal_json);
    } else {
        try appendLiteral(buf, &pos, "null");
    }

    try appendLiteral(buf, &pos, "}");
    _ = source_period_id;
    return buf[0..pos];
}

pub fn exportEntryJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    var header_stmt = try database.prepare(
        \\SELECT e.id, e.book_id, e.period_id, e.status, e.transaction_date, e.posting_date,
        \\  e.document_number, e.description, b.decimal_places, e.entry_type, e.reverses_entry_id,
        \\  p.period_number, p.year
        \\FROM ledger_entries e
        \\JOIN ledger_books b ON b.id = e.book_id
        \\JOIN ledger_periods p ON p.id = e.period_id
        \\WHERE e.id = ?;
    );
    defer header_stmt.finalize();
    try header_stmt.bindInt(1, entry_id);
    if (!try header_stmt.step()) return error.NotFound;
    const book_id = header_stmt.columnInt64(1);
    const period_number = header_stmt.columnInt(11);
    const period_year = header_stmt.columnInt(12);
    const decimal_places: u8 = @intCast(header_stmt.columnInt(8));

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"id\":");
    try appendEntryId(buf, &pos, header_stmt.columnInt64(0));
    try appendLiteral(buf, &pos, ",\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"period_id\":");
    try appendPeriodId(buf, &pos, period_year, period_number);
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
    try appendLiteral(buf, &pos, ",\"entry_type\":");
    try appendJsonString(buf, &pos, header_stmt.columnText(9) orelse "");
    if (header_stmt.columnText(10) != null) {
        try appendLiteral(buf, &pos, ",\"reverses_entry_id\":");
        try appendEntryId(buf, &pos, header_stmt.columnInt64(10));
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

pub fn exportReversalPairJson(database: db.Database, original_entry_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id
        \\FROM ledger_entries
        \\WHERE reverses_entry_id = ?
        \\ORDER BY id ASC
        \\LIMIT 1;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, original_entry_id);
    if (!try stmt.step()) return error.NotFound;
    const reversal_entry_id = stmt.columnInt64(0);

    var original_buf: [8192]u8 = undefined;
    const original_json = try exportEntryJson(database, original_entry_id, &original_buf);
    var reversal_buf: [8192]u8 = undefined;
    const reversal_json = try exportEntryJson(database, reversal_entry_id, &reversal_buf);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"original_entry\":");
    try appendLiteral(buf, &pos, original_json);
    try appendLiteral(buf, &pos, ",\"reversal_entry\":");
    try appendLiteral(buf, &pos, reversal_json);
    try appendLiteral(buf, &pos, "}");
    return buf[0..pos];
}

pub fn exportCounterpartyOpenItemJson(database: db.Database, open_item_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT oi.id, oi.book_id, oi.entry_line_id, oi.counterparty_id, oi.original_amount, oi.remaining_amount,
        \\  oi.due_date, oi.status,
        \\  sa.number, sa.name, sa.type, sa.status,
        \\  el.line_number, el.account_id, el.debit_amount, el.credit_amount,
        \\  b.decimal_places
        \\FROM ledger_open_items oi
        \\JOIN ledger_subledger_accounts sa ON sa.id = oi.counterparty_id
        \\JOIN ledger_entry_lines el ON el.id = oi.entry_line_id
        \\JOIN ledger_books b ON b.id = oi.book_id
        \\WHERE oi.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, open_item_id);
    if (!try stmt.step()) return error.NotFound;

    const book_id = stmt.columnInt64(1);
    const entry_line_id = stmt.columnInt64(2);
    const counterparty_id = stmt.columnInt64(3);
    const decimal_places: u8 = @intCast(stmt.columnInt(16));
    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"counterparty\":{\"id\":");
    try appendCounterpartyId(buf, &pos, counterparty_id);
    try appendLiteral(buf, &pos, ",\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"number\":");
    try appendJsonString(buf, &pos, stmt.columnText(8) orelse "");
    try appendLiteral(buf, &pos, ",\"name\":");
    try appendJsonString(buf, &pos, stmt.columnText(9) orelse "");
    try appendLiteral(buf, &pos, ",\"role\":");
    try appendJsonString(buf, &pos, stmt.columnText(10) orelse "");
    try appendLiteral(buf, &pos, ",\"status\":");
    try appendJsonString(buf, &pos, stmt.columnText(11) orelse "");

    try appendLiteral(buf, &pos, "},\"line\":{\"id\":");
    try appendLineId(buf, &pos, entry_line_id);
    try appendLiteral(buf, &pos, ",\"line_number\":");
    try appendInt(buf, &pos, stmt.columnInt(12));
    try appendLiteral(buf, &pos, ",\"account_id\":");
    try appendAccountId(buf, &pos, stmt.columnInt64(13));
    try appendLiteral(buf, &pos, ",\"debit_amount\":");
    try appendAmount(buf, &pos, stmt.columnInt64(14), decimal_places);
    try appendLiteral(buf, &pos, ",\"credit_amount\":");
    try appendAmount(buf, &pos, stmt.columnInt64(15), decimal_places);
    try appendLiteral(buf, &pos, ",\"counterparty_id\":");
    try appendCounterpartyId(buf, &pos, counterparty_id);

    try appendLiteral(buf, &pos, "},\"open_item\":{\"id\":");
    try appendOpenItemId(buf, &pos, stmt.columnInt64(0));
    try appendLiteral(buf, &pos, ",\"book_id\":");
    try appendBookId(buf, &pos, book_id);
    try appendLiteral(buf, &pos, ",\"entry_line_id\":");
    try appendLineId(buf, &pos, entry_line_id);
    try appendLiteral(buf, &pos, ",\"counterparty_id\":");
    try appendCounterpartyId(buf, &pos, counterparty_id);
    try appendLiteral(buf, &pos, ",\"original_amount\":");
    try appendAmount(buf, &pos, stmt.columnInt64(4), decimal_places);
    try appendLiteral(buf, &pos, ",\"remaining_amount\":");
    try appendAmount(buf, &pos, stmt.columnInt64(5), decimal_places);
    try appendLiteral(buf, &pos, ",\"status\":");
    try appendJsonString(buf, &pos, stmt.columnText(7) orelse "");
    if (stmt.columnText(6)) |due_date| {
        try appendLiteral(buf, &pos, ",\"due_date\":");
        try appendJsonString(buf, &pos, due_date);
    }
    try appendLiteral(buf, &pos, "}}");
    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const subledger_mod = @import("subledger.zig");
const open_item_mod = @import("open_item.zig");
const close_mod = @import("close.zig");
const revaluation_mod = @import("revaluation.zig");

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
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"entry_type\":\"standard\"") != null);
}

test "OBLE export: book snapshot" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Snapshot Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    _ = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = cash_id;

    var buf: [32768]u8 = undefined;
    const json = try exportBookSnapshotJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"book\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"accounts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"periods\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparties\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"policy_profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"retained_earnings_account\"") != null);
}

test "OBLE export: multi-currency entry" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try book_mod.Book.create(database, "FX Entity", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "FX-001", "2026-01-10", "2026-01-10", "USD funding", period_id, null, "admin");
    const usd_fx_rate: i64 = 565_000_000_000; // 56.5000000000
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 10_000_000_000, 0, "USD", usd_fx_rate, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 565_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [8192]u8 = undefined;
    const json = try exportEntryJson(database, entry_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"transaction_currency\":\"USD\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"fx_rate\":\"56.5000000000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"base_debit_amount\":\"5650.00\"") != null);
}

test "OBLE export: counterparties collection" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try book_mod.Book.create(database, "Example Entity", "PHP", 2, "admin");
    const ar_account_id = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const ap_account_id = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "admin");
    const customers_group_id = try subledger_mod.SubledgerGroup.create(database, book_id, "Customers", "customer", 1, ar_account_id, null, null, "admin");
    const suppliers_group_id = try subledger_mod.SubledgerGroup.create(database, book_id, "Suppliers", "supplier", 2, ap_account_id, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, book_id, "C001", "Customer ABC", "customer", customers_group_id, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, book_id, "S001", "Supplier XYZ", "supplier", suppliers_group_id, "admin");

    var buf: [8192]u8 = undefined;
    const json = try exportCounterpartiesJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"C001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"role\":\"customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"control_account_id\":\"acct-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"S001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"role\":\"supplier\"") != null);
}

test "OBLE export: policy profile" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Policy Book", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const is_id = try account_mod.Account.create(database, book_id, "3200", "Income Summary", .equity, false, "admin");
    const ob_id = try account_mod.Account.create(database, book_id, "3300", "Opening Balance", .equity, false, "admin");
    const suspense_id = try account_mod.Account.create(database, book_id, "9999", "Suspense", .asset, false, "admin");
    const fx_id = try account_mod.Account.create(database, book_id, "7999", "FX Gain Loss", .revenue, false, "admin");
    const rounding_id = try account_mod.Account.create(database, book_id, "6999", "Rounding", .expense, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    try book_mod.Book.setIncomeSummaryAccount(database, book_id, is_id, "admin");
    try book_mod.Book.setOpeningBalanceAccount(database, book_id, ob_id, "admin");
    try book_mod.Book.setSuspenseAccount(database, book_id, suspense_id, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_id, "admin");
    try book_mod.Book.setRoundingAccount(database, book_id, rounding_id, "admin");
    try book_mod.Book.setRequireApproval(database, book_id, true, "admin");
    try book_mod.Book.setEntityType(database, book_id, .corporation, "admin");

    var buf: [4096]u8 = undefined;
    const json = try exportPolicyProfileJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"retained_earnings_account\":\"acct-") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"income_summary_close\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"approval_required\"") != null);
}

test "OBLE export: close and reopen profile" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Close Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");
    try close_mod.closePeriod(database, book_id, jan_id, "admin");

    var buf: [8192]u8 = undefined;
    var json = try exportCloseReopenProfileJson(database, book_id, jan_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"period_status\":\"soft_closed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_number\":\"CLOSE-") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"next_opening_entry\":{\"id\":\"entry-") != null);

    try period_mod.Period.transitionWithReason(database, jan_id, .open, "Reopen after close", "admin");
    json = try exportCloseReopenProfileJson(database, book_id, jan_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"period_status\":\"open\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"void\"") != null);
    _ = feb_id;
}

test "OBLE export: reversal pair" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try book_mod.Book.create(database, "Example Entity", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "4000", "Accrual", .liability, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "ACC-001", "2026-01-15", "2026-01-15", "Accrual", period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 0, 250_00_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 250_00_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");
    const reversal_id = try entry_mod.Entry.reverse(database, entry_id, "Accrual reversal", "2026-01-31", null, "admin");

    var buf: [16384]u8 = undefined;
    const json = try exportReversalPairJson(database, entry_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"original_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reversal_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"reversed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reverses_entry_id\":\"entry-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":\"entry-2\"") != null or std.mem.indexOf(u8, json, "\"id\":\"entry-3\"") != null);
    try std.testing.expect(reversal_id > entry_id);
}

test "OBLE export: counterparty open item" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try book_mod.Book.create(database, "Example Entity", "PHP", 2, "admin");
    const ar_account_id = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_account_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const group_id = try subledger_mod.SubledgerGroup.create(database, book_id, "Customers", "customer", 1, ar_account_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(database, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", period_id, null, "admin");
    const receivable_line_id = try entry_mod.Entry.addLine(database, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_account_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_account_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const open_item_id = try open_item_mod.createOpenItem(database, receivable_line_id, customer_id, 500_00_000_000, "2026-02-15", book_id, "admin");
    try open_item_mod.allocatePayment(database, open_item_id, 200_00_000_000, "admin");

    var buf: [16384]u8 = undefined;
    const json = try exportCounterpartyOpenItemJson(database, open_item_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"role\":\"customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_id\":\"cp-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"original_amount\":\"500.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"remaining_amount\":\"300.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"partial\"") != null);
}

test "OBLE export: revaluation packet" {
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

    var buf: [32768]u8 = undefined;
    const json = try exportRevaluationPacketJson(database, result.entry_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"revaluation_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reversal_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"source_period_id\":\"period-2026-01\"") != null);
}
