const std = @import("std");
const db_mod = @import("db.zig");
const export_mod = @import("export.zig");

pub const SortOrder = enum {
    asc,
    desc,

    pub fn toSql(self: SortOrder) []const u8 {
        return switch (self) {
            .asc => " ASC",
            .desc => " DESC",
        };
    }
};

pub const DEFAULT_LIMIT: i32 = 100;
pub const MAX_LIMIT: i32 = 1000;

fn clampLimit(limit: i32) i32 {
    if (limit <= 0) return DEFAULT_LIMIT;
    if (limit > MAX_LIMIT) return MAX_LIMIT;
    return limit;
}

fn clampOffset(offset: i32) i32 {
    if (offset < 0) return 0;
    return offset;
}

fn writeJsonMeta(buf: []u8, total: i64, limit: i32, offset: i32) !usize {
    const s = std.fmt.bufPrint(buf, "{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"has_more\":{s},\"rows\":[", .{
        total, limit, offset, if (offset + limit < total) "true" else "false",
    }) catch return error.InvalidInput;
    return s.len;
}

fn writeCsvMeta(buf: []u8, total: i64, limit: i32, offset: i32) !usize {
    const s = std.fmt.bufPrint(buf, "# total={d},limit={d},offset={d}\n", .{
        total, limit, offset,
    }) catch return error.InvalidInput;
    return s.len;
}

// ── getBook ────────────────────────────────────────────────────

pub fn getBook(database: db_mod.Database, book_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, base_currency, decimal_places, status
        \\FROM ledger_books WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    const has_row = try stmt.step();
    if (!has_row) return error.NotFound;

    const id = stmt.columnInt64(0);
    const name = stmt.columnText(1) orelse "";
    const currency = stmt.columnText(2) orelse "";
    const decimals = stmt.columnInt(3);
    const status = stmt.columnText(4) orelse "";

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,name,base_currency,decimal_places,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], name);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], currency);
            const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.InvalidInput;
            pos += rest.len;
            pos += try export_mod.csvField(buf[pos..], status);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], name);
            const j2 = "\",\"base_currency\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], currency);
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.InvalidInput;
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j4 = "\"}";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
        },
    }
    return buf[0..pos];
}

// ── listBooks ──────────────────────────────────────────────────

pub fn listBooks(database: db_mod.Database, status_filter: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    // Count total
    var total: i64 = 0;
    {
        var stmt = if (status_filter != null)
            try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE status = ?;")
        else
            try database.prepare("SELECT COUNT(*) FROM ledger_books;");
        defer stmt.finalize();
        if (status_filter) |sf| try stmt.bindText(1, sf);
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, name, base_currency, decimal_places, status
        \\FROM ledger_books WHERE (? IS NULL OR status = ?)
        \\ORDER BY name ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, name, base_currency, decimal_places, status
        \\FROM ledger_books WHERE (? IS NULL OR status = ?)
        \\ORDER BY name DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    if (status_filter) |sf| {
        try stmt.bindText(1, sf);
        try stmt.bindText(2, sf);
    } else {
        try stmt.bindNull(1);
        try stmt.bindNull(2);
    }
    try stmt.bindInt(3, limit);
    try stmt.bindInt(4, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,base_currency,decimal_places,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const currency = stmt.columnText(2) orelse "";
                const decimals = stmt.columnInt(3);
                const status = stmt.columnText(4) orelse "";

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], currency);
                const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.InvalidInput;
                pos += rest.len;
                pos += try export_mod.csvField(buf[pos..], status);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const currency = stmt.columnText(2) orelse "";
                const decimals = stmt.columnInt(3);
                const status = stmt.columnText(4) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const j2 = "\",\"base_currency\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], currency);
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.InvalidInput;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], status);
                const j4 = "\"}";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── getAccount ─────────────────────────────────────────────────

pub fn getAccount(database: db_mod.Database, account_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, number, name, account_type, normal_balance, is_contra, status
        \\FROM ledger_accounts WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, account_id);
    const has_row = try stmt.step();
    if (!has_row) return error.NotFound;

    const id = stmt.columnInt64(0);
    const number = stmt.columnText(1) orelse "";
    const name = stmt.columnText(2) orelse "";
    const acct_type = stmt.columnText(3) orelse "";
    const normal_bal = stmt.columnText(4) orelse "";
    const is_contra = stmt.columnInt(5);
    const status = stmt.columnText(6) orelse "";

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,number,name,account_type,normal_balance,is_contra,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], number);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], name);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], acct_type);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], normal_bal);
            const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.InvalidInput;
            pos += rest.len;
            pos += try export_mod.csvField(buf[pos..], status);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{id}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], number);
            const j2 = "\",\"name\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], name);
            const j3 = "\",\"account_type\":\"";
            if (pos + j3.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], acct_type);
            const j4 = "\",\"normal_balance\":\"";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], normal_bal);
            const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{d},\"status\":\"", .{is_contra}) catch return error.InvalidInput;
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j6 = "\"}";
            if (pos + j6.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j6.len], j6);
            pos += j6.len;
        },
    }
    return buf[0..pos];
}

// ── listAccounts ───────────────────────────────────────────────

pub fn listAccounts(database: db_mod.Database, book_id: i64, type_filter: ?[]const u8, status_filter: ?[]const u8, name_search: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    // Count total
    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_accounts
            \\WHERE book_id = ?
            \\  AND (? IS NULL OR account_type = ?)
            \\  AND (? IS NULL OR status = ?)
            \\  AND (? IS NULL OR name LIKE '%' || ? || '%');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        if (status_filter) |sf| { try stmt.bindText(4, sf); try stmt.bindText(5, sf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        if (name_search) |ns| { try stmt.bindText(6, ns); try stmt.bindText(7, ns); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, number, name, account_type, normal_balance, is_contra, status
        \\FROM ledger_accounts
        \\WHERE book_id = ?
        \\  AND (? IS NULL OR account_type = ?)
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR name LIKE '%' || ? || '%')
        \\ORDER BY number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, number, name, account_type, normal_balance, is_contra, status
        \\FROM ledger_accounts
        \\WHERE book_id = ?
        \\  AND (? IS NULL OR account_type = ?)
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR name LIKE '%' || ? || '%')
        \\ORDER BY number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    if (status_filter) |sf| { try stmt.bindText(4, sf); try stmt.bindText(5, sf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    if (name_search) |ns| { try stmt.bindText(6, ns); try stmt.bindText(7, ns); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
    try stmt.bindInt(8, limit);
    try stmt.bindInt(9, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,number,name,account_type,normal_balance,is_contra,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const number = stmt.columnText(1) orelse "";
                const name = stmt.columnText(2) orelse "";
                const acct_type = stmt.columnText(3) orelse "";
                const normal_bal = stmt.columnText(4) orelse "";
                const is_contra = stmt.columnInt(5);
                const status = stmt.columnText(6) orelse "";

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], number);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], acct_type);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], normal_bal);
                const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.InvalidInput;
                pos += rest.len;
                pos += try export_mod.csvField(buf[pos..], status);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const id = stmt.columnInt64(0);
                const number = stmt.columnText(1) orelse "";
                const name = stmt.columnText(2) orelse "";
                const acct_type = stmt.columnText(3) orelse "";
                const normal_bal = stmt.columnText(4) orelse "";
                const is_contra = stmt.columnInt(5);
                const status = stmt.columnText(6) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{id}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], number);
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const j3 = "\",\"account_type\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], acct_type);
                const j4 = "\",\"normal_balance\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], normal_bal);
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{d},\"status\":\"", .{is_contra}) catch return error.InvalidInput;
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], status);
                const j6 = "\"}";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── getPeriod ──────────────────────────────────────────────────

pub fn getPeriod(database: db_mod.Database, period_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, period_number, year, start_date, end_date, period_type, status
        \\FROM ledger_periods WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, period_id);
    const has_row = try stmt.step();
    if (!has_row) return error.NotFound;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,name,period_number,year,start_date,end_date,period_type,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.InvalidInput;
            pos += nums.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.InvalidInput;
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
            const j3 = "\",\"end_date\":\"";
            if (pos + j3.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
            const j4 = "\",\"period_type\":\"";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
            const j5 = "\",\"status\":\"";
            if (pos + j5.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j5.len], j5);
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
            const j6 = "\"}";
            if (pos + j6.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j6.len], j6);
            pos += j6.len;
        },
    }
    return buf[0..pos];
}

// ── listPeriods ────────────────────────────────────────────────

pub fn listPeriods(database: db_mod.Database, book_id: i64, year_filter: ?i32, status_filter: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_periods
            \\WHERE book_id = ?
            \\  AND (? IS NULL OR year = ?)
            \\  AND (? IS NULL OR status = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (year_filter) |yf| { try stmt.bindInt(2, yf); try stmt.bindInt(3, yf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        if (status_filter) |sf| { try stmt.bindText(4, sf); try stmt.bindText(5, sf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, name, period_number, year, start_date, end_date, period_type, status
        \\FROM ledger_periods WHERE book_id = ?
        \\  AND (? IS NULL OR year = ?) AND (? IS NULL OR status = ?)
        \\ORDER BY year ASC, period_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, name, period_number, year, start_date, end_date, period_type, status
        \\FROM ledger_periods WHERE book_id = ?
        \\  AND (? IS NULL OR year = ?) AND (? IS NULL OR status = ?)
        \\ORDER BY year DESC, period_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (year_filter) |yf| { try stmt.bindInt(2, yf); try stmt.bindInt(3, yf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    if (status_filter) |sf| { try stmt.bindText(4, sf); try stmt.bindText(5, sf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    try stmt.bindInt(6, limit);
    try stmt.bindInt(7, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,period_number,year,start_date,end_date,period_type,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.InvalidInput;
                pos += nums.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.InvalidInput;
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j3 = "\",\"end_date\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j4 = "\",\"period_type\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
                const j6 = "\"}";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── listEntries ────────────────────────────────────────────────

pub fn listEntries(database: db_mod.Database, book_id: i64, status_filter: ?[]const u8, start_date: ?[]const u8, end_date: ?[]const u8, doc_search: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries
            \\WHERE book_id = ?
            \\  AND (? IS NULL OR status = ?)
            \\  AND (? IS NULL OR posting_date >= ?)
            \\  AND (? IS NULL OR posting_date <= ?)
            \\  AND (? IS NULL OR document_number LIKE '%' || ? || '%');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (status_filter) |sf| { try stmt.bindText(2, sf); try stmt.bindText(3, sf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        if (start_date) |sd| { try stmt.bindText(4, sd); try stmt.bindText(5, sd); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        if (end_date) |ed| { try stmt.bindText(6, ed); try stmt.bindText(7, ed); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
        if (doc_search) |ds| { try stmt.bindText(8, ds); try stmt.bindText(9, ds); } else { try stmt.bindNull(8); try stmt.bindNull(9); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id
        \\FROM ledger_entries WHERE book_id = ?
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR posting_date >= ?) AND (? IS NULL OR posting_date <= ?)
        \\  AND (? IS NULL OR document_number LIKE '%' || ? || '%')
        \\ORDER BY posting_date ASC, document_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id
        \\FROM ledger_entries WHERE book_id = ?
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR posting_date >= ?) AND (? IS NULL OR posting_date <= ?)
        \\  AND (? IS NULL OR document_number LIKE '%' || ? || '%')
        \\ORDER BY posting_date DESC, document_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (status_filter) |sf| { try stmt.bindText(2, sf); try stmt.bindText(3, sf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    if (start_date) |sd| { try stmt.bindText(4, sd); try stmt.bindText(5, sd); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    if (end_date) |ed| { try stmt.bindText(6, ed); try stmt.bindText(7, ed); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
    if (doc_search) |ds| { try stmt.bindText(8, ds); try stmt.bindText(9, ds); } else { try stmt.bindNull(8); try stmt.bindNull(9); }
    try stmt.bindInt(10, limit);
    try stmt.bindInt(11, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,document_number,transaction_date,posting_date,description,status,period_id\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                const pid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(6)}) catch return error.InvalidInput;
                pos += pid.len;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"document_number\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"transaction_date\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"posting_date\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = "\",\"description\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j6 = std.fmt.bufPrint(buf[pos..], "\",\"period_id\":{d}}}", .{stmt.columnInt64(6)}) catch return error.InvalidInput;
                pos += j6.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── listAuditLog ───────────────────────────────────────────────

pub fn listAuditLog(database: db_mod.Database, book_id: i64, entity_type_filter: ?[]const u8, action_filter: ?[]const u8, start_date: ?[]const u8, end_date: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_audit_log
            \\WHERE book_id = ?
            \\  AND (? IS NULL OR entity_type = ?)
            \\  AND (? IS NULL OR action = ?)
            \\  AND (? IS NULL OR performed_at >= ?)
            \\  AND (? IS NULL OR performed_at <= ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (entity_type_filter) |et| { try stmt.bindText(2, et); try stmt.bindText(3, et); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        if (action_filter) |af| { try stmt.bindText(4, af); try stmt.bindText(5, af); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        if (start_date) |sd| { try stmt.bindText(6, sd); try stmt.bindText(7, sd); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
        if (end_date) |ed| { try stmt.bindText(8, ed); try stmt.bindText(9, ed); } else { try stmt.bindNull(8); try stmt.bindNull(9); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, performed_at
        \\FROM ledger_audit_log WHERE book_id = ?
        \\  AND (? IS NULL OR entity_type = ?) AND (? IS NULL OR action = ?)
        \\  AND (? IS NULL OR performed_at >= ?) AND (? IS NULL OR performed_at <= ?)
        \\ORDER BY id ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, performed_at
        \\FROM ledger_audit_log WHERE book_id = ?
        \\  AND (? IS NULL OR entity_type = ?) AND (? IS NULL OR action = ?)
        \\  AND (? IS NULL OR performed_at >= ?) AND (? IS NULL OR performed_at <= ?)
        \\ORDER BY id DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (entity_type_filter) |et| { try stmt.bindText(2, et); try stmt.bindText(3, et); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    if (action_filter) |af| { try stmt.bindText(4, af); try stmt.bindText(5, af); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    if (start_date) |sd| { try stmt.bindText(6, sd); try stmt.bindText(7, sd); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
    if (end_date) |ed| { try stmt.bindText(8, ed); try stmt.bindText(9, ed); } else { try stmt.bindNull(8); try stmt.bindNull(9); }
    try stmt.bindInt(10, limit);
    try stmt.bindInt(11, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,entity_type,entity_id,action,field_changed,old_value,new_value,performed_by,performed_at\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                const eid_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt64(2)}) catch return error.InvalidInput;
                pos += eid_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(8) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"entity_type\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"entity_id\":{d},\"action\":\"", .{stmt.columnInt64(2)}) catch return error.InvalidInput;
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j3 = "\",\"field_changed\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j4 = "\",\"old_value\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j5 = "\",\"new_value\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j6 = "\",\"performed_by\":\"";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
                const j7 = "\",\"performed_at\":\"";
                if (pos + j7.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(8) orelse "");
                const j8 = "\"}";
                if (pos + j8.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j8.len], j8);
                pos += j8.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── getEntry ───────────────────────────────────────────────────

pub fn getEntry(database: db_mod.Database, entry_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id, metadata
        \\FROM ledger_entries WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, entry_id);
    const has_row = try stmt.step();
    if (!has_row) return error.NotFound;

    const id = stmt.columnInt64(0);
    const doc = stmt.columnText(1) orelse "";
    const txn_date = stmt.columnText(2) orelse "";
    const post_date = stmt.columnText(3) orelse "";
    const desc = stmt.columnText(4) orelse "";
    const status = stmt.columnText(5) orelse "";
    const period_id = stmt.columnInt64(6);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,document_number,transaction_date,posting_date,description,status,period_id\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], doc);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], txn_date);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], post_date);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], desc);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], status);
            const pid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{period_id}) catch return error.InvalidInput;
            pos += pid.len;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"document_number\":\"", .{id}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], doc);
            const j2 = "\",\"transaction_date\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], txn_date);
            const j3 = "\",\"posting_date\":\"";
            if (pos + j3.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], post_date);
            const j4 = "\",\"description\":\"";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], desc);
            const j5 = "\",\"status\":\"";
            if (pos + j5.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j5.len], j5);
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j6 = std.fmt.bufPrint(buf[pos..], "\",\"period_id\":{d}}}", .{period_id}) catch return error.InvalidInput;
            pos += j6.len;
        },
    }
    return buf[0..pos];
}

// ── listEntryLines ─────────────────────────────────────────────

pub fn listEntryLines(database: db_mod.Database, entry_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT el.id, el.line_number, a.number, a.name,
        \\  el.debit_amount, el.credit_amount, el.base_debit_amount, el.base_credit_amount,
        \\  el.transaction_currency, el.fx_rate, el.description, el.counterparty_id
        \\FROM ledger_entry_lines el
        \\JOIN ledger_accounts a ON a.id = el.account_id
        \\WHERE el.entry_id = ?
        \\ORDER BY el.line_number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, entry_id);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,line_number,account_number,account_name,debit_amount,credit_amount,base_debit,base_credit,currency,fx_rate,description,counterparty_id\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},{d},", .{ stmt.columnInt64(0), stmt.columnInt(1) }) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},", .{
                    stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.InvalidInput;
                pos += nums.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(8) orelse "");
                const fx = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt64(9)}) catch return error.InvalidInput;
                pos += fx.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(10) orelse "");
                const cp = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(11)}) catch return error.InvalidInput;
                pos += cp.len;
            }
        },
        .json => {
            const open = "{\"lines\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"line_number\":{d},\"account_number\":\"", .{ stmt.columnInt64(0), stmt.columnInt(1) }) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j2 = "\",\"account_name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"base_debit\":{d},\"base_credit\":{d},\"currency\":\"", .{
                    stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.InvalidInput;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(8) orelse "");
                const j4 = std.fmt.bufPrint(buf[pos..], "\",\"fx_rate\":{d},\"description\":\"", .{stmt.columnInt64(9)}) catch return error.InvalidInput;
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(10) orelse "");
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"counterparty_id\":{d}}}", .{stmt.columnInt64(11)}) catch return error.InvalidInput;
                pos += j5.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── getClassification ──────────────────────────────────────────

pub fn getClassification(database: db_mod.Database, classification_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare("SELECT id, name, report_type FROM ledger_classifications WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, classification_id);
    if (!(try stmt.step())) return error.NotFound;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,name,report_type\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"report_type\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = "\"}";
            if (pos + j3.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
        },
    }
    return buf[0..pos];
}

// ── getSubledgerGroup ─────────────────────────────────────────

pub fn getSubledgerGroup(database: db_mod.Database, group_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT sg.id, sg.name, sg.type, sg.group_number, a.number
        \\FROM ledger_subledger_groups sg
        \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
        \\WHERE sg.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, group_id);
    if (!(try stmt.step())) return error.NotFound;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,name,type,group_number,control_account_number\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            const gn = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt(3)}) catch return error.InvalidInput;
            pos += gn.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"type\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"group_number\":{d},\"control_account_number\":\"", .{stmt.columnInt(3)}) catch return error.InvalidInput;
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
            const j4 = "\"}";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
        },
    }
    return buf[0..pos];
}

// ── getSubledgerAccount ───────────────────────────────────────

pub fn getSubledgerAccount(database: db_mod.Database, account_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare("SELECT id, number, name, type, group_id FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, account_id);
    if (!(try stmt.step())) return error.NotFound;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,number,name,type,group_id\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
            const gid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(4)}) catch return error.InvalidInput;
            pos += gid.len;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"name\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = "\",\"type\":\"";
            if (pos + j3.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
            const j4 = std.fmt.bufPrint(buf[pos..], "\",\"group_id\":{d}}}", .{stmt.columnInt64(4)}) catch return error.InvalidInput;
            pos += j4.len;
        },
    }
    return buf[0..pos];
}

// ── listClassifications ────────────────────────────────────────

pub fn listClassifications(database: db_mod.Database, book_id: i64, type_filter: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_classifications
            \\WHERE book_id = ? AND (? IS NULL OR report_type = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, name, report_type FROM ledger_classifications
        \\WHERE book_id = ? AND (? IS NULL OR report_type = ?)
        \\ORDER BY name ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, name, report_type FROM ledger_classifications
        \\WHERE book_id = ? AND (? IS NULL OR report_type = ?)
        \\ORDER BY name DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    try stmt.bindInt(4, limit);
    try stmt.bindInt(5, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,report_type\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"report_type\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\"}";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── listSubledgerGroups ────────────────────────────────────────

pub fn listSubledgerGroups(database: db_mod.Database, book_id: i64, type_filter: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_subledger_groups
            \\WHERE book_id = ? AND (? IS NULL OR type = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT sg.id, sg.name, sg.type, sg.group_number, a.number
        \\FROM ledger_subledger_groups sg
        \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
        \\WHERE sg.book_id = ? AND (? IS NULL OR sg.type = ?)
        \\ORDER BY sg.name ASC LIMIT ? OFFSET ?;
    else
        \\SELECT sg.id, sg.name, sg.type, sg.group_number, a.number
        \\FROM ledger_subledger_groups sg
        \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
        \\WHERE sg.book_id = ? AND (? IS NULL OR sg.type = ?)
        \\ORDER BY sg.name DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (type_filter) |tf| { try stmt.bindText(2, tf); try stmt.bindText(3, tf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    try stmt.bindInt(4, limit);
    try stmt.bindInt(5, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,type,group_number,control_account_number\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                const gn = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt(3)}) catch return error.InvalidInput;
                pos += gn.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"type\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"group_number\":{d},\"control_account_number\":\"", .{stmt.columnInt(3)}) catch return error.InvalidInput;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j4 = "\"}";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── listSubledgerAccounts ──────────────────────────────────────

pub fn listSubledgerAccounts(database: db_mod.Database, book_id: i64, group_filter: ?i64, name_search: ?[]const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_subledger_accounts
            \\WHERE book_id = ?
            \\  AND (? IS NULL OR group_id = ?)
            \\  AND (? IS NULL OR name LIKE '%' || ? || '%');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        if (group_filter) |gf| { try stmt.bindInt(2, gf); try stmt.bindInt(3, gf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
        if (name_search) |ns| { try stmt.bindText(4, ns); try stmt.bindText(5, ns); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, number, name, type, group_id FROM ledger_subledger_accounts
        \\WHERE book_id = ? AND (? IS NULL OR group_id = ?)
        \\  AND (? IS NULL OR name LIKE '%' || ? || '%')
        \\ORDER BY number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, number, name, type, group_id FROM ledger_subledger_accounts
        \\WHERE book_id = ? AND (? IS NULL OR group_id = ?)
        \\  AND (? IS NULL OR name LIKE '%' || ? || '%')
        \\ORDER BY number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (group_filter) |gf| { try stmt.bindInt(2, gf); try stmt.bindInt(3, gf); } else { try stmt.bindNull(2); try stmt.bindNull(3); }
    if (name_search) |ns| { try stmt.bindText(4, ns); try stmt.bindText(5, ns); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    try stmt.bindInt(6, limit);
    try stmt.bindInt(7, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,number,name,type,group_id\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                const gid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(4)}) catch return error.InvalidInput;
                pos += gid.len;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"type\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = std.fmt.bufPrint(buf[pos..], "\",\"group_id\":{d}}}", .{stmt.columnInt64(4)}) catch return error.InvalidInput;
                pos += j4.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── subledgerReport ────────────────────────────────────────────

pub fn subledgerReport(database: db_mod.Database, book_id: i64, group_id: ?i64, name_search: ?[]const u8, start_date: []const u8, end_date: []const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(DISTINCT counterparty_id) FROM ledger_transaction_history
            \\WHERE book_id = ? AND counterparty_id IS NOT NULL
            \\  AND posting_date BETWEEN ? AND ?
            \\  AND (? IS NULL OR subledger_group_id = ?)
            \\  AND (? IS NULL OR counterparty_name LIKE '%' || ? || '%');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, start_date);
        try stmt.bindText(3, end_date);
        if (group_id) |gf| { try stmt.bindInt(4, gf); try stmt.bindInt(5, gf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        if (name_search) |ns| { try stmt.bindText(6, ns); try stmt.bindText(7, ns); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT counterparty_id, counterparty_number, counterparty_name,
        \\  subledger_group_name, account_number,
        \\  SUM(base_debit_amount), SUM(base_credit_amount)
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id IS NOT NULL
        \\  AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR subledger_group_id = ?)
        \\  AND (? IS NULL OR counterparty_name LIKE '%' || ? || '%')
        \\GROUP BY counterparty_id
        \\ORDER BY counterparty_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT counterparty_id, counterparty_number, counterparty_name,
        \\  subledger_group_name, account_number,
        \\  SUM(base_debit_amount), SUM(base_credit_amount)
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id IS NOT NULL
        \\  AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR subledger_group_id = ?)
        \\  AND (? IS NULL OR counterparty_name LIKE '%' || ? || '%')
        \\GROUP BY counterparty_id
        \\ORDER BY counterparty_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    if (group_id) |gf| { try stmt.bindInt(4, gf); try stmt.bindInt(5, gf); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    if (name_search) |ns| { try stmt.bindText(6, ns); try stmt.bindText(7, ns); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
    try stmt.bindInt(8, limit);
    try stmt.bindInt(9, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "counterparty_id,counterparty_number,counterparty_name,group_name,control_account,debit_balance,credit_balance\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d}\n", .{ stmt.columnInt64(5), stmt.columnInt64(6) }) catch return error.InvalidInput;
                pos += nums.len;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"counterparty_id\":{d},\"counterparty_number\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"counterparty_name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"group_name\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = "\",\"control_account\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"debit_balance\":{d},\"credit_balance\":{d}}}", .{ stmt.columnInt64(5), stmt.columnInt64(6) }) catch return error.InvalidInput;
                pos += j5.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── counterpartyLedger ─────────────────────────────────────────

pub fn counterpartyLedger(database: db_mod.Database, book_id: i64, counterparty_id: i64, account_filter: ?i64, start_date: []const u8, end_date: []const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_transaction_history
            \\WHERE book_id = ? AND counterparty_id = ?
            \\  AND posting_date BETWEEN ? AND ?
            \\  AND (? IS NULL OR account_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, counterparty_id);
        try stmt.bindText(3, start_date);
        try stmt.bindText(4, end_date);
        if (account_filter) |af| { try stmt.bindInt(5, af); try stmt.bindInt(6, af); } else { try stmt.bindNull(5); try stmt.bindNull(6); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    // Compute opening balance from prior transactions
    var opening_balance: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(base_debit_amount), 0), COALESCE(SUM(base_credit_amount), 0)
            \\FROM ledger_transaction_history
            \\WHERE book_id = ? AND counterparty_id = ?
            \\  AND posting_date < ?
            \\  AND (? IS NULL OR account_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, counterparty_id);
        try stmt.bindText(3, start_date);
        if (account_filter) |af| { try stmt.bindInt(4, af); try stmt.bindInt(5, af); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        _ = try stmt.step();
        const prior_debits = stmt.columnInt64(0);
        const prior_credits = stmt.columnInt64(1);
        opening_balance = prior_debits - prior_credits;
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT posting_date, document_number, entry_description,
        \\  account_number, account_name,
        \\  base_debit_amount, base_credit_amount
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id = ?
        \\  AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR account_id = ?)
        \\ORDER BY posting_date ASC, document_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT posting_date, document_number, entry_description,
        \\  account_number, account_name,
        \\  base_debit_amount, base_credit_amount
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id = ?
        \\  AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR account_id = ?)
        \\ORDER BY posting_date DESC, document_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindInt(2, counterparty_id);
    try stmt.bindText(3, start_date);
    try stmt.bindText(4, end_date);
    if (account_filter) |af| { try stmt.bindInt(5, af); try stmt.bindInt(6, af); } else { try stmt.bindNull(5); try stmt.bindNull(6); }
    try stmt.bindInt(7, limit);
    try stmt.bindInt(8, offset);

    var running: i64 = opening_balance;
    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const ob = std.fmt.bufPrint(buf[pos..], "# opening_balance={d}\n", .{opening_balance}) catch return error.InvalidInput;
            pos += ob.len;
            const header = "posting_date,document_number,description,account_number,account_name,debit_amount,credit_amount,running_balance\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const debit = stmt.columnInt64(5);
                const credit = stmt.columnInt64(6);
                running += debit - credit;

                pos += try export_mod.csvField(buf[pos..], stmt.columnText(0) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{ debit, credit, running }) catch return error.InvalidInput;
                pos += nums.len;
            }
        },
        .json => {
            const meta = std.fmt.bufPrint(buf[pos..], "{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"has_more\":{s},\"opening_balance\":{d},\"rows\":[", .{
                total, limit, offset, if (offset + limit < total) "true" else "false", opening_balance,
            }) catch return error.InvalidInput;
            pos += meta.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const debit = stmt.columnInt64(5);
                const credit = stmt.columnInt64(6);
                running += debit - credit;

                const j1 = "{\"posting_date\":\"";
                if (pos + j1.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(0) orelse "");
                const j2 = "\",\"document_number\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j3 = "\",\"description\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j4 = "\",\"account_number\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j5 = "\",\"account_name\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j6 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"running_balance\":{d}}}", .{ debit, credit, running }) catch return error.InvalidInput;
                pos += j6.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── listTransactions (paginated GL with counterparty filter) ───

pub fn listTransactions(database: db_mod.Database, book_id: i64, account_filter: ?i64, counterparty_filter: ?i64, start_date: []const u8, end_date: []const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_transaction_history
            \\WHERE book_id = ? AND posting_date BETWEEN ? AND ?
            \\  AND (? IS NULL OR account_id = ?)
            \\  AND (? IS NULL OR counterparty_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, start_date);
        try stmt.bindText(3, end_date);
        if (account_filter) |af| { try stmt.bindInt(4, af); try stmt.bindInt(5, af); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
        if (counterparty_filter) |cf| { try stmt.bindInt(6, cf); try stmt.bindInt(7, cf); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT posting_date, document_number, entry_description,
        \\  account_number, account_name, counterparty_number, counterparty_name,
        \\  base_debit_amount, base_credit_amount
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR account_id = ?)
        \\  AND (? IS NULL OR counterparty_id = ?)
        \\ORDER BY posting_date ASC, document_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT posting_date, document_number, entry_description,
        \\  account_number, account_name, counterparty_number, counterparty_name,
        \\  base_debit_amount, base_credit_amount
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR account_id = ?)
        \\  AND (? IS NULL OR counterparty_id = ?)
        \\ORDER BY posting_date DESC, document_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    if (account_filter) |af| { try stmt.bindInt(4, af); try stmt.bindInt(5, af); } else { try stmt.bindNull(4); try stmt.bindNull(5); }
    if (counterparty_filter) |cf| { try stmt.bindInt(6, cf); try stmt.bindInt(7, cf); } else { try stmt.bindNull(6); try stmt.bindNull(7); }
    try stmt.bindInt(8, limit);
    try stmt.bindInt(9, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "posting_date,document_number,description,account_number,account_name,counterparty_number,counterparty_name,debit_amount,credit_amount\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(0) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d}\n", .{ stmt.columnInt64(7), stmt.columnInt64(8) }) catch return error.InvalidInput;
                pos += nums.len;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = "{\"posting_date\":\"";
                if (pos + j1.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(0) orelse "");
                const j2 = "\",\"document_number\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j3 = "\",\"description\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j4 = "\",\"account_number\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j5 = "\",\"account_name\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j6 = "\",\"counterparty_number\":\"";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j7 = "\",\"counterparty_name\":\"";
                if (pos + j7.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j8 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d}}}", .{ stmt.columnInt64(7), stmt.columnInt64(8) }) catch return error.InvalidInput;
                pos += j8.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── subledgerReconciliation ────────────────────────────────────

pub fn subledgerReconciliation(database: db_mod.Database, book_id: i64, group_id: i64, as_of_date: []const u8, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    // Subledger total: sum of all counterparty transactions in this group
    var sl_debits: i64 = 0;
    var sl_credits: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(base_debit_amount), 0), COALESCE(SUM(base_credit_amount), 0)
            \\FROM ledger_transaction_history
            \\WHERE book_id = ? AND subledger_group_id = ?
            \\  AND posting_date <= ? AND counterparty_id IS NOT NULL;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, group_id);
        try stmt.bindText(3, as_of_date);
        _ = try stmt.step();
        sl_debits = stmt.columnInt64(0);
        sl_credits = stmt.columnInt64(1);
    }

    // Control account total: from balance cache
    var gl_debits: i64 = 0;
    var gl_credits: i64 = 0;
    var control_account_number: [50]u8 = undefined;
    var control_number_len: usize = 0;
    {
        var stmt = try database.prepare(
            \\SELECT a.number, COALESCE(SUM(ab.debit_sum), 0), COALESCE(SUM(ab.credit_sum), 0)
            \\FROM ledger_subledger_groups sg
            \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
            \\LEFT JOIN ledger_account_balances ab ON ab.account_id = a.id
            \\  AND ab.book_id = sg.book_id
            \\LEFT JOIN ledger_periods p ON p.id = ab.period_id AND p.end_date <= ?
            \\WHERE sg.id = ? AND sg.book_id = ?
            \\GROUP BY a.id;
        );
        defer stmt.finalize();
        try stmt.bindText(1, as_of_date);
        try stmt.bindInt(2, group_id);
        try stmt.bindInt(3, book_id);
        if (try stmt.step()) {
            const num = stmt.columnText(0) orelse "";
            control_number_len = @min(num.len, control_account_number.len);
            @memcpy(control_account_number[0..control_number_len], num[0..control_number_len]);
            gl_debits = stmt.columnInt64(1);
            gl_credits = stmt.columnInt64(2);
        } else {
            return error.NotFound;
        }
    }

    const sl_balance = sl_debits - sl_credits;
    const gl_balance = gl_debits - gl_credits;
    const difference = gl_balance - sl_balance;
    const is_reconciled = difference == 0;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "control_account,gl_debits,gl_credits,gl_balance,sl_debits,sl_credits,sl_balance,difference,reconciled\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            pos += try export_mod.csvField(buf[pos..], control_account_number[0..control_number_len]);
            const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},{d},{d},{d},{s}\n", .{
                gl_debits, gl_credits, gl_balance, sl_debits, sl_credits, sl_balance, difference, if (is_reconciled) "true" else "false",
            }) catch return error.InvalidInput;
            pos += nums.len;
        },
        .json => {
            const j1 = "{\"control_account\":\"";
            if (pos + j1.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j1.len], j1);
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], control_account_number[0..control_number_len]);
            const j2 = std.fmt.bufPrint(buf[pos..], "\",\"gl_debits\":{d},\"gl_credits\":{d},\"gl_balance\":{d},\"sl_debits\":{d},\"sl_credits\":{d},\"sl_balance\":{d},\"difference\":{d},\"reconciled\":{s}}}", .{
                gl_debits, gl_credits, gl_balance, sl_debits, sl_credits, sl_balance, difference, if (is_reconciled) "true" else "false",
            }) catch return error.InvalidInput;
            pos += j2.len;
        },
    }
    return buf[0..pos];
}

// ── agedSubledger ──────────────────────────────────────────────

pub fn agedSubledger(database: db_mod.Database, book_id: i64, group_id: ?i64, as_of_date: []const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    // Aging buckets: current (0-30), past_due_31_60, past_due_61_90, past_due_90_plus
    // We compute per-counterparty by comparing each transaction's posting_date to as_of_date
    // using SQLite date arithmetic

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(DISTINCT counterparty_id) FROM ledger_transaction_history
            \\WHERE book_id = ? AND counterparty_id IS NOT NULL
            \\  AND posting_date <= ?
            \\  AND (? IS NULL OR subledger_group_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, as_of_date);
        if (group_id) |gf| { try stmt.bindInt(3, gf); try stmt.bindInt(4, gf); } else { try stmt.bindNull(3); try stmt.bindNull(4); }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT counterparty_id, counterparty_number, counterparty_name,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 0 AND 30
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS current_bal,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 31 AND 60
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_31_60,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 61 AND 90
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_61_90,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) > 90
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_90_plus,
        \\  SUM(base_debit_amount - base_credit_amount) AS total_balance
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id IS NOT NULL
        \\  AND posting_date <= ?
        \\  AND (? IS NULL OR subledger_group_id = ?)
        \\GROUP BY counterparty_id
        \\ORDER BY counterparty_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT counterparty_id, counterparty_number, counterparty_name,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 0 AND 30
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS current_bal,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 31 AND 60
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_31_60,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) BETWEEN 61 AND 90
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_61_90,
        \\  SUM(CASE WHEN julianday(?) - julianday(posting_date) > 90
        \\    THEN base_debit_amount - base_credit_amount ELSE 0 END) AS pd_90_plus,
        \\  SUM(base_debit_amount - base_credit_amount) AS total_balance
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id IS NOT NULL
        \\  AND posting_date <= ?
        \\  AND (? IS NULL OR subledger_group_id = ?)
        \\GROUP BY counterparty_id
        \\ORDER BY counterparty_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    // Bind as_of_date 4 times for the 4 CASE expressions
    try stmt.bindText(1, as_of_date);
    try stmt.bindText(2, as_of_date);
    try stmt.bindText(3, as_of_date);
    try stmt.bindText(4, as_of_date);
    try stmt.bindInt(5, book_id);
    try stmt.bindText(6, as_of_date);
    if (group_id) |gf| { try stmt.bindInt(7, gf); try stmt.bindInt(8, gf); } else { try stmt.bindNull(7); try stmt.bindNull(8); }
    try stmt.bindInt(9, limit);
    try stmt.bindInt(10, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "counterparty_id,counterparty_number,counterparty_name,current_0_30,past_due_31_60,past_due_61_90,past_due_90_plus,total\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},{d}\n", .{
                    stmt.columnInt64(3), stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.InvalidInput;
                pos += nums.len;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) { if (pos >= buf.len) return error.InvalidInput; buf[pos] = ','; pos += 1; }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"counterparty_id\":{d},\"counterparty_number\":\"", .{stmt.columnInt64(0)}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"counterparty_name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"current_0_30\":{d},\"past_due_31_60\":{d},\"past_due_61_90\":{d},\"past_due_90_plus\":{d},\"total\":{d}}}", .{
                    stmt.columnInt64(3), stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.InvalidInput;
                pos += j3.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── Tests ──────────────────────────────────────────────────────

const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");

test "getBook: returns book data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test Book", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getBook(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Test Book\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"base_currency\":\"PHP\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "getBook: returns NotFound for nonexistent book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    const result = getBook(database, 999, &buf, .json);
    try std.testing.expectError(error.NotFound, result);
}

test "getBook: CSV format" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "USD", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try getBook(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,base_currency") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Test") != null);
}

test "listBooks: returns all books with pagination metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "Book B", "USD", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Book A\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Book B\"") != null);
}

test "listBooks: filter by status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Active Book", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "Archived Book", "USD", 2, "admin");
    try book_mod.Book.archive(database, 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, "active", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Active Book") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Archived Book") == null);
}

test "listBooks: pagination with limit and offset" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "A", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "B", "USD", 2, "admin");
    _ = try book_mod.Book.create(database, "C", "EUR", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 2, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_more\":true") != null);
}

test "listBooks: CSV with metadata comment" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try listBooks(database, null, .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "# total=1,limit=100,offset=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,base_currency") != null);
}

test "listBooks: empty result" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

test "getAccount: returns account data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getAccount(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Cash\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_type\":\"asset\"") != null);
}

test "getAccount: returns NotFound for nonexistent" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    const result = getAccount(database, 999, &buf, .json);
    try std.testing.expectError(error.NotFound, result);
}

test "listAccounts: returns all accounts with total" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"2000\"") != null);
}

test "listAccounts: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, "asset", null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "AP") == null);
}

test "listAccounts: text search on name" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash on Hand", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1100", "Cash in Bank", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, "Cash", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash on Hand") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash in Bank") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Accounts Payable") == null);
}

test "listAccounts: descending order" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, null, .desc, 100, 0, &buf, .json);

    // 2000 should come before 1000 in desc order
    const pos_2000 = std.mem.indexOf(u8, json, "\"number\":\"2000\"").?;
    const pos_1000 = std.mem.indexOf(u8, json, "\"number\":\"1000\"").?;
    try std.testing.expect(pos_2000 < pos_1000);
}

// ── Period tests ───────────────────────────────────────────────

const period_mod = @import("period.zig");

test "getPeriod: returns period data" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var buf: [4096]u8 = undefined;
    const json = try getPeriod(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan 2026\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"start_date\":\"2026-01-01\"") != null);
}

test "getPeriod: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getPeriod(database, 999, &buf, .json));
}

test "listPeriods: returns all with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Feb", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listPeriods(database, 1, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Feb\"") != null);
}

test "listPeriods: filter by year" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2027", 1, 2027, "2027-01-01", "2027-01-31", "regular", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listPeriods(database, 1, 2026, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Jan 2026") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Jan 2027") == null);
}

// ── Entry tests ────────────────────────────────────────────────

const entry_mod = @import("entry.zig");
const money = @import("money.zig");

test "listEntries: returns entries with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;

    // All entries
    const all = try listEntries(database, 1, null, null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":2") != null);

    // Only posted
    const posted = try listEntries(database, 1, "posted", null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, posted, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, posted, "JE-001") != null);

    // Search by doc number
    const search = try listEntries(database, 1, null, null, null, "DRAFT", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, search, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, search, "DRAFT-001") != null);
}

test "listEntries: date range filtering" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listEntries(database, 1, null, "2026-01-15", "2026-01-31", null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-002") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-001") == null);
}

// ── Audit Log tests ────────────────────────────────────────────

test "listAuditLog: returns audit records with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var buf: [32768]u8 = undefined;

    // All audit records
    const all = try listAuditLog(database, 1, null, null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"rows\":[") != null);

    // Filter by entity_type
    const acct_only = try listAuditLog(database, 1, "account", null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, acct_only, "\"entity_type\":\"account\"") != null);

    // Filter by action
    const creates = try listAuditLog(database, 1, null, "create", null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, creates, "\"action\":\"create\"") != null);
}

test "listAuditLog: descending order" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [32768]u8 = undefined;
    const json = try listAuditLog(database, 1, null, null, null, null, .desc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "listAuditLog: CSV with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [32768]u8 = undefined;
    const csv = try listAuditLog(database, 1, null, null, null, null, .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "# total=") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,entity_type") != null);
}

// ── getEntry tests ─────────────────────────────────────────────

test "getEntry: returns entry data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Test entry", 1, null, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getEntry(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_number\":\"JE-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"description\":\"Test entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"draft\"") != null);
}

test "getEntry: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getEntry(database, 999, &buf, .json));
}

// ── listEntryLines tests ───────────────────────────────────────

test "listEntryLines: returns lines for entry" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listEntryLines(database, eid, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"lines\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_number\":\"3000\"") != null);
}

test "listEntryLines: CSV format" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try listEntryLines(database, eid, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,line_number,account_number") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);
}

// ── listClassifications tests ──────────────────────────────────

const classification_mod = @import("classification.zig");

test "listClassifications: returns all with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try classification_mod.Classification.create(database, 1, "BS Layout", "balance_sheet", "admin");
    _ = try classification_mod.Classification.create(database, 1, "IS Layout", "income_statement", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listClassifications(database, 1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "BS Layout") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "IS Layout") != null);
}

test "listClassifications: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try classification_mod.Classification.create(database, 1, "BS", "balance_sheet", "admin");
    _ = try classification_mod.Classification.create(database, 1, "IS", "income_statement", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listClassifications(database, 1, "balance_sheet", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "BS") != null);
}

// ── listSubledgerGroups tests ──────────────────────────────────

const subledger_mod = @import("subledger.zig");

test "listSubledgerGroups: returns groups with control account" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Suppliers", "supplier", 2, 2, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerGroups(database, 1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Customers") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Suppliers") != null);
}

test "listSubledgerGroups: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerGroups(database, 1, "customer", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
}

// ── listSubledgerAccounts tests ────────────────────────────────

test "listSubledgerAccounts: returns counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Widget Inc", "customer", gid, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Widget Inc") != null);
}

test "listSubledgerAccounts: filter by group" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    const g1 = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    const g2 = try subledger_mod.SubledgerGroup.create(database, 1, "Suppliers", "supplier", 2, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Customer A", "customer", g1, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "S001", "Supplier X", "supplier", g2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, g1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Customer A") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Supplier X") == null);
}

test "listSubledgerAccounts: name search" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Beta LLC", "customer", gid, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, null, "Acme", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Beta LLC") == null);
}

// ── subledgerReport tests ──────────────────────────────────────

test "subledgerReport: returns counterparty balances" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try subledgerReport(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_name\":\"Acme Corp\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "subledgerReport: empty when no counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try subledgerReport(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

// ── counterpartyLedger tests ───────────────────────────────────

test "counterpartyLedger: returns transactions for counterparty" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "INV-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "counterpartyLedger: empty for nonexistent counterparty" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 999, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
}

// ── listTransactions tests ─────────────────────────────────────

test "listTransactions: paginated GL with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;

    // All transactions
    const all = try listTransactions(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":2") != null);

    // Filter by account
    const cash_only = try listTransactions(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, cash_only, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, cash_only, "\"account_number\":\"1000\"") != null);
}

test "listTransactions: pagination with limit" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listTransactions(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 1, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_more\":true") != null);
}

// ── counterpartyLedger running balance test ────────────────────

test "counterpartyLedger: includes opening_balance and running_balance" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"running_balance\":") != null);
}

// ── subledgerReconciliation tests ──────────────────────────────

test "subledgerReconciliation: JSON structure" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    var buf: [4096]u8 = undefined;
    const json = try subledgerReconciliation(database, 1, gid, "2026-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"control_account\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"gl_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"sl_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"difference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reconciled\":") != null);
}

// ── agedSubledger tests ────────────────────────────────────────

test "agedSubledger: returns aging buckets" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try agedSubledger(database, 1, gid, "2026-02-28", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_name\":\"Acme\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"current_0_30\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_31_60\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_61_90\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_90_plus\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":") != null);
}

test "agedSubledger: empty with no counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try agedSubledger(database, 1, null, "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

test "agedSubledger: CSV format with header" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try agedSubledger(database, 1, null, "2026-01-31", .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "counterparty_id,counterparty_number,counterparty_name,current_0_30") != null);
}

// ── subledgerReport name search test ───────────────────────────

test "subledgerReport: name search filter" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Beta LLC", "customer", gid, "admin");

    // Post entry for Acme only
    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    // Post entry for Beta
    const eid2 = try entry_mod.Entry.createDraft(database, 1, "INV-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 200000000000, 'PHP', 10000000000, 2, 2, 2);");
    try entry_mod.Entry.post(database, eid2, "admin");

    var buf: [16384]u8 = undefined;

    // Search for "Acme" - should only return Acme Corp
    const json = try subledgerReport(database, 1, null, "Acme", "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Beta LLC") == null);
}

// ── getClassification tests ────────────────────────────────────

test "getClassification: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    const cid = try classification_mod.Classification.create(database, 1, "BS Layout", "balance_sheet", "admin");

    var buf: [4096]u8 = undefined;
    const json = try getClassification(database, cid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"BS Layout\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"report_type\":\"balance_sheet\"") != null);
}

test "getClassification: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getClassification(database, 999, &buf, .json));
}

// ── getSubledgerGroup tests ────────────────────────────────────

test "getSubledgerGroup: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getSubledgerGroup(database, gid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Customers\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"type\":\"customer\"") != null);
}

test "getSubledgerGroup: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getSubledgerGroup(database, 999, &buf, .json));
}

// ── getSubledgerAccount tests ──────────────────────────────────

test "getSubledgerAccount: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    const aid = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getSubledgerAccount(database, aid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Acme Corp\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"C001\"") != null);
}

test "getSubledgerAccount: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getSubledgerAccount(database, 999, &buf, .json));
}
