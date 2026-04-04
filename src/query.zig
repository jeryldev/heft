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
