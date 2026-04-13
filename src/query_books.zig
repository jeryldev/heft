const std = @import("std");
const db_mod = @import("db.zig");
const export_mod = @import("export.zig");
const common = @import("query_common.zig");

pub const SortOrder = common.SortOrder;
const clampLimit = common.clampLimit;
const clampOffset = common.clampOffset;
const writeJsonMeta = common.writeJsonMeta;
const writeCsvMeta = common.writeCsvMeta;

// ── getBook ────────────────────────────────────────────────────

pub fn getBook(database: db_mod.Database, book_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, base_currency, decimal_places, status,
        \\rounding_account_id, fx_gain_loss_account_id,
        \\retained_earnings_account_id, income_summary_account_id,
        \\opening_balance_account_id, suspense_account_id
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
    const rounding_id = stmt.columnInt64(5);
    const fx_gl_id = stmt.columnInt64(6);
    const re_id = stmt.columnInt64(7);
    const is_id = stmt.columnInt64(8);
    const ob_id = stmt.columnInt64(9);
    const suspense_id = stmt.columnInt64(10);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,name,base_currency,decimal_places,status,rounding_account_id,fx_gain_loss_account_id,retained_earnings_account_id,income_summary_account_id,opening_balance_account_id,suspense_account_id\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], name);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], currency);
            const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.BufferTooSmall;
            pos += rest.len;
            pos += try export_mod.csvField(buf[pos..], status);
            const sys_accts = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},{d},{d}\n", .{ rounding_id, fx_gl_id, re_id, is_id, ob_id, suspense_id }) catch return error.BufferTooSmall;
            pos += sys_accts.len;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], name);
            const j2 = "\",\"base_currency\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], currency);
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.BufferTooSmall;
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j4 = std.fmt.bufPrint(buf[pos..], "\",\"rounding_account_id\":{d},\"fx_gain_loss_account_id\":{d},\"retained_earnings_account_id\":{d},\"income_summary_account_id\":{d},\"opening_balance_account_id\":{d},\"suspense_account_id\":{d}}}", .{ rounding_id, fx_gl_id, re_id, is_id, ob_id, suspense_id }) catch return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const currency = stmt.columnText(2) orelse "";
                const decimals = stmt.columnInt(3);
                const status = stmt.columnText(4) orelse "";

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], currency);
                const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.BufferTooSmall;
                pos += rest.len;
                pos += try export_mod.csvField(buf[pos..], status);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const currency = stmt.columnText(2) orelse "";
                const decimals = stmt.columnInt(3);
                const status = stmt.columnText(4) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const j2 = "\",\"base_currency\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], currency);
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.BufferTooSmall;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], status);
                const j4 = "\"}";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], number);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], name);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], acct_type);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], normal_bal);
            const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.BufferTooSmall;
            pos += rest.len;
            pos += try export_mod.csvField(buf[pos..], status);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{id}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], number);
            const j2 = "\",\"name\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], name);
            const j3 = "\",\"account_type\":\"";
            if (pos + j3.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], acct_type);
            const j4 = "\",\"normal_balance\":\"";
            if (pos + j4.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], normal_bal);
            const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{s},\"status\":\"", .{if (is_contra != 0) "true" else "false"}) catch return error.BufferTooSmall;
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j6 = "\"}";
            if (pos + j6.len > buf.len) return error.BufferTooSmall;
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
        if (type_filter) |tf| {
            try stmt.bindText(2, tf);
            try stmt.bindText(3, tf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
        if (status_filter) |sf| {
            try stmt.bindText(4, sf);
            try stmt.bindText(5, sf);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
        if (name_search) |ns| {
            try stmt.bindText(6, ns);
            try stmt.bindText(7, ns);
        } else {
            try stmt.bindNull(6);
            try stmt.bindNull(7);
        }
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
    if (type_filter) |tf| {
        try stmt.bindText(2, tf);
        try stmt.bindText(3, tf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    if (status_filter) |sf| {
        try stmt.bindText(4, sf);
        try stmt.bindText(5, sf);
    } else {
        try stmt.bindNull(4);
        try stmt.bindNull(5);
    }
    if (name_search) |ns| {
        try stmt.bindText(6, ns);
        try stmt.bindText(7, ns);
    } else {
        try stmt.bindNull(6);
        try stmt.bindNull(7);
    }
    try stmt.bindInt(8, limit);
    try stmt.bindInt(9, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,number,name,account_type,normal_balance,is_contra,status\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], number);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], acct_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], normal_bal);
                const rest = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.BufferTooSmall;
                pos += rest.len;
                pos += try export_mod.csvField(buf[pos..], status);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
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

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{id}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], number);
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const j3 = "\",\"account_type\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], acct_type);
                const j4 = "\",\"normal_balance\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], normal_bal);
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{s},\"status\":\"", .{if (is_contra != 0) "true" else "false"}) catch return error.BufferTooSmall;
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], status);
                const j6 = "\"}";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.BufferTooSmall;
            pos += nums.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.BufferTooSmall;
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
            const j3 = "\",\"end_date\":\"";
            if (pos + j3.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
            const j4 = "\",\"period_type\":\"";
            if (pos + j4.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
            const j5 = "\",\"status\":\"";
            if (pos + j5.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j5.len], j5);
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
            const j6 = "\"}";
            if (pos + j6.len > buf.len) return error.BufferTooSmall;
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
        if (year_filter) |yf| {
            try stmt.bindInt(2, yf);
            try stmt.bindInt(3, yf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
        if (status_filter) |sf| {
            try stmt.bindText(4, sf);
            try stmt.bindText(5, sf);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
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
    if (year_filter) |yf| {
        try stmt.bindInt(2, yf);
        try stmt.bindInt(3, yf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    if (status_filter) |sf| {
        try stmt.bindText(4, sf);
        try stmt.bindText(5, sf);
    } else {
        try stmt.bindNull(4);
        try stmt.bindNull(5);
    }
    try stmt.bindInt(6, limit);
    try stmt.bindInt(7, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,period_number,year,start_date,end_date,period_type,status\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.BufferTooSmall;
                pos += nums.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            pos += try writeJsonMeta(buf[pos..], total, limit, offset);
            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ stmt.columnInt(2), stmt.columnInt(3) }) catch return error.BufferTooSmall;
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j3 = "\",\"end_date\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j4 = "\",\"period_type\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
                const j6 = "\"}";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}
