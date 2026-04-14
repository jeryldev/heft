const std = @import("std");
const db_mod = @import("db.zig");
const export_mod = @import("export.zig");
const common = @import("query_common.zig");

pub const SortOrder = common.SortOrder;
const clampLimit = common.clampLimit;
const clampOffset = common.clampOffset;
const writeJsonMeta = common.writeJsonMeta;
const writeCsvMeta = common.writeCsvMeta;

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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"report_type\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = "\"}";
            if (pos + j3.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            const gn = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt(3)}) catch return error.BufferTooSmall;
            pos += gn.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"type\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"group_number\":{d},\"control_account_number\":\"", .{stmt.columnInt(3)}) catch return error.BufferTooSmall;
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
            const j4 = "\"}";
            if (pos + j4.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
            const gid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(4)}) catch return error.BufferTooSmall;
            pos += gid.len;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
            const j2 = "\",\"name\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
            const j3 = "\",\"type\":\"";
            if (pos + j3.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
            const j4 = std.fmt.bufPrint(buf[pos..], "\",\"group_id\":{d}}}", .{stmt.columnInt64(4)}) catch return error.BufferTooSmall;
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
        if (type_filter) |tf| {
            try stmt.bindText(2, tf);
            try stmt.bindText(3, tf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
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
    if (type_filter) |tf| {
        try stmt.bindText(2, tf);
        try stmt.bindText(3, tf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    try stmt.bindInt(4, limit);
    try stmt.bindInt(5, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,report_type\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
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
                const j2 = "\",\"report_type\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\"}";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
        if (type_filter) |tf| {
            try stmt.bindText(2, tf);
            try stmt.bindText(3, tf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
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
    if (type_filter) |tf| {
        try stmt.bindText(2, tf);
        try stmt.bindText(3, tf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    try stmt.bindInt(4, limit);
    try stmt.bindInt(5, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,name,type,group_number,control_account_number\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                const gn = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt(3)}) catch return error.BufferTooSmall;
                pos += gn.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
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
                const j2 = "\",\"type\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"group_number\":{d},\"control_account_number\":\"", .{stmt.columnInt(3)}) catch return error.BufferTooSmall;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
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
        if (group_filter) |gf| {
            try stmt.bindInt(2, gf);
            try stmt.bindInt(3, gf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
        if (name_search) |ns| {
            try stmt.bindText(4, ns);
            try stmt.bindText(5, ns);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
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
    if (group_filter) |gf| {
        try stmt.bindInt(2, gf);
        try stmt.bindInt(3, gf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    if (name_search) |ns| {
        try stmt.bindText(4, ns);
        try stmt.bindText(5, ns);
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
            const header = "id,number,name,type,group_id\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                const gid = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(4)}) catch return error.BufferTooSmall;
                pos += gid.len;
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
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"number\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"type\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = std.fmt.bufPrint(buf[pos..], "\",\"group_id\":{d}}}", .{stmt.columnInt64(4)}) catch return error.BufferTooSmall;
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
        if (group_id) |gf| {
            try stmt.bindInt(4, gf);
            try stmt.bindInt(5, gf);
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
        \\SELECT counterparty_id, counterparty_number, counterparty_name,
        \\  subledger_group_name, account_number,
        \\  SUM(base_debit_amount), SUM(base_credit_amount)
        \\FROM ledger_transaction_history
        \\WHERE book_id = ? AND counterparty_id IS NOT NULL
        \\  AND posting_date BETWEEN ? AND ?
        \\  AND (? IS NULL OR subledger_group_id = ?)
        \\  AND (? IS NULL OR counterparty_name LIKE '%' || ? || '%')
        \\GROUP BY counterparty_id, counterparty_number, counterparty_name, subledger_group_name, account_number
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
        \\GROUP BY counterparty_id, counterparty_number, counterparty_name, subledger_group_name, account_number
        \\ORDER BY counterparty_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    if (group_id) |gf| {
        try stmt.bindInt(4, gf);
        try stmt.bindInt(5, gf);
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
            const header = "counterparty_id,counterparty_number,counterparty_name,group_name,control_account,debit_balance,credit_balance\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d}\n", .{ stmt.columnInt64(5), stmt.columnInt64(6) }) catch return error.BufferTooSmall;
                pos += nums.len;
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
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"counterparty_id\":{d},\"counterparty_number\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"counterparty_name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"group_name\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = "\",\"control_account\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"debit_balance\":{d},\"credit_balance\":{d}}}", .{ stmt.columnInt64(5), stmt.columnInt64(6) }) catch return error.BufferTooSmall;
                pos += j5.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
        if (account_filter) |af| {
            try stmt.bindInt(5, af);
            try stmt.bindInt(6, af);
        } else {
            try stmt.bindNull(5);
            try stmt.bindNull(6);
        }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    var is_debit_normal = true;
    {
        var nb_stmt = try database.prepare(
            \\SELECT a.normal_balance
            \\FROM ledger_subledger_accounts sa
            \\JOIN ledger_subledger_groups sg ON sg.id = sa.group_id
            \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
            \\WHERE sa.id = ?;
        );
        defer nb_stmt.finalize();
        try nb_stmt.bindInt(1, counterparty_id);
        const has_nb = try nb_stmt.step();
        if (has_nb) {
            const nb = nb_stmt.columnText(0) orelse "debit";
            is_debit_normal = std.mem.eql(u8, nb, "debit");
        }
    }

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
        if (account_filter) |af| {
            try stmt.bindInt(4, af);
            try stmt.bindInt(5, af);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
        _ = try stmt.step();
        const prior_debits = stmt.columnInt64(0);
        const prior_credits = stmt.columnInt64(1);
        opening_balance = if (is_debit_normal)
            std.math.sub(i64, prior_debits, prior_credits) catch return error.AmountOverflow
        else
            std.math.sub(i64, prior_credits, prior_debits) catch return error.AmountOverflow;
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
    if (account_filter) |af| {
        try stmt.bindInt(5, af);
        try stmt.bindInt(6, af);
    } else {
        try stmt.bindNull(5);
        try stmt.bindNull(6);
    }
    try stmt.bindInt(7, limit);
    try stmt.bindInt(8, offset);

    var running: i64 = opening_balance;
    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const ob = std.fmt.bufPrint(buf[pos..], "# opening_balance={d}\n", .{opening_balance}) catch return error.BufferTooSmall;
            pos += ob.len;
            const header = "posting_date,document_number,description,account_number,account_name,debit_amount,credit_amount,running_balance\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const debit = stmt.columnInt64(5);
                const credit = stmt.columnInt64(6);
                const diff = if (is_debit_normal)
                    std.math.sub(i64, debit, credit) catch return error.AmountOverflow
                else
                    std.math.sub(i64, credit, debit) catch return error.AmountOverflow;
                running = std.math.add(i64, running, diff) catch return error.AmountOverflow;

                pos += try export_mod.csvField(buf[pos..], stmt.columnText(0) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{ debit, credit, running }) catch return error.BufferTooSmall;
                pos += nums.len;
            }
        },
        .json => {
            const meta = std.fmt.bufPrint(buf[pos..], "{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"has_more\":{s},\"opening_balance\":{d},\"rows\":[", .{
                total, limit, offset, if (@as(i64, offset) + @as(i64, limit) < total) "true" else "false", opening_balance,
            }) catch return error.BufferTooSmall;
            pos += meta.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;
                const debit = stmt.columnInt64(5);
                const credit = stmt.columnInt64(6);
                const diff = if (is_debit_normal)
                    std.math.sub(i64, debit, credit) catch return error.AmountOverflow
                else
                    std.math.sub(i64, credit, debit) catch return error.AmountOverflow;
                running = std.math.add(i64, running, diff) catch return error.AmountOverflow;

                const j1 = "{\"posting_date\":\"";
                if (pos + j1.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(0) orelse "");
                const j2 = "\",\"document_number\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j3 = "\",\"description\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j4 = "\",\"account_number\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j5 = "\",\"account_name\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j6 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"running_balance\":{d}}}", .{ debit, credit, running }) catch return error.BufferTooSmall;
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
        if (account_filter) |af| {
            try stmt.bindInt(4, af);
            try stmt.bindInt(5, af);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
        if (counterparty_filter) |cf| {
            try stmt.bindInt(6, cf);
            try stmt.bindInt(7, cf);
        } else {
            try stmt.bindNull(6);
            try stmt.bindNull(7);
        }
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
    if (account_filter) |af| {
        try stmt.bindInt(4, af);
        try stmt.bindInt(5, af);
    } else {
        try stmt.bindNull(4);
        try stmt.bindNull(5);
    }
    if (counterparty_filter) |cf| {
        try stmt.bindInt(6, cf);
        try stmt.bindInt(7, cf);
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
            const header = "posting_date,document_number,description,account_number,account_name,counterparty_number,counterparty_name,debit_amount,credit_amount\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(0) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(6) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d}\n", .{ stmt.columnInt64(7), stmt.columnInt64(8) }) catch return error.BufferTooSmall;
                pos += nums.len;
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
                const j1 = "{\"posting_date\":\"";
                if (pos + j1.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(0) orelse "");
                const j2 = "\",\"document_number\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j3 = "\",\"description\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j4 = "\",\"account_number\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j5 = "\",\"account_name\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j6 = "\",\"counterparty_number\":\"";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j7 = "\",\"counterparty_name\":\"";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j8 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d}}}", .{ stmt.columnInt64(7), stmt.columnInt64(8) }) catch return error.BufferTooSmall;
                pos += j8.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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

    var gl_debits: i64 = 0;
    var gl_credits: i64 = 0;
    var control_account_number: [50]u8 = undefined;
    var control_number_len: usize = 0;
    {
        var num_stmt = try database.prepare(
            \\SELECT a.number FROM ledger_subledger_groups sg
            \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
            \\WHERE sg.id = ? AND sg.book_id = ?;
        );
        defer num_stmt.finalize();
        try num_stmt.bindInt(1, group_id);
        try num_stmt.bindInt(2, book_id);
        if (try num_stmt.step()) {
            const num = num_stmt.columnText(0) orelse "";
            control_number_len = @min(num.len, control_account_number.len);
            @memcpy(control_account_number[0..control_number_len], num[0..control_number_len]);
        } else {
            return error.NotFound;
        }
    }
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(th.base_debit_amount), 0), COALESCE(SUM(th.base_credit_amount), 0)
            \\FROM ledger_transaction_history th
            \\JOIN ledger_subledger_groups sg ON sg.id = ?
            \\WHERE th.book_id = ? AND th.account_id = sg.gl_account_id AND th.posting_date <= ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, group_id);
        try stmt.bindInt(2, book_id);
        try stmt.bindText(3, as_of_date);
        _ = try stmt.step();
        gl_debits = stmt.columnInt64(0);
        gl_credits = stmt.columnInt64(1);
    }

    const sl_balance = std.math.sub(i64, sl_debits, sl_credits) catch return error.AmountOverflow;
    const gl_balance = std.math.sub(i64, gl_debits, gl_credits) catch return error.AmountOverflow;
    const difference = std.math.sub(i64, gl_balance, sl_balance) catch return error.AmountOverflow;
    const is_reconciled = difference == 0;

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "control_account,gl_debits,gl_credits,gl_balance,sl_debits,sl_credits,sl_balance,difference,reconciled\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            pos += try export_mod.csvField(buf[pos..], control_account_number[0..control_number_len]);
            const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},{d},{d},{d},{s}\n", .{
                gl_debits, gl_credits, gl_balance, sl_debits, sl_credits, sl_balance, difference, if (is_reconciled) "true" else "false",
            }) catch return error.BufferTooSmall;
            pos += nums.len;
        },
        .json => {
            const j1 = "{\"control_account\":\"";
            if (pos + j1.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j1.len], j1);
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], control_account_number[0..control_number_len]);
            const j2 = std.fmt.bufPrint(buf[pos..], "\",\"gl_debits\":{d},\"gl_credits\":{d},\"gl_balance\":{d},\"sl_debits\":{d},\"sl_credits\":{d},\"sl_balance\":{d},\"difference\":{d},\"reconciled\":{s}}}", .{
                gl_debits, gl_credits, gl_balance, sl_debits, sl_credits, sl_balance, difference, if (is_reconciled) "true" else "false",
            }) catch return error.BufferTooSmall;
            pos += j2.len;
        },
    }
    return buf[0..pos];
}

// ── agedSubledger ──────────────────────────────────────────────

pub fn agedSubledger(database: db_mod.Database, book_id: i64, group_id: ?i64, as_of_date: []const u8, order: SortOrder, limit_raw: i32, offset_raw: i32, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const limit = clampLimit(limit_raw);
    const offset = clampOffset(offset_raw);

    var cutoff_30_buf: [11]u8 = undefined;
    var cutoff_60_buf: [11]u8 = undefined;
    var cutoff_90_buf: [11]u8 = undefined;
    {
        var date_stmt = try database.prepare("SELECT date(?, '-30 day'), date(?, '-60 day'), date(?, '-90 day');");
        defer date_stmt.finalize();
        try date_stmt.bindText(1, as_of_date);
        try date_stmt.bindText(2, as_of_date);
        try date_stmt.bindText(3, as_of_date);
        _ = try date_stmt.step();
        @memcpy(cutoff_30_buf[0..10], date_stmt.columnText(0).?[0..10]);
        @memcpy(cutoff_60_buf[0..10], date_stmt.columnText(1).?[0..10]);
        @memcpy(cutoff_90_buf[0..10], date_stmt.columnText(2).?[0..10]);
    }
    const cutoff_30 = cutoff_30_buf[0..10];
    const cutoff_60 = cutoff_60_buf[0..10];
    const cutoff_90 = cutoff_90_buf[0..10];

    var total: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(DISTINCT sa.id)
            \\FROM ledger_entries e
            \\JOIN ledger_entry_lines l ON l.entry_id = e.id
            \\JOIN ledger_subledger_accounts sa ON sa.id = l.counterparty_id
            \\WHERE e.book_id = ? AND e.status = 'posted'
            \\  AND e.posting_date <= ?
            \\  AND (? IS NULL OR sa.group_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, as_of_date);
        if (group_id) |gf| {
            try stmt.bindInt(3, gf);
            try stmt.bindInt(4, gf);
        } else {
            try stmt.bindNull(3);
            try stmt.bindNull(4);
        }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT sa.id, sa.number, sa.name,
        \\  SUM(CASE WHEN e.posting_date >= ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS current_bal,
        \\  SUM(CASE WHEN e.posting_date >= ? AND e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_31_60,
        \\  SUM(CASE WHEN e.posting_date >= ? AND e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_61_90,
        \\  SUM(CASE WHEN e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_90_plus,
        \\  SUM(l.base_debit_amount - l.base_credit_amount) AS total_balance
        \\FROM ledger_entries e
        \\JOIN ledger_entry_lines l ON l.entry_id = e.id
        \\JOIN ledger_subledger_accounts sa ON sa.id = l.counterparty_id
        \\WHERE e.book_id = ? AND e.status = 'posted'
        \\  AND e.posting_date <= ?
        \\  AND (? IS NULL OR sa.group_id = ?)
        \\GROUP BY sa.id, sa.number, sa.name
        \\ORDER BY sa.number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT sa.id, sa.number, sa.name,
        \\  SUM(CASE WHEN e.posting_date >= ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS current_bal,
        \\  SUM(CASE WHEN e.posting_date >= ? AND e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_31_60,
        \\  SUM(CASE WHEN e.posting_date >= ? AND e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_61_90,
        \\  SUM(CASE WHEN e.posting_date < ?
        \\    THEN l.base_debit_amount - l.base_credit_amount ELSE 0 END) AS pd_90_plus,
        \\  SUM(l.base_debit_amount - l.base_credit_amount) AS total_balance
        \\FROM ledger_entries e
        \\JOIN ledger_entry_lines l ON l.entry_id = e.id
        \\JOIN ledger_subledger_accounts sa ON sa.id = l.counterparty_id
        \\WHERE e.book_id = ? AND e.status = 'posted'
        \\  AND e.posting_date <= ?
        \\  AND (? IS NULL OR sa.group_id = ?)
        \\GROUP BY sa.id, sa.number, sa.name
        \\ORDER BY sa.number DESC LIMIT ? OFFSET ?;
    ;

    var is_credit_normal = false;
    if (group_id) |gid| {
        var nb_stmt = try database.prepare(
            \\SELECT a.normal_balance FROM ledger_subledger_groups sg
            \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
            \\WHERE sg.id = ?;
        );
        defer nb_stmt.finalize();
        try nb_stmt.bindInt(1, gid);
        if (try nb_stmt.step()) {
            if (nb_stmt.columnText(0)) |nb| {
                is_credit_normal = std.mem.eql(u8, nb, "credit");
            }
        }
    }

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindText(1, cutoff_30);
    try stmt.bindText(2, cutoff_60);
    try stmt.bindText(3, cutoff_30);
    try stmt.bindText(4, cutoff_90);
    try stmt.bindText(5, cutoff_60);
    try stmt.bindText(6, cutoff_90);
    try stmt.bindInt(7, book_id);
    try stmt.bindText(8, as_of_date);
    if (group_id) |gf| {
        try stmt.bindInt(9, gf);
        try stmt.bindInt(10, gf);
    } else {
        try stmt.bindNull(9);
        try stmt.bindNull(10);
    }
    try stmt.bindInt(11, limit);
    try stmt.bindInt(12, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "counterparty_id,counterparty_number,counterparty_name,current_0_30,past_due_31_60,past_due_61_90,past_due_90_plus,total\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                const sign: i64 = if (is_credit_normal) -1 else 1;
                const current_0_30 = std.math.mul(i64, stmt.columnInt64(3), sign) catch return error.AmountOverflow;
                const past_due_31_60 = std.math.mul(i64, stmt.columnInt64(4), sign) catch return error.AmountOverflow;
                const past_due_61_90 = std.math.mul(i64, stmt.columnInt64(5), sign) catch return error.AmountOverflow;
                const past_due_90_plus = std.math.mul(i64, stmt.columnInt64(6), sign) catch return error.AmountOverflow;
                const total_amount = std.math.mul(i64, stmt.columnInt64(7), sign) catch return error.AmountOverflow;
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},{d}\n", .{
                    current_0_30, past_due_31_60, past_due_61_90, past_due_90_plus, total_amount,
                }) catch return error.BufferTooSmall;
                pos += nums.len;
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
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"counterparty_id\":{d},\"counterparty_number\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"counterparty_name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const sign: i64 = if (is_credit_normal) -1 else 1;
                const current_0_30 = std.math.mul(i64, stmt.columnInt64(3), sign) catch return error.AmountOverflow;
                const past_due_31_60 = std.math.mul(i64, stmt.columnInt64(4), sign) catch return error.AmountOverflow;
                const past_due_61_90 = std.math.mul(i64, stmt.columnInt64(5), sign) catch return error.AmountOverflow;
                const past_due_90_plus = std.math.mul(i64, stmt.columnInt64(6), sign) catch return error.AmountOverflow;
                const total_amount = std.math.mul(i64, stmt.columnInt64(7), sign) catch return error.AmountOverflow;
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"current_0_30\":{d},\"past_due_31_60\":{d},\"past_due_61_90\":{d},\"past_due_90_plus\":{d},\"total\":{d}}}", .{
                    current_0_30, past_due_31_60, past_due_61_90, past_due_90_plus, total_amount,
                }) catch return error.BufferTooSmall;
                pos += j3.len;
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}
