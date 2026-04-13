const std = @import("std");
const db_mod = @import("db.zig");
const export_mod = @import("export.zig");
const common = @import("query_common.zig");

pub const SortOrder = common.SortOrder;
const clampLimit = common.clampLimit;
const clampOffset = common.clampOffset;
const writeJsonMeta = common.writeJsonMeta;
const writeCsvMeta = common.writeCsvMeta;

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
        if (status_filter) |sf| {
            try stmt.bindText(2, sf);
            try stmt.bindText(3, sf);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
        if (start_date) |sd| {
            try stmt.bindText(4, sd);
            try stmt.bindText(5, sd);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
        if (end_date) |ed| {
            try stmt.bindText(6, ed);
            try stmt.bindText(7, ed);
        } else {
            try stmt.bindNull(6);
            try stmt.bindNull(7);
        }
        if (doc_search) |ds| {
            try stmt.bindText(8, ds);
            try stmt.bindText(9, ds);
        } else {
            try stmt.bindNull(8);
            try stmt.bindNull(9);
        }
        _ = try stmt.step();
        total = stmt.columnInt64(0);
    }

    const order_sql: [*:0]const u8 = if (order == .asc)
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id, metadata
        \\FROM ledger_entries WHERE book_id = ?
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR posting_date >= ?) AND (? IS NULL OR posting_date <= ?)
        \\  AND (? IS NULL OR document_number LIKE '%' || ? || '%')
        \\ORDER BY posting_date ASC, document_number ASC LIMIT ? OFFSET ?;
    else
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id, metadata
        \\FROM ledger_entries WHERE book_id = ?
        \\  AND (? IS NULL OR status = ?)
        \\  AND (? IS NULL OR posting_date >= ?) AND (? IS NULL OR posting_date <= ?)
        \\  AND (? IS NULL OR document_number LIKE '%' || ? || '%')
        \\ORDER BY posting_date DESC, document_number DESC LIMIT ? OFFSET ?;
    ;

    var stmt = try database.prepare(order_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (status_filter) |sf| {
        try stmt.bindText(2, sf);
        try stmt.bindText(3, sf);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    if (start_date) |sd| {
        try stmt.bindText(4, sd);
        try stmt.bindText(5, sd);
    } else {
        try stmt.bindNull(4);
        try stmt.bindNull(5);
    }
    if (end_date) |ed| {
        try stmt.bindText(6, ed);
        try stmt.bindText(7, ed);
    } else {
        try stmt.bindNull(6);
        try stmt.bindNull(7);
    }
    if (doc_search) |ds| {
        try stmt.bindText(8, ds);
        try stmt.bindText(9, ds);
    } else {
        try stmt.bindNull(8);
        try stmt.bindNull(9);
    }
    try stmt.bindInt(10, limit);
    try stmt.bindInt(11, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,document_number,transaction_date,posting_date,description,status,period_id,metadata\n";
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
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                const pid = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt64(6)}) catch return error.BufferTooSmall;
                pos += pid.len;
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
                const le_meta = stmt.columnText(7);
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"document_number\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = "\",\"transaction_date\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j3 = "\",\"posting_date\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j4 = "\",\"description\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j6 = std.fmt.bufPrint(buf[pos..], "\",\"period_id\":{d}", .{stmt.columnInt64(6)}) catch return error.BufferTooSmall;
                pos += j6.len;
                if (le_meta) |mv| {
                    const j7 = ",\"metadata\":\"";
                    if (pos + j7.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + j7.len], j7);
                    pos += j7.len;
                    pos += try export_mod.jsonString(buf[pos..], mv);
                    const j8 = "\"}";
                    if (pos + j8.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + j8.len], j8);
                    pos += j8.len;
                } else {
                    const j7 = ",\"metadata\":null}";
                    if (pos + j7.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + j7.len], j7);
                    pos += j7.len;
                }
            }
            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
        if (entity_type_filter) |et| {
            try stmt.bindText(2, et);
            try stmt.bindText(3, et);
        } else {
            try stmt.bindNull(2);
            try stmt.bindNull(3);
        }
        if (action_filter) |af| {
            try stmt.bindText(4, af);
            try stmt.bindText(5, af);
        } else {
            try stmt.bindNull(4);
            try stmt.bindNull(5);
        }
        if (start_date) |sd| {
            try stmt.bindText(6, sd);
            try stmt.bindText(7, sd);
        } else {
            try stmt.bindNull(6);
            try stmt.bindNull(7);
        }
        if (end_date) |ed| {
            try stmt.bindText(8, ed);
            try stmt.bindText(9, ed);
        } else {
            try stmt.bindNull(8);
            try stmt.bindNull(9);
        }
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
    if (entity_type_filter) |et| {
        try stmt.bindText(2, et);
        try stmt.bindText(3, et);
    } else {
        try stmt.bindNull(2);
        try stmt.bindNull(3);
    }
    if (action_filter) |af| {
        try stmt.bindText(4, af);
        try stmt.bindText(5, af);
    } else {
        try stmt.bindNull(4);
        try stmt.bindNull(5);
    }
    if (start_date) |sd| {
        try stmt.bindText(6, sd);
        try stmt.bindText(7, sd);
    } else {
        try stmt.bindNull(6);
        try stmt.bindNull(7);
    }
    if (end_date) |ed| {
        try stmt.bindText(8, ed);
        try stmt.bindText(9, ed);
    } else {
        try stmt.bindNull(8);
        try stmt.bindNull(9);
    }
    try stmt.bindInt(10, limit);
    try stmt.bindInt(11, offset);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            pos += try writeCsvMeta(buf[pos..], total, limit, offset);
            const header = "id,entity_type,entity_id,action,field_changed,old_value,new_value,performed_by,performed_at\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(1) orelse "");
                const eid_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt64(2)}) catch return error.BufferTooSmall;
                pos += eid_s.len;
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
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(7) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(8) orelse "");
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
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"entity_type\":\"", .{stmt.columnInt64(0)}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(1) orelse "");
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"entity_id\":{d},\"action\":\"", .{stmt.columnInt64(2)}) catch return error.BufferTooSmall;
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j3 = "\",\"field_changed\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(4) orelse "");
                const j4 = "\",\"old_value\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
                const j5 = "\",\"new_value\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(6) orelse "");
                const j6 = "\",\"performed_by\":\"";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(7) orelse "");
                const j7 = "\",\"performed_at\":\"";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(8) orelse "");
                const j8 = "\"}";
                if (pos + j8.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j8.len], j8);
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

// ── getEntry ───────────────────────────────────────────────────

pub fn getEntry(database: db_mod.Database, entry_id: i64, book_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, document_number, transaction_date, posting_date, description, status, period_id, metadata
        \\FROM ledger_entries WHERE id = ? AND book_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, entry_id);
    try stmt.bindInt(2, book_id);
    const has_row = try stmt.step();
    if (!has_row) return error.NotFound;

    const id = stmt.columnInt64(0);
    const doc = stmt.columnText(1) orelse "";
    const txn_date = stmt.columnText(2) orelse "";
    const post_date = stmt.columnText(3) orelse "";
    const desc = stmt.columnText(4) orelse "";
    const status = stmt.columnText(5) orelse "";
    const period_id = stmt.columnInt64(6);
    const metadata_val = stmt.columnText(7);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,document_number,transaction_date,posting_date,description,status,period_id,metadata\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;
            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try export_mod.csvField(buf[pos..], doc);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], txn_date);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], post_date);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], desc);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try export_mod.csvField(buf[pos..], status);
            const pid = std.fmt.bufPrint(buf[pos..], ",{d},", .{period_id}) catch return error.BufferTooSmall;
            pos += pid.len;
            pos += try export_mod.csvField(buf[pos..], metadata_val orelse "");
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"document_number\":\"", .{id}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try export_mod.jsonString(buf[pos..], doc);
            const j2 = "\",\"transaction_date\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try export_mod.jsonString(buf[pos..], txn_date);
            const j3 = "\",\"posting_date\":\"";
            if (pos + j3.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j3.len], j3);
            pos += j3.len;
            pos += try export_mod.jsonString(buf[pos..], post_date);
            const j4 = "\",\"description\":\"";
            if (pos + j4.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
            pos += try export_mod.jsonString(buf[pos..], desc);
            const j5 = "\",\"status\":\"";
            if (pos + j5.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j5.len], j5);
            pos += j5.len;
            pos += try export_mod.jsonString(buf[pos..], status);
            const j6 = std.fmt.bufPrint(buf[pos..], "\",\"period_id\":{d}", .{period_id}) catch return error.BufferTooSmall;
            pos += j6.len;
            if (metadata_val) |mv| {
                const j7 = ",\"metadata\":\"";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try export_mod.jsonString(buf[pos..], mv);
                const j8 = "\"}";
                if (pos + j8.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j8.len], j8);
                pos += j8.len;
            } else {
                const j7 = ",\"metadata\":null}";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
            }
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id_s = std.fmt.bufPrint(buf[pos..], "{d},{d},", .{ stmt.columnInt64(0), stmt.columnInt(1) }) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(2) orelse "");
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(3) orelse "");
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d},", .{
                    stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.BufferTooSmall;
                pos += nums.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(8) orelse "");
                const fx = std.fmt.bufPrint(buf[pos..], ",{d},", .{stmt.columnInt64(9)}) catch return error.BufferTooSmall;
                pos += fx.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(10) orelse "");
                const cp = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{stmt.columnInt64(11)}) catch return error.BufferTooSmall;
                pos += cp.len;
            }
        },
        .json => {
            const open = "{\"lines\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"line_number\":{d},\"account_number\":\"", .{ stmt.columnInt64(0), stmt.columnInt(1) }) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(2) orelse "");
                const j2 = "\",\"account_name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(3) orelse "");
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"base_debit\":{d},\"base_credit\":{d},\"currency\":\"", .{
                    stmt.columnInt64(4), stmt.columnInt64(5), stmt.columnInt64(6), stmt.columnInt64(7),
                }) catch return error.BufferTooSmall;
                pos += j3.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(8) orelse "");
                const j4 = std.fmt.bufPrint(buf[pos..], "\",\"fx_rate\":{d},\"description\":\"", .{stmt.columnInt64(9)}) catch return error.BufferTooSmall;
                pos += j4.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(10) orelse "");
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"counterparty_id\":{d}}}", .{stmt.columnInt64(11)}) catch return error.BufferTooSmall;
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
