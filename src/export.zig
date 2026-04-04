const std = @import("std");
const report_mod = @import("report.zig");
const classification_mod = @import("classification.zig");
const db_mod = @import("db.zig");

pub const ExportFormat = enum { csv, json };

pub fn csvField(dest: []u8, src: []const u8) !usize {
    var needs_quoting = false;
    for (src) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r') {
            needs_quoting = true;
            break;
        }
    }
    if (!needs_quoting) {
        if (src.len > dest.len) return error.InvalidInput;
        @memcpy(dest[0..src.len], src);
        return src.len;
    }
    var pos: usize = 0;
    if (pos >= dest.len) return error.InvalidInput;
    dest[pos] = '"';
    pos += 1;
    for (src) |c| {
        if (c == '"') {
            if (pos + 2 > dest.len) return error.InvalidInput;
            dest[pos] = '"';
            dest[pos + 1] = '"';
            pos += 2;
        } else {
            if (pos >= dest.len) return error.InvalidInput;
            dest[pos] = c;
            pos += 1;
        }
    }
    if (pos >= dest.len) return error.InvalidInput;
    dest[pos] = '"';
    pos += 1;
    return pos;
}

pub fn jsonString(dest: []u8, src: []const u8) !usize {
    var pos: usize = 0;
    for (src) |c| {
        switch (c) {
            '"' => {
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = '"';
                pos += 2;
            },
            '\\' => {
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = '\\';
                pos += 2;
            },
            '\n' => {
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = 'n';
                pos += 2;
            },
            '\t' => {
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = 't';
                pos += 2;
            },
            '\r' => {
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = 'r';
                pos += 2;
            },
            0x08 => { // backspace
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = 'b';
                pos += 2;
            },
            0x0C => { // form feed
                if (pos + 2 > dest.len) return error.InvalidInput;
                dest[pos] = '\\';
                dest[pos + 1] = 'f';
                pos += 2;
            },
            0x00...0x07, 0x0B, 0x0E...0x1F => { // other control chars
                if (pos + 6 > dest.len) return error.InvalidInput;
                const hex = "0123456789abcdef";
                dest[pos] = '\\';
                dest[pos + 1] = 'u';
                dest[pos + 2] = '0';
                dest[pos + 3] = '0';
                dest[pos + 4] = hex[c >> 4];
                dest[pos + 5] = hex[c & 0x0F];
                pos += 6;
            },
            else => {
                if (pos >= dest.len) return error.InvalidInput;
                dest[pos] = c;
                pos += 1;
            },
        }
    }
    return pos;
}

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

        pos += try csvField(buf[pos..], acct_num);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], acct_name);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], acct_type);
        const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d}\n", .{
            row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
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
    if (pos + mid.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + mid.len], mid);
    pos += mid.len;

    const tc = std.fmt.bufPrint(buf[pos..], "{d}", .{result.total_credits}) catch return error.InvalidInput;
    pos += tc.len;

    const arr_open = ",\"rows\":[";
    if (pos + arr_open.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + arr_open.len], arr_open);
    pos += arr_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
        }
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        const p1 = "{\"account\":\"";
        if (pos + p1.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p1.len], p1);
        pos += p1.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p2 = "\",\"name\":\"";
        if (pos + p2.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p3 = "\",\"type\":\"";
        if (pos + p3.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], acct_type);
        const nums = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d}}}", .{
            row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── LedgerResult Exports ───────────────────────────────────────

pub fn ledgerResultToCsv(result: *report_mod.LedgerResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const header = "posting_date,document_number,description,account_number,account_name,debit_amount,credit_amount,running_balance\n";
    if (pos + header.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    for (result.rows) |row| {
        pos += try csvField(buf[pos..], row.posting_date[0..row.posting_date_len]);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.document_number[0..row.document_number_len]);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.description[0..row.description_len]);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.account_number[0..row.account_number_len]);
        if (pos >= buf.len) return error.InvalidInput;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.account_name[0..row.account_name_len]);
        const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{
            row.debit_amount, row.credit_amount, row.running_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
    }

    return buf[0..pos];
}

pub fn ledgerResultToJson(result: *report_mod.LedgerResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const open = std.fmt.bufPrint(buf[pos..], "{{\"opening_balance\":{d},\"closing_balance\":{d},\"total_debits\":{d},\"total_credits\":{d},\"rows\":[", .{
        result.opening_balance, result.closing_balance, result.total_debits, result.total_credits,
    }) catch return error.InvalidInput;
    pos += open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
        }
        const p1 = "{\"posting_date\":\"";
        if (pos + p1.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p1.len], p1);
        pos += p1.len;
        pos += try jsonString(buf[pos..], row.posting_date[0..row.posting_date_len]);
        const p2 = "\",\"document_number\":\"";
        if (pos + p2.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], row.document_number[0..row.document_number_len]);
        const p3 = "\",\"description\":\"";
        if (pos + p3.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], row.description[0..row.description_len]);
        const p4 = "\",\"account_number\":\"";
        if (pos + p4.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p4.len], p4);
        pos += p4.len;
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        const p5 = "\",\"account_name\":\"";
        if (pos + p5.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p5.len], p5);
        pos += p5.len;
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        const nums = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"running_balance\":{d}}}", .{
            row.debit_amount, row.credit_amount, row.running_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── ClassifiedResult Exports ───────────────────────────────────

pub fn classifiedResultToCsv(result: *classification_mod.ClassifiedResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const header = "node_type,depth,label,account_id,debit_balance,credit_balance\n";
    if (pos + header.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    for (result.rows) |row| {
        pos += try csvField(buf[pos..], row.node_type[0..row.node_type_len]);
        const mid1 = std.fmt.bufPrint(buf[pos..], ",{d},", .{row.depth}) catch return error.InvalidInput;
        pos += mid1.len;
        pos += try csvField(buf[pos..], row.label[0..row.label_len]);
        const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{
            row.account_id, row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
    }

    return buf[0..pos];
}

pub fn classifiedResultToJson(result: *classification_mod.ClassifiedResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const open = std.fmt.bufPrint(buf[pos..], "{{\"total_debits\":{d},\"total_credits\":{d},\"unclassified_debits\":{d},\"unclassified_credits\":{d},\"rows\":[", .{
        result.total_debits, result.total_credits, result.unclassified_debits, result.unclassified_credits,
    }) catch return error.InvalidInput;
    pos += open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
        }
        const p1 = "{\"node_type\":\"";
        if (pos + p1.len > buf.len) return error.InvalidInput;
        @memcpy(buf[pos .. pos + p1.len], p1);
        pos += p1.len;
        pos += try jsonString(buf[pos..], row.node_type[0..row.node_type_len]);
        const mid1 = std.fmt.bufPrint(buf[pos..], "\",\"depth\":{d},\"label\":\"", .{row.depth}) catch return error.InvalidInput;
        pos += mid1.len;
        pos += try jsonString(buf[pos..], row.label[0..row.label_len]);
        const nums = std.fmt.bufPrint(buf[pos..], "\",\"account_id\":{d},\"debit\":{d},\"credit\":{d}}}", .{
            row.account_id, row.debit_balance, row.credit_balance,
        }) catch return error.InvalidInput;
        pos += nums.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.InvalidInput;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── Database-Querying Exports ──────────────────────────────────

pub fn exportChartOfAccounts(database: db_mod.Database, book_id: i64, buf: []u8, format: ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT number, name, account_type, normal_balance, is_contra, status
        \\FROM ledger_accounts WHERE book_id = ? ORDER BY number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;

    switch (format) {
        .csv => {
            const header = "number,name,account_type,normal_balance,is_contra,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const number = stmt.columnText(0) orelse "";
                const name = stmt.columnText(1) orelse "";
                const acct_type = stmt.columnText(2) orelse "";
                const normal_bal = stmt.columnText(3) orelse "";
                const is_contra = stmt.columnInt(4);
                const status = stmt.columnText(5) orelse "";

                pos += try csvField(buf[pos..], number);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], name);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], acct_type);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], normal_bal);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.InvalidInput;
                pos += nums.len;
                pos += try csvField(buf[pos..], status);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"accounts\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
            if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const number = stmt.columnText(0) orelse "";
                const name = stmt.columnText(1) orelse "";
                const acct_type = stmt.columnText(2) orelse "";
                const normal_bal = stmt.columnText(3) orelse "";
                const is_contra = stmt.columnInt(4);
                const status = stmt.columnText(5) orelse "";

                const j1 = "{\"number\":\"";
                if (pos + j1.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try jsonString(buf[pos..], number);
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try jsonString(buf[pos..], name);
                const j3 = "\",\"account_type\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], acct_type);
                const j4 = "\",\"normal_balance\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], normal_bal);
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{d},\"status\":\"", .{is_contra}) catch return error.InvalidInput;
                pos += j5.len;
                pos += try jsonString(buf[pos..], status);
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

pub fn exportJournalEntries(database: db_mod.Database, book_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8, format: ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT e.document_number, e.transaction_date, e.posting_date,
        \\  e.description, e.status,
        \\  el.line_number, a.number, a.name,
        \\  el.debit_amount, el.credit_amount,
        \\  el.transaction_currency, el.fx_rate
        \\FROM ledger_entries e
        \\JOIN ledger_entry_lines el ON el.entry_id = e.id
        \\JOIN ledger_accounts a ON a.id = el.account_id
        \\WHERE e.book_id = ? AND e.status != 'draft'
        \\  AND e.posting_date BETWEEN ? AND ?
        \\ORDER BY e.posting_date, e.document_number, el.line_number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);

    var pos: usize = 0;

    switch (format) {
        .csv => {
            const header = "document_number,transaction_date,posting_date,description,status,line_number,account_number,account_name,debit_amount,credit_amount,currency,fx_rate\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const doc = stmt.columnText(0) orelse "";
                const txn_date = stmt.columnText(1) orelse "";
                const post_date = stmt.columnText(2) orelse "";
                const desc = stmt.columnText(3) orelse "";
                const status = stmt.columnText(4) orelse "";
                const line_num = stmt.columnInt(5);
                const acct_num = stmt.columnText(6) orelse "";
                const acct_name = stmt.columnText(7) orelse "";
                const debit = stmt.columnInt64(8);
                const credit = stmt.columnInt64(9);
                const currency = stmt.columnText(10) orelse "";
                const fx_rate = stmt.columnInt64(11);

                pos += try csvField(buf[pos..], doc);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], txn_date);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], post_date);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], desc);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], status);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},", .{line_num}) catch return error.InvalidInput;
                pos += nums.len;
                pos += try csvField(buf[pos..], acct_num);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], acct_name);
                const nums2 = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ debit, credit }) catch return error.InvalidInput;
                pos += nums2.len;
                pos += try csvField(buf[pos..], currency);
                const fx = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{fx_rate}) catch return error.InvalidInput;
                pos += fx.len;
            }
        },
        .json => {
            const open = "{\"entries\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var prev_doc: [100]u8 = undefined;
            var prev_doc_len: usize = 0;
            var entry_open = false;

            while (try stmt.step()) {
                const doc = stmt.columnText(0) orelse "";
                const txn_date = stmt.columnText(1) orelse "";
                const post_date = stmt.columnText(2) orelse "";
                const desc = stmt.columnText(3) orelse "";
                const status = stmt.columnText(4) orelse "";
                const line_num = stmt.columnInt(5);
                const acct_num = stmt.columnText(6) orelse "";
                const acct_name = stmt.columnText(7) orelse "";
                const debit = stmt.columnInt64(8);
                const credit = stmt.columnInt64(9);
                const currency = stmt.columnText(10) orelse "";
                const fx_rate = stmt.columnInt64(11);

                const prev = prev_doc[0..prev_doc_len];
                const same_entry = std.mem.eql(u8, doc, prev);

                if (!same_entry) {
                    if (entry_open) {
                        const close_entry = "]},";
                        if (pos + close_entry.len > buf.len) return error.InvalidInput;
                        @memcpy(buf[pos .. pos + close_entry.len], close_entry);
                        pos += close_entry.len;
                    }

                    const h1 = "{\"document_number\":\"";
                    if (pos + h1.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h1.len], h1);
                    pos += h1.len;
                    pos += try jsonString(buf[pos..], doc);
                    const h2 = "\",\"transaction_date\":\"";
                    if (pos + h2.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h2.len], h2);
                    pos += h2.len;
                    pos += try jsonString(buf[pos..], txn_date);
                    const h3 = "\",\"posting_date\":\"";
                    if (pos + h3.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h3.len], h3);
                    pos += h3.len;
                    pos += try jsonString(buf[pos..], post_date);
                    const h4 = "\",\"description\":\"";
                    if (pos + h4.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h4.len], h4);
                    pos += h4.len;
                    pos += try jsonString(buf[pos..], desc);
                    const h5 = "\",\"status\":\"";
                    if (pos + h5.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h5.len], h5);
                    pos += h5.len;
                    pos += try jsonString(buf[pos..], status);
                    const h6 = "\",\"lines\":[";
                    if (pos + h6.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + h6.len], h6);
                    pos += h6.len;

                    const copy_len = @min(doc.len, prev_doc.len);
                    @memcpy(prev_doc[0..copy_len], doc[0..copy_len]);
                    prev_doc_len = copy_len;
                    entry_open = true;
                } else {
                    buf[pos] = ',';
                    pos += 1;
                }

                const l1 = std.fmt.bufPrint(buf[pos..], "{{\"line_number\":{d},\"account_number\":\"", .{line_num}) catch return error.InvalidInput;
                pos += l1.len;
                pos += try jsonString(buf[pos..], acct_num);
                const l2 = "\",\"account_name\":\"";
                if (pos + l2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + l2.len], l2);
                pos += l2.len;
                pos += try jsonString(buf[pos..], acct_name);
                const l3 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"currency\":\"", .{ debit, credit }) catch return error.InvalidInput;
                pos += l3.len;
                pos += try jsonString(buf[pos..], currency);
                const l4 = std.fmt.bufPrint(buf[pos..], "\",\"fx_rate\":{d}}}", .{fx_rate}) catch return error.InvalidInput;
                pos += l4.len;
            }

            if (entry_open) {
                const close_entry = "]}";
                if (pos + close_entry.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + close_entry.len], close_entry);
                pos += close_entry.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }

    return buf[0..pos];
}

pub fn exportAuditTrail(database: db_mod.Database, book_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8, format: ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, entity_type, entity_id, action, field_changed,
        \\  old_value, new_value, performed_by, performed_at
        \\FROM ledger_audit_log
        \\WHERE book_id = ? AND performed_at BETWEEN ? AND ?
        \\ORDER BY id;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);

    var pos: usize = 0;

    switch (format) {
        .csv => {
            const header = "id,entity_type,entity_id,action,field_changed,old_value,new_value,performed_by,performed_at\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const entity_type = stmt.columnText(1) orelse "";
                const entity_id = stmt.columnInt64(2);
                const action = stmt.columnText(3) orelse "";
                const field = stmt.columnText(4) orelse "";
                const old_val = stmt.columnText(5) orelse "";
                const new_val = stmt.columnText(6) orelse "";
                const performed_by = stmt.columnText(7) orelse "";
                const performed_at = stmt.columnText(8) orelse "";

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try csvField(buf[pos..], entity_type);
                const eid_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{entity_id}) catch return error.InvalidInput;
                pos += eid_s.len;
                pos += try csvField(buf[pos..], action);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], field);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], old_val);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], new_val);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], performed_by);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], performed_at);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"records\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
            if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const id = stmt.columnInt64(0);
                const entity_type = stmt.columnText(1) orelse "";
                const entity_id = stmt.columnInt64(2);
                const action = stmt.columnText(3) orelse "";
                const field = stmt.columnText(4) orelse "";
                const old_val = stmt.columnText(5) orelse "";
                const new_val = stmt.columnText(6) orelse "";
                const performed_by = stmt.columnText(7) orelse "";
                const performed_at = stmt.columnText(8) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"entity_type\":\"", .{id}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try jsonString(buf[pos..], entity_type);
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"entity_id\":{d},\"action\":\"", .{entity_id}) catch return error.InvalidInput;
                pos += j2.len;
                pos += try jsonString(buf[pos..], action);
                const j3 = "\",\"field_changed\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], field);
                const j4 = "\",\"old_value\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], old_val);
                const j5 = "\",\"new_value\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], new_val);
                const j6 = "\",\"performed_by\":\"";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try jsonString(buf[pos..], performed_by);
                const j7 = "\",\"performed_at\":\"";
                if (pos + j7.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try jsonString(buf[pos..], performed_at);
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

// ── Period, Subledger, Book Metadata Exports ───────────────────

pub fn exportPeriods(database: db_mod.Database, book_id: i64, buf: []u8, format: ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, name, period_number, year, start_date, end_date, period_type, status
        \\FROM ledger_periods WHERE book_id = ? ORDER BY year, period_number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;

    switch (format) {
        .csv => {
            const header = "id,name,period_number,year,start_date,end_date,period_type,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const period_number = stmt.columnInt(2);
                const year = stmt.columnInt(3);
                const start_date = stmt.columnText(4) orelse "";
                const end_date = stmt.columnText(5) orelse "";
                const period_type = stmt.columnText(6) orelse "";
                const status = stmt.columnText(7) orelse "";

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.InvalidInput;
                pos += id_s.len;
                pos += try csvField(buf[pos..], name);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ period_number, year }) catch return error.InvalidInput;
                pos += nums.len;
                pos += try csvField(buf[pos..], start_date);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], end_date);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], period_type);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], status);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"periods\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

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
                const period_number = stmt.columnInt(2);
                const year = stmt.columnInt(3);
                const start_date = stmt.columnText(4) orelse "";
                const end_date = stmt.columnText(5) orelse "";
                const period_type = stmt.columnText(6) orelse "";
                const status = stmt.columnText(7) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try jsonString(buf[pos..], name);
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ period_number, year }) catch return error.InvalidInput;
                pos += j2.len;
                pos += try jsonString(buf[pos..], start_date);
                const j3 = "\",\"end_date\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], end_date);
                const j4 = "\",\"period_type\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], period_type);
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], status);
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

pub fn exportSubledger(database: db_mod.Database, book_id: i64, buf: []u8, format: ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT sg.name, sg.type, a.number,
        \\  sa.number, sa.name, sa.type
        \\FROM ledger_subledger_accounts sa
        \\JOIN ledger_subledger_groups sg ON sg.id = sa.group_id
        \\JOIN ledger_accounts a ON a.id = sg.gl_account_id
        \\WHERE sa.book_id = ?
        \\ORDER BY sg.name, sa.number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);

    var pos: usize = 0;

    switch (format) {
        .csv => {
            const header = "group_name,group_type,control_account_number,counterparty_number,counterparty_name,counterparty_type\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const group_name = stmt.columnText(0) orelse "";
                const group_type = stmt.columnText(1) orelse "";
                const control_acct = stmt.columnText(2) orelse "";
                const cp_number = stmt.columnText(3) orelse "";
                const cp_name = stmt.columnText(4) orelse "";
                const cp_type = stmt.columnText(5) orelse "";

                pos += try csvField(buf[pos..], group_name);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], group_type);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], control_acct);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_number);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_name);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_type);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"subledger_accounts\":[";
            if (pos + open.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
            if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const group_name = stmt.columnText(0) orelse "";
                const group_type = stmt.columnText(1) orelse "";
                const control_acct = stmt.columnText(2) orelse "";
                const cp_number = stmt.columnText(3) orelse "";
                const cp_name = stmt.columnText(4) orelse "";
                const cp_type = stmt.columnText(5) orelse "";

                const j1 = "{\"group_name\":\"";
                if (pos + j1.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try jsonString(buf[pos..], group_name);
                const j2 = "\",\"group_type\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try jsonString(buf[pos..], group_type);
                const j3 = "\",\"control_account_number\":\"";
                if (pos + j3.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], control_acct);
                const j4 = "\",\"counterparty_number\":\"";
                if (pos + j4.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], cp_number);
                const j5 = "\",\"counterparty_name\":\"";
                if (pos + j5.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], cp_name);
                const j6 = "\",\"counterparty_type\":\"";
                if (pos + j6.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try jsonString(buf[pos..], cp_type);
                const j7 = "\"}";
                if (pos + j7.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }

    return buf[0..pos];
}

pub fn exportBookMetadata(database: db_mod.Database, book_id: i64, buf: []u8, format: ExportFormat) ![]u8 {
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
            pos += try csvField(buf[pos..], name);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = ',';
            pos += 1;
            pos += try csvField(buf[pos..], currency);
            const dec_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.InvalidInput;
            pos += dec_s.len;
            pos += try csvField(buf[pos..], status);
            if (pos >= buf.len) return error.InvalidInput;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.InvalidInput;
            pos += j1.len;
            pos += try jsonString(buf[pos..], name);
            const j2 = "\",\"base_currency\":\"";
            if (pos + j2.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try jsonString(buf[pos..], currency);
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.InvalidInput;
            pos += j3.len;
            pos += try jsonString(buf[pos..], status);
            const j4 = "\"}";
            if (pos + j4.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + j4.len], j4);
            pos += j4.len;
        },
    }

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
const subledger_mod = @import("subledger.zig");

fn setupAndPost() !struct { database: db.Database, result: *report_mod.ReportResult } {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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

// ── Setup Helpers for New Exports ──────────────────────────────

fn setupLedgerData() !struct { database: db.Database, gl_result: *report_mod.LedgerResult } {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const gl_result = try report_mod.generalLedger(database, 1, "2026-01-01", "2026-01-31");
    return .{ .database = database, .gl_result = gl_result };
}

fn setupClassifiedData() !struct { database: db.Database, result: *classification_mod.ClassifiedResult } {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 5_000_000_000_00, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const cid = try classification_mod.Classification.create(database, 1, "BS Layout", "balance_sheet", "admin");
    const assets_group = try classification_mod.ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(database, cid, 1, assets_group, 1, "admin");
    const liab_group = try classification_mod.ClassificationNode.addGroup(database, cid, "Liabilities", null, 2, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(database, cid, 2, liab_group, 1, "admin");

    const result = try classification_mod.classifiedReport(database, cid, "2026-01-31");
    return .{ .database = database, .result = result };
}

fn setupBookWithAccounts() !db.Database {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5100", "Purch Returns", .expense, true, "admin");
    return database;
}

fn setupFullBook() !db.Database {
    const database = try setupBookWithAccounts();
    errdefer database.close();
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // JE-001: Debit Cash (id=1), Credit Revenue (id=5)
    const eid1 = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 10_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 10_000_000_000_00, "PHP", money.FX_RATE_SCALE, 5, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    // JE-002: Debit COGS (id=6), Credit Cash (id=1)
    const eid2 = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 3_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 6, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 2, 0, 3_000_000_000_00, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try entry_mod.Entry.post(database, eid2, "admin");

    return database;
}

// ── LedgerResult CSV Tests ─────────────────────────────────────

test "ledgerResultToCsv: contains header row" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try ledgerResultToCsv(setup.gl_result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "posting_date,document_number,description,account_number,account_name,debit_amount,credit_amount,running_balance") != null);
}

test "ledgerResultToCsv: contains transaction data" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try ledgerResultToCsv(setup.gl_result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "2026-01-15") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "JE-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);
}

test "ledgerResultToCsv: correct line count" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try ledgerResultToCsv(setup.gl_result, &buf);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1 + @as(u32, @intCast(setup.gl_result.rows.len))), line_count);
}

test "ledgerResultToCsv: empty result produces header only" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    const result = try report_mod.generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const csv = try ledgerResultToCsv(result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "posting_date") != null);
    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "ledgerResultToCsv: buffer too small returns error" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var small_buf: [10]u8 = undefined;
    const result = ledgerResultToCsv(setup.gl_result, &small_buf);
    try std.testing.expectError(error.InvalidInput, result);
}

// ── LedgerResult JSON Tests ────────────────────────────────────

test "ledgerResultToJson: valid structure" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var buf: [8192]u8 = undefined;
    const json = try ledgerResultToJson(setup.gl_result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"closing_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_debits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_credits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "ledgerResultToJson: contains transaction data" {
    var setup = try setupLedgerData();
    defer setup.database.close();
    defer setup.gl_result.deinit();

    var buf: [8192]u8 = undefined;
    const json = try ledgerResultToJson(setup.gl_result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"posting_date\":\"2026-01-15\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_number\":\"JE-001\"") != null);
}

test "ledgerResultToJson: empty result produces empty rows" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    const result = try report_mod.generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const json = try ledgerResultToJson(result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

// ── ClassifiedResult CSV Tests ─────────────────────────────────

test "classifiedResultToCsv: contains header row" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try classifiedResultToCsv(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "node_type,depth,label,account_id,debit_balance,credit_balance") != null);
}

test "classifiedResultToCsv: contains classified data" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try classifiedResultToCsv(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, csv, "group") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Assets") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Liabilities") != null);
}

test "classifiedResultToCsv: correct line count" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [8192]u8 = undefined;
    const csv = try classifiedResultToCsv(setup.result, &buf);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1 + @as(u32, @intCast(setup.result.rows.len))), line_count);
}

test "classifiedResultToCsv: empty result produces header only" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    const cid = try classification_mod.Classification.create(database, 1, "Empty BS", "balance_sheet", "admin");
    const result = try classification_mod.classifiedReport(database, cid, "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const csv = try classifiedResultToCsv(result, &buf);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "classifiedResultToCsv: buffer too small returns error" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var small_buf: [10]u8 = undefined;
    const result = classifiedResultToCsv(setup.result, &small_buf);
    try std.testing.expectError(error.InvalidInput, result);
}

// ── ClassifiedResult JSON Tests ────────────────────────────────

test "classifiedResultToJson: valid structure" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [8192]u8 = undefined;
    const json = try classifiedResultToJson(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_debits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total_credits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"unclassified_debits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"unclassified_credits\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "classifiedResultToJson: contains node data" {
    var setup = try setupClassifiedData();
    defer setup.database.close();
    defer setup.result.deinit();

    var buf: [8192]u8 = undefined;
    const json = try classifiedResultToJson(setup.result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"node_type\":\"group\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"label\":\"Assets\"") != null);
}

test "classifiedResultToJson: empty result produces empty rows" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    const cid = try classification_mod.Classification.create(database, 1, "Empty BS", "balance_sheet", "admin");
    const result = try classification_mod.classifiedReport(database, cid, "2026-01-31");
    defer result.deinit();

    var buf: [4096]u8 = undefined;
    const json = try classifiedResultToJson(result, &buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

// ── Chart of Accounts Export Tests ─────────────────────────────

test "exportChartOfAccounts: CSV contains header" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportChartOfAccounts(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "number,name,account_type,normal_balance,is_contra,status") != null);
}

test "exportChartOfAccounts: CSV contains all accounts" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportChartOfAccounts(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Cash") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "2000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "AP") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "4000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Revenue") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "5100") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Purch Returns") != null);
}

test "exportChartOfAccounts: CSV correct line count" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportChartOfAccounts(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + 7 accounts
    try std.testing.expectEqual(@as(u32, 8), line_count);
}

test "exportChartOfAccounts: CSV contra account flag" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportChartOfAccounts(database, 1, &buf, .csv);

    // Purch Returns (5100) is contra, should show 1
    // Find the line with 5100 and verify it has ,1, (is_contra=1)
    var lines = std.mem.splitScalar(u8, csv, '\n');
    var found_contra = false;
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "5100") != null) {
            try std.testing.expect(std.mem.indexOf(u8, line, ",1,") != null);
            found_contra = true;
        }
    }
    try std.testing.expect(found_contra);
}

test "exportChartOfAccounts: JSON structure" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const json = try exportChartOfAccounts(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"accounts\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Cash\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_type\":\"asset\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportChartOfAccounts: empty book produces header only (CSV)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportChartOfAccounts(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "exportChartOfAccounts: empty book produces empty array (JSON)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try exportChartOfAccounts(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"accounts\":[]") != null);
}

test "exportChartOfAccounts: buffer too small returns error" {
    const database = try setupBookWithAccounts();
    defer database.close();

    var small_buf: [10]u8 = undefined;
    const result = exportChartOfAccounts(database, 1, &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}

// ── Journal Entries Export Tests ────────────────────────────────

test "exportJournalEntries: CSV contains header" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "document_number,transaction_date,posting_date,description,status,line_number,account_number,account_name,debit_amount,credit_amount,currency,fx_rate") != null);
}

test "exportJournalEntries: CSV contains entry data" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "JE-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "JE-002") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Cash") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Revenue") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "COGS") != null);
}

test "exportJournalEntries: CSV correct line count" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + 4 lines (2 entries x 2 lines each)
    try std.testing.expectEqual(@as(u32, 5), line_count);
}

test "exportJournalEntries: date range filtering" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-15", &buf, .csv);

    // Only JE-001 (posted 2026-01-10) should be included
    try std.testing.expect(std.mem.indexOf(u8, csv, "JE-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "JE-002") == null);
}

test "exportJournalEntries: empty range produces header only" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2025-01-01", "2025-01-31", &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "exportJournalEntries: JSON structure with nested lines" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const json = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"entries\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_number\":\"JE-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"lines\":[") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportJournalEntries: JSON empty range produces empty array" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const json = try exportJournalEntries(database, 1, "2025-01-01", "2025-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"entries\":[]") != null);
}

test "exportJournalEntries: excludes draft entries" {
    const database = try setupFullBook();
    defer database.close();

    // Create a draft (not posted) — should NOT appear in export
    _ = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-25", "2026-01-25", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;
    const csv = try exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "DRAFT-001") == null);
}

test "exportJournalEntries: buffer too small returns error" {
    const database = try setupFullBook();
    defer database.close();

    var small_buf: [10]u8 = undefined;
    const result = exportJournalEntries(database, 1, "2026-01-01", "2026-01-31", &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}

// ── Audit Trail Export Tests ───────────────────────────────────

test "exportAuditTrail: CSV contains header" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [32768]u8 = undefined;
    const csv = try exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,entity_type,entity_id,action,field_changed,old_value,new_value,performed_by,performed_at") != null);
}

test "exportAuditTrail: CSV contains audit records" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [32768]u8 = undefined;
    const csv = try exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &buf, .csv);

    // Should have audit records for: book create, account creates, period create,
    // entry creates, line adds, posts
    try std.testing.expect(std.mem.indexOf(u8, csv, "entry") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "post") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "admin") != null);
}

test "exportAuditTrail: CSV has multiple records" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [32768]u8 = undefined;
    const csv = try exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // Header + many audit records (book + 7 accounts + period + 2 entries + 4 lines + 2 posts = 16+)
    try std.testing.expect(line_count > 10);
}

test "exportAuditTrail: date range filtering" {
    const database = try setupFullBook();
    defer database.close();

    // Narrow range that excludes everything
    var buf: [32768]u8 = undefined;
    const csv = try exportAuditTrail(database, 1, "2025-01-01", "2025-01-31", &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // Only header
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "exportAuditTrail: JSON structure" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [32768]u8 = undefined;
    const json = try exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"records\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"entity_type\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"performed_by\":\"admin\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportAuditTrail: JSON empty range produces empty array" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [32768]u8 = undefined;
    const json = try exportAuditTrail(database, 1, "2025-01-01", "2025-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"records\":[]") != null);
}

test "exportAuditTrail: buffer too small returns error" {
    const database = try setupFullBook();
    defer database.close();

    var small_buf: [10]u8 = undefined;
    const result = exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}

test "exportAuditTrail: captures void reason" {
    const database = try setupFullBook();
    defer database.close();

    try entry_mod.Entry.voidEntry(database, 1, "Duplicate entry", "admin");

    var buf: [65536]u8 = undefined;
    const csv = try exportAuditTrail(database, 1, "2026-01-01", "2026-12-31", &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "void") != null);
}

// ── Subledger Setup Helper ─────────────────────────────────────

fn setupSubledgerBook() !db.Database {
    const database = try db.Database.open(":memory:");
    errdefer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    // Cash=1, AR=2, AP=3, Capital=4
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2100", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");

    // AR subledger group -> control account AR (id=2)
    const ar_group = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    // AP subledger group -> control account AP (id=3)
    const ap_group = try subledger_mod.SubledgerGroup.create(database, 1, "Suppliers", "supplier", 2, 3, null, null, "admin");

    // Counterparties
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C-001", "Acme Corp", "customer", ar_group, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "S-001", "Widget Inc", "supplier", ap_group, "admin");

    return database;
}

// ── Period List Export Tests ────────────────────────────────────

test "exportPeriods: CSV contains header" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,period_number,year,start_date,end_date,period_type,status") != null);
}

test "exportPeriods: CSV contains period data" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "Jan 2026") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "2026-01-01") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "2026-01-31") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "regular") != null);
}

test "exportPeriods: CSV correct line count" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + 1 period
    try std.testing.expectEqual(@as(u32, 2), line_count);
}

test "exportPeriods: multiple periods" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Mar 2026", 3, 2026, "2026-03-01", "2026-03-31", "regular", "admin");

    var buf: [8192]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 4), line_count);
}

test "exportPeriods: JSON structure" {
    const database = try setupFullBook();
    defer database.close();

    var buf: [8192]u8 = undefined;
    const json = try exportPeriods(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"periods\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan 2026\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"start_date\":\"2026-01-01\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportPeriods: empty book produces header only (CSV)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "exportPeriods: empty book produces empty array (JSON)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try exportPeriods(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"periods\":[]") != null);
}

test "exportPeriods: buffer too small returns error" {
    const database = try setupFullBook();
    defer database.close();

    var small_buf: [10]u8 = undefined;
    const result = exportPeriods(database, 1, &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}

test "exportPeriods: shows period status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try exportPeriods(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "soft_closed") != null);
}

// ── Subledger Export Tests ─────────────────────────────────────

test "exportSubledger: CSV contains header" {
    const database = try setupSubledgerBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportSubledger(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "group_name,group_type,control_account_number,counterparty_number,counterparty_name,counterparty_type") != null);
}

test "exportSubledger: CSV contains counterparty data" {
    const database = try setupSubledgerBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportSubledger(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Widget Inc") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Customers") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Suppliers") != null);
}

test "exportSubledger: CSV correct line count" {
    const database = try setupSubledgerBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const csv = try exportSubledger(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + 2 counterparties (Acme, Widget)
    try std.testing.expectEqual(@as(u32, 3), line_count);
}

test "exportSubledger: JSON structure" {
    const database = try setupSubledgerBook();
    defer database.close();

    var buf: [16384]u8 = undefined;
    const json = try exportSubledger(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"subledger_accounts\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_name\":\"Acme Corp\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"group_name\":\"Customers\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportSubledger: empty book produces header only (CSV)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportSubledger(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), line_count);
}

test "exportSubledger: empty book produces empty array (JSON)" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Empty", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try exportSubledger(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"subledger_accounts\":[]") != null);
}

test "exportSubledger: buffer too small returns error" {
    const database = try setupSubledgerBook();
    defer database.close();

    var small_buf: [10]u8 = undefined;
    const result = exportSubledger(database, 1, &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}

// ── Book Metadata Export Tests ─────────────────────────────────

test "exportBookMetadata: CSV contains header" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "My Ledger", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportBookMetadata(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,base_currency,decimal_places,status") != null);
}

test "exportBookMetadata: CSV contains book data" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "My Ledger", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportBookMetadata(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "My Ledger") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "PHP") != null);
}

test "exportBookMetadata: CSV correct line count" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "My Ledger", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try exportBookMetadata(database, 1, &buf, .csv);

    var line_count: u32 = 0;
    for (csv) |ch| {
        if (ch == '\n') line_count += 1;
    }
    // 1 header + 1 book row
    try std.testing.expectEqual(@as(u32, 2), line_count);
}

test "exportBookMetadata: JSON structure" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "My Ledger", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try exportBookMetadata(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"My Ledger\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"base_currency\":\"PHP\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"decimal_places\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"active\"") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "exportBookMetadata: nonexistent book returns error" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    const result = exportBookMetadata(database, 999, &buf, .csv);
    try std.testing.expectError(error.NotFound, result);
}

test "exportBookMetadata: buffer too small returns error" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "My Ledger", "PHP", 2, "admin");

    var small_buf: [10]u8 = undefined;
    const result = exportBookMetadata(database, 1, &small_buf, .csv);
    try std.testing.expectError(error.InvalidInput, result);
}
