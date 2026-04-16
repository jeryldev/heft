const std = @import("std");
const report_mod = @import("report.zig");
const classification_mod = @import("classification.zig");
const db_mod = @import("db.zig");

const money = @import("money.zig");

pub const ExportFormat = enum { csv, json };

fn fmtAmount(dest: []u8, amount: i64, dp: u8) !usize {
    const result = try money.formatDecimal(dest, amount, dp);
    return result.len;
}

fn appendAmountValue(buf: []u8, amount: i64, dp: u8, as_integer_minor_units: bool) !usize {
    if (as_integer_minor_units) {
        return (try std.fmt.bufPrint(buf, "{d}", .{amount})).len;
    }
    if (buf.len < 2) return error.BufferTooSmall;
    buf[0] = '"';
    const inner_len = try fmtAmount(buf[1..], amount, dp);
    if (1 + inner_len >= buf.len) return error.BufferTooSmall;
    buf[1 + inner_len] = '"';
    return inner_len + 2;
}

pub fn csvField(dest: []u8, src: []const u8) !usize {
    var needs_quoting = false;
    for (src) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r') {
            needs_quoting = true;
            break;
        }
    }
    if (!needs_quoting) {
        if (src.len > dest.len) return error.BufferTooSmall;
        @memcpy(dest[0..src.len], src);
        return src.len;
    }
    var pos: usize = 0;
    if (pos >= dest.len) return error.BufferTooSmall;
    dest[pos] = '"';
    pos += 1;
    for (src) |c| {
        if (c == '"') {
            if (pos + 2 > dest.len) return error.BufferTooSmall;
            dest[pos] = '"';
            dest[pos + 1] = '"';
            pos += 2;
        } else {
            if (pos >= dest.len) return error.BufferTooSmall;
            dest[pos] = c;
            pos += 1;
        }
    }
    if (pos >= dest.len) return error.BufferTooSmall;
    dest[pos] = '"';
    pos += 1;
    return pos;
}

pub fn jsonString(dest: []u8, src: []const u8) !usize {
    var pos: usize = 0;
    for (src) |c| {
        switch (c) {
            '"' => {
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = '"';
                pos += 2;
            },
            '\\' => {
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = '\\';
                pos += 2;
            },
            '\n' => {
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = 'n';
                pos += 2;
            },
            '\t' => {
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = 't';
                pos += 2;
            },
            '\r' => {
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = 'r';
                pos += 2;
            },
            0x08 => { // backspace
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = 'b';
                pos += 2;
            },
            0x0C => { // form feed
                if (pos + 2 > dest.len) return error.BufferTooSmall;
                dest[pos] = '\\';
                dest[pos + 1] = 'f';
                pos += 2;
            },
            0x00...0x07, 0x0B, 0x0E...0x1F => { // other control chars
                if (pos + 6 > dest.len) return error.BufferTooSmall;
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
                if (pos >= dest.len) return error.BufferTooSmall;
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
    if (pos + header.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    // Rows
    for (result.rows) |row| {
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        pos += try csvField(buf[pos..], acct_num);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], acct_name);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], acct_type);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.debit_balance, result.decimal_places);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.credit_balance, result.decimal_places);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '\n';
        pos += 1;
    }

    return buf[0..pos];
}

/// Export a ReportResult to JSON format in a caller-provided buffer.
pub fn reportToJson(result: *report_mod.ReportResult, buf: []u8) ![]u8 {
    return reportToJsonEx(result, "report", 0, buf, false);
}

pub fn reportToJsonEx(result: *report_mod.ReportResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;
    const dp = result.decimal_places;

    const open = "{\"packet_kind\":\"";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const mid0 = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, dp }) catch return error.BufferTooSmall;
    pos += mid0.len;
    const enc = if (as_integer_minor_units) "integer_minor_units" else "decimal_string";
    pos += try jsonString(buf[pos..], enc);
    const mid = "\"},\"totals\":{\"debits\":";
    if (pos + mid.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid.len], mid);
    pos += mid.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, dp, as_integer_minor_units);
    const mid2 = ",\"credits\":";
    if (pos + mid2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid2.len], mid2);
    pos += mid2.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, dp, as_integer_minor_units);
    const legacy = "},\"total_debits\":";
    if (pos + legacy.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy.len], legacy);
    pos += legacy.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, dp, as_integer_minor_units);
    const legacy2 = ",\"total_credits\":";
    if (pos + legacy2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy2.len], legacy2);
    pos += legacy2.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, dp, as_integer_minor_units);

    const arr_open = ",\"rows\":[";
    if (pos + arr_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + arr_open.len], arr_open);
    pos += arr_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        const p1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"account_id\":{d},\"account\":\"", .{ row.account_id, row.account_id }) catch return error.BufferTooSmall;
        pos += p1.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p1b = "\",\"number\":\"";
        if (pos + p1b.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p1b.len], p1b);
        pos += p1b.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p2 = "\",\"account_number\":\"";
        if (pos + p2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p2b = "\",\"name\":\"";
        if (pos + p2b.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2b.len], p2b);
        pos += p2b.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p2c = "\",\"account_name\":\"";
        if (pos + p2c.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2c.len], p2c);
        pos += p2c.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p3 = "\",\"type\":\"";
        if (pos + p3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], acct_type);
        const p4 = "\",\"debit\":";
        if (pos + p4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p4.len], p4);
        pos += p4.len;
        pos += try appendAmountValue(buf[pos..], row.debit_balance, dp, as_integer_minor_units);
        const p5 = ",\"credit\":";
        if (pos + p5.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p5.len], p5);
        pos += p5.len;
        pos += try appendAmountValue(buf[pos..], row.credit_balance, dp, as_integer_minor_units);
        const p6 = "}";
        if (pos + p6.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p6.len], p6);
        pos += p6.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── LedgerResult Exports ───────────────────────────────────────

pub fn ledgerResultToCsv(result: *report_mod.LedgerResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const header = "posting_date,document_number,description,account_number,account_name,debit_amount,credit_amount,running_balance,transaction_currency,transaction_debit,transaction_credit,fx_rate\n";
    if (pos + header.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    for (result.rows) |row| {
        pos += try csvField(buf[pos..], row.posting_date[0..row.posting_date_len]);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.document_number[0..row.document_number_len]);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.description[0..row.description_len]);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.account_number[0..row.account_number_len]);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.account_name[0..row.account_name_len]);
        const dp = result.decimal_places;
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.debit_amount, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.credit_amount, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.running_balance, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try csvField(buf[pos..], row.transaction_currency[0..row.transaction_currency_len]);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.transaction_debit, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.transaction_credit, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.fx_rate, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '\n';
        pos += 1;
    }

    return buf[0..pos];
}

pub fn ledgerResultToJson(result: *report_mod.LedgerResult, buf: []u8) ![]u8 {
    return ledgerResultToJsonEx(result, "ledger", 0, buf, false);
}

pub fn ledgerResultToJsonEx(result: *report_mod.LedgerResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;
    const open = "{\"packet_kind\":\"";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const mid = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, result.decimal_places }) catch return error.BufferTooSmall;
    pos += mid.len;
    pos += try jsonString(buf[pos..], if (as_integer_minor_units) "integer_minor_units" else "decimal_string");
    const mid2 = "\"},\"opening_balance\":";
    if (pos + mid2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid2.len], mid2);
    pos += mid2.len;
    pos += try appendAmountValue(buf[pos..], result.opening_balance, result.decimal_places, as_integer_minor_units);
    const mid3 = ",\"closing_balance\":";
    if (pos + mid3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid3.len], mid3);
    pos += mid3.len;
    pos += try appendAmountValue(buf[pos..], result.closing_balance, result.decimal_places, as_integer_minor_units);
    const mid4 = ",\"totals\":{\"debits\":";
    if (pos + mid4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid4.len], mid4);
    pos += mid4.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, result.decimal_places, as_integer_minor_units);
    const mid5 = ",\"credits\":";
    if (pos + mid5.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid5.len], mid5);
    pos += mid5.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, result.decimal_places, as_integer_minor_units);
    const mid6 = "},\"total_debits\":";
    if (pos + mid6.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid6.len], mid6);
    pos += mid6.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, result.decimal_places, as_integer_minor_units);
    const mid7 = ",\"total_credits\":";
    if (pos + mid7.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + mid7.len], mid7);
    pos += mid7.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, result.decimal_places, as_integer_minor_units);
    const rows_open = ",\"rows\":[";
    if (pos + rows_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + rows_open.len], rows_open);
    pos += rows_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const p1 = "{\"posting_date\":\"";
        if (pos + p1.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p1.len], p1);
        pos += p1.len;
        pos += try jsonString(buf[pos..], row.posting_date[0..row.posting_date_len]);
        const p2 = "\",\"document_number\":\"";
        if (pos + p2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], row.document_number[0..row.document_number_len]);
        const p3 = "\",\"description\":\"";
        if (pos + p3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], row.description[0..row.description_len]);
        const p4 = std.fmt.bufPrint(buf[pos..], "\",\"account_id\":{d},\"account_number\":\"", .{row.account_id}) catch return error.BufferTooSmall;
        pos += p4.len;
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        const p4b = "\",\"number\":\"";
        if (pos + p4b.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p4b.len], p4b);
        pos += p4b.len;
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        const p5 = "\",\"account_name\":\"";
        if (pos + p5.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p5.len], p5);
        pos += p5.len;
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        const p5b = "\",\"name\":\"";
        if (pos + p5b.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p5b.len], p5b);
        pos += p5b.len;
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        const p6 = "\",\"transaction_currency\":\"";
        if (pos + p6.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p6.len], p6);
        pos += p6.len;
        pos += try jsonString(buf[pos..], row.transaction_currency[0..row.transaction_currency_len]);
        const nums = "\",\"debit\":";
        if (pos + nums.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + nums.len], nums);
        pos += nums.len;
        pos += try appendAmountValue(buf[pos..], row.debit_amount, result.decimal_places, as_integer_minor_units);
        const nums2 = ",\"credit\":";
        if (pos + nums2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + nums2.len], nums2);
        pos += nums2.len;
        pos += try appendAmountValue(buf[pos..], row.credit_amount, result.decimal_places, as_integer_minor_units);
        const nums3 = ",\"running_balance\":";
        if (pos + nums3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + nums3.len], nums3);
        pos += nums3.len;
        pos += try appendAmountValue(buf[pos..], row.running_balance, result.decimal_places, as_integer_minor_units);
        const nums4 = ",\"transaction_debit\":";
        if (pos + nums4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + nums4.len], nums4);
        pos += nums4.len;
        pos += try appendAmountValue(buf[pos..], row.transaction_debit, result.decimal_places, as_integer_minor_units);
        const nums5 = ",\"transaction_credit\":";
        if (pos + nums5.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + nums5.len], nums5);
        pos += nums5.len;
        pos += try appendAmountValue(buf[pos..], row.transaction_credit, result.decimal_places, as_integer_minor_units);
        const nums6 = std.fmt.bufPrint(buf[pos..], ",\"fx_rate\":{d}}}", .{row.fx_rate}) catch return error.BufferTooSmall;
        pos += nums6.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

// ── ClassifiedResult Exports ───────────────────────────────────

pub fn classifiedResultToCsv(result: *classification_mod.ClassifiedResult, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const header = "node_type,depth,label,account_id,debit_balance,credit_balance\n";
    if (pos + header.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + header.len], header);
    pos += header.len;

    for (result.rows) |row| {
        pos += try csvField(buf[pos..], row.node_type[0..row.node_type_len]);
        const mid1 = std.fmt.bufPrint(buf[pos..], ",{d},", .{row.depth}) catch return error.BufferTooSmall;
        pos += mid1.len;
        pos += try csvField(buf[pos..], row.label[0..row.label_len]);
        const dp = result.decimal_places;
        const aid = std.fmt.bufPrint(buf[pos..], ",{d},", .{row.account_id}) catch return error.BufferTooSmall;
        pos += aid.len;
        pos += try fmtAmount(buf[pos..], row.debit_balance, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try fmtAmount(buf[pos..], row.credit_balance, dp);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '\n';
        pos += 1;
    }

    return buf[0..pos];
}

pub fn classifiedResultToJson(result: *classification_mod.ClassifiedResult, buf: []u8) ![]u8 {
    return classifiedResultToJsonEx(result, "classified_result", 0, buf, false);
}

pub fn classifiedResultToJsonEx(result: *classification_mod.ClassifiedResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;

    const dp = result.decimal_places;
    const hdr = "{\"packet_kind\":\"";
    if (pos + hdr.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr.len], hdr);
    pos += hdr.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const hdr0 = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, dp }) catch return error.BufferTooSmall;
    pos += hdr0.len;
    pos += try jsonString(buf[pos..], if (as_integer_minor_units) "integer_minor_units" else "decimal_string");
    const hdr2 = "\"},\"totals\":{\"debits\":";
    if (pos + hdr2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr2.len], hdr2);
    pos += hdr2.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, dp, as_integer_minor_units);
    const hdr2b = ",\"credits\":";
    if (pos + hdr2b.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr2b.len], hdr2b);
    pos += hdr2b.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, dp, as_integer_minor_units);
    const hdr3 = "},\"total_debits\":";
    if (pos + hdr3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr3.len], hdr3);
    pos += hdr3.len;
    pos += try appendAmountValue(buf[pos..], result.total_debits, dp, as_integer_minor_units);
    const hdr4 = ",\"total_credits\":";
    if (pos + hdr4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr4.len], hdr4);
    pos += hdr4.len;
    pos += try appendAmountValue(buf[pos..], result.total_credits, dp, as_integer_minor_units);
    const hdr4b = ",\"unclassified_debits\":";
    if (pos + hdr4b.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr4b.len], hdr4b);
    pos += hdr4b.len;
    pos += try appendAmountValue(buf[pos..], result.unclassified_debits, dp, as_integer_minor_units);
    const hdr5 = ",\"unclassified_credits\":";
    if (pos + hdr5.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr5.len], hdr5);
    pos += hdr5.len;
    pos += try appendAmountValue(buf[pos..], result.unclassified_credits, dp, as_integer_minor_units);
    const hdr6 = ",\"rows\":[";
    if (pos + hdr6.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + hdr6.len], hdr6);
    pos += hdr6.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const p1 = "{\"node_type\":\"";
        if (pos + p1.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p1.len], p1);
        pos += p1.len;
        pos += try jsonString(buf[pos..], row.node_type[0..row.node_type_len]);
        const mid1 = std.fmt.bufPrint(buf[pos..], "\",\"depth\":{d},\"label\":\"", .{row.depth}) catch return error.BufferTooSmall;
        pos += mid1.len;
        pos += try jsonString(buf[pos..], row.label[0..row.label_len]);
        const acct_part = std.fmt.bufPrint(buf[pos..], "\",\"account_id\":{d},\"debit\":", .{row.account_id}) catch return error.BufferTooSmall;
        pos += acct_part.len;
        pos += try appendAmountValue(buf[pos..], row.debit_balance, dp, as_integer_minor_units);
        const cr_part = ",\"credit\":";
        if (pos + cr_part.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + cr_part.len], cr_part);
        pos += cr_part.len;
        pos += try appendAmountValue(buf[pos..], row.credit_balance, dp, as_integer_minor_units);
        const close_row = "}";
        if (pos + close_row.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + close_row.len], close_row);
        pos += close_row.len;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;

    return buf[0..pos];
}

pub fn comparativeResultToJson(result: *report_mod.ComparativeReportResult, buf: []u8) ![]u8 {
    return comparativeResultToJsonEx(result, "comparative_report", 0, buf, false);
}

pub fn comparativeResultToJsonEx(result: *report_mod.ComparativeReportResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;
    const dp = result.decimal_places;

    const open = "{\"packet_kind\":\"";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const meta = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, dp }) catch return error.BufferTooSmall;
    pos += meta.len;
    pos += try jsonString(buf[pos..], if (as_integer_minor_units) "integer_minor_units" else "decimal_string");
    const totals_open = "\"},\"totals\":{\"current\":{\"debits\":";
    if (pos + totals_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals_open.len], totals_open);
    pos += totals_open.len;
    pos += try appendAmountValue(buf[pos..], result.current_total_debits, dp, as_integer_minor_units);
    const totals_mid = ",\"credits\":";
    if (pos + totals_mid.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals_mid.len], totals_mid);
    pos += totals_mid.len;
    pos += try appendAmountValue(buf[pos..], result.current_total_credits, dp, as_integer_minor_units);
    const totals_prior = "},\"prior\":{\"debits\":";
    if (pos + totals_prior.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals_prior.len], totals_prior);
    pos += totals_prior.len;
    pos += try appendAmountValue(buf[pos..], result.prior_total_debits, dp, as_integer_minor_units);
    if (pos + totals_mid.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals_mid.len], totals_mid);
    pos += totals_mid.len;
    pos += try appendAmountValue(buf[pos..], result.prior_total_credits, dp, as_integer_minor_units);
    const legacy = "}},\"current_total_debits\":";
    if (pos + legacy.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy.len], legacy);
    pos += legacy.len;
    pos += try appendAmountValue(buf[pos..], result.current_total_debits, dp, as_integer_minor_units);
    const legacy2 = ",\"current_total_credits\":";
    if (pos + legacy2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy2.len], legacy2);
    pos += legacy2.len;
    pos += try appendAmountValue(buf[pos..], result.current_total_credits, dp, as_integer_minor_units);
    const legacy3 = ",\"prior_total_debits\":";
    if (pos + legacy3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy3.len], legacy3);
    pos += legacy3.len;
    pos += try appendAmountValue(buf[pos..], result.prior_total_debits, dp, as_integer_minor_units);
    const legacy4 = ",\"prior_total_credits\":";
    if (pos + legacy4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy4.len], legacy4);
    pos += legacy4.len;
    pos += try appendAmountValue(buf[pos..], result.prior_total_credits, dp, as_integer_minor_units);
    const rows_open = ",\"rows\":[";
    if (pos + rows_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + rows_open.len], rows_open);
    pos += rows_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];
        const acct_type = row.account_type[0..row.account_type_len];

        const p1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"account_id\":{d},\"account\":\"", .{ row.account_id, row.account_id }) catch return error.BufferTooSmall;
        pos += p1.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p2 = "\",\"number\":\"";
        if (pos + p2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p3 = "\",\"account_number\":\"";
        if (pos + p3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p4 = "\",\"name\":\"";
        if (pos + p4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p4.len], p4);
        pos += p4.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p5 = "\",\"account_name\":\"";
        if (pos + p5.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p5.len], p5);
        pos += p5.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p6 = "\",\"type\":\"";
        if (pos + p6.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p6.len], p6);
        pos += p6.len;
        pos += try jsonString(buf[pos..], acct_type);
        const p7 = "\",\"account_type\":\"";
        if (pos + p7.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p7.len], p7);
        pos += p7.len;
        pos += try jsonString(buf[pos..], acct_type);
        const p8 = "\",\"current_debit\":";
        if (pos + p8.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p8.len], p8);
        pos += p8.len;
        pos += try appendAmountValue(buf[pos..], row.current_debit, dp, as_integer_minor_units);
        const p9 = ",\"current_credit\":";
        if (pos + p9.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p9.len], p9);
        pos += p9.len;
        pos += try appendAmountValue(buf[pos..], row.current_credit, dp, as_integer_minor_units);
        const p10 = ",\"prior_debit\":";
        if (pos + p10.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p10.len], p10);
        pos += p10.len;
        pos += try appendAmountValue(buf[pos..], row.prior_debit, dp, as_integer_minor_units);
        const p11 = ",\"prior_credit\":";
        if (pos + p11.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p11.len], p11);
        pos += p11.len;
        pos += try appendAmountValue(buf[pos..], row.prior_credit, dp, as_integer_minor_units);
        const p12 = ",\"variance_debit\":";
        if (pos + p12.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p12.len], p12);
        pos += p12.len;
        pos += try appendAmountValue(buf[pos..], row.variance_debit, dp, as_integer_minor_units);
        const p13 = ",\"variance_credit\":";
        if (pos + p13.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p13.len], p13);
        pos += p13.len;
        pos += try appendAmountValue(buf[pos..], row.variance_credit, dp, as_integer_minor_units);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '}';
        pos += 1;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;
    return buf[0..pos];
}

pub fn equityResultToJson(result: *report_mod.EquityResult, buf: []u8) ![]u8 {
    return equityResultToJsonEx(result, "equity_changes", 0, buf, false);
}

pub fn equityResultToJsonEx(result: *report_mod.EquityResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;
    const dp = result.decimal_places;

    const open = "{\"packet_kind\":\"";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const meta = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, dp }) catch return error.BufferTooSmall;
    pos += meta.len;
    pos += try jsonString(buf[pos..], if (as_integer_minor_units) "integer_minor_units" else "decimal_string");
    const totals = "\"},\"totals\":{\"opening\":";
    if (pos + totals.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals.len], totals);
    pos += totals.len;
    pos += try appendAmountValue(buf[pos..], result.total_opening, dp, as_integer_minor_units);
    const totals2 = ",\"closing\":";
    if (pos + totals2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals2.len], totals2);
    pos += totals2.len;
    pos += try appendAmountValue(buf[pos..], result.total_closing, dp, as_integer_minor_units);
    const legacy = "},\"net_income\":";
    if (pos + legacy.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy.len], legacy);
    pos += legacy.len;
    pos += try appendAmountValue(buf[pos..], result.net_income, dp, as_integer_minor_units);
    const legacy2 = ",\"total_opening\":";
    if (pos + legacy2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy2.len], legacy2);
    pos += legacy2.len;
    pos += try appendAmountValue(buf[pos..], result.total_opening, dp, as_integer_minor_units);
    const legacy3 = ",\"total_closing\":";
    if (pos + legacy3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy3.len], legacy3);
    pos += legacy3.len;
    pos += try appendAmountValue(buf[pos..], result.total_closing, dp, as_integer_minor_units);
    const rows_open = ",\"rows\":[";
    if (pos + rows_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + rows_open.len], rows_open);
    pos += rows_open.len;

    for (result.rows, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const acct_num = row.account_number[0..row.account_number_len];
        const acct_name = row.account_name[0..row.account_name_len];

        const p1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"account_id\":{d},\"account\":\"", .{ row.account_id, row.account_id }) catch return error.BufferTooSmall;
        pos += p1.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p2 = "\",\"number\":\"";
        if (pos + p2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p2.len], p2);
        pos += p2.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p3 = "\",\"account_number\":\"";
        if (pos + p3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p3.len], p3);
        pos += p3.len;
        pos += try jsonString(buf[pos..], acct_num);
        const p4 = "\",\"name\":\"";
        if (pos + p4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p4.len], p4);
        pos += p4.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p5 = "\",\"account_name\":\"";
        if (pos + p5.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p5.len], p5);
        pos += p5.len;
        pos += try jsonString(buf[pos..], acct_name);
        const p6 = "\",\"opening_balance\":";
        if (pos + p6.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p6.len], p6);
        pos += p6.len;
        pos += try appendAmountValue(buf[pos..], row.opening_balance, dp, as_integer_minor_units);
        const p7 = ",\"period_activity\":";
        if (pos + p7.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p7.len], p7);
        pos += p7.len;
        pos += try appendAmountValue(buf[pos..], row.period_activity, dp, as_integer_minor_units);
        const p8 = ",\"closing_balance\":";
        if (pos + p8.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p8.len], p8);
        pos += p8.len;
        pos += try appendAmountValue(buf[pos..], row.closing_balance, dp, as_integer_minor_units);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '}';
        pos += 1;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + close.len], close);
    pos += close.len;
    return buf[0..pos];
}

pub fn cashFlowIndirectResultToJson(result: *classification_mod.CashFlowIndirectResult, packet_kind: []const u8, book_id: i64, buf: []u8, as_integer_minor_units: bool) ![]u8 {
    var pos: usize = 0;
    const dp = result.decimal_places;

    const open = "{\"packet_kind\":\"";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += try jsonString(buf[pos..], packet_kind);
    const meta = std.fmt.bufPrint(buf[pos..], "\",\"book_id\":{d},\"meta\":{{\"decimal_places\":{d},\"amount_encoding\":\"", .{ book_id, dp }) catch return error.BufferTooSmall;
    pos += meta.len;
    pos += try jsonString(buf[pos..], if (as_integer_minor_units) "integer_minor_units" else "decimal_string");
    const totals = "\"},\"totals\":{\"operating\":";
    if (pos + totals.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals.len], totals);
    pos += totals.len;
    pos += try appendAmountValue(buf[pos..], result.operating_total, dp, as_integer_minor_units);
    const totals2 = ",\"investing\":";
    if (pos + totals2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals2.len], totals2);
    pos += totals2.len;
    pos += try appendAmountValue(buf[pos..], result.investing_total, dp, as_integer_minor_units);
    const totals3 = ",\"financing\":";
    if (pos + totals3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals3.len], totals3);
    pos += totals3.len;
    pos += try appendAmountValue(buf[pos..], result.financing_total, dp, as_integer_minor_units);
    const totals4 = ",\"net_cash_change\":";
    if (pos + totals4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + totals4.len], totals4);
    pos += totals4.len;
    pos += try appendAmountValue(buf[pos..], result.net_cash_change, dp, as_integer_minor_units);
    const legacy = "},\"net_income\":";
    if (pos + legacy.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy.len], legacy);
    pos += legacy.len;
    pos += try appendAmountValue(buf[pos..], result.net_income, dp, as_integer_minor_units);
    const legacy2 = ",\"operating_total\":";
    if (pos + legacy2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy2.len], legacy2);
    pos += legacy2.len;
    pos += try appendAmountValue(buf[pos..], result.operating_total, dp, as_integer_minor_units);
    const legacy3 = ",\"investing_total\":";
    if (pos + legacy3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy3.len], legacy3);
    pos += legacy3.len;
    pos += try appendAmountValue(buf[pos..], result.investing_total, dp, as_integer_minor_units);
    const legacy4 = ",\"financing_total\":";
    if (pos + legacy4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy4.len], legacy4);
    pos += legacy4.len;
    pos += try appendAmountValue(buf[pos..], result.financing_total, dp, as_integer_minor_units);
    const legacy5 = ",\"net_cash_change\":";
    if (pos + legacy5.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + legacy5.len], legacy5);
    pos += legacy5.len;
    pos += try appendAmountValue(buf[pos..], result.net_cash_change, dp, as_integer_minor_units);
    const rows_open = ",\"rows\":[";
    if (pos + rows_open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + rows_open.len], rows_open);
    pos += rows_open.len;

    for (result.adjustments, 0..) |row, i| {
        if (i > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const p1 = std.fmt.bufPrint(buf[pos..], "{{\"node_id\":{d},\"node_type\":\"", .{row.node_id}) catch return error.BufferTooSmall;
        pos += p1.len;
        pos += try jsonString(buf[pos..], row.node_type[0..row.node_type_len]);
        const p2 = std.fmt.bufPrint(buf[pos..], "\",\"depth\":{d},\"position\":{d},\"label\":\"", .{ row.depth, row.position }) catch return error.BufferTooSmall;
        pos += p2.len;
        pos += try jsonString(buf[pos..], row.label[0..row.label_len]);
        const p3 = std.fmt.bufPrint(buf[pos..], "\",\"account_id\":{d},\"debit\":", .{row.account_id}) catch return error.BufferTooSmall;
        pos += p3.len;
        pos += try appendAmountValue(buf[pos..], row.debit_balance, dp, as_integer_minor_units);
        const p4 = ",\"credit\":";
        if (pos + p4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + p4.len], p4);
        pos += p4.len;
        pos += try appendAmountValue(buf[pos..], row.credit_balance, dp, as_integer_minor_units);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '}';
        pos += 1;
    }

    const close = "]}";
    if (pos + close.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], acct_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], normal_bal);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},", .{is_contra}) catch return error.BufferTooSmall;
                pos += nums.len;
                pos += try csvField(buf[pos..], status);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"accounts\":[";
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

                const number = stmt.columnText(0) orelse "";
                const name = stmt.columnText(1) orelse "";
                const acct_type = stmt.columnText(2) orelse "";
                const normal_bal = stmt.columnText(3) orelse "";
                const is_contra = stmt.columnInt(4);
                const status = stmt.columnText(5) orelse "";

                const j1 = "{\"number\":\"";
                if (pos + j1.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try jsonString(buf[pos..], number);
                const j2 = "\",\"name\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try jsonString(buf[pos..], name);
                const j3 = "\",\"account_type\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], acct_type);
                const j4 = "\",\"normal_balance\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], normal_bal);
                const j5 = std.fmt.bufPrint(buf[pos..], "\",\"is_contra\":{s},\"status\":\"", .{if (is_contra != 0) "true" else "false"}) catch return error.BufferTooSmall;
                pos += j5.len;
                pos += try jsonString(buf[pos..], status);
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], txn_date);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], post_date);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], desc);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], status);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},", .{line_num}) catch return error.BufferTooSmall;
                pos += nums.len;
                pos += try csvField(buf[pos..], acct_num);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], acct_name);
                const nums2 = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ debit, credit }) catch return error.BufferTooSmall;
                pos += nums2.len;
                pos += try csvField(buf[pos..], currency);
                const fx = std.fmt.bufPrint(buf[pos..], ",{d}\n", .{fx_rate}) catch return error.BufferTooSmall;
                pos += fx.len;
            }
        },
        .json => {
            const open = "{\"entries\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
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
                        const close_entry = "]}";
                        if (pos + close_entry.len > buf.len) return error.BufferTooSmall;
                        @memcpy(buf[pos .. pos + close_entry.len], close_entry);
                        pos += close_entry.len;
                    }

                    const h1 = if (entry_open) ",{\"document_number\":\"" else "{\"document_number\":\"";
                    if (pos + h1.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h1.len], h1);
                    pos += h1.len;
                    pos += try jsonString(buf[pos..], doc);
                    const h2 = "\",\"transaction_date\":\"";
                    if (pos + h2.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h2.len], h2);
                    pos += h2.len;
                    pos += try jsonString(buf[pos..], txn_date);
                    const h3 = "\",\"posting_date\":\"";
                    if (pos + h3.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h3.len], h3);
                    pos += h3.len;
                    pos += try jsonString(buf[pos..], post_date);
                    const h4 = "\",\"description\":\"";
                    if (pos + h4.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h4.len], h4);
                    pos += h4.len;
                    pos += try jsonString(buf[pos..], desc);
                    const h5 = "\",\"status\":\"";
                    if (pos + h5.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h5.len], h5);
                    pos += h5.len;
                    pos += try jsonString(buf[pos..], status);
                    const h6 = "\",\"lines\":[";
                    if (pos + h6.len > buf.len) return error.BufferTooSmall;
                    @memcpy(buf[pos .. pos + h6.len], h6);
                    pos += h6.len;

                    const copy_len = @min(doc.len, prev_doc.len);
                    @memcpy(prev_doc[0..copy_len], doc[0..copy_len]);
                    prev_doc_len = copy_len;
                    entry_open = true;
                } else {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }

                const l1 = std.fmt.bufPrint(buf[pos..], "{{\"line_number\":{d},\"account_number\":\"", .{line_num}) catch return error.BufferTooSmall;
                pos += l1.len;
                pos += try jsonString(buf[pos..], acct_num);
                const l2 = "\",\"account_name\":\"";
                if (pos + l2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + l2.len], l2);
                pos += l2.len;
                pos += try jsonString(buf[pos..], acct_name);
                const l3 = std.fmt.bufPrint(buf[pos..], "\",\"debit\":{d},\"credit\":{d},\"currency\":\"", .{ debit, credit }) catch return error.BufferTooSmall;
                pos += l3.len;
                pos += try jsonString(buf[pos..], currency);
                const l4 = std.fmt.bufPrint(buf[pos..], "\",\"fx_rate\":{d}}}", .{fx_rate}) catch return error.BufferTooSmall;
                pos += l4.len;
            }

            if (entry_open) {
                const close_entry = "]}";
                if (pos + close_entry.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + close_entry.len], close_entry);
                pos += close_entry.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try csvField(buf[pos..], entity_type);
                const eid_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{entity_id}) catch return error.BufferTooSmall;
                pos += eid_s.len;
                pos += try csvField(buf[pos..], action);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], field);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], old_val);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], new_val);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], performed_by);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], performed_at);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"records\":[";
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

                const id = stmt.columnInt64(0);
                const entity_type = stmt.columnText(1) orelse "";
                const entity_id = stmt.columnInt64(2);
                const action = stmt.columnText(3) orelse "";
                const field = stmt.columnText(4) orelse "";
                const old_val = stmt.columnText(5) orelse "";
                const new_val = stmt.columnText(6) orelse "";
                const performed_by = stmt.columnText(7) orelse "";
                const performed_at = stmt.columnText(8) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"entity_type\":\"", .{id}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try jsonString(buf[pos..], entity_type);
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"entity_id\":{d},\"action\":\"", .{entity_id}) catch return error.BufferTooSmall;
                pos += j2.len;
                pos += try jsonString(buf[pos..], action);
                const j3 = "\",\"field_changed\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], field);
                const j4 = "\",\"old_value\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], old_val);
                const j5 = "\",\"new_value\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], new_val);
                const j6 = "\",\"performed_by\":\"";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try jsonString(buf[pos..], performed_by);
                const j7 = "\",\"performed_at\":\"";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
                pos += try jsonString(buf[pos..], performed_at);
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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

                const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_s.len;
                pos += try csvField(buf[pos..], name);
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},", .{ period_number, year }) catch return error.BufferTooSmall;
                pos += nums.len;
                pos += try csvField(buf[pos..], start_date);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], end_date);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], period_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], status);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"periods\":[";
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

                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const period_number = stmt.columnInt(2);
                const year = stmt.columnInt(3);
                const start_date = stmt.columnText(4) orelse "";
                const end_date = stmt.columnText(5) orelse "";
                const period_type = stmt.columnText(6) orelse "";
                const status = stmt.columnText(7) orelse "";

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.BufferTooSmall;
                pos += j1.len;
                pos += try jsonString(buf[pos..], name);
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"period_number\":{d},\"year\":{d},\"start_date\":\"", .{ period_number, year }) catch return error.BufferTooSmall;
                pos += j2.len;
                pos += try jsonString(buf[pos..], start_date);
                const j3 = "\",\"end_date\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], end_date);
                const j4 = "\",\"period_type\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], period_type);
                const j5 = "\",\"status\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], status);
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
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
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], group_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], control_acct);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_number);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try csvField(buf[pos..], cp_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"subledger_accounts\":[";
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

                const group_name = stmt.columnText(0) orelse "";
                const group_type = stmt.columnText(1) orelse "";
                const control_acct = stmt.columnText(2) orelse "";
                const cp_number = stmt.columnText(3) orelse "";
                const cp_name = stmt.columnText(4) orelse "";
                const cp_type = stmt.columnText(5) orelse "";

                const j1 = "{\"group_name\":\"";
                if (pos + j1.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j1.len], j1);
                pos += j1.len;
                pos += try jsonString(buf[pos..], group_name);
                const j2 = "\",\"group_type\":\"";
                if (pos + j2.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try jsonString(buf[pos..], group_type);
                const j3 = "\",\"control_account_number\":\"";
                if (pos + j3.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j3.len], j3);
                pos += j3.len;
                pos += try jsonString(buf[pos..], control_acct);
                const j4 = "\",\"counterparty_number\":\"";
                if (pos + j4.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j4.len], j4);
                pos += j4.len;
                pos += try jsonString(buf[pos..], cp_number);
                const j5 = "\",\"counterparty_name\":\"";
                if (pos + j5.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j5.len], j5);
                pos += j5.len;
                pos += try jsonString(buf[pos..], cp_name);
                const j6 = "\",\"counterparty_type\":\"";
                if (pos + j6.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j6.len], j6);
                pos += j6.len;
                pos += try jsonString(buf[pos..], cp_type);
                const j7 = "\"}";
                if (pos + j7.len > buf.len) return error.BufferTooSmall;
                @memcpy(buf[pos .. pos + j7.len], j7);
                pos += j7.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
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
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            const id_s = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
            pos += id_s.len;
            pos += try csvField(buf[pos..], name);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
            pos += try csvField(buf[pos..], currency);
            const dec_s = std.fmt.bufPrint(buf[pos..], ",{d},", .{decimals}) catch return error.BufferTooSmall;
            pos += dec_s.len;
            pos += try csvField(buf[pos..], status);
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = '\n';
            pos += 1;
        },
        .json => {
            const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.BufferTooSmall;
            pos += j1.len;
            pos += try jsonString(buf[pos..], name);
            const j2 = "\",\"base_currency\":\"";
            if (pos + j2.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + j2.len], j2);
            pos += j2.len;
            pos += try jsonString(buf[pos..], currency);
            const j3 = std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"status\":\"", .{decimals}) catch return error.BufferTooSmall;
            pos += j3.len;
            pos += try jsonString(buf[pos..], status);
            const j4 = "\"}";
            if (pos + j4.len > buf.len) return error.BufferTooSmall;
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
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
    try std.testing.expectError(error.BufferTooSmall, result);
}

// ── csvField unit tests ──────────────────────────────────────────

test "csvField: plain field no quoting" {
    var buf: [100]u8 = undefined;
    const len = try csvField(&buf, "hello");
    try std.testing.expectEqualStrings("hello", buf[0..len]);
}

test "csvField: field with comma is quoted" {
    var buf: [100]u8 = undefined;
    const len = try csvField(&buf, "hello,world");
    try std.testing.expectEqualStrings("\"hello,world\"", buf[0..len]);
}

test "csvField: field with double-quote is escaped" {
    var buf: [100]u8 = undefined;
    const len = try csvField(&buf, "say \"hi\"");
    try std.testing.expectEqualStrings("\"say \"\"hi\"\"\"", buf[0..len]);
}

test "csvField: field with newline is quoted" {
    var buf: [100]u8 = undefined;
    const len = try csvField(&buf, "line1\nline2");
    try std.testing.expectEqualStrings("\"line1\nline2\"", buf[0..len]);
}

test "csvField: empty field" {
    var buf: [100]u8 = undefined;
    const len = try csvField(&buf, "");
    try std.testing.expectEqual(@as(usize, 0), len);
}

// ── jsonString unit tests ────────────────────────────────────────

test "jsonString: plain string" {
    var buf: [100]u8 = undefined;
    const len = try jsonString(&buf, "hello");
    try std.testing.expectEqualStrings("hello", buf[0..len]);
}

test "jsonString: escapes double quote" {
    var buf: [100]u8 = undefined;
    const len = try jsonString(&buf, "say \"hi\"");
    try std.testing.expectEqualStrings("say \\\"hi\\\"", buf[0..len]);
}

test "jsonString: escapes backslash" {
    var buf: [100]u8 = undefined;
    const len = try jsonString(&buf, "path\\to");
    try std.testing.expectEqualStrings("path\\\\to", buf[0..len]);
}

test "jsonString: escapes newline and tab" {
    var buf: [100]u8 = undefined;
    const len = try jsonString(&buf, "a\nb\tc");
    try std.testing.expectEqualStrings("a\\nb\\tc", buf[0..len]);
}

test "jsonString: escapes control character" {
    var buf: [100]u8 = undefined;
    const input = [_]u8{ 'a', 0x01, 'b' };
    const len = try jsonString(&buf, &input);
    try std.testing.expectEqualStrings("a\\u0001b", buf[0..len]);
}

test "reportToCsv outputs formatted decimal amounts" {
    const s = try setupAndPost();
    defer {
        s.result.deinit();
        s.database.close();
    }
    var buf: [8192]u8 = undefined;
    const csv = try reportToCsv(s.result, &buf);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000.00") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "100000000000") == null);
}

test "reportToJson outputs formatted decimal amounts" {
    const s = try setupAndPost();
    defer {
        s.result.deinit();
        s.database.close();
    }
    var buf: [8192]u8 = undefined;
    const json = try reportToJson(s.result, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"1000.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "100000000000") == null);
}

test "ledgerResultToCsv outputs formatted decimal amounts" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 100_000_000_000, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 100_000_000_000, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const result = try report_mod.generalLedger(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();
    var buf: [16384]u8 = undefined;
    const csv = try ledgerResultToCsv(result, &buf);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000.00") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "100000000000") == null);
}
