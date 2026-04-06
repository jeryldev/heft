const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const export_mod = @import("export.zig");

pub const OpenItemStatus = enum {
    open,
    partial,
    closed,

    pub fn toString(self: OpenItemStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?OpenItemStatus {
        const map = .{
            .{ "open", OpenItemStatus.open },
            .{ "partial", OpenItemStatus.partial },
            .{ "closed", OpenItemStatus.closed },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub fn createOpenItem(database: db.Database, entry_line_id: i64, counterparty_id: i64, original_amount: i64, due_date: ?[]const u8, book_id: i64, performed_by: []const u8) !i64 {
    if (original_amount <= 0) return error.InvalidAmount;

    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    var stmt = try database.prepare(
        \\INSERT INTO ledger_open_items (entry_line_id, counterparty_id, original_amount, remaining_amount, due_date, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?);
    );
    defer stmt.finalize();
    try stmt.bindInt(1, entry_line_id);
    try stmt.bindInt(2, counterparty_id);
    try stmt.bindInt(3, original_amount);
    try stmt.bindInt(4, original_amount);
    if (due_date) |d| try stmt.bindText(5, d) else try stmt.bindNull(5);
    try stmt.bindInt(6, book_id);
    _ = stmt.step() catch return error.DuplicateNumber;

    const id = database.lastInsertRowId();
    try audit.log(database, "open_item", id, "create", null, null, null, performed_by, book_id);

    if (owns_txn) try database.commit();
    return id;
}

pub fn allocatePayment(database: db.Database, open_item_id: i64, amount: i64, performed_by: []const u8) !void {
    if (amount <= 0) return error.InvalidAmount;

    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    var remaining: i64 = 0;
    var book_id: i64 = 0;
    {
        var stmt = try database.prepare("SELECT remaining_amount, status, book_id FROM ledger_open_items WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, open_item_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        remaining = stmt.columnInt64(0);
        const status = OpenItemStatus.fromString(stmt.columnText(1).?) orelse return error.InvalidInput;
        if (status == .closed) return error.InvalidInput;
        book_id = stmt.columnInt64(2);
    }

    if (amount > remaining) return error.InvalidAmount;

    const new_remaining = remaining - amount;
    const new_status: OpenItemStatus = if (new_remaining == 0) .closed else .partial;

    {
        var stmt = try database.prepare(
            \\UPDATE ledger_open_items SET remaining_amount = ?, status = ?,
            \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            \\WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, new_remaining);
        try stmt.bindText(2, new_status.toString());
        try stmt.bindInt(3, open_item_id);
        _ = try stmt.step();
    }

    var amt_buf: [24]u8 = undefined;
    const amt_str = std.fmt.bufPrint(&amt_buf, "{d}", .{amount}) catch unreachable;
    try audit.log(database, "open_item", open_item_id, "allocate", "remaining_amount", amt_str, null, performed_by, book_id);

    if (owns_txn) try database.commit();
}

pub fn listOpenItems(database: db.Database, counterparty_id: i64, include_closed: bool, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    const sql: [*:0]const u8 = if (include_closed)
        \\SELECT oi.id, oi.entry_line_id, oi.original_amount, oi.remaining_amount,
        \\  oi.due_date, oi.status
        \\FROM ledger_open_items oi
        \\WHERE oi.counterparty_id = ?
        \\ORDER BY oi.due_date ASC, oi.id ASC;
    else
        \\SELECT oi.id, oi.entry_line_id, oi.original_amount, oi.remaining_amount,
        \\  oi.due_date, oi.status
        \\FROM ledger_open_items oi
        \\WHERE oi.counterparty_id = ? AND oi.status != 'closed'
        \\ORDER BY oi.due_date ASC, oi.id ASC;
    ;

    var stmt = try database.prepare(sql);
    defer stmt.finalize();
    try stmt.bindInt(1, counterparty_id);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "id,entry_line_id,original_amount,remaining_amount,due_date,status\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const row = std.fmt.bufPrint(buf[pos..], "{d},{d},{d},{d},", .{
                    stmt.columnInt64(0), stmt.columnInt64(1), stmt.columnInt64(2), stmt.columnInt64(3),
                }) catch return error.InvalidInput;
                pos += row.len;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(4) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], stmt.columnText(5) orelse "");
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"items\":[";
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
                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"entry_line_id\":{d},\"original_amount\":{d},\"remaining_amount\":{d},\"due_date\":", .{
                    stmt.columnInt64(0), stmt.columnInt64(1), stmt.columnInt64(2), stmt.columnInt64(3),
                }) catch return error.InvalidInput;
                pos += j1.len;
                if (stmt.columnText(4)) |dd| {
                    if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = '"';
                    pos += 1;
                    pos += try export_mod.jsonString(buf[pos..], dd);
                    if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = '"';
                    pos += 1;
                } else {
                    const null_str = "null";
                    if (pos + null_str.len > buf.len) return error.InvalidInput;
                    @memcpy(buf[pos .. pos + null_str.len], null_str);
                    pos += null_str.len;
                }
                const j2 = ",\"status\":\"";
                if (pos + j2.len > buf.len) return error.InvalidInput;
                @memcpy(buf[pos .. pos + j2.len], j2);
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], stmt.columnText(5) orelse "");
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

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const subledger_mod = @import("subledger.zig");
const money = @import("money.zig");

fn setupOpenItemDb() !struct { database: db.Database, book_id: i64, ar_acct: i64, cash_acct: i64, customer: i64, period_id: i64 } {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    const book_id = try book_mod.Book.create(database, "AR Test", "PHP", 2, "admin");
    const ar_acct = try account_mod.Account.create(database, book_id, "1200", "AR", .asset, false, "admin");
    const cash_acct = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    _ = revenue;
    const group_id = try subledger_mod.SubledgerGroup.create(database, book_id, "Customers", "customer", 1, ar_acct, null, null, "admin");
    const customer = try subledger_mod.SubledgerAccount.create(database, book_id, "CUST-001", "Acme Corp", "customer", group_id, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return .{ .database = database, .book_id = book_id, .ar_acct = ar_acct, .cash_acct = cash_acct, .customer = customer, .period_id = period_id };
}

fn postInvoice(database: db.Database, s: anytype, doc: []const u8, amount: i64) !i64 {
    const revenue_acct: i64 = 3;
    const eid = try entry_mod.Entry.createDraft(database, s.book_id, doc, "2026-01-15", "2026-01-15", null, s.period_id, null, "admin");
    const line_id = try entry_mod.Entry.addLine(database, eid, 1, amount, 0, "PHP", money.FX_RATE_SCALE, s.ar_acct, s.customer, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, amount, "PHP", money.FX_RATE_SCALE, revenue_acct, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
    return line_id;
}

test "createOpenItem for invoice" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    try std.testing.expect(oi_id > 0);
}

test "allocatePayment reduces remaining and changes status" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");

    try allocatePayment(s.database, oi_id, 60_000_000_000, "admin");

    var stmt = try s.database.prepare("SELECT remaining_amount, status FROM ledger_open_items WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, oi_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 40_000_000_000), stmt.columnInt64(0));
    try std.testing.expectEqualStrings("partial", stmt.columnText(1).?);
}

test "full payment closes open item" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");

    try allocatePayment(s.database, oi_id, 100_000_000_000, "admin");

    var stmt = try s.database.prepare("SELECT remaining_amount, status FROM ledger_open_items WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, oi_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    try std.testing.expectEqualStrings("closed", stmt.columnText(1).?);
}

test "overpayment rejected" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    const result = allocatePayment(s.database, oi_id, 200_000_000_000, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "payment on closed item rejected" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    try allocatePayment(s.database, oi_id, 100_000_000_000, "admin");
    const result = allocatePayment(s.database, oi_id, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "listOpenItems CSV excludes closed by default" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line1 = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const line2 = try postInvoice(s.database, s, "INV-002", 200_000_000_000);
    const oi1 = try createOpenItem(s.database, line1, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    _ = try createOpenItem(s.database, line2, s.customer, 200_000_000_000, "2026-02-28", s.book_id, "admin");
    try allocatePayment(s.database, oi1, 100_000_000_000, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try listOpenItems(s.database, s.customer, false, &buf, .csv);
    try std.testing.expect(std.mem.indexOf(u8, csv, "200000000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "closed") == null);
}

test "listOpenItems JSON includes all when requested" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line1 = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    _ = try createOpenItem(s.database, line1, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");

    var buf: [4096]u8 = undefined;
    const json = try listOpenItems(s.database, s.customer, true, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"open\"") != null);
}

test "allocatePayment writes audit log" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    const oi_id = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    try allocatePayment(s.database, oi_id, 50_000_000_000, "admin");

    var stmt = try s.database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'open_item' AND action = 'allocate';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "createOpenItem rejects zero amount" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const result = createOpenItem(s.database, 1, s.customer, 0, null, s.book_id, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "createOpenItem rejects duplicate entry_line_id" {
    const s = try setupOpenItemDb();
    defer s.database.close();
    const line_id = try postInvoice(s.database, s, "INV-001", 100_000_000_000);
    _ = try createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    const result = createOpenItem(s.database, line_id, s.customer, 100_000_000_000, "2026-02-14", s.book_id, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}
