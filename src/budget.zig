const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const export_mod = @import("export.zig");

pub const BudgetStatus = enum {
    draft,
    approved,
    closed,

    pub fn toString(self: BudgetStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?BudgetStatus {
        const map = .{
            .{ "draft", BudgetStatus.draft },
            .{ "approved", BudgetStatus.approved },
            .{ "closed", BudgetStatus.closed },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Budget = struct {
    pub fn create(database: db.Database, book_id: i64, name: []const u8, fiscal_year: i32, performed_by: []const u8) !i64 {
        if (name.len == 0) return error.InvalidInput;

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        var stmt = try database.prepare(
            \\INSERT INTO ledger_budgets (name, fiscal_year, book_id)
            \\VALUES (?, ?, ?);
        );
        defer stmt.finalize();
        try stmt.bindText(1, name);
        try stmt.bindInt(2, @intCast(fiscal_year));
        try stmt.bindInt(3, book_id);
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "budget", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    pub fn delete(database: db.Database, budget_id: i64, performed_by: []const u8) !void {
        try database.beginTransaction();
        errdefer database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_budgets WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, budget_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_budget_lines WHERE budget_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, budget_id);
            _ = try stmt.step();
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_budgets WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, budget_id);
            _ = try stmt.step();
        }

        try audit.log(database, "budget", budget_id, "delete", null, null, null, performed_by, book_id);
        try database.commit();
    }
};

pub const BudgetLine = struct {
    pub fn set(database: db.Database, budget_id: i64, account_id: i64, period_id: i64, amount: i64, performed_by: []const u8) !i64 {
        try database.beginTransaction();
        errdefer database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_budgets WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, budget_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (stmt.columnInt64(0) != book_id) return error.CrossBookViolation;
        }

        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (stmt.columnInt64(0) != book_id) return error.CrossBookViolation;
        }

        var stmt = try database.prepare(
            \\INSERT INTO ledger_budget_lines (amount, budget_id, account_id, period_id)
            \\VALUES (?, ?, ?, ?)
            \\ON CONFLICT (budget_id, account_id, period_id) DO UPDATE SET
            \\  amount = excluded.amount,
            \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, amount);
        try stmt.bindInt(2, budget_id);
        try stmt.bindInt(3, account_id);
        try stmt.bindInt(4, period_id);
        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "budget_line", id, "set", "amount", null, null, performed_by, book_id);

        try database.commit();
        return id;
    }
};

pub fn budgetVsActual(database: db.Database, budget_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var book_id: i64 = 0;
    {
        var stmt = try database.prepare("SELECT book_id FROM ledger_budgets WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, budget_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        book_id = stmt.columnInt64(0);
    }

    var stmt = try database.prepare(
        \\SELECT a.id, a.number, a.name,
        \\  COALESCE(budget.total_budget, 0) as budget_amount,
        \\  COALESCE(actual.actual_debit, 0) as actual_debit,
        \\  COALESCE(actual.actual_credit, 0) as actual_credit
        \\FROM ledger_accounts a
        \\LEFT JOIN (
        \\  SELECT bl.account_id, SUM(bl.amount) as total_budget
        \\  FROM ledger_budget_lines bl
        \\  JOIN ledger_periods p ON p.id = bl.period_id
        \\  WHERE bl.budget_id = ? AND p.start_date >= ? AND p.end_date <= ?
        \\  GROUP BY bl.account_id
        \\) budget ON budget.account_id = a.id
        \\LEFT JOIN (
        \\  SELECT ab.account_id,
        \\    SUM(ab.debit_sum) as actual_debit, SUM(ab.credit_sum) as actual_credit
        \\  FROM ledger_account_balances ab
        \\  JOIN ledger_periods p ON p.id = ab.period_id
        \\  WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
        \\  GROUP BY ab.account_id
        \\) actual ON actual.account_id = a.id
        \\WHERE a.book_id = ? AND (budget.total_budget IS NOT NULL OR actual.actual_debit IS NOT NULL)
        \\ORDER BY a.number ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    try stmt.bindInt(4, book_id);
    try stmt.bindText(5, start_date);
    try stmt.bindText(6, end_date);
    try stmt.bindInt(7, book_id);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "account_id,account_number,account_name,budget,actual_debit,actual_credit,variance\n";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const acct_id = stmt.columnInt64(0);
                const number = stmt.columnText(1) orelse "";
                const name = stmt.columnText(2) orelse "";
                const budget_amt = stmt.columnInt64(3);
                const actual_debit = stmt.columnInt64(4);
                const actual_credit = stmt.columnInt64(5);
                const actual_net = actual_debit - actual_credit;
                const variance = actual_net - budget_amt;

                const row = std.fmt.bufPrint(buf[pos..], "{d},", .{acct_id}) catch return error.InvalidInput;
                pos += row.len;
                pos += try export_mod.csvField(buf[pos..], number);
                if (pos >= buf.len) return error.InvalidInput;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], name);
                const rest = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d},{d}\n", .{ budget_amt, actual_debit, actual_credit, variance }) catch return error.InvalidInput;
                pos += rest.len;
            }
        },
        .json => {
            const header = "{\"rows\":[";
            if (pos + header.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            var first = true;
            while (try stmt.step()) {
                if (!first) {
                    if (pos >= buf.len) return error.InvalidInput;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const acct_id = stmt.columnInt64(0);
                const number = stmt.columnText(1) orelse "";
                const name = stmt.columnText(2) orelse "";
                const budget_amt = stmt.columnInt64(3);
                const actual_debit = stmt.columnInt64(4);
                const actual_credit = stmt.columnInt64(5);
                const actual_net = actual_debit - actual_credit;
                const variance = actual_net - budget_amt;

                const j1 = std.fmt.bufPrint(buf[pos..], "{{\"account_id\":{d},\"account_number\":\"", .{acct_id}) catch return error.InvalidInput;
                pos += j1.len;
                pos += try export_mod.jsonString(buf[pos..], number);
                const j2 = std.fmt.bufPrint(buf[pos..], "\",\"account_name\":\"", .{}) catch return error.InvalidInput;
                pos += j2.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const j3 = std.fmt.bufPrint(buf[pos..], "\",\"budget\":{d},\"actual_debit\":{d},\"actual_credit\":{d},\"variance\":{d}}}", .{ budget_amt, actual_debit, actual_credit, variance }) catch return error.InvalidInput;
                pos += j3.len;
            }

            const footer = "]}";
            if (pos + footer.len > buf.len) return error.InvalidInput;
            @memcpy(buf[pos .. pos + footer.len], footer);
            pos += footer.len;
        },
    }

    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    const schema = @import("schema.zig");
    try schema.createAll(database);
    return database;
}

fn createTestBook(database: db.Database) !i64 {
    const book_mod = @import("book.zig");
    return book_mod.Book.create(database, "Test Book", "PHP", 2, "admin");
}

fn createTestAccount(database: db.Database, book_id: i64, number: []const u8, name: []const u8, acct_type: @import("account.zig").AccountType) !i64 {
    const account_mod = @import("account.zig");
    return account_mod.Account.create(database, book_id, number, name, acct_type, false, "admin");
}

fn createTestPeriod(database: db.Database, book_id: i64) !i64 {
    const period_mod = @import("period.zig");
    return period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
}

test "Budget.create inserts and verifies via SQL" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);

    const id = try Budget.create(database, book_id, "FY2026 Budget", 2026, "admin");
    try std.testing.expect(id > 0);

    var stmt = try database.prepare("SELECT name, fiscal_year, status, book_id FROM ledger_budgets WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("FY2026 Budget", stmt.columnText(0).?);
    try std.testing.expectEqual(@as(i64, 2026), stmt.columnInt64(1));
    try std.testing.expectEqualStrings("draft", stmt.columnText(2).?);
    try std.testing.expectEqual(book_id, stmt.columnInt64(3));
}

test "BudgetLine.set inserts and verifies via SQL" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const acct_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    const line_id = try BudgetLine.set(database, budget_id, acct_id, period_id, 10_000_000_000_00, "admin");
    try std.testing.expect(line_id > 0);

    var stmt = try database.prepare("SELECT amount, budget_id, account_id, period_id FROM ledger_budget_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(budget_id, stmt.columnInt64(1));
    try std.testing.expectEqual(acct_id, stmt.columnInt64(2));
    try std.testing.expectEqual(period_id, stmt.columnInt64(3));
}

test "BudgetLine.set upserts on same account+period" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const acct_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    _ = try BudgetLine.set(database, budget_id, acct_id, period_id, 10_000_000_000_00, "admin");
    _ = try BudgetLine.set(database, budget_id, acct_id, period_id, 20_000_000_000_00, "admin");

    var stmt = try database.prepare("SELECT amount FROM ledger_budget_lines WHERE budget_id = ? AND account_id = ? AND period_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    try stmt.bindInt(2, acct_id);
    try stmt.bindInt(3, period_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 20_000_000_000_00), stmt.columnInt64(0));

    var count_stmt = try database.prepare("SELECT COUNT(*) FROM ledger_budget_lines WHERE budget_id = ?;");
    defer count_stmt.finalize();
    try count_stmt.bindInt(1, budget_id);
    _ = try count_stmt.step();
    try std.testing.expectEqual(@as(i32, 1), count_stmt.columnInt(0));
}

test "budgetVsActual with budget and actual data" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const cash_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const rev_id = try createTestAccount(database, book_id, "4000", "Revenue", .revenue);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    _ = try BudgetLine.set(database, budget_id, cash_id, period_id, 10_000_000_000_00, "admin");

    const entry_mod = @import("entry.zig");
    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 8_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 8_000_000_000_00, "PHP", 10_000_000_000, rev_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [8192]u8 = undefined;
    const csv_result = try budgetVsActual(database, budget_id, "2026-01-01", "2026-01-31", &buf, .csv);
    try std.testing.expect(std.mem.indexOf(u8, csv_result, "1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv_result, "10000000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv_result, "800000000000") != null);

    const json_result = try budgetVsActual(database, budget_id, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"account_number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"budget\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_result, "\"variance\":") != null);
}

test "budgetVsActual with no actual data shows zero actuals" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const cash_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    _ = try BudgetLine.set(database, budget_id, cash_id, period_id, 5_000_000_000_00, "admin");

    var buf: [8192]u8 = undefined;
    const result = try budgetVsActual(database, budget_id, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"actual_debit\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"actual_credit\":0") != null);
}

test "budgetVsActual with no budget data shows zero budget" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const cash_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const rev_id = try createTestAccount(database, book_id, "4000", "Revenue", .revenue);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    const entry_mod = @import("entry.zig");
    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 3_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 3_000_000_000_00, "PHP", 10_000_000_000, rev_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [8192]u8 = undefined;
    const result = try budgetVsActual(database, budget_id, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"budget\":0") != null);
}

test "Budget.delete cascades budget_lines" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const acct_id = try createTestAccount(database, book_id, "1000", "Cash", .asset);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    _ = try BudgetLine.set(database, budget_id, acct_id, period_id, 10_000_000_000_00, "admin");

    try Budget.delete(database, budget_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_budget_lines WHERE budget_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));

    var stmt2 = try database.prepare("SELECT COUNT(*) FROM ledger_budgets WHERE id = ?;");
    defer stmt2.finalize();
    try stmt2.bindInt(1, budget_id);
    _ = try stmt2.step();
    try std.testing.expectEqual(@as(i32, 0), stmt2.columnInt(0));
}

test "BudgetLine.set rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);
    const period_id = try createTestPeriod(database, book_id);
    const budget_id = try Budget.create(database, book_id, "FY2026", 2026, "admin");

    const book_mod = @import("book.zig");
    const book2_id = try book_mod.Book.create(database, "Other Book", "USD", 2, "admin");
    const other_acct = try createTestAccount(database, book2_id, "2000", "Other Cash", .asset);

    const result = BudgetLine.set(database, budget_id, other_acct, period_id, 100, "admin");
    try std.testing.expectError(error.CrossBookViolation, result);
}

test "Budget.create rejects archived book" {
    const database = try setupTestDb();
    defer database.close();
    const book_mod = @import("book.zig");
    const book_id = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    try book_mod.Book.archive(database, book_id, "admin");

    const result = Budget.create(database, book_id, "FY2026", 2026, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "Budget.create rejects duplicate name" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try createTestBook(database);

    _ = try Budget.create(database, book_id, "FY2026", 2026, "admin");
    const result = Budget.create(database, book_id, "FY2026", 2027, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "Budget.delete returns NotFound for nonexistent budget" {
    const database = try setupTestDb();
    defer database.close();

    const result = Budget.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "budgetVsActual returns NotFound for nonexistent budget" {
    const database = try setupTestDb();
    defer database.close();

    var buf: [1024]u8 = undefined;
    const result = budgetVsActual(database, 999, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expectError(error.NotFound, result);
}

test "BudgetStatus round-trips through toString and fromString" {
    const statuses = [_]BudgetStatus{ .draft, .approved, .closed };
    for (statuses) |s| {
        const str = s.toString();
        const parsed = BudgetStatus.fromString(str);
        try std.testing.expectEqual(s, parsed.?);
    }
    try std.testing.expect(BudgetStatus.fromString("invalid") == null);
}
