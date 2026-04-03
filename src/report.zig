const std = @import("std");
const db = @import("db.zig");

pub const ReportRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    account_type: [10]u8,
    account_type_len: usize,
    debit_balance: i64,
    credit_balance: i64,
};

pub const ReportResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []ReportRow,
    total_debits: i64,
    total_credits: i64,

    pub fn deinit(self: *ReportResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

fn verifyBookExists(database: db.Database, book_id: i64) !void {
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    if (stmt.columnInt(0) == 0) return error.NotFound;
}

fn copyText(dest: []u8, src: ?[]const u8) usize {
    const s = src orelse return 0;
    const len = @min(s.len, dest.len);
    @memcpy(dest[0..len], s[0..len]);
    return len;
}

pub fn trialBalance(database: db.Database, book_id: i64, as_of_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);

    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ReportRow){};

    var stmt = try database.prepare(
        \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
        \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
        \\FROM ledger_account_balances ab
        \\JOIN ledger_accounts a ON a.id = ab.account_id
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ?
        \\GROUP BY a.id
        \\ORDER BY a.number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, as_of_date);

    var total_debits: i64 = 0;
    var total_credits: i64 = 0;

    while (try stmt.step()) {
        var row: ReportRow = undefined;
        row.account_id = stmt.columnInt64(0);
        row.account_number_len = copyText(&row.account_number, stmt.columnText(1));
        row.account_name_len = copyText(&row.account_name, stmt.columnText(2));
        row.account_type_len = copyText(&row.account_type, stmt.columnText(3));

        const normal = stmt.columnText(4).?;
        const debit_sum = stmt.columnInt64(5);
        const credit_sum = stmt.columnInt64(6);

        if (std.mem.eql(u8, normal, "debit")) {
            row.debit_balance = debit_sum - credit_sum;
            row.credit_balance = 0;
            if (row.debit_balance < 0) {
                row.credit_balance = -row.debit_balance;
                row.debit_balance = 0;
            }
        } else {
            row.credit_balance = credit_sum - debit_sum;
            row.debit_balance = 0;
            if (row.credit_balance < 0) {
                row.debit_balance = -row.credit_balance;
                row.credit_balance = 0;
            }
        }

        total_debits += row.debit_balance;
        total_credits += row.credit_balance;
        try rows.append(allocator, row);
    }

    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    return result;
}

pub fn incomeStatement(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);

    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ReportRow){};

    var stmt = try database.prepare(
        \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
        \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
        \\FROM ledger_account_balances ab
        \\JOIN ledger_accounts a ON a.id = ab.account_id
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ?
        \\  AND p.start_date >= ? AND p.end_date <= ?
        \\  AND a.account_type IN ('revenue', 'expense')
        \\GROUP BY a.id
        \\ORDER BY a.number;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);

    var total_debits: i64 = 0;
    var total_credits: i64 = 0;

    while (try stmt.step()) {
        var row: ReportRow = undefined;
        row.account_id = stmt.columnInt64(0);
        row.account_number_len = copyText(&row.account_number, stmt.columnText(1));
        row.account_name_len = copyText(&row.account_name, stmt.columnText(2));
        row.account_type_len = copyText(&row.account_type, stmt.columnText(3));

        const debit_sum = stmt.columnInt64(5);
        const credit_sum = stmt.columnInt64(6);

        row.debit_balance = debit_sum;
        row.credit_balance = credit_sum;
        total_debits += debit_sum;
        total_credits += credit_sum;
        try rows.append(allocator, row);
    }

    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    return result;
}

pub fn balanceSheet(database: db.Database, book_id: i64, as_of_date: []const u8, fy_start_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);

    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ReportRow){};

    // Get A/L/E cumulative balances
    {
        var stmt = try database.prepare(
            \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
            \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.end_date <= ?
            \\  AND a.account_type IN ('asset', 'liability', 'equity')
            \\GROUP BY a.id
            \\ORDER BY a.number;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, as_of_date);

        while (try stmt.step()) {
            var row: ReportRow = undefined;
            row.account_id = stmt.columnInt64(0);
            row.account_number_len = copyText(&row.account_number, stmt.columnText(1));
            row.account_name_len = copyText(&row.account_name, stmt.columnText(2));
            row.account_type_len = copyText(&row.account_type, stmt.columnText(3));

            const normal = stmt.columnText(4).?;
            const debit_sum = stmt.columnInt64(5);
            const credit_sum = stmt.columnInt64(6);

            if (std.mem.eql(u8, normal, "debit")) {
                row.debit_balance = debit_sum - credit_sum;
                row.credit_balance = 0;
                if (row.debit_balance < 0) {
                    row.credit_balance = -row.debit_balance;
                    row.debit_balance = 0;
                }
            } else {
                row.credit_balance = credit_sum - debit_sum;
                row.debit_balance = 0;
                if (row.credit_balance < 0) {
                    row.debit_balance = -row.credit_balance;
                    row.credit_balance = 0;
                }
            }

            try rows.append(allocator, row);
        }
    }

    // Compute net income (revenue - expense) for the fiscal year
    var net_income: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT a.account_type, SUM(ab.debit_sum), SUM(ab.credit_sum)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ?
            \\  AND p.start_date >= ? AND p.end_date <= ?
            \\  AND a.account_type IN ('revenue', 'expense')
            \\GROUP BY a.account_type;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, fy_start_date);
        try stmt.bindText(3, as_of_date);

        while (try stmt.step()) {
            const acct_type = stmt.columnText(0).?;
            const debits = stmt.columnInt64(1);
            const credits = stmt.columnInt64(2);
            if (std.mem.eql(u8, acct_type, "revenue")) {
                net_income += credits - debits;
            } else {
                net_income -= debits - credits;
            }
        }
    }

    // Compute totals with net income injected into equity side
    var total_debits: i64 = 0;
    var total_credits: i64 = 0;
    for (rows.items[0..rows.items.len]) |row| {
        total_debits += row.debit_balance;
        total_credits += row.credit_balance;
    }
    // Net income goes to credit side (equity)
    if (net_income > 0) {
        total_credits += net_income;
    } else {
        total_debits += -net_income;
    }

    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    return result;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "Cost of Goods Sold", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

fn postEntry(database: db.Database, doc: []const u8, date: []const u8, period_id: i64, debits: []const struct { amount: i64, account_id: i64 }, credits: []const struct { amount: i64, account_id: i64 }) !void {
    const entry_id = try entry_mod.Entry.createDraft(database, 1, doc, date, date, null, period_id, null, "admin");
    var line_num: i32 = 1;
    for (debits) |d| {
        _ = try entry_mod.Entry.addLine(database, entry_id, line_num, d.amount, 0, "PHP", money.FX_RATE_SCALE, d.account_id, null, "admin");
        line_num += 1;
    }
    for (credits) |c| {
        _ = try entry_mod.Entry.addLine(database, entry_id, line_num, 0, c.amount, "PHP", money.FX_RATE_SCALE, c.account_id, null, "admin");
        line_num += 1;
    }
    try entry_mod.Entry.post(database, entry_id, "admin");
}

// ── Trial Balance tests ─────────────────────────────────────────

test "trial balance: single entry, two accounts" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "trial balance: multiple entries accumulate" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-01-16", 1, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    // Cash: 1500 debit, AP: 1500 credit
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_credits);
}

test "trial balance: voided entry excluded from balances" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

test "trial balance: all five account types" {
    const database = try setupTestDb();
    defer database.close();

    // Capital contribution: debit Cash, credit Capital
    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    // Sale: debit Cash, credit Revenue
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    // Purchase: debit COGS, credit AP
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    // 4 accounts with activity (Capital, Cash, Revenue, COGS, AP)
    try std.testing.expect(result.rows.len >= 4);
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "trial balance: empty book returns zero rows" {
    const database = try setupTestDb();
    defer database.close();

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

test "trial balance: nonexistent book returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = trialBalance(database, 999, "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

test "trial balance: debits always equal credits (accounting equation)" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-004", "2026-01-25", 1, &.{.{ .amount = 2_000_000_000_00, .account_id = 2 }}, &.{.{ .amount = 2_000_000_000_00, .account_id = 1 }});

    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

// ── Income Statement tests ──────────────────────────────────────

test "income statement: revenue minus expenses" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-002", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Revenue 5000, Expense 3000 => Net income in total_credits - total_debits
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), result.total_credits); // revenue
    try std.testing.expectEqual(@as(i64, 3_000_000_000_00), result.total_debits); // expenses
}

test "income statement: only revenue and expense accounts" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    // Only revenue account should appear (asset/equity excluded)
    for (result.rows) |row| {
        const acct_type = row.account_type[0..row.account_type_len];
        try std.testing.expect(std.mem.eql(u8, acct_type, "revenue") or std.mem.eql(u8, acct_type, "expense"));
    }
}

test "income statement: empty period returns zero" {
    const database = try setupTestDb();
    defer database.close();

    const result = try incomeStatement(database, 1, "2026-01-01", "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

// ── Balance Sheet tests ─────────────────────────────────────────

test "balance sheet: assets equal liabilities plus equity" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});
    try postEntry(database, "JE-003", "2026-01-20", 1, &.{.{ .amount = 3_000_000_000_00, .account_id = 5 }}, &.{.{ .amount = 3_000_000_000_00, .account_id = 2 }});

    const result = try balanceSheet(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // A = L + E (with net income injected)
    // Assets: Cash = 10000 + 5000 - 3000(via AP payment... wait, no)
    // Cash: debit 10000 + 5000 = 15000
    // AP: credit 3000
    // Capital: credit 10000
    // Net income: 5000 - 3000 = 2000
    // A (15000) = L (3000) + E (10000 + 2000) = 15000 ✓
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "balance sheet: includes net income from revenue minus expenses" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", "2026-01-10", 1, &.{.{ .amount = 10_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 10_000_000_000_00, .account_id = 3 }});
    try postEntry(database, "JE-002", "2026-01-15", 1, &.{.{ .amount = 5_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 5_000_000_000_00, .account_id = 4 }});

    const result = try balanceSheet(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    // Balance sheet should only have A/L/E rows (not R/E)
    // But net income (5000) should be reflected in the equity side
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "balance sheet: empty book" {
    const database = try setupTestDb();
    defer database.close();

    const result = try balanceSheet(database, 1, "2026-01-31", "2026-01-01");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    try std.testing.expectEqual(@as(i64, 0), result.total_credits);
}

// ── Multi-period tests ──────────────────────────────────────────

test "trial balance: spans multiple periods" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    // Trial balance as of Feb 28 should include both periods
    const result = try trialBalance(database, 1, "2026-02-28");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), result.total_debits);
}

test "trial balance: as_of_date before second period excludes it" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try postEntry(database, "JE-001", "2026-01-15", 1, &.{.{ .amount = 1_000_000_000_00, .account_id = 1 }}, &.{.{ .amount = 1_000_000_000_00, .account_id = 2 }});
    try postEntry(database, "JE-002", "2026-02-15", 2, &.{.{ .amount = 500_000_000_00, .account_id = 1 }}, &.{.{ .amount = 500_000_000_00, .account_id = 2 }});

    // Trial balance as of Jan 31 should only include Jan
    const result = try trialBalance(database, 1, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), result.total_debits);
}
