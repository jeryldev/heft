const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");

pub const SubledgerGroup = struct {
    const valid_types = [_][]const u8{ "customer", "supplier" };

    fn isValidType(t: []const u8) bool {
        for (valid_types) |vt| {
            if (std.mem.eql(u8, t, vt)) return true;
        }
        return false;
    }

    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, number_range_start, number_range_end, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?);
    ;

    pub fn create(database: db.Database, book_id: i64, name: []const u8, group_type: []const u8, group_number: i32, gl_account_id: i64, number_range_start: ?[]const u8, number_range_end: ?[]const u8, performed_by: []const u8) !i64 {
        if (!isValidType(group_type)) return error.InvalidInput;

        // Verify book exists and is active
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.InvalidInput;
        }

        // Verify GL account exists
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_accounts WHERE id = ? AND book_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, gl_account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, name);
        try stmt.bindText(2, group_type);
        try stmt.bindInt(3, @intCast(group_number));
        try stmt.bindInt(4, gl_account_id);
        if (number_range_start) |s| try stmt.bindText(5, s) else try stmt.bindNull(5);
        if (number_range_end) |e| try stmt.bindText(6, e) else try stmt.bindNull(6);
        try stmt.bindInt(7, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "subledger_group", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    pub fn isControlAccount(database: db.Database, account_id: i64) !bool {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_subledger_groups WHERE gl_account_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, account_id);
        _ = try stmt.step();
        return stmt.columnInt(0) > 0;
    }
};

pub const SubledgerAccount = struct {
    const valid_types = [_][]const u8{ "customer", "supplier", "both" };

    fn isValidType(t: []const u8) bool {
        for (valid_types) |vt| {
            if (std.mem.eql(u8, t, vt)) return true;
        }
        return false;
    }

    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES (?, ?, ?, ?, ?);
    ;

    pub fn create(database: db.Database, book_id: i64, number: []const u8, name: []const u8, account_type: []const u8, group_id: i64, performed_by: []const u8) !i64 {
        if (!isValidType(account_type)) return error.InvalidInput;

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, number);
        try stmt.bindText(2, name);
        try stmt.bindText(3, account_type);
        try stmt.bindInt(4, group_id);
        try stmt.bindInt(5, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "subledger_account", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }
};

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
    _ = try account_mod.Account.create(database, 1, "1200", "Accounts Receivable", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

// ── SubledgerGroup tests ────────────────────────────────────────

test "create subledger group returns id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try SubledgerGroup.create(database, 1, "Trade Customers", "customer", 1, 2, null, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create subledger group with number range" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, "C0001", "C9999", "admin");

    var stmt = try database.prepare("SELECT number_range_start, number_range_end FROM ledger_subledger_groups WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("C0001", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("C9999", stmt.columnText(1).?);
}

test "create subledger group writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'subledger_group';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("subledger_group", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
}

test "create subledger group rejects invalid type" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerGroup.create(database, 1, "Bad", "vendor", 1, 2, null, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create subledger group rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerGroup.create(database, 999, "Customers", "customer", 1, 2, null, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "create subledger group rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerGroup.create(database, 1, "Customers", "customer", 1, 999, null, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "create subledger group rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "isControlAccount returns true for linked account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    try std.testing.expect(try SubledgerGroup.isControlAccount(database, 2));
}

test "isControlAccount returns false for unlinked account" {
    const database = try setupTestDb();
    defer database.close();

    try std.testing.expect(!try SubledgerGroup.isControlAccount(database, 1));
}

// ── SubledgerAccount tests ──────────────────────────────────────

test "create subledger account returns id" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan dela Cruz", "customer", 1, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create subledger account writes audit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'subledger_account';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("subledger_account", stmt.columnText(0).?);
}

test "create subledger account rejects invalid type" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const result = SubledgerAccount.create(database, 1, "C0001", "Bad", "vendor", 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create subledger account rejects duplicate number" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    const result = SubledgerAccount.create(database, 1, "C0001", "Pedro", "customer", 1, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "create subledger account type both accepted" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Dual Corp", "both", 1, "admin");
    try std.testing.expect(id > 0);
}

// ── Control account enforcement tests ───────────────────────────

test "posting to control account without counterparty rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    const result = entry_mod.Entry.post(database, 1, "admin");
    try std.testing.expectError(error.MissingCounterparty, result);
}

test "posting to control account with counterparty succeeds" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");

    // Add AR line with counterparty via raw SQL (addLine doesn't expose counterparty_id yet)
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);
    );

    try entry_mod.Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "posting to non-control account with counterparty rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");

    // Cash (id=1) is NOT a control account but has counterparty
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 1, 1, 1);
    );

    const result = entry_mod.Entry.post(database, 1, "admin");
    try std.testing.expectError(error.InvalidCounterparty, result);
}
