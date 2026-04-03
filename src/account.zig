const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");

pub const AccountType = enum {
    asset,
    liability,
    equity,
    revenue,
    expense,

    pub fn toString(self: AccountType) []const u8 {
        return @tagName(self);
    }
};

pub const NormalBalance = enum {
    debit,
    credit,

    pub fn toString(self: NormalBalance) []const u8 {
        return @tagName(self);
    }
};

pub const Account = struct {
    pub fn deriveNormalBalance(account_type: AccountType, is_contra: bool) NormalBalance {
        const base: NormalBalance = switch (account_type) {
            .asset, .expense => .debit,
            .liability, .equity, .revenue => .credit,
        };
        if (is_contra) {
            return if (base == .debit) .credit else .debit;
        }
        return base;
    }

    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, is_contra, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?);
    ;

    pub fn create(database: db.Database, book_id: i64, number: []const u8, name: []const u8, account_type: AccountType, is_contra: bool, performed_by: []const u8) !i64 {
        if (number.len == 0 or number.len > 50) return error.InvalidInput;
        if (name.len == 0) return error.InvalidInput;

        // Verify book exists
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        const normal_balance = deriveNormalBalance(account_type, is_contra);

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, number);
        try stmt.bindText(2, name);
        try stmt.bindText(3, account_type.toString());
        try stmt.bindText(4, normal_balance.toString());
        try stmt.bindInt(5, if (is_contra) 1 else 0);
        try stmt.bindInt(6, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "account", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    const valid_statuses = [_][]const u8{ "active", "inactive", "archived" };

    fn isValidStatus(status: []const u8) bool {
        for (valid_statuses) |s| {
            if (std.mem.eql(u8, status, s)) return true;
        }
        return false;
    }

    pub fn updateStatus(database: db.Database, account_id: i64, new_status: []const u8, performed_by: []const u8) !void {
        if (!isValidStatus(new_status)) return error.InvalidInput;

        // Fetch current status and book_id
        var old_status_buf: [20]u8 = undefined;
        var old_status_len: usize = 0;
        var acct_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            @memcpy(old_status_buf[0..old.len], old);
            old_status_len = old.len;
            acct_book_id = stmt.columnInt64(1);
        }
        const old_status = old_status_buf[0..old_status_len];

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("UPDATE ledger_accounts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_status);
            try stmt.bindInt(2, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "account", account_id, "update", "status", old_status, new_status, performed_by, acct_book_id);

        try database.commit();
    }
};

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book = @import("book.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book.Book.create(database, "Test", "PHP", 2, "admin");
    return database;
}

// ── deriveNormalBalance tests (all 10 combinations) ─────────────

test "deriveNormalBalance: asset non-contra = debit" {
    try std.testing.expectEqual(NormalBalance.debit, Account.deriveNormalBalance(.asset, false));
}

test "deriveNormalBalance: asset contra = credit" {
    try std.testing.expectEqual(NormalBalance.credit, Account.deriveNormalBalance(.asset, true));
}

test "deriveNormalBalance: liability non-contra = credit" {
    try std.testing.expectEqual(NormalBalance.credit, Account.deriveNormalBalance(.liability, false));
}

test "deriveNormalBalance: liability contra = debit" {
    try std.testing.expectEqual(NormalBalance.debit, Account.deriveNormalBalance(.liability, true));
}

test "deriveNormalBalance: equity non-contra = credit" {
    try std.testing.expectEqual(NormalBalance.credit, Account.deriveNormalBalance(.equity, false));
}

test "deriveNormalBalance: equity contra = debit" {
    try std.testing.expectEqual(NormalBalance.debit, Account.deriveNormalBalance(.equity, true));
}

test "deriveNormalBalance: revenue non-contra = credit" {
    try std.testing.expectEqual(NormalBalance.credit, Account.deriveNormalBalance(.revenue, false));
}

test "deriveNormalBalance: revenue contra = debit" {
    try std.testing.expectEqual(NormalBalance.debit, Account.deriveNormalBalance(.revenue, true));
}

test "deriveNormalBalance: expense non-contra = debit" {
    try std.testing.expectEqual(NormalBalance.debit, Account.deriveNormalBalance(.expense, false));
}

test "deriveNormalBalance: expense contra = credit" {
    try std.testing.expectEqual(NormalBalance.credit, Account.deriveNormalBalance(.expense, true));
}

// ── create account tests ────────────────────────────────────────

test "create account returns auto-generated id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create account stores correct fields with derived normal_balance" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var stmt = try database.prepare("SELECT number, name, account_type, normal_balance, is_contra, status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("1000", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Cash", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("asset", stmt.columnText(2).?);
    try std.testing.expectEqualStrings("debit", stmt.columnText(3).?);
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(4));
    try std.testing.expectEqualStrings("active", stmt.columnText(5).?);
}

test "create contra account derives flipped normal_balance" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1900", "Accum Depreciation", .asset, true, "admin");

    var stmt = try database.prepare("SELECT normal_balance, is_contra FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("credit", stmt.columnText(0).?);
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(1));
}

test "create all five account types" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try Account.create(database, 1, "4000", "Sales", .revenue, false, "admin");
    _ = try Account.create(database, 1, "5000", "COGS", .expense, false, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_accounts WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 5), stmt.columnInt(0));
}

test "create account writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'account';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("account", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
}

test "create account rejects empty number" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.create(database, 1, "", "Cash", .asset, false, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create account rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.create(database, 1, "1000", "", .asset, false, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create account rejects number over 50 chars" {
    const database = try setupTestDb();
    defer database.close();

    const long_number = "123456789012345678901234567890123456789012345678901";
    const result = Account.create(database, 1, long_number, "Bad", .asset, false, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create account rejects duplicate number in same book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    const result = Account.create(database, 1, "1000", "Also Cash", .asset, false, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "create account allows same number in different books" {
    const database = try setupTestDb();
    defer database.close();

    _ = try book.Book.create(database, "Book B", "USD", 2, "admin");
    _ = try Account.create(database, 1, "1000", "Cash PHP", .asset, false, "admin");
    _ = try Account.create(database, 2, "1000", "Cash USD", .asset, false, "admin");
}

test "create account rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.create(database, 999, "1000", "Cash", .asset, false, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── updateStatus tests ──────────────────────────────────────────

test "updateStatus changes account to inactive" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, "inactive", "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("inactive", stmt.columnText(0).?);
}

test "updateStatus reactivates inactive account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, "inactive", "admin");
    try Account.updateStatus(database, 1, "active", "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("active", stmt.columnText(0).?);
}

test "updateStatus writes audit log with old and new values" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, "inactive", "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'account' AND action = 'update';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("status", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("active", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("inactive", stmt.columnText(2).?);
}

test "updateStatus rejects invalid status" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    const result = Account.updateStatus(database, 1, "deleted", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateStatus rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.updateStatus(database, 999, "inactive", "admin");
    try std.testing.expectError(error.NotFound, result);
}
