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

    pub fn fromString(s: []const u8) ?AccountType {
        const map = .{
            .{ "asset", AccountType.asset },
            .{ "liability", AccountType.liability },
            .{ "equity", AccountType.equity },
            .{ "revenue", AccountType.revenue },
            .{ "expense", AccountType.expense },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const NormalBalance = enum {
    debit,
    credit,

    pub fn toString(self: NormalBalance) []const u8 {
        return @tagName(self);
    }
};

pub const AccountStatus = enum {
    active,
    inactive,
    archived,

    pub fn canTransitionTo(self: AccountStatus, target: AccountStatus) bool {
        return switch (self) {
            .active => target == .inactive or target == .archived,
            .inactive => target == .active or target == .archived,
            .archived => false,
        };
    }

    pub fn toString(self: AccountStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?AccountStatus {
        const map = .{
            .{ "active", AccountStatus.active },
            .{ "inactive", AccountStatus.inactive },
            .{ "archived", AccountStatus.archived },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Account = struct {
    const max_number_len = 50;

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
        if (number.len == 0 or number.len > max_number_len) return error.InvalidInput;
        if (name.len == 0) return error.InvalidInput;

        const normal_balance = deriveNormalBalance(account_type, is_contra);

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        // Verify book exists and is active
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, number);
        try stmt.bindText(2, name);
        try stmt.bindText(3, account_type.toString());
        try stmt.bindText(4, normal_balance.toString());
        try stmt.bindInt(5, if (is_contra) 1 else 0);
        try stmt.bindInt(6, book_id);

        // Intentional: after FK validation above, the only realistic step failure
        // is the UNIQUE(book_id, number) constraint, so catch-all maps to DuplicateNumber
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "account", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn updateName(database: db.Database, account_id: i64, new_name: []const u8, performed_by: []const u8) !void {
        if (new_name.len == 0) return error.InvalidInput;

        var old_name_buf: [256]u8 = undefined;
        var old_name_len: usize = 0;
        var acct_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT name, book_id, status FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            old_name_len = @min(old.len, old_name_buf.len);
            @memcpy(old_name_buf[0..old_name_len], old[0..old_name_len]);
            acct_book_id = stmt.columnInt64(1);
            const status = AccountStatus.fromString(stmt.columnText(2).?) orelse return error.InvalidInput;
            if (status == .archived) return error.InvalidInput;
        }

        {
            const book_mod = @import("book.zig");
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, acct_book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            const bstatus = book_mod.BookStatus.fromString(bs_stmt.columnText(0).?) orelse return error.InvalidInput;
            if (bstatus == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_accounts SET name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_name);
            try stmt.bindInt(2, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "account", account_id, "update", "name", old_name_buf[0..old_name_len], new_name, performed_by, acct_book_id);

        if (owns_txn) try database.commit();
    }

    pub fn updateStatus(database: db.Database, account_id: i64, target: AccountStatus, performed_by: []const u8) !void {
        var current: AccountStatus = undefined;
        var acct_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        // Fetch current status and book_id
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            current = AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            acct_book_id = stmt.columnInt64(1);
        }

        {
            const book_mod = @import("book.zig");
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, acct_book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            const bstatus = book_mod.BookStatus.fromString(bs_stmt.columnText(0).?) orelse return error.InvalidInput;
            if (bstatus == .archived) return error.BookArchived;
        }

        if (!current.canTransitionTo(target)) return error.InvalidTransition;

        {
            var stmt = try database.prepare("UPDATE ledger_accounts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, target.toString());
            try stmt.bindInt(2, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "account", account_id, "update", "status", current.toString(), target.toString(), performed_by, acct_book_id);

        if (owns_txn) try database.commit();
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

test "create account rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    try book.Book.archive(database, 1, "admin");

    const result = Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

// ── updateStatus tests ──────────────────────────────────────────

test "updateStatus changes account to inactive" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .inactive, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("inactive", stmt.columnText(0).?);
}

test "updateStatus reactivates inactive account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .inactive, "admin");
    try Account.updateStatus(database, 1, .active, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("active", stmt.columnText(0).?);
}

test "updateStatus writes audit log with old and new values" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .inactive, "admin");

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
    // "deleted" is not a valid AccountStatus — but updateStatus now takes enum,
    // so invalid strings are caught at the C ABI layer (fromString returns null).
    // Test the enum-level guard instead: archived -> active
    try Account.updateStatus(database, 1, .archived, "admin");
    const result = Account.updateStatus(database, 1, .active, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "updateStatus rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.updateStatus(database, 999, .inactive, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── updateName tests ───────────────────────────────────────────

test "updateName changes account name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateName(database, 1, "Petty Cash", "admin");

    var stmt = try database.prepare("SELECT name FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Petty Cash", stmt.columnText(0).?);
}

test "updateName writes audit with old and new values" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateName(database, 1, "Petty Cash", "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'account' AND field_changed = 'name';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("name", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Cash", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("Petty Cash", stmt.columnText(2).?);
}

test "updateName rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    const result = Account.updateName(database, 1, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateName rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = Account.updateName(database, 999, "New", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "AccountType.fromString with invalid string returns null" {
    try std.testing.expect(AccountType.fromString("invalid") == null);
    try std.testing.expect(AccountType.fromString("") == null);
    try std.testing.expect(AccountType.fromString("ASSET") == null);
}

test "AccountStatus.fromString with invalid string returns null" {
    try std.testing.expect(AccountStatus.fromString("invalid") == null);
    try std.testing.expect(AccountStatus.fromString("") == null);
    try std.testing.expect(AccountStatus.fromString("ACTIVE") == null);
}

test "updateStatus active to archived" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .archived, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "updateStatus inactive to archived" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .inactive, "admin");
    try Account.updateStatus(database, 1, .archived, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "updateStatus archived to inactive rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .archived, "admin");
    const result = Account.updateStatus(database, 1, .inactive, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "updateName rejects archived account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try Account.updateStatus(database, 1, .archived, "admin");
    const result = Account.updateName(database, 1, "New", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}
