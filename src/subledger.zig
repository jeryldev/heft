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

        {
            var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = ? AND book_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, gl_account_id);
            try stmt.bindInt(2, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (!std.mem.eql(u8, stmt.columnText(0).?, "active")) return error.AccountInactive;
        }

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

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn updateName(database: db.Database, group_id: i64, new_name: []const u8, performed_by: []const u8) !void {
        if (new_name.len == 0) return error.InvalidInput;

        var old_name_buf: [256]u8 = undefined;
        var old_name_len: usize = 0;
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT name, book_id FROM ledger_subledger_groups WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, group_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            old_name_len = @min(old.len, old_name_buf.len);
            @memcpy(old_name_buf[0..old_name_len], old[0..old_name_len]);
            book_id = stmt.columnInt64(1);
        }

        // Verify book is active
        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_subledger_groups SET name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_name);
            try stmt.bindInt(2, group_id);
            _ = try stmt.step();
        }

        try audit.log(database, "subledger_group", group_id, "update", "name", old_name_buf[0..old_name_len], new_name, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn delete(database: db.Database, group_id: i64, performed_by: []const u8) !void {
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_subledger_groups WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, group_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        // Verify book is active
        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        // Reject if group has subledger accounts
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_subledger_accounts WHERE group_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, group_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_subledger_groups WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, group_id);
            _ = try stmt.step();
        }

        try audit.log(database, "subledger_group", group_id, "delete", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn isControlAccount(database: db.Database, account_id: i64) !bool {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_subledger_groups WHERE gl_account_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, account_id);
        _ = try stmt.step();
        return stmt.columnInt(0) > 0;
    }
};

pub const SubledgerAccountStatus = enum {
    active,
    inactive,
    archived,

    pub fn canTransitionTo(self: SubledgerAccountStatus, target: SubledgerAccountStatus) bool {
        return switch (self) {
            .active => target == .inactive or target == .archived,
            .inactive => target == .active or target == .archived,
            .archived => false,
        };
    }

    pub fn toString(self: SubledgerAccountStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?SubledgerAccountStatus {
        const map = .{
            .{ "active", SubledgerAccountStatus.active },
            .{ "inactive", SubledgerAccountStatus.inactive },
            .{ "archived", SubledgerAccountStatus.archived },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
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

        // Verify group exists and belongs to same book
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_subledger_groups WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, group_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (stmt.columnInt64(0) != book_id) return error.CrossBookViolation;
        }

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

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn updateName(database: db.Database, account_id: i64, new_name: []const u8, performed_by: []const u8) !void {
        if (new_name.len == 0) return error.InvalidInput;

        var old_name_buf: [256]u8 = undefined;
        var old_name_len: usize = 0;
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT name, book_id FROM ledger_subledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            old_name_len = @min(old.len, old_name_buf.len);
            @memcpy(old_name_buf[0..old_name_len], old[0..old_name_len]);
            book_id = stmt.columnInt64(1);
        }

        // Verify book is active
        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_subledger_accounts SET name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_name);
            try stmt.bindInt(2, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "subledger_account", account_id, "update", "name", old_name_buf[0..old_name_len], new_name, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn delete(database: db.Database, account_id: i64, performed_by: []const u8) !void {
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_subledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        // Verify book is active
        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        // Reject if entry lines reference this counterparty
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE counterparty_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_subledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "subledger_account", account_id, "delete", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn updateStatus(database: db.Database, account_id: i64, target: SubledgerAccountStatus, performed_by: []const u8) !void {
        var current: SubledgerAccountStatus = undefined;
        var acct_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_subledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            current = SubledgerAccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            acct_book_id = stmt.columnInt64(1);
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, acct_book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        if (!current.canTransitionTo(target)) return error.InvalidTransition;

        {
            var stmt = try database.prepare("UPDATE ledger_subledger_accounts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, target.toString());
            try stmt.bindInt(2, account_id);
            _ = try stmt.step();
        }

        try audit.log(database, "subledger_account", account_id, "update", "status", current.toString(), target.toString(), performed_by, acct_book_id);

        if (owns_txn) try database.commit();
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
    try std.testing.expectError(error.BookArchived, result);
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
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    const result = entry_mod.Entry.post(database, 1, "admin");
    try std.testing.expectError(error.MissingCounterparty, result);
}

test "posting to control account with counterparty succeeds" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

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
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    // Cash (id=1) is NOT a control account but has counterparty
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 1, 1, 1);
    );

    const result = entry_mod.Entry.post(database, 1, "admin");
    try std.testing.expectError(error.InvalidCounterparty, result);
}

test "duplicate subledger group rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const result = SubledgerGroup.create(database, 1, "Also Customers", "customer", 1, 2, null, null, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "updateStatus active to inactive" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, id, .inactive, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("inactive", stmt.columnText(0).?);
}

test "updateStatus inactive to active" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, id, .inactive, "admin");
    try SubledgerAccount.updateStatus(database, id, .active, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("active", stmt.columnText(0).?);
}

test "updateStatus active to archived" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, id, .archived, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "updateStatus archived to anything rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, id, .archived, "admin");

    const r1 = SubledgerAccount.updateStatus(database, id, .active, "admin");
    try std.testing.expectError(error.InvalidTransition, r1);
    const r2 = SubledgerAccount.updateStatus(database, id, .inactive, "admin");
    try std.testing.expectError(error.InvalidTransition, r2);
}

test "updateStatus nonexistent returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerAccount.updateStatus(database, 999, .inactive, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "updateStatus writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const id = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, id, .inactive, "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'subledger_account' AND action = 'update';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("status", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("active", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("inactive", stmt.columnText(2).?);
}

test "post rejects inactive counterparty" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const cid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.updateStatus(database, cid, .inactive, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);
    );

    const result = entry_mod.Entry.post(database, 1, "admin");
    try std.testing.expectError(error.AccountInactive, result);
}

// ── Real-world business scenario tests ──────────────────────────

fn setupArApDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Business", "PHP", 2, "admin");
    // Accounts: 1=Cash, 2=AR, 3=AP, 4=Revenue, 5=COGS
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "Accounts Receivable", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Sales Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4900", "Sales Returns", .revenue, true, "admin");
    _ = try account_mod.Account.create(database, 1, "5900", "Purchase Discounts", .expense, true, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    // Customer subledger on AR
    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Customer A", "customer", 1, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0002", "Customer B", "customer", 1, "admin");
    // Supplier subledger on AP
    _ = try SubledgerGroup.create(database, 1, "Suppliers", "supplier", 1, 3, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "S0001", "Supplier X", "supplier", 2, "admin");
    _ = try SubledgerAccount.create(database, 1, "S0002", "Supplier Y", "supplier", 2, "admin");
    return database;
}

fn postWithCounterparty(database: db.Database, doc: []const u8, debit_acct: i64, debit_amt: i64, debit_cp: ?i64, credit_acct: i64, credit_amt: i64, credit_cp: ?i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, 1, doc, "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, "PHP", money.FX_RATE_SCALE, debit_acct, null, null, "admin");

    // For credit line, use raw SQL if counterparty needed
    if (credit_cp) |cp| {
        var stmt = try database.prepare(
            \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
            \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
            \\VALUES (2, 0, ?, 'PHP', 10000000000, ?, ?, ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, credit_amt);
        try stmt.bindInt(2, credit_acct);
        try stmt.bindInt(3, eid);
        try stmt.bindInt(4, cp);
        _ = try stmt.step();
    } else {
        _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, "PHP", money.FX_RATE_SCALE, credit_acct, null, null, "admin");
    }

    // Handle debit counterparty if needed
    if (debit_cp) |cp| {
        var stmt = try database.prepare(
            "UPDATE ledger_entry_lines SET counterparty_id = ? WHERE entry_id = ? AND line_number = 1;",
        );
        defer stmt.finalize();
        try stmt.bindInt(1, cp);
        try stmt.bindInt(2, eid);
        _ = try stmt.step();
    }

    try entry_mod.Entry.post(database, eid, "admin");
}

test "AR: customer invoice — debit AR with counterparty, credit revenue" {
    const database = try setupArApDb();
    defer database.close();

    // Debit AR (counterparty=Customer A), Credit Revenue
    try postWithCounterparty(database, "INV-001", 2, 5_000_000_000_00, 1, 4, 5_000_000_000_00, null);

    // Verify balance cache: AR has 5000 debit
    var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 5_000_000_000_00), stmt.columnInt64(0));
}

test "AR: customer payment — debit cash, credit AR with counterparty" {
    const database = try setupArApDb();
    defer database.close();

    // Invoice
    try postWithCounterparty(database, "INV-001", 2, 5_000_000_000_00, 1, 4, 5_000_000_000_00, null);
    // Payment
    try postWithCounterparty(database, "PMT-001", 1, 5_000_000_000_00, null, 2, 5_000_000_000_00, 1);

    // AR should be zero (invoice 5000 debit - payment 5000 credit)
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
}

test "AP: supplier invoice — debit expense, credit AP with counterparty" {
    const database = try setupArApDb();
    defer database.close();

    // Debit COGS, Credit AP (counterparty=Supplier X)
    try postWithCounterparty(database, "BILL-001", 5, 3_000_000_000_00, null, 3, 3_000_000_000_00, 3);

    var stmt = try database.prepare("SELECT credit_sum FROM ledger_account_balances WHERE account_id = 3;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 3_000_000_000_00), stmt.columnInt64(0));
}

test "AP: supplier payment — debit AP with counterparty, credit cash" {
    const database = try setupArApDb();
    defer database.close();

    // Invoice
    try postWithCounterparty(database, "BILL-001", 5, 3_000_000_000_00, null, 3, 3_000_000_000_00, 3);
    // Payment
    try postWithCounterparty(database, "PMT-001", 3, 3_000_000_000_00, 3, 1, 3_000_000_000_00, null);

    // AP should be zero
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 3;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
}

test "multiple customers in single deposit entry" {
    const database = try setupArApDb();
    defer database.close();

    // First invoice two customers
    try postWithCounterparty(database, "INV-001", 2, 3_000_000_000_00, 1, 4, 3_000_000_000_00, null);
    try postWithCounterparty(database, "INV-002", 2, 5_000_000_000_00, 2, 4, 5_000_000_000_00, null);

    // Deposit batch: cash debit 8000, AR credit Customer A 3000 + Customer B 5000
    {
        const eid = try entry_mod.Entry.createDraft(database, 1, "DEP-001", "2026-01-20", "2026-01-20", null, 1, null, "admin");
        _ = try entry_mod.Entry.addLine(database, eid, 1, 8_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

        // Customer A credit
        {
            var stmt = try database.prepare(
                \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
                \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
                \\VALUES (2, 0, ?, 'PHP', 10000000000, 2, ?, ?);
            );
            defer stmt.finalize();
            try stmt.bindInt(1, 3_000_000_000_00);
            try stmt.bindInt(2, eid);
            try stmt.bindInt(3, 1);
            _ = try stmt.step();
        }
        // Customer B credit
        {
            var stmt = try database.prepare(
                \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
                \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
                \\VALUES (3, 0, ?, 'PHP', 10000000000, 2, ?, ?);
            );
            defer stmt.finalize();
            try stmt.bindInt(1, 5_000_000_000_00);
            try stmt.bindInt(2, eid);
            try stmt.bindInt(3, 2);
            _ = try stmt.step();
        }
        try entry_mod.Entry.post(database, eid, "admin");
    }

    // AR should be zero for both customers (invoiced then fully paid)
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
}

test "entry with both control and non-control lines" {
    const database = try setupArApDb();
    defer database.close();

    // Sale: Debit AR (control, counterparty), Credit Revenue (non-control)
    // This is the most common pattern — one line needs counterparty, one doesn't
    try postWithCounterparty(database, "INV-001", 2, 1_000_000_000_00, 1, 4, 1_000_000_000_00, null);

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "counterparty appears in transaction_history view with details" {
    const database = try setupArApDb();
    defer database.close();

    try postWithCounterparty(database, "INV-001", 2, 1_000_000_000_00, 1, 4, 1_000_000_000_00, null);

    var stmt = try database.prepare(
        \\SELECT counterparty_number, counterparty_name, subledger_group_name
        \\FROM ledger_transaction_history WHERE counterparty_id IS NOT NULL;
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("C0001", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Customer A", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("Customers", stmt.columnText(2).?);
}

test "transaction_history shows counterparty info for subledger entries" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan dela Cruz", "customer", 1, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);
    );

    try entry_mod.Entry.post(database, 1, "admin");

    // Verify counterparty info in transaction_history view
    var stmt = try database.prepare(
        \\SELECT counterparty_number, counterparty_name, subledger_group_name
        \\FROM ledger_transaction_history WHERE counterparty_id IS NOT NULL;
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("C0001", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Juan dela Cruz", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("Customers", stmt.columnText(2).?);
}

// ── SubledgerGroup.updateName tests ────────────────────────────

test "SubledgerGroup.updateName changes name" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Old Name", "customer", 1, 2, null, null, "admin");
    try SubledgerGroup.updateName(database, gid, "New Name", "admin");

    var stmt = try database.prepare("SELECT name FROM ledger_subledger_groups WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, gid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New Name", stmt.columnText(0).?);
}

test "SubledgerGroup.updateName writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Old", "customer", 1, 2, null, null, "admin");
    try SubledgerGroup.updateName(database, gid, "New", "admin");

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE entity_type = 'subledger_group' AND field_changed = 'name';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Old", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("New", stmt.columnText(1).?);
}

test "SubledgerGroup.updateName rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const result = SubledgerGroup.updateName(database, gid, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "SubledgerGroup.updateName rejects nonexistent group" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerGroup.updateName(database, 999, "New", "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── SubledgerGroup.delete tests ────────────────────────────────

test "SubledgerGroup.delete removes empty group" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    try SubledgerGroup.delete(database, gid, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_subledger_groups WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, gid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "SubledgerGroup.delete writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    try SubledgerGroup.delete(database, gid, "admin");

    var stmt = try database.prepare("SELECT action FROM ledger_audit_log WHERE entity_type = 'subledger_group' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("delete", stmt.columnText(0).?);
}

test "SubledgerGroup.delete rejects group with accounts" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", gid, "admin");

    const result = SubledgerGroup.delete(database, gid, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "SubledgerGroup.delete rejects nonexistent group" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerGroup.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── SubledgerAccount.updateName tests ──────────────────────────

test "SubledgerAccount.updateName changes name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Old Name", "customer", 1, "admin");
    try SubledgerAccount.updateName(database, aid, "New Name", "admin");

    var stmt = try database.prepare("SELECT name FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, aid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New Name", stmt.columnText(0).?);
}

test "SubledgerAccount.updateName writes audit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Old", "customer", 1, "admin");
    try SubledgerAccount.updateName(database, aid, "New", "admin");

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE entity_type = 'subledger_account' AND field_changed = 'name';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Old", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("New", stmt.columnText(1).?);
}

test "SubledgerAccount.updateName rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    const result = SubledgerAccount.updateName(database, aid, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "SubledgerAccount.updateName rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerAccount.updateName(database, 999, "New", "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── SubledgerAccount.delete tests ──────────────────────────────

test "SubledgerAccount.delete removes unreferenced account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.delete(database, aid, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_subledger_accounts WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, aid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "SubledgerAccount.delete writes audit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");
    try SubledgerAccount.delete(database, aid, "admin");

    var stmt = try database.prepare("SELECT action FROM ledger_audit_log WHERE entity_type = 'subledger_account' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("delete", stmt.columnText(0).?);
}

test "SubledgerAccount.delete rejects referenced counterparty" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    // Create entry line referencing this counterparty via raw SQL
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  transaction_currency, fx_rate, account_id, entry_id, counterparty_id)
        \\VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);
    );
    try entry_mod.Entry.post(database, 1, "admin");

    const result = SubledgerAccount.delete(database, aid, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "SubledgerAccount.delete rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerAccount.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "SubledgerGroup.updateName on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = SubledgerGroup.updateName(database, gid, "New Name", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "SubledgerAccount.create with invalid group_id returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "SubledgerAccount.create with cross-book group returns CrossBookViolation" {
    const database = try setupTestDb();
    defer database.close();

    // Create a second book with its own account
    _ = try book_mod.Book.create(database, "Book 2", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "1000", "Cash B2", .asset, false, "admin");

    // Create group in book 1
    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");

    // Try to create subledger account in book 2 using group from book 1
    const result = SubledgerAccount.create(database, 2, "C0001", "Juan", "customer", 1, "admin");
    try std.testing.expectError(error.CrossBookViolation, result);
}

test "SubledgerAccount.updateName on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = SubledgerAccount.updateName(database, aid, "New Name", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "SubledgerAccount.delete on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const aid = try SubledgerAccount.create(database, 1, "C0001", "Juan", "customer", 1, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = SubledgerAccount.delete(database, aid, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "SubledgerGroup.delete on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const gid = try SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = SubledgerGroup.delete(database, gid, "admin");
    try std.testing.expectError(error.BookArchived, result);
}
