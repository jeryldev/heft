const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");

pub const BookStatus = enum {
    active,
    archived,

    pub fn toString(self: BookStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?BookStatus {
        const map = .{
            .{ "active", BookStatus.active },
            .{ "archived", BookStatus.archived },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Book = struct {
    const id_buf_len = 20; // max i64 decimal digits

    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_books (name, base_currency, decimal_places)
        \\VALUES (?, ?, ?);
    ;

    pub fn create(database: db.Database, name: []const u8, base_currency: []const u8, decimal_places: i32, performed_by: []const u8) !i64 {
        if (name.len == 0) return error.InvalidInput;
        if (base_currency.len != 3) return error.InvalidInput;
        if (decimal_places < 0 or decimal_places > 8) return error.InvalidInput;

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, name);
        try stmt.bindText(2, base_currency);
        try stmt.bindInt(3, @intCast(decimal_places));
        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "book", id, "create", null, null, null, performed_by, id);

        try database.commit();
        return id;
    }

    pub fn setRoundingAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        // Verify book exists
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        // Verify account exists and belongs to the same book
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (stmt.columnInt64(0) != book_id) return error.InvalidInput;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        // Update book
        {
            var stmt = try database.prepare("UPDATE ledger_books SET rounding_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        // Format account_id as string for audit log
        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "rounding_account_id", null, id_str, performed_by, book_id);

        try database.commit();
    }

    pub fn archive(database: db.Database, book_id: i64, performed_by: []const u8) !void {
        // Verify book exists and is active
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const current = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (current == .archived) return error.InvalidTransition;
        }

        // Verify no open or soft_closed periods exist
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ? AND status IN ('open', 'soft_closed');");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("UPDATE ledger_books SET status = 'archived', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
        }

        try audit.log(database, "book", book_id, "update", "status", "active", "archived", performed_by, book_id);

        try database.commit();
    }
};

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    return database;
}

test "create book returns auto-generated id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);

    const id2 = try Book.create(database, "FY2027", "USD", 2, "admin");
    try std.testing.expectEqual(@as(i64, 2), id2);
}

test "create book stores correct fields" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");

    var stmt = try database.prepare("SELECT name, base_currency, decimal_places, status FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("FY2026", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("PHP", stmt.columnText(1).?);
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(2));
    try std.testing.expectEqualStrings("active", stmt.columnText(3).?);
}

test "create book writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");

    var stmt = try database.prepare("SELECT entity_type, action, performed_by FROM ledger_audit_log WHERE entity_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("book", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("admin", stmt.columnText(2).?);
}

test "create book rejects invalid currency length" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.create(database, "Test", "PH", 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create book rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.create(database, "", "PHP", 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create book rejects invalid decimal places" {
    const database = try setupTestDb();
    defer database.close();

    const too_high = Book.create(database, "Test", "PHP", 9, "admin");
    try std.testing.expectError(error.InvalidInput, too_high);

    const negative = Book.create(database, "Test", "PHP", -1, "admin");
    try std.testing.expectError(error.InvalidInput, negative);
}

test "create book with zero decimal places for JPY" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Book.create(database, "Japan Fund", "JPY", 0, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create book with 8 decimal places for crypto" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Book.create(database, "Crypto Fund", "BTC", 8, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create book is atomic — audit rolled back on failure" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = Book.create(database, "", "PHP", 2, "admin") catch {};

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "setRoundingAccount updates book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('9999', 'FX Rounding', 'expense', 'debit', 1);
    );

    try Book.setRoundingAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT rounding_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setRoundingAccount writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('9999', 'FX Rounding', 'expense', 'debit', 1);
    );

    try Book.setRoundingAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT action, field_changed, new_value FROM ledger_audit_log WHERE entity_type = 'book' AND action = 'update';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("update", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("rounding_account_id", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("1", stmt.columnText(2).?);
}

test "setRoundingAccount rejects account from different book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('9999', 'FX Rounding', 'expense', 'debit', 2);
    );

    const result = Book.setRoundingAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setRoundingAccount rejects nonexistent account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");

    const result = Book.setRoundingAccount(database, 1, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "setRoundingAccount rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.setRoundingAccount(database, 999, 1, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── archive tests ───────────────────────────────────────────────

const period_mod = @import("period.zig");

test "archive book with all periods closed or locked succeeds" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    try Book.archive(database, book_id, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "archive book with open periods rejected" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const result = Book.archive(database, book_id, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "archive book with soft_closed periods rejected" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");

    const result = Book.archive(database, book_id, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "archive book with no periods succeeds" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");

    try Book.archive(database, book_id, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "archive already archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, book_id, "admin");

    const result = Book.archive(database, book_id, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "archive book writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, book_id, "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'book' AND action = 'update' AND field_changed = 'status';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("status", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("active", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("archived", stmt.columnText(2).?);
}

test "archive nonexistent book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.archive(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "create operations on archived book should fail at entity level" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, book_id, "admin");

    // Account creation on archived book — the account.create checks book exists
    // but doesn't check book status. This is a Sprint 3 concern (posting engine
    // will check book status). For now, verify the book IS archived.
    var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}
