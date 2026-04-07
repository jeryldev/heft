const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const account_mod = @import("account.zig");

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

    fn isSystemAccount(database: db.Database, book_id: i64, account_id: i64, exclude_column: []const u8) !bool {
        const columns = [_][]const u8{
            "rounding_account_id",
            "fx_gain_loss_account_id",
            "retained_earnings_account_id",
            "income_summary_account_id",
            "opening_balance_account_id",
            "suspense_account_id",
        };
        var stmt = try database.prepare(
            \\SELECT rounding_account_id, fx_gain_loss_account_id,
            \\  retained_earnings_account_id, income_summary_account_id,
            \\  opening_balance_account_id, suspense_account_id
            \\FROM ledger_books WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return false;
        inline for (columns, 0..) |col, i| {
            if (!std.mem.eql(u8, col, exclude_column)) {
                if (stmt.columnInt64(@intCast(i)) == account_id) return true;
            }
        }
        return false;
    }

    pub fn create(database: db.Database, name: []const u8, base_currency: []const u8, decimal_places: i32, performed_by: []const u8) !i64 {
        if (name.len == 0) return error.InvalidInput;
        if (base_currency.len != 3) return error.InvalidInput;
        if (decimal_places < 0 or decimal_places > 8) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, name);
        try stmt.bindText(2, base_currency);
        try stmt.bindInt(3, @intCast(decimal_places));
        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "book", id, "create", null, null, null, performed_by, id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn setRoundingAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        // Verify book exists and is not archived
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status_enum = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status_enum != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = account_mod.AccountType.fromString(stmt.columnText(2).?) orelse return error.InvalidInput;
            if (acct_type != .revenue and acct_type != .expense) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "rounding_account_id")) return error.InvalidInput;

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

        if (owns_txn) try database.commit();
    }

    pub fn setFxGainLossAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status_enum = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status_enum != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = account_mod.AccountType.fromString(stmt.columnText(2).?) orelse return error.InvalidInput;
            if (acct_type != .revenue and acct_type != .expense) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "fx_gain_loss_account_id")) return error.InvalidInput;

        {
            var stmt = try database.prepare("UPDATE ledger_books SET fx_gain_loss_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "fx_gain_loss_account_id", null, id_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setRetainedEarningsAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type, is_contra FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = stmt.columnText(2).?;
            const is_contra = stmt.columnInt(3);
            if (!std.mem.eql(u8, acct_type, "equity") or is_contra != 0) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "retained_earnings_account_id")) return error.InvalidInput;

        {
            var stmt = try database.prepare("UPDATE ledger_books SET retained_earnings_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "retained_earnings_account_id", null, id_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setIncomeSummaryAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type, is_contra FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = stmt.columnText(2).?;
            const is_contra = stmt.columnInt(3);
            if (!std.mem.eql(u8, acct_type, "equity") or is_contra != 0) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "income_summary_account_id")) return error.InvalidInput;

        {
            var stmt = try database.prepare("UPDATE ledger_books SET income_summary_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "income_summary_account_id", null, id_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setOpeningBalanceAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type, is_contra FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = stmt.columnText(2).?;
            const is_contra = stmt.columnInt(3);
            if (!std.mem.eql(u8, acct_type, "equity") or is_contra != 0) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "opening_balance_account_id")) return error.InvalidInput;

        {
            var stmt = try database.prepare("UPDATE ledger_books SET opening_balance_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "opening_balance_account_id", null, id_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setSuspenseAccount(database: db.Database, book_id: i64, account_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT status, book_id, account_type FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const acct_status = account_mod.AccountStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (acct_status != .active) return error.AccountInactive;
            if (stmt.columnInt64(1) != book_id) return error.InvalidInput;
            const acct_type = stmt.columnText(2).?;
            if (!std.mem.eql(u8, acct_type, "asset") and !std.mem.eql(u8, acct_type, "liability")) return error.InvalidInput;
        }

        if (try isSystemAccount(database, book_id, account_id, "suspense_account_id")) return error.InvalidInput;

        {
            var stmt = try database.prepare("UPDATE ledger_books SET suspense_account_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var id_buf: [id_buf_len]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{account_id}) catch unreachable;

        try audit.log(database, "book", book_id, "update", "suspense_account_id", null, id_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn validateOpeningBalanceMigration(database: db.Database, book_id: i64) !void {
        var stmt = try database.prepare("SELECT status, opening_balance_account_id FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
        if (status == .archived) return error.BookArchived;
        const ob_id = stmt.columnInt64(1);
        if (ob_id <= 0) return error.OpeningBalanceAccountRequired;
    }

    pub fn updateName(database: db.Database, book_id: i64, new_name: []const u8, performed_by: []const u8) !void {
        if (new_name.len == 0) return error.InvalidInput;

        var old_name_buf: [256]u8 = undefined;
        var old_name_len: usize = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT name, status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            old_name_len = @min(old.len, old_name_buf.len);
            @memcpy(old_name_buf[0..old_name_len], old[0..old_name_len]);
            const status = BookStatus.fromString(stmt.columnText(1).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_books SET name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_name);
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        try audit.log(database, "book", book_id, "update", "name", old_name_buf[0..old_name_len], new_name, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn archive(database: db.Database, book_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var old_status_buf: [16]u8 = undefined;
        var old_status_len: usize = 0;

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const current = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (current == .archived) return error.InvalidTransition;
            const cs = current.toString();
            old_status_len = cs.len;
            @memcpy(old_status_buf[0..old_status_len], cs);
        }

        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ? AND status IN ('open', 'soft_closed');");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_books SET status = 'archived', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
        }

        try audit.log(database, "book", book_id, "update", "status", old_status_buf[0..old_status_len], "archived", performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setRequireApproval(database: db.Database, book_id: i64, require: bool, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var old_val: i32 = 0;
        {
            var stmt = try database.prepare("SELECT status, require_approval FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
            old_val = stmt.columnInt(1);
        }

        {
            var stmt = try database.prepare("UPDATE ledger_books SET require_approval = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, if (require) @as(i64, 1) else @as(i64, 0));
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var old_buf: [2]u8 = undefined;
        var new_buf: [2]u8 = undefined;
        const old_str = std.fmt.bufPrint(&old_buf, "{d}", .{old_val}) catch unreachable;
        const new_str = std.fmt.bufPrint(&new_buf, "{d}", .{@as(i32, if (require) 1 else 0)}) catch unreachable;
        try audit.log(database, "book", book_id, "update", "require_approval", old_str, new_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn setFyStartMonth(database: db.Database, book_id: i64, month: i32, performed_by: []const u8) !void {
        if (month < 1 or month > 12) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var old_val: i32 = 0;
        {
            var stmt = try database.prepare("SELECT status, fy_start_month FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = BookStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status == .archived) return error.BookArchived;
            old_val = stmt.columnInt(1);
        }

        {
            var stmt = try database.prepare("UPDATE ledger_books SET fy_start_month = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, @as(i64, @intCast(month)));
            try stmt.bindInt(2, book_id);
            _ = try stmt.step();
        }

        var old_buf: [4]u8 = undefined;
        var new_buf: [4]u8 = undefined;
        const old_str = std.fmt.bufPrint(&old_buf, "{d}", .{old_val}) catch unreachable;
        const new_str = std.fmt.bufPrint(&new_buf, "{d}", .{month}) catch unreachable;
        try audit.log(database, "book", book_id, "update", "fy_start_month", old_str, new_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn getFyStartDate(as_of_date: []const u8, fy_start_month: i32) [10]u8 {
        var result: [10]u8 = "0000-00-01".*;
        var year: i32 = 0;
        for (as_of_date[0..4]) |c| {
            year = year * 10 + @as(i32, c - '0');
        }
        var month: i32 = 0;
        for (as_of_date[5..7]) |c| {
            month = month * 10 + @as(i32, c - '0');
        }
        if (fy_start_month > 1 and month < fy_start_month) {
            year -= 1;
        }
        const uy: u32 = @intCast(year);
        const um: u32 = @intCast(fy_start_month);
        result[0] = @intCast('0' + (uy / 1000) % 10);
        result[1] = @intCast('0' + (uy / 100) % 10);
        result[2] = @intCast('0' + (uy / 10) % 10);
        result[3] = @intCast('0' + uy % 10);
        result[5] = @intCast('0' + (um / 10) % 10);
        result[6] = @intCast('0' + um % 10);
        return result;
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

test "setRoundingAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('9999', 'FX Rounding', 'expense', 'debit', 1);
    );
    try Book.archive(database, book_id, "admin");

    const result = Book.setRoundingAccount(database, book_id, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setRoundingAccount rejects inactive account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "9999", "FX Rounding", .expense, false, "admin");
    try account_mod.Account.updateStatus(database, 1, .inactive, "admin");

    const result = Book.setRoundingAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.AccountInactive, result);
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

// ── updateName tests ───────────────────────────────────────────

test "updateName changes book name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Old Name", "PHP", 2, "admin");
    try Book.updateName(database, 1, "New Name", "admin");

    var stmt = try database.prepare("SELECT name FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New Name", stmt.columnText(0).?);
}

test "updateName writes audit log with old and new values" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Old Name", "PHP", 2, "admin");
    try Book.updateName(database, 1, "New Name", "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'book' AND action = 'update' AND field_changed = 'name';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("name", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Old Name", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("New Name", stmt.columnText(2).?);
}

test "updateName rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    const result = Book.updateName(database, 1, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateName rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.updateName(database, 999, "Name", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "updateName rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, 1, "admin");
    const result = Book.updateName(database, 1, "New", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "create book rejects empty base_currency" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.create(database, "Test", "", 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create book rejects base_currency longer than 3" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.create(database, "Test", "PHPP", 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create operations on archived book should fail at entity level" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, book_id, "admin");

    // Account creation on archived book — account.create checks book exists
    // AND checks book status, rejecting archived books. Verify the book IS archived.
    var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

// ── setFxGainLossAccount tests ─────────────────────────────────

test "setFxGainLossAccount happy path with expense account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "8001", "FX Gain/Loss", .expense, false, "admin");

    try Book.setFxGainLossAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT fx_gain_loss_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setFxGainLossAccount happy path with revenue account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "4001", "FX Gain", .revenue, false, "admin");

    try Book.setFxGainLossAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT fx_gain_loss_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setFxGainLossAccount rejects asset account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1001", "Cash", .asset, false, "admin");

    const result = Book.setFxGainLossAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setFxGainLossAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "8001", "FX Gain/Loss", .expense, false, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.setFxGainLossAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setFxGainLossAccount rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "8001", "FX Gain/Loss", .expense, false, "admin");

    const result = Book.setFxGainLossAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── setRetainedEarningsAccount tests ───────────────────────────

test "setRetainedEarningsAccount happy path with equity account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3100", "Retained Earnings", .equity, false, "admin");

    try Book.setRetainedEarningsAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT retained_earnings_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setRetainedEarningsAccount rejects asset account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1001", "Cash", .asset, false, "admin");

    const result = Book.setRetainedEarningsAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setRetainedEarningsAccount rejects equity contra account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3200", "Treasury Stock", .equity, true, "admin");

    const result = Book.setRetainedEarningsAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setRetainedEarningsAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3100", "Retained Earnings", .equity, false, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.setRetainedEarningsAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setRetainedEarningsAccount rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "3100", "Retained Earnings", .equity, false, "admin");

    const result = Book.setRetainedEarningsAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── setIncomeSummaryAccount tests ──────────────────────────────

test "setIncomeSummaryAccount happy path with equity account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3300", "Income Summary", .equity, false, "admin");

    try Book.setIncomeSummaryAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT income_summary_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setIncomeSummaryAccount rejects revenue account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "4001", "Sales", .revenue, false, "admin");

    const result = Book.setIncomeSummaryAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setIncomeSummaryAccount rejects equity contra account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3200", "Treasury Stock", .equity, true, "admin");

    const result = Book.setIncomeSummaryAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setIncomeSummaryAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3300", "Income Summary", .equity, false, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.setIncomeSummaryAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setIncomeSummaryAccount rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "3300", "Income Summary", .equity, false, "admin");

    const result = Book.setIncomeSummaryAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── setOpeningBalanceAccount tests ─────────────────────────────

test "setOpeningBalanceAccount happy path with equity account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3400", "Opening Balance Equity", .equity, false, "admin");

    try Book.setOpeningBalanceAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT opening_balance_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setOpeningBalanceAccount rejects expense account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "8001", "Misc Expense", .expense, false, "admin");

    const result = Book.setOpeningBalanceAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setOpeningBalanceAccount rejects equity contra account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3200", "Treasury Stock", .equity, true, "admin");

    const result = Book.setOpeningBalanceAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setOpeningBalanceAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3400", "Opening Balance Equity", .equity, false, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.setOpeningBalanceAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setOpeningBalanceAccount rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "3400", "Opening Balance Equity", .equity, false, "admin");

    const result = Book.setOpeningBalanceAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── setSuspenseAccount tests ───────────────────────────────────

test "setSuspenseAccount happy path with asset account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1900", "Suspense", .asset, false, "admin");

    try Book.setSuspenseAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT suspense_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setSuspenseAccount happy path with liability account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "2900", "Suspense Liability", .liability, false, "admin");

    try Book.setSuspenseAccount(database, 1, 1, "admin");

    var stmt = try database.prepare("SELECT suspense_account_id FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "setSuspenseAccount rejects equity account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3100", "Equity Acct", .equity, false, "admin");

    const result = Book.setSuspenseAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "setSuspenseAccount rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1900", "Suspense", .asset, false, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.setSuspenseAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setSuspenseAccount rejects cross-book account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "1900", "Suspense", .asset, false, "admin");

    const result = Book.setSuspenseAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── validateOpeningBalanceMigration tests ───────────────────────

test "validateOpeningBalanceMigration without designation returns error" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");

    const result = Book.validateOpeningBalanceMigration(database, 1);
    try std.testing.expectError(error.OpeningBalanceAccountRequired, result);
}

test "validateOpeningBalanceMigration with designation succeeds" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "3400", "Opening Balance Equity", .equity, false, "admin");
    try Book.setOpeningBalanceAccount(database, 1, 1, "admin");

    try Book.validateOpeningBalanceMigration(database, 1);
}

test "validateOpeningBalanceMigration rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Book.create(database, "FY2026", "PHP", 2, "admin");
    try Book.archive(database, 1, "admin");

    const result = Book.validateOpeningBalanceMigration(database, 1);
    try std.testing.expectError(error.BookArchived, result);
}

test "validateOpeningBalanceMigration rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Book.validateOpeningBalanceMigration(database, 999);
    try std.testing.expectError(error.NotFound, result);
}

// ── integration: designate all system accounts ─────────────────

test "integration: designate RE, OB, IS accounts and verify book" {
    const database = try setupTestDb();
    defer database.close();

    const book_id = try Book.create(database, "FY2026", "PHP", 2, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const ob_id = try account_mod.Account.create(database, book_id, "3400", "Opening Balance Equity", .equity, false, "admin");
    const is_id = try account_mod.Account.create(database, book_id, "3300", "Income Summary", .equity, false, "admin");

    try Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");
    try Book.setOpeningBalanceAccount(database, book_id, ob_id, "admin");
    try Book.setIncomeSummaryAccount(database, book_id, is_id, "admin");

    try Book.validateOpeningBalanceMigration(database, book_id);

    var stmt = try database.prepare("SELECT retained_earnings_account_id, opening_balance_account_id, income_summary_account_id FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqual(re_id, stmt.columnInt64(0));
    try std.testing.expectEqual(ob_id, stmt.columnInt64(1));
    try std.testing.expectEqual(is_id, stmt.columnInt64(2));
}

test "setRequireApproval enables approval" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try Book.create(database, "Test", "PHP", 2, "admin");
    try Book.setRequireApproval(database, book_id, true, "admin");
    var stmt = try database.prepare("SELECT require_approval FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "setRequireApproval disables approval" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try Book.create(database, "Test", "PHP", 2, "admin");
    try Book.setRequireApproval(database, book_id, true, "admin");
    try Book.setRequireApproval(database, book_id, false, "admin");
    var stmt = try database.prepare("SELECT require_approval FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "setRequireApproval rejects archived book" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try Book.create(database, "Test", "PHP", 2, "admin");
    try Book.archive(database, book_id, "admin");
    const result = Book.setRequireApproval(database, book_id, true, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "setRequireApproval rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();
    const result = Book.setRequireApproval(database, 999, true, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "setRequireApproval writes audit log" {
    const database = try setupTestDb();
    defer database.close();
    const book_id = try Book.create(database, "Test", "PHP", 2, "admin");
    try Book.setRequireApproval(database, book_id, true, "admin");
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'book' AND action = 'update' AND field_changed = 'require_approval';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "setRoundingAccount rejects non-PL account type" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    const result = Book.setRoundingAccount(database, 1, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "duplicate system account role rejected" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "Test", "PHP", 2, "admin");
    const re = try account_mod.Account.create(database, 1, "3100", "RE", .equity, false, "admin");
    const is_acct = try account_mod.Account.create(database, 1, "3200", "IS", .equity, false, "admin");
    try Book.setIncomeSummaryAccount(database, 1, is_acct, "admin");
    const result = Book.setRetainedEarningsAccount(database, 1, is_acct, "admin");
    try std.testing.expectError(error.InvalidInput, result);
    try Book.setRetainedEarningsAccount(database, 1, re, "admin");
}

test "setFyStartMonth: default is 1 (January)" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "Test", "PHP", 2, "admin");
    var stmt = try database.prepare("SELECT fy_start_month FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "setFyStartMonth: set to April (India)" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "India Book", "INR", 2, "admin");
    try Book.setFyStartMonth(database, 1, 4, "admin");
    var stmt = try database.prepare("SELECT fy_start_month FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
}

test "setFyStartMonth: rejects 0 and 13" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "Test", "PHP", 2, "admin");
    try std.testing.expectError(error.InvalidInput, Book.setFyStartMonth(database, 1, 0, "admin"));
    try std.testing.expectError(error.InvalidInput, Book.setFyStartMonth(database, 1, 13, "admin"));
}

test "setFyStartMonth: rejects archived book" {
    const database = try setupTestDb();
    defer database.close();
    _ = try Book.create(database, "Test", "PHP", 2, "admin");
    {
        var stmt = try database.prepare("UPDATE ledger_books SET status = 'archived' WHERE id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
    }
    try std.testing.expectError(error.BookArchived, Book.setFyStartMonth(database, 1, 4, "admin"));
}

test "getFyStartDate: calendar year (Jan start)" {
    const result = Book.getFyStartDate("2026-06-15", 1);
    try std.testing.expect(std.mem.eql(u8, &result, "2026-01-01"));
}

test "getFyStartDate: India April start, date in FY" {
    const result = Book.getFyStartDate("2026-06-15", 4);
    try std.testing.expect(std.mem.eql(u8, &result, "2026-04-01"));
}

test "getFyStartDate: India April start, date before April" {
    const result = Book.getFyStartDate("2026-02-15", 4);
    try std.testing.expect(std.mem.eql(u8, &result, "2025-04-01"));
}

test "getFyStartDate: Australia July start, date in FY" {
    const result = Book.getFyStartDate("2026-09-30", 7);
    try std.testing.expect(std.mem.eql(u8, &result, "2026-07-01"));
}

test "getFyStartDate: Australia July start, date before July" {
    const result = Book.getFyStartDate("2026-03-31", 7);
    try std.testing.expect(std.mem.eql(u8, &result, "2025-07-01"));
}
