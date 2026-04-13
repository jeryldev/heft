// Heft — The embedded accounting engine
// Copyright (C) 2026 Jeryl Donato Estopace
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// db.zig: Thin wrapper around SQLite's C API.
// Every SQLite interaction in the engine goes through this module.

const std = @import("std");

const c = @cImport({
    @cInclude("sqlite3.h");
});

// ── Database ────────────────────────────────────────────────────

pub const Database = struct {
    handle: *c.sqlite3,

    pub fn open(path: [*:0]const u8) !Database {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        const self = Database{
            .handle = db orelse return error.SqliteOpenFailed,
        };
        errdefer _ = c.sqlite3_close(self.handle);

        try self.exec("PRAGMA foreign_keys = ON;");
        try self.exec("PRAGMA journal_mode = WAL;");
        try self.exec("PRAGMA busy_timeout = 5000;");

        return self;
    }

    fn drainLeakedStatementsImpl(self: Database, log_warning: bool) usize {
        var count: usize = 0;
        while (c.sqlite3_next_stmt(self.handle, null)) |stmt| {
            _ = c.sqlite3_finalize(stmt);
            count += 1;
        }
        if (log_warning and count > 0) {
            std.log.warn("drainLeakedStatements: finalized {d} leaked statement(s)", .{count});
        }
        return count;
    }

    /// Finalize any leaked prepared statements before close. Called at the C ABI
    /// boundary (internal_close) to prevent the close() assert from firing when
    /// a C caller or BEAM GC (Sprint 8) leaks statements. The loop is guaranteed
    /// to terminate: sqlite3_finalize removes the statement from SQLite's internal
    /// linked list, so each iteration shrinks the list by exactly one.
    /// Returns the number of statements finalized. In debug builds, a nonzero
    /// count indicates a genuine leak that should be fixed at the source.
    pub fn drainLeakedStatements(self: Database) usize {
        return self.drainLeakedStatementsImpl(true);
    }

    /// Test-only helper for leak-cleanup assertions where the cleanup is expected
    /// and should not emit warning noise into the normal test output.
    pub fn drainLeakedStatementsSilent(self: Database) usize {
        return self.drainLeakedStatementsImpl(false);
    }

    pub fn close(self: Database) void {
        const rc = c.sqlite3_close(self.handle);
        std.debug.assert(rc == c.SQLITE_OK);
    }

    pub fn exec(self: Database, sql: [*:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.handle, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg != null) {
                std.log.debug("sqlite3_exec failed: {s}", .{std.mem.span(err_msg)});
                c.sqlite3_free(err_msg);
            }
            return error.SqliteExecFailed;
        }
    }

    pub fn prepare(self: Database, sql: [*:0]const u8) !Statement {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.handle, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
        return Statement{
            .handle = stmt orelse return error.SqlitePrepareFailed,
        };
    }

    pub fn isInTransaction(self: Database) bool {
        return c.sqlite3_get_autocommit(self.handle) == 0;
    }

    pub fn beginTransaction(self: Database) !void {
        try self.exec("BEGIN IMMEDIATE;");
    }

    pub fn beginTransactionIfNeeded(self: Database) !bool {
        if (self.isInTransaction()) return false;
        try self.exec("BEGIN IMMEDIATE;");
        return true;
    }

    pub fn commit(self: Database) !void {
        try self.exec("COMMIT;");
    }

    pub fn lastInsertRowId(self: Database) i64 {
        return c.sqlite3_last_insert_rowid(self.handle);
    }

    pub fn rollback(self: Database) void {
        self.exec("ROLLBACK;") catch {};
    }
};

// ── Statement ───────────────────────────────────────────────────

pub const Statement = struct {
    handle: *c.sqlite3_stmt,

    pub fn finalize(self: Statement) void {
        _ = c.sqlite3_finalize(self.handle);
    }

    pub fn reset(self: Statement) void {
        _ = c.sqlite3_reset(self.handle);
    }

    pub fn clearBindings(self: Statement) void {
        _ = c.sqlite3_clear_bindings(self.handle);
    }

    pub fn step(self: Statement) !bool {
        const rc = c.sqlite3_step(self.handle);
        return switch (rc) {
            c.SQLITE_ROW => true,
            c.SQLITE_DONE => false,
            else => error.SqliteStepFailed,
        };
    }

    pub fn bindNull(self: Statement, col: c_int) !void {
        const rc = c.sqlite3_bind_null(self.handle, col);
        if (rc != c.SQLITE_OK) return error.SqliteBindFailed;
    }

    pub fn bindInt(self: Statement, col: c_int, value: i64) !void {
        const rc = c.sqlite3_bind_int64(self.handle, col, value);
        if (rc != c.SQLITE_OK) return error.SqliteBindFailed;
    }

    /// Caller must keep `value` alive until the next step() or finalize().
    /// Uses SQLITE_STATIC (no copy) — safe for bind-then-step-in-same-scope patterns.
    pub fn bindText(self: Statement, col: c_int, value: []const u8) !void {
        if (value.len > std.math.maxInt(c_int)) return error.SqliteBindFailed;
        const rc = c.sqlite3_bind_text(
            self.handle,
            col,
            value.ptr,
            @intCast(value.len),
            null,
        );
        if (rc != c.SQLITE_OK) return error.SqliteBindFailed;
    }

    pub fn columnInt(self: Statement, col: c_int) i32 {
        return c.sqlite3_column_int(self.handle, col);
    }

    pub fn columnInt64(self: Statement, col: c_int) i64 {
        return c.sqlite3_column_int64(self.handle, col);
    }

    /// Returned slice points into SQLite's internal buffer.
    /// Valid only until the next step(), reset(), or finalize() on this statement.
    /// Copy the bytes if you need the value to outlive the current row.
    pub fn columnText(self: Statement, col: c_int) ?[]const u8 {
        const ptr = c.sqlite3_column_text(self.handle, col);
        if (ptr == null) return null;
        const len = c.sqlite3_column_bytes(self.handle, col);
        return ptr[0..@intCast(len)];
    }
};

// ── Tests ───────────────────────────────────────────────────────

test "Database open and close" {
    const db = try Database.open(":memory:");
    defer db.close();
}

test "Database exec creates a table" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
}

test "Statement prepare, step, and read" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
    try db.exec("INSERT INTO test (name) VALUES ('hello');");

    var stmt = try db.prepare("SELECT COUNT(*) FROM test;");
    defer stmt.finalize();
    const hasRow = try stmt.step();
    try std.testing.expect(hasRow);
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "bindText and columnText" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
    try db.exec("INSERT INTO test (name) VALUES ('hello');");

    var stmt = try db.prepare("SELECT name FROM test WHERE name = ?;");
    defer stmt.finalize();
    try stmt.bindText(1, "hello");
    const hasRow = try stmt.step();
    try std.testing.expect(hasRow);
    try std.testing.expectEqualStrings("hello", stmt.columnText(0).?);
}

test "bindInt and columnInt" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");
    try db.exec("INSERT INTO test (val) VALUES (42);");

    var stmt = try db.prepare("SELECT val FROM test WHERE val = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, 42);
    const hasRow = try stmt.step();
    try std.testing.expect(hasRow);
    try std.testing.expectEqual(@as(i32, 42), stmt.columnInt(0));
}

test "open fails with invalid path" {
    const result = Database.open("/no/such/dir/test.db");
    try std.testing.expectError(error.SqliteOpenFailed, result);
}

test "exec fails with invalid SQL" {
    const db = try Database.open(":memory:");
    defer db.close();
    const result = db.exec("NOT VALID SQL;");
    try std.testing.expectError(error.SqliteExecFailed, result);
}

test "prepare fails with invalid SQL" {
    const db = try Database.open(":memory:");
    defer db.close();
    const result = db.prepare("NOT VALID SQL;");
    try std.testing.expectError(error.SqlitePrepareFailed, result);
}

test "columnText returns null for NULL column" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
    try db.exec("INSERT INTO test (name) VALUES (NULL);");
    var stmt = try db.prepare("SELECT name FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
}

test "step returns false when no rows" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY);");
    var stmt = try db.prepare("SELECT * FROM test;");
    defer stmt.finalize();
    const hasRow = try stmt.step();
    try std.testing.expect(!hasRow);
}

test "drainLeakedStatements finalizes leaked statement" {
    const db = try Database.open(":memory:");
    _ = try db.prepare("SELECT 1;");
    try std.testing.expectEqual(@as(usize, 1), db.drainLeakedStatementsSilent());
    db.close();
}

test "drainLeakedStatements handles multiple leaked statements" {
    const db = try Database.open(":memory:");
    _ = try db.prepare("SELECT 1;");
    _ = try db.prepare("SELECT 2;");
    _ = try db.prepare("SELECT 3;");
    try std.testing.expectEqual(@as(usize, 3), db.drainLeakedStatementsSilent());
    db.close();
}

test "drainLeakedStatements is safe when nothing is leaked" {
    const db = try Database.open(":memory:");
    try std.testing.expectEqual(@as(usize, 0), db.drainLeakedStatementsSilent());
    db.close();
}

test "reset enables statement reuse" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");

    {
        var stmt = try db.prepare("INSERT INTO test (name) VALUES (?);");
        defer stmt.finalize();

        try stmt.bindText(1, "first");
        _ = try stmt.step();
        stmt.reset();

        try stmt.bindText(1, "second");
        _ = try stmt.step();
    }

    var count_stmt = try db.prepare("SELECT COUNT(*) FROM test;");
    defer count_stmt.finalize();
    _ = try count_stmt.step();
    try std.testing.expectEqual(@as(i32, 2), count_stmt.columnInt(0));
}

test "clearBindings sets parameters to NULL" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");

    {
        var stmt = try db.prepare("INSERT INTO test (name) VALUES (?);");
        defer stmt.finalize();

        try stmt.bindText(1, "hello");
        _ = try stmt.step();
        stmt.reset();

        stmt.clearBindings();
        _ = try stmt.step();
    }

    var count_stmt = try db.prepare("SELECT COUNT(*) FROM test WHERE name IS NULL;");
    defer count_stmt.finalize();
    _ = try count_stmt.step();
    try std.testing.expectEqual(@as(i32, 1), count_stmt.columnInt(0));
}

test "lastInsertRowId returns auto-generated id" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");

    try db.exec("INSERT INTO test (name) VALUES ('first');");
    try std.testing.expectEqual(@as(i64, 1), db.lastInsertRowId());

    try db.exec("INSERT INTO test (name) VALUES ('second');");
    try std.testing.expectEqual(@as(i64, 2), db.lastInsertRowId());
}

test "bindNull inserts NULL value" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");

    {
        var stmt = try db.prepare("INSERT INTO test (name) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindNull(1);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT name FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
}

test "columnInt64 reads large i64 values" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, amount INTEGER);");

    // 10,000.50 scaled by 10^8 = 1_000_050_000_000 (exceeds i32 max)
    const large_amount: i64 = 1_000_050_000_000;
    {
        var stmt = try db.prepare("INSERT INTO test (amount) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindInt(1, large_amount);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT amount FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(large_amount, stmt.columnInt64(0));
}

test "columnInt64 handles negative values" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, amount INTEGER);");

    const negative: i64 = -500_000_000_000;
    {
        var stmt = try db.prepare("INSERT INTO test (amount) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindInt(1, negative);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT amount FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(negative, stmt.columnInt64(0));
}

test "bindInt handles i64 max boundary" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");

    const max_val = std.math.maxInt(i64);
    {
        var stmt = try db.prepare("INSERT INTO test (val) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindInt(1, max_val);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT val FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(max_val, stmt.columnInt64(0));
}

test "bindInt handles i64 min boundary" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");

    const min_val = std.math.minInt(i64);
    {
        var stmt = try db.prepare("INSERT INTO test (val) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindInt(1, min_val);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT val FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(min_val, stmt.columnInt64(0));
}

test "bindInt handles zero" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");

    {
        var stmt = try db.prepare("INSERT INTO test (val) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindInt(1, 0);
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT val FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
}

test "bindText handles empty string" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");

    {
        var stmt = try db.prepare("INSERT INTO test (name) VALUES (?);");
        defer stmt.finalize();
        try stmt.bindText(1, "");
        _ = try stmt.step();
    }

    var stmt = try db.prepare("SELECT name FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    const val = stmt.columnText(0);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("", val.?);
}

test "columnInt64 returns 0 for NULL column" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");
    try db.exec("INSERT INTO test (val) VALUES (NULL);");

    var stmt = try db.prepare("SELECT val FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
}

test "lastInsertRowId returns 0 before any insert" {
    const db = try Database.open(":memory:");
    defer db.close();
    try std.testing.expectEqual(@as(i64, 0), db.lastInsertRowId());
}

test "reset and rebind in a loop inserts correct count" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");

    {
        var stmt = try db.prepare("INSERT INTO test (val) VALUES (?);");
        defer stmt.finalize();

        for (1..101) |i| {
            try stmt.bindInt(1, @intCast(i));
            _ = try stmt.step();
            stmt.reset();
            stmt.clearBindings();
        }
    }

    var stmt = try db.prepare("SELECT COUNT(*) FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 100), stmt.columnInt(0));
}

test "bindNull on NOT NULL column fails on step" {
    const database = try Database.open(":memory:");
    defer database.close();
    try database.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
    var stmt = try database.prepare("INSERT INTO test (name) VALUES (?);");
    defer stmt.finalize();
    try stmt.bindNull(1);
    try std.testing.expectError(error.SqliteStepFailed, stmt.step());
}

test "transaction rollback undoes changes" {
    const database = try Database.open(":memory:");
    defer database.close();
    try database.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");
    try database.beginTransaction();
    try database.exec("INSERT INTO test VALUES (1, 100);");
    database.rollback();
    var stmt = try database.prepare("SELECT COUNT(*) FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "statement reset and reuse with bindInt and columnInt64" {
    const database = try Database.open(":memory:");
    defer database.close();
    try database.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");
    try database.exec("INSERT INTO test VALUES (1, 10);");
    try database.exec("INSERT INTO test VALUES (2, 20);");
    var stmt = try database.prepare("SELECT val FROM test WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, 1);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 10), stmt.columnInt64(0));
    stmt.reset();
    stmt.clearBindings();
    try stmt.bindInt(1, 2);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 20), stmt.columnInt64(0));
}

test "nested begin fails with SQLITE_ERROR" {
    const database = try Database.open(":memory:");
    defer database.close();

    try database.beginTransaction();
    const result = database.beginTransaction();
    try std.testing.expectError(error.SqliteExecFailed, result);
    database.rollback();
}

test "isInTransaction returns false outside transaction" {
    const database = try Database.open(":memory:");
    defer database.close();
    try std.testing.expect(!database.isInTransaction());
}

test "isInTransaction returns true inside transaction" {
    const database = try Database.open(":memory:");
    defer database.close();
    try database.beginTransaction();
    try std.testing.expect(database.isInTransaction());
    database.rollback();
}

test "beginTransactionIfNeeded starts transaction when none active" {
    const database = try Database.open(":memory:");
    defer database.close();
    const owns = try database.beginTransactionIfNeeded();
    try std.testing.expect(owns);
    try std.testing.expect(database.isInTransaction());
    database.rollback();
}

test "beginTransactionIfNeeded is no-op when already in transaction" {
    const database = try Database.open(":memory:");
    defer database.close();
    try database.beginTransaction();
    const owns = try database.beginTransactionIfNeeded();
    try std.testing.expect(!owns);
    try std.testing.expect(database.isInTransaction());
    database.rollback();
}

test "rollback outside transaction is safe" {
    const db = try Database.open(":memory:");
    defer db.close();
    db.rollback();
}

test "transaction commit persists, rollback discards" {
    const db = try Database.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER);");

    try db.beginTransaction();
    try db.exec("INSERT INTO test (val) VALUES (1);");
    try db.commit();

    try db.beginTransaction();
    try db.exec("INSERT INTO test (val) VALUES (2);");
    db.rollback();

    var stmt = try db.prepare("SELECT COUNT(*) FROM test;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}
