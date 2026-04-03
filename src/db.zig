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

        return self;
    }

    pub fn drainLeakedStatements(self: Database) void {
        while (c.sqlite3_next_stmt(self.handle, null)) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
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

    pub fn beginTransaction(self: Database) !void {
        try self.exec("BEGIN IMMEDIATE;");
    }

    pub fn commit(self: Database) !void {
        try self.exec("COMMIT;");
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

    pub fn step(self: Statement) !bool {
        const rc = c.sqlite3_step(self.handle);
        return switch (rc) {
            c.SQLITE_ROW => true,
            c.SQLITE_DONE => false,
            else => error.SqliteStepFailed,
        };
    }

    pub fn bindInt(self: Statement, col: c_int, value: i64) !void {
        const rc = c.sqlite3_bind_int64(self.handle, col, value);
        if (rc != c.SQLITE_OK) return error.SqliteBindFailed;
    }

    /// Caller must keep `value` alive until the next step() or finalize().
    /// Uses SQLITE_STATIC (no copy) — safe for bind-then-step-in-same-scope patterns.
    pub fn bindText(self: Statement, col: c_int, value: []const u8) !void {
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
    db.drainLeakedStatements();
    db.close();
}

test "drainLeakedStatements handles multiple leaked statements" {
    const db = try Database.open(":memory:");
    _ = try db.prepare("SELECT 1;");
    _ = try db.prepare("SELECT 2;");
    _ = try db.prepare("SELECT 3;");
    db.drainLeakedStatements();
    db.close();
}

test "drainLeakedStatements is safe when nothing is leaked" {
    const db = try Database.open(":memory:");
    db.drainLeakedStatements();
    db.close();
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
