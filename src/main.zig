// Heft — The embedded accounting engine
// Copyright (C) 2026 Jeryl Donato Estopace
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// main.zig: C ABI surface. Every public function lives here.
// Internal logic lives in db.zig, schema.zig, and future entity modules.
// This file is thin — convert between C types and Zig types, nothing more.

const std = @import("std");
const heft = @import("heft");

// ── LedgerDB ────────────────────────────────────────────────────
// The opaque handle returned to C callers. Heap-allocated because
// it crosses the C ABI as a pointer.

pub const LedgerDB = struct {
    sqlite: heft.db.Database,
};

// ── Internal (Zig idioms) ───────────────────────────────────────

fn internal_open(path: [*:0]const u8) !*LedgerDB {
    const db = try heft.db.Database.open(path);
    errdefer db.close();

    try heft.schema.createAll(db);

    const handle = std.heap.c_allocator.create(LedgerDB) catch return error.OutOfMemory;
    handle.* = .{ .sqlite = db };
    return handle;
}

fn internal_close(handle: *LedgerDB) void {
    handle.sqlite.drainLeakedStatements();
    handle.sqlite.close();
    std.heap.c_allocator.destroy(handle);
}

// ── C ABI Exports ───────────────────────────────────────────────

pub export fn ledger_open(path: [*:0]const u8) ?*LedgerDB {
    return internal_open(path) catch null;
}

pub export fn ledger_close(handle: ?*LedgerDB) void {
    const h = handle orelse return;
    internal_close(h);
}

pub export fn ledger_version() [*:0]const u8 {
    return "0.0.1";
}

// ── Tests ───────────────────────────────────────────────────────

fn cleanupTestFile(name: [*:0]const u8) void {
    const cwd = std.fs.cwd();
    const base = std.mem.span(name);
    cwd.deleteFile(base) catch {};
    // WAL mode creates -wal and -shm sidecar files
    var wal_buf: [256]u8 = undefined;
    var shm_buf: [256]u8 = undefined;
    const wal_name = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{base}) catch return;
    const shm_name = std.fmt.bufPrint(&shm_buf, "{s}-shm", .{base}) catch return;
    cwd.deleteFile(wal_name) catch {};
    cwd.deleteFile(shm_name) catch {};
}

test "ledger_version returns 0.0.1" {
    const v = std.mem.span(ledger_version());
    try std.testing.expectEqualStrings("0.0.1", v);
}

test "ledger_open returns non-null, ledger_close cleans up" {
    defer cleanupTestFile("test-open-close.ledger");
    const handle = ledger_open("test-open-close.ledger");
    try std.testing.expect(handle != null);
    if (handle) |h| ledger_close(h);
}

test "ledger_open creates all 11 schema tables" {
    defer cleanupTestFile("test-schema-main.ledger");
    const handle = ledger_open("test-schema-main.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const expected_tables = [_][]const u8{
            "ledger_books",
            "ledger_accounts",
            "ledger_periods",
            "ledger_classifications",
            "ledger_classification_nodes",
            "ledger_subledger_groups",
            "ledger_subledger_accounts",
            "ledger_entries",
            "ledger_entry_lines",
            "ledger_account_balances",
            "ledger_audit_log",
        };

        for (expected_tables) |table_name| {
            var stmt = try h.sqlite.prepare(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;",
            );
            defer stmt.finalize();
            try stmt.bindText(1, table_name);
            _ = try stmt.step();
            try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
        }
    }
}

test "ledger_open enables WAL mode" {
    defer cleanupTestFile("test-wal.ledger");
    const handle = ledger_open("test-wal.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA journal_mode;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("wal", stmt.columnText(0).?);
    }
}

test "ledger_open enables foreign keys" {
    defer cleanupTestFile("test-fk.ledger");
    const handle = ledger_open("test-fk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA foreign_keys;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }
}

test "ledger_open returns null for invalid path" {
    const handle = ledger_open("/no/such/dir/bad.ledger");
    try std.testing.expect(handle == null);
}

test "ledger_open is idempotent on existing file" {
    defer cleanupTestFile("test-idempotent.ledger");
    const h1 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    // Open same file again — schema uses IF NOT EXISTS
    const h2 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| ledger_close(h);
}
