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

const SCHEMA_VERSION: i32 = 1;

fn internal_open(path: [*:0]const u8) !*LedgerDB {
    const db = try heft.db.Database.open(path);
    errdefer db.close();

    const version = blk: {
        var stmt = try db.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        break :blk stmt.columnInt(0);
    };

    if (version == 0) {
        try heft.schema.createAll(db);
    } else if (version > SCHEMA_VERSION) {
        return error.SchemaVersionMismatch;
    }

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

// ── Sprint 2: Entity C ABI Exports ─────────────────────────────

pub export fn ledger_create_book(handle: ?*LedgerDB, name: [*:0]const u8, base_currency: [*:0]const u8, decimal_places: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.book.Book.create(h.sqlite, std.mem.span(name), std.mem.span(base_currency), decimal_places, std.mem.span(performed_by)) catch -1;
}

pub export fn ledger_create_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, is_contra: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const at = heft.account.AccountType.fromString(std.mem.span(account_type)) orelse return -1;
    return heft.account.Account.create(h.sqlite, book_id, std.mem.span(number), std.mem.span(name), at, is_contra != 0, std.mem.span(performed_by)) catch -1;
}

pub export fn ledger_create_period(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, period_number: i32, year: i32, start_date: [*:0]const u8, end_date: [*:0]const u8, period_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.period.Period.create(h.sqlite, book_id, std.mem.span(name), period_number, year, std.mem.span(start_date), std.mem.span(end_date), std.mem.span(period_type), std.mem.span(performed_by)) catch -1;
}

pub export fn ledger_update_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const status = heft.account.AccountStatus.fromString(std.mem.span(new_status)) orelse return false;
    heft.account.Account.updateStatus(h.sqlite, account_id, status, std.mem.span(performed_by)) catch return false;
    return true;
}

pub export fn ledger_transition_period(handle: ?*LedgerDB, period_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const ts = heft.period.PeriodStatus.fromString(std.mem.span(target_status)) orelse return false;
    heft.period.Period.transition(h.sqlite, period_id, ts, std.mem.span(performed_by)) catch return false;
    return true;
}

pub export fn ledger_set_rounding_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setRoundingAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch return false;
    return true;
}

pub export fn ledger_bulk_create_periods(handle: ?*LedgerDB, book_id: i64, fiscal_year: i32, start_month: i32, granularity: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const gran = heft.period.PeriodGranularity.fromString(std.mem.span(granularity)) orelse return false;
    heft.period.Period.bulkCreate(h.sqlite, book_id, fiscal_year, start_month, gran, std.mem.span(performed_by)) catch return false;
    return true;
}

pub export fn ledger_archive_book(handle: ?*LedgerDB, book_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.archive(h.sqlite, book_id, std.mem.span(performed_by)) catch return false;
    return true;
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

test "ledger_open sets schema version 1 on new file" {
    defer cleanupTestFile("test-version.ledger");
    const handle = ledger_open("test-version.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }
}

test "ledger_open rejects future schema version" {
    defer cleanupTestFile("test-future-version.ledger");

    const h1 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const raw_db = try heft.db.Database.open("test-future-version.ledger");
    try raw_db.exec("PRAGMA user_version = 999;");
    raw_db.close();

    const h2 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h2 == null);
}

test "ledger_open preserves schema version on reopen" {
    defer cleanupTestFile("test-reopen-version.ledger");

    const h1 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const h2 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
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

// ── Sprint 2: C ABI integration tests ──────────────────────────

test "C ABI: full lifecycle book -> account -> period -> transition" {
    defer cleanupTestFile("test-cabi-lifecycle.ledger");
    const handle = ledger_open("test-cabi-lifecycle.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(acct_id > 0);

        const period_id = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(period_id > 0);

        try std.testing.expect(ledger_transition_period(h, period_id, "soft_closed", "admin"));
        try std.testing.expect(ledger_update_account_status(h, acct_id, "archived", "admin"));
        try std.testing.expect(ledger_set_rounding_account(h, book_id, acct_id, "admin"));
    }
}

test "C ABI: bulk create periods via C boundary" {
    defer cleanupTestFile("test-cabi-bulk.ledger");
    const handle = ledger_open("test-cabi-bulk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        try std.testing.expect(ledger_bulk_create_periods(h, book_id, 2026, 1, "monthly", "admin"));

        var stmt = try h.sqlite.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
    }
}

test "C ABI: null handle returns error values" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_book(null, "Test", "PHP", 2, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_account(null, 1, "1000", "Cash", "asset", 0, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_period(null, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin"));
    try std.testing.expect(!ledger_update_account_status(null, 1, "inactive", "admin"));
    try std.testing.expect(!ledger_transition_period(null, 1, "soft_closed", "admin"));
    try std.testing.expect(!ledger_set_rounding_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_bulk_create_periods(null, 1, 2026, 1, "monthly", "admin"));
    try std.testing.expect(!ledger_archive_book(null, 1, "admin"));
}

test "C ABI: invalid account_type string returns -1" {
    defer cleanupTestFile("test-cabi-bad-type.ledger");
    const handle = ledger_open("test-cabi-bad-type.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const result = ledger_create_account(h, book_id, "1000", "Cash", "invalid_type", 0, "admin");
        try std.testing.expectEqual(@as(i64, -1), result);
    }
}

test "C ABI: invalid granularity string returns false" {
    defer cleanupTestFile("test-cabi-bad-gran.ledger");
    const handle = ledger_open("test-cabi-bad-gran.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        try std.testing.expect(!ledger_bulk_create_periods(h, book_id, 2026, 1, "weekly", "admin"));
    }
}

test "C ABI: invalid account status string returns false" {
    defer cleanupTestFile("test-cabi-bad-acct-status.ledger");
    const handle = ledger_open("test-cabi-bad-acct-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(!ledger_update_account_status(h, acct_id, "deleted", "admin"));
    }
}

test "C ABI: invalid period status string returns false" {
    defer cleanupTestFile("test-cabi-bad-period-status.ledger");
    const handle = ledger_open("test-cabi-bad-period-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_transition_period(h, 1, "deleted", "admin"));
    }
}

test "C ABI: archive book via C boundary" {
    defer cleanupTestFile("test-cabi-archive.ledger");
    const handle = ledger_open("test-cabi-archive.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(ledger_archive_book(h, book_id, "admin"));

        var stmt = try h.sqlite.prepare("SELECT status FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
    }
}

test "C ABI: archive book with open periods returns false" {
    defer cleanupTestFile("test-cabi-archive-fail.ledger");
    const handle = ledger_open("test-cabi-archive-fail.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_archive_book(h, book_id, "admin"));
    }
}
