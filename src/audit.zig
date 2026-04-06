const std = @import("std");
const db = @import("db.zig");

pub const insert_sql: [*:0]const u8 =
    \\INSERT INTO ledger_audit_log
    \\  (entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, book_id, hash_chain)
    \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
;

pub const genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";

pub fn computeHash(prev_hash: []const u8, entity_type: []const u8, entity_id: i64, action: []const u8) [64]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(prev_hash);
    hasher.update(entity_type);
    var id_buf: [20]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{entity_id}) catch unreachable;
    hasher.update(id_str);
    hasher.update(action);
    const digest = hasher.finalResult();
    return std.fmt.bytesToHex(digest, .lower);
}

fn getPreviousHash(database: db.Database) [64]u8 {
    var stmt = database.prepare("SELECT hash_chain FROM ledger_audit_log ORDER BY id DESC LIMIT 1;") catch {
        std.log.warn("audit: failed to prepare hash chain query, using genesis hash", .{});
        return genesis_hash.*;
    };
    defer stmt.finalize();
    const has_row = stmt.step() catch {
        std.log.warn("audit: failed to step hash chain query, using genesis hash", .{});
        return genesis_hash.*;
    };
    if (!has_row) return genesis_hash.*;
    if (stmt.columnText(0)) |h| {
        if (h.len == 64) {
            var result: [64]u8 = undefined;
            @memcpy(&result, h[0..64]);
            return result;
        }
        std.log.warn("audit: hash_chain has invalid length {d}, expected 64, using genesis hash", .{h.len});
    } else {
        std.log.debug("audit: hash_chain is NULL on existing row, using genesis hash", .{});
    }
    return genesis_hash.*;
}

fn bindAndStep(database: db.Database, stmt: *db.Statement, entity_type: []const u8, entity_id: i64, action: []const u8, field_changed: ?[]const u8, old_value: ?[]const u8, new_value: ?[]const u8, performed_by: []const u8, book_id: i64) !void {
    const prev_hash = getPreviousHash(database);
    const hash = computeHash(&prev_hash, entity_type, entity_id, action);
    try stmt.bindText(1, entity_type);
    try stmt.bindInt(2, entity_id);
    try stmt.bindText(3, action);
    if (field_changed) |f| try stmt.bindText(4, f) else try stmt.bindNull(4);
    if (old_value) |o| try stmt.bindText(5, o) else try stmt.bindNull(5);
    if (new_value) |n| try stmt.bindText(6, n) else try stmt.bindNull(6);
    try stmt.bindText(7, performed_by);
    try stmt.bindInt(8, book_id);
    try stmt.bindText(9, &hash);
    _ = try stmt.step();
    stmt.reset();
    stmt.clearBindings();
}

pub fn log(
    database: db.Database,
    entity_type: []const u8,
    entity_id: i64,
    action: []const u8,
    field_changed: ?[]const u8,
    old_value: ?[]const u8,
    new_value: ?[]const u8,
    performed_by: []const u8,
    book_id: i64,
) !void {
    var stmt = try database.prepare(insert_sql);
    defer stmt.finalize();
    try bindAndStep(database, &stmt, entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, book_id);
}

pub fn logWithStmt(
    database: db.Database,
    stmt: *db.Statement,
    entity_type: []const u8,
    entity_id: i64,
    action: []const u8,
    field_changed: ?[]const u8,
    old_value: ?[]const u8,
    new_value: ?[]const u8,
    performed_by: []const u8,
    book_id: i64,
) !void {
    try bindAndStep(database, stmt, entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, book_id);
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    return database;
}

test "audit log records create action" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT entity_type, entity_id, action, performed_by FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("book", stmt.columnText(0).?);
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(1));
    try std.testing.expectEqualStrings("create", stmt.columnText(2).?);
    try std.testing.expectEqualStrings("admin", stmt.columnText(3).?);
}

test "audit log records update with field change" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "book", 1, "update", "name", "Old Name", "New Name", "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("name", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Old Name", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("New Name", stmt.columnText(2).?);
}

test "audit log stores null for unchanged fields on create" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "account", 1, "create", null, null, null, "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
    try std.testing.expect(stmt.columnText(1) == null);
    try std.testing.expect(stmt.columnText(2) == null);
}

test "audit log rolled back with transaction on failure" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    database.rollback();

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "audit log multiple entries in same transaction" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    try log(database, "book", 1, "update", "name", "Old", "New", "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "audit log records correct book_id" {
    const database = try setupTestDb();
    defer database.close();

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book 2', 'USD');");

    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    try log(database, "book", 2, "create", null, null, null, "admin", 2);
    try database.commit();

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE book_id = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "logWithStmt writes three sequential entries correctly" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    var stmt = try database.prepare(insert_sql);
    defer stmt.finalize();

    try logWithStmt(database, &stmt, "entry_line", 1, "update", "debit_amount", "100", "200", "admin", 1);
    try logWithStmt(database, &stmt, "entry_line", 1, "update", "credit_amount", "0", "50", "admin", 1);
    try logWithStmt(database, &stmt, "entry_line", 1, "update", "fx_rate", "10000000000", "20000000000", "admin", 1);
    try database.commit();

    var count_stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log;");
    defer count_stmt.finalize();
    _ = try count_stmt.step();
    try std.testing.expectEqual(@as(i32, 3), count_stmt.columnInt(0));
}

test "logWithStmt resets statement between calls" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    var stmt = try database.prepare(insert_sql);
    defer stmt.finalize();

    try logWithStmt(database, &stmt, "book", 1, "update", "name", "Old", "New", "admin", 1);
    try logWithStmt(database, &stmt, "account", 2, "create", null, null, null, "user1", 1);
    try database.commit();

    // Verify both entries have correct distinct data
    var q = try database.prepare("SELECT entity_type, entity_id, action FROM ledger_audit_log ORDER BY id;");
    defer q.finalize();

    _ = try q.step();
    try std.testing.expectEqualStrings("book", q.columnText(0).?);
    try std.testing.expectEqual(@as(i64, 1), q.columnInt64(1));
    try std.testing.expectEqualStrings("update", q.columnText(2).?);

    _ = try q.step();
    try std.testing.expectEqualStrings("account", q.columnText(0).?);
    try std.testing.expectEqual(@as(i64, 2), q.columnInt64(1));
    try std.testing.expectEqualStrings("create", q.columnText(2).?);
}

test "audit log timestamp auto-populated" {
    const database = try setupTestDb();
    defer database.close();

    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT performed_at FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    const ts = stmt.columnText(0).?;
    try std.testing.expect(ts.len >= 20);
    try std.testing.expect(ts[4] == '-');
    try std.testing.expect(ts[7] == '-');
    try std.testing.expect(ts[10] == 'T');
}

test "audit log entity_type at max length" {
    const database = try setupTestDb();
    defer database.close();

    const long_type = "abcdefghij" ** 5;
    try database.beginTransaction();
    try log(database, long_type, 1, "create", null, null, null, "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT entity_type FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings(long_type, stmt.columnText(0).?);
}

test "audit log performed_by at max length" {
    const database = try setupTestDb();
    defer database.close();

    const long_user = "u" ** 100;
    try database.beginTransaction();
    try log(database, "book", 1, "create", null, null, null, long_user, 1);
    try database.commit();

    var stmt = try database.prepare("SELECT performed_by FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings(long_user, stmt.columnText(0).?);
}

test "audit log old_value/new_value at max length" {
    const database = try setupTestDb();
    defer database.close();

    const long_val = "v" ** 4000;
    try database.beginTransaction();
    try log(database, "book", 1, "update", "field", long_val, long_val, "admin", 1);
    try database.commit();

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(usize, 4000), stmt.columnText(0).?.len);
    try std.testing.expectEqual(@as(usize, 4000), stmt.columnText(1).?.len);
}

test "hash chain populated on every audit insert" {
    const database = try setupTestDb();
    defer database.close();
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    try log(database, "book", 1, "update", "name", "Old", "New", "admin", 1);
    var stmt = try database.prepare("SELECT hash_chain FROM ledger_audit_log ORDER BY id;");
    defer stmt.finalize();
    _ = try stmt.step();
    const h1_raw = stmt.columnText(0).?;
    try std.testing.expectEqual(@as(usize, 64), h1_raw.len);
    var h1_copy: [64]u8 = undefined;
    @memcpy(&h1_copy, h1_raw[0..64]);
    _ = try stmt.step();
    const h2 = stmt.columnText(0).?;
    try std.testing.expectEqual(@as(usize, 64), h2.len);
    try std.testing.expect(!std.mem.eql(u8, &h1_copy, h2));
}

test "hash chain links from genesis hash" {
    const database = try setupTestDb();
    defer database.close();
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try log(database, "book", 1, "create", null, null, null, "admin", 1);
    var stmt = try database.prepare("SELECT hash_chain FROM ledger_audit_log WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    var h1_buf: [64]u8 = undefined;
    @memcpy(&h1_buf, stmt.columnText(0).?[0..64]);
    const expected = computeHash(genesis_hash, "book", 1, "create");
    try std.testing.expect(std.mem.eql(u8, &h1_buf, &expected));
}
