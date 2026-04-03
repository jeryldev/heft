const std = @import("std");
const db = @import("db.zig");

const insert_sql: [*:0]const u8 =
    \\INSERT INTO ledger_audit_log
    \\  (entity_type, entity_id, action, field_changed, old_value, new_value, performed_by, book_id)
    \\VALUES (?, ?, ?, ?, ?, ?, ?, ?);
;

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

    try stmt.bindText(1, entity_type);
    try stmt.bindInt(2, entity_id);
    try stmt.bindText(3, action);
    if (field_changed) |f| try stmt.bindText(4, f) else try stmt.bindNull(4);
    if (old_value) |o| try stmt.bindText(5, o) else try stmt.bindNull(5);
    if (new_value) |n| try stmt.bindText(6, n) else try stmt.bindNull(6);
    try stmt.bindText(7, performed_by);
    try stmt.bindInt(8, book_id);

    _ = try stmt.step();
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
