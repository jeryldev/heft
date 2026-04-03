const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");

pub const PeriodStatus = enum {
    open,
    soft_closed,
    closed,
    locked,

    pub fn canTransitionTo(self: PeriodStatus, target: PeriodStatus) bool {
        return switch (self) {
            .open => target == .soft_closed,
            .soft_closed => target == .closed or target == .open,
            .closed => target == .locked or target == .open,
            .locked => false,
        };
    }

    pub fn toString(self: PeriodStatus) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?PeriodStatus {
        const map = .{
            .{ "open", PeriodStatus.open },
            .{ "soft_closed", PeriodStatus.soft_closed },
            .{ "closed", PeriodStatus.closed },
            .{ "locked", PeriodStatus.locked },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Period = struct {
    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, period_type, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?);
    ;

    const valid_types = [_][]const u8{ "regular", "adjustment" };

    fn isValidType(period_type: []const u8) bool {
        for (valid_types) |t| {
            if (std.mem.eql(u8, period_type, t)) return true;
        }
        return false;
    }

    pub fn create(database: db.Database, book_id: i64, name: []const u8, period_number: i32, year: i32, start_date: []const u8, end_date: []const u8, period_type: []const u8, performed_by: []const u8) !i64 {
        if (name.len == 0) return error.InvalidInput;
        if (period_number < 1 or period_number > 16) return error.InvalidInput;
        if (!isValidType(period_type)) return error.InvalidInput;

        // Verify book exists
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, name);
        try stmt.bindInt(2, @intCast(period_number));
        try stmt.bindInt(3, @intCast(year));
        try stmt.bindText(4, start_date);
        try stmt.bindText(5, end_date);
        try stmt.bindText(6, period_type);
        try stmt.bindInt(7, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "period", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    pub fn transition(database: db.Database, period_id: i64, target_status: PeriodStatus, performed_by: []const u8) !void {
        // Fetch current status and book_id
        var old_status_buf: [20]u8 = undefined;
        var old_status_len: usize = 0;
        var current: PeriodStatus = undefined;
        var period_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status_str = stmt.columnText(0).?;
            @memcpy(old_status_buf[0..status_str.len], status_str);
            old_status_len = status_str.len;
            current = PeriodStatus.fromString(status_str) orelse return error.InvalidInput;
            period_book_id = stmt.columnInt64(1);
        }
        const old_status = old_status_buf[0..old_status_len];

        if (current == .locked) return error.PeriodLocked;
        if (!current.canTransitionTo(target_status)) return error.InvalidTransition;

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("UPDATE ledger_periods SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, target_status.toString());
            try stmt.bindInt(2, period_id);
            _ = try stmt.step();
        }

        try audit.log(database, "period", period_id, "transition", "status", old_status, target_status.toString(), performed_by, period_book_id);

        try database.commit();
    }

    const month_names = [_][]const u8{
        "January", "February", "March",     "April",   "May",      "June",
        "July",    "August",   "September", "October", "November", "December",
    };

    const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    fn isLeapYear(y: i32) bool {
        const year: u32 = @intCast(y);
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
    }

    fn lastDay(month: u8, year: i32) u8 {
        if (month == 2 and isLeapYear(year)) return 29;
        return days_in_month[month - 1];
    }

    pub fn bulkCreate(database: db.Database, book_id: i64, year: i32, month_count: i32, performed_by: []const u8) !void {
        if (month_count < 1 or month_count > 12) return error.InvalidInput;

        // Verify book exists
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var insert_stmt = try database.prepare(create_sql);
        defer insert_stmt.finalize();

        const audit_sql: [*:0]const u8 =
            \\INSERT INTO ledger_audit_log
            \\  (entity_type, entity_id, action, performed_by, book_id)
            \\VALUES ('period', ?, 'create', ?, ?);
        ;
        var audit_stmt = try database.prepare(audit_sql);
        defer audit_stmt.finalize();

        var start_buf: [16]u8 = undefined;
        var end_buf: [16]u8 = undefined;
        var name_buf: [48]u8 = undefined;

        const count: u8 = @intCast(month_count);
        for (1..@as(u9, count) + 1) |m| {
            const month: u8 = @intCast(m);
            const last = lastDay(month, year);

            const yr: u32 = @intCast(year);
            const start_date = std.fmt.bufPrint(&start_buf, "{d:0>4}-{d:0>2}-01", .{ yr, month }) catch unreachable;
            const end_date = std.fmt.bufPrint(&end_buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yr, month, last }) catch unreachable;
            const period_name = std.fmt.bufPrint(&name_buf, "{s} {d}", .{ month_names[month - 1], yr }) catch unreachable;

            try insert_stmt.bindText(1, period_name);
            try insert_stmt.bindInt(2, @intCast(month));
            try insert_stmt.bindInt(3, @intCast(year));
            try insert_stmt.bindText(4, start_date);
            try insert_stmt.bindText(5, end_date);
            try insert_stmt.bindText(6, "regular");
            try insert_stmt.bindInt(7, book_id);

            _ = insert_stmt.step() catch return error.DuplicateNumber;
            insert_stmt.reset();
            insert_stmt.clearBindings();

            const id = database.lastInsertRowId();
            try audit_stmt.bindInt(1, id);
            try audit_stmt.bindText(2, performed_by);
            try audit_stmt.bindInt(3, book_id);
            _ = try audit_stmt.step();
            audit_stmt.reset();
            audit_stmt.clearBindings();
        }

        try database.commit();
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

// ── PeriodStatus state machine tests ────────────────────────────

test "PeriodStatus: open -> soft_closed allowed" {
    try std.testing.expect(PeriodStatus.open.canTransitionTo(.soft_closed));
}

test "PeriodStatus: open -> closed not allowed" {
    try std.testing.expect(!PeriodStatus.open.canTransitionTo(.closed));
}

test "PeriodStatus: open -> locked not allowed" {
    try std.testing.expect(!PeriodStatus.open.canTransitionTo(.locked));
}

test "PeriodStatus: open -> open not allowed" {
    try std.testing.expect(!PeriodStatus.open.canTransitionTo(.open));
}

test "PeriodStatus: soft_closed -> closed allowed" {
    try std.testing.expect(PeriodStatus.soft_closed.canTransitionTo(.closed));
}

test "PeriodStatus: soft_closed -> open allowed (revert)" {
    try std.testing.expect(PeriodStatus.soft_closed.canTransitionTo(.open));
}

test "PeriodStatus: soft_closed -> locked not allowed" {
    try std.testing.expect(!PeriodStatus.soft_closed.canTransitionTo(.locked));
}

test "PeriodStatus: closed -> locked allowed" {
    try std.testing.expect(PeriodStatus.closed.canTransitionTo(.locked));
}

test "PeriodStatus: closed -> open allowed (emergency reopen)" {
    try std.testing.expect(PeriodStatus.closed.canTransitionTo(.open));
}

test "PeriodStatus: closed -> soft_closed not allowed" {
    try std.testing.expect(!PeriodStatus.closed.canTransitionTo(.soft_closed));
}

test "PeriodStatus: locked -> any not allowed" {
    try std.testing.expect(!PeriodStatus.locked.canTransitionTo(.open));
    try std.testing.expect(!PeriodStatus.locked.canTransitionTo(.soft_closed));
    try std.testing.expect(!PeriodStatus.locked.canTransitionTo(.closed));
    try std.testing.expect(!PeriodStatus.locked.canTransitionTo(.locked));
}

test "PeriodStatus: fromString round-trips all variants" {
    try std.testing.expectEqual(PeriodStatus.open, PeriodStatus.fromString("open").?);
    try std.testing.expectEqual(PeriodStatus.soft_closed, PeriodStatus.fromString("soft_closed").?);
    try std.testing.expectEqual(PeriodStatus.closed, PeriodStatus.fromString("closed").?);
    try std.testing.expectEqual(PeriodStatus.locked, PeriodStatus.fromString("locked").?);
    try std.testing.expect(PeriodStatus.fromString("invalid") == null);
}

// ── create period tests ─────────────────────────────────────────

test "create period returns auto-generated id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create period stores correct fields" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var stmt = try database.prepare("SELECT name, period_number, year, start_date, end_date, period_type, status FROM ledger_periods WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("January 2026", stmt.columnText(0).?);
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(1));
    try std.testing.expectEqual(@as(i32, 2026), stmt.columnInt(2));
    try std.testing.expectEqualStrings("2026-01-01", stmt.columnText(3).?);
    try std.testing.expectEqualStrings("2026-01-31", stmt.columnText(4).?);
    try std.testing.expectEqualStrings("regular", stmt.columnText(5).?);
    try std.testing.expectEqualStrings("open", stmt.columnText(6).?);
}

test "create period writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'period';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("period", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
}

test "create period rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.create(database, 1, "", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create period rejects period_number out of range" {
    const database = try setupTestDb();
    defer database.close();

    const zero = Period.create(database, 1, "Bad", 0, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.InvalidInput, zero);

    const seventeen = Period.create(database, 1, "Bad", 17, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.InvalidInput, seventeen);
}

test "create period rejects invalid period_type" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.create(database, 1, "Bad", 1, 2026, "2026-01-01", "2026-01-31", "special", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create period rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.create(database, 999, "January", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "create period rejects duplicate period_number+year in same book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const result = Period.create(database, 1, "Also January", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "create adjustment period accepted" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Period.create(database, 1, "Year-End Adj 1", 13, 2026, "2026-12-01", "2026-12-31", "adjustment", "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

// ── transition tests ────────────────────────────────────────────

test "transition period from open to soft_closed" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("soft_closed", stmt.columnText(0).?);
}

test "transition through full lifecycle: open -> soft_closed -> closed -> locked" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");
    try Period.transition(database, 1, .closed, "admin");
    try Period.transition(database, 1, .locked, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("locked", stmt.columnText(0).?);
}

test "transition writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'period' AND action = 'transition';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("status", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("open", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("soft_closed", stmt.columnText(2).?);
}

test "transition rejects invalid transition (open -> locked)" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const result = Period.transition(database, 1, .locked, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "transition rejects locked -> anything" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");
    try Period.transition(database, 1, .closed, "admin");
    try Period.transition(database, 1, .locked, "admin");

    const result = Period.transition(database, 1, .open, "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

test "transition allows soft_closed -> open (revert)" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");
    try Period.transition(database, 1, .open, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("open", stmt.columnText(0).?);
}

test "transition allows closed -> open (emergency reopen)" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");
    try Period.transition(database, 1, .closed, "admin");
    try Period.transition(database, 1, .open, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("open", stmt.columnText(0).?);
}

test "transition rejects nonexistent period" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.transition(database, 999, .soft_closed, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── bulkCreate tests ────────────────────────────────────────────

test "bulkCreate creates 12 monthly periods" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 12, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
}

test "bulkCreate sets correct period numbers and dates" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 3, "admin");

    {
        var stmt = try database.prepare("SELECT period_number, start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
        try std.testing.expectEqualStrings("2026-01-01", stmt.columnText(1).?);
        try std.testing.expectEqualStrings("2026-01-31", stmt.columnText(2).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-02-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-02-28", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 3;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-03-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-03-31", stmt.columnText(1).?);
    }
}

test "bulkCreate writes audit log for each period" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 3, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'period';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

test "bulkCreate is atomic — all or nothing" {
    const database = try setupTestDb();
    defer database.close();

    // Create period 1 manually first, so bulk create of 12 will conflict on period_number 1
    _ = try Period.create(database, 1, "Already Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // bulkCreate should fail because period 1 already exists
    const result = Period.bulkCreate(database, 1, 2026, 12, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);

    // Only the manually created period should exist (bulk was rolled back)
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "bulkCreate rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.bulkCreate(database, 999, 2026, 12, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "bulkCreate handles leap year February" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2024, 2, "admin");

    var stmt = try database.prepare("SELECT end_date FROM ledger_periods WHERE period_number = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2024-02-29", stmt.columnText(0).?);
}
