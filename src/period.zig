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

pub const PeriodGranularity = enum {
    monthly,
    quarterly,
    semi_annual,
    annual,

    pub fn periodCount(self: PeriodGranularity) u8 {
        return switch (self) {
            .monthly => 12,
            .quarterly => 4,
            .semi_annual => 2,
            .annual => 1,
        };
    }

    pub fn monthsPerPeriod(self: PeriodGranularity) u8 {
        return switch (self) {
            .monthly => 1,
            .quarterly => 3,
            .semi_annual => 6,
            .annual => 12,
        };
    }

    pub fn fromString(s: []const u8) ?PeriodGranularity {
        const map = .{
            .{ "monthly", PeriodGranularity.monthly },
            .{ "quarterly", PeriodGranularity.quarterly },
            .{ "semi_annual", PeriodGranularity.semi_annual },
            .{ "annual", PeriodGranularity.annual },
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

        // Overlap detection for regular periods
        if (std.mem.eql(u8, period_type, "regular")) {
            var overlap_stmt = try database.prepare(
                \\SELECT COUNT(*) FROM ledger_periods
                \\WHERE book_id = ? AND period_type = 'regular'
                \\  AND start_date <= ? AND end_date >= ?;
            );
            defer overlap_stmt.finalize();
            try overlap_stmt.bindInt(1, book_id);
            try overlap_stmt.bindText(2, end_date);
            try overlap_stmt.bindText(3, start_date);
            _ = try overlap_stmt.step();
            if (overlap_stmt.columnInt(0) > 0) return error.InvalidInput;
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

    const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    fn isLeapYear(y: i32) bool {
        const year: u32 = @intCast(y);
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
    }

    fn lastDay(month: u8, year: i32) u8 {
        if (month == 2 and isLeapYear(year)) return 29;
        return days_in_month[month - 1];
    }

    fn calendarMonth(start_month: u8, offset: u8) u8 {
        return (start_month + offset - 1) % 12 + 1;
    }

    fn calendarYear(fiscal_year: i32, start_month: u8, actual_month: u8) u32 {
        if (start_month == 1) return @intCast(fiscal_year);
        return if (actual_month >= start_month) @as(u32, @intCast(fiscal_year)) - 1 else @intCast(fiscal_year);
    }

    pub fn bulkCreate(database: db.Database, book_id: i64, fiscal_year: i32, start_month: i32, granularity: PeriodGranularity, performed_by: []const u8) !void {
        if (start_month < 1 or start_month > 12) return error.InvalidInput;

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

        const period_count = granularity.periodCount();
        const months_per = granularity.monthsPerPeriod();
        const sm: u8 = @intCast(start_month);

        for (0..period_count) |p| {
            const period_num: u8 = @intCast(p + 1);
            const month_offset: u8 = @intCast(p * months_per);

            // Start date: first day of the first month in this period
            const first_month = calendarMonth(sm, month_offset);
            const first_year = calendarYear(fiscal_year, sm, first_month);

            // End date: last day of the last month in this period
            const last_month_offset: u8 = month_offset + months_per - 1;
            const end_month = calendarMonth(sm, last_month_offset);
            const end_year = calendarYear(fiscal_year, sm, end_month);
            const end_day = lastDay(end_month, @intCast(end_year));

            const start_date = std.fmt.bufPrint(&start_buf, "{d:0>4}-{d:0>2}-01", .{ first_year, first_month }) catch unreachable;
            const end_date = std.fmt.bufPrint(&end_buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ end_year, end_month, end_day }) catch unreachable;
            const period_name = std.fmt.bufPrint(&name_buf, "P{d} FY{d}", .{ period_num, @as(u32, @intCast(fiscal_year)) }) catch unreachable;

            try insert_stmt.bindText(1, period_name);
            try insert_stmt.bindInt(2, @intCast(period_num));
            try insert_stmt.bindInt(3, @intCast(fiscal_year));
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
const account_mod = @import("account.zig");

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
    // Same dates trigger overlap detection before UNIQUE constraint
    const result = Period.create(database, 1, "Also January", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expectError(error.InvalidInput, result);
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

    try Period.bulkCreate(database, 1, 2026, 1, .monthly, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
}

test "bulkCreate monthly sets correct dates for Jan, Feb, Dec" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .monthly, "admin");

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
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 12;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-12-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-12-31", stmt.columnText(1).?);
    }
}

test "bulkCreate writes audit log for each period" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .monthly, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'period';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
}

test "bulkCreate is atomic — all or nothing" {
    const database = try setupTestDb();
    defer database.close();

    // Create period 1 manually first, so bulk create of 12 will conflict on period_number 1
    _ = try Period.create(database, 1, "Already Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // bulkCreate should fail because period 1 already exists
    const result = Period.bulkCreate(database, 1, 2026, 1, .monthly, "admin");
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

    const result = Period.bulkCreate(database, 999, 2026, 1, .monthly, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "bulkCreate handles leap year February" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2024, 1, .monthly, "admin");

    var stmt = try database.prepare("SELECT end_date FROM ledger_periods WHERE period_number = 2;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2024-02-29", stmt.columnText(0).?);
}

// ── Non-calendar fiscal year tests ──────────────────────────────

test "bulkCreate monthly FY Apr-Mar: dates span two calendar years" {
    const database = try setupTestDb();
    defer database.close();

    // FY2027 starting April: Apr 2026 - Mar 2027
    try Period.bulkCreate(database, 1, 2027, 4, .monthly, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
}

test "bulkCreate monthly FY Apr-Mar: period 1 is April, period 12 is March" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 4, .monthly, "admin");

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-04-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-04-30", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 12;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2027-03-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2027-03-31", stmt.columnText(1).?);
    }
}

test "bulkCreate monthly FY Jul-Jun (Australia)" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 7, .monthly, "admin");

    {
        var stmt = try database.prepare("SELECT start_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-07-01", stmt.columnText(0).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 12;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2027-06-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2027-06-30", stmt.columnText(1).?);
    }
}

test "bulkCreate monthly FY Oct-Sep (US Federal)" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 10, .monthly, "admin");

    {
        var stmt = try database.prepare("SELECT start_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-10-01", stmt.columnText(0).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 12;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2027-09-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2027-09-30", stmt.columnText(1).?);
    }
}

test "bulkCreate non-calendar FY with leap year Feb" {
    const database = try setupTestDb();
    defer database.close();

    // FY2024 starting Oct: period 5 = Feb 2024 (leap year)
    try Period.bulkCreate(database, 1, 2024, 10, .monthly, "admin");

    var stmt = try database.prepare("SELECT end_date FROM ledger_periods WHERE period_number = 5;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2024-02-29", stmt.columnText(0).?);
}

// ── Quarterly tests ─────────────────────────────────────────────

test "bulkCreate quarterly calendar year: 4 periods" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .quarterly, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
}

test "bulkCreate quarterly calendar year: correct date ranges" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .quarterly, "admin");

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-01-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-03-31", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 4;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-10-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-12-31", stmt.columnText(1).?);
    }
}

test "bulkCreate quarterly FY Apr: Q1=Apr-Jun, Q4=Jan-Mar" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 4, .quarterly, "admin");

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-04-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-06-30", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 4;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2027-01-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2027-03-31", stmt.columnText(1).?);
    }
}

// ── Semi-annual tests ───────────────────────────────────────────

test "bulkCreate semi-annual calendar year: 2 periods" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .semi_annual, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "bulkCreate semi-annual calendar year: correct date ranges" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .semi_annual, "admin");

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-01-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-06-30", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-07-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-12-31", stmt.columnText(1).?);
    }
}

test "bulkCreate semi-annual FY Jul: H1=Jul-Dec, H2=Jan-Jun" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 7, .semi_annual, "admin");

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2026-07-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2026-12-31", stmt.columnText(1).?);
    }

    {
        var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("2027-01-01", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("2027-06-30", stmt.columnText(1).?);
    }
}

// ── Annual tests ────────────────────────────────────────────────

test "bulkCreate annual calendar year: 1 period" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .annual, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "bulkCreate annual calendar year: Jan-Dec" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .annual, "admin");

    var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2026-01-01", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("2026-12-31", stmt.columnText(1).?);
}

test "bulkCreate annual FY Apr: Apr-Mar" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2027, 4, .annual, "admin");

    var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE period_number = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2026-04-01", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("2027-03-31", stmt.columnText(1).?);
}

// ── Validation tests ────────────────────────────────────────────

test "bulkCreate rejects start_month 0" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.bulkCreate(database, 1, 2026, 0, .monthly, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "bulkCreate rejects start_month 13" {
    const database = try setupTestDb();
    defer database.close();

    const result = Period.bulkCreate(database, 1, 2026, 13, .monthly, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "adjustment period coexists with bulk monthly" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .monthly, "admin");
    _ = try Period.create(database, 1, "Year-End Adj", 13, 2026, "2026-12-01", "2026-12-31", "adjustment", "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 13), stmt.columnInt(0));
}

test "adjustment period coexists with bulk quarterly" {
    const database = try setupTestDb();
    defer database.close();

    try Period.bulkCreate(database, 1, 2026, 1, .quarterly, "admin");
    _ = try Period.create(database, 1, "Year-End Adj", 13, 2026, "2026-12-01", "2026-12-31", "adjustment", "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 5), stmt.columnInt(0));
}

// ── Period overlap detection tests ───────────────────────────────

test "regular periods with overlapping dates rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Overlapping dates with different period_number
    const result = Period.create(database, 1, "Mid Jan", 2, 2026, "2026-01-15", "2026-02-15", "regular", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "adjustment period overlapping regular period allowed" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "Dec 2026", 12, 2026, "2026-12-01", "2026-12-31", "regular", "admin");

    // Adjustment period with same dates — allowed
    _ = try Period.create(database, 1, "Year-End Adj", 13, 2026, "2026-12-01", "2026-12-31", "adjustment", "admin");
}

test "regular periods in different books can overlap" {
    const database = try setupTestDb();
    defer database.close();

    _ = try book.Book.create(database, "Book B", "USD", 2, "admin");

    _ = try Period.create(database, 1, "Jan A", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try Period.create(database, 2, "Jan B", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
}

test "non-overlapping regular periods accepted" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
}

// ── Account status lifecycle tests ──────────────────────────────

test "account archived rejects further transitions" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try account_mod.Account.updateStatus(database, 1, "archived", "admin");

    const to_active = account_mod.Account.updateStatus(database, 1, "active", "admin");
    try std.testing.expectError(error.InvalidTransition, to_active);

    const to_inactive = account_mod.Account.updateStatus(database, 1, "inactive", "admin");
    try std.testing.expectError(error.InvalidTransition, to_inactive);
}

test "account full lifecycle: active -> inactive -> active -> archived" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    try account_mod.Account.updateStatus(database, 1, "inactive", "admin");
    try account_mod.Account.updateStatus(database, 1, "active", "admin");
    try account_mod.Account.updateStatus(database, 1, "archived", "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_accounts WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
}

test "account same-status transition rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    const result = account_mod.Account.updateStatus(database, 1, "active", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

// ── Audit chronological tests ───────────────────────────────────

test "audit records for multiple transitions in chronological order" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Period.create(database, 1, "January 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");
    try Period.transition(database, 1, .closed, "admin");
    try Period.transition(database, 1, .locked, "admin");

    var stmt = try database.prepare(
        \\SELECT old_value, new_value FROM ledger_audit_log
        \\WHERE entity_type = 'period' AND action = 'transition'
        \\ORDER BY id;
    );
    defer stmt.finalize();

    _ = try stmt.step();
    try std.testing.expectEqualStrings("open", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("soft_closed", stmt.columnText(1).?);

    _ = try stmt.step();
    try std.testing.expectEqualStrings("soft_closed", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("closed", stmt.columnText(1).?);

    _ = try stmt.step();
    try std.testing.expectEqualStrings("closed", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("locked", stmt.columnText(1).?);
}

test "full lifecycle audit trail: book -> account -> period -> transitions" {
    const database = try setupTestDb();
    defer database.close();

    // setupTestDb already created a book (id=1)
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try Period.transition(database, 1, .soft_closed, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log ORDER BY id;");
    defer stmt.finalize();

    _ = try stmt.step();
    try std.testing.expectEqualStrings("book", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    _ = try stmt.step();
    try std.testing.expectEqualStrings("account", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    _ = try stmt.step();
    try std.testing.expectEqualStrings("period", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    _ = try stmt.step();
    try std.testing.expectEqualStrings("period", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("transition", stmt.columnText(1).?);
}
