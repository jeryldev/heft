const std = @import("std");
const db = @import("db.zig");
const oble_export = @import("oble_export.zig");
const oble_import = @import("oble_import.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

pub const ImportContext = oble_import.ImportContext;

const BookPayload = struct {
    id: []const u8,
    name: []const u8,
    base_currency: []const u8,
    decimal_places: u8,
    status: ?[]const u8 = null,
};

const AccountPayload = struct {
    id: []const u8,
    book_id: []const u8,
    number: []const u8,
    name: []const u8,
    account_type: []const u8,
    status: ?[]const u8 = null,
};

const PeriodPayload = struct {
    id: []const u8,
    book_id: []const u8,
    name: []const u8,
    start_date: []const u8,
    end_date: []const u8,
    status: ?[]const u8 = null,
    period_number: i32,
    year: i32,
};

const CoreBundlePayload = struct {
    book: BookPayload,
    accounts: []const AccountPayload,
    periods: []const PeriodPayload,
};

pub fn exportBookJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportBookJson(database, book_id, buf);
}

pub fn exportAccountsJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportAccountsJson(database, book_id, buf);
}

pub fn exportPeriodsJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportPeriodsJson(database, book_id, buf);
}

pub fn exportEntryJson(database: db.Database, entry_id: i64, buf: []u8) ![]u8 {
    return oble_export.exportEntryJson(database, entry_id, buf);
}

pub fn importBookJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    return oble_import.importBookJson(database, ctx, json, performed_by);
}

pub fn importAccountsJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    return oble_import.importAccountsJson(database, ctx, json, performed_by);
}

pub fn importPeriodsJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    return oble_import.importPeriodsJson(database, ctx, json, performed_by);
}

pub fn importEntryJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    return oble_import.importEntryJson(database, ctx, json, performed_by);
}

pub fn exportCoreBundleJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    const book_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(book_buf);
    const accounts_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(accounts_buf);
    const periods_buf = try std.heap.c_allocator.alloc(u8, buf.len);
    defer std.heap.c_allocator.free(periods_buf);

    const book_json = try oble_export.exportBookJson(database, book_id, book_buf);
    const accounts_json = try oble_export.exportAccountsJson(database, book_id, accounts_buf);
    const periods_json = try oble_export.exportPeriodsJson(database, book_id, periods_buf);

    return std.fmt.bufPrint(buf, "{{\"book\":{s},\"accounts\":{s},\"periods\":{s}}}", .{
        book_json,
        accounts_json,
        periods_json,
    }) catch return error.BufferTooSmall;
}

pub fn importCoreBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    try oble_import.validateImportPayload(json);
    var parsed = try std.json.parseFromSlice(CoreBundlePayload, std.heap.c_allocator, json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    if (ctx.book_ids.contains(parsed.value.book.id)) return error.DuplicateNumber;

    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    const book_id = try book_mod.Book.create(
        database,
        parsed.value.book.name,
        parsed.value.book.base_currency,
        parsed.value.book.decimal_places,
        performed_by,
    );
    try putUnique(ctx, &ctx.book_ids, parsed.value.book.id, book_id);
    if (parsed.value.book.status) |status| {
        if (!std.mem.eql(u8, status, "active")) return error.InvalidInput;
    }

    for (parsed.value.accounts) |payload| {
        const mapped_book_id = try resolveId(&ctx.book_ids, payload.book_id);
        const account_type = account_mod.AccountType.fromString(payload.account_type) orelse return error.InvalidInput;
        const account_id = try account_mod.Account.create(database, mapped_book_id, payload.number, payload.name, account_type, false, performed_by);
        try putUnique(ctx, &ctx.account_ids, payload.id, account_id);
        if (payload.status) |status| {
            const parsed_status = account_mod.AccountStatus.fromString(status) orelse return error.InvalidInput;
            if (parsed_status != .active) try account_mod.Account.updateStatus(database, account_id, parsed_status, performed_by);
        }
    }

    for (parsed.value.periods) |payload| {
        const mapped_book_id = try resolveId(&ctx.book_ids, payload.book_id);
        const period_id = try period_mod.Period.create(
            database,
            mapped_book_id,
            payload.name,
            payload.period_number,
            payload.year,
            payload.start_date,
            payload.end_date,
            "regular",
            performed_by,
        );
        try putUnique(ctx, &ctx.period_ids, payload.id, period_id);
        if (payload.status) |status| {
            const parsed_status = period_mod.PeriodStatus.fromString(status) orelse return error.InvalidInput;
            switch (parsed_status) {
                .open => {},
                .soft_closed => try period_mod.Period.transition(database, period_id, .soft_closed, performed_by),
                .closed => {
                    try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
                    try period_mod.Period.transition(database, period_id, .closed, performed_by);
                },
                .locked => {
                    try period_mod.Period.transition(database, period_id, .soft_closed, performed_by);
                    try period_mod.Period.transition(database, period_id, .closed, performed_by);
                    try period_mod.Period.transition(database, period_id, .locked, performed_by);
                },
            }
        }
    }

    if (owns_txn) try database.commit();
    return book_id;
}

test "OBLE core: export and import core bundle round-trips" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try source_db.exec("PRAGMA foreign_keys = ON;");
    try schema.createAll(source_db);

    const book_id = try @import("book.zig").Book.create(source_db, "Core Book", "USD", 2, "admin");
    _ = try @import("account.zig").Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try @import("account.zig").Account.create(source_db, book_id, "2000", "Payables", .liability, false, "admin");
    _ = try @import("period.zig").Period.create(source_db, book_id, "FY2026-P01", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var bundle_buf: [512 * 1024]u8 = undefined;
    const exported = try exportCoreBundleJson(source_db, book_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try target_db.exec("PRAGMA foreign_keys = ON;");
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    const imported_book_id = try importCoreBundleJson(target_db, &ctx, exported, "admin");

    var round_buf: [512 * 1024]u8 = undefined;
    const round_trip = try exportCoreBundleJson(target_db, imported_book_id, &round_buf);

    try std.testing.expectEqualStrings(exported, round_trip);
}

test "OBLE core: duplicate core import leaves no partial side effects" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try source_db.exec("PRAGMA foreign_keys = ON;");
    try schema.createAll(source_db);

    const book_id = try @import("book.zig").Book.create(source_db, "Core Book", "USD", 2, "admin");
    _ = try @import("account.zig").Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try @import("period.zig").Period.create(source_db, book_id, "FY2026-P01", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var bundle_buf: [512 * 1024]u8 = undefined;
    const exported = try exportCoreBundleJson(source_db, book_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try target_db.exec("PRAGMA foreign_keys = ON;");
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    _ = try importCoreBundleJson(target_db, &ctx, exported, "admin");
    try std.testing.expectError(error.DuplicateNumber, importCoreBundleJson(target_db, &ctx, exported, "admin"));

    var stmt = try target_db.prepare("SELECT COUNT(*) FROM ledger_books;");
    defer stmt.finalize();
    try std.testing.expect(try stmt.step());
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

fn putUnique(ctx: *ImportContext, map: *std.StringHashMap(i64), key: []const u8, value: i64) !void {
    if (map.contains(key)) return error.DuplicateNumber;
    if (map.count() >= ctx.max_ids_per_kind) return error.TooManyImportIds;

    const owned_key = try ctx.stableAllocator().dupe(u8, key);
    errdefer ctx.stableAllocator().free(owned_key);
    try map.putNoClobber(owned_key, value);
}

fn resolveId(map: *const std.StringHashMap(i64), key: []const u8) !i64 {
    return map.get(key) orelse error.NotFound;
}
