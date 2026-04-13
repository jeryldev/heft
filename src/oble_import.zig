const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");
const oble_export = @import("oble_export.zig");

pub const ImportContext = struct {
    allocator: std.mem.Allocator,
    book_ids: std.StringHashMap(i64),
    account_ids: std.StringHashMap(i64),
    period_ids: std.StringHashMap(i64),
    entry_ids: std.StringHashMap(i64),

    pub fn init(allocator: std.mem.Allocator) ImportContext {
        return .{
            .allocator = allocator,
            .book_ids = std.StringHashMap(i64).init(allocator),
            .account_ids = std.StringHashMap(i64).init(allocator),
            .period_ids = std.StringHashMap(i64).init(allocator),
            .entry_ids = std.StringHashMap(i64).init(allocator),
        };
    }

    pub fn deinit(self: *ImportContext) void {
        self.book_ids.deinit();
        self.account_ids.deinit();
        self.period_ids.deinit();
        self.entry_ids.deinit();
    }
};

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

const EntryLinePayload = struct {
    id: []const u8,
    line_number: i32,
    account_id: []const u8,
    debit_amount: []const u8,
    credit_amount: []const u8,
    transaction_currency: ?[]const u8 = null,
    fx_rate: ?[]const u8 = null,
    counterparty_id: ?[]const u8 = null,
};

const EntryPayload = struct {
    id: []const u8,
    book_id: []const u8,
    period_id: []const u8,
    status: []const u8,
    transaction_date: []const u8,
    posting_date: []const u8,
    document_number: ?[]const u8 = null,
    description: ?[]const u8 = null,
    lines: []const EntryLinePayload,
};

fn putUnique(map: *std.StringHashMap(i64), key: []const u8, value: i64) !void {
    const gop = try map.getOrPut(key);
    if (gop.found_existing) return error.DuplicateNumber;
    gop.value_ptr.* = value;
}

fn resolveId(map: *const std.StringHashMap(i64), key: []const u8) !i64 {
    return map.get(key) orelse error.NotFound;
}

fn parseAccountType(text: []const u8) !account_mod.AccountType {
    return account_mod.AccountType.fromString(text) orelse error.InvalidInput;
}

fn parseAccountStatus(text: []const u8) !account_mod.AccountStatus {
    return account_mod.AccountStatus.fromString(text) orelse error.InvalidInput;
}

fn parsePeriodStatus(text: []const u8) !period_mod.PeriodStatus {
    return period_mod.PeriodStatus.fromString(text) orelse error.InvalidInput;
}

fn applyAccountStatus(database: db.Database, account_id: i64, status: []const u8, performed_by: []const u8) !void {
    const parsed = try parseAccountStatus(status);
    if (parsed == .active) return;
    try account_mod.Account.updateStatus(database, account_id, parsed, performed_by);
}

fn applyPeriodStatus(database: db.Database, period_id: i64, status: []const u8, performed_by: []const u8) !void {
    const parsed = try parsePeriodStatus(status);
    switch (parsed) {
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

fn getBookBaseCurrency(database: db.Database, book_id: i64, buf: []u8) ![]const u8 {
    var stmt = try database.prepare("SELECT base_currency FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (!try stmt.step()) return error.NotFound;
    const currency = stmt.columnText(0) orelse return error.InvalidInput;
    if (currency.len > buf.len) return error.InvalidInput;
    @memcpy(buf[0..currency.len], currency);
    return buf[0..currency.len];
}

pub fn importBookJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(BookPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try book_mod.Book.create(database, payload.name, payload.base_currency, payload.decimal_places, performed_by);
    try putUnique(&ctx.book_ids, payload.id, book_id);

    if (payload.status) |status| {
        if (!std.mem.eql(u8, status, "active")) return error.InvalidInput;
    }
    return book_id;
}

pub fn importAccountsJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    var parsed = try std.json.parseFromSlice([]AccountPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    for (parsed.value) |payload| {
        const book_id = try resolveId(&ctx.book_ids, payload.book_id);
        const account_type = try parseAccountType(payload.account_type);
        const account_id = try account_mod.Account.create(database, book_id, payload.number, payload.name, account_type, false, performed_by);
        try putUnique(&ctx.account_ids, payload.id, account_id);
        if (payload.status) |status| try applyAccountStatus(database, account_id, status, performed_by);
    }
}

pub fn importPeriodsJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    var parsed = try std.json.parseFromSlice([]PeriodPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    for (parsed.value) |payload| {
        const book_id = try resolveId(&ctx.book_ids, payload.book_id);
        const period_id = try period_mod.Period.create(
            database,
            book_id,
            payload.name,
            payload.period_number,
            payload.year,
            payload.start_date,
            payload.end_date,
            "regular",
            performed_by,
        );
        try putUnique(&ctx.period_ids, payload.id, period_id);
        if (payload.status) |status| try applyPeriodStatus(database, period_id, status, performed_by);
    }
}

pub fn importEntryJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(EntryPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try resolveId(&ctx.book_ids, payload.book_id);
    const period_id = try resolveId(&ctx.period_ids, payload.period_id);
    const document_number = payload.document_number orelse return error.InvalidInput;

    const entry_id = try entry_mod.Entry.createDraft(
        database,
        book_id,
        document_number,
        payload.transaction_date,
        payload.posting_date,
        payload.description,
        period_id,
        null,
        performed_by,
    );
    errdefer _ = ctx.entry_ids.remove(payload.id);
    try putUnique(&ctx.entry_ids, payload.id, entry_id);

    var base_currency_buf: [8]u8 = undefined;
    const base_currency = try getBookBaseCurrency(database, book_id, &base_currency_buf);

    for (payload.lines) |line| {
        if (line.counterparty_id != null) return error.InvalidInput;
        const account_id = try resolveId(&ctx.account_ids, line.account_id);
        const debit_amount = try money.parseDecimal(line.debit_amount, money.AMOUNT_SCALE);
        const credit_amount = try money.parseDecimal(line.credit_amount, money.AMOUNT_SCALE);
        const currency = line.transaction_currency orelse base_currency;
        const fx_rate = if (line.fx_rate) |rate| try money.parseDecimal(rate, money.FX_RATE_SCALE) else money.FX_RATE_SCALE;
        _ = try entry_mod.Entry.addLine(
            database,
            entry_id,
            line.line_number,
            debit_amount,
            credit_amount,
            currency,
            fx_rate,
            account_id,
            null,
            null,
            performed_by,
        );
    }

    if (std.mem.eql(u8, payload.status, "posted")) {
        try entry_mod.Entry.post(database, entry_id, performed_by);
    } else if (!std.mem.eql(u8, payload.status, "draft")) {
        return error.InvalidInput;
    }

    return entry_id;
}

// ── Tests ───────────────────────────────────────────────────────

test "OBLE import: core example packet" {
    const allocator = std.testing.allocator;
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const book_json = try std.fs.cwd().readFileAlloc(allocator, "docs/oble/examples/core-book.json", 4096);
    defer allocator.free(book_json);
    const accounts_json = try std.fs.cwd().readFileAlloc(allocator, "docs/oble/examples/core-accounts.json", 8192);
    defer allocator.free(accounts_json);
    const periods_json = try std.fs.cwd().readFileAlloc(allocator, "docs/oble/examples/core-periods.json", 8192);
    defer allocator.free(periods_json);
    const entry_json = try std.fs.cwd().readFileAlloc(allocator, "docs/oble/examples/core-entry-posted.json", 8192);
    defer allocator.free(entry_json);

    const book_id = try importBookJson(database, &ctx, book_json, "admin");
    try importAccountsJson(database, &ctx, accounts_json, "admin");
    try importPeriodsJson(database, &ctx, periods_json, "admin");
    const entry_id = try importEntryJson(database, &ctx, entry_json, "admin");

    try std.testing.expect(book_id > 0);
    try std.testing.expect(entry_id > 0);
    try std.testing.expectEqual(@as(?i64, book_id), ctx.book_ids.get("book-001"));
    try std.testing.expectEqual(@as(?i64, entry_id), ctx.entry_ids.get("entry-2026-01-001"));

    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_accounts WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }

    {
        var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, ctx.period_ids.get("period-2025-12").?);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("closed", stmt.columnText(0).?);
    }

    {
        var stmt = try database.prepare("SELECT status, document_number FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("JE-001", stmt.columnText(1).?);
    }
}

test "OBLE round-trip: export import export core packet" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Example Entity", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(source_db, book_id, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");

    const closed_period_id = try period_mod.Period.create(source_db, book_id, "Dec 2025", 12, 2025, "2025-12-01", "2025-12-31", "regular", "admin");
    try period_mod.Period.transition(source_db, closed_period_id, .soft_closed, "admin");
    try period_mod.Period.transition(source_db, closed_period_id, .closed, "admin");

    const open_period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "JE-001", "2026-01-10", "2026-01-10", "Owner capital injection", open_period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 1_000_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 1_000_00_000_000, "PHP", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var periods_buf: [8192]u8 = undefined;
    var entry_buf: [8192]u8 = undefined;

    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const periods_json = try oble_export.exportPeriodsJson(source_db, book_id, &periods_buf);
    const entry_json = try oble_export.exportEntryJson(source_db, entry_id, &entry_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookJson(target_db, &ctx, book_json, "admin");
    try importAccountsJson(target_db, &ctx, accounts_json, "admin");
    try importPeriodsJson(target_db, &ctx, periods_json, "admin");
    const imported_entry_id = try importEntryJson(target_db, &ctx, entry_json, "admin");

    var round_book_buf: [4096]u8 = undefined;
    var round_accounts_buf: [8192]u8 = undefined;
    var round_periods_buf: [8192]u8 = undefined;
    var round_entry_buf: [8192]u8 = undefined;

    const round_book_json = try oble_export.exportBookJson(target_db, imported_book_id, &round_book_buf);
    const round_accounts_json = try oble_export.exportAccountsJson(target_db, imported_book_id, &round_accounts_buf);
    const round_periods_json = try oble_export.exportPeriodsJson(target_db, imported_book_id, &round_periods_buf);
    const round_entry_json = try oble_export.exportEntryJson(target_db, imported_entry_id, &round_entry_buf);

    try std.testing.expectEqualStrings(book_json, round_book_json);
    try std.testing.expectEqualStrings(accounts_json, round_accounts_json);
    try std.testing.expectEqualStrings(periods_json, round_periods_json);
    try std.testing.expectEqualStrings(entry_json, round_entry_json);
}
