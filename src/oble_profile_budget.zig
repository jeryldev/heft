const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const budget_mod = @import("budget.zig");
const oble_core = @import("oble_core.zig");
const oble_import = @import("oble_import.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

pub const ImportContext = oble_import.ImportContext;

const BudgetPayload = struct {
    id: []const u8,
    book_id: []const u8,
    name: []const u8,
    fiscal_year: i32,
    status: []const u8,
};

const BudgetLinePayload = struct {
    id: []const u8,
    budget_id: []const u8,
    account_id: []const u8,
    period_id: []const u8,
    amount: i64,
};

const BudgetProfileBundlePayload = struct {
    budget: BudgetPayload,
    budget_lines: []const BudgetLinePayload,
};

pub fn exportBudgetProfileBundleJson(database: db.Database, budget_id: i64, buf: []u8) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT id, book_id, name, fiscal_year, status
        \\FROM ledger_budgets
        \\WHERE id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    if (!try stmt.step()) return error.NotFound;

    const book_id = stmt.columnInt64(1);
    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"budget\":");
    pos += try writeBudgetJson(
        buf[pos..],
        budget_id,
        book_id,
        stmt.columnText(2).?,
        stmt.columnInt(3),
        stmt.columnText(4).?,
    );

    try appendLiteral(buf, &pos, ",\"budget_lines\":[");

    var line_stmt = try database.prepare(
        \\SELECT bl.id, bl.account_id, bl.period_id, bl.amount, p.period_number, p.year
        \\FROM ledger_budget_lines bl
        \\JOIN ledger_periods p ON p.id = bl.period_id
        \\WHERE budget_id = ?
        \\ORDER BY bl.period_id ASC, bl.account_id ASC, bl.id ASC;
    );
    defer line_stmt.finalize();
    try line_stmt.bindInt(1, budget_id);

    var first = true;
    while (try line_stmt.step()) {
        if (!first) try appendLiteral(buf, &pos, ",");
        first = false;
        pos += try writeBudgetLineJson(
            buf[pos..],
            line_stmt.columnInt64(0),
            budget_id,
            line_stmt.columnInt64(1),
            line_stmt.columnInt64(3),
            line_stmt.columnInt(4),
            line_stmt.columnInt(5),
        );
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn importBudgetProfileBundleJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    try oble_import.validateImportPayload(json);
    var parsed = try std.json.parseFromSlice(BudgetProfileBundlePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try resolveId(&ctx.book_ids, payload.budget.book_id);
    const budget_id = try budget_mod.Budget.create(
        database,
        book_id,
        payload.budget.name,
        payload.budget.fiscal_year,
        performed_by,
    );
    try putUnique(ctx, &ctx.budget_ids, payload.budget.id, budget_id);

    for (payload.budget_lines) |line| {
        const line_id = try budget_mod.BudgetLine.set(
            database,
            try resolveId(&ctx.budget_ids, line.budget_id),
            try resolveId(&ctx.account_ids, line.account_id),
            try resolveId(&ctx.period_ids, line.period_id),
            line.amount,
            performed_by,
        );
        try putUnique(ctx, &ctx.budget_line_ids, line.id, line_id);
    }

    const target_status = budget_mod.BudgetStatus.fromString(payload.budget.status) orelse return error.InvalidInput;
    switch (target_status) {
        .draft => {},
        .approved => try budget_mod.Budget.transition(database, budget_id, .approved, performed_by),
        .closed => {
            try budget_mod.Budget.transition(database, budget_id, .approved, performed_by);
            try budget_mod.Budget.transition(database, budget_id, .closed, performed_by);
        },
    }

    return budget_id;
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

fn writeBudgetJson(buf: []u8, budget_id: i64, book_id: i64, name: []const u8, fiscal_year: i32, status: []const u8) !usize {
    var pos: usize = 0;
    const prefix = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"id\":\"budget-{d}\",\"book_id\":\"book-{d}\",\"name\":\"",
        .{ budget_id, book_id },
    );
    pos += prefix.len;
    pos += try jsonString(buf[pos..], name);
    const suffix = try std.fmt.bufPrint(buf[pos..], "\",\"fiscal_year\":{d},\"status\":\"", .{fiscal_year});
    pos += suffix.len;
    pos += try jsonString(buf[pos..], status);
    try appendLiteral(buf, &pos, "\"}");
    return pos;
}

fn writeBudgetLineJson(
    buf: []u8,
    budget_line_id: i64,
    budget_id: i64,
    account_id: i64,
    amount: i64,
    period_number: i32,
    fiscal_year: i32,
) !usize {
    const period_no: u32 = @intCast(period_number);
    const rendered = try std.fmt.bufPrint(
        buf,
        "{{\"id\":\"budget-line-{d}\",\"budget_id\":\"budget-{d}\",\"account_id\":\"acct-{d}\",\"period_id\":\"period-{d}-{d:0>2}\",\"amount\":{d}}}",
        .{ budget_line_id, budget_id, account_id, fiscal_year, period_no, amount },
    );
    return rendered.len;
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}

fn jsonString(buf: []u8, text: []const u8) !usize {
    var pos: usize = 0;
    for (text) |c| {
        switch (c) {
            '"', '\\' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = c;
                pos += 2;
            },
            '\n' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = 'n';
                pos += 2;
            },
            else => {
                if (pos + 1 > buf.len) return error.BufferTooSmall;
                buf[pos] = c;
                pos += 1;
            },
        }
    }
    return pos;
}

test "OBLE budget profile: export and import bundle round-trips" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Budget Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const budget_id = try budget_mod.Budget.create(source_db, book_id, "FY2026 Plan", 2026, "admin");
    _ = try budget_mod.BudgetLine.set(source_db, budget_id, cash_id, jan_id, 10_000_000_000, "admin");
    _ = try budget_mod.BudgetLine.set(source_db, budget_id, revenue_id, feb_id, 25_000_000_000, "admin");
    try budget_mod.Budget.transition(source_db, budget_id, .approved, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var budget_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const budget_json = try exportBudgetProfileBundleJson(source_db, budget_id, &budget_buf);
    try std.testing.expect(std.mem.indexOf(u8, budget_json, "\"period_id\":\"period-2026-01\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, budget_json, "\"period_id\":\"period-2026-02\"") != null);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(std.heap.c_allocator);
    defer ctx.deinit();

    _ = try oble_core.importCoreBundleJson(target_db, &ctx, core_json, "admin");
    const imported_budget_id = try importBudgetProfileBundleJson(target_db, &ctx, budget_json, "admin");

    var round_buf: [256 * 1024]u8 = undefined;
    const round_json = try exportBudgetProfileBundleJson(target_db, imported_budget_id, &round_buf);
    try std.testing.expectEqualStrings(budget_json, round_json);
}
