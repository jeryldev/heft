const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const subledger_mod = @import("subledger.zig");
const open_item_mod = @import("open_item.zig");
const money = @import("money.zig");
const oble_export = @import("oble_export.zig");

pub const ImportContext = struct {
    allocator: std.mem.Allocator,
    id_arena: std.heap.ArenaAllocator,
    book_ids: std.StringHashMap(i64),
    account_ids: std.StringHashMap(i64),
    period_ids: std.StringHashMap(i64),
    entry_ids: std.StringHashMap(i64),
    line_ids: std.StringHashMap(i64),
    counterparty_ids: std.StringHashMap(i64),
    open_item_ids: std.StringHashMap(i64),

    pub fn init(allocator: std.mem.Allocator) ImportContext {
        return .{
            .allocator = allocator,
            .id_arena = std.heap.ArenaAllocator.init(allocator),
            .book_ids = std.StringHashMap(i64).init(allocator),
            .account_ids = std.StringHashMap(i64).init(allocator),
            .period_ids = std.StringHashMap(i64).init(allocator),
            .entry_ids = std.StringHashMap(i64).init(allocator),
            .line_ids = std.StringHashMap(i64).init(allocator),
            .counterparty_ids = std.StringHashMap(i64).init(allocator),
            .open_item_ids = std.StringHashMap(i64).init(allocator),
        };
    }

    pub fn deinit(self: *ImportContext) void {
        self.book_ids.deinit();
        self.account_ids.deinit();
        self.period_ids.deinit();
        self.entry_ids.deinit();
        self.line_ids.deinit();
        self.counterparty_ids.deinit();
        self.open_item_ids.deinit();
        self.id_arena.deinit();
    }

    pub fn stableAllocator(self: *ImportContext) std.mem.Allocator {
        return self.id_arena.allocator();
    }
};

pub const ReversalPairImportIds = struct {
    original_entry_id: i64,
    reversal_entry_id: i64,
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
    entry_type: ?[]const u8 = null,
    reverses_entry_id: ?[]const u8 = null,
    lines: []const EntryLinePayload,
};

const ReversalPairPayload = struct {
    original_entry: EntryPayload,
    reversal_entry: EntryPayload,
};

const CounterpartyPayload = struct {
    id: []const u8,
    book_id: []const u8,
    number: []const u8,
    name: []const u8,
    role: []const u8,
    status: ?[]const u8 = null,
    control_account_id: ?[]const u8 = null,
};

const OpenItemLinePayload = struct {
    id: []const u8,
    line_number: i32,
    account_id: []const u8,
    debit_amount: []const u8,
    credit_amount: []const u8,
    counterparty_id: ?[]const u8 = null,
};

const OpenItemPayload = struct {
    id: []const u8,
    book_id: []const u8,
    entry_line_id: []const u8,
    counterparty_id: []const u8,
    original_amount: []const u8,
    remaining_amount: []const u8,
    status: []const u8,
    due_date: ?[]const u8 = null,
};

const CounterpartyOpenItemPayload = struct {
    counterparty: CounterpartyPayload,
    line: OpenItemLinePayload,
    open_item: OpenItemPayload,
};

const PolicyDesignationPayload = struct {
    rounding_account: ?[]const u8 = null,
    fx_gain_loss_account: ?[]const u8 = null,
    retained_earnings_account: ?[]const u8 = null,
    income_summary_account: ?[]const u8 = null,
    opening_balance_account: ?[]const u8 = null,
    suspense_account: ?[]const u8 = null,
    dividends_drawings_account: ?[]const u8 = null,
    current_year_earnings_account: ?[]const u8 = null,
};

const PolicyProfileNamePayload = struct {
    name: []const u8,
};

const PolicyProfilePayload = struct {
    book_id: []const u8,
    entity_type: []const u8,
    fy_start_month: i32,
    require_approval: bool,
    designations: PolicyDesignationPayload,
    policy_profiles: []const PolicyProfileNamePayload = &.{},
};

const BookSnapshotPayload = struct {
    book: BookPayload,
    accounts: []const AccountPayload,
    periods: []const PeriodPayload,
    counterparties: ?[]const CounterpartyPayload = null,
    policy_profile: ?PolicyProfilePayload = null,
};

fn putUnique(ctx: *ImportContext, map: *std.StringHashMap(i64), key: []const u8, value: i64) !void {
    const owned_key = try ctx.stableAllocator().dupe(u8, key);
    errdefer ctx.stableAllocator().free(owned_key);
    const gop = try map.getOrPut(owned_key);
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

fn parseCounterpartyStatus(text: []const u8) !subledger_mod.SubledgerAccountStatus {
    return subledger_mod.SubledgerAccountStatus.fromString(text) orelse error.InvalidInput;
}

fn parseEntityType(text: []const u8) !book_mod.EntityType {
    return book_mod.EntityType.fromString(text) orelse error.InvalidInput;
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

fn ensureSubledgerGroup(database: db.Database, book_id: i64, role: []const u8, gl_account_id: i64, performed_by: []const u8) !i64 {
    {
        var stmt = try database.prepare(
            \\SELECT id
            \\FROM ledger_subledger_groups
            \\WHERE book_id = ? AND type = ? AND gl_account_id = ?
            \\ORDER BY id ASC
            \\LIMIT 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, role);
        try stmt.bindInt(3, gl_account_id);
        if (try stmt.step()) return stmt.columnInt64(0);
    }

    var next_group_number: i32 = 1;
    {
        var stmt = try database.prepare("SELECT COALESCE(MAX(group_number), 0) + 1 FROM ledger_subledger_groups WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        next_group_number = stmt.columnInt(0);
    }

    const group_name = if (std.mem.eql(u8, role, "supplier")) "Imported Suppliers" else "Imported Customers";
    return subledger_mod.SubledgerGroup.create(database, book_id, group_name, role, next_group_number, gl_account_id, null, null, performed_by);
}

fn ensureCounterpartyImported(
    database: db.Database,
    ctx: *ImportContext,
    payload: CounterpartyPayload,
    gl_account_id: i64,
    performed_by: []const u8,
) !i64 {
    if (ctx.counterparty_ids.get(payload.id)) |existing_id| return existing_id;

    const book_id = try resolveId(&ctx.book_ids, payload.book_id);
    const group_id = try ensureSubledgerGroup(database, book_id, payload.role, gl_account_id, performed_by);
    const counterparty_id = try subledger_mod.SubledgerAccount.create(
        database,
        book_id,
        payload.number,
        payload.name,
        payload.role,
        group_id,
        performed_by,
    );
    try putUnique(ctx, &ctx.counterparty_ids, payload.id, counterparty_id);
    if (payload.status) |status| {
        const parsed = try parseCounterpartyStatus(status);
        if (parsed != .active) try subledger_mod.SubledgerAccount.updateStatus(database, counterparty_id, parsed, performed_by);
    }
    return counterparty_id;
}

pub fn importBookJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(BookPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try book_mod.Book.create(database, payload.name, payload.base_currency, payload.decimal_places, performed_by);
    try putUnique(ctx, &ctx.book_ids, payload.id, book_id);

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
        try putUnique(ctx, &ctx.account_ids, payload.id, account_id);
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
        try putUnique(ctx, &ctx.period_ids, payload.id, period_id);
        if (payload.status) |status| try applyPeriodStatus(database, period_id, status, performed_by);
    }
}

pub fn importEntryJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(EntryPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    return importEntryPayload(database, ctx, parsed.value, null, performed_by);
}

pub fn importCounterpartiesJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !void {
    var parsed = try std.json.parseFromSlice([]CounterpartyPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    for (parsed.value) |payload| {
        const gl_account_id = if (payload.control_account_id) |id|
            try resolveId(&ctx.account_ids, id)
        else
            return error.InvalidInput;
        _ = try ensureCounterpartyImported(database, ctx, payload, gl_account_id, performed_by);
    }
}

fn importEntryPayload(
    database: db.Database,
    ctx: *ImportContext,
    payload: EntryPayload,
    forced_status: ?[]const u8,
    performed_by: []const u8,
) !i64 {
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
    try putUnique(ctx, &ctx.entry_ids, payload.id, entry_id);

    var base_currency_buf: [8]u8 = undefined;
    const base_currency = try getBookBaseCurrency(database, book_id, &base_currency_buf);

    for (payload.lines) |line| {
        const account_id = try resolveId(&ctx.account_ids, line.account_id);
        const debit_amount = try money.parseDecimal(line.debit_amount, money.AMOUNT_SCALE);
        const credit_amount = try money.parseDecimal(line.credit_amount, money.AMOUNT_SCALE);
        const currency = line.transaction_currency orelse base_currency;
        const fx_rate = if (line.fx_rate) |rate| try money.parseDecimal(rate, money.FX_RATE_SCALE) else money.FX_RATE_SCALE;
        const counterparty_id = if (line.counterparty_id) |counterparty_key|
            try resolveId(&ctx.counterparty_ids, counterparty_key)
        else
            null;
        const line_id = try entry_mod.Entry.addLine(
            database,
            entry_id,
            line.line_number,
            debit_amount,
            credit_amount,
            currency,
            fx_rate,
            account_id,
            counterparty_id,
            null,
            performed_by,
        );
        try putUnique(ctx, &ctx.line_ids, line.id, line_id);
    }

    const target_status = forced_status orelse payload.status;
    if (std.mem.eql(u8, target_status, "posted")) {
        try entry_mod.Entry.post(database, entry_id, performed_by);
    } else if (!std.mem.eql(u8, target_status, "draft")) {
        return error.InvalidInput;
    }

    return entry_id;
}

pub fn importReversalPairJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !ReversalPairImportIds {
    var parsed = try std.json.parseFromSlice(ReversalPairPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const original_entry_id = try importEntryPayload(database, ctx, payload.original_entry, "posted", performed_by);

    if (payload.reversal_entry.reverses_entry_id) |reverses_entry_id| {
        if (!std.mem.eql(u8, reverses_entry_id, payload.original_entry.id)) return error.InvalidInput;
    }
    if (!std.mem.eql(u8, payload.reversal_entry.transaction_date, payload.reversal_entry.posting_date)) {
        return error.InvalidInput;
    }

    const reversal_period_id = try resolveId(&ctx.period_ids, payload.reversal_entry.period_id);
    const reversal_reason = payload.reversal_entry.description orelse return error.InvalidInput;
    const reversal_entry_id = try entry_mod.Entry.reverse(
        database,
        original_entry_id,
        reversal_reason,
        payload.reversal_entry.posting_date,
        reversal_period_id,
        performed_by,
    );
    try putUnique(ctx, &ctx.entry_ids, payload.reversal_entry.id, reversal_entry_id);

    if (payload.reversal_entry.document_number) |expected_doc| {
        var stmt = try database.prepare("SELECT document_number FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, reversal_entry_id);
        if (!try stmt.step()) return error.NotFound;
        const actual_doc = stmt.columnText(0) orelse return error.InvalidInput;
        if (!std.mem.eql(u8, actual_doc, expected_doc)) return error.InvalidInput;
    }

    return .{
        .original_entry_id = original_entry_id,
        .reversal_entry_id = reversal_entry_id,
    };
}

pub fn importCounterpartyOpenItemJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !struct { counterparty_id: i64, open_item_id: i64 } {
    var parsed = try std.json.parseFromSlice(CounterpartyOpenItemPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const control_account_id = try resolveId(&ctx.account_ids, payload.line.account_id);
    const counterparty_id = try ensureCounterpartyImported(database, ctx, payload.counterparty, control_account_id, performed_by);

    if (payload.line.counterparty_id) |line_counterparty_id| {
        if (!std.mem.eql(u8, line_counterparty_id, payload.counterparty.id)) return error.InvalidInput;
    }
    if (!std.mem.eql(u8, payload.open_item.counterparty_id, payload.counterparty.id)) return error.InvalidInput;
    if (!std.mem.eql(u8, payload.open_item.entry_line_id, payload.line.id)) return error.InvalidInput;

    const entry_line_id = try resolveId(&ctx.line_ids, payload.line.id);
    const book_id = try resolveId(&ctx.book_ids, payload.open_item.book_id);
    const original_amount = try money.parseDecimal(payload.open_item.original_amount, money.AMOUNT_SCALE);
    const remaining_amount = try money.parseDecimal(payload.open_item.remaining_amount, money.AMOUNT_SCALE);
    if (remaining_amount < 0 or remaining_amount > original_amount) return error.InvalidAmount;

    const open_item_id = try open_item_mod.createOpenItem(
        database,
        entry_line_id,
        counterparty_id,
        original_amount,
        payload.open_item.due_date,
        book_id,
        performed_by,
    );
    try putUnique(ctx, &ctx.open_item_ids, payload.open_item.id, open_item_id);

    const allocated_amount = original_amount - remaining_amount;
    if (allocated_amount > 0) {
        try open_item_mod.allocatePayment(database, open_item_id, allocated_amount, performed_by);
    }

    return .{
        .counterparty_id = counterparty_id,
        .open_item_id = open_item_id,
    };
}

pub fn importPolicyProfileJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(PolicyProfilePayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try resolveId(&ctx.book_ids, payload.book_id);

    try book_mod.Book.setEntityType(database, book_id, try parseEntityType(payload.entity_type), performed_by);
    try book_mod.Book.setFyStartMonth(database, book_id, payload.fy_start_month, performed_by);
    try book_mod.Book.setRequireApproval(database, book_id, payload.require_approval, performed_by);

    if (payload.designations.rounding_account) |id|
        try book_mod.Book.setRoundingAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.fx_gain_loss_account) |id|
        try book_mod.Book.setFxGainLossAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.retained_earnings_account) |id|
        try book_mod.Book.setRetainedEarningsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.income_summary_account) |id|
        try book_mod.Book.setIncomeSummaryAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.opening_balance_account) |id|
        try book_mod.Book.setOpeningBalanceAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.suspense_account) |id|
        try book_mod.Book.setSuspenseAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.dividends_drawings_account) |id|
        try book_mod.Book.setDividendsDrawingsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    if (payload.designations.current_year_earnings_account) |id|
        try book_mod.Book.setCurrentYearEarningsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);

    return book_id;
}

pub fn importBookSnapshotJson(database: db.Database, ctx: *ImportContext, json: []const u8, performed_by: []const u8) !i64 {
    var parsed = try std.json.parseFromSlice(BookSnapshotPayload, ctx.allocator, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const payload = parsed.value;
    const book_id = try book_mod.Book.create(database, payload.book.name, payload.book.base_currency, payload.book.decimal_places, performed_by);
    try putUnique(ctx, &ctx.book_ids, payload.book.id, book_id);

    if (payload.book.status) |status| {
        if (!std.mem.eql(u8, status, "active")) return error.InvalidInput;
    }

    for (payload.accounts) |account| {
        const account_type = try parseAccountType(account.account_type);
        const account_id = try account_mod.Account.create(database, book_id, account.number, account.name, account_type, false, performed_by);
        try putUnique(ctx, &ctx.account_ids, account.id, account_id);
        if (account.status) |status| try applyAccountStatus(database, account_id, status, performed_by);
    }

    for (payload.periods) |period| {
        const period_id = try period_mod.Period.create(
            database,
            book_id,
            period.name,
            period.period_number,
            period.year,
            period.start_date,
            period.end_date,
            "regular",
            performed_by,
        );
        try putUnique(ctx, &ctx.period_ids, period.id, period_id);
        if (period.status) |status| try applyPeriodStatus(database, period_id, status, performed_by);
    }

    if (payload.counterparties) |counterparties| {
        for (counterparties) |counterparty| {
            const gl_account_id = if (counterparty.control_account_id) |id|
                try resolveId(&ctx.account_ids, id)
            else
                return error.InvalidInput;
            _ = try ensureCounterpartyImported(database, ctx, counterparty, gl_account_id, performed_by);
        }
    }

    if (payload.policy_profile) |policy| {
        try book_mod.Book.setEntityType(database, book_id, try parseEntityType(policy.entity_type), performed_by);
        try book_mod.Book.setFyStartMonth(database, book_id, policy.fy_start_month, performed_by);
        try book_mod.Book.setRequireApproval(database, book_id, policy.require_approval, performed_by);

        if (policy.designations.rounding_account) |id|
            try book_mod.Book.setRoundingAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.fx_gain_loss_account) |id|
            try book_mod.Book.setFxGainLossAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.retained_earnings_account) |id|
            try book_mod.Book.setRetainedEarningsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.income_summary_account) |id|
            try book_mod.Book.setIncomeSummaryAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.opening_balance_account) |id|
            try book_mod.Book.setOpeningBalanceAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.suspense_account) |id|
            try book_mod.Book.setSuspenseAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.dividends_drawings_account) |id|
            try book_mod.Book.setDividendsDrawingsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
        if (policy.designations.current_year_earnings_account) |id|
            try book_mod.Book.setCurrentYearEarningsAccount(database, book_id, try resolveId(&ctx.account_ids, id), performed_by);
    }

    return book_id;
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

test "OBLE import: reversal pair example packet" {
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
    const reversal_json = try std.fs.cwd().readFileAlloc(allocator, "docs/oble/examples/reversal-pair.json", 16384);
    defer allocator.free(reversal_json);

    _ = try importBookJson(database, &ctx, book_json, "admin");
    try importAccountsJson(database, &ctx, accounts_json, "admin");
    try importPeriodsJson(database, &ctx, periods_json, "admin");
    const ids = try importReversalPairJson(database, &ctx, reversal_json, "admin");

    {
        var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, ids.original_entry_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("reversed", stmt.columnText(0).?);
    }

    {
        var stmt = try database.prepare("SELECT status, reverses_entry_id, document_number FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, ids.reversal_entry_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
        try std.testing.expectEqual(ids.original_entry_id, stmt.columnInt64(1));
        try std.testing.expectEqualStrings("REV-ACC-001", stmt.columnText(2).?);
    }
}

test "OBLE round-trip: export import export reversal pair" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Example Entity", "PHP", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "4000", "Accrual", .liability, false, "admin");

    _ = try period_mod.Period.create(source_db, book_id, "Dec 2025", 12, 2025, "2025-12-01", "2025-12-31", "regular", "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "ACC-001", "2026-01-15", "2026-01-15", "Accrual", period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 0, 250_00_000_000, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 250_00_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");
    _ = try entry_mod.Entry.reverse(source_db, entry_id, "Accrual reversal", "2026-01-31", period_id, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var periods_buf: [8192]u8 = undefined;
    var reversal_buf: [16384]u8 = undefined;

    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const periods_json = try oble_export.exportPeriodsJson(source_db, book_id, &periods_buf);
    const reversal_json = try oble_export.exportReversalPairJson(source_db, entry_id, &reversal_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookJson(target_db, &ctx, book_json, "admin");
    try importAccountsJson(target_db, &ctx, accounts_json, "admin");
    try importPeriodsJson(target_db, &ctx, periods_json, "admin");
    const imported_ids = try importReversalPairJson(target_db, &ctx, reversal_json, "admin");

    var round_reversal_buf: [16384]u8 = undefined;
    const round_reversal_json = try oble_export.exportReversalPairJson(target_db, imported_ids.original_entry_id, &round_reversal_buf);

    try std.testing.expect(imported_book_id > 0);
    try std.testing.expect(imported_ids.reversal_entry_id > imported_ids.original_entry_id);
    try std.testing.expectEqualStrings(reversal_json, round_reversal_json);
}

test "OBLE round-trip: export import export counterparty open item" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Example Entity", "PHP", 2, "admin");
    const ar_account_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_account_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_account_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", period_id, null, "admin");
    const receivable_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_account_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_account_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const open_item_id = try open_item_mod.createOpenItem(source_db, receivable_line_id, customer_id, 500_00_000_000, "2026-02-15", book_id, "admin");
    try open_item_mod.allocatePayment(source_db, open_item_id, 200_00_000_000, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var periods_buf: [8192]u8 = undefined;
    var entry_buf: [16384]u8 = undefined;
    var open_item_buf: [16384]u8 = undefined;

    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const periods_json = try oble_export.exportPeriodsJson(source_db, book_id, &periods_buf);
    const entry_json = try oble_export.exportEntryJson(source_db, entry_id, &entry_buf);
    const open_item_json = try oble_export.exportCounterpartyOpenItemJson(source_db, open_item_id, &open_item_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookJson(target_db, &ctx, book_json, "admin");
    try importAccountsJson(target_db, &ctx, accounts_json, "admin");
    try importPeriodsJson(target_db, &ctx, periods_json, "admin");
    {
        var parsed = try std.json.parseFromSlice(CounterpartyOpenItemPayload, allocator, open_item_json, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const control_account_id = try resolveId(&ctx.account_ids, parsed.value.line.account_id);
        _ = try ensureCounterpartyImported(target_db, &ctx, parsed.value.counterparty, control_account_id, "admin");
    }
    _ = try importEntryJson(target_db, &ctx, entry_json, "admin");
    _ = try importCounterpartyOpenItemJson(target_db, &ctx, open_item_json, "admin");

    var imported_open_item_id = ctx.open_item_ids.get("oi-1") orelse ctx.open_item_ids.get("oi-001") orelse 0;
    if (imported_open_item_id == 0) {
        var stmt = try target_db.prepare("SELECT id FROM ledger_open_items ORDER BY id ASC LIMIT 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        imported_open_item_id = stmt.columnInt64(0);
    }

    var round_open_item_buf: [16384]u8 = undefined;
    const round_open_item_json = try oble_export.exportCounterpartyOpenItemJson(target_db, imported_open_item_id, &round_open_item_buf);

    try std.testing.expect(imported_book_id > 0);
    try std.testing.expectEqualStrings(open_item_json, round_open_item_json);
}

test "OBLE round-trip: export import export counterparties collection" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Counterparty Source", "PHP", 2, "admin");
    const ar_account_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const ap_account_id = try account_mod.Account.create(source_db, book_id, "2000", "Accounts Payable", .liability, false, "admin");
    const customers_group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_account_id, null, null, "admin");
    const suppliers_group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Suppliers", "supplier", 2, ap_account_id, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", customers_group_id, "admin");
    _ = try subledger_mod.SubledgerAccount.create(source_db, book_id, "S001", "Supplier XYZ", "supplier", suppliers_group_id, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var counterparties_buf: [8192]u8 = undefined;
    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const counterparties_json = try oble_export.exportCounterpartiesJson(source_db, book_id, &counterparties_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookJson(target_db, &ctx, book_json, "admin");
    try importAccountsJson(target_db, &ctx, accounts_json, "admin");
    try importCounterpartiesJson(target_db, &ctx, counterparties_json, "admin");

    var round_counterparties_buf: [8192]u8 = undefined;
    const round_counterparties_json = try oble_export.exportCounterpartiesJson(target_db, imported_book_id, &round_counterparties_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_counterparties_json, "\"role\":\"customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_counterparties_json, "\"role\":\"supplier\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_counterparties_json, "\"control_account_id\":\"acct-") != null);
}

test "OBLE round-trip: export import export policy profile" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Policy Entity", "PHP", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const is_id = try account_mod.Account.create(source_db, book_id, "3200", "Income Summary", .equity, false, "admin");
    const ob_id = try account_mod.Account.create(source_db, book_id, "3300", "Opening Balance", .equity, false, "admin");
    const suspense_id = try account_mod.Account.create(source_db, book_id, "9999", "Suspense", .asset, false, "admin");
    const fx_id = try account_mod.Account.create(source_db, book_id, "7999", "FX Gain Loss", .revenue, false, "admin");
    const rounding_id = try account_mod.Account.create(source_db, book_id, "6999", "Rounding", .expense, false, "admin");
    const draws_id = try account_mod.Account.create(source_db, book_id, "3400", "Drawings", .equity, true, "admin");
    const cye_id = try account_mod.Account.create(source_db, book_id, "3210", "Current Year Earnings", .equity, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setIncomeSummaryAccount(source_db, book_id, is_id, "admin");
    try book_mod.Book.setOpeningBalanceAccount(source_db, book_id, ob_id, "admin");
    try book_mod.Book.setSuspenseAccount(source_db, book_id, suspense_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_id, "admin");
    try book_mod.Book.setRoundingAccount(source_db, book_id, rounding_id, "admin");
    try book_mod.Book.setDividendsDrawingsAccount(source_db, book_id, draws_id, "admin");
    try book_mod.Book.setCurrentYearEarningsAccount(source_db, book_id, cye_id, "admin");
    try book_mod.Book.setRequireApproval(source_db, book_id, true, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var policy_buf: [8192]u8 = undefined;

    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const policy_json = try oble_export.exportPolicyProfileJson(source_db, book_id, &policy_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookJson(target_db, &ctx, book_json, "admin");
    try importAccountsJson(target_db, &ctx, accounts_json, "admin");
    _ = try importPolicyProfileJson(target_db, &ctx, policy_json, "admin");

    try std.testing.expect(imported_book_id > 0);
    {
        var stmt = try target_db.prepare(
            \\SELECT entity_type, fy_start_month, require_approval,
            \\  rounding_account_id, fx_gain_loss_account_id,
            \\  retained_earnings_account_id, income_summary_account_id,
            \\  opening_balance_account_id, suspense_account_id,
            \\  dividends_drawings_account_id, current_year_earnings_account_id
            \\FROM ledger_books
            \\WHERE id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, imported_book_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("corporation", stmt.columnText(0).?);
        try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(1));
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(2));
        try std.testing.expectEqual(ctx.account_ids.get("acct-7").?, stmt.columnInt64(3));
        try std.testing.expectEqual(ctx.account_ids.get("acct-6").?, stmt.columnInt64(4));
        try std.testing.expectEqual(ctx.account_ids.get("acct-2").?, stmt.columnInt64(5));
        try std.testing.expectEqual(ctx.account_ids.get("acct-3").?, stmt.columnInt64(6));
        try std.testing.expectEqual(ctx.account_ids.get("acct-4").?, stmt.columnInt64(7));
        try std.testing.expectEqual(ctx.account_ids.get("acct-5").?, stmt.columnInt64(8));
        try std.testing.expectEqual(ctx.account_ids.get("acct-8").?, stmt.columnInt64(9));
        try std.testing.expectEqual(ctx.account_ids.get("acct-9").?, stmt.columnInt64(10));
    }

    var round_policy_buf: [8192]u8 = undefined;
    const round_policy_json = try oble_export.exportPolicyProfileJson(target_db, imported_book_id, &round_policy_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"fy_start_month\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"name\":\"income_summary_close\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_policy_json, "\"name\":\"approval_required\"") != null);
}

test "OBLE round-trip: export import export book snapshot" {
    const allocator = std.testing.allocator;

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Snapshot Source", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const customers_group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", customers_group_id, "admin");
    _ = cash_id;

    var snapshot_buf: [32768]u8 = undefined;
    const snapshot_json = try oble_export.exportBookSnapshotJson(source_db, book_id, &snapshot_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = ImportContext.init(allocator);
    defer ctx.deinit();

    const imported_book_id = try importBookSnapshotJson(target_db, &ctx, snapshot_json, "admin");

    var round_snapshot_buf: [32768]u8 = undefined;
    const round_snapshot_json = try oble_export.exportBookSnapshotJson(target_db, imported_book_id, &round_snapshot_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_snapshot_json, "\"book\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_snapshot_json, "\"counterparties\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_snapshot_json, "\"policy_profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_snapshot_json, "\"retained_earnings_account\"") != null);
}
