const std = @import("std");
const db = @import("db.zig");
const cache = @import("cache.zig");

pub const MAX_REPORT_ROWS: usize = 50_000;

pub fn ensureFreshCache(database: db.Database, book_id: i64, sql: [*:0]const u8, binds: anytype) !void {
    var stmt = try database.prepare(sql);
    defer stmt.finalize();
    inline for (binds, 0..) |bind, i| {
        const col: c_int = @intCast(i + 1);
        switch (@TypeOf(bind)) {
            i64 => try stmt.bindInt(col, bind),
            []const u8 => try stmt.bindText(col, bind),
            else => @compileError("unsupported bind type"),
        }
    }
    var period_ids: [200]i64 = undefined;
    var count: usize = 0;
    while (try stmt.step()) {
        period_ids[count] = stmt.columnInt64(0);
        count += 1;
        if (count >= period_ids.len) {
            _ = try cache.recalculateStale(database, book_id, period_ids[0..count]);
            count = 0;
        }
    }
    if (count > 0) {
        _ = try cache.recalculateStale(database, book_id, period_ids[0..count]);
    }
}

pub const ReportRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    account_type: [16]u8,
    account_type_len: usize,
    debit_balance: i64,
    credit_balance: i64,
};

pub const ReportResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []ReportRow,
    total_debits: i64,
    total_credits: i64,
    decimal_places: u8,
    truncated: bool = false,

    pub fn deinit(self: *ReportResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

pub const TransactionRow = struct {
    posting_date: [10]u8,
    posting_date_len: usize,
    document_number: [100]u8,
    document_number_len: usize,
    description: [1001]u8,
    description_len: usize,
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    debit_amount: i64,
    credit_amount: i64,
    running_balance: i64,
    transaction_currency: [4]u8,
    transaction_currency_len: usize,
    transaction_debit: i64,
    transaction_credit: i64,
    fx_rate: i64,
};

pub const LedgerResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []TransactionRow,
    opening_balance: i64,
    closing_balance: i64,
    total_debits: i64,
    total_credits: i64,
    decimal_places: u8,
    truncated: bool = false,

    pub fn deinit(self: *LedgerResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

pub const ComparativeReportRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    account_type: [16]u8,
    account_type_len: usize,
    current_debit: i64,
    current_credit: i64,
    prior_debit: i64,
    prior_credit: i64,
    variance_debit: i64,
    variance_credit: i64,
};

pub const ComparativeReportResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []ComparativeReportRow,
    current_total_debits: i64,
    current_total_credits: i64,
    prior_total_debits: i64,
    prior_total_credits: i64,
    decimal_places: u8 = 2,
    truncated: bool = false,

    pub fn deinit(self: *ComparativeReportResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

pub const EquityRow = struct {
    account_id: i64,
    account_number: [50]u8,
    account_number_len: usize,
    account_name: [256]u8,
    account_name_len: usize,
    opening_balance: i64,
    period_activity: i64,
    closing_balance: i64,
};

pub const EquityResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []EquityRow,
    net_income: i64,
    total_opening: i64,
    total_closing: i64,
    decimal_places: u8 = 2,
    truncated: bool = false,

    pub fn deinit(self: *EquityResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

pub const RunningMode = enum { none, debit_normal, credit_normal };

pub fn copyText(dest: []u8, src: ?[]const u8) usize {
    const s = src orelse return 0;
    const len = @min(s.len, dest.len);
    @memcpy(dest[0..len], s[0..len]);
    return len;
}

pub fn buildLedgerResult(database: db.Database, sql: [*:0]const u8, binds: anytype, running_mode: RunningMode) !*LedgerResult {
    const result = try std.heap.c_allocator.create(LedgerResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(TransactionRow){};
    var stmt = try database.prepare(sql);
    defer stmt.finalize();

    inline for (binds, 0..) |bind, i| {
        const col: c_int = @intCast(i + 1);
        switch (@TypeOf(bind)) {
            i64 => try stmt.bindInt(col, bind),
            []const u8 => try stmt.bindText(col, bind),
            else => @compileError("unsupported bind type"),
        }
    }

    var total_debits: i64 = 0;
    var total_credits: i64 = 0;
    var running: i64 = 0;

    while (try stmt.step()) {
        var row: TransactionRow = undefined;
        row.posting_date_len = copyText(&row.posting_date, stmt.columnText(0));
        row.document_number_len = copyText(&row.document_number, stmt.columnText(1));
        row.description_len = copyText(&row.description, stmt.columnText(2));
        row.account_id = stmt.columnInt64(3);
        row.account_number_len = copyText(&row.account_number, stmt.columnText(4));
        row.account_name_len = copyText(&row.account_name, stmt.columnText(5));
        row.debit_amount = stmt.columnInt64(6);
        row.credit_amount = stmt.columnInt64(7);
        row.transaction_currency_len = copyText(&row.transaction_currency, stmt.columnText(8));
        row.transaction_debit = stmt.columnInt64(9);
        row.transaction_credit = stmt.columnInt64(10);
        row.fx_rate = stmt.columnInt64(11);

        switch (running_mode) {
            .debit_normal => {
                const delta = std.math.sub(i64, row.debit_amount, row.credit_amount) catch return error.AmountOverflow;
                running = std.math.add(i64, running, delta) catch return error.AmountOverflow;
                row.running_balance = running;
            },
            .credit_normal => {
                const delta = std.math.sub(i64, row.credit_amount, row.debit_amount) catch return error.AmountOverflow;
                running = std.math.add(i64, running, delta) catch return error.AmountOverflow;
                row.running_balance = running;
            },
            .none => row.running_balance = 0,
        }

        total_debits = std.math.add(i64, total_debits, row.debit_amount) catch return error.AmountOverflow;
        total_credits = std.math.add(i64, total_credits, row.credit_amount) catch return error.AmountOverflow;
        try rows.append(allocator, row);
        if (rows.items.len >= MAX_REPORT_ROWS) break;
    }

    result.truncated = rows.items.len >= MAX_REPORT_ROWS;
    result.rows = try rows.toOwnedSlice(allocator);
    result.opening_balance = 0;
    result.closing_balance = running;
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    return result;
}

pub fn buildReportResult(database: db.Database, sql: [*:0]const u8, binds: anytype) !*ReportResult {
    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ReportRow){};
    var stmt = try database.prepare(sql);
    defer stmt.finalize();

    inline for (binds, 0..) |bind, i| {
        const col: c_int = @intCast(i + 1);
        switch (@TypeOf(bind)) {
            i64 => try stmt.bindInt(col, bind),
            []const u8 => try stmt.bindText(col, bind),
            else => @compileError("unsupported bind type"),
        }
    }

    var total_debits: i64 = 0;
    var total_credits: i64 = 0;

    while (try stmt.step()) {
        var row: ReportRow = undefined;
        row.account_id = stmt.columnInt64(0);
        row.account_number_len = copyText(&row.account_number, stmt.columnText(1));
        row.account_name_len = copyText(&row.account_name, stmt.columnText(2));
        row.account_type_len = copyText(&row.account_type, stmt.columnText(3));

        const normal = stmt.columnText(4).?;
        const debit_sum = stmt.columnInt64(5);
        const credit_sum = stmt.columnInt64(6);

        if (std.mem.eql(u8, normal, "debit")) {
            row.debit_balance = std.math.sub(i64, debit_sum, credit_sum) catch return error.AmountOverflow;
            row.credit_balance = 0;
            if (row.debit_balance < 0) {
                row.credit_balance = std.math.negate(row.debit_balance) catch return error.AmountOverflow;
                row.debit_balance = 0;
            }
        } else {
            row.credit_balance = std.math.sub(i64, credit_sum, debit_sum) catch return error.AmountOverflow;
            row.debit_balance = 0;
            if (row.credit_balance < 0) {
                row.debit_balance = std.math.negate(row.credit_balance) catch return error.AmountOverflow;
                row.credit_balance = 0;
            }
        }

        total_debits = std.math.add(i64, total_debits, row.debit_balance) catch return error.AmountOverflow;
        total_credits = std.math.add(i64, total_credits, row.credit_balance) catch return error.AmountOverflow;
        try rows.append(allocator, row);
        if (rows.items.len >= MAX_REPORT_ROWS) break;
    }

    result.truncated = rows.items.len >= MAX_REPORT_ROWS;
    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    return result;
}

pub fn verifyBookExists(database: db.Database, book_id: i64) !void {
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    if (stmt.columnInt(0) == 0) return error.NotFound;
}

pub fn getDecimalPlaces(database: db.Database, book_id: i64) !u8 {
    var stmt = try database.prepare("SELECT decimal_places FROM ledger_books WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    _ = try stmt.step();
    const dp = stmt.columnInt(0);
    return if (dp >= 0 and dp <= 8) @intCast(dp) else 2;
}
