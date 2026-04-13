const std = @import("std");
const db = @import("db.zig");
const book_mod = @import("book.zig");
const common = @import("report_common.zig");

pub const ReportRow = common.ReportRow;
pub const ReportResult = common.ReportResult;
const buildReportResult = common.buildReportResult;
const ensureFreshCache = common.ensureFreshCache;
const verifyBookExists = common.verifyBookExists;
const getDecimalPlaces = common.getDecimalPlaces;
const copyText = common.copyText;
const MAX_REPORT_ROWS = common.MAX_REPORT_ROWS;

const tb_sql: [*:0]const u8 =
    \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
    \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
    \\FROM ledger_account_balances ab
    \\JOIN ledger_accounts a ON a.id = ab.account_id
    \\JOIN ledger_periods p ON p.id = ab.period_id
    \\WHERE ab.book_id = ? AND p.end_date <= ?
    \\GROUP BY a.id
    \\ORDER BY a.number;
;

pub fn trialBalance(database: db.Database, book_id: i64, as_of_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, as_of_date });
    const result = try buildReportResult(database, tb_sql, .{ book_id, as_of_date });
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}

const is_sql: [*:0]const u8 =
    \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
    \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
    \\FROM ledger_account_balances ab
    \\JOIN ledger_accounts a ON a.id = ab.account_id
    \\JOIN ledger_periods p ON p.id = ab.period_id
    \\WHERE ab.book_id = ?
    \\  AND p.start_date >= ? AND p.end_date <= ?
    \\  AND a.account_type IN ('revenue', 'expense')
    \\GROUP BY a.id
    \\ORDER BY a.number;
;

pub fn incomeStatement(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, start_date, end_date });

    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ReportRow){};
    var stmt = try database.prepare(is_sql);
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);

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
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}

const tbm_sql: [*:0]const u8 =
    \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
    \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
    \\FROM ledger_account_balances ab
    \\JOIN ledger_accounts a ON a.id = ab.account_id
    \\JOIN ledger_periods p ON p.id = ab.period_id
    \\WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
    \\GROUP BY a.id
    \\ORDER BY a.number ASC;
;

pub fn trialBalanceMovement(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, start_date, end_date });
    const result = try buildReportResult(database, tbm_sql, .{ book_id, start_date, end_date });
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}

const ni_sql: [*:0]const u8 =
    \\SELECT a.account_type, SUM(ab.debit_sum), SUM(ab.credit_sum)
    \\FROM ledger_account_balances ab
    \\JOIN ledger_accounts a ON a.id = ab.account_id
    \\JOIN ledger_periods p ON p.id = ab.period_id
    \\WHERE ab.book_id = ?
    \\  AND p.start_date >= ? AND p.end_date <= ?
    \\  AND a.account_type IN ('revenue', 'expense')
    \\GROUP BY a.account_type;
;

const bs_sql: [*:0]const u8 =
    \\SELECT a.id, a.number, a.name, a.account_type, a.normal_balance,
    \\  SUM(ab.debit_sum), SUM(ab.credit_sum)
    \\FROM ledger_account_balances ab
    \\JOIN ledger_accounts a ON a.id = ab.account_id
    \\JOIN ledger_periods p ON p.id = ab.period_id
    \\WHERE ab.book_id = ? AND p.end_date <= ?
    \\  AND a.account_type IN ('asset', 'liability', 'equity')
    \\GROUP BY a.id
    \\ORDER BY a.number;
;

pub fn balanceSheetAuto(database: db.Database, book_id: i64, as_of_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, as_of_date });
    const result = try buildReportResult(database, bs_sql, .{ book_id, as_of_date });
    errdefer result.deinit();
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}

/// Sprint D.6: Plain balance sheet that returns only real A/L/E rows.
/// Per Rule 1, every line must correspond to a posted journal entry.
/// Mid-period balance sheets against unclosed periods will NOT balance
/// (A != L+E) — call closePeriod first to materialize closing entries.
pub fn balanceSheet(database: db.Database, book_id: i64, as_of_date: []const u8) !*ReportResult {
    return balanceSheetAuto(database, book_id, as_of_date);
}

/// Convenience for callers (typically interim management reporting) who
/// want a self-balancing balance sheet without first closing the period.
/// Synthesizes a "current period earnings" row from rev/exp net activity
/// and injects it into the equity totals. Does NOT mutate state — purely
/// presentation-layer projection.
pub fn balanceSheetAutoWithProjectedRE(database: db.Database, book_id: i64, as_of_date: []const u8) !*ReportResult {
    var fy_month: i32 = 1;
    {
        var stmt = try database.prepare("SELECT fy_start_month FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        fy_month = stmt.columnInt(0);
    }
    const fy_start = try book_mod.Book.getFyStartDate(as_of_date, fy_month);
    return balanceSheetWithProjectedRE(database, book_id, as_of_date, &fy_start);
}

pub fn balanceSheetWithProjectedRE(database: db.Database, book_id: i64, as_of_date: []const u8, fy_start_date: []const u8) !*ReportResult {
    try verifyBookExists(database, book_id);
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, as_of_date });

    // Get A/L/E rows using shared builder
    const result = try buildReportResult(database, bs_sql, .{ book_id, as_of_date });
    errdefer result.deinit();

    // Compute net income (revenue - expense) for the fiscal year
    var net_income: i64 = 0;
    {
        var stmt = try database.prepare(ni_sql);
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, fy_start_date);
        try stmt.bindText(3, as_of_date);

        while (try stmt.step()) {
            const acct_type = stmt.columnText(0).?;
            const debits = stmt.columnInt64(1);
            const credits = stmt.columnInt64(2);
            if (std.mem.eql(u8, acct_type, "revenue")) {
                const delta = std.math.sub(i64, credits, debits) catch return error.AmountOverflow;
                net_income = std.math.add(i64, net_income, delta) catch return error.AmountOverflow;
            } else {
                const exp_delta = std.math.sub(i64, debits, credits) catch return error.AmountOverflow;
                net_income = std.math.sub(i64, net_income, exp_delta) catch return error.AmountOverflow;
            }
        }
    }

    // Add synthetic net income row when retained_earnings_account_id is designated
    var re_account_id: ?i64 = null;
    {
        var re_stmt = try database.prepare("SELECT retained_earnings_account_id FROM ledger_books WHERE id = ?;");
        defer re_stmt.finalize();
        try re_stmt.bindInt(1, book_id);
        _ = try re_stmt.step();
        const re_id = re_stmt.columnInt64(0);
        if (re_id > 0) re_account_id = re_id;
    }

    if (re_account_id) |re_id| {
        if (net_income != 0) {
            var acct_stmt = try database.prepare("SELECT number, name FROM ledger_accounts WHERE id = ?;");
            defer acct_stmt.finalize();
            try acct_stmt.bindInt(1, re_id);
            if (try acct_stmt.step()) {
                var ni_row: ReportRow = std.mem.zeroes(ReportRow);
                ni_row.account_id = re_id;
                ni_row.account_number_len = copyText(&ni_row.account_number, acct_stmt.columnText(0));
                ni_row.account_name_len = copyText(&ni_row.account_name, acct_stmt.columnText(1));
                const ni_type = "equity";
                ni_row.account_type_len = @min(ni_type.len, ni_row.account_type.len);
                @memcpy(ni_row.account_type[0..ni_row.account_type_len], ni_type[0..ni_row.account_type_len]);

                if (net_income > 0) {
                    ni_row.credit_balance = net_income;
                } else {
                    const abs_ni = std.math.negate(net_income) catch return error.AmountOverflow;
                    ni_row.debit_balance = abs_ni;
                }

                const allocator = result.arena.allocator();
                const old_len = result.rows.len;
                result.rows = try allocator.realloc(result.rows, old_len + 1);
                result.rows[old_len] = ni_row;
            }
        }
    }

    // Inject net income into totals (equity side)
    if (net_income > 0) {
        result.total_credits = std.math.add(i64, result.total_credits, net_income) catch return error.AmountOverflow;
    } else {
        const abs_ni = std.math.negate(net_income) catch return error.AmountOverflow;
        result.total_debits = std.math.add(i64, result.total_debits, abs_ni) catch return error.AmountOverflow;
    }

    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}
