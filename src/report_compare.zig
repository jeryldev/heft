const std = @import("std");
const db = @import("db.zig");
const money = @import("money.zig");
const common = @import("report_common.zig");
const statements = @import("report_statements.zig");

pub const ReportRow = common.ReportRow;
pub const ReportResult = common.ReportResult;
pub const ComparativeReportRow = common.ComparativeReportRow;
pub const ComparativeReportResult = common.ComparativeReportResult;
pub const EquityRow = common.EquityRow;
pub const EquityResult = common.EquityResult;
pub const TranslationRates = struct {
    closing_rate: i64,
    average_rate: i64,
};
const ensureFreshCache = common.ensureFreshCache;
const verifyBookExists = common.verifyBookExists;
const copyText = common.copyText;
const MAX_REPORT_ROWS = common.MAX_REPORT_ROWS;
const trialBalance = statements.trialBalance;
const incomeStatement = statements.incomeStatement;
const balanceSheetWithProjectedRE = statements.balanceSheetWithProjectedRE;
const trialBalanceMovement = statements.trialBalanceMovement;

fn mergeComparative(current: *ReportResult, prior: *ReportResult) !*ComparativeReportResult {
    const result = try std.heap.c_allocator.create(ComparativeReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(ComparativeReportRow){};

    // Bounded transitively by MAX_REPORT_ROWS because both inputs are report outputs.
    var prior_map = std.AutoHashMapUnmanaged(i64, usize){};
    for (prior.rows, 0..) |_, idx| {
        try prior_map.put(allocator, prior.rows[idx].account_id, idx);
    }

    // Bounded transitively by MAX_REPORT_ROWS because both inputs are report outputs.
    var used_prior = std.AutoHashMapUnmanaged(i64, void){};
    for (current.rows) |crow| {
        var comp_row = std.mem.zeroes(ComparativeReportRow);
        comp_row.account_id = crow.account_id;
        comp_row.account_number_len = crow.account_number_len;
        @memcpy(comp_row.account_number[0..crow.account_number_len], crow.account_number[0..crow.account_number_len]);
        comp_row.account_name_len = crow.account_name_len;
        @memcpy(comp_row.account_name[0..crow.account_name_len], crow.account_name[0..crow.account_name_len]);
        comp_row.account_type_len = crow.account_type_len;
        @memcpy(comp_row.account_type[0..crow.account_type_len], crow.account_type[0..crow.account_type_len]);
        comp_row.current_debit = crow.debit_balance;
        comp_row.current_credit = crow.credit_balance;

        if (prior_map.get(crow.account_id)) |pidx| {
            const prow = prior.rows[pidx];
            comp_row.prior_debit = prow.debit_balance;
            comp_row.prior_credit = prow.credit_balance;
            try used_prior.put(allocator, crow.account_id, {});
        }

        comp_row.variance_debit = std.math.sub(i64, comp_row.current_debit, comp_row.prior_debit) catch return error.AmountOverflow;
        comp_row.variance_credit = std.math.sub(i64, comp_row.current_credit, comp_row.prior_credit) catch return error.AmountOverflow;

        try rows.append(allocator, comp_row);
        if (rows.items.len >= MAX_REPORT_ROWS) break;
    }

    for (prior.rows) |prow| {
        if (rows.items.len >= MAX_REPORT_ROWS) break;
        if (used_prior.get(prow.account_id) == null) {
            var comp_row = std.mem.zeroes(ComparativeReportRow);
            comp_row.account_id = prow.account_id;
            comp_row.account_number_len = prow.account_number_len;
            @memcpy(comp_row.account_number[0..prow.account_number_len], prow.account_number[0..prow.account_number_len]);
            comp_row.account_name_len = prow.account_name_len;
            @memcpy(comp_row.account_name[0..prow.account_name_len], prow.account_name[0..prow.account_name_len]);
            comp_row.account_type_len = prow.account_type_len;
            @memcpy(comp_row.account_type[0..prow.account_type_len], prow.account_type[0..prow.account_type_len]);
            comp_row.prior_debit = prow.debit_balance;
            comp_row.prior_credit = prow.credit_balance;
            comp_row.variance_debit = std.math.negate(prow.debit_balance) catch return error.AmountOverflow;
            comp_row.variance_credit = std.math.negate(prow.credit_balance) catch return error.AmountOverflow;
            try rows.append(allocator, comp_row);
        }
    }

    result.truncated = rows.items.len >= MAX_REPORT_ROWS;
    result.rows = try rows.toOwnedSlice(allocator);
    result.current_total_debits = current.total_debits;
    result.current_total_credits = current.total_credits;
    result.prior_total_debits = prior.total_debits;
    result.prior_total_credits = prior.total_credits;
    return result;
}

pub fn trialBalanceComparative(database: db.Database, book_id: i64, current_date: []const u8, prior_date: []const u8) !*ComparativeReportResult {
    const current = try trialBalance(database, book_id, current_date);
    defer current.deinit();
    const prior = try trialBalance(database, book_id, prior_date);
    defer prior.deinit();
    return mergeComparative(current, prior);
}

pub fn incomeStatementComparative(database: db.Database, book_id: i64, cur_start: []const u8, cur_end: []const u8, prior_start: []const u8, prior_end: []const u8) !*ComparativeReportResult {
    const current = try incomeStatement(database, book_id, cur_start, cur_end);
    defer current.deinit();
    const prior = try incomeStatement(database, book_id, prior_start, prior_end);
    defer prior.deinit();
    return mergeComparative(current, prior);
}

pub fn balanceSheetComparative(database: db.Database, book_id: i64, current_date: []const u8, prior_date: []const u8, fy_start: []const u8) !*ComparativeReportResult {
    const current = try balanceSheetWithProjectedRE(database, book_id, current_date, fy_start);
    defer current.deinit();
    const prior = try balanceSheetWithProjectedRE(database, book_id, prior_date, fy_start);
    defer prior.deinit();
    return mergeComparative(current, prior);
}

pub fn trialBalanceMovementComparative(database: db.Database, book_id: i64, cur_start: []const u8, cur_end: []const u8, prior_start: []const u8, prior_end: []const u8) !*ComparativeReportResult {
    const current = try trialBalanceMovement(database, book_id, cur_start, cur_end);
    defer current.deinit();
    const prior = try trialBalanceMovement(database, book_id, prior_start, prior_end);
    defer prior.deinit();
    return mergeComparative(current, prior);
}

pub fn equityChanges(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8, fy_start_date: []const u8) !*EquityResult {
    try verifyBookExists(database, book_id);

    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, end_date });

    const result = try std.heap.c_allocator.create(EquityResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var rows = std.ArrayListUnmanaged(EquityRow){};

    var stmt = try database.prepare(
        \\SELECT a.id, a.number, a.name,
        \\  COALESCE(opening.open_debit, 0), COALESCE(opening.open_credit, 0),
        \\  COALESCE(activity.act_debit, 0), COALESCE(activity.act_credit, 0),
        \\  a.is_contra
        \\FROM ledger_accounts a
        \\LEFT JOIN (
        \\  SELECT ab.account_id,
        \\    SUM(ab.debit_sum) as open_debit, SUM(ab.credit_sum) as open_credit
        \\  FROM ledger_account_balances ab
        \\  JOIN ledger_periods p ON p.id = ab.period_id
        \\  WHERE ab.book_id = ? AND p.end_date < ?
        \\  GROUP BY ab.account_id
        \\) opening ON opening.account_id = a.id
        \\LEFT JOIN (
        \\  SELECT ab.account_id,
        \\    SUM(ab.debit_sum) as act_debit, SUM(ab.credit_sum) as act_credit
        \\  FROM ledger_account_balances ab
        \\  JOIN ledger_periods p ON p.id = ab.period_id
        \\  WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
        \\  GROUP BY ab.account_id
        \\) activity ON activity.account_id = a.id
        \\WHERE a.book_id = ? AND a.account_type = 'equity'
        \\  AND (opening.open_debit IS NOT NULL OR activity.act_debit IS NOT NULL)
        \\ORDER BY a.number ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindInt(3, book_id);
    try stmt.bindText(4, start_date);
    try stmt.bindText(5, end_date);
    try stmt.bindInt(6, book_id);

    var total_opening: i64 = 0;
    var total_closing: i64 = 0;

    while (try stmt.step()) {
        var row = std.mem.zeroes(EquityRow);
        row.account_id = stmt.columnInt64(0);
        row.account_number_len = copyText(&row.account_number, stmt.columnText(1));
        row.account_name_len = copyText(&row.account_name, stmt.columnText(2));

        const open_debit = stmt.columnInt64(3);
        const open_credit = stmt.columnInt64(4);
        const act_debit = stmt.columnInt64(5);
        const act_credit = stmt.columnInt64(6);
        const is_contra = stmt.columnInt(7);

        if (is_contra == 1) {
            row.opening_balance = std.math.sub(i64, open_debit, open_credit) catch return error.AmountOverflow;
            row.period_activity = std.math.sub(i64, act_debit, act_credit) catch return error.AmountOverflow;
        } else {
            row.opening_balance = std.math.sub(i64, open_credit, open_debit) catch return error.AmountOverflow;
            row.period_activity = std.math.sub(i64, act_credit, act_debit) catch return error.AmountOverflow;
        }
        row.closing_balance = std.math.add(i64, row.opening_balance, row.period_activity) catch return error.AmountOverflow;

        total_opening = std.math.add(i64, total_opening, row.opening_balance) catch return error.AmountOverflow;
        total_closing = std.math.add(i64, total_closing, row.closing_balance) catch return error.AmountOverflow;

        try rows.append(allocator, row);
        if (rows.items.len >= MAX_REPORT_ROWS) break;
    }

    var net_income: i64 = 0;
    {
        var ni_stmt = try database.prepare(
            \\SELECT a.account_type,
            \\  COALESCE(SUM(ab.credit_sum), 0), COALESCE(SUM(ab.debit_sum), 0)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
            \\  AND a.account_type IN ('revenue', 'expense')
            \\GROUP BY a.account_type;
        );
        defer ni_stmt.finalize();
        try ni_stmt.bindInt(1, book_id);
        try ni_stmt.bindText(2, fy_start_date);
        try ni_stmt.bindText(3, end_date);

        while (try ni_stmt.step()) {
            const acct_type = ni_stmt.columnText(0).?;
            const credits = ni_stmt.columnInt64(1);
            const debits = ni_stmt.columnInt64(2);
            if (std.mem.eql(u8, acct_type, "revenue")) {
                const delta = std.math.sub(i64, credits, debits) catch return error.AmountOverflow;
                net_income = std.math.add(i64, net_income, delta) catch return error.AmountOverflow;
            } else {
                const delta = std.math.sub(i64, debits, credits) catch return error.AmountOverflow;
                net_income = std.math.sub(i64, net_income, delta) catch return error.AmountOverflow;
            }
        }
    }

    result.truncated = rows.items.len >= MAX_REPORT_ROWS;
    result.rows = try rows.toOwnedSlice(allocator);
    result.net_income = net_income;
    result.total_opening = total_opening;
    result.total_closing = total_closing;
    return result;
}

pub fn translateReportResult(source: *ReportResult, rates: TranslationRates) !*ReportResult {
    const result = try std.heap.c_allocator.create(ReportResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    const rows = try allocator.alloc(ReportRow, source.rows.len);
    var total_debits: i64 = 0;
    var total_credits: i64 = 0;

    for (source.rows, 0..) |row, i| {
        rows[i] = row;
        const acct_type = row.account_type[0..row.account_type_len];
        const is_bs = std.mem.eql(u8, acct_type, "asset") or
            std.mem.eql(u8, acct_type, "liability") or
            std.mem.eql(u8, acct_type, "equity");
        const rate = if (is_bs) rates.closing_rate else rates.average_rate;

        if (row.debit_balance != 0) {
            rows[i].debit_balance = try money.computeBaseAmount(row.debit_balance, rate);
        }
        if (row.credit_balance != 0) {
            rows[i].credit_balance = try money.computeBaseAmount(row.credit_balance, rate);
        }
        total_debits = std.math.add(i64, total_debits, rows[i].debit_balance) catch return error.AmountOverflow;
        total_credits = std.math.add(i64, total_credits, rows[i].credit_balance) catch return error.AmountOverflow;
    }

    result.rows = rows;
    result.total_debits = total_debits;
    result.total_credits = total_credits;
    result.decimal_places = source.decimal_places;
    result.truncated = source.truncated;
    return result;
}
