const std = @import("std");
const db = @import("db.zig");
const common = @import("report_common.zig");

pub const LedgerResult = common.LedgerResult;
const RunningMode = common.RunningMode;
const buildLedgerResult = common.buildLedgerResult;
const ensureFreshCache = common.ensureFreshCache;
const verifyBookExists = common.verifyBookExists;
const getDecimalPlaces = common.getDecimalPlaces;

const gl_sql: [*:0]const u8 =
    \\SELECT e.posting_date, e.document_number,
    \\  e.description, l.account_id, a.number,
    \\  a.name, l.base_debit_amount, l.base_credit_amount,
    \\  l.transaction_currency, l.debit_amount, l.credit_amount, l.fx_rate
    \\FROM ledger_entries e
    \\JOIN ledger_entry_lines l ON l.entry_id = e.id
    \\JOIN ledger_accounts a ON a.id = l.account_id
    \\WHERE e.book_id = ? AND e.status = 'posted'
    \\  AND e.posting_date >= ? AND e.posting_date <= ?
    \\ORDER BY e.posting_date, e.id, l.line_number;
;

const al_sql: [*:0]const u8 =
    \\SELECT e.posting_date, e.document_number,
    \\  e.description, l.account_id, a.number,
    \\  a.name, l.base_debit_amount, l.base_credit_amount,
    \\  l.transaction_currency, l.debit_amount, l.credit_amount, l.fx_rate
    \\FROM ledger_entries e
    \\JOIN ledger_entry_lines l ON l.entry_id = e.id
    \\JOIN ledger_accounts a ON a.id = l.account_id
    \\WHERE e.book_id = ? AND l.account_id = ?
    \\  AND e.status = 'posted'
    \\  AND e.posting_date >= ? AND e.posting_date <= ?
    \\ORDER BY e.posting_date, e.id, l.line_number;
;

pub fn generalLedger(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8) !*LedgerResult {
    try verifyBookExists(database, book_id);
    const result = try buildLedgerResult(database, gl_sql, .{ book_id, start_date, end_date }, .none);
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}

pub fn accountLedger(database: db.Database, book_id: i64, account_id: i64, start_date: []const u8, end_date: []const u8) !*LedgerResult {
    try verifyBookExists(database, book_id);

    // Recalculate stale cache for opening balance periods and detail periods
    try ensureFreshCache(database, book_id,
        \\SELECT DISTINCT ab.period_id FROM ledger_account_balances ab
        \\JOIN ledger_periods p ON p.id = ab.period_id
        \\WHERE ab.book_id = ? AND p.end_date <= ? AND ab.is_stale = 1;
    , .{ book_id, end_date });

    // Determine running balance direction from account's normal_balance
    var mode: RunningMode = .debit_normal;
    {
        var stmt = try database.prepare("SELECT normal_balance FROM ledger_accounts WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, account_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        if (std.mem.eql(u8, stmt.columnText(0).?, "credit")) mode = .credit_normal;
    }

    // Compute opening balance from prior periods
    var opening: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(ab.debit_sum), 0), COALESCE(SUM(ab.credit_sum), 0)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND ab.account_id = ? AND p.end_date < ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, account_id);
        try stmt.bindText(3, start_date);
        _ = try stmt.step();
        const prior_debits = stmt.columnInt64(0);
        const prior_credits = stmt.columnInt64(1);
        opening = switch (mode) {
            .debit_normal => std.math.sub(i64, prior_debits, prior_credits) catch return error.AmountOverflow,
            .credit_normal => std.math.sub(i64, prior_credits, prior_debits) catch return error.AmountOverflow,
            .none => 0,
        };
    }

    const result = try buildLedgerResult(database, al_sql, .{ book_id, account_id, start_date, end_date }, mode);

    result.opening_balance = opening;
    if (opening != 0) {
        for (result.rows) |*row| {
            row.running_balance = std.math.add(i64, row.running_balance, opening) catch return error.AmountOverflow;
        }
        result.closing_balance = std.math.add(i64, result.closing_balance, opening) catch return error.AmountOverflow;
    }
    result.decimal_places = try getDecimalPlaces(database, book_id);

    return result;
}

const jr_sql: [*:0]const u8 =
    \\SELECT e.posting_date, e.document_number,
    \\  e.description, l.account_id, a.number,
    \\  a.name, l.base_debit_amount, l.base_credit_amount,
    \\  l.transaction_currency, l.debit_amount, l.credit_amount, l.fx_rate
    \\FROM ledger_entries e
    \\JOIN ledger_entry_lines l ON l.entry_id = e.id
    \\JOIN ledger_accounts a ON a.id = l.account_id
    \\WHERE e.book_id = ? AND e.status = 'posted'
    \\  AND e.posting_date >= ? AND e.posting_date <= ?
    \\ORDER BY e.document_number, e.id, l.line_number;
;

pub fn journalRegister(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8) !*LedgerResult {
    try verifyBookExists(database, book_id);
    const result = try buildLedgerResult(database, jr_sql, .{ book_id, start_date, end_date }, .none);
    result.decimal_places = try getDecimalPlaces(database, book_id);
    return result;
}
