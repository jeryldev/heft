// Heft — The embedded accounting engine
// Copyright (C) 2026 Jeryl Donato Estopace
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// main.zig: C ABI surface. Every public function lives here.
// Internal logic lives in db.zig, schema.zig, and future entity modules.
// This file is thin — convert between C types and Zig types, nothing more.

const std = @import("std");
const heft = @import("heft");

// ── LedgerDB ────────────────────────────────────────────────────
// The opaque handle returned to C callers. Heap-allocated because
// it crosses the C ABI as a pointer.

pub const LedgerDB = struct {
    sqlite: heft.db.Database,
};

// ── Internal (Zig idioms) ───────────────────────────────────────

const SCHEMA_VERSION = heft.schema.SCHEMA_VERSION;

fn internal_open(path: [*:0]const u8) !*LedgerDB {
    const db = try heft.db.Database.open(path);
    errdefer db.close();

    const version = blk: {
        var stmt = try db.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        break :blk stmt.columnInt(0);
    };

    if (version == 0) {
        try heft.schema.createAll(db);
    } else if (version < SCHEMA_VERSION) {
        try heft.schema.migrate(db, version);
    } else if (version > SCHEMA_VERSION) {
        return error.SchemaVersionMismatch;
    }

    const handle = std.heap.c_allocator.create(LedgerDB) catch return error.OutOfMemory;
    handle.* = .{ .sqlite = db };
    return handle;
}

fn internal_close(handle: *LedgerDB) void {
    handle.sqlite.drainLeakedStatements();
    handle.sqlite.close();
    std.heap.c_allocator.destroy(handle);
}

// ── C ABI Exports ───────────────────────────────────────────────

pub export fn ledger_open(path: ?[*:0]const u8) ?*LedgerDB {
    const p = path orelse {
        setError(mapError(error.InvalidInput));
        return null;
    };
    return internal_open(p) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_close(handle: ?*LedgerDB) void {
    const h = handle orelse return;
    internal_close(h);
}

pub export fn ledger_version() [*:0]const u8 {
    return "0.0.1";
}

// ── Sprint 2: Entity C ABI Exports ─────────────────────────────

pub export fn ledger_create_book(handle: ?*LedgerDB, name: [*:0]const u8, base_currency: [*:0]const u8, decimal_places: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.book.Book.create(h.sqlite, std.mem.span(name), std.mem.span(base_currency), decimal_places, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_create_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, is_contra: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const at = heft.account.AccountType.fromString(std.mem.span(account_type)) orelse {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    return heft.account.Account.create(h.sqlite, book_id, std.mem.span(number), std.mem.span(name), at, is_contra != 0, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_create_period(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, period_number: i32, year: i32, start_date: [*:0]const u8, end_date: [*:0]const u8, period_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.period.Period.create(h.sqlite, book_id, std.mem.span(name), period_number, year, std.mem.span(start_date), std.mem.span(end_date), std.mem.span(period_type), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_update_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const status = heft.account.AccountStatus.fromString(std.mem.span(new_status)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.account.Account.updateStatus(h.sqlite, account_id, status, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_transition_period(handle: ?*LedgerDB, period_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const ts = heft.period.PeriodStatus.fromString(std.mem.span(target_status)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.period.Period.transition(h.sqlite, period_id, ts, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_rounding_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setRoundingAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_fx_gain_loss_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setFxGainLossAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_retained_earnings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setRetainedEarningsAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_income_summary_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setIncomeSummaryAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_opening_balance_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setOpeningBalanceAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_suspense_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setSuspenseAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_validate_opening_balance(handle: ?*LedgerDB, book_id: i64) bool {
    const h = handle orelse return false;
    heft.book.Book.validateOpeningBalanceMigration(h.sqlite, book_id) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_bulk_create_periods(handle: ?*LedgerDB, book_id: i64, fiscal_year: i32, start_month: i32, granularity: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const gran = heft.period.PeriodGranularity.fromString(std.mem.span(granularity)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.period.Period.bulkCreate(h.sqlite, book_id, fiscal_year, start_month, gran, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_create_draft(handle: ?*LedgerDB, book_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, period_id: i64, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    return heft.entry.Entry.createDraft(h.sqlite, book_id, std.mem.span(document_number), std.mem.span(transaction_date), std.mem.span(posting_date), desc, period_id, meta, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_add_line(handle: ?*LedgerDB, entry_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    if (counterparty_id < 0) {
        setError(mapError(error.InvalidInput));
        return -1;
    }
    const cp: ?i64 = if (counterparty_id > 0) counterparty_id else null;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    return heft.entry.Entry.addLine(h.sqlite, entry_id, line_number, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, cp, desc, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_edit_draft(handle: ?*LedgerDB, entry_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, metadata: ?[*:0]const u8, period_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    heft.entry.Entry.editDraft(h.sqlite, entry_id, std.mem.span(document_number), std.mem.span(transaction_date), std.mem.span(posting_date), desc, meta, period_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_edit_posted(handle: ?*LedgerDB, entry_id: i64, description: ?[*:0]const u8, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    heft.entry.Entry.editPosted(h.sqlite, entry_id, desc, meta, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_post_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.post(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_void_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.voidEntry(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_reverse_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, reversal_date: [*:0]const u8, target_period_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const tp: ?i64 = if (target_period_id > 0) target_period_id else null;
    return heft.entry.Entry.reverse(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(reversal_date), tp, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_remove_line(handle: ?*LedgerDB, line_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.removeLine(h.sqlite, line_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_delete_draft(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.deleteDraft(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_edit_line(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.editLine(h.sqlite, line_id, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, null, null, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_trial_balance(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse return null;
    return heft.report.trialBalance(h.sqlite, book_id, std.mem.span(as_of_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_income_statement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse return null;
    return heft.report.incomeStatement(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_trial_balance_movement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse return null;
    return heft.report.trialBalanceMovement(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_balance_sheet(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse return null;
    return heft.report.balanceSheet(h.sqlite, book_id, std.mem.span(as_of_date), std.mem.span(fy_start_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_balance_sheet_auto(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse return null;
    return heft.report.balanceSheetAuto(h.sqlite, book_id, std.mem.span(as_of_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_set_fy_start_month(handle: ?*LedgerDB, book_id: i64, month: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setFyStartMonth(h.sqlite, book_id, month, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_entity_type(handle: ?*LedgerDB, book_id: i64, entity_type: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const et = heft.book.EntityType.fromString(std.mem.span(entity_type)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.book.Book.setEntityType(h.sqlite, book_id, et, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_translate_report(source: ?*heft.report.ReportResult, closing_rate: i64, average_rate: i64) ?*heft.report.ReportResult {
    const src = source orelse return null;
    const rates = heft.report.TranslationRates{
        .closing_rate = closing_rate,
        .average_rate = average_rate,
    };
    return heft.report.translateReportResult(src, rates) catch return null;
}

pub export fn ledger_general_ledger(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse return null;
    return heft.report.generalLedger(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_account_ledger(handle: ?*LedgerDB, book_id: i64, account_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse return null;
    return heft.report.accountLedger(h.sqlite, book_id, account_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_journal_register(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse return null;
    return heft.report.journalRegister(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_free_ledger_result(result: ?*heft.report.LedgerResult) void {
    const r = result orelse return;
    r.deinit();
}

pub export fn ledger_free_result(result: ?*heft.report.ReportResult) void {
    const r = result orelse return;
    r.deinit();
}

pub export fn ledger_trial_balance_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse return null;
    return heft.report.trialBalanceComparative(h.sqlite, book_id, std.mem.span(current_date), std.mem.span(prior_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_income_statement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse return null;
    return heft.report.incomeStatementComparative(h.sqlite, book_id, std.mem.span(cur_start), std.mem.span(cur_end), std.mem.span(prior_start), std.mem.span(prior_end)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_balance_sheet_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8, fy_start: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse return null;
    return heft.report.balanceSheetComparative(h.sqlite, book_id, std.mem.span(current_date), std.mem.span(prior_date), std.mem.span(fy_start)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_trial_balance_movement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse return null;
    return heft.report.trialBalanceMovementComparative(h.sqlite, book_id, std.mem.span(cur_start), std.mem.span(cur_end), std.mem.span(prior_start), std.mem.span(prior_end)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_free_comparative_result(result: ?*heft.report.ComparativeReportResult) void {
    const r = result orelse return;
    r.deinit();
}

pub export fn ledger_equity_changes(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.EquityResult {
    const h = handle orelse return null;
    return heft.report.equityChanges(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), std.mem.span(fy_start_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_free_equity_result(result: ?*heft.report.EquityResult) void {
    const r = result orelse return;
    r.deinit();
}

pub export fn ledger_result_row_count(result: ?*heft.report.ReportResult) i32 {
    const r = result orelse return 0;
    return safeIntCast(r.rows.len);
}

pub export fn ledger_result_total_debits(result: ?*heft.report.ReportResult) i64 {
    const r = result orelse return 0;
    return r.total_debits;
}

pub export fn ledger_result_total_credits(result: ?*heft.report.ReportResult) i64 {
    const r = result orelse return 0;
    return r.total_credits;
}

pub export fn ledger_create_subledger_group(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, group_type: [*:0]const u8, group_number: i32, gl_account_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.subledger.SubledgerGroup.create(h.sqlite, book_id, std.mem.span(name), std.mem.span(group_type), group_number, gl_account_id, null, null, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_create_subledger_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, group_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.subledger.SubledgerAccount.create(h.sqlite, book_id, std.mem.span(number), std.mem.span(name), std.mem.span(account_type), group_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_create_classification(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, report_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.classification.Classification.create(h.sqlite, book_id, std.mem.span(name), std.mem.span(report_type), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_add_group_node(handle: ?*LedgerDB, classification_id: i64, label: [*:0]const u8, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const pid: ?i64 = if (parent_id == 0) null else parent_id;
    return heft.classification.ClassificationNode.addGroup(h.sqlite, classification_id, std.mem.span(label), pid, position, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_add_account_node(handle: ?*LedgerDB, classification_id: i64, account_id: i64, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const pid: ?i64 = if (parent_id == 0) null else parent_id;
    return heft.classification.ClassificationNode.addAccount(h.sqlite, classification_id, account_id, pid, position, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_move_node(handle: ?*LedgerDB, node_id: i64, new_parent_id: i64, new_position: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const pid: ?i64 = if (new_parent_id == 0) null else new_parent_id;
    heft.classification.ClassificationNode.move(h.sqlite, node_id, pid, new_position, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_classified_report(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse return null;
    return heft.classification.classifiedReport(h.sqlite, classification_id, std.mem.span(as_of_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_cash_flow_statement(handle: ?*LedgerDB, classification_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse return null;
    return heft.classification.cashFlowStatement(h.sqlite, classification_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        setError(mapError(err));
        return null;
    };
}

pub export fn ledger_free_classified_result(result: ?*heft.classification.ClassifiedResult) void {
    const r = result orelse return;
    r.deinit();
}

pub export fn ledger_delete_classification(handle: ?*LedgerDB, classification_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.classification.Classification.delete(h.sqlite, classification_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_verify(handle: ?*LedgerDB, book_id: i64, out_errors: ?*u32, out_warnings: ?*u32) bool {
    const h = handle orelse return false;
    const err_ptr = out_errors orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    const warn_ptr = out_warnings orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    const result = heft.verify_mod.verify(h.sqlite, book_id) catch |err| {
        setError(mapError(err));
        return false;
    };
    err_ptr.* = result.errors;
    warn_ptr.* = result.warnings;
    return result.passed();
}

pub export fn ledger_archive_book(handle: ?*LedgerDB, book_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.archive(h.sqlite, book_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_close_period(handle: ?*LedgerDB, book_id: i64, period_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.close.closePeriod(h.sqlite, book_id, period_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_revalue_forex_balances(handle: ?*LedgerDB, book_id: i64, period_id: i64, rates_json: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const json = std.mem.span(rates_json);

    var rates_buf: [50]heft.revaluation.CurrencyRate = undefined;
    const rate_count = heft.revaluation.parseRatesJson(json, &rates_buf) catch {
        setError(mapError(error.InvalidInput));
        return -1;
    };

    if (rate_count == 0) {
        setError(mapError(error.InvalidInput));
        return -1;
    }

    const result = heft.revaluation.revalueForexBalances(h.sqlite, book_id, period_id, rates_buf[0..rate_count], std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return result.entry_id;
}

// ── Error Reporting ────────────────────────────────────────────

threadlocal var last_error_code: i32 = 0;

/// Returns the error code from the most recent failed operation on this thread.
/// Only meaningful after a function returns -1, false, or null.
/// The error code is NOT cleared on success — check the return value first.
pub export fn ledger_last_error() i32 {
    return last_error_code;
}

fn setError(code: i32) void {
    last_error_code = code;
}

// Error codes for C consumers
// 0 = no error
// 1 = not found, 2 = invalid input, 3 = period closed, 4 = period locked,
// 5 = already posted, 6 = unbalanced entry, 7 = duplicate number,
// 8 = invalid transition, 9 = account inactive, 10 = missing counterparty,
// 11 = invalid counterparty, 12 = amount overflow, 13 = void reason required,
// 14 = reverse reason required, 15 = circular reference, 16 = too few lines,
// 17 = schema version mismatch, 18 = out of memory,
// 20 = invalid amount, 21 = book archived, 22 = cross-book violation,
// 23 = invalid fx rate, 24 = invalid decimal places, 25 = buffer too small,
// 26 = retained earnings account required, 27 = fx gain/loss account required,
// 28 = opening balance account required, 29 = income summary account required,
// 30 = approval required, 31 = too many accounts,
// 90-94 = sqlite errors (open, exec, prepare, step, bind),
// 99 = unknown

fn mapError(err: anyerror) i32 {
    return switch (err) {
        error.NotFound => 1,
        error.InvalidInput => 2,
        error.PeriodClosed => 3,
        error.PeriodLocked => 4,
        error.AlreadyPosted => 5,
        error.UnbalancedEntry => 6,
        error.DuplicateNumber => 7,
        error.InvalidTransition => 8,
        error.AccountInactive => 9,
        error.MissingCounterparty => 10,
        error.InvalidCounterparty => 11,
        error.AmountOverflow => 12,
        error.VoidReasonRequired => 13,
        error.ReverseReasonRequired => 14,
        error.CircularReference => 15,
        error.TooFewLines => 16,
        error.SchemaVersionMismatch => 17,
        error.OutOfMemory => 18,
        error.InvalidAmount => 20,
        error.BookArchived => 21,
        error.CrossBookViolation => 22,
        error.InvalidFxRate => 23,
        error.InvalidDecimalPlaces => 24,
        error.BufferTooSmall => 25,
        error.RetainedEarningsAccountRequired => 26,
        error.FxGainLossAccountRequired => 27,
        error.OpeningBalanceAccountRequired => 28,
        error.IncomeSummaryAccountRequired => 29,
        error.ApprovalRequired => 30,
        error.TooManyAccounts => 31,
        error.SqliteOpenFailed => 90,
        error.SqliteExecFailed => 91,
        error.SqlitePrepareFailed => 92,
        error.SqliteStepFailed => 93,
        error.SqliteBindFailed => 94,
        else => 99,
    };
}

// ── Helpers ────────────────────────────────────────────────────

fn safeBuf(buf: ?[*]u8, buf_len: i32) ?[]u8 {
    const b = buf orelse return null;
    if (buf_len <= 0) return null;
    return b[0..@intCast(buf_len)];
}

fn safeIntCast(val: usize) i32 {
    if (val > @as(usize, @intCast(std.math.maxInt(i32)))) return std.math.maxInt(i32);
    return @intCast(val);
}

// ── CRUD Exports (Sprint 10) ──────────────────────────────────

pub export fn ledger_update_book_name(handle: ?*LedgerDB, book_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.updateName(h.sqlite, book_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.account.Account.updateName(h.sqlite, account_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_account_parent(handle: ?*LedgerDB, account_id: i64, parent_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const pid: ?i64 = if (parent_id > 0) parent_id else null;
    heft.account.Account.setParent(h.sqlite, account_id, pid, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_account_monetary(handle: ?*LedgerDB, account_id: i64, is_monetary: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.account.Account.setMonetary(h.sqlite, account_id, is_monetary != 0, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_classification_name(handle: ?*LedgerDB, classification_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.classification.Classification.updateName(h.sqlite, classification_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_node_label(handle: ?*LedgerDB, node_id: i64, new_label: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.classification.ClassificationNode.updateLabel(h.sqlite, node_id, std.mem.span(new_label), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_delete_node(handle: ?*LedgerDB, node_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.classification.ClassificationNode.delete(h.sqlite, node_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_subledger_group_name(handle: ?*LedgerDB, group_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.subledger.SubledgerGroup.updateName(h.sqlite, group_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_delete_subledger_group(handle: ?*LedgerDB, group_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.subledger.SubledgerGroup.delete(h.sqlite, group_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_subledger_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.subledger.SubledgerAccount.updateName(h.sqlite, account_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_delete_subledger_account(handle: ?*LedgerDB, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.subledger.SubledgerAccount.delete(h.sqlite, account_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_update_subledger_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const status = heft.subledger.SubledgerAccountStatus.fromString(std.mem.span(new_status)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.subledger.SubledgerAccount.updateStatus(h.sqlite, account_id, status, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

// ── Query/List Exports (buffer-based) ─────────────────────────
// Pattern: write CSV/JSON into caller buffer, return bytes written or -1

pub export fn ledger_get_book(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getBook(h.sqlite, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_books(handle: ?*LedgerDB, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listBooks(h.sqlite, sf, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_get_account(handle: ?*LedgerDB, account_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getAccount(h.sqlite, account_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_accounts(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, status_filter: ?[*:0]const u8, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listAccounts(h.sqlite, book_id, tf, sf, ns, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_get_period(handle: ?*LedgerDB, period_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getPeriod(h.sqlite, period_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_periods(handle: ?*LedgerDB, book_id: i64, year_filter: i32, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const yf: ?i32 = if (year_filter > 0) year_filter else null;
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listPeriods(h.sqlite, book_id, yf, sf, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_get_entry(handle: ?*LedgerDB, entry_id: i64, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getEntry(h.sqlite, entry_id, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_entries(handle: ?*LedgerDB, book_id: i64, status_filter: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, doc_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const sd: ?[]const u8 = if (start_date) |s| std.mem.span(s) else null;
    const ed: ?[]const u8 = if (end_date) |s| std.mem.span(s) else null;
    const ds: ?[]const u8 = if (doc_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listEntries(h.sqlite, book_id, sf, sd, ed, ds, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_entry_lines(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.listEntryLines(h.sqlite, entry_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_classifications(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listClassifications(h.sqlite, book_id, tf, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_subledger_groups(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listSubledgerGroups(h.sqlite, book_id, tf, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_subledger_accounts(handle: ?*LedgerDB, book_id: i64, group_filter: i64, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_filter > 0) group_filter else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listSubledgerAccounts(h.sqlite, book_id, gf, ns, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_audit_log(handle: ?*LedgerDB, book_id: i64, entity_type: ?[*:0]const u8, action: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const et: ?[]const u8 = if (entity_type) |s| std.mem.span(s) else null;
    const af: ?[]const u8 = if (action) |s| std.mem.span(s) else null;
    const sd: ?[]const u8 = if (start_date) |s| std.mem.span(s) else null;
    const ed: ?[]const u8 = if (end_date) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listAuditLog(h.sqlite, book_id, et, af, sd, ed, order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Subledger Report Exports ──────────────────────────────────

pub export fn ledger_subledger_report(handle: ?*LedgerDB, book_id: i64, group_id: i64, name_search: ?[*:0]const u8, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_id > 0) group_id else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.subledgerReport(h.sqlite, book_id, gf, ns, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_counterparty_ledger(handle: ?*LedgerDB, book_id: i64, counterparty_id: i64, account_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const af: ?i64 = if (account_filter > 0) account_filter else null;
    const result = heft.query_mod.counterpartyLedger(h.sqlite, book_id, counterparty_id, af, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_transactions(handle: ?*LedgerDB, book_id: i64, account_filter: i64, counterparty_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const af: ?i64 = if (account_filter > 0) account_filter else null;
    const cf: ?i64 = if (counterparty_filter > 0) counterparty_filter else null;
    const result = heft.query_mod.listTransactions(h.sqlite, book_id, af, cf, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_subledger_reconciliation(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.subledgerReconciliation(h.sqlite, book_id, group_id, std.mem.span(as_of_date), safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_aged_subledger(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const order: heft.query_mod.SortOrder = if (sort_order == 0) .asc else if (sort_order == 1) .desc else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_id > 0) group_id else null;
    const result = heft.query_mod.agedSubledger(h.sqlite, book_id, gf, std.mem.span(as_of_date), order, limit, offset, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Fix: ledger_edit_line with description + counterparty ─────

pub export fn ledger_edit_line_full(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    if (counterparty_id < 0) {
        setError(mapError(error.InvalidInput));
        return false;
    }
    const cp: ?i64 = if (counterparty_id > 0) counterparty_id else null;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    heft.entry.Entry.editLine(h.sqlite, line_id, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, cp, desc, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

// ── Sprint 17: Dimension Exports ──────────────────────────────

pub export fn ledger_create_dimension(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, dimension_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const dt = heft.dimension.DimensionType.fromString(std.mem.span(dimension_type)) orelse {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    return heft.dimension.Dimension.create(h.sqlite, book_id, std.mem.span(name), dt, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_delete_dimension(handle: ?*LedgerDB, dimension_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.dimension.Dimension.delete(h.sqlite, dimension_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_create_dimension_value(handle: ?*LedgerDB, dimension_id: i64, code: [*:0]const u8, label: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.dimension.DimensionValue.create(h.sqlite, dimension_id, std.mem.span(code), std.mem.span(label), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_delete_dimension_value(handle: ?*LedgerDB, value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.dimension.DimensionValue.delete(h.sqlite, value_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_assign_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.dimension.LineDimension.assign(h.sqlite, line_id, dimension_value_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_remove_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.dimension.LineDimension.remove(h.sqlite, line_id, dimension_value_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_dimension_summary(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const b = safeBuf(buf, buf_len) orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.dimensionSummary(h.sqlite, book_id, dimension_id, std.mem.span(start_date), std.mem.span(end_date), b, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Sprint 20D: Dimension List Exports ───────────────────────

pub export fn ledger_list_dimensions(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.dimension.listDimensions(h.sqlite, book_id, tf, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_list_dimension_values(handle: ?*LedgerDB, dimension_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.listDimensionValues(h.sqlite, dimension_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Sprint 20C: Schema Self-Description ──────────────────────

pub export fn ledger_describe_schema(handle: ?*LedgerDB, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.describe.describeSchema(h.sqlite, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Sprint 20B: Approval Workflow ─────────────────────────────

pub export fn ledger_approve_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.approve(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_reject_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.entry.Entry.reject(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_require_approval(handle: ?*LedgerDB, book_id: i64, require: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.book.Book.setRequireApproval(h.sqlite, book_id, require != 0, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

// ── Sprint 18B: Budget C ABI ──────────────────────────────────

pub export fn ledger_create_budget(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, fiscal_year: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.budget.Budget.create(h.sqlite, book_id, std.mem.span(name), fiscal_year, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_delete_budget(handle: ?*LedgerDB, budget_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.budget.Budget.delete(h.sqlite, budget_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_set_budget_line(handle: ?*LedgerDB, budget_id: i64, account_id: i64, period_id: i64, amount: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    return heft.budget.BudgetLine.set(h.sqlite, budget_id, account_id, period_id, amount, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_budget_vs_actual(handle: ?*LedgerDB, budget_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const b = safeBuf(buf, buf_len) orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.budget.budgetVsActual(h.sqlite, budget_id, std.mem.span(start_date), std.mem.span(end_date), b, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Sprint 19: Batch Operations ─────────────────────────────────

pub export fn ledger_batch_post(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    const h = handle orelse return false;
    const json = std.mem.span(entry_ids_json);
    var ids_buf: [1000]i64 = undefined;
    const count = heft.batch.parseIdArray(json, &ids_buf) catch {
        setError(mapError(error.InvalidInput));
        return false;
    };
    if (count == 0) {
        setError(mapError(error.InvalidInput));
        return false;
    }
    const result = heft.batch.batchPost(h.sqlite, ids_buf[0..count], std.mem.span(performed_by));
    if (out_succeeded) |p| p.* = result.succeeded;
    if (out_failed) |p| p.* = result.failed;
    return result.failed == 0;
}

pub export fn ledger_batch_void(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, reason: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    const h = handle orelse return false;
    const json = std.mem.span(entry_ids_json);
    var ids_buf: [1000]i64 = undefined;
    const count = heft.batch.parseIdArray(json, &ids_buf) catch {
        setError(mapError(error.InvalidInput));
        return false;
    };
    if (count == 0) {
        setError(mapError(error.InvalidInput));
        return false;
    }
    const result = heft.batch.batchVoid(h.sqlite, ids_buf[0..count], std.mem.span(reason), std.mem.span(performed_by));
    if (out_succeeded) |p| p.* = result.succeeded;
    if (out_failed) |p| p.* = result.failed;
    return result.failed == 0;
}

// ── Sprint 13A: Cache Recalculation ────────────────────────────

pub export fn ledger_recalculate_balances(handle: ?*LedgerDB, book_id: i64) i32 {
    const h = handle orelse return -1;
    const count = heft.cache.recalculateAllStale(h.sqlite, book_id) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(count);
}

// ── Export Wrappers ────────────────────────────────────────────

pub export fn ledger_export_chart_of_accounts(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportChartOfAccounts(h.sqlite, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_export_journal_entries(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportJournalEntries(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_export_audit_trail(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportAuditTrail(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_export_periods(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportPeriods(h.sqlite, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_export_subledger(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportSubledger(h.sqlite, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_export_book_metadata(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportBookMetadata(h.sqlite, book_id, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_transition_budget(handle: ?*LedgerDB, budget_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    const target = heft.budget.BudgetStatus.fromString(std.mem.span(target_status)) orelse {
        setError(mapError(error.InvalidInput));
        return false;
    };
    heft.budget.Budget.transition(h.sqlite, budget_id, target, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

// ── Open Item Wrappers ─────────────────────────────────────────

pub export fn ledger_create_open_item(handle: ?*LedgerDB, entry_line_id: i64, counterparty_id: i64, original_amount: i64, due_date: ?[*:0]const u8, book_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return -1;
    const dd: ?[]const u8 = if (due_date) |d| std.mem.span(d) else null;
    return heft.open_item.createOpenItem(h.sqlite, entry_line_id, counterparty_id, original_amount, dd, book_id, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return -1;
    };
}

pub export fn ledger_allocate_payment(handle: ?*LedgerDB, open_item_id: i64, amount: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return false;
    heft.open_item.allocatePayment(h.sqlite, open_item_id, amount, std.mem.span(performed_by)) catch |err| {
        setError(mapError(err));
        return false;
    };
    return true;
}

pub export fn ledger_list_open_items(handle: ?*LedgerDB, counterparty_id: i64, include_closed: bool, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.open_item.listOpenItems(h.sqlite, counterparty_id, include_closed, safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

pub export fn ledger_cash_flow_indirect(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, classification_id: i64) ?*heft.classification.CashFlowIndirectResult {
    const h = handle orelse return null;
    return heft.classification.cashFlowStatementIndirect(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), classification_id) catch null;
}

pub export fn ledger_free_cash_flow_indirect(result: ?*heft.classification.CashFlowIndirectResult) void {
    if (result) |r| r.deinit();
}

pub export fn ledger_classified_trial_balance(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse return null;
    return heft.classification.classifiedTrialBalance(h.sqlite, classification_id, std.mem.span(as_of_date)) catch null;
}

pub export fn ledger_dimension_summary_rollup(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return -1;
    const fmt: heft.export_mod.ExportFormat = if (format == 0) .csv else if (format == 1) .json else {
        setError(mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.dimensionSummaryRollup(h.sqlite, book_id, dimension_id, std.mem.span(start_date), std.mem.span(end_date), safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        setError(mapError(err));
        return -1;
    };
    return safeIntCast(result.len);
}

// ── Tests ───────────────────────────────────────────────────────

fn cleanupTestFile(name: [*:0]const u8) void {
    const cwd = std.fs.cwd();
    const base = std.mem.span(name);
    cwd.deleteFile(base) catch {};
    // WAL mode creates -wal and -shm sidecar files
    var wal_buf: [256]u8 = undefined;
    var shm_buf: [256]u8 = undefined;
    const wal_name = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{base}) catch return;
    const shm_name = std.fmt.bufPrint(&shm_buf, "{s}-shm", .{base}) catch return;
    cwd.deleteFile(wal_name) catch {};
    cwd.deleteFile(shm_name) catch {};
}

test "ledger_version returns 0.0.1" {
    const v = std.mem.span(ledger_version());
    try std.testing.expectEqualStrings("0.0.1", v);
}

test "ledger_open returns non-null, ledger_close cleans up" {
    defer cleanupTestFile("test-open-close.ledger");
    const handle = ledger_open("test-open-close.ledger");
    try std.testing.expect(handle != null);
    if (handle) |h| ledger_close(h);
}

test "ledger_open creates all 16 schema tables" {
    defer cleanupTestFile("test-schema-main.ledger");
    const handle = ledger_open("test-schema-main.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const expected_tables = [_][]const u8{
            "ledger_books",
            "ledger_accounts",
            "ledger_periods",
            "ledger_classifications",
            "ledger_classification_nodes",
            "ledger_subledger_groups",
            "ledger_subledger_accounts",
            "ledger_entries",
            "ledger_entry_lines",
            "ledger_account_balances",
            "ledger_dimensions",
            "ledger_dimension_values",
            "ledger_line_dimensions",
            "ledger_budgets",
            "ledger_budget_lines",
            "ledger_audit_log",
        };

        for (expected_tables) |table_name| {
            var stmt = try h.sqlite.prepare(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;",
            );
            defer stmt.finalize();
            try stmt.bindText(1, table_name);
            _ = try stmt.step();
            try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
        }
    }
}

test "ledger_open enables WAL mode" {
    defer cleanupTestFile("test-wal.ledger");
    const handle = ledger_open("test-wal.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA journal_mode;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("wal", stmt.columnText(0).?);
    }
}

test "ledger_open enables foreign keys" {
    defer cleanupTestFile("test-fk.ledger");
    const handle = ledger_open("test-fk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA foreign_keys;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }
}

test "ledger_open sets schema version on new file" {
    defer cleanupTestFile("test-version.ledger");
    const handle = ledger_open("test-version.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(SCHEMA_VERSION, stmt.columnInt(0));
    }
}

test "ledger_open rejects future schema version" {
    defer cleanupTestFile("test-future-version.ledger");

    const h1 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const raw_db = try heft.db.Database.open("test-future-version.ledger");
    try raw_db.exec("PRAGMA user_version = 999;");
    raw_db.close();

    const h2 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h2 == null);
}

test "ledger_open preserves schema version on reopen" {
    defer cleanupTestFile("test-reopen-version.ledger");

    const h1 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const h2 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(SCHEMA_VERSION, stmt.columnInt(0));
    }
}

test "ledger_open returns null for invalid path" {
    const handle = ledger_open("/no/such/dir/bad.ledger");
    try std.testing.expect(handle == null);
}

test "ledger_open is idempotent on existing file" {
    defer cleanupTestFile("test-idempotent.ledger");
    const h1 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    // Open same file again — schema uses IF NOT EXISTS
    const h2 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| ledger_close(h);
}

// ── Sprint 2: C ABI integration tests ──────────────────────────

test "C ABI: full lifecycle book -> account -> period -> transition" {
    defer cleanupTestFile("test-cabi-lifecycle.ledger");
    const handle = ledger_open("test-cabi-lifecycle.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(acct_id > 0);
        const rounding_acct = ledger_create_account(h, book_id, "7000", "Rounding", "expense", 0, "admin");
        try std.testing.expect(rounding_acct > 0);

        const period_id = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(period_id > 0);

        try std.testing.expect(ledger_transition_period(h, period_id, "soft_closed", "admin"));
        try std.testing.expect(ledger_set_rounding_account(h, book_id, rounding_acct, "admin"));
        try std.testing.expect(ledger_update_account_status(h, acct_id, "archived", "admin"));
    }
}

test "C ABI: bulk create periods via C boundary" {
    defer cleanupTestFile("test-cabi-bulk.ledger");
    const handle = ledger_open("test-cabi-bulk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        try std.testing.expect(ledger_bulk_create_periods(h, book_id, 2026, 1, "monthly", "admin"));

        var stmt = try h.sqlite.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
    }
}

test "C ABI: null handle returns error values" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_book(null, "Test", "PHP", 2, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_account(null, 1, "1000", "Cash", "asset", 0, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_period(null, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin"));
    try std.testing.expect(!ledger_update_account_status(null, 1, "inactive", "admin"));
    try std.testing.expect(!ledger_transition_period(null, 1, "soft_closed", "admin"));
    try std.testing.expect(!ledger_set_rounding_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_bulk_create_periods(null, 1, 2026, 1, "monthly", "admin"));
    try std.testing.expect(!ledger_archive_book(null, 1, "admin"));
}

test "C ABI: invalid account_type string returns -1" {
    defer cleanupTestFile("test-cabi-bad-type.ledger");
    const handle = ledger_open("test-cabi-bad-type.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const result = ledger_create_account(h, book_id, "1000", "Cash", "invalid_type", 0, "admin");
        try std.testing.expectEqual(@as(i64, -1), result);
    }
}

test "C ABI: invalid granularity string returns false" {
    defer cleanupTestFile("test-cabi-bad-gran.ledger");
    const handle = ledger_open("test-cabi-bad-gran.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        try std.testing.expect(!ledger_bulk_create_periods(h, book_id, 2026, 1, "weekly", "admin"));
    }
}

test "C ABI: invalid account status string returns false" {
    defer cleanupTestFile("test-cabi-bad-acct-status.ledger");
    const handle = ledger_open("test-cabi-bad-acct-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(!ledger_update_account_status(h, acct_id, "deleted", "admin"));
    }
}

test "C ABI: invalid period status string returns false" {
    defer cleanupTestFile("test-cabi-bad-period-status.ledger");
    const handle = ledger_open("test-cabi-bad-period-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_transition_period(h, 1, "deleted", "admin"));
    }
}

test "C ABI: archive book via C boundary" {
    defer cleanupTestFile("test-cabi-archive.ledger");
    const handle = ledger_open("test-cabi-archive.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(ledger_archive_book(h, book_id, "admin"));

        var stmt = try h.sqlite.prepare("SELECT status FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
    }
}

test "C ABI: archive book with open periods returns false" {
    defer cleanupTestFile("test-cabi-archive-fail.ledger");
    const handle = ledger_open("test-cabi-archive-fail.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_archive_book(h, book_id, "admin"));
    }
}

// ── Sprint 3 C ABI integration tests ───────────────────────────

test "C ABI: full posting lifecycle through C boundary" {
    defer cleanupTestFile("test-cabi-posting.ledger");
    const handle = ledger_open("test-cabi-posting.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(entry_id > 0);

        const line1 = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        try std.testing.expect(line1 > 0);

        const line2 = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        try std.testing.expect(line2 > 0);

        try std.testing.expect(ledger_post_entry(h, entry_id, "admin"));

        var stmt = try h.sqlite.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
    }
}

test "C ABI: void entry through C boundary" {
    defer cleanupTestFile("test-cabi-void.ledger");
    const handle = ledger_open("test-cabi-void.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        try std.testing.expect(ledger_void_entry(h, entry_id, "Error", "admin"));
    }
}

test "C ABI: reverse entry through C boundary" {
    defer cleanupTestFile("test-cabi-reverse.ledger");
    const handle = ledger_open("test-cabi-reverse.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const reversal_id = ledger_reverse_entry(h, entry_id, "Accrual reversal", "2026-01-31", 0, "admin");
        try std.testing.expect(reversal_id > 0);
    }
}

test "C ABI: null handle returns error for Sprint 3 exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_draft(null, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_line(null, 1, 1, 100, 0, "PHP", 10_000_000_000, 1, 0, null, "admin"));
    try std.testing.expect(!ledger_post_entry(null, 1, "admin"));
    try std.testing.expect(!ledger_void_entry(null, 1, "Error", "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_reverse_entry(null, 1, "Reason", "2026-01-31", 0, "admin"));
    try std.testing.expect(!ledger_remove_line(null, 1, "admin"));
    try std.testing.expect(!ledger_delete_draft(null, 1, "admin"));
    try std.testing.expect(!ledger_edit_line(null, 1, 100, 0, "PHP", 10_000_000_000, 1, "admin"));
}

test "C ABI: edit line through C boundary" {
    defer cleanupTestFile("test-cabi-editline.ledger");
    const handle = ledger_open("test-cabi-editline.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line_id = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");

        try std.testing.expect(ledger_edit_line(h, line_id, 2_000_000_000_00, 0, "PHP", 10_000_000_000, 1, "admin"));
    }
}

test "C ABI: remove line and delete draft through C boundary" {
    defer cleanupTestFile("test-cabi-draft-ops.ledger");
    const handle = ledger_open("test-cabi-draft-ops.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line_id = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");

        try std.testing.expect(ledger_remove_line(h, line_id, "admin"));
        try std.testing.expect(ledger_delete_draft(h, entry_id, "admin"));
    }
}

// ── Sprint 4 C ABI: Report tests ───────────────────────────────

test "C ABI: trial balance through C boundary" {
    defer cleanupTestFile("test-cabi-tb.ledger");
    const handle = ledger_open("test-cabi-tb.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_trial_balance(h, book_id, "2026-01-31");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expect(ledger_result_row_count(r) >= 2);
            try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: income statement through C boundary" {
    defer cleanupTestFile("test-cabi-is.ledger");
    const handle = ledger_open("test-cabi-is.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "4000", "Revenue", "revenue", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_income_statement(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expectEqual(@as(i64, 5_000_000_000_00), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: balance sheet through C boundary" {
    defer cleanupTestFile("test-cabi-bs.ledger");
    const handle = ledger_open("test-cabi-bs.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_balance_sheet(h, book_id, "2026-01-31", "2026-01-01");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: null handle returns null/error for all exports" {
    try std.testing.expect(ledger_trial_balance(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_income_statement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_trial_balance_movement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_balance_sheet(null, 1, "2026-01-31", "2026-01-01") == null);
    try std.testing.expect(ledger_general_ledger(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_account_ledger(null, 1, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_journal_register(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expectEqual(@as(i64, -1), ledger_create_subledger_group(null, 1, "X", "customer", 1, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_subledger_account(null, 1, "X", "X", "customer", 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_classification(null, 1, "X", "balance_sheet", "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_group_node(null, 1, "X", 0, 0, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_account_node(null, 1, 1, 0, 0, "admin"));
    try std.testing.expect(!ledger_move_node(null, 1, 0, 0, "admin"));
    try std.testing.expect(!ledger_delete_classification(null, 1, "admin"));
    try std.testing.expect(ledger_classified_report(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_cash_flow_statement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_trial_balance_comparative(null, 1, "2026-01-31", "2025-12-31") == null);
    try std.testing.expect(ledger_income_statement_comparative(null, 1, "2026-01-01", "2026-01-31", "2025-01-01", "2025-12-31") == null);
    try std.testing.expect(ledger_balance_sheet_comparative(null, 1, "2026-01-31", "2025-12-31", "2026-01-01") == null);
    try std.testing.expect(ledger_trial_balance_movement_comparative(null, 1, "2026-01-01", "2026-01-31", "2025-01-01", "2025-12-31") == null);
}

test "C ABI: free null classified result is safe" {
    ledger_free_classified_result(null);
}

test "C ABI: classified report through C boundary" {
    defer cleanupTestFile("test-cabi-cls.ledger");
    const handle = ledger_open("test-cabi-cls.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const cls_id = ledger_create_classification(h, book_id, "BS", "balance_sheet", "admin");
        try std.testing.expect(cls_id > 0);

        const group = ledger_add_group_node(h, cls_id, "Assets", 0, 0, "admin");
        try std.testing.expect(group > 0);

        _ = ledger_add_account_node(h, cls_id, 1, group, 0, "admin");

        const result = ledger_classified_report(h, cls_id, "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| ledger_free_classified_result(r);
    }
}

test "C ABI: cash flow statement through C boundary" {
    defer cleanupTestFile("test-cabi-cfs.ledger");
    const handle = ledger_open("test-cabi-cfs.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const cls_id = ledger_create_classification(h, book_id, "SCF", "cash_flow", "admin");
        try std.testing.expect(cls_id > 0);

        const operating = ledger_add_group_node(h, cls_id, "Operating", 0, 0, "admin");
        try std.testing.expect(operating > 0);

        _ = ledger_add_account_node(h, cls_id, 1, operating, 0, "admin");

        const result = ledger_cash_flow_statement(h, cls_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| {
            try std.testing.expect(r.rows.len == 2);
            try std.testing.expect(r.rows[0].debit_balance == 10_000_000_000_00);
            ledger_free_classified_result(r);
        }
    }
}

test "C ABI: free null results is safe" {
    ledger_free_result(null);
    ledger_free_ledger_result(null);
    ledger_free_comparative_result(null);
}

test "C ABI: GL through C boundary" {
    defer cleanupTestFile("test-cabi-gl.ledger");
    const handle = ledger_open("test-cabi-gl.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const gl = ledger_general_ledger(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(gl != null);
        if (gl) |r| ledger_free_ledger_result(r);

        const al = ledger_account_ledger(h, book_id, 1, "2026-01-01", "2026-01-31");
        try std.testing.expect(al != null);
        if (al) |r| ledger_free_ledger_result(r);

        const jr = ledger_journal_register(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(jr != null);
        if (jr) |r| ledger_free_ledger_result(r);
    }
}

test "C ABI: ledger_edit_draft changes header fields" {
    defer cleanupTestFile("test-cabi-editdraft.ledger");
    const handle = ledger_open("test-cabi-editdraft.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(entry_id > 0);

        // Edit the draft header
        try std.testing.expect(ledger_edit_draft(h, entry_id, "JE-999", "2026-01-20", "2026-01-25", "Updated desc", null, 1, "admin"));

        // Verify via null handle rejection
        try std.testing.expect(!ledger_edit_draft(null, entry_id, "JE-999", "2026-01-20", "2026-01-25", null, null, 1, "admin"));
    }
}

test "C ABI: ledger_edit_posted changes description on posted entry" {
    defer cleanupTestFile("test-cabi-editposted.ledger");
    const handle = ledger_open("test-cabi-editposted.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        // Edit description on posted entry
        try std.testing.expect(ledger_edit_posted(h, entry_id, "Updated memo", null, "admin"));

        // Null handle rejection
        try std.testing.expect(!ledger_edit_posted(null, entry_id, "Memo", null, "admin"));
    }
}

test "C ABI: ledger_free_classified_result with null is safe" {
    ledger_free_classified_result(null);
}

test "C ABI: ledger_update_book_name" {
    defer cleanupTestFile("test-cabi-updatename.ledger");
    const handle = ledger_open("test-cabi-updatename.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Old", "PHP", 2, "admin");
        try std.testing.expect(ledger_update_book_name(h, 1, "New", "admin"));
        try std.testing.expect(!ledger_update_book_name(null, 1, "New", "admin"));
    }
}

test "C ABI: ledger_update_account_name" {
    defer cleanupTestFile("test-cabi-acctname.ledger");
    const handle = ledger_open("test-cabi-acctname.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(ledger_update_account_name(h, 1, "Petty Cash", "admin"));
    }
}

test "C ABI: ledger_set_account_monetary" {
    defer cleanupTestFile("test-cabi-monetary.ledger");
    const handle = ledger_open("test-cabi-monetary.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1500", "Equipment", "asset", 0, "admin");
        try std.testing.expect(ledger_set_account_monetary(h, 1, 0, "admin"));
    }
}

test "C ABI: ledger_set_account_parent" {
    defer cleanupTestFile("test-cabi-parent.ledger");
    const handle = ledger_open("test-cabi-parent.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Current Assets", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "1001", "Cash", "asset", 0, "admin");
        try std.testing.expect(ledger_set_account_parent(h, 2, 1, "admin"));
        try std.testing.expect(ledger_set_account_parent(h, 2, 0, "admin"));
    }
}

test "C ABI: ledger_set_fy_start_month" {
    defer cleanupTestFile("test-cabi-fy.ledger");
    const handle = ledger_open("test-cabi-fy.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "India", "INR", 2, "admin");
        try std.testing.expect(ledger_set_fy_start_month(h, 1, 4, "admin"));
    }
}

test "C ABI: ledger_set_entity_type" {
    defer cleanupTestFile("test-cabi-entity.ledger");
    const handle = ledger_open("test-cabi-entity.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Juan dela Cruz", "PHP", 2, "admin");
        try std.testing.expect(ledger_set_entity_type(h, 1, "sole_proprietorship", "admin"));
        // Invalid entity type string must return false
        try std.testing.expect(!ledger_set_entity_type(h, 1, "nonsense", "admin"));
        // Null handle returns false
        try std.testing.expect(!ledger_set_entity_type(null, 1, "corporation", "admin"));
    }
}

test "C ABI: ledger_balance_sheet_auto" {
    defer cleanupTestFile("test-cabi-bsauto.ledger");
    const handle = ledger_open("test-cabi-bsauto.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(ledger_add_line(h, eid, 1, 1000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin") > 0);
        try std.testing.expect(ledger_add_line(h, eid, 2, 0, 1000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin") > 0);
        try std.testing.expect(ledger_post_entry(h, eid, "admin"));
        const result = ledger_balance_sheet_auto(h, 1, "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| {
            defer r.deinit();
            try std.testing.expectEqual(r.total_debits, r.total_credits);
        }
    }
}

test "C ABI: ledger_translate_report" {
    defer cleanupTestFile("test-cabi-translate.ledger");
    const handle = ledger_open("test-cabi-translate.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, eid, 1, 5650_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, eid, 2, 0, 5650_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, eid, "admin");
        const tb = try heft.report.trialBalance(h.sqlite, 1, "2026-01-31");
        defer tb.deinit();
        const translated = ledger_translate_report(tb, 180_000_000, 175_000_000);
        try std.testing.expect(translated != null);
        if (translated) |t| {
            defer t.deinit();
            try std.testing.expectEqual(tb.rows.len, t.rows.len);
        }
    }
}

test "C ABI: ledger_delete_node" {
    defer cleanupTestFile("test-cabi-delnode.ledger");
    const handle = ledger_open("test-cabi-delnode.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        const cid = ledger_create_classification(h, 1, "BS", "balance_sheet", "admin");
        const gid = ledger_add_group_node(h, cid, "Assets", 0, 1, "admin");
        try std.testing.expect(gid > 0);
        try std.testing.expect(ledger_delete_node(h, gid, "admin"));
    }
}

test "C ABI: ledger_get_book returns bytes" {
    defer cleanupTestFile("test-cabi-getbook.ledger");
    const handle = ledger_open("test-cabi-getbook.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        var buf: [4096]u8 = undefined;
        const len = ledger_get_book(h, 1, &buf, 4096, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Test\"") != null);
    }
}

test "C ABI: ledger_list_books returns paginated JSON" {
    defer cleanupTestFile("test-cabi-listbooks.ledger");
    const handle = ledger_open("test-cabi-listbooks.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Book A", "PHP", 2, "admin");
        _ = ledger_create_book(h, "Book B", "USD", 2, "admin");
        var buf: [8192]u8 = undefined;
        const len = ledger_list_books(h, null, 0, 100, 0, &buf, 8192, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    }
}

test "C ABI: ledger_list_accounts with filter" {
    defer cleanupTestFile("test-cabi-listaccts.ledger");
    const handle = ledger_open("test-cabi-listaccts.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "2000", "AP", "liability", 0, "admin");
        var buf: [8192]u8 = undefined;
        const len = ledger_list_accounts(h, 1, "asset", null, null, 0, 100, 0, &buf, 8192, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "Cash") != null);
    }
}

test "C ABI: ledger_list_entries with date range" {
    defer cleanupTestFile("test-cabi-listentries.ledger");
    const handle = ledger_open("test-cabi-listentries.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        _ = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        var buf: [16384]u8 = undefined;
        const len = ledger_list_entries(h, 1, null, "2026-01-01", "2026-01-31", null, 0, 100, 0, &buf, 16384, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "JE-001") != null);
    }
}

test "C ABI: ledger_list_audit_log" {
    defer cleanupTestFile("test-cabi-listaudit.ledger");
    const handle = ledger_open("test-cabi-listaudit.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        var buf: [32768]u8 = undefined;
        const len = ledger_list_audit_log(h, 1, null, null, null, null, 0, 100, 0, &buf, 32768, 1);
        try std.testing.expect(len > 0);
    }
}

test "C ABI: null handle returns -1 for all query exports" {
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_get_book(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_books(null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_account(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_accounts(null, 1, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_period(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_periods(null, 1, 0, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_entry(null, 1, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_entries(null, 1, null, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_entry_lines(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_classifications(null, 1, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_subledger_groups(null, 1, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_subledger_accounts(null, 1, 0, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_audit_log(null, 1, null, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_subledger_report(null, 1, 0, null, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_counterparty_ledger(null, 1, 1, 0, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_transactions(null, 1, 0, 0, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_subledger_reconciliation(null, 1, 1, "2026-01-31", &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_aged_subledger(null, 1, 0, "2026-01-31", 0, 100, 0, &buf, 1024, 1));
}

test "C ABI: null handle returns false for all CRUD exports" {
    try std.testing.expect(!ledger_update_book_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_account_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_set_account_parent(null, 1, 2, "admin"));
    try std.testing.expect(!ledger_set_account_monetary(null, 1, 0, "admin"));
    try std.testing.expect(!ledger_set_fy_start_month(null, 1, 4, "admin"));
    try std.testing.expect(ledger_balance_sheet_auto(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_translate_report(null, 10_000_000_000, 10_000_000_000) == null);
    try std.testing.expect(!ledger_update_classification_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_node_label(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_delete_node(null, 1, "admin"));
    try std.testing.expect(!ledger_update_subledger_group_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_delete_subledger_group(null, 1, "admin"));
    try std.testing.expect(!ledger_update_subledger_account_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_subledger_account_status(null, 1, "inactive", "admin"));
    try std.testing.expect(!ledger_delete_subledger_account(null, 1, "admin"));
    try std.testing.expect(!ledger_edit_line_full(null, 1, 100, 0, "PHP", 10_000_000_000, 1, 0, null, "admin"));
}

test "C ABI: ledger_last_error returns error code after failure" {
    defer cleanupTestFile("test-cabi-lasterr.ledger");
    const handle = ledger_open("test-cabi-lasterr.ledger");
    if (handle) |h| {
        defer ledger_close(h);

        // Create book succeeds — error should be 0
        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        // Try to create book with invalid currency — should fail and set error
        const bad = ledger_create_book(h, "Bad", "XX", 2, "admin");
        try std.testing.expectEqual(@as(i64, -1), bad);
        const err = ledger_last_error();
        try std.testing.expectEqual(@as(i32, 2), err);
    }
}

test "C ABI: ledger_last_error after post failure" {
    defer cleanupTestFile("test-cabi-lasterr2.ledger");
    const handle = ledger_open("test-cabi-lasterr2.ledger");
    if (handle) |h| {
        defer ledger_close(h);

        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        // Only 1 line — post should fail with TooFewLines
        _ = ledger_add_line(h, eid, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        const posted = ledger_post_entry(h, eid, "admin");
        try std.testing.expect(!posted);
        const err = ledger_last_error();
        try std.testing.expectEqual(@as(i32, 16), err);
    }
}

test "C ABI: ledger_open null path returns null" {
    const handle = ledger_open(null);
    try std.testing.expect(handle == null);
    try std.testing.expect(ledger_last_error() > 0);
}

test "C ABI: ledger_verify null out params returns false" {
    defer cleanupTestFile("test-verify-null.ledger");
    const h = ledger_open("test-verify-null.ledger");
    try std.testing.expect(h != null);
    defer if (h) |handle| ledger_close(handle);
    if (h) |handle| {
        const book_id = ledger_create_book(handle, "Test", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);
        try std.testing.expect(!ledger_verify(handle, book_id, null, null));
    }
}

test "C ABI: ledger_verify clean book passes" {
    defer cleanupTestFile("test-verify-clean.ledger");
    const h = ledger_open("test-verify-clean.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1001", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "4001", "Revenue", "revenue", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(h, entry_id, 1, 100_000_000, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(h, entry_id, 2, 0, 100_000_000, "PHP", 10_000_000_000, 2, 0, null, "admin");
    _ = ledger_post_entry(h, entry_id, "admin");
    var errors: u32 = 99;
    var warnings: u32 = 99;
    const passed = ledger_verify(h, book_id, &errors, &warnings);
    try std.testing.expect(passed);
    try std.testing.expectEqual(@as(u32, 0), errors);
}

test "C ABI: ledger_edit_line_full happy path" {
    defer cleanupTestFile("test-editfull.ledger");
    const h = ledger_open("test-editfull.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1001", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "4001", "Revenue", "revenue", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = ledger_add_line(h, entry_id, 1, 100_000_000, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    try std.testing.expect(line_id > 0);
    const ok = ledger_edit_line_full(h, line_id, 200_000_000, 0, "PHP", 10_000_000_000, 1, 0, "Office supplies", "admin");
    try std.testing.expect(ok);
}

test "C ABI: invalid format value returns error" {
    defer cleanupTestFile("test-badfmt.ledger");
    const h = ledger_open("test-badfmt.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    var buf: [1024]u8 = undefined;
    const result = ledger_get_book(h, book_id, &buf, 1024, 99);
    try std.testing.expectEqual(@as(i32, -1), result);
    try std.testing.expectEqual(@as(i32, 2), ledger_last_error());
}

// ── Sprint 12: System account designation + parameter gap tests ──

test "C ABI: null handle returns false for system account exports" {
    try std.testing.expect(!ledger_set_fx_gain_loss_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_retained_earnings_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_income_summary_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_opening_balance_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_suspense_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_validate_opening_balance(null, 1));
}

test "C ABI: ledger_create_draft with description and metadata" {
    defer cleanupTestFile("test-cabi-draft-desc.ledger");
    const h = ledger_open("test-cabi-draft-desc.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-100", "2026-01-15", "2026-01-15", "Office supplies purchase", 1, "{\"dept\":\"ops\"}", "admin");
    try std.testing.expect(entry_id > 0);
    var buf: [8192]u8 = undefined;
    const len = ledger_get_entry(h, entry_id, book_id, &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "Office supplies purchase") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "ops") != null);
}

test "C ABI: ledger_add_line with description" {
    defer cleanupTestFile("test-cabi-line-desc.ledger");
    const h = ledger_open("test-cabi-line-desc.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "5000", "Supplies", "expense", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-200", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = ledger_add_line(h, entry_id, 1, 500_000_000, 0, "PHP", 10_000_000_000, 1, 0, "Pens and paper", "admin");
    try std.testing.expect(line_id > 0);
    var buf: [8192]u8 = undefined;
    const len = ledger_list_entry_lines(h, entry_id, &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "Pens and paper") != null);
}

test "C ABI: ledger_reverse_entry with target_period_id" {
    defer cleanupTestFile("test-cabi-rev-period.ledger");
    const h = ledger_open("test-cabi-rev-period.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
    const p1 = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const p2 = ledger_create_period(h, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    try std.testing.expect(p1 > 0);
    try std.testing.expect(p2 > 0);
    const entry_id = ledger_create_draft(h, book_id, "JE-300", "2026-01-15", "2026-01-15", null, p1, null, "admin");
    _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
    _ = ledger_post_entry(h, entry_id, "admin");
    const reversal_id = ledger_reverse_entry(h, entry_id, "Period correction", "2026-02-01", p2, "admin");
    try std.testing.expect(reversal_id > 0);
}

test "C ABI: system account designation happy path" {
    defer cleanupTestFile("test-cabi-sysacct.ledger");
    const h = ledger_open("test-cabi-sysacct.ledger") orelse unreachable;
    defer ledger_close(h);

    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    try std.testing.expect(book_id > 0);

    const expense_acct = ledger_create_account(h, book_id, "6900", "FX Rounding", "expense", 0, "admin");
    const equity_acct = ledger_create_account(h, book_id, "3100", "Retained Earnings", "equity", 0, "admin");
    const equity_acct2 = ledger_create_account(h, book_id, "3200", "Income Summary", "equity", 0, "admin");
    const equity_acct3 = ledger_create_account(h, book_id, "3300", "Opening Balance", "equity", 0, "admin");
    const asset_acct = ledger_create_account(h, book_id, "8000", "Suspense", "asset", 0, "admin");

    try std.testing.expect(expense_acct > 0);
    try std.testing.expect(equity_acct > 0);
    try std.testing.expect(equity_acct2 > 0);
    try std.testing.expect(equity_acct3 > 0);
    try std.testing.expect(asset_acct > 0);

    try std.testing.expect(ledger_set_fx_gain_loss_account(h, book_id, expense_acct, "admin"));
    try std.testing.expect(ledger_set_retained_earnings_account(h, book_id, equity_acct, "admin"));
    try std.testing.expect(ledger_set_income_summary_account(h, book_id, equity_acct2, "admin"));
    try std.testing.expect(ledger_set_opening_balance_account(h, book_id, equity_acct3, "admin"));
    try std.testing.expect(ledger_set_suspense_account(h, book_id, asset_acct, "admin"));

    var buf: [4096]u8 = undefined;
    const len = ledger_get_book(h, book_id, &buf, 4096, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];

    try std.testing.expect(std.mem.indexOf(u8, json, "\"fx_gain_loss_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"retained_earnings_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"income_summary_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"suspense_account_id\":") != null);
}

// ── Sprint 14: Period closing C ABI tests ──

test "C ABI: null handle returns false for ledger_close_period" {
    try std.testing.expect(!ledger_close_period(null, 1, 1, "admin"));
}

// ── Sprint 15: FX revaluation C ABI tests ──

test "C ABI: null handle returns -1 for ledger_revalue_forex_balances" {
    try std.testing.expectEqual(@as(i64, -1), ledger_revalue_forex_balances(null, 1, 1, "[{\"currency\":\"USD\",\"rate\":57000000000}]", "admin"));
}

// ── Sprint 16B: Equity changes C ABI tests ──

test "C ABI: null handle returns null for ledger_equity_changes" {
    try std.testing.expect(ledger_equity_changes(null, 1, "2026-01-01", "2026-01-31", "2026-01-01") == null);
}

test "C ABI: free null equity result is safe" {
    ledger_free_equity_result(null);
}

// ── Sprint 17: Dimension C ABI tests ──

test "C ABI: null handle returns error for dimension exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_dimension(null, 1, "Tax", "tax_code", "admin"));
    try std.testing.expect(!ledger_delete_dimension(null, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_dimension_value(null, 1, "VAT12", "VAT 12%", "admin"));
    try std.testing.expect(!ledger_delete_dimension_value(null, 1, "admin"));
    try std.testing.expect(!ledger_assign_line_dimension(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_remove_line_dimension(null, 1, 1, "admin"));
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_dimension_summary(null, 1, 1, "2026-01-01", "2026-01-31", &buf, 1024, 1));
}

test "C ABI: invalid dimension_type string returns -1" {
    defer cleanupTestFile("test-cabi-bad-dimtype.ledger");
    const handle = ledger_open("test-cabi-bad-dimtype.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const result = ledger_create_dimension(h, book_id, "Bad", "invalid_type", "admin");
        try std.testing.expectEqual(@as(i64, -1), result);
    }
}

test "C ABI: dimension full lifecycle through C boundary" {
    defer cleanupTestFile("test-cabi-dim-lifecycle.ledger");
    const handle = ledger_open("test-cabi-dim-lifecycle.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "4000", "Revenue", "revenue", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const dim_id = ledger_create_dimension(h, book_id, "Tax Code", "tax_code", "admin");
        try std.testing.expect(dim_id > 0);

        const val_id = ledger_create_dimension_value(h, dim_id, "VAT12", "VAT 12%", "admin");
        try std.testing.expect(val_id > 0);

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line1 = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        try std.testing.expect(ledger_assign_line_dimension(h, line1, val_id, "admin"));

        var buf: [4096]u8 = undefined;
        const len = ledger_dimension_summary(h, book_id, dim_id, "2026-01-01", "2026-01-31", &buf, 4096, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "VAT12") != null);

        try std.testing.expect(ledger_remove_line_dimension(h, line1, val_id, "admin"));
        try std.testing.expect(ledger_delete_dimension_value(h, val_id, "admin"));
        try std.testing.expect(ledger_delete_dimension(h, dim_id, "admin"));
    }
}

// ── Sprint 18B: Budget C ABI tests ──

test "C ABI: null handle returns error for budget exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_budget(null, 1, "FY2026", 2026, "admin"));
    try std.testing.expect(!ledger_delete_budget(null, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_set_budget_line(null, 1, 1, 1, 100, "admin"));
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_budget_vs_actual(null, 1, "2026-01-01", "2026-01-31", &buf, 1024, 1));
}

// ── Sprint 19: Batch C ABI tests ──

test "C ABI: null handle returns false for batch exports" {
    var succeeded: u32 = 99;
    var failed: u32 = 99;
    try std.testing.expect(!ledger_batch_post(null, "[1,2]", "admin", &succeeded, &failed));
    try std.testing.expect(!ledger_batch_void(null, "[1,2]", "Error", "admin", &succeeded, &failed));
}

test "C ABI: batch post and void lifecycle" {
    defer cleanupTestFile("test-cabi-batch.ledger");
    const handle = ledger_open("test-cabi-batch.ledger") orelse unreachable;
    defer ledger_close(handle);

    _ = ledger_create_book(handle, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, 1, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, 1, "2000", "AP", "liability", 0, "admin");
    _ = ledger_create_period(handle, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const e1 = ledger_create_draft(handle, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, e1, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, e1, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    const e2 = ledger_create_draft(handle, 1, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, e2, 1, 2_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, e2, 2, 0, 2_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    var succeeded: u32 = 0;
    var failed: u32 = 0;

    var ids_json_buf: [65]u8 = undefined;
    const ids_json = std.fmt.bufPrint(ids_json_buf[0..64], "[{d},{d}]", .{ e1, e2 }) catch unreachable;
    ids_json_buf[ids_json.len] = 0;
    const ids_z: [*:0]const u8 = ids_json_buf[0..ids_json.len :0];

    const post_ok = ledger_batch_post(handle, ids_z, "admin", &succeeded, &failed);
    try std.testing.expect(post_ok);
    try std.testing.expectEqual(@as(u32, 2), succeeded);
    try std.testing.expectEqual(@as(u32, 0), failed);

    const void_ok = ledger_batch_void(handle, ids_z, "Batch correction", "admin", &succeeded, &failed);
    try std.testing.expect(void_ok);
    try std.testing.expectEqual(@as(u32, 2), succeeded);
    try std.testing.expectEqual(@as(u32, 0), failed);
}

test "C ABI: ledger_describe_schema null handle returns -1" {
    var buf: [64]u8 = undefined;
    const result = ledger_describe_schema(null, &buf, 64, 0);
    try std.testing.expectEqual(@as(i32, -1), result);
}

test "C ABI: ledger_describe_schema happy path returns > 0" {
    defer cleanupTestFile("test-describe-schema.ledger");
    const handle = ledger_open("test-describe-schema.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    var buf: [65536]u8 = undefined;
    const csv_len = ledger_describe_schema(handle, &buf, 65536, 0);
    try std.testing.expect(csv_len > 0);

    const json_len = ledger_describe_schema(handle, &buf, 65536, 1);
    try std.testing.expect(json_len > 0);
}

test "C ABI: ledger_approve_entry null handle returns false" {
    const result = ledger_approve_entry(null, 1, "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_reject_entry null handle returns false" {
    const result = ledger_reject_entry(null, 1, "bad", "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_set_require_approval null handle returns false" {
    const result = ledger_set_require_approval(null, 1, 1, "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_recalculate_balances null handle returns -1" {
    const result = ledger_recalculate_balances(null, 1);
    try std.testing.expectEqual(@as(i32, -1), result);
}

test "C ABI: comprehensive lifecycle — book, accounts, period, draft, lines, approve, post, TB, verify, close, describe" {
    defer cleanupTestFile("test-cabi-comprehensive.ledger");
    const handle = ledger_open("test-cabi-comprehensive.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Comprehensive", "PHP", 2, "admin");
    try std.testing.expect(book_id > 0);

    const cash_id = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    try std.testing.expect(cash_id > 0);
    const rev_id = ledger_create_account(handle, book_id, "4000", "Revenue", "revenue", 0, "admin");
    try std.testing.expect(rev_id > 0);
    const equity_id = ledger_create_account(handle, book_id, "3000", "Capital", "equity", 0, "admin");
    try std.testing.expect(equity_id > 0);

    const period_id = ledger_create_period(handle, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expect(period_id > 0);

    try std.testing.expect(ledger_set_require_approval(handle, book_id, 1, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", "Service income", period_id, null, "admin");
    try std.testing.expect(entry_id > 0);

    const l1 = ledger_add_line(handle, entry_id, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, 0, null, "admin");
    try std.testing.expect(l1 > 0);
    const l2 = ledger_add_line(handle, entry_id, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, rev_id, 0, null, "admin");
    try std.testing.expect(l2 > 0);

    try std.testing.expect(ledger_approve_entry(handle, entry_id, "manager"));

    try std.testing.expect(ledger_post_entry(handle, entry_id, "admin"));

    const tb = ledger_trial_balance(handle, book_id, "2026-01-31");
    try std.testing.expect(tb != null);
    if (tb) |r| {
        defer ledger_free_result(r);
        try std.testing.expect(ledger_result_row_count(r) >= 2);
        try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        try std.testing.expectEqual(@as(i64, 5_000_000_000_00), ledger_result_total_debits(r));
    }

    var err_count: u32 = 0;
    var warn_count: u32 = 0;
    try std.testing.expect(ledger_verify(handle, book_id, &err_count, &warn_count));
    try std.testing.expectEqual(@as(u32, 0), err_count);

    try std.testing.expect(ledger_transition_period(handle, period_id, "soft_closed", "admin"));
    try std.testing.expect(ledger_transition_period(handle, period_id, "closed", "admin"));

    var desc_buf: [65536]u8 = undefined;
    const json_len = ledger_describe_schema(handle, &desc_buf, 65536, 1);
    try std.testing.expect(json_len > 0);
    const csv_len = ledger_describe_schema(handle, &desc_buf, 65536, 0);
    try std.testing.expect(csv_len > 0);
}

test "C ABI: ledger_reject_entry rejects draft with reason" {
    defer cleanupTestFile("test-cabi-reject.ledger");
    const handle = ledger_open("test-cabi-reject.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, book_id, "2000", "AP", "liability", 0, "admin");
    _ = ledger_create_period(handle, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try std.testing.expect(ledger_set_require_approval(handle, book_id, 1, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    try std.testing.expect(ledger_reject_entry(handle, entry_id, "Incorrect amounts", "manager"));

    try std.testing.expect(!ledger_post_entry(handle, entry_id, "admin"));
}

test "C ABI: ledger_revalue_forex_balances with valid rates" {
    defer cleanupTestFile("test-cabi-revalue.ledger");
    const handle = ledger_open("test-cabi-revalue.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "FX Book", "PHP", 2, "admin");
    const cash_php = ledger_create_account(handle, book_id, "1000", "Cash PHP", "asset", 0, "admin");
    const cash_usd = ledger_create_account(handle, book_id, "1001", "Cash USD", "asset", 0, "admin");
    const fx_gl = ledger_create_account(handle, book_id, "5000", "FX Gain/Loss", "expense", 0, "admin");
    _ = ledger_create_period(handle, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try std.testing.expect(ledger_set_fx_gain_loss_account(handle, book_id, fx_gl, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, entry_id, 1, 10_000_000_000, 0, "USD", 565_000_000_000, cash_usd, 0, null, "admin");
    _ = ledger_add_line(handle, entry_id, 2, 0, 565_000_000_000, "PHP", 10_000_000_000, cash_php, 0, null, "admin");
    try std.testing.expect(ledger_post_entry(handle, entry_id, "admin"));

    const reval_entry_id = ledger_revalue_forex_balances(handle, book_id, 1, "[{\"currency\":\"USD\",\"rate\":570000000000}]", "admin");
    try std.testing.expect(reval_entry_id > 0);
}

test "C ABI: export wrappers null handle returns -1" {
    var buf: [4096]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_export_chart_of_accounts(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_journal_entries(null, 1, "2026-01-01", "2026-12-31", &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_audit_trail(null, 1, "2026-01-01", "2026-12-31", &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_periods(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_subledger(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_book_metadata(null, 1, &buf, 4096, 0));
    try std.testing.expect(!ledger_transition_budget(null, 1, "approved", "admin"));
}

test "C ABI: export chart of accounts via C boundary" {
    defer cleanupTestFile("test-cabi-export-coa.ledger");
    const handle = ledger_open("test-cabi-export-coa.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Export Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, book_id, "4000", "Revenue", "revenue", 0, "admin");

    var buf: [8192]u8 = undefined;
    const csv_len = ledger_export_chart_of_accounts(handle, book_id, &buf, 8192, 0);
    try std.testing.expect(csv_len > 0);
    const csv = buf[0..@intCast(csv_len)];
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);

    const json_len = ledger_export_chart_of_accounts(handle, book_id, &buf, 8192, 1);
    try std.testing.expect(json_len > 0);
    const json = buf[0..@intCast(json_len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
}

test "C ABI: export audit trail via C boundary" {
    defer cleanupTestFile("test-cabi-export-audit.ledger");
    const handle = ledger_open("test-cabi-export-audit.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    _ = ledger_create_book(handle, "Audit Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const len = ledger_export_audit_trail(handle, 1, "2020-01-01", "2030-12-31", &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "create") != null);
}

test "C ABI: ledger_transition_budget via C boundary" {
    defer cleanupTestFile("test-cabi-budget-trans.ledger");
    const handle = ledger_open("test-cabi-budget-trans.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Budget Test", "PHP", 2, "admin");
    const budget_id = heft.budget.Budget.create(handle.sqlite, book_id, "FY2026", 2026, "admin") catch return error.TestUnexpectedResult;
    try std.testing.expect(ledger_transition_budget(handle, budget_id, "approved", "admin"));
    try std.testing.expect(!ledger_transition_budget(handle, budget_id, "draft", "admin"));
}
