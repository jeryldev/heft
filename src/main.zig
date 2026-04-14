// Heft — The embedded accounting engine
// Copyright (C) 2026 Jeryl Donato Estopace
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// main.zig: C ABI surface. Every public function lives here.
// Internal logic lives in db.zig, schema.zig, and future entity modules.
// This file is thin — convert between C types and Zig types, nothing more.

const std = @import("std");
const heft = @import("heft");
const abi_common = @import("abi_common.zig");
const abi_buffers = @import("abi_buffers.zig");
const abi_core = @import("abi_core.zig");
const abi_reports = @import("abi_reports.zig");
const VERSION = abi_common.VERSION;

// ── LedgerDB ────────────────────────────────────────────────────
// The opaque handle returned to C callers. Heap-allocated because
// it crosses the C ABI as a pointer.

pub const LedgerDB = abi_common.LedgerDB;

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
    _ = handle.sqlite.drainLeakedStatements();
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
    return VERSION;
}

// ── Sprint 2: Entity C ABI Exports ─────────────────────────────

pub export fn ledger_create_book(handle: ?*LedgerDB, name: [*:0]const u8, base_currency: [*:0]const u8, decimal_places: i32, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_book(handle, name, base_currency, decimal_places, performed_by);
}

pub export fn ledger_create_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, is_contra: i32, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_account(handle, book_id, number, name, account_type, is_contra, performed_by);
}

pub export fn ledger_create_period(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, period_number: i32, year: i32, start_date: [*:0]const u8, end_date: [*:0]const u8, period_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_period(handle, book_id, name, period_number, year, start_date, end_date, period_type, performed_by);
}

pub export fn ledger_update_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_account_status(handle, account_id, new_status, performed_by);
}

pub export fn ledger_transition_period(handle: ?*LedgerDB, period_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_transition_period(handle, period_id, target_status, performed_by);
}

pub export fn ledger_set_rounding_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_rounding_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_fx_gain_loss_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_fx_gain_loss_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_retained_earnings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_retained_earnings_account(handle, book_id, account_id, performed_by);
}

/// Generic alias for ledger_set_retained_earnings_account. Sets the
/// equity close target for any entity type (corporation RE, sole prop
/// Owner's Capital, nonprofit Net Assets, etc).
pub export fn ledger_set_equity_close_target(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_equity_close_target(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_dividends_drawings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_dividends_drawings_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_current_year_earnings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_current_year_earnings_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_income_summary_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_income_summary_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_opening_balance_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_opening_balance_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_set_suspense_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_suspense_account(handle, book_id, account_id, performed_by);
}

pub export fn ledger_validate_opening_balance(handle: ?*LedgerDB, book_id: i64) bool {
    return abi_core.ledger_validate_opening_balance(handle, book_id);
}

pub export fn ledger_bulk_create_periods(handle: ?*LedgerDB, book_id: i64, fiscal_year: i32, start_month: i32, granularity: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_bulk_create_periods(handle, book_id, fiscal_year, start_month, granularity, performed_by);
}

pub export fn ledger_create_draft(handle: ?*LedgerDB, book_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, period_id: i64, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_draft(handle, book_id, document_number, transaction_date, posting_date, description, period_id, metadata, performed_by);
}

pub export fn ledger_add_line(handle: ?*LedgerDB, entry_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_add_line(handle, entry_id, line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, counterparty_id, description, performed_by);
}

pub export fn ledger_edit_draft(handle: ?*LedgerDB, entry_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, metadata: ?[*:0]const u8, period_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_edit_draft(handle, entry_id, document_number, transaction_date, posting_date, description, metadata, period_id, performed_by);
}

pub export fn ledger_edit_posted(handle: ?*LedgerDB, entry_id: i64, description: ?[*:0]const u8, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_edit_posted(handle, entry_id, description, metadata, performed_by);
}

pub export fn ledger_post_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_post_entry(handle, entry_id, performed_by);
}

pub export fn ledger_void_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_void_entry(handle, entry_id, reason, performed_by);
}

pub export fn ledger_reverse_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, reversal_date: [*:0]const u8, target_period_id: i64, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_reverse_entry(handle, entry_id, reason, reversal_date, target_period_id, performed_by);
}

pub export fn ledger_remove_line(handle: ?*LedgerDB, line_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_remove_line(handle, line_id, performed_by);
}

pub export fn ledger_delete_draft(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_draft(handle, entry_id, performed_by);
}

pub export fn ledger_edit_line(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_edit_line(handle, line_id, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, performed_by);
}

// ── Report exports — OWNERSHIP CONTRACT ───────────────────────────
//
// All ledger_* functions in this section that return a non-null pointer
// transfer ownership of the returned struct to the C caller. The caller
// MUST eventually call the matching free function or the memory leaks:
//
//   ReportResult            -> ledger_free_result
//   LedgerResult            -> ledger_free_ledger_result
//   ComparativeReportResult -> ledger_free_comparative_result
//
// A null return means an error occurred — call ledger_last_error() to
// retrieve the error code. Null returns NEVER need freeing.
//
// The returned structs are opaque from C: access fields via the
// ledger_result_* / ledger_ledger_result_* / ledger_comparative_result_*
// accessor exports below. Do not dereference the pointer directly.

pub export fn ledger_trial_balance(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    return abi_reports.ledger_trial_balance(handle, book_id, as_of_date);
}

pub export fn ledger_income_statement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    return abi_reports.ledger_income_statement(handle, book_id, start_date, end_date);
}

pub export fn ledger_trial_balance_movement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    return abi_reports.ledger_trial_balance_movement(handle, book_id, start_date, end_date);
}

pub export fn ledger_balance_sheet(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.ReportResult {
    return abi_reports.ledger_balance_sheet(handle, book_id, as_of_date, fy_start_date);
}

pub export fn ledger_balance_sheet_auto(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    return abi_reports.ledger_balance_sheet_auto(handle, book_id, as_of_date);
}

pub export fn ledger_set_fy_start_month(handle: ?*LedgerDB, book_id: i64, month: i32, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_fy_start_month(handle, book_id, month, performed_by);
}

pub export fn ledger_set_entity_type(handle: ?*LedgerDB, book_id: i64, entity_type: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_entity_type(handle, book_id, entity_type, performed_by);
}

pub export fn ledger_translate_report(source: ?*heft.report.ReportResult, closing_rate: i64, average_rate: i64) ?*heft.report.ReportResult {
    return abi_reports.ledger_translate_report(source, closing_rate, average_rate);
}

pub export fn ledger_general_ledger(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    return abi_reports.ledger_general_ledger(handle, book_id, start_date, end_date);
}

pub export fn ledger_account_ledger(handle: ?*LedgerDB, book_id: i64, account_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    return abi_reports.ledger_account_ledger(handle, book_id, account_id, start_date, end_date);
}

pub export fn ledger_journal_register(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    return abi_reports.ledger_journal_register(handle, book_id, start_date, end_date);
}

pub export fn ledger_free_ledger_result(result: ?*heft.report.LedgerResult) void {
    return abi_reports.ledger_free_ledger_result(result);
}

pub export fn ledger_free_result(result: ?*heft.report.ReportResult) void {
    return abi_reports.ledger_free_result(result);
}

pub export fn ledger_trial_balance_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    return abi_reports.ledger_trial_balance_comparative(handle, book_id, current_date, prior_date);
}

pub export fn ledger_income_statement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    return abi_reports.ledger_income_statement_comparative(handle, book_id, cur_start, cur_end, prior_start, prior_end);
}

pub export fn ledger_balance_sheet_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8, fy_start: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    return abi_reports.ledger_balance_sheet_comparative(handle, book_id, current_date, prior_date, fy_start);
}

pub export fn ledger_trial_balance_movement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    return abi_reports.ledger_trial_balance_movement_comparative(handle, book_id, cur_start, cur_end, prior_start, prior_end);
}

pub export fn ledger_free_comparative_result(result: ?*heft.report.ComparativeReportResult) void {
    return abi_reports.ledger_free_comparative_result(result);
}

pub export fn ledger_equity_changes(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.EquityResult {
    return abi_reports.ledger_equity_changes(handle, book_id, start_date, end_date, fy_start_date);
}

pub export fn ledger_free_equity_result(result: ?*heft.report.EquityResult) void {
    return abi_reports.ledger_free_equity_result(result);
}

pub export fn ledger_result_row_count(result: ?*heft.report.ReportResult) i32 {
    return abi_reports.ledger_result_row_count(result);
}

pub export fn ledger_result_total_debits(result: ?*heft.report.ReportResult) i64 {
    return abi_reports.ledger_result_total_debits(result);
}

pub export fn ledger_result_total_credits(result: ?*heft.report.ReportResult) i64 {
    return abi_reports.ledger_result_total_credits(result);
}

pub export fn ledger_result_decimal_places(result: ?*heft.report.ReportResult) i32 {
    return abi_reports.ledger_result_decimal_places(result);
}

pub export fn ledger_comparative_result_decimal_places(result: ?*heft.report.ComparativeReportResult) i32 {
    return abi_reports.ledger_comparative_result_decimal_places(result);
}

pub export fn ledger_equity_result_decimal_places(result: ?*heft.report.EquityResult) i32 {
    return abi_reports.ledger_equity_result_decimal_places(result);
}

pub export fn ledger_create_subledger_group(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, group_type: [*:0]const u8, group_number: i32, gl_account_id: i64, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_subledger_group(handle, book_id, name, group_type, group_number, gl_account_id, performed_by);
}

pub export fn ledger_create_subledger_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, group_id: i64, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_subledger_account(handle, book_id, number, name, account_type, group_id, performed_by);
}

pub export fn ledger_create_classification(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, report_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_classification(handle, book_id, name, report_type, performed_by);
}

pub export fn ledger_add_group_node(handle: ?*LedgerDB, classification_id: i64, label: [*:0]const u8, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_add_group_node(handle, classification_id, label, parent_id, position, performed_by);
}

pub export fn ledger_add_account_node(handle: ?*LedgerDB, classification_id: i64, account_id: i64, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_add_account_node(handle, classification_id, account_id, parent_id, position, performed_by);
}

pub export fn ledger_move_node(handle: ?*LedgerDB, node_id: i64, new_parent_id: i64, new_position: i32, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_move_node(handle, node_id, new_parent_id, new_position, performed_by);
}

pub export fn ledger_classified_report(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    return abi_reports.ledger_classified_report(handle, classification_id, as_of_date);
}

pub export fn ledger_cash_flow_statement(handle: ?*LedgerDB, classification_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    return abi_reports.ledger_cash_flow_statement(handle, classification_id, start_date, end_date);
}

pub export fn ledger_free_classified_result(result: ?*heft.classification.ClassifiedResult) void {
    return abi_reports.ledger_free_classified_result(result);
}

pub export fn ledger_delete_classification(handle: ?*LedgerDB, classification_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_classification(handle, classification_id, performed_by);
}

pub export fn ledger_verify(handle: ?*LedgerDB, book_id: i64, out_errors: ?*u32, out_warnings: ?*u32) bool {
    return abi_core.ledger_verify(handle, book_id, out_errors, out_warnings);
}

pub export fn ledger_archive_book(handle: ?*LedgerDB, book_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_archive_book(handle, book_id, performed_by);
}

pub export fn ledger_close_period(handle: ?*LedgerDB, book_id: i64, period_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_close_period(handle, book_id, period_id, performed_by);
}

pub export fn ledger_revalue_forex_balances(handle: ?*LedgerDB, book_id: i64, period_id: i64, rates_json: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_revalue_forex_balances(handle, book_id, period_id, rates_json, performed_by);
}

// ── Error Reporting ────────────────────────────────────────────

/// Returns the error code from the most recent failed operation on this thread.
/// Only meaningful after a function returns -1, false, or null.
/// The error code is NOT cleared on success — check the return value first.
pub export fn ledger_last_error() i32 {
    return abi_common.ledgerLastError();
}

fn setError(code: i32) void {
    abi_common.setError(code);
}

fn invalidHandleBool() bool {
    return abi_common.invalidHandleBool();
}

fn invalidHandleI64() i64 {
    return abi_common.invalidHandleI64();
}

fn invalidHandleI32() i32 {
    return abi_common.invalidHandleI32();
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
// 32 = period not in balance, 33 = no next period, 34 = cannot reopen cascade,
// 35 = equity allocation required, 36 = equity allocation total invalid,
// 37 = suspense account not clear,
// 90-94 = sqlite errors (open, exec, prepare, step, bind),
// 99 = unknown

fn mapError(err: anyerror) i32 {
    return abi_common.mapError(err);
}

// ── Helpers ────────────────────────────────────────────────────

fn safeBuf(buf: ?[*]u8, buf_len: i32) ?[]u8 {
    return abi_common.safeBuf(buf, buf_len);
}

fn safeIntCast(val: usize) i32 {
    return abi_common.safeIntCast(val);
}

// ── CRUD Exports (Sprint 10) ──────────────────────────────────

pub export fn ledger_update_book_name(handle: ?*LedgerDB, book_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_book_name(handle, book_id, new_name, performed_by);
}

pub export fn ledger_update_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_account_name(handle, account_id, new_name, performed_by);
}

pub export fn ledger_set_account_parent(handle: ?*LedgerDB, account_id: i64, parent_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_account_parent(handle, account_id, parent_id, performed_by);
}

pub export fn ledger_set_account_monetary(handle: ?*LedgerDB, account_id: i64, is_monetary: i32, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_account_monetary(handle, account_id, is_monetary, performed_by);
}

pub export fn ledger_update_classification_name(handle: ?*LedgerDB, classification_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_classification_name(handle, classification_id, new_name, performed_by);
}

pub export fn ledger_update_node_label(handle: ?*LedgerDB, node_id: i64, new_label: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_node_label(handle, node_id, new_label, performed_by);
}

pub export fn ledger_delete_node(handle: ?*LedgerDB, node_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_node(handle, node_id, performed_by);
}

pub export fn ledger_update_subledger_group_name(handle: ?*LedgerDB, group_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_subledger_group_name(handle, group_id, new_name, performed_by);
}

pub export fn ledger_delete_subledger_group(handle: ?*LedgerDB, group_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_subledger_group(handle, group_id, performed_by);
}

pub export fn ledger_update_subledger_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_subledger_account_name(handle, account_id, new_name, performed_by);
}

pub export fn ledger_delete_subledger_account(handle: ?*LedgerDB, account_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_subledger_account(handle, account_id, performed_by);
}

pub export fn ledger_update_subledger_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_update_subledger_account_status(handle, account_id, new_status, performed_by);
}

// ── Query/List Exports (buffer-based) ─────────────────────────
// Pattern: write CSV/JSON into caller buffer, return bytes written or -1

pub export fn ledger_get_book(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_get_book(handle, book_id, buf, buf_len, format);
}

pub export fn ledger_list_books(handle: ?*LedgerDB, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_books(handle, status_filter, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_get_account(handle: ?*LedgerDB, account_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_get_account(handle, account_id, buf, buf_len, format);
}

pub export fn ledger_list_accounts(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, status_filter: ?[*:0]const u8, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_accounts(handle, book_id, type_filter, status_filter, name_search, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_get_period(handle: ?*LedgerDB, period_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_get_period(handle, period_id, buf, buf_len, format);
}

pub export fn ledger_list_periods(handle: ?*LedgerDB, book_id: i64, year_filter: i32, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_periods(handle, book_id, year_filter, status_filter, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_get_entry(handle: ?*LedgerDB, entry_id: i64, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_get_entry(handle, entry_id, book_id, buf, buf_len, format);
}

pub export fn ledger_list_entries(handle: ?*LedgerDB, book_id: i64, status_filter: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, doc_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_entries(handle, book_id, status_filter, start_date, end_date, doc_search, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_list_entry_lines(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_entry_lines(handle, entry_id, buf, buf_len, format);
}

pub export fn ledger_list_classifications(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_classifications(handle, book_id, type_filter, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_list_subledger_groups(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_subledger_groups(handle, book_id, type_filter, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_list_subledger_accounts(handle: ?*LedgerDB, book_id: i64, group_filter: i64, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_subledger_accounts(handle, book_id, group_filter, name_search, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_list_audit_log(handle: ?*LedgerDB, book_id: i64, entity_type: ?[*:0]const u8, action: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_audit_log(handle, book_id, entity_type, action, start_date, end_date, sort_order, limit, offset, buf, buf_len, format);
}

// ── Subledger Report Exports ──────────────────────────────────

pub export fn ledger_subledger_report(handle: ?*LedgerDB, book_id: i64, group_id: i64, name_search: ?[*:0]const u8, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_subledger_report(handle, book_id, group_id, name_search, start_date, end_date, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_counterparty_ledger(handle: ?*LedgerDB, book_id: i64, counterparty_id: i64, account_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_counterparty_ledger(handle, book_id, counterparty_id, account_filter, start_date, end_date, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_list_transactions(handle: ?*LedgerDB, book_id: i64, account_filter: i64, counterparty_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_transactions(handle, book_id, account_filter, counterparty_filter, start_date, end_date, sort_order, limit, offset, buf, buf_len, format);
}

pub export fn ledger_subledger_reconciliation(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_subledger_reconciliation(handle, book_id, group_id, as_of_date, buf, buf_len, format);
}

pub export fn ledger_aged_subledger(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_aged_subledger(handle, book_id, group_id, as_of_date, sort_order, limit, offset, buf, buf_len, format);
}

// ── Fix: ledger_edit_line with description + counterparty ─────

pub export fn ledger_edit_line_full(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_edit_line_full(handle, line_id, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, counterparty_id, description, performed_by);
}

// ── Sprint 17: Dimension Exports ──────────────────────────────

pub export fn ledger_create_dimension(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, dimension_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_buffers.ledger_create_dimension(handle, book_id, name, dimension_type, performed_by);
}

pub export fn ledger_delete_dimension(handle: ?*LedgerDB, dimension_id: i64, performed_by: [*:0]const u8) bool {
    return abi_buffers.ledger_delete_dimension(handle, dimension_id, performed_by);
}

pub export fn ledger_create_dimension_value(handle: ?*LedgerDB, dimension_id: i64, code: [*:0]const u8, label: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    return abi_buffers.ledger_create_dimension_value(handle, dimension_id, code, label, performed_by);
}

pub export fn ledger_delete_dimension_value(handle: ?*LedgerDB, value_id: i64, performed_by: [*:0]const u8) bool {
    return abi_buffers.ledger_delete_dimension_value(handle, value_id, performed_by);
}

pub export fn ledger_assign_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    return abi_buffers.ledger_assign_line_dimension(handle, line_id, dimension_value_id, performed_by);
}

pub export fn ledger_remove_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    return abi_buffers.ledger_remove_line_dimension(handle, line_id, dimension_value_id, performed_by);
}

pub export fn ledger_dimension_summary(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_dimension_summary(handle, book_id, dimension_id, start_date, end_date, buf, buf_len, format);
}

// ── Sprint 20D: Dimension List Exports ───────────────────────

pub export fn ledger_list_dimensions(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_dimensions(handle, book_id, type_filter, buf, buf_len, format);
}

pub export fn ledger_list_dimension_values(handle: ?*LedgerDB, dimension_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_dimension_values(handle, dimension_id, buf, buf_len, format);
}

// ── Sprint 20C: Schema Self-Description ──────────────────────

pub export fn ledger_describe_schema(handle: ?*LedgerDB, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_describe_schema(handle, buf, buf_len, format);
}

// ── Sprint 20B: Approval Workflow ─────────────────────────────

pub export fn ledger_approve_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_approve_entry(handle, entry_id, performed_by);
}

pub export fn ledger_reject_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_reject_entry(handle, entry_id, reason, performed_by);
}

pub export fn ledger_set_require_approval(handle: ?*LedgerDB, book_id: i64, require: i32, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_set_require_approval(handle, book_id, require, performed_by);
}

// ── Sprint 18B: Budget C ABI ──────────────────────────────────

pub export fn ledger_create_budget(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, fiscal_year: i32, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_budget(handle, book_id, name, fiscal_year, performed_by);
}

pub export fn ledger_delete_budget(handle: ?*LedgerDB, budget_id: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_delete_budget(handle, budget_id, performed_by);
}

pub export fn ledger_set_budget_line(handle: ?*LedgerDB, budget_id: i64, account_id: i64, period_id: i64, amount: i64, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_set_budget_line(handle, budget_id, account_id, period_id, amount, performed_by);
}

pub export fn ledger_budget_vs_actual(handle: ?*LedgerDB, budget_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_budget_vs_actual(handle, budget_id, start_date, end_date, buf, buf_len, format);
}

// ── Sprint 19: Batch Operations ─────────────────────────────────

pub export fn ledger_batch_post(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    return abi_core.ledger_batch_post(handle, entry_ids_json, performed_by, out_succeeded, out_failed);
}

pub export fn ledger_batch_void(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, reason: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    return abi_core.ledger_batch_void(handle, entry_ids_json, reason, performed_by, out_succeeded, out_failed);
}

// ── Sprint 13A: Cache Recalculation ────────────────────────────

pub export fn ledger_recalculate_balances(handle: ?*LedgerDB, book_id: i64) i32 {
    return abi_core.ledger_recalculate_balances(handle, book_id);
}

// ── Export Wrappers ────────────────────────────────────────────

pub export fn ledger_export_chart_of_accounts(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_chart_of_accounts(handle, book_id, buf, buf_len, format);
}

pub export fn ledger_export_journal_entries(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_journal_entries(handle, book_id, start_date, end_date, buf, buf_len, format);
}

pub export fn ledger_export_audit_trail(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_audit_trail(handle, book_id, start_date, end_date, buf, buf_len, format);
}

pub export fn ledger_export_periods(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_periods(handle, book_id, buf, buf_len, format);
}

pub export fn ledger_export_subledger(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_subledger(handle, book_id, buf, buf_len, format);
}

pub export fn ledger_export_book_metadata(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_export_book_metadata(handle, book_id, buf, buf_len, format);
}

pub export fn ledger_oble_export_book(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_book(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_core_bundle(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_core_bundle(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_book_snapshot(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_book_snapshot(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_accounts(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_accounts(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_periods(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_periods(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_counterparties(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_counterparties(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_counterparty_profile_bundle(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_counterparty_profile_bundle(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_policy_profile(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_policy_profile(handle, book_id, buf, buf_len);
}

pub export fn ledger_oble_export_close_profile(handle: ?*LedgerDB, book_id: i64, period_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_close_profile(handle, book_id, period_id, buf, buf_len);
}

pub export fn ledger_oble_export_policy_lifecycle_bundle(handle: ?*LedgerDB, book_id: i64, period_id: i64, revaluation_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_policy_lifecycle_bundle(handle, book_id, period_id, revaluation_entry_id, buf, buf_len);
}

pub export fn ledger_oble_export_entry(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_entry(handle, entry_id, buf, buf_len);
}

pub export fn ledger_oble_export_reversal_pair(handle: ?*LedgerDB, original_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_reversal_pair(handle, original_entry_id, buf, buf_len);
}

pub export fn ledger_oble_export_counterparty_open_item(handle: ?*LedgerDB, open_item_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_counterparty_open_item(handle, open_item_id, buf, buf_len);
}

pub export fn ledger_oble_export_revaluation_packet(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_revaluation_packet(handle, entry_id, buf, buf_len);
}

pub export fn ledger_oble_export_fx_profile_bundle(handle: ?*LedgerDB, entry_id: i64, revaluation_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    return abi_buffers.ledger_oble_export_fx_profile_bundle(handle, entry_id, revaluation_entry_id, buf, buf_len);
}

pub export fn ledger_transition_budget(handle: ?*LedgerDB, budget_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_transition_budget(handle, budget_id, target_status, performed_by);
}

// ── Open Item Wrappers ─────────────────────────────────────────

pub export fn ledger_create_open_item(handle: ?*LedgerDB, entry_line_id: i64, counterparty_id: i64, original_amount: i64, due_date: ?[*:0]const u8, book_id: i64, performed_by: [*:0]const u8) i64 {
    return abi_core.ledger_create_open_item(handle, entry_line_id, counterparty_id, original_amount, due_date, book_id, performed_by);
}

pub export fn ledger_allocate_payment(handle: ?*LedgerDB, open_item_id: i64, amount: i64, performed_by: [*:0]const u8) bool {
    return abi_core.ledger_allocate_payment(handle, open_item_id, amount, performed_by);
}

pub export fn ledger_list_open_items(handle: ?*LedgerDB, counterparty_id: i64, include_closed: bool, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_list_open_items(handle, counterparty_id, include_closed, buf, buf_len, format);
}

pub export fn ledger_cash_flow_indirect(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, classification_id: i64) ?*heft.classification.CashFlowIndirectResult {
    return abi_reports.ledger_cash_flow_indirect(handle, book_id, start_date, end_date, classification_id);
}

pub export fn ledger_free_cash_flow_indirect(result: ?*heft.classification.CashFlowIndirectResult) void {
    return abi_reports.ledger_free_cash_flow_indirect(result);
}

pub export fn ledger_classified_trial_balance(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    return abi_reports.ledger_classified_trial_balance(handle, classification_id, as_of_date);
}

pub export fn ledger_dimension_summary_rollup(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    return abi_buffers.ledger_dimension_summary_rollup(handle, book_id, dimension_id, start_date, end_date, buf, buf_len, format);
}
