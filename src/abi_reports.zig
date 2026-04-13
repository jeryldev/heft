const std = @import("std");
const heft = @import("heft");
const common = @import("abi_common.zig");

const LedgerDB = common.LedgerDB;

pub fn ledger_trial_balance(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.trialBalance(h.sqlite, book_id, std.mem.span(as_of_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_income_statement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.incomeStatement(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_trial_balance_movement(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.trialBalanceMovement(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_balance_sheet(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.balanceSheetWithProjectedRE(h.sqlite, book_id, std.mem.span(as_of_date), std.mem.span(fy_start_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_balance_sheet_auto(handle: ?*LedgerDB, book_id: i64, as_of_date: [*:0]const u8) ?*heft.report.ReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.balanceSheetAuto(h.sqlite, book_id, std.mem.span(as_of_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_translate_report(source: ?*heft.report.ReportResult, closing_rate: i64, average_rate: i64) ?*heft.report.ReportResult {
    const src = source orelse return null;
    const rates = heft.report.TranslationRates{
        .closing_rate = closing_rate,
        .average_rate = average_rate,
    };
    return heft.report.translateReportResult(src, rates) catch return null;
}

pub fn ledger_general_ledger(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.generalLedger(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_account_ledger(handle: ?*LedgerDB, book_id: i64, account_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.accountLedger(h.sqlite, book_id, account_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_journal_register(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.report.LedgerResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.journalRegister(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_free_ledger_result(result: ?*heft.report.LedgerResult) void {
    const r = result orelse return;
    r.deinit();
}

pub fn ledger_free_result(result: ?*heft.report.ReportResult) void {
    const r = result orelse return;
    r.deinit();
}

pub fn ledger_trial_balance_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.trialBalanceComparative(h.sqlite, book_id, std.mem.span(current_date), std.mem.span(prior_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_income_statement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.incomeStatementComparative(h.sqlite, book_id, std.mem.span(cur_start), std.mem.span(cur_end), std.mem.span(prior_start), std.mem.span(prior_end)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_balance_sheet_comparative(handle: ?*LedgerDB, book_id: i64, current_date: [*:0]const u8, prior_date: [*:0]const u8, fy_start: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.balanceSheetComparative(h.sqlite, book_id, std.mem.span(current_date), std.mem.span(prior_date), std.mem.span(fy_start)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_trial_balance_movement_comparative(handle: ?*LedgerDB, book_id: i64, cur_start: [*:0]const u8, cur_end: [*:0]const u8, prior_start: [*:0]const u8, prior_end: [*:0]const u8) ?*heft.report.ComparativeReportResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.trialBalanceMovementComparative(h.sqlite, book_id, std.mem.span(cur_start), std.mem.span(cur_end), std.mem.span(prior_start), std.mem.span(prior_end)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_free_comparative_result(result: ?*heft.report.ComparativeReportResult) void {
    const r = result orelse return;
    r.deinit();
}

pub fn ledger_equity_changes(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, fy_start_date: [*:0]const u8) ?*heft.report.EquityResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.report.equityChanges(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), std.mem.span(fy_start_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_free_equity_result(result: ?*heft.report.EquityResult) void {
    const r = result orelse return;
    r.deinit();
}

pub fn ledger_result_row_count(result: ?*heft.report.ReportResult) i32 {
    const r = result orelse return 0;
    return common.safeIntCast(r.rows.len);
}

pub fn ledger_result_total_debits(result: ?*heft.report.ReportResult) i64 {
    const r = result orelse return 0;
    return r.total_debits;
}

pub fn ledger_result_total_credits(result: ?*heft.report.ReportResult) i64 {
    const r = result orelse return 0;
    return r.total_credits;
}

pub fn ledger_classified_report(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.classification.classifiedReport(h.sqlite, classification_id, std.mem.span(as_of_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_cash_flow_statement(handle: ?*LedgerDB, classification_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.classification.cashFlowStatement(h.sqlite, classification_id, std.mem.span(start_date), std.mem.span(end_date)) catch |err| {
        common.setError(common.mapError(err));
        return null;
    };
}

pub fn ledger_free_classified_result(result: ?*heft.classification.ClassifiedResult) void {
    const r = result orelse return;
    r.deinit();
}

pub fn ledger_cash_flow_indirect(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, classification_id: i64) ?*heft.classification.CashFlowIndirectResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.classification.cashFlowStatementIndirect(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), classification_id) catch null;
}

pub fn ledger_free_cash_flow_indirect(result: ?*heft.classification.CashFlowIndirectResult) void {
    if (result) |r| r.deinit();
}

pub fn ledger_classified_trial_balance(handle: ?*LedgerDB, classification_id: i64, as_of_date: [*:0]const u8) ?*heft.classification.ClassifiedResult {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    return heft.classification.classifiedTrialBalance(h.sqlite, classification_id, std.mem.span(as_of_date)) catch null;
}
