const std = @import("std");
const heft = @import("heft");
const common = @import("abi_common.zig");

const LedgerDB = common.LedgerDB;

pub fn ledger_set_busy_timeout(handle: ?*LedgerDB, timeout_ms: i32) bool {
    const h = handle orelse return common.invalidHandleBool();
    h.sqlite.setBusyTimeout(timeout_ms) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_book(handle: ?*LedgerDB, name: [*:0]const u8, base_currency: [*:0]const u8, decimal_places: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.book.Book.create(h.sqlite, std.mem.span(name), std.mem.span(base_currency), decimal_places, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_create_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, is_contra: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const at = heft.account.AccountType.fromString(std.mem.span(account_type)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return heft.account.Account.create(h.sqlite, book_id, std.mem.span(number), std.mem.span(name), at, is_contra != 0, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_create_period(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, period_number: i32, year: i32, start_date: [*:0]const u8, end_date: [*:0]const u8, period_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.period.Period.create(h.sqlite, book_id, std.mem.span(name), period_number, year, std.mem.span(start_date), std.mem.span(end_date), std.mem.span(period_type), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_update_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const status = heft.account.AccountStatus.fromString(std.mem.span(new_status)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.account.Account.updateStatus(h.sqlite, account_id, status, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_transition_period(handle: ?*LedgerDB, period_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const ts = heft.period.PeriodStatus.fromString(std.mem.span(target_status)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.period.Period.transition(h.sqlite, period_id, ts, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_rounding_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setRoundingAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_fx_gain_loss_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setFxGainLossAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_retained_earnings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setRetainedEarningsAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_equity_close_target(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    return ledger_set_retained_earnings_account(handle, book_id, account_id, performed_by);
}

pub fn ledger_set_dividends_drawings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setDividendsDrawingsAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_current_year_earnings_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setCurrentYearEarningsAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_income_summary_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setIncomeSummaryAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_opening_balance_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setOpeningBalanceAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_suspense_account(handle: ?*LedgerDB, book_id: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setSuspenseAccount(h.sqlite, book_id, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_validate_opening_balance(handle: ?*LedgerDB, book_id: i64) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.validateOpeningBalanceMigration(h.sqlite, book_id) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_bulk_create_periods(handle: ?*LedgerDB, book_id: i64, fiscal_year: i32, start_month: i32, granularity: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const gran = heft.period.PeriodGranularity.fromString(std.mem.span(granularity)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.period.Period.bulkCreate(h.sqlite, book_id, fiscal_year, start_month, gran, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_draft(handle: ?*LedgerDB, book_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, period_id: i64, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    return heft.entry.Entry.createDraft(h.sqlite, book_id, std.mem.span(document_number), std.mem.span(transaction_date), std.mem.span(posting_date), desc, period_id, meta, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_add_line(handle: ?*LedgerDB, entry_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    if (counterparty_id < 0) {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    }
    const cp: ?i64 = if (counterparty_id > 0) counterparty_id else null;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    return heft.entry.Entry.addLine(h.sqlite, entry_id, line_number, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, cp, desc, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_edit_draft(handle: ?*LedgerDB, entry_id: i64, document_number: [*:0]const u8, transaction_date: [*:0]const u8, posting_date: [*:0]const u8, description: ?[*:0]const u8, metadata: ?[*:0]const u8, period_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    heft.entry.Entry.editDraft(h.sqlite, entry_id, std.mem.span(document_number), std.mem.span(transaction_date), std.mem.span(posting_date), desc, meta, period_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_edit_posted(handle: ?*LedgerDB, entry_id: i64, description: ?[*:0]const u8, metadata: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    const meta: ?[]const u8 = if (metadata) |m| std.mem.span(m) else null;
    heft.entry.Entry.editPosted(h.sqlite, entry_id, desc, meta, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_post_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.post(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_void_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.voidEntry(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_reverse_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, reversal_date: [*:0]const u8, target_period_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const tp: ?i64 = if (target_period_id > 0) target_period_id else null;
    return heft.entry.Entry.reverse(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(reversal_date), tp, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_remove_line(handle: ?*LedgerDB, line_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.removeLine(h.sqlite, line_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_delete_draft(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.deleteDraft(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_edit_line(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.editLine(h.sqlite, line_id, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, null, null, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_fy_start_month(handle: ?*LedgerDB, book_id: i64, month: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setFyStartMonth(h.sqlite, book_id, month, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_entity_type(handle: ?*LedgerDB, book_id: i64, entity_type: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const et = heft.book.EntityType.fromString(std.mem.span(entity_type)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.book.Book.setEntityType(h.sqlite, book_id, et, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_subledger_group(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, group_type: [*:0]const u8, group_number: i32, gl_account_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.subledger.SubledgerGroup.create(h.sqlite, book_id, std.mem.span(name), std.mem.span(group_type), group_number, gl_account_id, null, null, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_create_subledger_account(handle: ?*LedgerDB, book_id: i64, number: [*:0]const u8, name: [*:0]const u8, account_type: [*:0]const u8, group_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.subledger.SubledgerAccount.create(h.sqlite, book_id, std.mem.span(number), std.mem.span(name), std.mem.span(account_type), group_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_create_classification(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, report_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.classification.Classification.create(h.sqlite, book_id, std.mem.span(name), std.mem.span(report_type), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_add_group_node(handle: ?*LedgerDB, classification_id: i64, label: [*:0]const u8, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const pid: ?i64 = if (parent_id == 0) null else parent_id;
    return heft.classification.ClassificationNode.addGroup(h.sqlite, classification_id, std.mem.span(label), pid, position, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_add_account_node(handle: ?*LedgerDB, classification_id: i64, account_id: i64, parent_id: i64, position: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const pid: ?i64 = if (parent_id == 0) null else parent_id;
    return heft.classification.ClassificationNode.addAccount(h.sqlite, classification_id, account_id, pid, position, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_move_node(handle: ?*LedgerDB, node_id: i64, new_parent_id: i64, new_position: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const pid: ?i64 = if (new_parent_id == 0) null else new_parent_id;
    heft.classification.ClassificationNode.move(h.sqlite, node_id, pid, new_position, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_delete_classification(handle: ?*LedgerDB, classification_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.classification.Classification.delete(h.sqlite, classification_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_verify(handle: ?*LedgerDB, book_id: i64, out_errors: ?*u32, out_warnings: ?*u32) bool {
    const h = handle orelse return common.invalidHandleBool();
    const err_ptr = out_errors orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    const warn_ptr = out_warnings orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    const result = heft.verify_mod.verify(h.sqlite, book_id) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    err_ptr.* = result.errors;
    warn_ptr.* = result.warnings;
    return result.passed();
}

pub fn ledger_archive_book(handle: ?*LedgerDB, book_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.archive(h.sqlite, book_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_close_period(handle: ?*LedgerDB, book_id: i64, period_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.close.closePeriod(h.sqlite, book_id, period_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_revalue_forex_balances(handle: ?*LedgerDB, book_id: i64, period_id: i64, rates_json: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const json = std.mem.span(rates_json);

    var rates_buf: [50]heft.revaluation.CurrencyRate = undefined;
    const rate_count = heft.revaluation.parseRatesJson(json, &rates_buf) catch {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };

    if (rate_count == 0) {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    }

    const result = heft.revaluation.revalueForexBalances(h.sqlite, book_id, period_id, rates_buf[0..rate_count], std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return result.entry_id;
}

pub fn ledger_update_book_name(handle: ?*LedgerDB, book_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.updateName(h.sqlite, book_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.account.Account.updateName(h.sqlite, account_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_account_parent(handle: ?*LedgerDB, account_id: i64, parent_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const pid: ?i64 = if (parent_id > 0) parent_id else null;
    heft.account.Account.setParent(h.sqlite, account_id, pid, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_account_monetary(handle: ?*LedgerDB, account_id: i64, is_monetary: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.account.Account.setMonetary(h.sqlite, account_id, is_monetary != 0, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_classification_name(handle: ?*LedgerDB, classification_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.classification.Classification.updateName(h.sqlite, classification_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_node_label(handle: ?*LedgerDB, node_id: i64, new_label: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.classification.ClassificationNode.updateLabel(h.sqlite, node_id, std.mem.span(new_label), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_delete_node(handle: ?*LedgerDB, node_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.classification.ClassificationNode.delete(h.sqlite, node_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_subledger_group_name(handle: ?*LedgerDB, group_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.subledger.SubledgerGroup.updateName(h.sqlite, group_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_delete_subledger_group(handle: ?*LedgerDB, group_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.subledger.SubledgerGroup.delete(h.sqlite, group_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_subledger_account_name(handle: ?*LedgerDB, account_id: i64, new_name: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.subledger.SubledgerAccount.updateName(h.sqlite, account_id, std.mem.span(new_name), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_delete_subledger_account(handle: ?*LedgerDB, account_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.subledger.SubledgerAccount.delete(h.sqlite, account_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_update_subledger_account_status(handle: ?*LedgerDB, account_id: i64, new_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const status = heft.subledger.SubledgerAccountStatus.fromString(std.mem.span(new_status)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.subledger.SubledgerAccount.updateStatus(h.sqlite, account_id, status, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_edit_line_full(handle: ?*LedgerDB, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: [*:0]const u8, fx_rate: i64, account_id: i64, counterparty_id: i64, description: ?[*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    if (counterparty_id < 0) {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    }
    const cp: ?i64 = if (counterparty_id > 0) counterparty_id else null;
    const desc: ?[]const u8 = if (description) |d| std.mem.span(d) else null;
    heft.entry.Entry.editLine(h.sqlite, line_id, debit_amount, credit_amount, std.mem.span(transaction_currency), fx_rate, account_id, cp, desc, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_approve_entry(handle: ?*LedgerDB, entry_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.approve(h.sqlite, entry_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_reject_entry(handle: ?*LedgerDB, entry_id: i64, reason: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.entry.Entry.reject(h.sqlite, entry_id, std.mem.span(reason), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_require_approval(handle: ?*LedgerDB, book_id: i64, require: i32, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.book.Book.setRequireApproval(h.sqlite, book_id, require != 0, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_budget(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, fiscal_year: i32, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.budget.Budget.create(h.sqlite, book_id, std.mem.span(name), fiscal_year, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_delete_budget(handle: ?*LedgerDB, budget_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.budget.Budget.delete(h.sqlite, budget_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_set_budget_line(handle: ?*LedgerDB, budget_id: i64, account_id: i64, period_id: i64, amount: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.budget.BudgetLine.set(h.sqlite, budget_id, account_id, period_id, amount, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_batch_post(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    const h = handle orelse return common.invalidHandleBool();
    const json = std.mem.span(entry_ids_json);
    var ids_buf: [1000]i64 = undefined;
    const count = heft.batch.parseIdArray(json, &ids_buf) catch {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    if (count == 0) {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    }
    const result = heft.batch.batchPost(h.sqlite, ids_buf[0..count], std.mem.span(performed_by));
    if (out_succeeded) |p| p.* = result.succeeded;
    if (out_failed) |p| p.* = result.failed;
    return result.failed == 0;
}

pub fn ledger_batch_void(handle: ?*LedgerDB, entry_ids_json: [*:0]const u8, reason: [*:0]const u8, performed_by: [*:0]const u8, out_succeeded: ?*u32, out_failed: ?*u32) bool {
    const h = handle orelse return common.invalidHandleBool();
    const json = std.mem.span(entry_ids_json);
    var ids_buf: [1000]i64 = undefined;
    const count = heft.batch.parseIdArray(json, &ids_buf) catch {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    if (count == 0) {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    }
    const result = heft.batch.batchVoid(h.sqlite, ids_buf[0..count], std.mem.span(reason), std.mem.span(performed_by));
    if (out_succeeded) |p| p.* = result.succeeded;
    if (out_failed) |p| p.* = result.failed;
    return result.failed == 0;
}

pub fn ledger_recalculate_balances(handle: ?*LedgerDB, book_id: i64) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const count = heft.cache.recalculateAllStale(h.sqlite, book_id) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(count);
}

pub fn ledger_transition_budget(handle: ?*LedgerDB, budget_id: i64, target_status: [*:0]const u8, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    const target = heft.budget.BudgetStatus.fromString(std.mem.span(target_status)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    heft.budget.Budget.transition(h.sqlite, budget_id, target, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_open_item(handle: ?*LedgerDB, entry_line_id: i64, counterparty_id: i64, original_amount: i64, due_date: ?[*:0]const u8, book_id: i64, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const dd: ?[]const u8 = if (due_date) |d| std.mem.span(d) else null;
    return heft.open_item.createOpenItem(h.sqlite, entry_line_id, counterparty_id, original_amount, dd, book_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_allocate_payment(handle: ?*LedgerDB, open_item_id: i64, amount: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.open_item.allocatePayment(h.sqlite, open_item_id, amount, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}
