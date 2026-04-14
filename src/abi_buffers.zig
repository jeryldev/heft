const std = @import("std");
const heft = @import("heft");
const common = @import("abi_common.zig");

const LedgerDB = common.LedgerDB;

fn exportFormat(format: i32) ?heft.export_mod.ExportFormat {
    return if (format == 0) .csv else if (format == 1) .json else null;
}

fn queryOrder(sort_order: i32) ?heft.query_mod.SortOrder {
    return if (sort_order == 0) .asc else if (sort_order == 1) .desc else null;
}

pub fn ledger_get_book(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getBook(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_books(handle: ?*LedgerDB, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listBooks(h.sqlite, sf, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_get_account(handle: ?*LedgerDB, account_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getAccount(h.sqlite, account_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_accounts(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, status_filter: ?[*:0]const u8, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listAccounts(h.sqlite, book_id, tf, sf, ns, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_get_period(handle: ?*LedgerDB, period_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getPeriod(h.sqlite, period_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_periods(handle: ?*LedgerDB, book_id: i64, year_filter: i32, status_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const yf: ?i32 = if (year_filter > 0) year_filter else null;
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listPeriods(h.sqlite, book_id, yf, sf, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_get_entry(handle: ?*LedgerDB, entry_id: i64, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.getEntry(h.sqlite, entry_id, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_entries(handle: ?*LedgerDB, book_id: i64, status_filter: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, doc_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const sf: ?[]const u8 = if (status_filter) |s| std.mem.span(s) else null;
    const sd: ?[]const u8 = if (start_date) |s| std.mem.span(s) else null;
    const ed: ?[]const u8 = if (end_date) |s| std.mem.span(s) else null;
    const ds: ?[]const u8 = if (doc_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listEntries(h.sqlite, book_id, sf, sd, ed, ds, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_entry_lines(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.listEntryLines(h.sqlite, entry_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_classifications(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listClassifications(h.sqlite, book_id, tf, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_subledger_groups(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listSubledgerGroups(h.sqlite, book_id, tf, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_subledger_accounts(handle: ?*LedgerDB, book_id: i64, group_filter: i64, name_search: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_filter > 0) group_filter else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listSubledgerAccounts(h.sqlite, book_id, gf, ns, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_audit_log(handle: ?*LedgerDB, book_id: i64, entity_type: ?[*:0]const u8, action: ?[*:0]const u8, start_date: ?[*:0]const u8, end_date: ?[*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const et: ?[]const u8 = if (entity_type) |s| std.mem.span(s) else null;
    const af: ?[]const u8 = if (action) |s| std.mem.span(s) else null;
    const sd: ?[]const u8 = if (start_date) |s| std.mem.span(s) else null;
    const ed: ?[]const u8 = if (end_date) |s| std.mem.span(s) else null;
    const result = heft.query_mod.listAuditLog(h.sqlite, book_id, et, af, sd, ed, order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_subledger_report(handle: ?*LedgerDB, book_id: i64, group_id: i64, name_search: ?[*:0]const u8, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_id > 0) group_id else null;
    const ns: ?[]const u8 = if (name_search) |s| std.mem.span(s) else null;
    const result = heft.query_mod.subledgerReport(h.sqlite, book_id, gf, ns, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_counterparty_ledger(handle: ?*LedgerDB, book_id: i64, counterparty_id: i64, account_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const af: ?i64 = if (account_filter > 0) account_filter else null;
    const result = heft.query_mod.counterpartyLedger(h.sqlite, book_id, counterparty_id, af, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_transactions(handle: ?*LedgerDB, book_id: i64, account_filter: i64, counterparty_filter: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const af: ?i64 = if (account_filter > 0) account_filter else null;
    const cf: ?i64 = if (counterparty_filter > 0) counterparty_filter else null;
    const result = heft.query_mod.listTransactions(h.sqlite, book_id, af, cf, std.mem.span(start_date), std.mem.span(end_date), order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_subledger_reconciliation(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.query_mod.subledgerReconciliation(h.sqlite, book_id, group_id, std.mem.span(as_of_date), common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_aged_subledger(handle: ?*LedgerDB, book_id: i64, group_id: i64, as_of_date: [*:0]const u8, sort_order: i32, limit: i32, offset: i32, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const order = queryOrder(sort_order) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const gf: ?i64 = if (group_id > 0) group_id else null;
    const result = heft.query_mod.agedSubledger(h.sqlite, book_id, gf, std.mem.span(as_of_date), order, limit, offset, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_create_dimension(handle: ?*LedgerDB, book_id: i64, name: [*:0]const u8, dimension_type: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    const dt = heft.dimension.DimensionType.fromString(std.mem.span(dimension_type)) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return heft.dimension.Dimension.create(h.sqlite, book_id, std.mem.span(name), dt, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_delete_dimension(handle: ?*LedgerDB, dimension_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.dimension.Dimension.delete(h.sqlite, dimension_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_create_dimension_value(handle: ?*LedgerDB, dimension_id: i64, code: [*:0]const u8, label: [*:0]const u8, performed_by: [*:0]const u8) i64 {
    const h = handle orelse return common.invalidHandleI64();
    return heft.dimension.DimensionValue.create(h.sqlite, dimension_id, std.mem.span(code), std.mem.span(label), std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_delete_dimension_value(handle: ?*LedgerDB, value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.dimension.DimensionValue.delete(h.sqlite, value_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_assign_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.dimension.LineDimension.assign(h.sqlite, line_id, dimension_value_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_remove_line_dimension(handle: ?*LedgerDB, line_id: i64, dimension_value_id: i64, performed_by: [*:0]const u8) bool {
    const h = handle orelse return common.invalidHandleBool();
    heft.dimension.LineDimension.remove(h.sqlite, line_id, dimension_value_id, std.mem.span(performed_by)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_dimension_summary(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const b = common.safeBuf(buf, buf_len) orelse return -1;
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.dimensionSummary(h.sqlite, book_id, dimension_id, std.mem.span(start_date), std.mem.span(end_date), b, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_dimensions(handle: ?*LedgerDB, book_id: i64, type_filter: ?[*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const tf: ?[]const u8 = if (type_filter) |s| std.mem.span(s) else null;
    const result = heft.dimension.listDimensions(h.sqlite, book_id, tf, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_dimension_values(handle: ?*LedgerDB, dimension_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.listDimensionValues(h.sqlite, dimension_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_describe_schema(handle: ?*LedgerDB, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.describe.describeSchema(h.sqlite, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_budget_vs_actual(handle: ?*LedgerDB, budget_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const b = common.safeBuf(buf, buf_len) orelse return -1;
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.budget.budgetVsActual(h.sqlite, budget_id, std.mem.span(start_date), std.mem.span(end_date), b, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_chart_of_accounts(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportChartOfAccounts(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_journal_entries(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportJournalEntries(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_audit_trail(handle: ?*LedgerDB, book_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportAuditTrail(h.sqlite, book_id, std.mem.span(start_date), std.mem.span(end_date), common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_periods(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportPeriods(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_subledger(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportSubledger(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_export_book_metadata(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.export_mod.exportBookMetadata(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_book(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_core.exportBookJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_core_bundle(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_core.exportCoreBundleJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_book_snapshot(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_counterparty.exportBookSnapshotJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_accounts(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_core.exportAccountsJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_periods(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_core.exportPeriodsJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_counterparties(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_counterparty.exportCounterpartiesJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_policy_profile(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_policy.exportPolicyProfileJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_close_profile(handle: ?*LedgerDB, book_id: i64, period_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_policy.exportCloseReopenProfileJson(h.sqlite, book_id, period_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_entry(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_core.exportEntryJson(h.sqlite, entry_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_reversal_pair(handle: ?*LedgerDB, original_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_export.exportReversalPairJson(h.sqlite, original_entry_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_counterparty_open_item(handle: ?*LedgerDB, open_item_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_counterparty.exportCounterpartyOpenItemJson(h.sqlite, open_item_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_revaluation_packet(handle: ?*LedgerDB, entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_fx.exportRevaluationPacketJson(h.sqlite, entry_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_fx_profile_bundle(handle: ?*LedgerDB, entry_id: i64, revaluation_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const maybe_revaluation_entry_id = if (revaluation_entry_id > 0) revaluation_entry_id else null;
    const result = heft.oble_profile_fx.exportMultiCurrencyBundleJson(h.sqlite, entry_id, maybe_revaluation_entry_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_counterparty_profile_bundle(handle: ?*LedgerDB, book_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const result = heft.oble_profile_counterparty.exportCounterpartyProfileBundleJson(h.sqlite, book_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_oble_export_policy_lifecycle_bundle(handle: ?*LedgerDB, book_id: i64, period_id: i64, revaluation_entry_id: i64, buf: ?[*]u8, buf_len: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const maybe_revaluation_entry_id = if (revaluation_entry_id > 0) revaluation_entry_id else null;
    const result = heft.oble_profile_policy.exportPolicyLifecycleBundleJson(h.sqlite, book_id, period_id, maybe_revaluation_entry_id, common.safeBuf(buf, buf_len) orelse return -1) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_list_open_items(handle: ?*LedgerDB, counterparty_id: i64, include_closed: bool, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.open_item.listOpenItems(h.sqlite, counterparty_id, include_closed, common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}

pub fn ledger_dimension_summary_rollup(handle: ?*LedgerDB, book_id: i64, dimension_id: i64, start_date: [*:0]const u8, end_date: [*:0]const u8, buf: ?[*]u8, buf_len: i32, format: i32) i32 {
    const h = handle orelse return common.invalidHandleI32();
    const fmt = exportFormat(format) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const result = heft.dimension.dimensionSummaryRollup(h.sqlite, book_id, dimension_id, std.mem.span(start_date), std.mem.span(end_date), common.safeBuf(buf, buf_len) orelse return -1, fmt) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
    return common.safeIntCast(result.len);
}
