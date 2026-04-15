const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const classification_mod = @import("classification.zig");
const subledger_mod = @import("subledger.zig");
const close_mod = @import("close.zig");
const revaluation_mod = @import("revaluation.zig");
const money = @import("money.zig");
const oble_export = @import("oble_export.zig");
const oble_import = @import("oble_import.zig");
const oble_results = @import("oble_profile_results.zig");

test "CONFORMANCE: OBLE Core profile" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Core Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(source_db, book_id, "3000", "Capital", .equity, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "JE-001", "2026-01-10", "2026-01-10", "Capital injection", period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 1_000_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 1_000_00_000_000, "PHP", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    var snapshot_buf: [32768]u8 = undefined;
    const snapshot_json = try oble_export.exportBookSnapshotJson(source_db, book_id, &snapshot_buf);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_json, "\"book\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_json, "\"accounts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_json, "\"periods\"") != null);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = oble_import.ImportContext.init(std.testing.allocator);
    defer ctx.deinit();
    const imported_book_id = try oble_import.importBookSnapshotJson(target_db, &ctx, snapshot_json, "admin");
    try std.testing.expect(imported_book_id > 0);
}

test "CONFORMANCE: OBLE Counterparty/Subledger profile" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "AR Book", "PHP", 2, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    var book_buf: [4096]u8 = undefined;
    var accounts_buf: [8192]u8 = undefined;
    var counterparties_buf: [8192]u8 = undefined;

    const book_json = try oble_export.exportBookJson(source_db, book_id, &book_buf);
    const accounts_json = try oble_export.exportAccountsJson(source_db, book_id, &accounts_buf);
    const counterparties_json = try oble_export.exportCounterpartiesJson(source_db, book_id, &counterparties_buf);
    try std.testing.expect(std.mem.indexOf(u8, counterparties_json, "\"control_account_id\"") != null);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var ctx = oble_import.ImportContext.init(std.testing.allocator);
    defer ctx.deinit();
    _ = try oble_import.importBookJson(target_db, &ctx, book_json, "admin");
    try oble_import.importAccountsJson(target_db, &ctx, accounts_json, "admin");
    try oble_import.importCounterpartiesJson(target_db, &ctx, counterparties_json, "admin");

    var round_counterparties_buf: [8192]u8 = undefined;
    const round_counterparties_json = try oble_export.exportCounterpartiesJson(target_db, ctx.book_ids.get("book-1").?, &round_counterparties_buf);
    try std.testing.expect(std.mem.indexOf(u8, round_counterparties_json, "\"role\":\"customer\"") != null);
}

test "CONFORMANCE: OBLE Multi-Currency profile" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "FX Book", "USD", 2, "admin");
    try book_mod.Book.setEntityType(database, book_id, .corporation, "admin");

    const cash_id = try account_mod.Account.create(database, book_id, "1010", "Cash USD", .asset, false, "admin");
    const payable_id = try account_mod.Account.create(database, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(database, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var entry_buf: [16384]u8 = undefined;
    const entry_json = try oble_export.exportEntryJson(database, entry_id, &entry_buf);
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"transaction_currency\":\"USD\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_json, "\"base_credit_amount\"") != null);

    const rates = [_]revaluation_mod.CurrencyRate{
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const result = try revaluation_mod.revalueForexBalances(database, book_id, jan_id, &rates, "admin");
    var reval_buf: [32768]u8 = undefined;
    const reval_json = try oble_export.exportRevaluationPacketJson(database, result.entry_id, &reval_buf);
    try std.testing.expect(std.mem.indexOf(u8, reval_json, "\"revaluation_entry\"") != null);
}

test "CONFORMANCE: OBLE Close/Reopen profile" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Close Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    try close_mod.closePeriod(database, book_id, jan_id, "admin");
    var close_buf: [16384]u8 = undefined;
    const close_json = try oble_export.exportCloseReopenProfileJson(database, book_id, jan_id, &close_buf);
    try std.testing.expect(std.mem.indexOf(u8, close_json, "\"closing_entries\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, close_json, "\"next_opening_entry\"") != null);
}

test "CONFORMANCE: OBLE statement-result packet family" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Statement Conformance Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, capital_id, "admin");

    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const open_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-001", "2026-01-01", "2026-01-01", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 1, 1_000_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 2, 0, 1_000_00_000_000, "PHP", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(database, open_id, "admin");

    const sale_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-02-10", "2026-02-10", null, feb_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 1, 200_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 2, 0, 200_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, sale_id, "admin");

    var statement_buf: [32 * 1024]u8 = undefined;
    const tb_json = try oble_results.exportTrialBalanceResultPacketJson(database, book_id, "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, tb_json, "\"packet_kind\":\"trial_balance\"") != null);

    const movement_json = try oble_results.exportTrialBalanceMovementResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, movement_json, "\"packet_kind\":\"trial_balance_movement\"") != null);

    const income_json = try oble_results.exportIncomeStatementResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, income_json, "\"packet_kind\":\"income_statement\"") != null);

    const balance_json = try oble_results.exportBalanceSheetResultPacketJson(database, book_id, "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, balance_json, "\"packet_kind\":\"balance_sheet\"") != null);

    var comparative_buf: [32 * 1024]u8 = undefined;
    const tb_cmp_json = try oble_results.exportTrialBalanceComparativeResultPacketJson(database, book_id, "2026-02-28", "2026-01-31", &comparative_buf);
    try std.testing.expect(std.mem.indexOf(u8, tb_cmp_json, "\"packet_kind\":\"trial_balance_comparative\"") != null);

    const movement_cmp_json = try oble_results.exportTrialBalanceMovementComparativeResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", "2026-01-01", "2026-01-31", &comparative_buf);
    try std.testing.expect(std.mem.indexOf(u8, movement_cmp_json, "\"packet_kind\":\"trial_balance_movement_comparative\"") != null);

    const income_cmp_json = try oble_results.exportIncomeStatementComparativeResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", "2026-01-01", "2026-01-31", &comparative_buf);
    try std.testing.expect(std.mem.indexOf(u8, income_cmp_json, "\"packet_kind\":\"income_statement_comparative\"") != null);

    const balance_cmp_json = try oble_results.exportBalanceSheetComparativeResultPacketJson(database, book_id, "2026-02-28", "2026-01-31", "2026-01-01", &comparative_buf);
    try std.testing.expect(std.mem.indexOf(u8, balance_cmp_json, "\"packet_kind\":\"balance_sheet_comparative\"") != null);

    var equity_buf: [32 * 1024]u8 = undefined;
    const equity_json = try oble_results.exportEquityChangesResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", "2026-01-01", &equity_buf);
    try std.testing.expect(std.mem.indexOf(u8, equity_json, "\"packet_kind\":\"equity_changes\"") != null);
}

test "CONFORMANCE: OBLE indirect cash flow, integrity, and translated result packets" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Derived Result Conformance Book", "PHP", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const open_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-001", "2026-01-01", "2026-01-01", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 1, 1_000_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 2, 0, 1_000_00_000_000, "PHP", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(database, open_id, "admin");

    const sale_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-001", "2026-02-10", "2026-02-10", null, feb_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 1, 200_00_000_000, 0, "PHP", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 2, 0, 200_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, sale_id, "admin");

    const cf_cls = try classification_mod.Classification.create(database, book_id, "Cash Flow", "cash_flow", "admin");
    _ = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Operating Activities", null, 1, "admin");
    _ = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Investing Activities", null, 2, "admin");
    _ = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Financing Activities", null, 3, "admin");

    var buf: [32 * 1024]u8 = undefined;
    const indirect_json = try oble_results.exportCashFlowIndirectResultPacketJson(database, book_id, cf_cls, "2026-01-01", "2026-01-31", &buf);
    try std.testing.expect(std.mem.indexOf(u8, indirect_json, "\"packet_kind\":\"cash_flow_indirect\"") != null);

    const integrity_json = try oble_results.exportIntegritySummaryResultPacketJson(database, book_id, &buf);
    try std.testing.expect(std.mem.indexOf(u8, integrity_json, "\"packet_kind\":\"integrity_summary\"") != null);

    const translated_json = try oble_results.exportTranslatedTrialBalanceResultPacketJson(database, book_id, "2026-02-28", "USD", 180000000, 185000000, &buf);
    try std.testing.expect(std.mem.indexOf(u8, translated_json, "\"packet_kind\":\"translated_trial_balance\"") != null);
}
