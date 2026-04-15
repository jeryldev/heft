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
const oble_import_session = @import("oble_import_session.zig");
const oble_profile_budget = @import("oble_profile_budget.zig");
const oble_profile_classification = @import("oble_profile_classification.zig");
const oble_profile_counterparty = @import("oble_profile_counterparty.zig");
const oble_profile_dimension = @import("oble_profile_dimension.zig");
const oble_profile_fx = @import("oble_profile_fx.zig");
const oble_profile_policy = @import("oble_profile_policy.zig");
const oble_results = @import("oble_profile_results.zig");

fn readExampleJson(allocator: std.mem.Allocator, relative_path: []const u8, max_bytes: usize) ![]u8 {
    if (std.fs.cwd().readFileAlloc(allocator, relative_path, max_bytes)) |bytes| {
        return bytes;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    const absolute_path = try std.fs.path.join(allocator, &.{ "/Users/jeryldev/code/zig_projects/heft", relative_path });
    defer allocator.free(absolute_path);
    return std.fs.cwd().readFileAlloc(allocator, absolute_path, max_bytes);
}

fn putMappedId(ctx: *oble_import.ImportContext, map: *std.StringHashMap(i64), logical_id: []const u8, physical_id: i64) !void {
    const owned_key = try ctx.stableAllocator().dupe(u8, logical_id);
    errdefer ctx.stableAllocator().free(owned_key);
    try map.putNoClobber(owned_key, physical_id);
}

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

test "CONFORMANCE: OBLE audit-trail result packet" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Audit Conformance Book", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");

    var buf: [32 * 1024]u8 = undefined;
    const audit_json = try oble_results.exportAuditTrailResultPacketJson(database, book_id, "2020-01-01", "2030-12-31", &buf);
    try std.testing.expect(std.mem.indexOf(u8, audit_json, "\"packet_kind\":\"audit_trail\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, audit_json, "\"records\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, audit_json, "\"hash_chain\":\"") != null);
}

test "CONFORMANCE: published OBLE legacy example packets import through Heft" {
    const allocator = std.testing.allocator;
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var ctx = oble_import.ImportContext.init(allocator);
    defer ctx.deinit();

    const book_json = try readExampleJson(allocator, "docs/oble/examples/core-book.json", 4096);
    defer allocator.free(book_json);
    const accounts_json = try readExampleJson(allocator, "docs/oble/examples/core-accounts.json", 8192);
    defer allocator.free(accounts_json);
    const periods_json = try readExampleJson(allocator, "docs/oble/examples/core-periods.json", 8192);
    defer allocator.free(periods_json);
    const entry_json = try readExampleJson(allocator, "docs/oble/examples/core-entry-posted.json", 8192);
    defer allocator.free(entry_json);
    const counterparties_json = try readExampleJson(allocator, "docs/oble/examples/counterparties.json", 8192);
    defer allocator.free(counterparties_json);
    const policy_json = try readExampleJson(allocator, "docs/oble/examples/policy-profile.json", 4096);
    defer allocator.free(policy_json);
    const reversal_json = try readExampleJson(allocator, "docs/oble/examples/reversal-pair.json", 16384);
    defer allocator.free(reversal_json);

    const book_id = try oble_import.importBookJson(database, &ctx, book_json, "admin");
    try oble_import.importAccountsJson(database, &ctx, accounts_json, "admin");
    try oble_import.importPeriodsJson(database, &ctx, periods_json, "admin");
    const ar_id = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const ap_id = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "admin");
    try putMappedId(&ctx, &ctx.account_ids, "acct-1100", ar_id);
    try putMappedId(&ctx, &ctx.account_ids, "acct-2000", ap_id);
    const entry_id = try oble_import.importEntryJson(database, &ctx, entry_json, "admin");
    try oble_import.importCounterpartiesJson(database, &ctx, counterparties_json, "admin");
    _ = try oble_import.importPolicyProfileJson(database, &ctx, policy_json, "admin");
    const reversal_ids = try oble_import.importReversalPairJson(database, &ctx, reversal_json, "admin");

    try std.testing.expect(book_id > 0);
    try std.testing.expect(entry_id > 0);
    try std.testing.expect(reversal_ids.reversal_entry_id > reversal_ids.original_entry_id);
    try std.testing.expect(ctx.counterparty_ids.get("cp-001") != null);
}

test "CONFORMANCE: published OBLE open-item and snapshot examples import through Heft" {
    const allocator = std.testing.allocator;

    {
        const database = try db.Database.open(":memory:");
        defer database.close();
        try schema.createAll(database);

        var ctx = oble_import.ImportContext.init(allocator);
        defer ctx.deinit();

        const book_json = try readExampleJson(allocator, "docs/oble/examples/core-book.json", 4096);
        defer allocator.free(book_json);
        const accounts_json = try readExampleJson(allocator, "docs/oble/examples/core-accounts.json", 8192);
        defer allocator.free(accounts_json);
        const periods_json = try readExampleJson(allocator, "docs/oble/examples/core-periods.json", 8192);
        defer allocator.free(periods_json);
        const counterparties_json = try readExampleJson(allocator, "docs/oble/examples/counterparties.json", 8192);
        defer allocator.free(counterparties_json);
        const open_item_json = try readExampleJson(allocator, "docs/oble/examples/counterparty-open-item.json", 8192);
        defer allocator.free(open_item_json);

        const book_id = try oble_import.importBookJson(database, &ctx, book_json, "admin");
        try oble_import.importAccountsJson(database, &ctx, accounts_json, "admin");
        try oble_import.importPeriodsJson(database, &ctx, periods_json, "admin");
        const ar_id = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
        const ap_id = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "admin");
        try putMappedId(&ctx, &ctx.account_ids, "acct-1100", ar_id);
        try putMappedId(&ctx, &ctx.account_ids, "acct-2000", ap_id);
        try oble_import.importCounterpartiesJson(database, &ctx, counterparties_json, "admin");

        const period_id = ctx.period_ids.get("period-2026-01").?;
        const revenue_id = ctx.account_ids.get("acct-4000").?;
        const counterparty_id = ctx.counterparty_ids.get("cp-001").?;

        const invoice_id = try entry_mod.Entry.createDraft(database, book_id, "INV-020", "2026-01-20", "2026-01-20", "Receivable", period_id, null, "admin");
        const line_id = try entry_mod.Entry.addLine(database, invoice_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_id, counterparty_id, null, "admin");
        _ = try entry_mod.Entry.addLine(database, invoice_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
        try entry_mod.Entry.post(database, invoice_id, "admin");
        try putMappedId(&ctx, &ctx.line_ids, "line-100", line_id);

        const ids = try oble_import.importCounterpartyOpenItemJson(database, &ctx, open_item_json, "admin");
        try std.testing.expect(ids.counterparty_id == counterparty_id);
        try std.testing.expect(ids.open_item_id > 0);
    }

    {
        const database = try db.Database.open(":memory:");
        defer database.close();
        try schema.createAll(database);

        var ctx = oble_import.ImportContext.init(allocator);
        defer ctx.deinit();

        const snapshot_json = try readExampleJson(allocator, "docs/oble/examples/book-snapshot.json", 16384);
        defer allocator.free(snapshot_json);

        const book_id = try oble_import.importBookSnapshotJson(database, &ctx, snapshot_json, "admin");
        try std.testing.expect(book_id > 0);
        try std.testing.expect(ctx.counterparty_ids.get("cp-001") != null);
    }
}

test "CONFORMANCE: published OBLE profile examples import through Heft" {
    const allocator = std.testing.allocator;
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var ctx = oble_import.ImportContext.init(allocator);
    defer ctx.deinit();

    const book_id = try book_mod.Book.create(database, "Profile Example Book", "PHP", 2, "admin");
    const acct_1 = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const acct_2 = try account_mod.Account.create(database, book_id, "2000", "Receivable", .asset, false, "admin");
    const period_1 = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const period_2 = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "DIM-001", "2026-01-05", "2026-01-05", null, period_1, null, "admin");
    const line_id = try entry_mod.Entry.addLine(database, entry_id, 1, 100_00_000_000, 0, "PHP", money.FX_RATE_SCALE, acct_1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 100_00_000_000, "PHP", money.FX_RATE_SCALE, acct_2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    try putMappedId(&ctx, &ctx.book_ids, "book-1", book_id);
    try putMappedId(&ctx, &ctx.account_ids, "acct-1", acct_1);
    try putMappedId(&ctx, &ctx.account_ids, "acct-2", acct_2);
    try putMappedId(&ctx, &ctx.period_ids, "period-2026-01", period_1);
    try putMappedId(&ctx, &ctx.period_ids, "period-2026-02", period_2);
    try putMappedId(&ctx, &ctx.line_ids, "line-1", line_id);

    const classification_json = try readExampleJson(allocator, "docs/oble/examples/classification-profile.json", 8192);
    defer allocator.free(classification_json);
    const dimension_json = try readExampleJson(allocator, "docs/oble/examples/dimension-profile.json", 8192);
    defer allocator.free(dimension_json);
    const budget_json = try readExampleJson(allocator, "docs/oble/examples/budget-profile.json", 8192);
    defer allocator.free(budget_json);

    const classification_id = try oble_profile_classification.importClassificationProfileBundleJson(database, &ctx, classification_json, "admin");
    try oble_profile_dimension.importDimensionProfileBundleJson(database, &ctx, dimension_json, "admin");
    const budget_id = try oble_profile_budget.importBudgetProfileBundleJson(database, &ctx, budget_json, "admin");

    try std.testing.expect(classification_id > 0);
    try std.testing.expect(budget_id > 0);
    try std.testing.expect(ctx.classification_ids.get("classification-1") != null);
    try std.testing.expect(ctx.dimension_ids.get("dimension-1") != null);
    try std.testing.expect(ctx.budget_ids.get("budget-1") != null);
}

test "CONFORMANCE: OBLE profile bundles compose through a single import session" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Bundle Session Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "JE-001", "2026-01-10", "2026-01-10", "Sale", jan_id, null, "admin");
    const cash_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 100_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 100_00_000_000, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const classification_id = try classification_mod.Classification.create(source_db, book_id, "Income", "income_statement", "admin");
    const revenue_group = try classification_mod.ClassificationNode.addGroup(source_db, classification_id, "Revenue", null, 1, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(source_db, classification_id, revenue_id, revenue_group, 1, "admin");

    const dimension_id = try @import("dimension.zig").Dimension.create(source_db, book_id, "Region", .segment, "admin");
    const dimension_value_id = try @import("dimension.zig").DimensionValue.create(source_db, dimension_id, "SEA", "Southeast Asia", "admin");
    try @import("dimension.zig").LineDimension.assign(source_db, cash_line_id, dimension_value_id, "admin");

    const budget_id = try @import("budget.zig").Budget.create(source_db, book_id, "FY2026", 2026, "admin");
    _ = try @import("budget.zig").BudgetLine.set(source_db, budget_id, revenue_id, jan_id, 250_00_000_000, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var classification_buf: [64 * 1024]u8 = undefined;
    var dimension_buf: [64 * 1024]u8 = undefined;
    var budget_buf: [64 * 1024]u8 = undefined;
    var policy_buf: [64 * 1024]u8 = undefined;
    var entry_buf: [64 * 1024]u8 = undefined;
    const core_json = try @import("oble_core.zig").exportCoreBundleJson(source_db, book_id, &core_buf);
    const entry_json = try @import("oble_core.zig").exportEntryJson(source_db, entry_id, &entry_buf);
    const classification_json = try oble_profile_classification.exportClassificationProfileBundleJson(source_db, classification_id, &classification_buf);
    const dimension_json = try oble_profile_dimension.exportDimensionProfileBundleJson(source_db, book_id, &dimension_buf);
    const budget_json = try oble_profile_budget.exportBudgetProfileBundleJson(source_db, budget_id, &budget_buf);
    const policy_json = try oble_profile_policy.exportPolicyProfileJson(source_db, book_id, &policy_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = oble_import_session.Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    const imported_book_id = try session.importCoreBundleJson(core_json);
    _ = try session.importEntryJson(entry_json);
    _ = try session.importClassificationProfileBundleJson(classification_json);
    try session.importDimensionProfileBundleJson(dimension_json);
    _ = try session.importBudgetProfileBundleJson(budget_json);
    _ = try session.importPolicyProfileJson(policy_json);

    var round_classification_buf: [64 * 1024]u8 = undefined;
    var round_dimension_buf: [64 * 1024]u8 = undefined;
    var round_budget_buf: [64 * 1024]u8 = undefined;
    const round_classification = try oble_profile_classification.exportClassificationProfileBundleJson(target_db, session.resolveImportedId(.classification, "classification-1").?, &round_classification_buf);
    const round_dimension = try oble_profile_dimension.exportDimensionProfileBundleJson(target_db, imported_book_id, &round_dimension_buf);
    const round_budget = try oble_profile_budget.exportBudgetProfileBundleJson(target_db, session.resolveImportedId(.budget, "budget-1").?, &round_budget_buf);

    try std.testing.expect(std.mem.indexOf(u8, round_classification, "\"nodes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_dimension, "\"line_dimension_assignments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_budget, "\"budget_lines\"") != null);
}

test "CONFORMANCE: OBLE operational bundles compose with policy and FX exports" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Operational Bundle Book", "USD", 2, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const ar_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-05", "2026-01-05", "Invoice", jan_id, null, "admin");
    const ar_line_id = try entry_mod.Entry.addLine(source_db, ar_entry_id, 1, 100_00_000_000, 0, "USD", money.FX_RATE_SCALE, ar_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, ar_entry_id, 2, 0, 100_00_000_000, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, ar_entry_id, "admin");
    const open_item_id = try @import("open_item.zig").createOpenItem(source_db, ar_line_id, customer_id, 100_00_000_000, "2026-01-31", book_id, "admin");

    const fx_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, fx_entry_id, "admin");

    const rates = [_]revaluation_mod.CurrencyRate{.{ .currency = "EUR", .new_rate = 1_200_000_0000 }};
    const revalue_result = try revaluation_mod.revalueForexBalances(source_db, book_id, jan_id, &rates, "admin");
    try close_mod.closePeriod(source_db, book_id, jan_id, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var counterparty_buf: [64 * 1024]u8 = undefined;
    var open_item_buf: [64 * 1024]u8 = undefined;
    var policy_lifecycle_buf: [64 * 1024]u8 = undefined;
    var fx_buf: [64 * 1024]u8 = undefined;
    var ar_entry_buf: [64 * 1024]u8 = undefined;
    const core_json = try @import("oble_core.zig").exportCoreBundleJson(source_db, book_id, &core_buf);
    const ar_entry_json = try @import("oble_core.zig").exportEntryJson(source_db, ar_entry_id, &ar_entry_buf);
    const counterparties_json = try oble_profile_counterparty.exportCounterpartiesJson(source_db, book_id, &counterparty_buf);
    const open_item_json = try oble_profile_counterparty.exportCounterpartyOpenItemJson(source_db, open_item_id, &open_item_buf);
    const policy_lifecycle_json = try oble_profile_policy.exportPolicyLifecycleBundleJson(source_db, book_id, jan_id, revalue_result.entry_id, &policy_lifecycle_buf);
    const fx_json = try oble_profile_fx.exportMultiCurrencyBundleJson(source_db, fx_entry_id, revalue_result.entry_id, &fx_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = oble_import_session.Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    const imported_book_id = try session.importCoreBundleJson(core_json);
    try session.importCounterpartiesJson(counterparties_json);
    _ = try session.importEntryJson(ar_entry_json);
    _ = try oble_profile_counterparty.importCounterpartyOpenItemJson(target_db, &session.ctx, open_item_json, "admin");
    const policy_import = try session.importPolicyLifecycleBundleJson(policy_lifecycle_json);
    const fx_import = try session.importMultiCurrencyBundleJson(fx_json);

    var round_counterparty_buf: [64 * 1024]u8 = undefined;
    var round_policy_buf: [64 * 1024]u8 = undefined;
    const round_counterparty = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(target_db, imported_book_id, &round_counterparty_buf);
    const round_policy = try oble_profile_policy.exportPolicyProfileJson(target_db, imported_book_id, &round_policy_buf);

    try std.testing.expect(policy_import.has_close_reopen_profile);
    try std.testing.expect(policy_import.has_revaluation_packet);
    try std.testing.expect(fx_import.foreign_currency_entry_id > 0);
    try std.testing.expect(fx_import.has_revaluation_packet);
    try std.testing.expect(std.mem.indexOf(u8, round_counterparty, "\"open_items\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, round_policy, "\"designations\"") != null);
}
