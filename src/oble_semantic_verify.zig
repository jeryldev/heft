const std = @import("std");
const db = @import("db.zig");
const oble_core = @import("oble_core.zig");
const oble_profile_counterparty = @import("oble_profile_counterparty.zig");
const oble_profile_policy = @import("oble_profile_policy.zig");
const report_mod = @import("report.zig");
const export_mod = @import("export.zig");

pub const VerificationReport = struct {
    core_equal: bool,
    counterparty_equal: bool,
    policy_equal: bool,
    trial_balance_equal: bool,

    pub fn passed(self: VerificationReport) bool {
        return self.core_equal and self.counterparty_equal and self.policy_equal and self.trial_balance_equal;
    }
};

fn maxPeriodEndDate(database: db.Database, book_id: i64) ![10]u8 {
    var stmt = try database.prepare(
        \\SELECT MAX(end_date)
        \\FROM ledger_periods
        \\WHERE book_id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (!try stmt.step()) return error.NotFound;
    const text = stmt.columnText(0) orelse return error.NotFound;
    if (text.len != 10) return error.InvalidInput;
    var out: [10]u8 = undefined;
    @memcpy(&out, text);
    return out;
}

fn exportTrialBalanceJson(database: db.Database, book_id: i64, buf: []u8) ![]u8 {
    const as_of_date = try maxPeriodEndDate(database, book_id);
    const result = try report_mod.trialBalance(database, book_id, &as_of_date);
    defer result.deinit();
    return export_mod.reportToJson(result, buf);
}

pub fn verifyBookSemantics(source_db: db.Database, source_book_id: i64, target_db: db.Database, target_book_id: i64) !VerificationReport {
    var source_core_buf: [256 * 1024]u8 = undefined;
    var target_core_buf: [256 * 1024]u8 = undefined;
    const source_core = try oble_core.exportCoreBundleJson(source_db, source_book_id, &source_core_buf);
    const target_core = try oble_core.exportCoreBundleJson(target_db, target_book_id, &target_core_buf);

    var source_counterparty_buf: [256 * 1024]u8 = undefined;
    var target_counterparty_buf: [256 * 1024]u8 = undefined;
    const source_counterparty = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(source_db, source_book_id, &source_counterparty_buf);
    const target_counterparty = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(target_db, target_book_id, &target_counterparty_buf);

    var source_policy_buf: [64 * 1024]u8 = undefined;
    var target_policy_buf: [64 * 1024]u8 = undefined;
    const source_policy = try oble_profile_policy.exportPolicyProfileJson(source_db, source_book_id, &source_policy_buf);
    const target_policy = try oble_profile_policy.exportPolicyProfileJson(target_db, target_book_id, &target_policy_buf);

    var source_tb_buf: [128 * 1024]u8 = undefined;
    var target_tb_buf: [128 * 1024]u8 = undefined;
    const source_tb = try exportTrialBalanceJson(source_db, source_book_id, &source_tb_buf);
    const target_tb = try exportTrialBalanceJson(target_db, target_book_id, &target_tb_buf);

    return .{
        .core_equal = std.mem.eql(u8, source_core, target_core),
        .counterparty_equal = std.mem.eql(u8, source_counterparty, target_counterparty),
        .policy_equal = std.mem.eql(u8, source_policy, target_policy),
        .trial_balance_equal = std.mem.eql(u8, source_tb, target_tb),
    };
}

test "OBLE semantic verify: imported book matches source semantics" {
    const schema = @import("schema.zig");
    const book_mod = @import("book.zig");
    const account_mod = @import("account.zig");
    const period_mod = @import("period.zig");
    const entry_mod = @import("entry.zig");
    const subledger_mod = @import("subledger.zig");
    const open_item_mod = @import("open_item.zig");
    const money = @import("money.zig");
    const oble_import_session = @import("oble_import_session.zig");

    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Verify Source", "PHP", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setRequireApproval(source_db, book_id, true, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", jan_id, null, "admin");
    const receivable_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.approve(source_db, entry_id, "reviewer");
    try entry_mod.Entry.post(source_db, entry_id, "admin");
    _ = try open_item_mod.createOpenItem(source_db, receivable_line_id, customer_id, 500_00_000_000, "2026-02-15", book_id, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var counterparties_buf: [64 * 1024]u8 = undefined;
    var entry_buf: [64 * 1024]u8 = undefined;
    var profile_buf: [128 * 1024]u8 = undefined;
    var policy_buf: [16 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const counterparties_json = try oble_profile_counterparty.exportCounterpartiesJson(source_db, book_id, &counterparties_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const profile_json = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(source_db, book_id, &profile_buf);
    const policy_json = try oble_profile_policy.exportPolicyProfileJson(source_db, book_id, &policy_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = oble_import_session.Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();
    const imported_book_id = try session.importCoreBundleJson(core_json);
    try session.importCounterpartiesJson(counterparties_json);
    _ = try session.importEntryJson(entry_json);
    try session.importCounterpartyProfileBundleJson(profile_json);
    _ = try session.importPolicyProfileJson(policy_json);

    const report = try verifyBookSemantics(source_db, book_id, target_db, imported_book_id);
    try std.testing.expect(report.passed());
}
