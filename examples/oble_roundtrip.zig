const std = @import("std");
const heft = @import("heft");

pub fn main() !void {
    const out = std.fs.File.stdout().deprecatedWriter();

    const source_db = try heft.db.Database.open(":memory:");
    defer source_db.close();
    try heft.schema.createAll(source_db);

    const source_book_id = try heft.book.Book.create(source_db, "Roundtrip Co", "USD", 2, "example");
    const cash_id = try heft.account.Account.create(source_db, source_book_id, "1000", "Cash", .asset, false, "example");
    const retained_earnings_id = try heft.account.Account.create(source_db, source_book_id, "3100", "Retained Earnings", .equity, false, "example");
    const revenue_id = try heft.account.Account.create(source_db, source_book_id, "4000", "Consulting Revenue", .revenue, false, "example");
    try heft.book.Book.setRetainedEarningsAccount(source_db, source_book_id, retained_earnings_id, "example");

    const jan_id = try heft.period.Period.create(
        source_db,
        source_book_id,
        "January 2026",
        1,
        2026,
        "2026-01-01",
        "2026-01-31",
        "regular",
        "example",
    );

    const entry_id = try heft.entry.Entry.createDraft(
        source_db,
        source_book_id,
        "INV-2001",
        "2026-01-20",
        "2026-01-20",
        "Roundtrip invoice",
        jan_id,
        null,
        "example",
    );
    _ = try heft.entry.Entry.addLine(
        source_db,
        entry_id,
        1,
        250 * heft.money.AMOUNT_SCALE,
        0,
        "USD",
        heft.money.FX_RATE_SCALE,
        cash_id,
        null,
        "Cash",
        "example",
    );
    _ = try heft.entry.Entry.addLine(
        source_db,
        entry_id,
        2,
        0,
        250 * heft.money.AMOUNT_SCALE,
        "USD",
        heft.money.FX_RATE_SCALE,
        revenue_id,
        null,
        "Revenue",
        "example",
    );
    try heft.entry.Entry.post(source_db, entry_id, "example");

    var core_buf: [128 * 1024]u8 = undefined;
    var entry_buf: [64 * 1024]u8 = undefined;
    var policy_buf: [32 * 1024]u8 = undefined;
    const core_json = try heft.oble_core.exportCoreBundleJson(source_db, source_book_id, &core_buf);
    const entry_json = try heft.oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const policy_json = try heft.oble_profile_policy.exportPolicyProfileJson(source_db, source_book_id, &policy_buf);

    const target_db = try heft.db.Database.open(":memory:");
    defer target_db.close();
    try heft.schema.createAll(target_db);

    var session = heft.oble_import_session.Session.init(target_db, std.heap.c_allocator, "example");
    defer session.deinit();

    const target_book_id = try session.importCoreBundleJson(core_json);
    _ = try session.importEntryJson(entry_json);
    _ = try session.importPolicyProfileJson(policy_json);

    const report = try heft.oble_semantic_verify.verifyBookSemantics(
        source_db,
        source_book_id,
        target_db,
        target_book_id,
    );

    try out.writeAll("OBLE roundtrip verification\n");
    try out.print("core_equal={any}\n", .{report.core_equal});
    try out.print("counterparty_equal={any}\n", .{report.counterparty_equal});
    try out.print("policy_equal={any}\n", .{report.policy_equal});
    try out.print("trial_balance_equal={any}\n", .{report.trial_balance_equal});
    try out.print("passed={any}\n", .{report.passed()});
}
