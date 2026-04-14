const std = @import("std");
const heft = @import("heft");

fn text(bytes: []const u8, len: usize) []const u8 {
    return bytes[0..len];
}

pub fn main() !void {
    const out = std.fs.File.stdout().deprecatedWriter();

    const database = try heft.db.Database.open(":memory:");
    defer database.close();
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Example Co", "USD", 2, "example");
    const cash_id = try heft.account.Account.create(database, book_id, "1000", "Cash", .asset, false, "example");
    const revenue_id = try heft.account.Account.create(database, book_id, "4000", "Service Revenue", .revenue, false, "example");
    const retained_earnings_id = try heft.account.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "example");
    try heft.book.Book.setRetainedEarningsAccount(database, book_id, retained_earnings_id, "example");

    const jan_id = try heft.period.Period.create(
        database,
        book_id,
        "January 2026",
        1,
        2026,
        "2026-01-01",
        "2026-01-31",
        "regular",
        "example",
    );

    const entry_id = try heft.entry.Entry.createDraft(
        database,
        book_id,
        "INV-1001",
        "2026-01-15",
        "2026-01-15",
        "Example sale",
        jan_id,
        null,
        "example",
    );
    _ = try heft.entry.Entry.addLine(
        database,
        entry_id,
        1,
        125 * heft.money.AMOUNT_SCALE,
        0,
        "USD",
        heft.money.FX_RATE_SCALE,
        cash_id,
        null,
        "Cash received",
        "example",
    );
    _ = try heft.entry.Entry.addLine(
        database,
        entry_id,
        2,
        0,
        125 * heft.money.AMOUNT_SCALE,
        "USD",
        heft.money.FX_RATE_SCALE,
        revenue_id,
        null,
        "Revenue earned",
        "example",
    );
    try heft.entry.Entry.post(database, entry_id, "example");

    const trial_balance = try heft.report.trialBalance(database, book_id, "2026-01-31");
    defer trial_balance.deinit();

    try out.writeAll("Trial balance as of 2026-01-31\n");
    for (trial_balance.rows) |row| {
        var debit_buf: [32]u8 = undefined;
        var credit_buf: [32]u8 = undefined;
        const debit = try heft.money.formatDecimal(&debit_buf, row.debit_balance, trial_balance.decimal_places);
        const credit = try heft.money.formatDecimal(&credit_buf, row.credit_balance, trial_balance.decimal_places);
        try out.print(
            "{s} {s}: debit={s} credit={s}\n",
            .{
                text(&row.account_number, row.account_number_len),
                text(&row.account_name, row.account_name_len),
                debit,
                credit,
            },
        );
    }

    var oble_buf: [4096]u8 = undefined;
    const core_bundle = try heft.oble_core.exportCoreBundleJson(database, book_id, &oble_buf);
    try out.writeAll("\nOBLE core bundle preview:\n");
    try out.writeAll(core_bundle);
    try out.writeByte('\n');
}
