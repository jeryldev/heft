const std = @import("std");
const db = @import("db.zig");
const oble_core = @import("oble_core.zig");
const oble_import = @import("oble_import.zig");
const oble_profile_counterparty = @import("oble_profile_counterparty.zig");
const oble_profile_policy = @import("oble_profile_policy.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const subledger_mod = @import("subledger.zig");
const entry_mod = @import("entry.zig");
const open_item_mod = @import("open_item.zig");
const money = @import("money.zig");

pub const ImportContext = oble_import.ImportContext;
pub const ReversalPairImportIds = oble_import.ReversalPairImportIds;
pub const EntityKind = enum {
    book,
    account,
    period,
    entry,
    line,
    counterparty,
    open_item,
};

pub const Session = struct {
    database: db.Database,
    performed_by: []const u8,
    ctx: ImportContext,

    pub fn init(database: db.Database, allocator: std.mem.Allocator, performed_by: []const u8) Session {
        return .{
            .database = database,
            .performed_by = performed_by,
            .ctx = ImportContext.init(allocator),
        };
    }

    pub fn deinit(self: *Session) void {
        self.ctx.deinit();
    }

    pub fn resolveImportedId(self: *const Session, kind: EntityKind, logical_id: []const u8) ?i64 {
        return switch (kind) {
            .book => self.ctx.book_ids.get(logical_id),
            .account => self.ctx.account_ids.get(logical_id),
            .period => self.ctx.period_ids.get(logical_id),
            .entry => self.ctx.entry_ids.get(logical_id),
            .line => self.ctx.line_ids.get(logical_id),
            .counterparty => self.ctx.counterparty_ids.get(logical_id),
            .open_item => self.ctx.open_item_ids.get(logical_id),
        };
    }

    pub fn importCoreBundleJson(self: *Session, json: []const u8) !i64 {
        return oble_core.importCoreBundleJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importEntryJson(self: *Session, json: []const u8) !i64 {
        return oble_core.importEntryJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importReversalPairJson(self: *Session, json: []const u8) !ReversalPairImportIds {
        return oble_import.importReversalPairJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importCounterpartiesJson(self: *Session, json: []const u8) !void {
        return oble_profile_counterparty.importCounterpartiesJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importCounterpartyProfileBundleJson(self: *Session, json: []const u8) !void {
        return oble_profile_counterparty.importCounterpartyProfileBundleJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importPolicyProfileJson(self: *Session, json: []const u8) !i64 {
        return oble_profile_policy.importPolicyProfileJson(self.database, &self.ctx, json, self.performed_by);
    }

    pub fn importBookSnapshotJson(self: *Session, json: []const u8) !i64 {
        return oble_profile_counterparty.importBookSnapshotJson(self.database, &self.ctx, json, self.performed_by);
    }
};

test "OBLE import session: core then counterparty profile flow" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Session Source", "PHP", 2, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4100", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", period_id, null, "admin");
    const receivable_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");
    _ = try open_item_mod.createOpenItem(source_db, receivable_line_id, customer_id, 500_00_000_000, "2026-02-15", book_id, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var counterparties_buf: [32 * 1024]u8 = undefined;
    var entry_buf: [32 * 1024]u8 = undefined;
    var profile_buf: [128 * 1024]u8 = undefined;

    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const counterparties_json = try oble_profile_counterparty.exportCounterpartiesJson(source_db, book_id, &counterparties_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const profile_json = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(source_db, book_id, &profile_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    const imported_book_id = try session.importCoreBundleJson(core_json);
    try session.importCounterpartiesJson(counterparties_json);
    _ = try session.importEntryJson(entry_json);
    try session.importCounterpartyProfileBundleJson(profile_json);

    try std.testing.expectEqual(imported_book_id, session.resolveImportedId(.book, "book-1").?);
    try std.testing.expect(session.resolveImportedId(.counterparty, "cp-1") != null);
    try std.testing.expect(session.resolveImportedId(.open_item, "oi-1") != null);

    var round_buf: [128 * 1024]u8 = undefined;
    const round_json = try oble_profile_counterparty.exportCounterpartyProfileBundleJson(target_db, imported_book_id, &round_buf);
    try std.testing.expectEqualStrings(profile_json, round_json);
}

test "OBLE import session: importing dependent entry before counterparties fails" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Session Source", "PHP", 2, "admin");
    const ar_id = try account_mod.Account.create(source_db, book_id, "1100", "Accounts Receivable", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4100", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(source_db, book_id, "Customers", "customer", 1, ar_id, null, null, "admin");
    const customer_id = try subledger_mod.SubledgerAccount.create(source_db, book_id, "C001", "Customer ABC", "customer", group_id, "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "INV-001", "2026-01-15", "2026-01-15", "Invoice", period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 500_00_000_000, 0, "PHP", money.FX_RATE_SCALE, ar_id, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 500_00_000_000, "PHP", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var entry_buf: [32 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    try std.testing.expectError(error.NotFound, session.importEntryJson(entry_json));
}

test "OBLE import session: duplicate core import in same session is rejected" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Core Source", "USD", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    try std.testing.expectError(error.DuplicateNumber, session.importCoreBundleJson(core_json));
}
