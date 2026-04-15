const std = @import("std");
const db = @import("db.zig");
const oble_core = @import("oble_core.zig");
const oble_import = @import("oble_import.zig");
const oble_profile_budget = @import("oble_profile_budget.zig");
const oble_profile_classification = @import("oble_profile_classification.zig");
const oble_profile_counterparty = @import("oble_profile_counterparty.zig");
const oble_profile_dimension = @import("oble_profile_dimension.zig");
const oble_profile_fx = @import("oble_profile_fx.zig");
const oble_profile_policy = @import("oble_profile_policy.zig");
const oble_reconstruction = @import("oble_reconstruction.zig");
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
pub const MultiCurrencyImportResult = oble_profile_fx.MultiCurrencyImportResult;
pub const PolicyLifecycleImportResult = oble_profile_policy.PolicyLifecycleImportResult;
pub const EntityKind = enum {
    book,
    account,
    period,
    entry,
    line,
    counterparty,
    open_item,
    classification,
    classification_node,
    dimension,
    dimension_value,
    budget,
    budget_line,
};

pub const Session = struct {
    database: db.Database,
    allocator: std.mem.Allocator,
    performed_by_owned: []u8,
    max_payload_bytes: usize,
    ctx: ImportContext,

    pub fn init(database: db.Database, allocator: std.mem.Allocator, performed_by: []const u8) Session {
        return initWithLimits(
            database,
            allocator,
            performed_by,
            oble_import.DEFAULT_MAX_IMPORT_IDS_PER_KIND,
            oble_import.DEFAULT_MAX_IMPORT_PAYLOAD_BYTES,
        );
    }

    pub fn initWithLimits(
        database: db.Database,
        allocator: std.mem.Allocator,
        performed_by: []const u8,
        max_ids_per_kind: usize,
        max_payload_bytes: usize,
    ) Session {
        const owned_actor = allocator.dupe(u8, performed_by) catch @panic("out of memory");
        return .{
            .database = database,
            .allocator = allocator,
            .performed_by_owned = owned_actor,
            .max_payload_bytes = max_payload_bytes,
            .ctx = ImportContext.initWithLimits(allocator, max_ids_per_kind),
        };
    }

    pub fn deinit(self: *Session) void {
        self.ctx.deinit();
        self.allocator.free(self.performed_by_owned);
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
            .classification => self.ctx.classification_ids.get(logical_id),
            .classification_node => self.ctx.classification_node_ids.get(logical_id),
            .dimension => self.ctx.dimension_ids.get(logical_id),
            .dimension_value => self.ctx.dimension_value_ids.get(logical_id),
            .budget => self.ctx.budget_ids.get(logical_id),
            .budget_line => self.ctx.budget_line_ids.get(logical_id),
        };
    }

    pub fn setMaxPayloadBytes(self: *Session, max_payload_bytes: usize) !void {
        if (max_payload_bytes == 0) return error.InvalidInput;
        self.max_payload_bytes = max_payload_bytes;
    }

    fn ensurePayloadWithinLimit(self: *const Session, json: []const u8) !void {
        return oble_import.validateImportPayloadWithLimit(json, self.max_payload_bytes);
    }

    pub fn importCoreBundleJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_core.importCoreBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importEntryJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_core.importEntryJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importReversalPairJson(self: *Session, json: []const u8) !ReversalPairImportIds {
        try self.ensurePayloadWithinLimit(json);
        return oble_import.importReversalPairJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importCounterpartiesJson(self: *Session, json: []const u8) !void {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_counterparty.importCounterpartiesJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importCounterpartyProfileBundleJson(self: *Session, json: []const u8) !void {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_counterparty.importCounterpartyProfileBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importClassificationProfileBundleJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_classification.importClassificationProfileBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importDimensionProfileBundleJson(self: *Session, json: []const u8) !void {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_dimension.importDimensionProfileBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importBudgetProfileBundleJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_budget.importBudgetProfileBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importPolicyProfileJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_policy.importPolicyProfileJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importMultiCurrencyBundleJson(self: *Session, json: []const u8) !MultiCurrencyImportResult {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_fx.importMultiCurrencyBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importPolicyLifecycleBundleJson(self: *Session, json: []const u8) !PolicyLifecycleImportResult {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_policy.importPolicyLifecycleBundleJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn importBookSnapshotJson(self: *Session, json: []const u8) !i64 {
        try self.ensurePayloadWithinLimit(json);
        return oble_profile_counterparty.importBookSnapshotJson(self.database, &self.ctx, json, self.performed_by_owned);
    }

    pub fn reconstructCloseForImportedPeriod(self: *Session, period_logical_id: []const u8) !void {
        const period_id = self.resolveImportedId(.period, period_logical_id) orelse return error.NotFound;
        return oble_reconstruction.reconstructCloseForPeriod(self.database, period_id, self.performed_by_owned);
    }

    pub fn reconstructRevaluationForImportedPeriod(self: *Session, period_logical_id: []const u8, rates: []const @import("revaluation.zig").CurrencyRate) !i64 {
        const period_id = self.resolveImportedId(.period, period_logical_id) orelse return error.NotFound;
        return oble_reconstruction.reconstructRevaluationForPeriod(self.database, period_id, rates, self.performed_by_owned);
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

test "OBLE import session: FX bundle imports entry and reports derived revaluation presence" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "FX Session Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1010", "Cash USD", .asset, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const rates = [_](@import("revaluation.zig").CurrencyRate){
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const revalue_result = try @import("revaluation.zig").revalueForexBalances(source_db, book_id, jan_id, &rates, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var bundle_buf: [64 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const bundle_json = try oble_profile_fx.exportMultiCurrencyBundleJson(source_db, entry_id, revalue_result.entry_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    const import_result = try session.importMultiCurrencyBundleJson(bundle_json);
    try std.testing.expect(import_result.foreign_currency_entry_id > 0);
    try std.testing.expect(import_result.has_revaluation_packet);
}

test "OBLE import session: policy lifecycle bundle imports safe policy and reports derived packets" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Policy Session Source", "USD", 2, "admin");
    try book_mod.Book.setEntityType(source_db, book_id, .corporation, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");

    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, re_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");
    try book_mod.Book.setFyStartMonth(source_db, book_id, 4, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-28", "2026-02-28", "regular", "admin");

    const sale_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 1, 1_000_000_000_00, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 2, 0, 1_000_000_000_00, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, sale_entry_id, "admin");

    const fx_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, fx_entry_id, "admin");

    const rates = [_](@import("revaluation.zig").CurrencyRate){
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const revalue_result = try @import("revaluation.zig").revalueForexBalances(source_db, book_id, jan_id, &rates, "admin");
    try @import("close.zig").closePeriod(source_db, book_id, jan_id, "admin");
    try book_mod.Book.setRequireApproval(source_db, book_id, true, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var bundle_buf: [64 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const bundle_json = try oble_profile_policy.exportPolicyLifecycleBundleJson(source_db, book_id, jan_id, revalue_result.entry_id, &bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    const imported_book_id = try session.importCoreBundleJson(core_json);
    const import_result = try session.importPolicyLifecycleBundleJson(bundle_json);
    try std.testing.expectEqual(imported_book_id, import_result.book_id);
    try std.testing.expect(import_result.has_close_reopen_profile);
    try std.testing.expect(import_result.has_revaluation_packet);
}

test "OBLE import session: reconstruction helpers operate on imported logical IDs" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Recon Session Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const retained_earnings_id = try account_mod.Account.create(source_db, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    const payable_id = try account_mod.Account.create(source_db, book_id, "2010", "Payable EUR", .liability, false, "admin");
    const fx_gain_loss_id = try account_mod.Account.create(source_db, book_id, "7990", "FX Gain Loss", .revenue, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(source_db, book_id, retained_earnings_id, "admin");
    try book_mod.Book.setFxGainLossAccount(source_db, book_id, fx_gain_loss_id, "admin");

    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const sale_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "SALE-001", "2026-01-10", "2026-01-10", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 1, 1_000_000_000_00, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, sale_entry_id, 2, 0, 1_000_000_000_00, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, sale_entry_id, "admin");

    const fx_entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "FX-001", "2026-01-15", "2026-01-15", "Foreign payable", jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 1, 110_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, fx_entry_id, 2, 0, 100_00_000_000, "EUR", 1_100_000_0000, payable_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, fx_entry_id, "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    var policy_buf: [16 * 1024]u8 = undefined;
    var fx_bundle_buf: [64 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const policy_json = try oble_profile_policy.exportPolicyProfileJson(source_db, book_id, &policy_buf);
    const fx_bundle_json = try oble_profile_fx.exportMultiCurrencyBundleJson(source_db, fx_entry_id, null, &fx_bundle_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    _ = try session.importPolicyProfileJson(policy_json);
    _ = try session.importMultiCurrencyBundleJson(fx_bundle_json);

    const rates = [_](@import("revaluation.zig").CurrencyRate){
        .{ .currency = "EUR", .new_rate = 1_200_000_0000 },
    };
    const reval_entry_id = try session.reconstructRevaluationForImportedPeriod("period-2026-01", &rates);
    try std.testing.expect(reval_entry_id > 0);

    try session.reconstructCloseForImportedPeriod("period-2026-01");

    var close_buf: [32 * 1024]u8 = undefined;
    const imported_book_id = session.resolveImportedId(.book, "book-1").?;
    const imported_period_id = session.resolveImportedId(.period, "period-2026-01").?;
    const close_json = try oble_profile_policy.exportCloseReopenProfileJson(target_db, imported_book_id, imported_period_id, &close_buf);
    try std.testing.expect(std.mem.indexOf(u8, close_json, "\"closing_entries\"") != null);
}

test "OBLE import session: classification profile imports and resolves logical IDs" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Classification Session Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const receivable_id = try account_mod.Account.create(source_db, book_id, "1100", "Receivables", .asset, false, "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const classification_id = try @import("classification.zig").Classification.create(source_db, book_id, "Balance Sheet", "balance_sheet", "admin");
    const assets_group_id = try @import("classification.zig").ClassificationNode.addGroup(source_db, classification_id, "Assets", null, 1, "admin");
    _ = try @import("classification.zig").ClassificationNode.addAccount(source_db, classification_id, cash_id, assets_group_id, 1, "admin");
    _ = try @import("classification.zig").ClassificationNode.addAccount(source_db, classification_id, receivable_id, assets_group_id, 2, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var classification_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const classification_json = try oble_profile_classification.exportClassificationProfileBundleJson(source_db, classification_id, &classification_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    const imported_classification_id = try session.importClassificationProfileBundleJson(classification_json);

    try std.testing.expectEqual(imported_classification_id, session.resolveImportedId(.classification, "classification-1").?);
    try std.testing.expect(session.resolveImportedId(.classification_node, "classification-node-1") != null);
    try std.testing.expect(session.resolveImportedId(.classification_node, "classification-node-2") != null);
    try std.testing.expect(session.resolveImportedId(.classification_node, "classification-node-3") != null);

    var round_buf: [256 * 1024]u8 = undefined;
    const round_json = try oble_profile_classification.exportClassificationProfileBundleJson(target_db, imported_classification_id, &round_buf);
    try std.testing.expectEqualStrings(classification_json, round_json);
}

test "OBLE import session: dimension profile imports and resolves logical IDs" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Dimension Session Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(source_db, book_id, "SALE-001", "2026-01-10", "2026-01-10", "Sale", period_id, null, "admin");
    const cash_line_id = try entry_mod.Entry.addLine(source_db, entry_id, 1, 100_00_000_000, 0, "USD", 10_000_000_000, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(source_db, entry_id, 2, 0, 100_00_000_000, "USD", 10_000_000_000, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(source_db, entry_id, "admin");

    const dimension_mod = @import("dimension.zig");
    const dimension_id = try dimension_mod.Dimension.create(source_db, book_id, "Tax Code", .tax_code, "admin");
    const parent_value_id = try dimension_mod.DimensionValue.create(source_db, dimension_id, "VAT", "VAT", "admin");
    const child_value_id = try dimension_mod.DimensionValue.createWithParent(source_db, dimension_id, "VAT12", "VAT 12%", parent_value_id, "admin");
    try dimension_mod.LineDimension.assign(source_db, cash_line_id, child_value_id, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var entry_buf: [64 * 1024]u8 = undefined;
    var dimension_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const entry_json = try oble_core.exportEntryJson(source_db, entry_id, &entry_buf);
    const dimension_json = try oble_profile_dimension.exportDimensionProfileBundleJson(source_db, book_id, &dimension_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    _ = try session.importEntryJson(entry_json);
    try session.importDimensionProfileBundleJson(dimension_json);

    try std.testing.expect(session.resolveImportedId(.dimension, "dimension-1") != null);
    try std.testing.expect(session.resolveImportedId(.dimension_value, "dimension-value-1") != null);
    try std.testing.expect(session.resolveImportedId(.dimension_value, "dimension-value-2") != null);

    var round_buf: [256 * 1024]u8 = undefined;
    const imported_book_id = session.resolveImportedId(.book, "book-1").?;
    const round_json = try oble_profile_dimension.exportDimensionProfileBundleJson(target_db, imported_book_id, &round_buf);
    try std.testing.expectEqualStrings(dimension_json, round_json);
}

test "OBLE import session: budget profile imports and resolves logical IDs" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Budget Session Source", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(source_db, book_id, "4000", "Revenue", .revenue, false, "admin");
    const jan_id = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(source_db, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const budget_id = try @import("budget.zig").Budget.create(source_db, book_id, "FY2026 Plan", 2026, "admin");
    _ = try @import("budget.zig").BudgetLine.set(source_db, budget_id, cash_id, jan_id, 10_000_000_000, "admin");
    _ = try @import("budget.zig").BudgetLine.set(source_db, budget_id, revenue_id, feb_id, 25_000_000_000, "admin");
    try @import("budget.zig").Budget.transition(source_db, budget_id, .approved, "admin");

    var core_buf: [256 * 1024]u8 = undefined;
    var budget_buf: [256 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);
    const budget_json = try oble_profile_budget.exportBudgetProfileBundleJson(source_db, budget_id, &budget_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.init(target_db, std.testing.allocator, "admin");
    defer session.deinit();

    _ = try session.importCoreBundleJson(core_json);
    const imported_budget_id = try session.importBudgetProfileBundleJson(budget_json);

    try std.testing.expectEqual(imported_budget_id, session.resolveImportedId(.budget, "budget-1").?);
    try std.testing.expect(session.resolveImportedId(.budget_line, "budget-line-1") != null);
    try std.testing.expect(session.resolveImportedId(.budget_line, "budget-line-2") != null);

    var round_buf: [256 * 1024]u8 = undefined;
    const round_json = try oble_profile_budget.exportBudgetProfileBundleJson(target_db, imported_budget_id, &round_buf);
    try std.testing.expectEqualStrings(budget_json, round_json);
}

test "OBLE import session: max payload limit rejects oversized JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var session = Session.init(database, std.testing.allocator, "admin");
    defer session.deinit();
    try session.setMaxPayloadBytes(16);

    const oversized = "01234567890123456";
    try std.testing.expectError(error.PayloadTooLarge, session.importCoreBundleJson(oversized));
}

test "OBLE import session: per-kind id cap rejects oversized logical-id sets" {
    const source_db = try db.Database.open(":memory:");
    defer source_db.close();
    try schema.createAll(source_db);

    const book_id = try book_mod.Book.create(source_db, "Cap Source", "USD", 2, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(source_db, book_id, "2000", "Payable", .liability, false, "admin");
    _ = try period_mod.Period.create(source_db, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var core_buf: [128 * 1024]u8 = undefined;
    const core_json = try oble_core.exportCoreBundleJson(source_db, book_id, &core_buf);

    const target_db = try db.Database.open(":memory:");
    defer target_db.close();
    try schema.createAll(target_db);

    var session = Session.initWithLimits(
        target_db,
        std.testing.allocator,
        "admin",
        1,
        oble_import.DEFAULT_MAX_IMPORT_PAYLOAD_BYTES,
    );
    defer session.deinit();

    try std.testing.expectError(error.TooManyImportIds, session.importCoreBundleJson(core_json));
}
