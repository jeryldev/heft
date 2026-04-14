const std = @import("std");
const heft = @import("heft");
const common = @import("abi_common.zig");

const LedgerDB = common.LedgerDB;

pub const LedgerOBLEImportSession = struct {
    parent: *LedgerDB,
    session: heft.oble_import_session.Session,
    performed_by_owned: []u8,
};

const EntityKind = heft.oble_import_session.EntityKind;

fn kindFromInt(kind: i32) ?EntityKind {
    return switch (kind) {
        1 => .book,
        2 => .account,
        3 => .period,
        4 => .entry,
        5 => .line,
        6 => .counterparty,
        7 => .open_item,
        else => null,
    };
}

pub fn ledger_oble_import_session_open(handle: ?*LedgerDB, performed_by: ?[*:0]const u8) ?*LedgerOBLEImportSession {
    const h = handle orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };
    const actor = performed_by orelse {
        common.setError(common.mapError(error.InvalidInput));
        return null;
    };

    const performed_by_slice = std.mem.span(actor);
    const performed_by_owned = std.heap.c_allocator.dupe(u8, performed_by_slice) catch {
        common.setError(common.mapError(error.OutOfMemory));
        return null;
    };
    errdefer std.heap.c_allocator.free(performed_by_owned);

    const import_session = std.heap.c_allocator.create(LedgerOBLEImportSession) catch {
        common.setError(common.mapError(error.OutOfMemory));
        return null;
    };
    errdefer std.heap.c_allocator.destroy(import_session);

    import_session.* = .{
        .parent = h,
        .session = heft.oble_import_session.Session.init(h.sqlite, std.heap.c_allocator, performed_by_owned),
        .performed_by_owned = performed_by_owned,
    };
    return import_session;
}

pub fn ledger_oble_import_session_close(session: ?*LedgerOBLEImportSession) void {
    const s = session orelse return;
    s.session.deinit();
    std.heap.c_allocator.destroy(s);
}

pub fn ledger_oble_import_core_bundle(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) i64 {
    const s = session orelse return common.invalidHandleI64();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return s.session.importCoreBundleJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_oble_import_entry(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) i64 {
    const s = session orelse return common.invalidHandleI64();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return s.session.importEntryJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_oble_import_reversal_pair(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) bool {
    const s = session orelse return common.invalidHandleBool();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    _ = s.session.importReversalPairJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_oble_import_counterparties(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) bool {
    const s = session orelse return common.invalidHandleBool();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    s.session.importCounterpartiesJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_oble_import_counterparty_profile_bundle(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) bool {
    const s = session orelse return common.invalidHandleBool();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return false;
    };
    s.session.importCounterpartyProfileBundleJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return false;
    };
    return true;
}

pub fn ledger_oble_import_policy_profile(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) i64 {
    const s = session orelse return common.invalidHandleI64();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return s.session.importPolicyProfileJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_oble_import_book_snapshot(session: ?*LedgerOBLEImportSession, json: ?[*:0]const u8) i64 {
    const s = session orelse return common.invalidHandleI64();
    const payload = json orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return s.session.importBookSnapshotJson(std.mem.span(payload)) catch |err| {
        common.setError(common.mapError(err));
        return -1;
    };
}

pub fn ledger_oble_import_resolve_id(session: ?*LedgerOBLEImportSession, entity_kind: i32, logical_id: ?[*:0]const u8) i64 {
    const s = session orelse return common.invalidHandleI64();
    const kind = kindFromInt(entity_kind) orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    const logical = logical_id orelse {
        common.setError(common.mapError(error.InvalidInput));
        return -1;
    };
    return s.session.resolveImportedId(kind, std.mem.span(logical)) orelse blk: {
        common.setError(common.mapError(error.NotFound));
        break :blk -1;
    };
}
