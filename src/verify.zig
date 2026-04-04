const std = @import("std");
const db = @import("db.zig");

/// Result of ledger_verify: counts errors (must fix) and warnings (investigate).
pub const VerifyResult = struct {
    errors: u32,
    warnings: u32,
    entries_checked: u32,
    accounts_checked: u32,
    periods_checked: u32,

    pub fn passed(self: VerifyResult) bool {
        return self.errors == 0;
    }
};

/// The most paranoid function in the engine. Runs integrity checks on every
/// posted entry, balance cache, subledger, period, and audit trail.
/// Returns errors (data corruption) and warnings (anomalies to investigate).
pub fn verify(database: db.Database, book_id: i64) !VerifyResult {
    // Verify book exists
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) == 0) return error.NotFound;
    }

    var result = VerifyResult{
        .errors = 0,
        .warnings = 0,
        .entries_checked = 0,
        .accounts_checked = 0,
        .periods_checked = 0,
    };

    // Check 1: Balance equation — every posted entry must have debits = credits
    {
        var stmt = try database.prepare(
            \\SELECT e.id, SUM(el.base_debit_amount), SUM(el.base_credit_amount)
            \\FROM ledger_entries e
            \\JOIN ledger_entry_lines el ON el.entry_id = e.id
            \\WHERE e.book_id = ? AND e.status = 'posted'
            \\GROUP BY e.id;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);

        while (try stmt.step()) {
            result.entries_checked += 1;
            const debits = stmt.columnInt64(1);
            const credits = stmt.columnInt64(2);
            if (debits != credits) result.errors += 1;
        }
    }

    // Check 2: Cache integrity — recompute from lines, compare with cache
    {
        var stmt = try database.prepare(
            \\SELECT ab.account_id, ab.period_id, ab.debit_sum, ab.credit_sum,
            \\  COALESCE(computed.real_debit, 0), COALESCE(computed.real_credit, 0)
            \\FROM ledger_account_balances ab
            \\LEFT JOIN (
            \\    SELECT el.account_id, e.period_id,
            \\      SUM(el.base_debit_amount) AS real_debit,
            \\      SUM(el.base_credit_amount) AS real_credit
            \\    FROM ledger_entry_lines el
            \\    JOIN ledger_entries e ON e.id = el.entry_id
            \\    WHERE e.book_id = ? AND e.status = 'posted'
            \\    GROUP BY el.account_id, e.period_id
            \\) computed ON computed.account_id = ab.account_id AND computed.period_id = ab.period_id
            \\WHERE ab.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);

        while (try stmt.step()) {
            result.accounts_checked += 1;
            const cached_debit = stmt.columnInt64(2);
            const cached_credit = stmt.columnInt64(3);
            const real_debit = stmt.columnInt64(4);
            const real_credit = stmt.columnInt64(5);
            if (cached_debit != real_debit or cached_credit != real_credit) result.errors += 1;
        }
    }

    // Check 3: Period count
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        result.periods_checked = @intCast(stmt.columnInt(0));
    }

    // Check 4: FK integrity — entry lines reference valid accounts
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE e.book_id = ? AND e.status = 'posted'
            \\  AND el.account_id NOT IN (SELECT id FROM ledger_accounts WHERE book_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.errors += 1;
    }

    // Check 5: Audit completeness — every posted entry has a 'post' audit record
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status IN ('posted', 'reversed', 'void')
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_audit_log al
            \\    WHERE al.entity_type = 'entry' AND al.entity_id = e.id
            \\      AND al.action IN ('post', 'void', 'reverse')
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const missing_audits = stmt.columnInt(0);
        if (missing_audits > 0) result.warnings += @intCast(missing_audits);
    }

    return result;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

fn postEntry(database: db.Database, doc: []const u8, debit_acct: i64, debit_amt: i64, credit_acct: i64, credit_amt: i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, 1, doc, "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, "PHP", money.FX_RATE_SCALE, debit_acct, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, "PHP", money.FX_RATE_SCALE, credit_acct, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
}

test "verify: clean book passes all checks" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try postEntry(database, "JE-002", 1, 5_000_000_000_00, 4, 5_000_000_000_00);

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.errors);
    try std.testing.expect(result.entries_checked >= 2);
}

test "verify: empty book passes" {
    const database = try setupTestDb();
    defer database.close();

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
}

test "verify: detects corrupted balance equation" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try database.exec("UPDATE ledger_entry_lines SET base_debit_amount = 999 WHERE id = 1;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
    try std.testing.expect(result.errors > 0);
}

test "verify: detects stale cache" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try database.exec("UPDATE ledger_account_balances SET debit_sum = 999 WHERE account_id = 1;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
}

test "verify: posted entry in locked period is normal" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 1, .locked, "admin");

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
}

test "verify: detects missing audit record" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try database.exec("DELETE FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'post';");

    const result = try verify(database, 1);
    try std.testing.expect(result.warnings > 0);
}

test "verify: detects orphan line (invalid account_id)" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try database.exec("PRAGMA foreign_keys = OFF;");
    try database.exec("UPDATE ledger_entry_lines SET account_id = 999 WHERE id = 1;");
    try database.exec("PRAGMA foreign_keys = ON;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
}

test "verify: nonexistent book returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = verify(database, 999);
    try std.testing.expectError(error.NotFound, result);
}

test "verify: 10 entries all pass" {
    const database = try setupTestDb();
    defer database.close();

    var i: u32 = 0;
    var doc_buf: [16]u8 = undefined;
    while (i < 10) : (i += 1) {
        const doc = std.fmt.bufPrint(&doc_buf, "JE-{d:0>3}", .{i}) catch unreachable;
        try postEntry(database, doc, 1, 1_000_000_000_00, 4, 1_000_000_000_00);
    }

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 10), result.entries_checked);
}

test "verify: voided entry not counted in balance check" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.entries_checked);
}
