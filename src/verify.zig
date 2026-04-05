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
            \\WHERE e.book_id = ? AND e.status IN ('posted', 'reversed')
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

    // Check 3a: Period count
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const pc = stmt.columnInt(0);
        result.periods_checked = if (pc >= 0) @as(u32, @intCast(pc)) else 0;
    }

    // Check 3b: Period date overlaps (regular periods only)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_periods p1
            \\JOIN ledger_periods p2 ON p1.book_id = p2.book_id AND p1.id < p2.id
            \\  AND p1.period_type = 'regular' AND p2.period_type = 'regular'
            \\  AND p1.start_date <= p2.end_date AND p2.start_date <= p1.end_date
            \\WHERE p1.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.warnings += 1;
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

    // Check 5a: Every posted/void/reversed entry has a 'post' audit record
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status IN ('posted', 'reversed', 'void')
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_audit_log al
            \\    WHERE al.entity_type = 'entry' AND al.entity_id = e.id
            \\      AND al.action = 'post'
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const missing = stmt.columnInt(0);
        if (missing > 0) result.warnings += @as(u32, @intCast(missing));
    }

    // Check 5b: Every void entry has a 'void' audit record
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status = 'void'
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_audit_log al
            \\    WHERE al.entity_type = 'entry' AND al.entity_id = e.id
            \\      AND al.action = 'void'
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const missing = stmt.columnInt(0);
        if (missing > 0) result.warnings += @as(u32, @intCast(missing));
    }

    // Check 5c: Every reversed entry has a 'reverse' audit record
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status = 'reversed'
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_audit_log al
            \\    WHERE al.entity_type = 'entry' AND al.entity_id = e.id
            \\      AND al.action = 'reverse'
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const missing = stmt.columnInt(0);
        if (missing > 0) result.warnings += @as(u32, @intCast(missing));
    }

    // Check 6: Orphaned cache entries (account deleted via raw SQL)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_account_balances ab
            \\WHERE ab.book_id = ?
            \\  AND ab.account_id NOT IN (SELECT id FROM ledger_accounts WHERE book_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.errors += 1;
    }

    // Check 7: Orphaned entry lines (entry deleted via raw SQL)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entry_lines el
            \\WHERE el.entry_id NOT IN (SELECT id FROM ledger_entries WHERE book_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.errors += 1;
    }

    // Check 8: Duplicate line numbers within an entry
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM (
            \\  SELECT el.entry_id, el.line_number, COUNT(*) as cnt
            \\  FROM ledger_entry_lines el
            \\  JOIN ledger_entries e ON e.id = el.entry_id
            \\  WHERE e.book_id = ? AND e.status = 'posted'
            \\  GROUP BY el.entry_id, el.line_number
            \\  HAVING cnt > 1
            \\);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.warnings += 1;
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
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, "PHP", money.FX_RATE_SCALE, debit_acct, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, "PHP", money.FX_RATE_SCALE, credit_acct, null, null, "admin");
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
    // Temporarily drop trigger to simulate audit gap (trigger prevents DELETE)
    try database.exec("DROP TRIGGER IF EXISTS protect_audit_log_delete;");
    try database.exec("DELETE FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'post';");
    try database.exec("CREATE TRIGGER protect_audit_log_delete BEFORE DELETE ON ledger_audit_log BEGIN SELECT RAISE(ABORT, 'audit log is immutable: DELETE not allowed'); END;");

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

test "verify: reversal checks both original and reversal entries" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    _ = try entry_mod.Entry.reverse(database, 1, "Correction", "2026-01-15", null, "admin");

    const result = try verify(database, 1);
    try std.testing.expectEqual(@as(u32, 2), result.entries_checked);
}

test "verify: corrupted reversal entry detected by check 1" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    const reversal_id = try entry_mod.Entry.reverse(database, 1, "Correction", "2026-01-15", null, "admin");

    const pre_result = try verify(database, 1);
    const pre_errors = pre_result.errors;

    {
        var stmt = try database.prepare("UPDATE ledger_entry_lines SET base_debit_amount = 999 WHERE entry_id = ? AND base_debit_amount > 0;");
        defer stmt.finalize();
        try stmt.bindInt(1, reversal_id);
        _ = try stmt.step();
    }

    const result = try verify(database, 1);
    try std.testing.expect(result.errors > pre_errors);
}

test "verify: cache mismatch from deleted account via raw SQL" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);

    try database.exec("PRAGMA foreign_keys = OFF;");
    try database.exec("UPDATE ledger_entry_lines SET account_id = 888 WHERE account_id = 1;");
    try database.exec("PRAGMA foreign_keys = ON;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
    try std.testing.expect(result.errors > 0);
}

test "verify: cache corruption from raw balance update" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try postEntry(database, "JE-002", 1, 2_000_000_000_00, 4, 2_000_000_000_00);

    try database.exec("UPDATE ledger_account_balances SET credit_sum = 12345 WHERE account_id = 3;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
    try std.testing.expect(result.errors > 0);
    try std.testing.expect(result.accounts_checked >= 2);
}

test "verify: multiple periods counted correctly" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Mar 2026", 3, 2026, "2026-03-01", "2026-03-31", "regular", "admin");

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 3), result.periods_checked);
}

test "verify: void entry missing all audit records triggers warning" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    try database.exec("DROP TRIGGER IF EXISTS protect_audit_log_delete;");
    try database.exec("DELETE FROM ledger_audit_log WHERE entity_type = 'entry' AND entity_id = 1;");
    try database.exec("CREATE TRIGGER protect_audit_log_delete BEFORE DELETE ON ledger_audit_log BEGIN SELECT RAISE(ABORT, 'audit log is immutable: DELETE not allowed'); END;");

    const result = try verify(database, 1);
    try std.testing.expect(result.warnings > 0);
}

test "verify: reversed entry missing all audit records triggers warning" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    _ = try entry_mod.Entry.reverse(database, 1, "Correction", "2026-01-15", null, "admin");

    try database.exec("DROP TRIGGER IF EXISTS protect_audit_log_delete;");
    try database.exec("DELETE FROM ledger_audit_log WHERE entity_type = 'entry' AND entity_id = 1;");
    try database.exec("CREATE TRIGGER protect_audit_log_delete BEFORE DELETE ON ledger_audit_log BEGIN SELECT RAISE(ABORT, 'audit log is immutable: DELETE not allowed'); END;");

    const result = try verify(database, 1);
    try std.testing.expect(result.warnings > 0);
}

test "verify: FK integrity detects account from wrong book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try book_mod.Book.create(database, "Other", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "9000", "Foreign", .asset, false, "admin");

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);

    try database.exec("PRAGMA foreign_keys = OFF;");
    try database.exec("UPDATE ledger_entry_lines SET account_id = 6 WHERE id = 1;");
    try database.exec("PRAGMA foreign_keys = ON;");

    const result = try verify(database, 1);
    try std.testing.expect(!result.passed());
    try std.testing.expect(result.errors > 0);
}

test "verify: clean book with posts and void passes all checks" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    try postEntry(database, "JE-002", 5, 2_000_000_000_00, 4, 2_000_000_000_00);

    try entry_mod.Entry.voidEntry(database, 2, "Duplicate", "admin");

    const result = try verify(database, 1);
    try std.testing.expectEqual(@as(u32, 0), result.errors);
    try std.testing.expectEqual(@as(u32, 0), result.warnings);
    try std.testing.expectEqual(@as(u32, 1), result.entries_checked);
}

test "verify: stale cache flag does not cause verify failure" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);

    try database.exec("UPDATE ledger_account_balances SET is_stale = 1 WHERE book_id = 1;");

    const result = try verify(database, 1);
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.errors);
}

test "verify: duplicate line numbers detected" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-001", 1, 1_000_000_000_00, 3, 1_000_000_000_00);

    // Remove UNIQUE(entry_id, line_number) constraint by recreating the table
    // to simulate data corruption that Check 8 should detect
    try database.exec("PRAGMA foreign_keys = OFF;");
    try database.exec("ALTER TABLE ledger_entry_lines RENAME TO ledger_entry_lines_old;");
    try database.exec(
        \\CREATE TABLE ledger_entry_lines (
        \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
        \\  line_number INTEGER NOT NULL,
        \\  debit_amount INTEGER NOT NULL DEFAULT 0,
        \\  credit_amount INTEGER NOT NULL DEFAULT 0,
        \\  base_debit_amount INTEGER NOT NULL DEFAULT 0,
        \\  base_credit_amount INTEGER NOT NULL DEFAULT 0,
        \\  fx_rate INTEGER NOT NULL DEFAULT 10000000000,
        \\  transaction_currency TEXT NOT NULL,
        \\  description TEXT,
        \\  quantity INTEGER,
        \\  unit_type TEXT,
        \\  counterparty_id INTEGER,
        \\  account_id INTEGER NOT NULL,
        \\  entry_id INTEGER NOT NULL,
        \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        \\);
    );
    try database.exec(
        \\INSERT INTO ledger_entry_lines
        \\  SELECT * FROM ledger_entry_lines_old;
    );
    try database.exec("DROP TABLE ledger_entry_lines_old;");

    // Now insert a duplicate line_number for the same entry
    try database.exec(
        \\INSERT INTO ledger_entry_lines
        \\  (line_number, debit_amount, credit_amount, transaction_currency,
        \\   fx_rate, account_id, entry_id, base_debit_amount, base_credit_amount)
        \\VALUES (1, 100000000, 0, 'PHP', 10000000000, 1, 1, 100000000, 0);
    );
    try database.exec("PRAGMA foreign_keys = ON;");

    const result = try verify(database, 1);
    try std.testing.expect(result.warnings > 0);
}
