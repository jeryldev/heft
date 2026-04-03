// Heft — The embedded accounting engine
// Copyright (C) 2026 Jeryl Donato Estopace
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// schema.zig: DDL for all ledger tables, indexes, and views.
// All tables prefixed with "ledger_" to coexist with application tables.
//
// Storage conventions:
//   Amounts:    INTEGER scaled by 10^8  (10,000.50 → 1000050000000)
//   FX rates:   INTEGER scaled by 10^10 (1.0 → 10000000000)
//   Timestamps: TEXT in UTC ISO 8601    ("2026-04-03T14:30:00Z")
//   Dates:      TEXT in ISO 8601        ("2026-04-03")

const std = @import("std");
const db = @import("db.zig");

/// Create all ledger tables, indexes, and views.
/// Safe to call multiple times — uses IF NOT EXISTS.
pub fn createAll(database: db.Database) !void {
    try database.beginTransaction();
    errdefer database.rollback();

    for (tables) |ddl| try database.exec(ddl);
    for (indexes) |idx| try database.exec(idx);
    for (views) |v| try database.exec(v);

    try database.commit();
}

// ── Tables (11) ─────────────────────────────────────────────────
// Order matters — foreign keys reference earlier tables.

const tables = [_][*:0]const u8{

    // ── 1. Books ────────────────────────────────────────────────
    // The top-level container. One book per business/entity/fund.
    // base_currency and decimal_places are IMMUTABLE after creation —
    // changing them would invalidate every computed balance.
    \\CREATE TABLE IF NOT EXISTS ledger_books (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL,
    \\  base_currency TEXT NOT NULL,
    \\  decimal_places INTEGER NOT NULL DEFAULT 2,
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'archived')),
    \\  rounding_account_id INTEGER,
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    \\);
    ,

    // ── 2. Accounts ─────────────────────────────────────────────
    // Flat chart of accounts. Five types. normal_balance auto-derived:
    //   asset, expense → debit
    //   liability, equity, revenue → credit
    //   is_contra flips the normal balance
    \\CREATE TABLE IF NOT EXISTS ledger_accounts (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  number TEXT NOT NULL,
    \\  name TEXT NOT NULL,
    \\  account_type TEXT NOT NULL
    \\    CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    \\  normal_balance TEXT NOT NULL
    \\    CHECK (normal_balance IN ('debit', 'credit')),
    \\  is_contra INTEGER NOT NULL DEFAULT 0,
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'inactive', 'archived')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, number)
    \\);
    ,

    // ── 3. Periods ──────────────────────────────────────────────
    // Time boundaries for posting control.
    // period_number 1-12 = regular months, 13-16 = adjustment periods.
    // Status lifecycle: open → soft_closed → closed → locked
    \\CREATE TABLE IF NOT EXISTS ledger_periods (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL,
    \\  period_number INTEGER NOT NULL
    \\    CHECK (period_number BETWEEN 1 AND 16),
    \\  year INTEGER NOT NULL,
    \\  start_date TEXT NOT NULL,
    \\  end_date TEXT NOT NULL,
    \\  period_type TEXT NOT NULL DEFAULT 'regular'
    \\    CHECK (period_type IN ('regular', 'adjustment')),
    \\  status TEXT NOT NULL DEFAULT 'open'
    \\    CHECK (status IN ('open', 'soft_closed', 'closed', 'locked')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, period_number, year),
    \\  CHECK (end_date >= start_date)
    \\);
    ,

    // ── 4. Classifications ──────────────────────────────────────
    // Named grouping schemes for reports. Multiple per book.
    // report_type constrains which accounts can appear in the tree.
    \\CREATE TABLE IF NOT EXISTS ledger_classifications (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL,
    \\  report_type TEXT NOT NULL
    \\    CHECK (report_type IN ('balance_sheet', 'income_statement', 'trial_balance')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,

    // ── 5. Classification Nodes ─────────────────────────────────
    // Tree structure. Each node is a group (container) or account (leaf).
    // parent_id = self-reference for tree hierarchy.
    // position = sibling ordering. depth = cached tree level (0 = root).
    \\CREATE TABLE IF NOT EXISTS ledger_classification_nodes (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  node_type TEXT NOT NULL
    \\    CHECK (node_type IN ('group', 'account')),
    \\  label TEXT,
    \\  parent_id INTEGER REFERENCES ledger_classification_nodes(id),
    \\  account_id INTEGER REFERENCES ledger_accounts(id),
    \\  position INTEGER NOT NULL DEFAULT 0,
    \\  depth INTEGER NOT NULL DEFAULT 0,
    \\  classification_id INTEGER NOT NULL REFERENCES ledger_classifications(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    \\);
    ,

    // ── 6. Subledger Groups ─────────────────────────────────────
    // Links counterparties to a GL control account.
    // When enabled, engine enforces: no direct posting to control account.
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_groups (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL,
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier')),
    \\  group_number INTEGER NOT NULL,
    \\  number_range_start TEXT,
    \\  number_range_end TEXT,
    \\  gl_account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, type, group_number)
    \\);
    ,

    // ── 7. Subledger Accounts ───────────────────────────────────
    // Thin accounting identity. No business data (address, tax_id) —
    // that lives in the application layer.
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_accounts (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  number TEXT NOT NULL,
    \\  name TEXT NOT NULL,
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier', 'both')),
    \\  group_id INTEGER NOT NULL REFERENCES ledger_subledger_groups(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, number)
    \\);
    ,

    // ── 8. Entries (Journal Entries) ────────────────────────────
    // The transaction header. Dual dating: transaction_date (when it happened,
    // IAS 21 spot rate) vs posting_date (when it hits the books, determines period).
    // Status: draft → posted → reversed|void
    \\CREATE TABLE IF NOT EXISTS ledger_entries (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  document_number TEXT NOT NULL,
    \\  transaction_date TEXT NOT NULL,
    \\  posting_date TEXT NOT NULL,
    \\  description TEXT,
    \\  status TEXT NOT NULL DEFAULT 'draft'
    \\    CHECK (status IN ('draft', 'posted', 'reversed', 'void')),
    \\  void_reason TEXT,
    \\  reversed_reason TEXT,
    \\  reverses_entry_id INTEGER REFERENCES ledger_entries(id),
    \\  posted_at TEXT,
    \\  posted_by TEXT,
    \\  metadata TEXT,
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, document_number)
    \\);
    ,

    // ── 9. Entry Lines ──────────────────────────────────────────
    // The atomic accounting data. Multi-currency at the line level.
    // Exactly one of debit_amount or credit_amount must be > 0 (CHECK enforced).
    // base_* amounts are engine-computed: transaction_amount × fx_rate.
    // All amounts: INTEGER scaled by 10^8.
    // fx_rate: INTEGER scaled by 10^10. Default 10000000000 = 1.0.
    \\CREATE TABLE IF NOT EXISTS ledger_entry_lines (
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
    \\  counterparty_id INTEGER REFERENCES ledger_subledger_accounts(id),
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  entry_id INTEGER NOT NULL REFERENCES ledger_entries(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (entry_id, line_number),
    \\  CHECK ((debit_amount > 0 AND credit_amount = 0)
    \\      OR (debit_amount = 0 AND credit_amount > 0))
    \\);
    ,

    // ── 10. Account Balances (Cache) ────────────────────────────
    // Pre-computed balance per account per period. Updated atomically
    // in the same transaction as entry posting. Staleness-tracked:
    // when a period balance is stale, the engine recalculates from
    // lines before returning it to the caller. Self-healing.
    \\CREATE TABLE IF NOT EXISTS ledger_account_balances (
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  debit_sum INTEGER NOT NULL DEFAULT 0,
    \\  credit_sum INTEGER NOT NULL DEFAULT 0,
    \\  balance INTEGER NOT NULL DEFAULT 0,
    \\  entry_count INTEGER NOT NULL DEFAULT 0,
    \\  is_stale INTEGER NOT NULL DEFAULT 0,
    \\  stale_since TEXT,
    \\  last_recalculated_at TEXT,
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  PRIMARY KEY (account_id, period_id)
    \\);
    ,

    // ── 11. Audit Log ───────────────────────────────────────────
    // Append-only change log. Every mutation writes here in the same
    // SQLite transaction. Required for GoBD (Germany), UGB/BAO (Austria),
    // BIR (Philippines), ACRA (Singapore) compliance.
    // NO updates. NO deletes. EVER.
    \\CREATE TABLE IF NOT EXISTS ledger_audit_log (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  entity_type TEXT NOT NULL,
    \\  entity_id INTEGER NOT NULL,
    \\  action TEXT NOT NULL,
    \\  field_changed TEXT,
    \\  old_value TEXT,
    \\  new_value TEXT,
    \\  performed_by TEXT NOT NULL,
    \\  performed_at TEXT NOT NULL
    \\    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id)
    \\);
    ,
};

// ── Indexes (10) ────────────────────────────────────────────────

const indexes = [_][*:0]const u8{
    // Primary query path: find posted entries by book and date
    \\CREATE INDEX IF NOT EXISTS idx_entries_book_status_date
    \\  ON ledger_entries (book_id, status, posting_date);
    ,
    // Balance computation: aggregate lines by account
    \\CREATE INDEX IF NOT EXISTS idx_lines_account
    \\  ON ledger_entry_lines (account_id);
    ,
    // Journal view: lines by entry
    \\CREATE INDEX IF NOT EXISTS idx_lines_entry
    \\  ON ledger_entry_lines (entry_id, line_number);
    ,
    // Subledger: lines by counterparty (partial index — only non-null)
    \\CREATE INDEX IF NOT EXISTS idx_lines_counterparty
    \\  ON ledger_entry_lines (counterparty_id)
    \\  WHERE counterparty_id IS NOT NULL;
    ,
    // Balance cache lookup
    \\CREATE INDEX IF NOT EXISTS idx_balances_account_period
    \\  ON ledger_account_balances (account_id, period_id);
    ,
    // Classification tree traversal
    \\CREATE INDEX IF NOT EXISTS idx_class_nodes_tree
    \\  ON ledger_classification_nodes (classification_id, parent_id, position);
    ,
    // Account lookup by book and number
    \\CREATE INDEX IF NOT EXISTS idx_accounts_book_number
    \\  ON ledger_accounts (book_id, number);
    ,
    // Period lookup by book and dates
    \\CREATE INDEX IF NOT EXISTS idx_periods_book_dates
    \\  ON ledger_periods (book_id, start_date, end_date);
    ,
    // Audit log: find changes for a specific entity
    \\CREATE INDEX IF NOT EXISTS idx_audit_log_entity
    \\  ON ledger_audit_log (entity_type, entity_id);
    ,
    // Audit log: find changes by book and time
    \\CREATE INDEX IF NOT EXISTS idx_audit_log_book_date
    \\  ON ledger_audit_log (book_id, performed_at);
    ,
};

// ── Views (1) ───────────────────────────────────────────────────

const views = [_][*:0]const u8{
    // The "super query" — pre-joins all tables. Every report queries this
    // view instead of writing raw JOINs. Zero storage cost (SQLite evaluates
    // on demand). Only includes posted entries.
    \\CREATE VIEW IF NOT EXISTS ledger_transaction_history AS
    \\SELECT
    \\  l.id AS line_id, l.line_number,
    \\  l.debit_amount, l.credit_amount,
    \\  l.base_debit_amount, l.base_credit_amount,
    \\  l.fx_rate, l.transaction_currency,
    \\  l.description AS line_description,
    \\  l.quantity, l.unit_type,
    \\  l.account_id, l.entry_id, l.counterparty_id,
    \\  e.document_number, e.transaction_date, e.posting_date,
    \\  e.description AS entry_description, e.status AS entry_status,
    \\  e.metadata, e.posted_at, e.posted_by,
    \\  e.reverses_entry_id, e.void_reason, e.reversed_reason,
    \\  e.book_id, e.period_id,
    \\  a.number AS account_number, a.name AS account_name,
    \\  a.account_type, a.normal_balance, a.is_contra,
    \\  a.status AS account_status,
    \\  p.name AS period_name, p.period_number,
    \\  p.year AS period_year, p.start_date AS period_start_date,
    \\  p.end_date AS period_end_date, p.period_type,
    \\  p.status AS period_status,
    \\  sa.number AS counterparty_number, sa.name AS counterparty_name,
    \\  sa.type AS counterparty_type,
    \\  sg.id AS subledger_group_id, sg.name AS subledger_group_name,
    \\  sg.gl_account_id AS control_account_id
    \\FROM ledger_entry_lines l
    \\INNER JOIN ledger_entries e ON e.id = l.entry_id
    \\INNER JOIN ledger_accounts a ON a.id = l.account_id
    \\INNER JOIN ledger_periods p ON p.id = e.period_id
    \\LEFT JOIN ledger_subledger_accounts sa ON sa.id = l.counterparty_id
    \\LEFT JOIN ledger_subledger_groups sg ON sg.id = sa.group_id
    \\WHERE e.status = 'posted';
    ,
};

// ── Tests ───────────────────────────────────────────────────────

test "createAll creates 11 tables" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'ledger_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 11), stmt.columnInt(0));
}

test "createAll creates 10 indexes" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 10), stmt.columnInt(0));
}

test "createAll creates transaction history view" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='ledger_transaction_history';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "createAll is idempotent" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);
    try createAll(database); // second call should not error
}

test "entry_lines CHECK constraint enforces debit XOR credit" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    // Insert a book, account, period, and entry to satisfy FKs
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );

    // Valid: debit > 0, credit = 0
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1);
    );

    // Invalid: both zero — should fail CHECK constraint
    const both_zero = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (2, 0, 0, 0, 'PHP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, both_zero);
}

test "periods CHECK constraint enforces end_date >= start_date" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    // Invalid: end_date < start_date
    const bad_dates = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Bad', 1, 2026, '2026-01-31', '2026-01-01', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_dates);
}

test "accounts UNIQUE constraint enforces unique number per book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );

    // Duplicate number in same book — should fail
    const dupe = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Also Cash', 'asset', 'debit', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "foreign key constraint rejects invalid account_id" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );

    // account_id 999 does not exist — FK should reject
    const bad_fk = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 999, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "each table exists with correct name" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const expected_tables = [_][]const u8{
        "ledger_books",
        "ledger_accounts",
        "ledger_periods",
        "ledger_classifications",
        "ledger_classification_nodes",
        "ledger_subledger_groups",
        "ledger_subledger_accounts",
        "ledger_entries",
        "ledger_entry_lines",
        "ledger_account_balances",
        "ledger_audit_log",
    };

    for (expected_tables) |table_name| {
        var stmt = try database.prepare(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;",
        );
        defer stmt.finalize();
        try stmt.bindText(1, table_name);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }
}
