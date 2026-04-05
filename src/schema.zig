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

pub const SCHEMA_VERSION: i32 = 5;

pub fn migrate(database: db.Database, from_version: i32) !void {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    if (from_version < 5) {
        database.exec("ALTER TABLE ledger_audit_log ADD COLUMN hash_chain TEXT;") catch {};
        database.exec("ALTER TABLE ledger_dimension_values ADD COLUMN parent_value_id INTEGER REFERENCES ledger_dimension_values(id);") catch {};
    }

    const version_pragma = comptime std.fmt.comptimePrint("PRAGMA user_version = {d};", .{SCHEMA_VERSION});
    try database.exec(version_pragma);

    if (owns_txn) try database.commit();
}

pub fn createAll(database: db.Database) !void {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    for (tables) |ddl| try database.exec(ddl);
    for (indexes) |idx| try database.exec(idx);
    for (views) |v| try database.exec(v);
    for (triggers) |trg| try database.exec(trg);
    const version_pragma = comptime std.fmt.comptimePrint("PRAGMA user_version = {d};", .{SCHEMA_VERSION});
    try database.exec(version_pragma);

    if (owns_txn) try database.commit();
}

// ── Tables (16) ─────────────────────────────────────────────────
// Order matters — foreign keys reference earlier tables.

const tables = [_][*:0]const u8{

    // ── 1. Books ────────────────────────────────────────────────
    // The top-level container. One book per business/entity/fund.
    // base_currency and decimal_places are IMMUTABLE after creation —
    // changing them would invalidate every computed balance.
    \\CREATE TABLE IF NOT EXISTS ledger_books (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  base_currency TEXT NOT NULL
    \\    CHECK (length(base_currency) = 3),
    \\  decimal_places INTEGER NOT NULL DEFAULT 2
    \\    CHECK (decimal_places BETWEEN 0 AND 8),
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'archived')),
    \\  rounding_account_id INTEGER,
    \\  fx_gain_loss_account_id INTEGER,
    \\  retained_earnings_account_id INTEGER,
    \\  income_summary_account_id INTEGER,
    \\  opening_balance_account_id INTEGER,
    \\  suspense_account_id INTEGER,
    \\  require_approval INTEGER NOT NULL DEFAULT 0
    \\    CHECK (require_approval IN (0, 1)),
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
    \\  number TEXT NOT NULL
    \\    CHECK (length(number) BETWEEN 1 AND 50),
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  account_type TEXT NOT NULL
    \\    CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    \\  normal_balance TEXT NOT NULL
    \\    CHECK (normal_balance IN ('debit', 'credit')),
    \\  is_contra INTEGER NOT NULL DEFAULT 0
    \\    CHECK (is_contra IN (0, 1)),
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
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 100),
    \\  period_number INTEGER NOT NULL
    \\    CHECK (period_number BETWEEN 1 AND 16),
    \\  year INTEGER NOT NULL,
    \\  start_date TEXT NOT NULL
    \\    CHECK (length(start_date) = 10 AND date(start_date) IS NOT NULL),
    \\  end_date TEXT NOT NULL
    \\    CHECK (length(end_date) = 10 AND date(end_date) IS NOT NULL),
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
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  report_type TEXT NOT NULL
    \\    CHECK (report_type IN ('balance_sheet', 'income_statement', 'trial_balance', 'cash_flow')),
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
    \\  label TEXT
    \\    CHECK (label IS NULL OR length(label) <= 255),
    \\  parent_id INTEGER REFERENCES ledger_classification_nodes(id),
    \\  account_id INTEGER REFERENCES ledger_accounts(id),
    \\  position INTEGER NOT NULL DEFAULT 0,
    \\  depth INTEGER NOT NULL DEFAULT 0,
    \\  classification_id INTEGER NOT NULL REFERENCES ledger_classifications(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  CHECK ((node_type = 'group' AND label IS NOT NULL) OR (node_type = 'account' AND account_id IS NOT NULL))
    \\);
    ,

    // ── 6. Subledger Groups ─────────────────────────────────────
    // Links counterparties to a GL control account.
    // When enabled, engine enforces: no direct posting to control account.
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_groups (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier')),
    \\  group_number INTEGER NOT NULL,
    \\  number_range_start TEXT
    \\    CHECK (number_range_start IS NULL OR length(number_range_start) <= 50),
    \\  number_range_end TEXT
    \\    CHECK (number_range_end IS NULL OR length(number_range_end) <= 50),
    \\  gl_account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, type, group_number),
    \\  CHECK (number_range_start IS NULL OR number_range_end IS NULL OR number_range_start <= number_range_end)
    \\);
    ,

    // ── 7. Subledger Accounts ───────────────────────────────────
    // Thin accounting identity. No business data (address, tax_id) —
    // that lives in the application layer.
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_accounts (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  number TEXT NOT NULL
    \\    CHECK (length(number) BETWEEN 1 AND 50),
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier', 'both')),
    \\  group_id INTEGER NOT NULL REFERENCES ledger_subledger_groups(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'inactive', 'archived')),
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
    \\  document_number TEXT NOT NULL
    \\    CHECK (length(document_number) BETWEEN 1 AND 100),
    \\  transaction_date TEXT NOT NULL
    \\    CHECK (length(transaction_date) = 10 AND date(transaction_date) IS NOT NULL),
    \\  posting_date TEXT NOT NULL
    \\    CHECK (length(posting_date) = 10 AND date(posting_date) IS NOT NULL),
    \\  description TEXT
    \\    CHECK (description IS NULL OR length(description) <= 1000),
    \\  status TEXT NOT NULL DEFAULT 'draft'
    \\    CHECK (status IN ('draft', 'posted', 'reversed', 'void')),
    \\  void_reason TEXT
    \\    CHECK (void_reason IS NULL OR length(void_reason) <= 500),
    \\  reversed_reason TEXT
    \\    CHECK (reversed_reason IS NULL OR length(reversed_reason) <= 500),
    \\  reverses_entry_id INTEGER REFERENCES ledger_entries(id),
    \\  posted_at TEXT,
    \\  posted_by TEXT
    \\    CHECK (posted_by IS NULL OR length(posted_by) <= 100),
    \\  metadata TEXT
    \\    CHECK (metadata IS NULL OR length(metadata) <= 10000),
    \\  approval_status TEXT NOT NULL DEFAULT 'none'
    \\    CHECK (approval_status IN ('none', 'pending', 'approved', 'rejected')),
    \\  approved_by TEXT
    \\    CHECK (approved_by IS NULL OR length(approved_by) <= 100),
    \\  approved_at TEXT,
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, document_number),
    \\  CHECK (reverses_entry_id IS NULL OR reverses_entry_id != id)
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
    \\  fx_rate INTEGER NOT NULL DEFAULT 10000000000
    \\    CHECK (fx_rate > 0),
    \\  transaction_currency TEXT NOT NULL
    \\    CHECK (length(transaction_currency) = 3),
    \\  description TEXT
    \\    CHECK (description IS NULL OR length(description) <= 1000),
    \\  quantity INTEGER,
    \\  unit_type TEXT
    \\    CHECK (unit_type IS NULL OR length(unit_type) <= 50),
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
    \\  is_stale INTEGER NOT NULL DEFAULT 0
    \\    CHECK (is_stale IN (0, 1)),
    \\  stale_since TEXT,
    \\  last_recalculated_at TEXT,
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  PRIMARY KEY (account_id, period_id)
    \\);
    ,

    // ── 11. Dimensions ──────────────────────────────────────────
    // Universal tagging infrastructure: tax codes, cost centers,
    // departments, projects, segments. Any industry or jurisdiction
    // can tag entry lines without engine changes.
    \\CREATE TABLE IF NOT EXISTS ledger_dimensions (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 100),
    \\  dimension_type TEXT NOT NULL
    \\    CHECK (dimension_type IN ('tax_code', 'cost_center', 'department', 'project', 'segment', 'custom')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,

    // ── 12. Dimension Values ───────────────────────────────────
    // Codes within a dimension (e.g. "VAT12" under tax_code).
    \\CREATE TABLE IF NOT EXISTS ledger_dimension_values (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  code TEXT NOT NULL
    \\    CHECK (length(code) BETWEEN 1 AND 50),
    \\  label TEXT NOT NULL
    \\    CHECK (length(label) BETWEEN 1 AND 255),
    \\  dimension_id INTEGER NOT NULL REFERENCES ledger_dimensions(id),
    \\  parent_value_id INTEGER REFERENCES ledger_dimension_values(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (dimension_id, code)
    \\);
    ,

    // ── 13. Line Dimensions ────────────────────────────────────
    // Many-to-many: entry lines tagged with dimension values.
    \\CREATE TABLE IF NOT EXISTS ledger_line_dimensions (
    \\  line_id INTEGER NOT NULL REFERENCES ledger_entry_lines(id),
    \\  dimension_value_id INTEGER NOT NULL REFERENCES ledger_dimension_values(id),
    \\  PRIMARY KEY (line_id, dimension_value_id)
    \\);
    ,

    // ── 14. Budgets ─────────────────────────────────────────────
    // Budget headers. One budget per book+name. Informational only —
    // budgets do not block posting or affect balance cache.
    \\CREATE TABLE IF NOT EXISTS ledger_budgets (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  fiscal_year INTEGER NOT NULL,
    \\  status TEXT NOT NULL DEFAULT 'draft'
    \\    CHECK (status IN ('draft', 'approved', 'closed')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,

    // ── 15. Budget Lines ──────────────────────────────────────────
    // Per-account, per-period budget amounts. Fixed-point x 10^8.
    \\CREATE TABLE IF NOT EXISTS ledger_budget_lines (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  amount INTEGER NOT NULL DEFAULT 0,
    \\  budget_id INTEGER NOT NULL REFERENCES ledger_budgets(id),
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (budget_id, account_id, period_id)
    \\);
    ,

    // ── 16. Audit Log ───────────────────────────────────────────
    // Append-only change log. Every mutation writes here in the same
    // SQLite transaction. Required for GoBD (Germany), UGB/BAO (Austria),
    // BIR (Philippines), ACRA (Singapore) compliance.
    // NO updates. NO deletes. EVER.
    \\CREATE TABLE IF NOT EXISTS ledger_audit_log (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  entity_type TEXT NOT NULL
    \\    CHECK (length(entity_type) BETWEEN 1 AND 50),
    \\  entity_id INTEGER NOT NULL,
    \\  action TEXT NOT NULL
    \\    CHECK (length(action) BETWEEN 1 AND 50),
    \\  field_changed TEXT
    \\    CHECK (field_changed IS NULL OR length(field_changed) <= 100),
    \\  old_value TEXT
    \\    CHECK (old_value IS NULL OR length(old_value) <= 4000),
    \\  new_value TEXT
    \\    CHECK (new_value IS NULL OR length(new_value) <= 4000),
    \\  performed_by TEXT NOT NULL
    \\    CHECK (length(performed_by) BETWEEN 1 AND 100),
    \\  performed_at TEXT NOT NULL
    \\    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  hash_chain TEXT
    \\);
    ,
};

// ── Indexes (13) ────────────────────────────────────────────────

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
    // Composite: entries by book, period, and status (report queries)
    \\CREATE INDEX IF NOT EXISTS idx_entries_book_period
    \\  ON ledger_entries (book_id, period_id, status);
    ,
    // Balance cache: lookup by period and account
    \\CREATE INDEX IF NOT EXISTS idx_balances_period
    \\  ON ledger_account_balances (period_id, account_id);
    ,
    // Partial: find reversal entries
    \\CREATE INDEX IF NOT EXISTS idx_entries_reverses
    \\  ON ledger_entries (reverses_entry_id)
    \\  WHERE reverses_entry_id IS NOT NULL;
    ,
    // Dimension queries: find lines by dimension value
    \\CREATE INDEX IF NOT EXISTS idx_line_dimensions_value
    \\  ON ledger_line_dimensions (dimension_value_id);
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

// ── Triggers (2) ───────────────────────────────────────────────
// Protect audit log from modification. GoBD requires WORM compliance.

const triggers = [_][*:0]const u8{
    \\CREATE TRIGGER IF NOT EXISTS protect_audit_log_delete
    \\BEFORE DELETE ON ledger_audit_log
    \\BEGIN
    \\  SELECT RAISE(ABORT, 'audit log is immutable: DELETE not allowed');
    \\END;
    ,
    \\CREATE TRIGGER IF NOT EXISTS protect_audit_log_update
    \\BEFORE UPDATE ON ledger_audit_log
    \\BEGIN
    \\  SELECT RAISE(ABORT, 'audit log is immutable: UPDATE not allowed');
    \\END;
    ,
};

// ── Tests ───────────────────────────────────────────────────────

test "createAll creates 16 tables" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'ledger_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 16), stmt.columnInt(0));
}

test "createAll creates 13 indexes" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 13), stmt.columnInt(0));
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

test "createAll sets user_version to 4" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare("PRAGMA user_version;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(SCHEMA_VERSION, stmt.columnInt(0));
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

test "base_currency CHECK rejects wrong length" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    // 2 chars — too short
    const too_short = database.exec(
        "INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PH');",
    );
    try std.testing.expectError(error.SqliteExecFailed, too_short);

    // 4 chars — too long
    const too_long = database.exec(
        "INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHPP');",
    );
    try std.testing.expectError(error.SqliteExecFailed, too_long);

    // 3 chars — valid
    try database.exec(
        "INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');",
    );
}

test "account number CHECK rejects invalid length" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    // Empty string — too short
    const empty = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('', 'Empty', 'asset', 'debit', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, empty);

    // 51 chars — too long
    const long_number = "12345678901234567890123456789012345678901234567890X";
    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id) VALUES (?, 'Long', 'asset', 'debit', 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, long_number);
        const result = stmt.step();
        try std.testing.expectError(error.SqliteStepFailed, result);
    }

    // 50 chars — valid boundary
    const max_number = "12345678901234567890123456789012345678901234567890";
    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id) VALUES (?, 'Max', 'asset', 'debit', 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, max_number);
        _ = try stmt.step();
    }
}

test "account_type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad_type = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'invalid', 'debit', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_type);
}

test "period_number CHECK enforces range 1-16" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    // 0 — below range
    const zero = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Bad', 0, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, zero);

    // 17 — above range
    const seventeen = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Bad', 17, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, seventeen);

    // 1 — valid boundary
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    // 16 — valid boundary
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Adj4', 16, 2026, '2026-12-01', '2026-12-31', 1);
    );
}

test "entry status CHECK rejects invalid status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    const bad_status = database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date,
        \\  status, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 'invalid', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_status);
}

test "entry_lines CHECK rejects both debit and credit positive" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    // Both positive — should fail
    const both_positive = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 200, 100, 'PHP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, both_positive);
}

test "period UNIQUE constraint enforces one period_number per year per book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    // Same book, same period_number, same year — should fail
    const dupe = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Also Jan', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "document_number UNIQUE constraint enforces unique per book" {
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

    // Same document_number in same book — should fail
    const dupe = database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-16', '2026-01-16', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "classification UNIQUE constraint enforces unique name per book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS BS', 'balance_sheet', 1);
    );

    const dupe = database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS BS', 'income_statement', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "subledger_group UNIQUE constraint enforces unique type+number per book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 1);
    );

    const dupe = database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Also Customers', 'customer', 1, 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "FK rejects invalid book_id on account" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad_fk = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid period_id on entry" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad_fk = database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 999, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "entry_line UNIQUE constraint enforces unique line_number per entry" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1);
    );

    // Same line_number in same entry — should fail
    const dupe = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 200, 0, 200, 'PHP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "book status CHECK rejects invalid status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad = database.exec(
        "INSERT INTO ledger_books (name, base_currency, status) VALUES ('Test', 'PHP', 'deleted');",
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "normal_balance CHECK rejects invalid value" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'both', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "account status CHECK rejects invalid status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, status, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 'deleted', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "period_type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, period_type, book_id)
        \\VALUES ('Jan', 1, 2026, '2026-01-01', '2026-01-31', 'special', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "period status CHECK rejects invalid status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, status, book_id)
        \\VALUES ('Jan', 1, 2026, '2026-01-01', '2026-01-31', 'deleted', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "classification report_type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('Bad', 'journal_register', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "classification_node node_type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS', 'balance_sheet', 1);
    );

    const bad = database.exec(
        \\INSERT INTO ledger_classification_nodes (node_type, label, classification_id)
        \\VALUES ('folder', 'Assets', 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "subledger_group type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );

    const bad = database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Bad', 'vendor', 1, 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "subledger_account type CHECK rejects invalid type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 1);
    );

    const bad = database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Bad', 'vendor', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "subledger_account UNIQUE constraint enforces unique number per book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Juan', 'customer', 1, 1);
    );

    const dupe = database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Also Juan', 'customer', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
}

test "FK rejects invalid entry_id on entry_line" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );

    const bad_fk = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on entry" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    const bad_fk = database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on period" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad_fk = database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan', 1, 2026, '2026-01-01', '2026-01-31', 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid classification_id on node" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad_fk = database.exec(
        \\INSERT INTO ledger_classification_nodes (node_type, label, classification_id)
        \\VALUES ('group', 'Assets', 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid gl_account_id on subledger_group" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad_fk = database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 999, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid group_id on subledger_account" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad_fk = database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Juan', 'customer', 999, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on audit_log" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad_fk = database.exec(
        \\INSERT INTO ledger_audit_log (entity_type, entity_id, action, performed_by, book_id)
        \\VALUES ('book', 1, 'create', 'admin', 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid counterparty_id on entry_line" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    const bad_fk = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id, counterparty_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1, 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on classification" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    const bad_fk = database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS', 'balance_sheet', 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on subledger_group" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );

    const bad_fk = database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "FK rejects invalid book_id on subledger_account" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 1);
    );

    const bad_fk = database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Juan', 'customer', 1, 999);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad_fk);
}

test "decimal_places CHECK enforces range 0-8" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    // -1 — below range
    const negative = database.exec(
        "INSERT INTO ledger_books (name, base_currency, decimal_places) VALUES ('Test', 'PHP', -1);",
    );
    try std.testing.expectError(error.SqliteExecFailed, negative);

    // 9 — above range
    const nine = database.exec(
        "INSERT INTO ledger_books (name, base_currency, decimal_places) VALUES ('Test', 'PHP', 9);",
    );
    try std.testing.expectError(error.SqliteExecFailed, nine);

    // 0 — valid boundary
    try database.exec(
        "INSERT INTO ledger_books (name, base_currency, decimal_places) VALUES ('Zero', 'JPY', 0);",
    );

    // 8 — valid boundary
    try database.exec(
        "INSERT INTO ledger_books (name, base_currency, decimal_places) VALUES ('Eight', 'BTC', 8);",
    );
}

test "is_contra CHECK enforces boolean 0 or 1" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const bad = database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, is_contra, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 5, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "fx_rate CHECK enforces positive value" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    // fx_rate = 0 — should fail
    const zero_rate = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, fx_rate, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 0, 'PHP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, zero_rate);

    // fx_rate = -1 — should fail
    const negative_rate = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, fx_rate, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, -1, 'PHP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, negative_rate);
}

// ── Business scenario tests ────────────────────────────────────

test "credit entry line accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('2000', 'AP', 'liability', 'credit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );

    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_credit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 0, 50000000000, 50000000000, 'PHP', 1, 1);
    );

    var stmt = try database.prepare("SELECT credit_amount FROM ledger_entry_lines WHERE line_number = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 50000000000), stmt.columnInt64(0));
}

test "multiple account types coexist in same book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const types = [_][2][]const u8{
        .{ "asset", "debit" },
        .{ "liability", "credit" },
        .{ "equity", "credit" },
        .{ "revenue", "credit" },
        .{ "expense", "debit" },
    };

    for (types, 0..) |pair, i| {
        var stmt = try database.prepare(
            "INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id) VALUES (?, ?, ?, ?, 1);",
        );
        defer stmt.finalize();

        var num_buf: [4]u8 = undefined;
        const num = std.fmt.bufPrint(&num_buf, "{d}", .{1000 + i}) catch unreachable;
        try stmt.bindText(1, num);
        try stmt.bindText(2, pair[0]);
        try stmt.bindText(3, pair[0]);
        try stmt.bindText(4, pair[1]);
        _ = try stmt.step();
    }

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_accounts WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 5), stmt.columnInt(0));
}

test "same account number allowed in different books" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book A', 'PHP');");
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book B', 'USD');");

    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash PHP', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash USD', 'asset', 'debit', 2);
    );
}

test "same period number allowed in different years" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2025', 1, 2025, '2025-01-01', '2025-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
}

test "same document number allowed in different books" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book A', 'PHP');");
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book B', 'USD');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan A', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan B', 1, 2026, '2026-01-01', '2026-01-31', 2);
    );

    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 2, 2);
    );
}

test "entry with metadata JSON stored and retrieved" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    const json = "{\"source\":\"invoice\",\"ref\":\"INV-2026-001\"}";
    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_entries (document_number, transaction_date, posting_date, metadata, period_id, book_id) VALUES ('JE-001', '2026-01-15', '2026-01-15', ?, 1, 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, json);
        _ = try stmt.step();
    }

    var stmt = try database.prepare("SELECT metadata FROM ledger_entries WHERE document_number = 'JE-001';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings(json, stmt.columnText(0).?);
}

test "entry line with quantity and unit_type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, quantity, unit_type, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 500, 'shares', 1, 1);
    );

    var stmt = try database.prepare("SELECT quantity, unit_type FROM ledger_entry_lines WHERE line_number = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 500), stmt.columnInt(0));
    try std.testing.expectEqualStrings("shares", stmt.columnText(1).?);
}

test "multiple entry lines on same entry with different line numbers" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('2000', 'AP', 'liability', 'credit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );

    // Debit line
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1);
    );
    // Credit line
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_credit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (2, 0, 100, 100, 'PHP', 2, 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "contra account with is_contra = 1 accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, is_contra, book_id)
        \\VALUES ('1900', 'Accum Depreciation', 'asset', 'credit', 1, 1);
    );

    var stmt = try database.prepare("SELECT is_contra, normal_balance FROM ledger_accounts WHERE number = '1900';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    try std.testing.expectEqualStrings("credit", stmt.columnText(1).?);
}

test "default values populated correctly" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    // Verify book defaults
    {
        var stmt = try database.prepare(
            "SELECT decimal_places, status FROM ledger_books WHERE id = 1;",
        );
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
        try std.testing.expectEqualStrings("active", stmt.columnText(1).?);
    }

    // Verify account defaults
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    {
        var stmt = try database.prepare(
            "SELECT is_contra, status FROM ledger_accounts WHERE id = 1;",
        );
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
        try std.testing.expectEqualStrings("active", stmt.columnText(1).?);
    }

    // Verify period defaults
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    {
        var stmt = try database.prepare(
            "SELECT period_type, status FROM ledger_periods WHERE id = 1;",
        );
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("regular", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("open", stmt.columnText(1).?);
    }

    // Verify entry defaults
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 1, 1);
    );
    {
        var stmt = try database.prepare(
            "SELECT status FROM ledger_entries WHERE id = 1;",
        );
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("draft", stmt.columnText(0).?);
    }

    // Verify entry line defaults
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1);
    );
    {
        var stmt = try database.prepare(
            "SELECT fx_rate FROM ledger_entry_lines WHERE id = 1;",
        );
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 10000000000), stmt.columnInt64(0));
    }
}

test "books with different base currencies coexist" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Philippines', 'PHP');");
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('United States', 'USD');");
    try database.exec("INSERT INTO ledger_books (name, base_currency, decimal_places) VALUES ('Japan', 'JPY', 0);");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

test "audit log accepts entries and timestamps are populated" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_audit_log (entity_type, entity_id, action, field_changed,
        \\  old_value, new_value, performed_by, book_id)
        \\VALUES ('book', 1, 'create', NULL, NULL, NULL, 'admin', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_audit_log (entity_type, entity_id, action, field_changed,
        \\  old_value, new_value, performed_by, book_id)
        \\VALUES ('book', 1, 'update', 'name', 'Test', 'Production', 'admin', 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "transaction history view shows only posted entries" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    // Draft entry — should NOT appear in view
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, period_id, book_id)
        \\VALUES ('DRAFT-001', '2026-01-15', '2026-01-15', 'draft', 1, 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', 1, 1);
    );

    // Posted entry — should appear in view
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, period_id, book_id)
        \\VALUES ('POST-001', '2026-01-15', '2026-01-15', 'posted', 1, 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 200, 0, 200, 'PHP', 1, 2);
    );

    // View should only show the posted entry's line
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

// ── Edge case tests ────────────────────────────────────────────

test "unicode in account names" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    const names = [_]struct { num: []const u8, name: []const u8 }{
        .{ .num = "1000", .name = "Efectivo \xc3\xa9" },
        .{ .num = "1001", .name = "\xe4\xbc\x9a\xe8\xae\xa1" },
        .{ .num = "1002", .name = "Caf\xc3\xa9 & Cr\xc3\xa8me" },
    };

    for (names) |n| {
        var stmt = try database.prepare(
            "INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id) VALUES (?, ?, 'asset', 'debit', 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, n.num);
        try stmt.bindText(2, n.name);
        _ = try stmt.step();
    }

    var stmt = try database.prepare("SELECT name FROM ledger_accounts WHERE number = '1001';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("\xe4\xbc\x9a\xe8\xae\xa1", stmt.columnText(0).?);
}

test "description at max length accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    var max_desc: [1000]u8 = undefined;
    @memset(&max_desc, 'A');

    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, period_id, book_id) VALUES ('JE-001', '2026-01-15', '2026-01-15', ?, 1, 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, &max_desc);
        _ = try stmt.step();
    }

    var stmt = try database.prepare("SELECT length(description) FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1000), stmt.columnInt(0));
}

test "description over max length rejected" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    var over_desc: [1001]u8 = undefined;
    @memset(&over_desc, 'A');

    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, period_id, book_id) VALUES ('JE-001', '2026-01-15', '2026-01-15', ?, 1, 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, &over_desc);
        const result = stmt.step();
        try std.testing.expectError(error.SqliteStepFailed, result);
    }
}

test "transaction_currency CHECK enforces 3 chars on entry lines" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    const bad = database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHPP', 1, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, bad);
}

test "document_number over 100 chars rejected" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    var long_doc: [101]u8 = undefined;
    @memset(&long_doc, 'X');

    {
        var stmt = try database.prepare(
            "INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id) VALUES (?, '2026-01-15', '2026-01-15', 1, 1);",
        );
        defer stmt.finalize();
        try stmt.bindText(1, &long_doc);
        const result = stmt.step();
        try std.testing.expectError(error.SqliteStepFailed, result);
    }
}

test "adjustment period coexists with regular period" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, period_type, book_id)
        \\VALUES ('Dec 2026', 12, 2026, '2026-12-01', '2026-12-31', 'regular', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, period_type, book_id)
        \\VALUES ('Adj 1', 13, 2026, '2026-12-01', '2026-12-31', 'adjustment', 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "entry can reference another entry via reverses_entry_id" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    // Original entry
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15', 'reversed', 1, 1);
    );
    // Reversal entry pointing to original
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, reverses_entry_id, period_id, book_id)
        \\VALUES ('JE-002', '2026-01-16', '2026-01-16', 'posted', 1, 1, 1);
    );

    var stmt = try database.prepare("SELECT reverses_entry_id FROM ledger_entries WHERE document_number = 'JE-002';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "classification node can self-reference parent within same classification" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS', 'balance_sheet', 1);
    );

    // Root node (no parent)
    try database.exec(
        \\INSERT INTO ledger_classification_nodes (node_type, label, parent_id, classification_id)
        \\VALUES ('group', 'Assets', NULL, 1);
    );
    // Child node referencing parent
    try database.exec(
        \\INSERT INTO ledger_classification_nodes (node_type, label, parent_id, classification_id)
        \\VALUES ('group', 'Current Assets', 1, 1);
    );
    // Leaf node with account reference
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_classification_nodes (node_type, label, parent_id, account_id, depth, classification_id)
        \\VALUES ('account', 'Cash', 2, 1, 2, 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes WHERE classification_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

test "account balance cache with staleness tracking" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    // Insert fresh cache row
    try database.exec(
        \\INSERT INTO ledger_account_balances (account_id, period_id, debit_sum, credit_sum, balance, entry_count, is_stale, book_id)
        \\VALUES (1, 1, 10000000000, 0, 10000000000, 1, 0, 1);
    );

    // Mark stale
    try database.exec(
        \\UPDATE ledger_account_balances SET is_stale = 1, stale_since = '2026-01-16T10:00:00Z'
        \\WHERE account_id = 1 AND period_id = 1;
    );

    var stmt = try database.prepare("SELECT is_stale, stale_since FROM ledger_account_balances WHERE account_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    try std.testing.expectEqualStrings("2026-01-16T10:00:00Z", stmt.columnText(1).?);
}

test "inserted_at timestamp auto-populated" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    var stmt = try database.prepare("SELECT inserted_at, updated_at FROM ledger_books WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();

    const inserted = stmt.columnText(0);
    const updated = stmt.columnText(1);
    try std.testing.expect(inserted != null);
    try std.testing.expect(updated != null);
    // ISO 8601 format: 2026-04-03T14:30:00Z (20 chars)
    try std.testing.expectEqual(@as(usize, 20), inserted.?.len);
    try std.testing.expectEqual(@as(usize, 20), updated.?.len);
}

test "multiple classifications per book with different report types" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");

    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS BS', 'balance_sheet', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('IFRS IS', 'income_statement', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_classifications (name, report_type, book_id)
        \\VALUES ('Mgmt TB', 'trial_balance', 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classifications WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

test "subledger account type both accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, 1, 1);
    );

    try database.exec(
        \\INSERT INTO ledger_subledger_accounts (number, name, type, group_id, book_id)
        \\VALUES ('20000001', 'Dual Corp', 'both', 1, 1);
    );

    var stmt = try database.prepare("SELECT type FROM ledger_subledger_accounts WHERE number = '20000001';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("both", stmt.columnText(0).?);
}

test "entry with null optional fields accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );

    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date,
        \\  description, void_reason, reversed_reason, reverses_entry_id,
        \\  posted_at, posted_by, metadata, period_id, book_id)
        \\VALUES ('JE-001', '2026-01-15', '2026-01-15',
        \\  NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, 1);
    );

    var stmt = try database.prepare(
        "SELECT description, void_reason, reversed_reason, metadata FROM ledger_entries WHERE id = 1;",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
    try std.testing.expect(stmt.columnText(1) == null);
    try std.testing.expect(stmt.columnText(2) == null);
    try std.testing.expect(stmt.columnText(3) == null);
}

test "entry line with null counterparty accepted" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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

    try database.exec(
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
        \\  base_debit_amount, transaction_currency, counterparty_id, account_id, entry_id)
        \\VALUES (1, 100, 0, 100, 'PHP', NULL, 1, 1);
    );

    var stmt = try database.prepare("SELECT counterparty_id FROM ledger_entry_lines WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
}

test "subledger group with number range fields" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1200', 'AR', 'asset', 'debit', 1);
    );

    try database.exec(
        \\INSERT INTO ledger_subledger_groups (name, type, group_number, number_range_start, number_range_end, gl_account_id, book_id)
        \\VALUES ('Customers', 'customer', 1, '20000001', '20009999', 1, 1);
    );

    var stmt = try database.prepare("SELECT number_range_start, number_range_end FROM ledger_subledger_groups WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("20000001", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("20009999", stmt.columnText(1).?);
}

test "two entries in same period accepted" {
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
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, period_id, book_id)
        \\VALUES ('JE-002', '2026-01-16', '2026-01-16', 1, 1);
    );

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "account balance PK prevents duplicate account-period pair" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

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
        \\INSERT INTO ledger_account_balances (account_id, period_id, debit_sum, credit_sum, balance, entry_count, book_id)
        \\VALUES (1, 1, 100, 0, 100, 1, 1);
    );

    const dupe = database.exec(
        \\INSERT INTO ledger_account_balances (account_id, period_id, debit_sum, credit_sum, balance, entry_count, book_id)
        \\VALUES (1, 1, 200, 0, 200, 2, 1);
    );
    try std.testing.expectError(error.SqliteExecFailed, dupe);
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

test "audit log trigger prevents DELETE" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    // Insert a test audit record
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec("INSERT INTO ledger_audit_log (entity_type, entity_id, action, performed_by, book_id) VALUES ('book', 1, 'create', 'admin', 1);");

    // Attempt to DELETE should fail
    const result = database.exec("DELETE FROM ledger_audit_log WHERE id = 1;");
    try std.testing.expectError(error.SqliteExecFailed, result);
}

test "audit log trigger prevents UPDATE" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec("INSERT INTO ledger_audit_log (entity_type, entity_id, action, performed_by, book_id) VALUES ('book', 1, 'create', 'admin', 1);");

    // Attempt to UPDATE should fail
    const result = database.exec("UPDATE ledger_audit_log SET action = 'delete' WHERE id = 1;");
    try std.testing.expectError(error.SqliteExecFailed, result);
}

test "createAll creates 2 audit protection triggers" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'protect_audit_log%';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "audit log trigger rejects DELETE" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec("INSERT INTO ledger_audit_log (entity_type, entity_id, action, performed_by, book_id) VALUES ('book', 1, 'create', 'admin', 1);");
    const result = database.exec("DELETE FROM ledger_audit_log WHERE id = 1;");
    try std.testing.expectError(error.SqliteExecFailed, result);
}

test "audit log trigger rejects UPDATE" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec("INSERT INTO ledger_audit_log (entity_type, entity_id, action, performed_by, book_id) VALUES ('book', 1, 'create', 'admin', 1);");
    const result = database.exec("UPDATE ledger_audit_log SET action = 'modified' WHERE id = 1;");
    try std.testing.expectError(error.SqliteExecFailed, result);
}

test "migrate from v4 to v5 adds hash_chain and parent_value_id columns" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try database.exec("PRAGMA user_version = 4;");
    try createAll(database);
    try database.exec("PRAGMA user_version = 4;");
    try migrate(database, 4);

    var stmt = try database.prepare("PRAGMA user_version;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, SCHEMA_VERSION), stmt.columnInt(0));

    var col_stmt = try database.prepare("SELECT hash_chain FROM ledger_audit_log LIMIT 0;");
    defer col_stmt.finalize();

    var dim_stmt = try database.prepare("SELECT parent_value_id FROM ledger_dimension_values LIMIT 0;");
    defer dim_stmt.finalize();
}

test "migrate is no-op when already at current version" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);
    try migrate(database, SCHEMA_VERSION);

    var stmt = try database.prepare("PRAGMA user_version;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, SCHEMA_VERSION), stmt.columnInt(0));
}
