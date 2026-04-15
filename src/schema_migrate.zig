const std = @import("std");
const db = @import("db.zig");

pub fn migrate(database: db.Database, from_version: i32, target_version: i32) !void {
    const owns_txn = try database.beginTransactionIfNeeded();
    errdefer if (owns_txn) database.rollback();

    if (from_version < 5) {
        database.exec("ALTER TABLE ledger_audit_log ADD COLUMN hash_chain TEXT;") catch |err| {
            std.log.debug("migrate v5: hash_chain column: {s} (expected if exists)", .{@errorName(err)});
        };
        database.exec("ALTER TABLE ledger_dimension_values ADD COLUMN parent_value_id INTEGER REFERENCES ledger_dimension_values(id);") catch |err| {
            std.log.debug("migrate v5: parent_value_id column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 6) {
        database.exec(
            \\CREATE TABLE IF NOT EXISTS ledger_open_items (
            \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
            \\  entry_line_id INTEGER NOT NULL REFERENCES ledger_entry_lines(id),
            \\  counterparty_id INTEGER NOT NULL REFERENCES ledger_subledger_accounts(id),
            \\  original_amount INTEGER NOT NULL CHECK (original_amount > 0),
            \\  remaining_amount INTEGER NOT NULL CHECK (remaining_amount >= 0),
            \\  due_date TEXT CHECK (due_date IS NULL OR length(due_date) = 10),
            \\  status TEXT NOT NULL DEFAULT 'open'
            \\    CHECK (status IN ('open', 'partial', 'closed')),
            \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
            \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            \\  UNIQUE (entry_line_id)
            \\);
        ) catch |err| {
            std.log.debug("migrate v6: open_items table: {s}", .{@errorName(err)});
        };
        database.exec(
            \\CREATE INDEX IF NOT EXISTS idx_open_items_counterparty
            \\  ON ledger_open_items (counterparty_id, status, due_date);
        ) catch |err| {
            std.log.debug("migrate v6: open_items index: {s}", .{@errorName(err)});
        };
    }

    if (from_version < 7) {
        database.exec("ALTER TABLE ledger_accounts ADD COLUMN is_monetary INTEGER NOT NULL DEFAULT 1 CHECK (is_monetary IN (0, 1));") catch |err| {
            std.log.debug("migrate v7: is_monetary column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 8) {
        database.exec("ALTER TABLE ledger_books ADD COLUMN fy_start_month INTEGER NOT NULL DEFAULT 1 CHECK (fy_start_month BETWEEN 1 AND 12);") catch |err| {
            std.log.debug("migrate v8: fy_start_month column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 9) {
        database.exec("ALTER TABLE ledger_accounts ADD COLUMN parent_id INTEGER REFERENCES ledger_accounts(id);") catch |err| {
            std.log.debug("migrate v9: parent_id column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 10) {
        const entity_type_sql: [*:0]const u8 =
            \\ALTER TABLE ledger_books ADD COLUMN entity_type TEXT NOT NULL DEFAULT 'corporation'
            \\  CHECK (entity_type IN ('corporation', 'sole_proprietorship', 'partnership',
            \\                          'llc', 'nonprofit', 'cooperative', 'fund',
            \\                          'government', 'other'));
        ;
        database.exec(entity_type_sql) catch |err| {
            std.log.debug("migrate v10: entity_type column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 11) {
        database.exec("ALTER TABLE ledger_books ADD COLUMN dividends_drawings_account_id INTEGER;") catch |err| {
            std.log.debug("migrate v11: dividends_drawings column: {s} (expected if exists)", .{@errorName(err)});
        };
        database.exec("ALTER TABLE ledger_books ADD COLUMN current_year_earnings_account_id INTEGER;") catch |err| {
            std.log.debug("migrate v11: current_year_earnings column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    if (from_version < 12) {
        database.exec(
            \\CREATE TABLE IF NOT EXISTS ledger_equity_allocations (
            \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
            \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
            \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
            \\  allocation_name TEXT NOT NULL
            \\    CHECK (length(allocation_name) BETWEEN 1 AND 100),
            \\  allocation_type TEXT NOT NULL DEFAULT 'percentage'
            \\    CHECK (allocation_type IN ('percentage', 'ratio', 'equal', 'fixed_amount')),
            \\  allocation_value INTEGER NOT NULL
            \\    CHECK (allocation_value >= 0),
            \\  effective_date TEXT NOT NULL
            \\    CHECK (length(effective_date) = 10),
            \\  end_date TEXT
            \\    CHECK (end_date IS NULL OR length(end_date) = 10),
            \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            \\  CHECK (end_date IS NULL OR end_date >= effective_date)
            \\);
        ) catch |err| {
            std.log.debug("migrate v12: equity_allocations table: {s}", .{@errorName(err)});
        };
        database.exec(
            \\CREATE INDEX IF NOT EXISTS idx_equity_allocations_book_dates
            \\  ON ledger_equity_allocations (book_id, effective_date, end_date);
        ) catch |err| {
            std.log.debug("migrate v12: equity_allocations index: {s}", .{@errorName(err)});
        };
    }

    if (from_version < 13) {
        database.exec(
            \\ALTER TABLE ledger_entries ADD COLUMN entry_type TEXT NOT NULL DEFAULT 'standard'
            \\  CHECK (entry_type IN ('standard', 'opening', 'closing', 'reversal', 'adjusting'));
        ) catch |err| {
            std.log.debug("migrate v13: entry_type column: {s} (expected if exists)", .{@errorName(err)});
        };
    }

    var version_buf: [32:0]u8 = undefined;
    const version_pragma = try std.fmt.bufPrintZ(&version_buf, "PRAGMA user_version = {d};", .{target_version});
    try database.exec(version_pragma);

    if (owns_txn) try database.commit();
}
