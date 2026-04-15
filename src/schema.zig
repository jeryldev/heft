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
const schema_indexes = @import("schema_indexes.zig");
const schema_migrate = @import("schema_migrate.zig");
const schema_tables = @import("schema_tables.zig");
const schema_triggers = @import("schema_triggers.zig");
const schema_views = @import("schema_views.zig");

pub const SCHEMA_VERSION: i32 = 13;
const tables = schema_tables.tables;
const indexes = schema_indexes.indexes;
const views = schema_views.views;
const triggers = schema_triggers.triggers;

pub fn migrate(database: db.Database, from_version: i32) !void {
    try schema_migrate.migrate(database, from_version, SCHEMA_VERSION);
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

// ── Tests ───────────────────────────────────────────────────────

test "createAll creates 18 tables" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'ledger_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 18), stmt.columnInt(0));
}

test "createAll creates 17 indexes" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    var stmt = try database.prepare(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%';",
    );
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 17), stmt.columnInt(0));
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

test "createAll sets user_version to current schema version" {
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

test "migrate from v12 to v13 adds entry_type column with default" {
    const database = try db.Database.open(":memory:");
    defer database.close();

    // Construct minimum v12-like state: ledger_books + ledger_entries without
    // the entry_type column. This simulates a production database that was
    // created under schema v12 before Sprint D.
    try database.exec(
        \\CREATE TABLE ledger_books (
        \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
        \\  name TEXT NOT NULL,
        \\  base_currency TEXT NOT NULL
        \\);
    );
    try database.exec(
        \\CREATE TABLE ledger_entries (
        \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
        \\  document_number TEXT NOT NULL,
        \\  transaction_date TEXT NOT NULL,
        \\  posting_date TEXT NOT NULL,
        \\  status TEXT NOT NULL DEFAULT 'draft',
        \\  period_id INTEGER NOT NULL DEFAULT 1,
        \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id)
        \\);
    );
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    try database.exec("INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, book_id) VALUES ('JE-V12', '2026-01-01', '2026-01-01', 'posted', 1);");
    try database.exec("PRAGMA user_version = 12;");

    // Sanity: entry_type does not exist yet.
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM pragma_table_info('ledger_entries') WHERE name = 'entry_type';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    try migrate(database, 12);

    // user_version bumped to 13.
    {
        var stmt = try database.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, SCHEMA_VERSION), stmt.columnInt(0));
    }

    // entry_type column now exists.
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM pragma_table_info('ledger_entries') WHERE name = 'entry_type';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }

    // Pre-existing v12 row got the default 'standard' value.
    {
        var stmt = try database.prepare("SELECT entry_type FROM ledger_entries WHERE document_number = 'JE-V12';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("standard", stmt.columnText(0).?);
    }

    // CHECK constraint is enforced: invalid value rejected.
    const bad_insert = database.exec("INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, book_id, entry_type) VALUES ('JE-BAD', '2026-01-02', '2026-01-02', 'posted', 1, 'not_a_type');");
    try std.testing.expectError(error.SqliteExecFailed, bad_insert);

    // Valid 'reversal' value accepted.
    try database.exec("INSERT INTO ledger_entries (document_number, transaction_date, posting_date, status, book_id, entry_type) VALUES ('JE-REV', '2026-01-03', '2026-01-03', 'posted', 1, 'reversal');");
}

test "migrate from v12 to v13 is idempotent" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try createAll(database);

    // Already at v13. Running migrate again must be a no-op and not error.
    try migrate(database, SCHEMA_VERSION);
    try migrate(database, SCHEMA_VERSION);

    var stmt = try database.prepare("PRAGMA user_version;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, SCHEMA_VERSION), stmt.columnInt(0));
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
