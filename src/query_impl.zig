const std = @import("std");
const books = @import("query_books.zig");
const entries = @import("query_entries.zig");
const subledger = @import("query_subledger.zig");

pub const SortOrder = books.SortOrder;
pub const DEFAULT_LIMIT = books.DEFAULT_LIMIT;
pub const MAX_LIMIT = books.MAX_LIMIT;

pub const getBook = books.getBook;
pub const listBooks = books.listBooks;
pub const getAccount = books.getAccount;
pub const listAccounts = books.listAccounts;
pub const getPeriod = books.getPeriod;
pub const listPeriods = books.listPeriods;
pub const listEntries = entries.listEntries;
pub const listAuditLog = entries.listAuditLog;
pub const getEntry = entries.getEntry;
pub const listEntryLines = entries.listEntryLines;
pub const getClassification = subledger.getClassification;
pub const getSubledgerGroup = subledger.getSubledgerGroup;
pub const getSubledgerAccount = subledger.getSubledgerAccount;
pub const listClassifications = subledger.listClassifications;
pub const listSubledgerGroups = subledger.listSubledgerGroups;
pub const listSubledgerAccounts = subledger.listSubledgerAccounts;
pub const subledgerReport = subledger.subledgerReport;
pub const counterpartyLedger = subledger.counterpartyLedger;
pub const listTransactions = subledger.listTransactions;
pub const subledgerReconciliation = subledger.subledgerReconciliation;
pub const agedSubledger = subledger.agedSubledger;

// ── Tests ──────────────────────────────────────────────────────

const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");

test "getBook: returns book data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test Book", "PHP", 2, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getBook(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Test Book\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"base_currency\":\"PHP\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rounding_account_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"fx_gain_loss_account_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"retained_earnings_account_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"income_summary_account_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance_account_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"suspense_account_id\":0") != null);
    try std.testing.expect(json[0] == '{');
    try std.testing.expect(json[json.len - 1] == '}');
}

test "getBook: returns NotFound for nonexistent book" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    const result = getBook(database, 999, &buf, .json);
    try std.testing.expectError(error.NotFound, result);
}

test "getBook: CSV format" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "USD", 2, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try getBook(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,base_currency,decimal_places,status,rounding_account_id,fx_gain_loss_account_id") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "Test") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, ",0,0,0,0,0,0\n") != null);
}

test "listBooks: returns all books with pagination metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Book A", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "Book B", "USD", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Book A\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Book B\"") != null);
}

test "listBooks: filter by status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Active Book", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "Archived Book", "USD", 2, "admin");
    try book_mod.Book.archive(database, 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, "active", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Active Book") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Archived Book") == null);
}

test "listBooks: pagination with limit and offset" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "A", "PHP", 2, "admin");
    _ = try book_mod.Book.create(database, "B", "USD", 2, "admin");
    _ = try book_mod.Book.create(database, "C", "EUR", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 2, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_more\":true") != null);
}

test "listBooks: CSV with metadata comment" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try listBooks(database, null, .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "# total=1,limit=100,offset=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name,base_currency") != null);
}

test "listBooks: empty result" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [8192]u8 = undefined;
    const json = try listBooks(database, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

test "getAccount: returns account data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getAccount(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Cash\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_type\":\"asset\"") != null);
}

test "getAccount: returns NotFound for nonexistent" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    const result = getAccount(database, 999, &buf, .json);
    try std.testing.expectError(error.NotFound, result);
}

test "listAccounts: returns all accounts with total" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"2000\"") != null);
}

test "listAccounts: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, "asset", null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "AP") == null);
}

test "listAccounts: text search on name" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash on Hand", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1100", "Cash in Bank", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "Accounts Payable", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, "Cash", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash on Hand") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash in Bank") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Accounts Payable") == null);
}

test "listAccounts: descending order" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, null, null, .desc, 100, 0, &buf, .json);

    // 2000 should come before 1000 in desc order
    const pos_2000 = std.mem.indexOf(u8, json, "\"number\":\"2000\"").?;
    const pos_1000 = std.mem.indexOf(u8, json, "\"number\":\"1000\"").?;
    try std.testing.expect(pos_2000 < pos_1000);
}

// ── Period tests ───────────────────────────────────────────────

const period_mod = @import("period.zig");

test "getPeriod: returns period data" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    var buf: [4096]u8 = undefined;
    const json = try getPeriod(database, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan 2026\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"start_date\":\"2026-01-01\"") != null);
}

test "getPeriod: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getPeriod(database, 999, &buf, .json));
}

test "listPeriods: returns all with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Feb", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listPeriods(database, 1, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Feb\"") != null);
}

test "listPeriods: filter by year" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2027", 1, 2027, "2027-01-01", "2027-01-31", "regular", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listPeriods(database, 1, 2026, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Jan 2026") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Jan 2027") == null);
}

// ── Entry tests ────────────────────────────────────────────────

const entry_mod = @import("entry.zig");
const money = @import("money.zig");

test "listEntries: returns entries with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "DRAFT-001", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;

    // All entries
    const all = try listEntries(database, 1, null, null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":2") != null);

    // Only posted
    const posted = try listEntries(database, 1, "posted", null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, posted, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, posted, "JE-001") != null);

    // Search by doc number
    const search = try listEntries(database, 1, null, null, null, "DRAFT", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, search, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, search, "DRAFT-001") != null);
}

test "listEntries: date range filtering" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listEntries(database, 1, null, "2026-01-15", "2026-01-31", null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-002") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-001") == null);
}

// ── Audit Log tests ────────────────────────────────────────────

test "listAuditLog: returns audit records with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var buf: [32768]u8 = undefined;

    // All audit records
    const all = try listAuditLog(database, 1, null, null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"rows\":[") != null);

    // Filter by entity_type
    const acct_only = try listAuditLog(database, 1, "account", null, null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, acct_only, "\"entity_type\":\"account\"") != null);

    // Filter by action
    const creates = try listAuditLog(database, 1, null, "create", null, null, .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, creates, "\"action\":\"create\"") != null);
}

test "listAuditLog: descending order" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [32768]u8 = undefined;
    const json = try listAuditLog(database, 1, null, null, null, null, .desc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "listAuditLog: CSV with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [32768]u8 = undefined;
    const csv = try listAuditLog(database, 1, null, null, null, null, .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "# total=") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,entity_type") != null);
}

// ── getEntry tests ─────────────────────────────────────────────

test "getEntry: returns entry data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Test entry", 1, null, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getEntry(database, 1, 1, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"document_number\":\"JE-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"description\":\"Test entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":\"draft\"") != null);
}

test "getEntry: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getEntry(database, 999, 1, &buf, .json));
}

test "getEntry: cross-book entry returns NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Book 1", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Test entry", 1, null, "admin");

    const book2_id = try book_mod.Book.create(database, "Book 2", "USD", 2, "admin");

    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getEntry(database, 1, book2_id, &buf, .json));
}

// ── listEntryLines tests ───────────────────────────────────────

test "listEntryLines: returns lines for entry" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listEntryLines(database, eid, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"lines\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_number\":\"1000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_number\":\"3000\"") != null);
}

test "listEntryLines: CSV format" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try listEntryLines(database, eid, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,line_number,account_number") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);
}

// ── listClassifications tests ──────────────────────────────────

const classification_mod = @import("classification.zig");

test "listClassifications: returns all with metadata" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try classification_mod.Classification.create(database, 1, "BS Layout", "balance_sheet", "admin");
    _ = try classification_mod.Classification.create(database, 1, "IS Layout", "income_statement", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listClassifications(database, 1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "BS Layout") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "IS Layout") != null);
}

test "listClassifications: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try classification_mod.Classification.create(database, 1, "BS", "balance_sheet", "admin");
    _ = try classification_mod.Classification.create(database, 1, "IS", "income_statement", "admin");

    var buf: [8192]u8 = undefined;
    const json = try listClassifications(database, 1, "balance_sheet", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "BS") != null);
}

// ── listSubledgerGroups tests ──────────────────────────────────

const subledger_mod = @import("subledger.zig");

test "listSubledgerGroups: returns groups with control account" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Suppliers", "supplier", 2, 2, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerGroups(database, 1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Customers") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Suppliers") != null);
}

test "listSubledgerGroups: filter by type" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerGroups(database, 1, "customer", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
}

// ── listSubledgerAccounts tests ────────────────────────────────

test "listSubledgerAccounts: returns counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Widget Inc", "customer", gid, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Widget Inc") != null);
}

test "listSubledgerAccounts: filter by group" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    const g1 = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    const g2 = try subledger_mod.SubledgerGroup.create(database, 1, "Suppliers", "supplier", 2, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Customer A", "customer", g1, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "S001", "Supplier X", "supplier", g2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, g1, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Customer A") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Supplier X") == null);
}

test "listSubledgerAccounts: name search" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Beta LLC", "customer", gid, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listSubledgerAccounts(database, 1, null, "Acme", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Beta LLC") == null);
}

// ── subledgerReport tests ──────────────────────────────────────

test "subledgerReport: returns counterparty balances" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try subledgerReport(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_name\":\"Acme Corp\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "subledgerReport: empty when no counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try subledgerReport(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

// ── counterpartyLedger tests ───────────────────────────────────

test "counterpartyLedger: returns transactions for counterparty" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "INV-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[") != null);
}

test "counterpartyLedger: empty for nonexistent counterparty" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 999, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
}

// ── listTransactions tests ─────────────────────────────────────

test "listTransactions: paginated GL with filters" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;

    // All transactions
    const all = try listTransactions(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, all, "\"total\":2") != null);

    // Filter by account
    const cash_only = try listTransactions(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, cash_only, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, cash_only, "\"account_number\":\"1000\"") != null);
}

test "listTransactions: pagination with limit" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listTransactions(database, 1, null, null, "2026-01-01", "2026-01-31", .asc, 1, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_more\":true") != null);
}

// ── counterpartyLedger running balance test ────────────────────

test "counterpartyLedger: includes opening_balance and running_balance" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try counterpartyLedger(database, 1, 1, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"running_balance\":") != null);
}

// ── subledgerReconciliation tests ──────────────────────────────

test "subledgerReconciliation: JSON structure" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    var buf: [4096]u8 = undefined;
    const json = try subledgerReconciliation(database, 1, gid, "2026-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"control_account\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"gl_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"sl_balance\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"difference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reconciled\":") != null);
}

test "subledgerReconciliation: reconciled with posted entries" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [4096]u8 = undefined;
    const json = try subledgerReconciliation(database, 1, gid, "2026-01-31", &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"difference\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"reconciled\":true") != null);
}

// ── agedSubledger tests ────────────────────────────────────────

test "agedSubledger: returns aging buckets" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 500000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [16384]u8 = undefined;
    const json = try agedSubledger(database, 1, gid, "2026-02-28", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"counterparty_name\":\"Acme\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"current_0_30\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_31_60\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_61_90\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"past_due_90_plus\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":") != null);
}

test "agedSubledger: empty with no counterparties" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const json = try agedSubledger(database, 1, null, "2026-01-31", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"rows\":[]") != null);
}

test "agedSubledger: CSV format with header" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try agedSubledger(database, 1, null, "2026-01-31", .asc, 100, 0, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "counterparty_id,counterparty_number,counterparty_name,current_0_30") != null);
}

// ── subledgerReport name search test ───────────────────────────

test "subledgerReport: name search filter" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");
    _ = try subledger_mod.SubledgerAccount.create(database, 1, "C002", "Beta LLC", "customer", gid, "admin");

    // Post entry for Acme only
    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 100000000000, 'PHP', 10000000000, 2, 1, 1);");
    try entry_mod.Entry.post(database, eid, "admin");

    // Post entry for Beta
    const eid2 = try entry_mod.Entry.createDraft(database, 1, "INV-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try database.exec("INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, counterparty_id) VALUES (2, 0, 200000000000, 'PHP', 10000000000, 2, 2, 2);");
    try entry_mod.Entry.post(database, eid2, "admin");

    var buf: [16384]u8 = undefined;

    // Search for "Acme" - should only return Acme Corp
    const json = try subledgerReport(database, 1, null, "Acme", "2026-01-01", "2026-01-31", .asc, 100, 0, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Beta LLC") == null);
}

// ── getClassification tests ────────────────────────────────────

test "getClassification: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    const cid = try classification_mod.Classification.create(database, 1, "BS Layout", "balance_sheet", "admin");

    var buf: [4096]u8 = undefined;
    const json = try getClassification(database, cid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"BS Layout\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"report_type\":\"balance_sheet\"") != null);
}

test "getClassification: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getClassification(database, 999, &buf, .json));
}

// ── getSubledgerGroup tests ────────────────────────────────────

test "getSubledgerGroup: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getSubledgerGroup(database, gid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Customers\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"type\":\"customer\"") != null);
}

test "getSubledgerGroup: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getSubledgerGroup(database, 999, &buf, .json));
}

// ── getSubledgerAccount tests ──────────────────────────────────

test "getSubledgerAccount: returns data as JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 1, null, null, "admin");
    const aid = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme Corp", "customer", gid, "admin");

    var buf: [4096]u8 = undefined;
    const json = try getSubledgerAccount(database, aid, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Acme Corp\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"C001\"") != null);
}

test "getSubledgerAccount: NotFound" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    var buf: [4096]u8 = undefined;
    try std.testing.expectError(error.NotFound, getSubledgerAccount(database, 999, &buf, .json));
}

// ── Missing filter path tests ────────────────────────────────────

test "listAccounts: filter by status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    try account_mod.Account.updateStatus(database, 2, .archived, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listAccounts(database, 1, null, "active", null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Cash") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "AP") == null);
}

test "listPeriods: filter by status" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = try period_mod.Period.create(database, 1, "Feb", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    try period_mod.Period.transition(database, 2, .soft_closed, "admin");

    var buf: [8192]u8 = undefined;
    const json = try listPeriods(database, 1, null, "open", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Jan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Feb\"") == null);
}

test "listEntries: doc_search partial match" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "INV-2026-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "PAY-2026-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.createDraft(database, 1, "INV-2026-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listEntries(database, 1, null, null, null, "INV", .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "INV-2026-001") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "INV-2026-002") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "PAY-2026-001") == null);
}

test "listEntries: filter by status posted only" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const eid1 = try entry_mod.Entry.createDraft(database, 1, "JE-POST", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    _ = try entry_mod.Entry.createDraft(database, 1, "JE-DRAFT", "2026-01-20", "2026-01-20", null, 1, null, "admin");

    var buf: [16384]u8 = undefined;
    const json = try listEntries(database, 1, "posted", null, null, null, .asc, 100, 0, &buf, .json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-POST") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "JE-DRAFT") == null);
}

test "getAccount: CSV format with header and data" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");

    var buf: [4096]u8 = undefined;
    const csv = try getAccount(database, 1, &buf, .csv);

    try std.testing.expect(std.mem.indexOf(u8, csv, "id,number,name,account_type,normal_balance,is_contra,status\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1,1000,Cash,asset,debit,0,active\n") != null);
}
