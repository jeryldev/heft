const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const money = @import("money.zig");

pub const EntryStatus = enum {
    draft,
    posted,
    reversed,
    void,

    pub fn toString(self: EntryStatus) []const u8 {
        return switch (self) {
            .void => "void",
            else => @tagName(self),
        };
    }

    pub fn fromString(s: []const u8) ?EntryStatus {
        const map = .{
            .{ "draft", EntryStatus.draft },
            .{ "posted", EntryStatus.posted },
            .{ "reversed", EntryStatus.reversed },
            .{ "void", EntryStatus.void },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Entry = struct {
    const create_sql: [*:0]const u8 =
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, metadata, period_id, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?);
    ;

    const line_sql: [*:0]const u8 =
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, description)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    ;

    const balance_sql: [*:0]const u8 =
        \\INSERT INTO ledger_account_balances (account_id, period_id, debit_sum, credit_sum, balance, entry_count, book_id)
        \\VALUES (?, ?, ?, ?, ? - ?, 1, ?)
        \\ON CONFLICT (account_id, period_id) DO UPDATE SET
        \\  debit_sum = debit_sum + excluded.debit_sum,
        \\  credit_sum = credit_sum + excluded.credit_sum,
        \\  balance = balance + excluded.balance,
        \\  entry_count = entry_count + 1,
        \\  is_stale = 0,
        \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now');
    ;

    pub fn createDraft(database: db.Database, book_id: i64, document_number: []const u8, transaction_date: []const u8, posting_date: []const u8, description: ?[]const u8, period_id: i64, metadata: ?[]const u8, performed_by: []const u8) !i64 {
        if (document_number.len == 0) return error.InvalidInput;

        // Verify book exists and is active
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.InvalidInput;
        }

        // Verify period exists and posting_date falls within range
        {
            var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE id = ? AND book_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            try stmt.bindInt(2, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const start = stmt.columnText(0).?;
            const end = stmt.columnText(1).?;
            if (std.mem.order(u8, posting_date, start) == .lt or std.mem.order(u8, posting_date, end) == .gt) {
                return error.InvalidInput;
            }
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, document_number);
        try stmt.bindText(2, transaction_date);
        try stmt.bindText(3, posting_date);
        if (description) |d| try stmt.bindText(4, d) else try stmt.bindNull(4);
        if (metadata) |m| try stmt.bindText(5, m) else try stmt.bindNull(5);
        try stmt.bindInt(6, period_id);
        try stmt.bindInt(7, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "entry", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    pub fn addLine(database: db.Database, entry_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: []const u8, fx_rate: i64, account_id: i64, description: ?[]const u8, performed_by: []const u8) !i64 {
        // Verify entry exists and is draft
        var entry_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = stmt.columnText(0).?;
            if (!std.mem.eql(u8, status, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(1);
        }

        // Verify account exists, is active, and belongs to same book
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_accounts WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, account_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = stmt.columnText(0).?;
            if (std.mem.eql(u8, status, "inactive") or std.mem.eql(u8, status, "archived")) return error.AccountInactive;
            if (stmt.columnInt64(1) != entry_book_id) return error.InvalidInput;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare(line_sql);
        defer stmt.finalize();

        try stmt.bindInt(1, @intCast(line_number));
        try stmt.bindInt(2, debit_amount);
        try stmt.bindInt(3, credit_amount);
        try stmt.bindText(4, transaction_currency);
        try stmt.bindInt(5, fx_rate);
        try stmt.bindInt(6, account_id);
        try stmt.bindInt(7, entry_id);
        if (description) |d| try stmt.bindText(8, d) else try stmt.bindNull(8);

        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "entry_line", id, "create", null, null, null, performed_by, entry_book_id);

        try database.commit();
        return id;
    }

    pub fn removeLine(database: db.Database, line_id: i64, performed_by: []const u8) !void {
        // Fetch line's entry_id and verify entry is draft
        var entry_id: i64 = 0;
        var entry_book_id: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT el.entry_id, e.status, e.book_id
                \\FROM ledger_entry_lines el
                \\JOIN ledger_entries e ON e.id = el.entry_id
                \\WHERE el.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            entry_id = stmt.columnInt64(0);
            const status = stmt.columnText(1).?;
            if (!std.mem.eql(u8, status, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(2);
        }

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("DELETE FROM ledger_entry_lines WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            _ = try stmt.step();
        }

        try audit.log(database, "entry_line", line_id, "delete", null, null, null, performed_by, entry_book_id);

        try database.commit();
    }

    pub fn deleteDraft(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        var entry_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = stmt.columnText(0).?;
            if (!std.mem.eql(u8, status, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(1);
        }

        try database.beginTransaction();
        errdefer database.rollback();

        // Delete all lines first (FK constraint)
        {
            var stmt = try database.prepare("DELETE FROM ledger_entry_lines WHERE entry_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
        }

        // Delete the entry
        {
            var stmt = try database.prepare("DELETE FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
        }

        try audit.log(database, "entry", entry_id, "delete", null, null, null, performed_by, entry_book_id);

        try database.commit();
    }

    pub fn post(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        // Step 1: Fetch entry — verify status = 'draft'
        var period_id: i64 = 0;
        var entry_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, period_id, book_id FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status != .draft) return error.AlreadyPosted;
            period_id = stmt.columnInt64(1);
            entry_book_id = stmt.columnInt64(2);
        }

        // Step 2: Fetch period — verify status = 'open' or 'soft_closed'
        {
            var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            _ = try stmt.step();
            const period_status = stmt.columnText(0).?;
            if (std.mem.eql(u8, period_status, "closed")) return error.PeriodClosed;
            if (std.mem.eql(u8, period_status, "locked")) return error.PeriodLocked;
        }

        // Step 3: Count lines — verify >= 2
        var line_count: i32 = 0;
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
            line_count = stmt.columnInt(0);
            if (line_count < 2) return error.TooFewLines;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        // Step 4: Compute base amounts for each line
        {
            var read_stmt = try database.prepare("SELECT id, debit_amount, credit_amount, fx_rate FROM ledger_entry_lines WHERE entry_id = ?;");
            defer read_stmt.finalize();
            try read_stmt.bindInt(1, entry_id);

            var update_stmt = try database.prepare("UPDATE ledger_entry_lines SET base_debit_amount = ?, base_credit_amount = ? WHERE id = ?;");
            defer update_stmt.finalize();

            while (try read_stmt.step()) {
                const line_id = read_stmt.columnInt64(0);
                const debit = read_stmt.columnInt64(1);
                const credit = read_stmt.columnInt64(2);
                const fx_rate = read_stmt.columnInt64(3);

                const base_debit = try money.computeBaseAmount(debit, fx_rate);
                const base_credit = try money.computeBaseAmount(credit, fx_rate);

                try update_stmt.bindInt(1, base_debit);
                try update_stmt.bindInt(2, base_credit);
                try update_stmt.bindInt(3, line_id);
                _ = try update_stmt.step();
                update_stmt.reset();
                update_stmt.clearBindings();
            }
        }

        // Step 5: Verify balance equation
        {
            var stmt = try database.prepare("SELECT SUM(base_debit_amount), SUM(base_credit_amount) FROM ledger_entry_lines WHERE entry_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
            const total_debits = stmt.columnInt64(0);
            const total_credits = stmt.columnInt64(1);
            if (total_debits != total_credits) return error.UnbalancedEntry;
        }

        // Step 6: Update entry status to 'posted'
        {
            var stmt = try database.prepare("UPDATE ledger_entries SET status = 'posted', posted_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), posted_by = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, performed_by);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        // Step 7: Update balance cache
        {
            var line_stmt = try database.prepare("SELECT account_id, base_debit_amount, base_credit_amount FROM ledger_entry_lines WHERE entry_id = ?;");
            defer line_stmt.finalize();
            try line_stmt.bindInt(1, entry_id);

            var cache_stmt = try database.prepare(balance_sql);
            defer cache_stmt.finalize();

            while (try line_stmt.step()) {
                const acct_id = line_stmt.columnInt64(0);
                const base_debit = line_stmt.columnInt64(1);
                const base_credit = line_stmt.columnInt64(2);

                try cache_stmt.bindInt(1, acct_id);
                try cache_stmt.bindInt(2, period_id);
                try cache_stmt.bindInt(3, base_debit);
                try cache_stmt.bindInt(4, base_credit);
                try cache_stmt.bindInt(5, base_debit);
                try cache_stmt.bindInt(6, base_credit);
                try cache_stmt.bindInt(7, entry_book_id);
                _ = try cache_stmt.step();
                cache_stmt.reset();
                cache_stmt.clearBindings();
            }
        }

        // Step 8: Mark future periods stale
        {
            var stale_stmt = try database.prepare(
                \\UPDATE ledger_account_balances
                \\SET is_stale = 1, stale_since = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE book_id = ? AND period_id != ? AND is_stale = 0;
            );
            defer stale_stmt.finalize();
            try stale_stmt.bindInt(1, entry_book_id);
            try stale_stmt.bindInt(2, period_id);
            _ = try stale_stmt.step();
        }

        // Step 9: Audit log
        try audit.log(database, "entry", entry_id, "post", "status", "draft", "posted", performed_by, entry_book_id);

        try database.commit();
    }

    pub fn voidEntry(database: db.Database, entry_id: i64, reason: []const u8, performed_by: []const u8) !void {
        if (reason.len == 0) return error.VoidReasonRequired;

        var period_id: i64 = 0;
        var entry_book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT status, period_id, book_id FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status != .posted) return error.InvalidTransition;
            period_id = stmt.columnInt64(1);
            entry_book_id = stmt.columnInt64(2);
        }

        try database.beginTransaction();
        errdefer database.rollback();

        // Update entry status
        {
            var stmt = try database.prepare("UPDATE ledger_entries SET status = 'void', void_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, reason);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        // Reverse balance cache using the POSTED base amounts
        {
            var line_stmt = try database.prepare("SELECT account_id, base_debit_amount, base_credit_amount FROM ledger_entry_lines WHERE entry_id = ?;");
            defer line_stmt.finalize();
            try line_stmt.bindInt(1, entry_id);

            var cache_stmt = try database.prepare("UPDATE ledger_account_balances SET debit_sum = debit_sum - ?, credit_sum = credit_sum - ?, balance = balance - (? - ?), entry_count = entry_count - 1, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE account_id = ? AND period_id = ?;");
            defer cache_stmt.finalize();

            while (try line_stmt.step()) {
                const acct_id = line_stmt.columnInt64(0);
                const base_debit = line_stmt.columnInt64(1);
                const base_credit = line_stmt.columnInt64(2);

                try cache_stmt.bindInt(1, base_debit);
                try cache_stmt.bindInt(2, base_credit);
                try cache_stmt.bindInt(3, base_debit);
                try cache_stmt.bindInt(4, base_credit);
                try cache_stmt.bindInt(5, acct_id);
                try cache_stmt.bindInt(6, period_id);
                _ = try cache_stmt.step();
                cache_stmt.reset();
                cache_stmt.clearBindings();
            }
        }

        try audit.log(database, "entry", entry_id, "void", "status", "posted", "void", performed_by, entry_book_id);
        try audit.log(database, "entry", entry_id, "void", "void_reason", null, reason, performed_by, entry_book_id);

        try database.commit();
    }

    pub fn reverse(database: db.Database, entry_id: i64, reason: []const u8, reversal_date: []const u8, performed_by: []const u8) !i64 {
        if (reason.len == 0) return error.ReverseReasonRequired;

        var period_id: i64 = 0;
        var entry_book_id: i64 = 0;
        var doc_number_buf: [64]u8 = undefined;
        var doc_number_len: usize = 0;
        {
            var stmt = try database.prepare("SELECT status, period_id, book_id, document_number FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status != .posted) return error.InvalidTransition;
            period_id = stmt.columnInt64(1);
            entry_book_id = stmt.columnInt64(2);
            const dn = stmt.columnText(3).?;
            @memcpy(doc_number_buf[0..dn.len], dn);
            doc_number_len = dn.len;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        // Mark original as reversed
        {
            var stmt = try database.prepare("UPDATE ledger_entries SET status = 'reversed', reversed_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, reason);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        // Create reversal entry
        var rev_doc_buf: [72]u8 = undefined;
        const rev_doc = std.fmt.bufPrint(&rev_doc_buf, "REV-{s}", .{doc_number_buf[0..doc_number_len]}) catch return error.InvalidInput;
        {
            var stmt = try database.prepare(
                \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, status, reverses_entry_id, posted_at, posted_by, period_id, book_id)
                \\VALUES (?, ?, ?, ?, 'posted', ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?, ?);
            );
            defer stmt.finalize();
            try stmt.bindText(1, rev_doc);
            try stmt.bindText(2, reversal_date);
            try stmt.bindText(3, reversal_date);
            try stmt.bindText(4, reason);
            try stmt.bindInt(5, entry_id);
            try stmt.bindText(6, performed_by);
            try stmt.bindInt(7, period_id);
            try stmt.bindInt(8, entry_book_id);
            _ = try stmt.step();
        }
        const reversal_id = database.lastInsertRowId();

        // Copy lines with flipped debits/credits
        {
            var read_stmt = try database.prepare("SELECT line_number, debit_amount, credit_amount, base_debit_amount, base_credit_amount, fx_rate, transaction_currency, account_id FROM ledger_entry_lines WHERE entry_id = ?;");
            defer read_stmt.finalize();
            try read_stmt.bindInt(1, entry_id);

            var write_stmt = try database.prepare(
                \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, base_debit_amount, base_credit_amount, fx_rate, transaction_currency, account_id, entry_id)
                \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            );
            defer write_stmt.finalize();

            var cache_stmt = try database.prepare(balance_sql);
            defer cache_stmt.finalize();

            while (try read_stmt.step()) {
                const line_num = read_stmt.columnInt64(0);
                const orig_debit = read_stmt.columnInt64(1);
                const orig_credit = read_stmt.columnInt64(2);
                const orig_base_debit = read_stmt.columnInt64(3);
                const orig_base_credit = read_stmt.columnInt64(4);
                const fx_rate = read_stmt.columnInt64(5);
                const currency = read_stmt.columnText(6).?;
                const acct_id = read_stmt.columnInt64(7);

                // Flip: debit becomes credit, credit becomes debit
                try write_stmt.bindInt(1, line_num);
                try write_stmt.bindInt(2, orig_credit); // flipped
                try write_stmt.bindInt(3, orig_debit); // flipped
                try write_stmt.bindInt(4, orig_base_credit); // flipped
                try write_stmt.bindInt(5, orig_base_debit); // flipped
                try write_stmt.bindInt(6, fx_rate);
                try write_stmt.bindText(7, currency);
                try write_stmt.bindInt(8, acct_id);
                try write_stmt.bindInt(9, reversal_id);
                _ = try write_stmt.step();
                write_stmt.reset();
                write_stmt.clearBindings();

                // Update balance cache with flipped amounts
                try cache_stmt.bindInt(1, acct_id);
                try cache_stmt.bindInt(2, period_id);
                try cache_stmt.bindInt(3, orig_base_credit); // flipped debit
                try cache_stmt.bindInt(4, orig_base_debit); // flipped credit
                try cache_stmt.bindInt(5, orig_base_credit);
                try cache_stmt.bindInt(6, orig_base_debit);
                try cache_stmt.bindInt(7, entry_book_id);
                _ = try cache_stmt.step();
                cache_stmt.reset();
                cache_stmt.clearBindings();
            }
        }

        try audit.log(database, "entry", entry_id, "reverse", "status", "posted", "reversed", performed_by, entry_book_id);
        try audit.log(database, "entry", entry_id, "reverse", "reversed_reason", null, reason, performed_by, entry_book_id);
        try audit.log(database, "entry", reversal_id, "create", null, null, null, performed_by, entry_book_id);

        try database.commit();
        return reversal_id;
    }
};

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

// ── createDraft tests ───────────────────────────────────────────

test "createDraft returns auto-generated id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Test entry", 1, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "createDraft stores correct fields" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Payment received", 1, null, "admin");

    var stmt = try database.prepare("SELECT document_number, status, description FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("JE-001", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("draft", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("Payment received", stmt.columnText(2).?);
}

test "createDraft with metadata" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, "{\"source\":\"invoice\"}", "admin");

    var stmt = try database.prepare("SELECT metadata FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("{\"source\":\"invoice\"}", stmt.columnText(0).?);
}

test "createDraft writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'entry';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
}

test "createDraft rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.createDraft(database, 999, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "createDraft rejects archived book" {
    const database = try setupTestDb();
    defer database.close();

    // Close period first so book can be archived
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "createDraft rejects nonexistent period" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 999, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "createDraft rejects duplicate document_number" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.createDraft(database, 1, "JE-001", "2026-01-16", "2026-01-16", null, 1, null, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

// ── addLine tests ───────────────────────────────────────────────

test "addLine adds debit line to draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), line_id);
}

test "addLine adds credit line to draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), line_id);
}

test "addLine rejects line on non-draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.addLine(database, 1, 3, 500_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "addLine rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.addLine(database, 999, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "addLine rejects inactive account" {
    const database = try setupTestDb();
    defer database.close();

    try account_mod.Account.updateStatus(database, 1, .inactive, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    try std.testing.expectError(error.AccountInactive, result);
}

// ── post tests ──────────────────────────────────────────────────

test "post balanced entry succeeds" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status, posted_by FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("admin", stmt.columnText(1).?);
}

test "post computes base amounts" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT base_debit_amount, base_credit_amount FROM ledger_entry_lines WHERE line_number = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
}

test "post updates balance cache" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    // Check Cash account balance cache
    var stmt = try database.prepare("SELECT debit_sum, credit_sum, entry_count FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(2));
}

test "post writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT action, field_changed, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'post';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("post", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("status", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("posted", stmt.columnText(2).?);
}

test "post rejects unbalanced entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.UnbalancedEntry, result);
}

test "post rejects entry with fewer than 2 lines" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.TooFewLines, result);
}

test "post rejects already posted entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");
    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "post rejects entry in closed period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "post rejects entry in locked period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 1, .locked, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

test "post allows entry in soft_closed period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "post nonexistent entry returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.post(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "post two entries accumulates balance cache" {
    const database = try setupTestDb();
    defer database.close();

    // Entry 1: debit Cash 1000, credit AP 1000
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    // Entry 2: debit Cash 500, credit AP 500
    _ = try Entry.createDraft(database, 1, "JE-002", "2026-01-16", "2026-01-16", null, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 1, 500_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 2, "admin");

    // Cash should have 1500 debit, AP should have 1500 credit
    var stmt = try database.prepare("SELECT debit_sum, entry_count FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_500_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(1));
}

// ── voidEntry tests ─────────────────────────────────────────────

test "voidEntry voids a posted entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    try Entry.voidEntry(database, 1, "Entered in error", "admin");

    var stmt = try database.prepare("SELECT status, void_reason FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("void", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Entered in error", stmt.columnText(1).?);
}

test "voidEntry reverses balance cache" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    var stmt = try database.prepare("SELECT debit_sum, credit_sum, entry_count FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(2));
}

test "voidEntry rejects draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.voidEntry(database, 1, "Error", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "voidEntry rejects empty reason" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.voidEntry(database, 1, "", "admin");
    try std.testing.expectError(error.VoidReasonRequired, result);
}

test "voidEntry writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    var stmt = try database.prepare("SELECT action, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'void';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("void", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("void", stmt.columnText(1).?);
}

// ── reverse tests ───────────────────────────────────────────────

test "reverse creates new entry with flipped lines" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const reversal_id = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", "admin");
    try std.testing.expect(reversal_id > 1);

    // Original entry should be 'reversed'
    {
        var stmt = try database.prepare("SELECT status, reversed_reason FROM ledger_entries WHERE id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("reversed", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("Accrual reversal", stmt.columnText(1).?);
    }

    // Reversal entry should be 'posted' and reference original
    {
        var stmt = try database.prepare("SELECT status, reverses_entry_id FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, reversal_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
        try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(1));
    }

    // Reversal lines should have flipped debits/credits
    {
        var stmt = try database.prepare("SELECT debit_amount, credit_amount FROM ledger_entry_lines WHERE entry_id = ? ORDER BY line_number;");
        defer stmt.finalize();
        try stmt.bindInt(1, reversal_id);

        // Line 1: original was debit, reversal should be credit
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
        try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(1));

        // Line 2: original was credit, reversal should be debit
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
        try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    }
}

test "reverse rejects draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.reverse(database, 1, "Reason", "2026-01-31", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "reverse rejects empty reason" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.reverse(database, 1, "", "2026-01-31", "admin");
    try std.testing.expectError(error.ReverseReasonRequired, result);
}

test "reverse balance cache nets to zero" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    _ = try Entry.reverse(database, 1, "Reversal", "2026-01-31", "admin");

    // Cash account: debit 1000 from original + credit 1000 from reversal = net 0
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0)); // original debit
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(1)); // reversal credit
}

// ── Multi-currency tests ────────────────────────────────────────

test "post with FX rate computes correct base amounts" {
    const database = try setupTestDb();
    defer database.close();

    // 100 USD at 56.50 PHP/USD = 5,650 PHP
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const usd_amount: i64 = 10_000_000_000; // 100.00 * 10^8
    const fx: i64 = 565_000_000_000; // 56.50 * 10^10
    _ = try Entry.addLine(database, 1, 1, usd_amount, 0, "USD", fx, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, usd_amount, "USD", fx, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT base_debit_amount FROM ledger_entry_lines WHERE line_number = 1 AND entry_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    // 10_000_000_000 * 565_000_000_000 / 10_000_000_000 = 565_000_000_000 (5,650.00)
    try std.testing.expectEqual(@as(i64, 565_000_000_000), stmt.columnInt64(0));
}

// ── Edge case tests ─────────────────────────────────────────────

test "void already-voided entry rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    const result = Entry.voidEntry(database, 1, "Double void", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "reverse already-reversed entry rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Reversal", "2026-01-31", "admin");

    const result = Entry.reverse(database, 1, "Double reverse", "2026-01-31", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "post 3-line entry: split payment" {
    const database = try setupTestDb();
    defer database.close();

    // Create tax payable account
    _ = try account_mod.Account.create(database, 1, "2100", "Tax Payable", .liability, false, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Split payment", 1, null, "admin");
    // Debit Cash 1000
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    // Credit AP 800
    _ = try Entry.addLine(database, 1, 2, 0, 800_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    // Credit Tax Payable 200
    _ = try Entry.addLine(database, 1, 3, 0, 200_000_000_00, "PHP", money.FX_RATE_SCALE, 3, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);

    // Verify all 3 accounts have cache entries
    var count_stmt = try database.prepare("SELECT COUNT(*) FROM ledger_account_balances WHERE period_id = 1;");
    defer count_stmt.finalize();
    _ = try count_stmt.step();
    try std.testing.expectEqual(@as(i32, 3), count_stmt.columnInt(0));
}

test "addLine rejects account from different book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try book_mod.Book.create(database, "Book B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, 2, "1000", "Cash USD", .asset, false, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    // Account 3 belongs to book 2, entry belongs to book 1
    const result = Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 3, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "post entry with zero-amount line rejected by schema" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    // Both debit and credit = 0 violates CHECK constraint
    const result = Entry.addLine(database, 1, 1, 0, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    try std.testing.expectError(error.SqliteStepFailed, result);
}

test "full lifecycle: create -> post -> void -> verify cache zero" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 5_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Cancelled", "admin");

    // All cache entries should be zero
    var stmt = try database.prepare("SELECT SUM(debit_sum), SUM(credit_sum), SUM(entry_count) FROM ledger_account_balances;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(2));
}

test "full lifecycle: create -> post -> reverse -> verify cache balanced" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 2_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", "admin");

    // For each account, debit_sum should equal credit_sum (net zero)
    var stmt = try database.prepare("SELECT account_id, debit_sum, credit_sum FROM ledger_account_balances ORDER BY account_id;");
    defer stmt.finalize();

    // Cash: debit 2000 (original) + credit 2000 (reversal)
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(2));

    // AP: credit 2000 (original) + debit 2000 (reversal)
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(2));
}

// ── Business edge case tests ────────────────────────────────────

test "post, void, then post new entry — cache correct" {
    const database = try setupTestDb();
    defer database.close();

    // Post entry 1: debit Cash 1000, credit AP 1000
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    // Void it
    try Entry.voidEntry(database, 1, "Wrong amount", "admin");

    // Post entry 2: debit Cash 2000, credit AP 2000
    _ = try Entry.createDraft(database, 1, "JE-002", "2026-01-16", "2026-01-16", null, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 2, 0, 2_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 2, "admin");

    // Cache should only reflect entry 2 (entry 1 voided)
    var stmt = try database.prepare("SELECT debit_sum, credit_sum, entry_count FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(2));
}

test "reverse 3-line entry creates correct flipped lines" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "2100", "Tax Payable", .liability, false, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 800_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    _ = try Entry.addLine(database, 1, 3, 0, 200_000_000_00, "PHP", money.FX_RATE_SCALE, 3, null, "admin");
    try Entry.post(database, 1, "admin");

    const rev_id = try Entry.reverse(database, 1, "Reversal", "2026-01-31", "admin");

    // Reversal should have 3 lines
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, rev_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

test "compound entry: two lines on same account" {
    const database = try setupTestDb();
    defer database.close();

    // Partial payment: debit Cash 1000, credit AP 700 + credit AP 300
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 700_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    _ = try Entry.addLine(database, 1, 3, 0, 300_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    // AP balance cache should show total credit of 1000
    var stmt = try database.prepare("SELECT credit_sum FROM ledger_account_balances WHERE account_id = 2 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
}

test "multi-currency void reverses base amounts correctly" {
    const database = try setupTestDb();
    defer database.close();

    const usd_amount: i64 = 10_000_000_000; // 100.00 USD
    const fx: i64 = 565_000_000_000; // 56.50 PHP/USD

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, usd_amount, 0, "USD", fx, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, usd_amount, "USD", fx, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "FX error", "admin");

    // Cache should be zero after void
    var stmt = try database.prepare("SELECT debit_sum, credit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));
}

test "addLine writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'entry_line';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry_line", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);
}

test "reverse writes audit log for both entries" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", "admin");

    // Original entry: reverse action
    {
        var stmt = try database.prepare("SELECT action, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'reverse';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("reverse", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("posted", stmt.columnText(1).?);
        try std.testing.expectEqualStrings("reversed", stmt.columnText(2).?);
    }

    // Reversal entry: create action
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'create';");
        defer stmt.finalize();
        _ = try stmt.step();
        // 2 creates: original draft + reversal entry
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }
}

test "full posting lifecycle audit trail in order" {
    const database = try setupTestDb();
    defer database.close();

    // Create draft + lines + post + void
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    var stmt = try database.prepare(
        \\SELECT entity_type, action FROM ledger_audit_log
        \\WHERE entity_type IN ('entry', 'entry_line')
        \\ORDER BY id;
    );
    defer stmt.finalize();

    // 1: entry create
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    // 2: line 1 create
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry_line", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    // 3: line 2 create
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry_line", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("create", stmt.columnText(1).?);

    // 4: entry post
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("post", stmt.columnText(1).?);

    // 5: entry void (status change)
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("void", stmt.columnText(1).?);

    // 6: entry void (reason)
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("void", stmt.columnText(1).?);

    // No more rows
    const more = try stmt.step();
    try std.testing.expect(!more);
}

// ── Validation gap tests ────────────────────────────────────────

test "createDraft rejects empty document_number" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.createDraft(database, 1, "", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "createDraft rejects posting_date outside period range" {
    const database = try setupTestDb();
    defer database.close();

    // Period is Jan 2026 (01-01 to 01-31), posting_date is Feb
    const result = Entry.createDraft(database, 1, "JE-001", "2026-02-15", "2026-02-15", null, 1, null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "createDraft accepts posting_date at period boundaries" {
    const database = try setupTestDb();
    defer database.close();

    // Start of period
    const id1 = try Entry.createDraft(database, 1, "JE-001", "2026-01-01", "2026-01-01", null, 1, null, "admin");
    try std.testing.expect(id1 > 0);

    // End of period
    const id2 = try Entry.createDraft(database, 1, "JE-002", "2026-01-31", "2026-01-31", null, 1, null, "admin");
    try std.testing.expect(id2 > 0);
}

// ── Draft editing tests ─────────────────────────────────────────

test "removeLine removes line from draft" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.removeLine(database, line_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "removeLine rejects line on posted entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.removeLine(database, line_id, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "removeLine writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");

    try Entry.removeLine(database, line_id, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'entry_line' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry_line", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("delete", stmt.columnText(1).?);
}

test "removeLine rejects nonexistent line" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.removeLine(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "deleteDraft deletes entry and all lines" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.deleteDraft(database, 1, "admin");

    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entries;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }
}

test "deleteDraft rejects posted entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.deleteDraft(database, 1, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "deleteDraft writes audit log for entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    try Entry.deleteDraft(database, 1, "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("entry", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("delete", stmt.columnText(1).?);
}

test "deleteDraft rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.deleteDraft(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── Multi-currency cross-currency test ──────────────────────────

test "post with different currencies per line balances in base" {
    const database = try setupTestDb();
    defer database.close();

    // Line 1: Debit Cash-USD 100.00 at 56.50 PHP/USD = 5,650.00 PHP base
    // Line 2: Credit Revenue 5,650.00 PHP at 1.00 = 5,650.00 PHP base
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 565_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, "admin");

    try Entry.post(database, 1, "admin");

    // Both base amounts should be 565_000_000_000
    var stmt = try database.prepare("SELECT base_debit_amount, base_credit_amount FROM ledger_entry_lines WHERE entry_id = 1 ORDER BY line_number;");
    defer stmt.finalize();

    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 565_000_000_000), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(1));

    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
    try std.testing.expectEqual(@as(i64, 565_000_000_000), stmt.columnInt64(1));
}

// ── Void/Reverse reason audit tests ─────────────────────────────

test "voidEntry records reason in audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Entered in error", "admin");

    var stmt = try database.prepare("SELECT field_changed, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'void' AND field_changed = 'void_reason';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("void_reason", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Entered in error", stmt.columnText(1).?);
}

test "reverse records reason in audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", "admin");

    var stmt = try database.prepare("SELECT field_changed, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'reverse' AND field_changed = 'reversed_reason';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("reversed_reason", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Accrual reversal", stmt.columnText(1).?);
}

test "post entry then close period — entry stays posted" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, "admin");
    try Entry.post(database, 1, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    // Entry should still be posted
    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}
