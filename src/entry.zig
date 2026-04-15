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
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, metadata, entry_type, period_id, book_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    ;

    pub const EntryType = enum {
        standard,
        opening,
        closing,
        reversal,
        adjusting,

        pub fn toString(self: EntryType) []const u8 {
            return switch (self) {
                .standard => "standard",
                .opening => "opening",
                .closing => "closing",
                .reversal => "reversal",
                .adjusting => "adjusting",
            };
        }
    };

    const line_sql: [*:0]const u8 =
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, entry_id, description, counterparty_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    ;

    const line_with_base_sql: [*:0]const u8 =
        \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, base_debit_amount, base_credit_amount, transaction_currency, fx_rate, account_id, entry_id, description, counterparty_id)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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

    const balance_delta_sql: [*:0]const u8 =
        \\INSERT INTO ledger_account_balances (account_id, period_id, debit_sum, credit_sum, balance, entry_count, book_id)
        \\VALUES (?, ?, ?, ?, ? - ?, ?, ?)
        \\ON CONFLICT (account_id, period_id) DO UPDATE SET
        \\  debit_sum = debit_sum + excluded.debit_sum,
        \\  credit_sum = credit_sum + excluded.credit_sum,
        \\  balance = balance + excluded.balance,
        \\  entry_count = entry_count + excluded.entry_count,
        \\  is_stale = 0,
        \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now');
    ;

    const CacheDelta = struct {
        debit_sum: i64 = 0,
        credit_sum: i64 = 0,
        entry_count: i64 = 0,
    };

    const PostContext = struct {
        period_id: i64,
        entry_book_id: i64,
        rounding_account_id: i64,
        is_opening_entry: bool,
        period_end_date_buf: [11]u8,
        period_end_date_len: usize,

        fn periodEndDate(self: *const PostContext) []const u8 {
            return self.period_end_date_buf[0..self.period_end_date_len];
        }
    };

    const PostingLineTotals = struct {
        total_base_debits: i64 = 0,
        total_base_credits: i64 = 0,
        line_count: i32 = 0,
    };

    const MaxCacheDeltaAccounts: usize = 500;

    /// Verifies an account exists, is active (not inactive/archived), and belongs
    /// to the expected book. Returns NotFound if missing, AccountInactive if not
    /// active, CrossBookViolation if the account is in a different book.
    /// Used by addLine, editLine, and post() rounding-line auto-insertion so all
    /// three call sites share the same validation semantics.
    fn verifyAccountPostable(database: db.Database, account_id: i64, expected_book_id: i64) !void {
        var stmt = try database.prepare("SELECT status, book_id FROM ledger_accounts WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, account_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        const status = stmt.columnText(0).?;
        if (std.mem.eql(u8, status, "inactive") or std.mem.eql(u8, status, "archived")) return error.AccountInactive;
        if (stmt.columnInt64(1) != expected_book_id) return error.CrossBookViolation;
    }

    fn markFuturePeriodsStale(database: db.Database, book_id: i64, period_end_date: []const u8) !void {
        var stale_stmt = try database.prepare(
            \\UPDATE ledger_account_balances
            \\SET is_stale = 1, stale_since = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            \\WHERE book_id = ? AND is_stale = 0
            \\  AND period_id IN (
            \\    SELECT id FROM ledger_periods
            \\    WHERE book_id = ? AND start_date > ?
            \\  );
        );
        defer stale_stmt.finalize();
        try stale_stmt.bindInt(1, book_id);
        try stale_stmt.bindInt(2, book_id);
        try stale_stmt.bindText(3, period_end_date);
        _ = try stale_stmt.step();
    }

    fn insertLine(database: db.Database, entry_id: i64, entry_book_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: []const u8, fx_rate: i64, account_id: i64, counterparty_id: ?i64, description: ?[]const u8, performed_by: []const u8) !i64 {
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
        if (counterparty_id) |cp| try stmt.bindInt(9, cp) else try stmt.bindNull(9);

        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "entry_line", id, "create", null, null, null, performed_by, entry_book_id);
        return id;
    }

    fn accumulateCacheDelta(cache_deltas: *std.AutoHashMap(i64, CacheDelta), account_id: i64, debit_amount: i64, credit_amount: i64, entry_count_delta: i64) !void {
        if (cache_deltas.getPtr(account_id) == null and cache_deltas.count() >= MaxCacheDeltaAccounts) return error.TooManyAccounts;
        const gop = try cache_deltas.getOrPut(account_id);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        gop.value_ptr.debit_sum = std.math.add(i64, gop.value_ptr.debit_sum, debit_amount) catch return error.AmountOverflow;
        gop.value_ptr.credit_sum = std.math.add(i64, gop.value_ptr.credit_sum, credit_amount) catch return error.AmountOverflow;
        gop.value_ptr.entry_count = std.math.add(i64, gop.value_ptr.entry_count, entry_count_delta) catch return error.AmountOverflow;
    }

    fn flushCacheDeltas(database: db.Database, cache_deltas: *std.AutoHashMap(i64, CacheDelta), period_id: i64, book_id: i64) !void {
        var cache_stmt = try database.prepare(balance_delta_sql);
        defer cache_stmt.finalize();

        var iter = cache_deltas.iterator();
        while (iter.next()) |entry| {
            const delta = entry.value_ptr.*;
            try cache_stmt.bindInt(1, entry.key_ptr.*);
            try cache_stmt.bindInt(2, period_id);
            try cache_stmt.bindInt(3, delta.debit_sum);
            try cache_stmt.bindInt(4, delta.credit_sum);
            try cache_stmt.bindInt(5, delta.debit_sum);
            try cache_stmt.bindInt(6, delta.credit_sum);
            try cache_stmt.bindInt(7, delta.entry_count);
            try cache_stmt.bindInt(8, book_id);
            _ = try cache_stmt.step();
            cache_stmt.reset();
            cache_stmt.clearBindings();
        }
    }

    fn loadPostContext(database: db.Database, entry_id: i64) !PostContext {
        var ctx = PostContext{
            .period_id = 0,
            .entry_book_id = 0,
            .rounding_account_id = 0,
            .is_opening_entry = false,
            .period_end_date_buf = undefined,
            .period_end_date_len = 0,
        };

        var stmt = try database.prepare(
            \\SELECT e.status, e.period_id, e.book_id, e.entry_type, e.approval_status,
            \\  b.require_approval, b.rounding_account_id, p.status, p.end_date
            \\FROM ledger_entries e
            \\JOIN ledger_books b ON b.id = e.book_id
            \\JOIN ledger_periods p ON p.id = e.period_id
            \\WHERE e.id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
        if (status != .draft) return error.AlreadyPosted;
        ctx.period_id = stmt.columnInt64(1);
        ctx.entry_book_id = stmt.columnInt64(2);
        ctx.rounding_account_id = stmt.columnInt64(6);
        if (stmt.columnText(3)) |et| {
            if (std.mem.eql(u8, et, "opening")) ctx.is_opening_entry = true;
        }

        if (stmt.columnInt(5) == 1) {
            const approval = stmt.columnText(4).?;
            if (!std.mem.eql(u8, approval, "approved")) return error.ApprovalRequired;
        }

        const period_status = stmt.columnText(7).?;
        if (std.mem.eql(u8, period_status, "closed")) return error.PeriodClosed;
        if (std.mem.eql(u8, period_status, "locked")) return error.PeriodLocked;
        if (stmt.columnText(8)) |end_date| {
            ctx.period_end_date_len = @min(end_date.len, ctx.period_end_date_buf.len);
            @memcpy(ctx.period_end_date_buf[0..ctx.period_end_date_len], end_date[0..ctx.period_end_date_len]);
        }

        return ctx;
    }

    fn processPostingLines(database: db.Database, entry_id: i64, is_opening_entry: bool, cache_deltas: *std.AutoHashMap(i64, CacheDelta)) !PostingLineTotals {
        var totals = PostingLineTotals{};

        var read_stmt = try database.prepare(
            \\SELECT el.id, el.account_id, el.debit_amount, el.credit_amount, el.base_debit_amount, el.base_credit_amount, el.fx_rate,
            \\  el.counterparty_id, cp.status,
            \\  (SELECT COUNT(*) FROM ledger_subledger_groups sg WHERE sg.gl_account_id = el.account_id)
            \\FROM ledger_entry_lines el
            \\LEFT JOIN ledger_subledger_accounts cp ON cp.id = el.counterparty_id
            \\WHERE el.entry_id = ?;
        );
        defer read_stmt.finalize();
        try read_stmt.bindInt(1, entry_id);

        var update_stmt = try database.prepare("UPDATE ledger_entry_lines SET base_debit_amount = ?, base_credit_amount = ? WHERE id = ?;");
        defer update_stmt.finalize();

        while (try read_stmt.step()) {
            totals.line_count += 1;
            const line_id = read_stmt.columnInt64(0);
            const acct_id = read_stmt.columnInt64(1);
            const debit = read_stmt.columnInt64(2);
            const credit = read_stmt.columnInt64(3);
            const stored_base_debit = read_stmt.columnInt64(4);
            const stored_base_credit = read_stmt.columnInt64(5);
            const fx_rate = read_stmt.columnInt64(6);
            const cp_id_raw = read_stmt.columnInt64(7);
            const cp_status = read_stmt.columnText(8);
            const has_counterparty = cp_id_raw != 0;
            const is_control = read_stmt.columnInt(9) > 0;

            if (!is_opening_entry) {
                if (is_control and !has_counterparty) return error.MissingCounterparty;
                if (!is_control and has_counterparty) return error.InvalidCounterparty;
            }

            if (has_counterparty) {
                if (cp_status == null or !std.mem.eql(u8, cp_status.?, "active")) return error.AccountInactive;
            }

            const base_debit = try money.computeBaseAmount(debit, fx_rate);
            const base_credit = try money.computeBaseAmount(credit, fx_rate);

            if (stored_base_debit != base_debit or stored_base_credit != base_credit) {
                try update_stmt.bindInt(1, base_debit);
                try update_stmt.bindInt(2, base_credit);
                try update_stmt.bindInt(3, line_id);
                _ = try update_stmt.step();
                update_stmt.reset();
                update_stmt.clearBindings();
            }

            totals.total_base_debits = std.math.add(i64, totals.total_base_debits, base_debit) catch return error.AmountOverflow;
            totals.total_base_credits = std.math.add(i64, totals.total_base_credits, base_credit) catch return error.AmountOverflow;

            if (!is_opening_entry) try accumulateCacheDelta(cache_deltas, acct_id, base_debit, base_credit, 1);
        }

        return totals;
    }

    fn maybeApplyRoundingAdjustment(database: db.Database, entry_id: i64, performed_by: []const u8, post_ctx: PostContext, totals: *PostingLineTotals, cache_deltas: *std.AutoHashMap(i64, CacheDelta)) !void {
        if (totals.total_base_debits == totals.total_base_credits) return;

        const diff = std.math.sub(i64, totals.total_base_debits, totals.total_base_credits) catch return error.AmountOverflow;
        const abs_diff: i64 = if (diff == std.math.minInt(i64)) return error.AmountOverflow else if (diff < 0) -diff else diff;
        const max_rounding: i64 = 100;
        if (abs_diff > max_rounding) return error.UnbalancedEntry;

        const rounding_acct_id: ?i64 = if (post_ctx.rounding_account_id > 0) post_ctx.rounding_account_id else null;
        const ra_id = rounding_acct_id orelse return error.UnbalancedEntry;
        try verifyAccountPostable(database, ra_id, post_ctx.entry_book_id);

        const next_line = totals.line_count + 1;
        var rounding_debit: i64 = 0;
        var rounding_credit: i64 = 0;
        if (diff > 0) {
            rounding_credit = diff;
        } else {
            rounding_debit = std.math.negate(diff) catch return error.AmountOverflow;
        }

        var round_stmt = try database.prepare(
            \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount,
            \\  base_debit_amount, base_credit_amount, transaction_currency, fx_rate,
            \\  account_id, entry_id, description)
            \\VALUES (?, ?, ?, ?, ?, (SELECT base_currency FROM ledger_books WHERE id = ?),
            \\  10000000000, ?, ?, 'FX rounding adjustment');
        );
        defer round_stmt.finalize();
        try round_stmt.bindInt(1, @intCast(next_line));
        try round_stmt.bindInt(2, rounding_debit);
        try round_stmt.bindInt(3, rounding_credit);
        try round_stmt.bindInt(4, rounding_debit);
        try round_stmt.bindInt(5, rounding_credit);
        try round_stmt.bindInt(6, post_ctx.entry_book_id);
        try round_stmt.bindInt(7, ra_id);
        try round_stmt.bindInt(8, entry_id);
        _ = try round_stmt.step();

        const rounding_line_id = database.lastInsertRowId();
        try audit.log(database, "entry_line", rounding_line_id, "create", null, null, "FX rounding auto-post", performed_by, post_ctx.entry_book_id);
        try accumulateCacheDelta(cache_deltas, ra_id, rounding_debit, rounding_credit, 1);
    }

    fn markEntryPosted(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        var stmt = try database.prepare("UPDATE ledger_entries SET status = 'posted', posted_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), posted_by = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindText(1, performed_by);
        try stmt.bindInt(2, entry_id);
        _ = try stmt.step();
    }

    pub const LineAppender = struct {
        database: db.Database,
        line_stmt: db.Statement,
        audit_stmt: db.Statement,
        entry_id: i64,
        entry_book_id: i64,
        performed_by: []const u8,

        pub fn init(database: db.Database, entry_id: i64, entry_book_id: i64, performed_by: []const u8) !LineAppender {
            return .{
                .database = database,
                .line_stmt = try database.prepare(line_with_base_sql),
                .audit_stmt = try database.prepare(audit.insert_sql),
                .entry_id = entry_id,
                .entry_book_id = entry_book_id,
                .performed_by = performed_by,
            };
        }

        pub fn deinit(self: *LineAppender) void {
            self.line_stmt.finalize();
            self.audit_stmt.finalize();
        }

        pub fn add(self: *LineAppender, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: []const u8, fx_rate: i64, account_id: i64, counterparty_id: ?i64, description: ?[]const u8) !i64 {
            if (debit_amount < 0 or credit_amount < 0) return error.InvalidAmount;
            if ((debit_amount > 0 and credit_amount > 0) or (debit_amount == 0 and credit_amount == 0)) return error.InvalidAmount;

            const base_debit = try money.computeBaseAmount(debit_amount, fx_rate);
            const base_credit = try money.computeBaseAmount(credit_amount, fx_rate);

            try self.line_stmt.bindInt(1, @intCast(line_number));
            try self.line_stmt.bindInt(2, debit_amount);
            try self.line_stmt.bindInt(3, credit_amount);
            try self.line_stmt.bindInt(4, base_debit);
            try self.line_stmt.bindInt(5, base_credit);
            try self.line_stmt.bindText(6, transaction_currency);
            try self.line_stmt.bindInt(7, fx_rate);
            try self.line_stmt.bindInt(8, account_id);
            try self.line_stmt.bindInt(9, self.entry_id);
            if (description) |d| try self.line_stmt.bindText(10, d) else try self.line_stmt.bindNull(10);
            if (counterparty_id) |cp| try self.line_stmt.bindInt(11, cp) else try self.line_stmt.bindNull(11);

            _ = try self.line_stmt.step();
            self.line_stmt.reset();
            self.line_stmt.clearBindings();

            const id = self.database.lastInsertRowId();
            try audit.logWithStmt(self.database, &self.audit_stmt, "entry_line", id, "create", null, null, null, self.performed_by, self.entry_book_id);
            return id;
        }
    };

    pub fn createDraft(database: db.Database, book_id: i64, document_number: []const u8, transaction_date: []const u8, posting_date: []const u8, description: ?[]const u8, period_id: i64, metadata: ?[]const u8, performed_by: []const u8) !i64 {
        return createDraftAs(database, book_id, document_number, transaction_date, posting_date, description, period_id, metadata, .standard, performed_by);
    }

    pub fn createDraftAs(database: db.Database, book_id: i64, document_number: []const u8, transaction_date: []const u8, posting_date: []const u8, description: ?[]const u8, period_id: i64, metadata: ?[]const u8, entry_type: EntryType, performed_by: []const u8) !i64 {
        if (document_number.len == 0) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        // Verify book exists and is active
        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
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

        var stmt = try database.prepare(create_sql);
        defer stmt.finalize();

        try stmt.bindText(1, document_number);
        try stmt.bindText(2, transaction_date);
        try stmt.bindText(3, posting_date);
        if (description) |d| try stmt.bindText(4, d) else try stmt.bindNull(4);
        if (metadata) |m| try stmt.bindText(5, m) else try stmt.bindNull(5);
        try stmt.bindText(6, entry_type.toString());
        try stmt.bindInt(7, period_id);
        try stmt.bindInt(8, book_id);

        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "entry", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn addLine(database: db.Database, entry_id: i64, line_number: i32, debit_amount: i64, credit_amount: i64, transaction_currency: []const u8, fx_rate: i64, account_id: i64, counterparty_id: ?i64, description: ?[]const u8, performed_by: []const u8) !i64 {
        // Rule 16 / CLAUDE.md: validate before mutation. The schema CHECK also
        // enforces this, but a Zig-layer guard returns a meaningful domain error
        // instead of a generic SqliteStepFailed.
        if (debit_amount < 0 or credit_amount < 0) return error.InvalidAmount;
        if ((debit_amount > 0 and credit_amount > 0) or (debit_amount == 0 and credit_amount == 0)) return error.InvalidAmount;

        // Verify entry exists and is draft
        var entry_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

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
        try verifyAccountPostable(database, account_id, entry_book_id);
        const id = try insertLine(database, entry_id, entry_book_id, line_number, debit_amount, credit_amount, transaction_currency, fx_rate, account_id, counterparty_id, description, performed_by);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn removeLine(database: db.Database, line_id: i64, performed_by: []const u8) !void {
        var entry_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

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
            const status = stmt.columnText(1).?;
            if (!std.mem.eql(u8, status, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(2);
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_entry_lines WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            _ = try stmt.step();
        }

        try audit.log(database, "entry_line", line_id, "delete", null, null, null, performed_by, entry_book_id);

        if (owns_txn) try database.commit();
    }

    const amt_buf_len = 24;

    pub fn editLine(database: db.Database, line_id: i64, debit_amount: i64, credit_amount: i64, transaction_currency: []const u8, fx_rate: i64, account_id: i64, counterparty_id: ?i64, description: ?[]const u8, performed_by: []const u8) !void {
        // Rule 16 / CLAUDE.md: validate before mutation. Same guard as addLine.
        if (debit_amount < 0 or credit_amount < 0) return error.InvalidAmount;
        if ((debit_amount > 0 and credit_amount > 0) or (debit_amount == 0 and credit_amount == 0)) return error.InvalidAmount;

        // Fetch line's entry, verify draft, and read old values
        var entry_book_id: i64 = 0;
        var old_debit: i64 = 0;
        var old_credit: i64 = 0;
        var old_fx: i64 = 0;
        var old_account: i64 = 0;
        var old_counterparty: i64 = 0;
        var old_currency_buf: [16]u8 = undefined;
        var old_currency_len: usize = 0;
        var old_desc_buf: [1001]u8 = undefined;
        var old_desc_len: usize = 0;
        var old_has_desc = false;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT e.status, e.book_id, el.debit_amount, el.credit_amount,
                \\  el.transaction_currency, el.fx_rate, el.account_id,
                \\  el.counterparty_id, el.description
                \\FROM ledger_entry_lines el
                \\JOIN ledger_entries e ON e.id = el.entry_id
                \\WHERE el.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (!std.mem.eql(u8, stmt.columnText(0).?, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(1);
            old_debit = stmt.columnInt64(2);
            old_credit = stmt.columnInt64(3);
            const cur = stmt.columnText(4).?;
            const copy_len = @min(cur.len, old_currency_buf.len);
            @memcpy(old_currency_buf[0..copy_len], cur[0..copy_len]);
            old_currency_len = copy_len;
            old_fx = stmt.columnInt64(5);
            old_account = stmt.columnInt64(6);
            old_counterparty = stmt.columnInt64(7);
            if (stmt.columnText(8)) |d| {
                old_has_desc = true;
                old_desc_len = @min(d.len, old_desc_buf.len);
                @memcpy(old_desc_buf[0..old_desc_len], d[0..old_desc_len]);
            }
        }

        // Verify account exists, is active, and belongs to same book
        try verifyAccountPostable(database, account_id, entry_book_id);

        {
            var stmt = try database.prepare(
                \\UPDATE ledger_entry_lines SET
                \\  debit_amount = ?, credit_amount = ?,
                \\  transaction_currency = ?, fx_rate = ?,
                \\  account_id = ?, counterparty_id = ?, description = ?,
                \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, debit_amount);
            try stmt.bindInt(2, credit_amount);
            try stmt.bindText(3, transaction_currency);
            try stmt.bindInt(4, fx_rate);
            try stmt.bindInt(5, account_id);
            if (counterparty_id) |cp| try stmt.bindInt(6, cp) else try stmt.bindNull(6);
            if (description) |d| try stmt.bindText(7, d) else try stmt.bindNull(7);
            try stmt.bindInt(8, line_id);
            _ = try stmt.step();
        }

        // Audit each changed field — prepare audit statement ONCE, reuse for all fields
        var old_buf: [amt_buf_len]u8 = undefined;
        var new_buf: [amt_buf_len]u8 = undefined;
        {
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();

            if (old_debit != debit_amount) {
                const old_s = std.fmt.bufPrint(&old_buf, "{d}", .{old_debit}) catch unreachable;
                const new_s = std.fmt.bufPrint(&new_buf, "{d}", .{debit_amount}) catch unreachable;
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "debit_amount", old_s, new_s, performed_by, entry_book_id);
            }
            if (old_credit != credit_amount) {
                const old_s = std.fmt.bufPrint(&old_buf, "{d}", .{old_credit}) catch unreachable;
                const new_s = std.fmt.bufPrint(&new_buf, "{d}", .{credit_amount}) catch unreachable;
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "credit_amount", old_s, new_s, performed_by, entry_book_id);
            }
            if (!std.mem.eql(u8, old_currency_buf[0..old_currency_len], transaction_currency)) {
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "transaction_currency", old_currency_buf[0..old_currency_len], transaction_currency, performed_by, entry_book_id);
            }
            if (old_fx != fx_rate) {
                const old_s = std.fmt.bufPrint(&old_buf, "{d}", .{old_fx}) catch unreachable;
                const new_s = std.fmt.bufPrint(&new_buf, "{d}", .{fx_rate}) catch unreachable;
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "fx_rate", old_s, new_s, performed_by, entry_book_id);
            }
            if (old_account != account_id) {
                const old_s = std.fmt.bufPrint(&old_buf, "{d}", .{old_account}) catch unreachable;
                const new_s = std.fmt.bufPrint(&new_buf, "{d}", .{account_id}) catch unreachable;
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "account_id", old_s, new_s, performed_by, entry_book_id);
            }
            const new_cp = counterparty_id orelse 0;
            if (old_counterparty != new_cp) {
                const old_cp_s: ?[]const u8 = if (old_counterparty != 0) blk: {
                    break :blk std.fmt.bufPrint(&old_buf, "{d}", .{old_counterparty}) catch unreachable;
                } else null;
                const new_cp_s: ?[]const u8 = if (counterparty_id) |cp| blk: {
                    break :blk std.fmt.bufPrint(&new_buf, "{d}", .{cp}) catch unreachable;
                } else null;
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "counterparty_id", old_cp_s, new_cp_s, performed_by, entry_book_id);
            }
            const old_desc = if (old_has_desc) old_desc_buf[0..old_desc_len] else "";
            const new_desc = description orelse "";
            if (!std.mem.eql(u8, old_desc, new_desc)) {
                try audit.logWithStmt(database, &audit_stmt, "entry_line", line_id, "update", "description", if (old_has_desc) old_desc_buf[0..old_desc_len] else null, description, performed_by, entry_book_id);
            }
        }

        if (owns_txn) try database.commit();
    }

    pub fn deleteDraft(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        var entry_book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

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

        if (owns_txn) try database.commit();
    }

    pub fn post(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();
        const post_ctx = try loadPostContext(database, entry_id);

        var cache_deltas = std.AutoHashMap(i64, CacheDelta).init(std.heap.c_allocator);
        defer cache_deltas.deinit();

        var totals = try processPostingLines(database, entry_id, post_ctx.is_opening_entry, &cache_deltas);
        if (totals.line_count < 2) return error.TooFewLines;

        try maybeApplyRoundingAdjustment(database, entry_id, performed_by, post_ctx, &totals, &cache_deltas);

        if (!post_ctx.is_opening_entry) {
            try flushCacheDeltas(database, &cache_deltas, post_ctx.period_id, post_ctx.entry_book_id);
        }

        try markEntryPosted(database, entry_id, performed_by);

        if (!post_ctx.is_opening_entry) {
            try markFuturePeriodsStale(database, post_ctx.entry_book_id, post_ctx.periodEndDate());
        }

        try audit.log(database, "entry", entry_id, "post", "status", "draft", "posted", performed_by, post_ctx.entry_book_id);

        if (owns_txn) try database.commit();
    }

    pub fn voidEntry(database: db.Database, entry_id: i64, reason: []const u8, performed_by: []const u8) !void {
        if (reason.len == 0) return error.VoidReasonRequired;

        var period_id: i64 = 0;
        var entry_book_id: i64 = 0;
        var is_opening_entry = false;
        var period_end_date_buf: [11]u8 = undefined;
        var period_end_date_len: usize = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT e.status, e.period_id, e.book_id, e.entry_type, p.end_date
                \\FROM ledger_entries e
                \\JOIN ledger_periods p ON p.id = e.period_id
                \\WHERE e.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status != .posted) return error.InvalidTransition;
            period_id = stmt.columnInt64(1);
            entry_book_id = stmt.columnInt64(2);
            // Opening entries skipped cache updates at post time; they must
            // also skip cache reversal at void time.
            if (stmt.columnText(3)) |et| {
                if (std.mem.eql(u8, et, "opening")) is_opening_entry = true;
            }
            if (stmt.columnText(4)) |end_date| {
                period_end_date_len = @min(end_date.len, period_end_date_buf.len);
                @memcpy(period_end_date_buf[0..period_end_date_len], end_date[0..period_end_date_len]);
            }
        }
        const period_end_date = period_end_date_buf[0..period_end_date_len];

        // Verify period is open or soft_closed
        {
            var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            _ = try stmt.step();
            const period_status = stmt.columnText(0).?;
            if (std.mem.eql(u8, period_status, "closed")) return error.PeriodClosed;
            if (std.mem.eql(u8, period_status, "locked")) return error.PeriodLocked;
        }

        // Reject void if entry has non-closed open items (prevents orphaned AR/AP)
        {
            var stmt = try database.prepare(
                \\SELECT COUNT(*) FROM ledger_open_items oi
                \\JOIN ledger_entry_lines el ON el.id = oi.entry_line_id
                \\WHERE el.entry_id = ? AND oi.status != 'closed';
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        // Update entry status
        {
            var stmt = try database.prepare("UPDATE ledger_entries SET status = 'void', void_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, reason);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        // Reverse balance cache using the POSTED base amounts.
        // Opening entries skipped cache updates at post time; skip reversal too.
        if (!is_opening_entry) {
            var line_stmt = try database.prepare("SELECT account_id, base_debit_amount, base_credit_amount FROM ledger_entry_lines WHERE entry_id = ?;");
            defer line_stmt.finalize();
            try line_stmt.bindInt(1, entry_id);
            // Bounded by the number of distinct accounts on one entry, which is
            // small in normal use and still transitively bounded by engine rules.
            var cache_deltas = std.AutoHashMap(i64, CacheDelta).init(std.heap.c_allocator);
            defer cache_deltas.deinit();

            while (try line_stmt.step()) {
                const acct_id = line_stmt.columnInt64(0);
                const base_debit = line_stmt.columnInt64(1);
                const base_credit = line_stmt.columnInt64(2);
                try accumulateCacheDelta(&cache_deltas, acct_id, -base_debit, -base_credit, -1);
            }

            try flushCacheDeltas(database, &cache_deltas, period_id, entry_book_id);
        }

        // Mark future periods stale
        {
            try markFuturePeriodsStale(database, entry_book_id, period_end_date);
        }

        {
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();
            try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "void", "status", "posted", "void", performed_by, entry_book_id);
            try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "void", "void_reason", null, reason, performed_by, entry_book_id);
        }

        if (owns_txn) try database.commit();
    }

    pub fn reverse(database: db.Database, entry_id: i64, reason: []const u8, reversal_date: []const u8, target_period_id: ?i64, performed_by: []const u8) !i64 {
        if (reason.len == 0) return error.ReverseReasonRequired;

        var original_period_id: i64 = 0;
        var entry_book_id: i64 = 0;
        var doc_number_buf: [128]u8 = undefined;
        var doc_number_len: usize = 0;
        var reversal_period_end_date_buf: [11]u8 = undefined;
        var reversal_period_end_date_len: usize = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status, period_id, book_id, document_number FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = EntryStatus.fromString(stmt.columnText(0).?) orelse return error.InvalidInput;
            if (status != .posted) return error.InvalidTransition;
            original_period_id = stmt.columnInt64(1);
            entry_book_id = stmt.columnInt64(2);
            const dn = stmt.columnText(3).?;
            const copy_len = @min(dn.len, doc_number_buf.len);
            @memcpy(doc_number_buf[0..copy_len], dn[0..copy_len]);
            doc_number_len = copy_len;
        }

        // Reversal period: use target if provided, otherwise original
        const reversal_period_id = target_period_id orelse original_period_id;

        // Guard: reject if the original entry's period is permanently locked.
        // reverse() only flips the original's status field (posted -> reversed)
        // without changing lines, base amounts, or cache in the original period,
        // so closed periods are allowed (the reversal's economic effect lands in
        // a different period via reversal_period_id). But locked means absolute
        // immutability — even a status field change is prohibited.
        {
            var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, original_period_id);
            _ = try stmt.step();
            const orig_period_status = stmt.columnText(0).?;
            if (std.mem.eql(u8, orig_period_status, "locked")) return error.PeriodLocked;
        }

        // Reject reversal if entry has non-closed open items (prevents orphaned AR/AP)
        {
            var stmt = try database.prepare(
                \\SELECT COUNT(*) FROM ledger_open_items oi
                \\JOIN ledger_entry_lines el ON el.id = oi.entry_line_id
                \\WHERE el.entry_id = ? AND oi.status != 'closed';
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        // Verify reversal period is open or soft_closed and reversal_date is within range
        {
            var stmt = try database.prepare("SELECT status, start_date, end_date FROM ledger_periods WHERE id = ? AND book_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, reversal_period_id);
            try stmt.bindInt(2, entry_book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const period_status = stmt.columnText(0).?;
            if (std.mem.eql(u8, period_status, "closed")) return error.PeriodClosed;
            if (std.mem.eql(u8, period_status, "locked")) return error.PeriodLocked;
            const start = stmt.columnText(1).?;
            const end = stmt.columnText(2).?;
            reversal_period_end_date_len = @min(end.len, reversal_period_end_date_buf.len);
            @memcpy(reversal_period_end_date_buf[0..reversal_period_end_date_len], end[0..reversal_period_end_date_len]);
            if (std.mem.order(u8, reversal_date, start) == .lt or std.mem.order(u8, reversal_date, end) == .gt) {
                return error.InvalidInput;
            }
        }
        const reversal_period_end_date = reversal_period_end_date_buf[0..reversal_period_end_date_len];

        // Mark original as reversed
        {
            var stmt = try database.prepare("UPDATE ledger_entries SET status = 'reversed', reversed_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, reason);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        // Create reversal entry
        var rev_doc_buf: [136]u8 = undefined;
        const max_doc_for_rev = @min(doc_number_len, 96); // REV- prefix + 96 = 100 (schema max)
        const rev_doc = std.fmt.bufPrint(&rev_doc_buf, "REV-{s}", .{doc_number_buf[0..max_doc_for_rev]}) catch return error.InvalidInput;
        {
            var stmt = try database.prepare(
                \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, description, status, reverses_entry_id, posted_at, posted_by, period_id, book_id, entry_type)
                \\VALUES (?, ?, ?, ?, 'posted', ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?, ?, 'reversal');
            );
            defer stmt.finalize();
            try stmt.bindText(1, rev_doc);
            try stmt.bindText(2, reversal_date);
            try stmt.bindText(3, reversal_date);
            try stmt.bindText(4, reason);
            try stmt.bindInt(5, entry_id);
            try stmt.bindText(6, performed_by);
            try stmt.bindInt(7, reversal_period_id);
            try stmt.bindInt(8, entry_book_id);
            _ = try stmt.step();
        }
        const reversal_id = database.lastInsertRowId();

        // Copy lines with flipped debits/credits
        {
            var read_stmt = try database.prepare("SELECT id, line_number, debit_amount, credit_amount, base_debit_amount, base_credit_amount, fx_rate, transaction_currency, account_id, description, counterparty_id FROM ledger_entry_lines WHERE entry_id = ?;");
            defer read_stmt.finalize();
            try read_stmt.bindInt(1, entry_id);

            var write_stmt = try database.prepare(
                \\INSERT INTO ledger_entry_lines (line_number, debit_amount, credit_amount, base_debit_amount, base_credit_amount, fx_rate, transaction_currency, account_id, entry_id, description, counterparty_id)
                \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            );
            defer write_stmt.finalize();

            var dim_copy_stmt = try database.prepare(
                \\INSERT INTO ledger_line_dimensions (line_id, dimension_value_id)
                \\SELECT ?, dimension_value_id FROM ledger_line_dimensions WHERE line_id = ?;
            );
            defer dim_copy_stmt.finalize();

            // Bounded by the number of distinct accounts on one entry, which is
            // small in normal use and still transitively bounded by engine rules.
            var cache_deltas = std.AutoHashMap(i64, CacheDelta).init(std.heap.c_allocator);
            defer cache_deltas.deinit();

            while (try read_stmt.step()) {
                const orig_line_id = read_stmt.columnInt64(0);
                const line_num = read_stmt.columnInt64(1);
                const orig_debit = read_stmt.columnInt64(2);
                const orig_credit = read_stmt.columnInt64(3);
                const orig_base_debit = read_stmt.columnInt64(4);
                const orig_base_credit = read_stmt.columnInt64(5);
                const fx_rate = read_stmt.columnInt64(6);
                const currency = read_stmt.columnText(7).?;
                const acct_id = read_stmt.columnInt64(8);
                const line_desc = read_stmt.columnText(9);
                const cp_id = read_stmt.columnInt64(10);

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
                if (line_desc) |d| try write_stmt.bindText(10, d) else try write_stmt.bindNull(10);
                if (cp_id != 0) try write_stmt.bindInt(11, cp_id) else try write_stmt.bindNull(11);
                _ = try write_stmt.step();
                const reversal_line_id = database.lastInsertRowId();
                write_stmt.reset();
                write_stmt.clearBindings();

                try dim_copy_stmt.bindInt(1, reversal_line_id);
                try dim_copy_stmt.bindInt(2, orig_line_id);
                _ = try dim_copy_stmt.step();
                dim_copy_stmt.reset();
                dim_copy_stmt.clearBindings();

                try accumulateCacheDelta(&cache_deltas, acct_id, orig_base_credit, orig_base_debit, 1);
            }

            try flushCacheDeltas(database, &cache_deltas, reversal_period_id, entry_book_id);
        }

        // Mark future periods stale
        {
            try markFuturePeriodsStale(database, entry_book_id, reversal_period_end_date);
        }

        {
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();
            try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "reverse", "status", "posted", "reversed", performed_by, entry_book_id);
            try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "reverse", "reversed_reason", null, reason, performed_by, entry_book_id);
            try audit.logWithStmt(database, &audit_stmt, "entry", reversal_id, "create", null, null, null, performed_by, entry_book_id);
            // The reversal entry is written directly with status='posted' (bypassing
            // the normal post() path). verify.zig Check 5a requires every posted/
            // reversed/void entry to have a 'post' audit record, so we log one
            // explicitly here to preserve the invariant.
            try audit.logWithStmt(database, &audit_stmt, "entry", reversal_id, "post", null, null, null, performed_by, entry_book_id);
        }

        if (owns_txn) try database.commit();
        return reversal_id;
    }

    pub fn editDraft(database: db.Database, entry_id: i64, document_number: []const u8, transaction_date: []const u8, posting_date: []const u8, description: ?[]const u8, metadata: ?[]const u8, period_id: i64, performed_by: []const u8) !void {
        if (document_number.len == 0) return error.InvalidInput;

        var entry_book_id: i64 = 0;
        var old_doc_buf: [128]u8 = undefined;
        var old_doc_len: usize = 0;
        var old_txn_date_buf: [16]u8 = undefined;
        var old_txn_date_len: usize = 0;
        var old_post_date_buf: [16]u8 = undefined;
        var old_post_date_len: usize = 0;
        var old_desc_buf: [1001]u8 = undefined;
        var old_desc_len: usize = 0;
        var old_has_desc = false;
        var old_meta_buf: [10001]u8 = undefined;
        var old_meta_len: usize = 0;
        var old_has_meta = false;
        var old_period_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT status, book_id, document_number, transaction_date,
                \\  posting_date, description, metadata, period_id
                \\FROM ledger_entries WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (!std.mem.eql(u8, stmt.columnText(0).?, "draft")) return error.AlreadyPosted;
            entry_book_id = stmt.columnInt64(1);

            const doc = stmt.columnText(2).?;
            old_doc_len = @min(doc.len, old_doc_buf.len);
            @memcpy(old_doc_buf[0..old_doc_len], doc[0..old_doc_len]);

            const txn = stmt.columnText(3).?;
            old_txn_date_len = @min(txn.len, old_txn_date_buf.len);
            @memcpy(old_txn_date_buf[0..old_txn_date_len], txn[0..old_txn_date_len]);

            const pdate = stmt.columnText(4).?;
            old_post_date_len = @min(pdate.len, old_post_date_buf.len);
            @memcpy(old_post_date_buf[0..old_post_date_len], pdate[0..old_post_date_len]);

            if (stmt.columnText(5)) |d| {
                old_has_desc = true;
                old_desc_len = @min(d.len, old_desc_buf.len);
                @memcpy(old_desc_buf[0..old_desc_len], d[0..old_desc_len]);
            }
            if (stmt.columnText(6)) |m| {
                old_has_meta = true;
                old_meta_len = @min(m.len, old_meta_buf.len);
                @memcpy(old_meta_buf[0..old_meta_len], m[0..old_meta_len]);
            }
            old_period_id = stmt.columnInt64(7);
        }

        // Validate posting_date within period date range
        {
            var stmt = try database.prepare("SELECT start_date, end_date FROM ledger_periods WHERE id = ? AND book_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            try stmt.bindInt(2, entry_book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const start = stmt.columnText(0).?;
            const end = stmt.columnText(1).?;
            if (std.mem.order(u8, posting_date, start) == .lt or std.mem.order(u8, posting_date, end) == .gt) {
                return error.InvalidInput;
            }
        }

        {
            var stmt = try database.prepare(
                \\UPDATE ledger_entries SET
                \\  document_number = ?, transaction_date = ?, posting_date = ?,
                \\  description = ?, metadata = ?, period_id = ?,
                \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindText(1, document_number);
            try stmt.bindText(2, transaction_date);
            try stmt.bindText(3, posting_date);
            if (description) |d| try stmt.bindText(4, d) else try stmt.bindNull(4);
            if (metadata) |m| try stmt.bindText(5, m) else try stmt.bindNull(5);
            try stmt.bindInt(6, period_id);
            try stmt.bindInt(7, entry_id);
            _ = stmt.step() catch return error.DuplicateNumber;
        }

        // Field-level audit for each changed field
        {
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();

            if (!std.mem.eql(u8, old_doc_buf[0..old_doc_len], document_number)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "document_number", old_doc_buf[0..old_doc_len], document_number, performed_by, entry_book_id);
            }
            if (!std.mem.eql(u8, old_txn_date_buf[0..old_txn_date_len], transaction_date)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "transaction_date", old_txn_date_buf[0..old_txn_date_len], transaction_date, performed_by, entry_book_id);
            }
            if (!std.mem.eql(u8, old_post_date_buf[0..old_post_date_len], posting_date)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "posting_date", old_post_date_buf[0..old_post_date_len], posting_date, performed_by, entry_book_id);
            }

            const old_desc = if (old_has_desc) old_desc_buf[0..old_desc_len] else "";
            const new_desc = description orelse "";
            if (!std.mem.eql(u8, old_desc, new_desc)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "description", if (old_has_desc) old_desc_buf[0..old_desc_len] else null, description, performed_by, entry_book_id);
            }

            const old_meta = if (old_has_meta) old_meta_buf[0..old_meta_len] else "";
            const new_meta = metadata orelse "";
            if (!std.mem.eql(u8, old_meta, new_meta)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "metadata", if (old_has_meta) old_meta_buf[0..old_meta_len] else null, metadata, performed_by, entry_book_id);
            }

            if (old_period_id != period_id) {
                var old_buf: [amt_buf_len]u8 = undefined;
                var new_buf: [amt_buf_len]u8 = undefined;
                const old_s = std.fmt.bufPrint(&old_buf, "{d}", .{old_period_id}) catch unreachable;
                const new_s = std.fmt.bufPrint(&new_buf, "{d}", .{period_id}) catch unreachable;
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "period_id", old_s, new_s, performed_by, entry_book_id);
            }
        }

        if (owns_txn) try database.commit();
    }

    pub fn editPosted(database: db.Database, entry_id: i64, description: ?[]const u8, metadata: ?[]const u8, performed_by: []const u8) !void {
        var entry_book_id: i64 = 0;
        var old_desc_buf: [1001]u8 = undefined;
        var old_desc_len: usize = 0;
        var old_has_desc = false;
        var old_meta_buf: [10001]u8 = undefined;
        var old_meta_len: usize = 0;
        var old_has_meta = false;
        var period_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT status, book_id, description, metadata, period_id
                \\FROM ledger_entries WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const status = stmt.columnText(0).?;
            if (std.mem.eql(u8, status, "draft")) return error.InvalidInput;
            if (std.mem.eql(u8, status, "void")) return error.InvalidInput;
            if (std.mem.eql(u8, status, "reversed")) return error.InvalidInput;
            entry_book_id = stmt.columnInt64(1);

            if (stmt.columnText(2)) |d| {
                old_has_desc = true;
                old_desc_len = @min(d.len, old_desc_buf.len);
                @memcpy(old_desc_buf[0..old_desc_len], d[0..old_desc_len]);
            }
            if (stmt.columnText(3)) |m| {
                old_has_meta = true;
                old_meta_len = @min(m.len, old_meta_buf.len);
                @memcpy(old_meta_buf[0..old_meta_len], m[0..old_meta_len]);
            }
            period_id = stmt.columnInt64(4);
        }

        // Verify period is open or soft_closed
        {
            var stmt = try database.prepare("SELECT status FROM ledger_periods WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, period_id);
            _ = try stmt.step();
            const period_status = stmt.columnText(0).?;
            if (std.mem.eql(u8, period_status, "closed")) return error.PeriodClosed;
            if (std.mem.eql(u8, period_status, "locked")) return error.PeriodLocked;
        }

        {
            var stmt = try database.prepare(
                \\UPDATE ledger_entries SET
                \\  description = ?, metadata = ?,
                \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE id = ?;
            );
            defer stmt.finalize();
            if (description) |d| try stmt.bindText(1, d) else try stmt.bindNull(1);
            if (metadata) |m| try stmt.bindText(2, m) else try stmt.bindNull(2);
            try stmt.bindInt(3, entry_id);
            _ = try stmt.step();
        }

        {
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();

            const old_desc = if (old_has_desc) old_desc_buf[0..old_desc_len] else "";
            const new_desc = description orelse "";
            if (!std.mem.eql(u8, old_desc, new_desc)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "description", if (old_has_desc) old_desc_buf[0..old_desc_len] else null, description, performed_by, entry_book_id);
            }

            const old_meta = if (old_has_meta) old_meta_buf[0..old_meta_len] else "";
            const new_meta = metadata orelse "";
            if (!std.mem.eql(u8, old_meta, new_meta)) {
                try audit.logWithStmt(database, &audit_stmt, "entry", entry_id, "update", "metadata", if (old_has_meta) old_meta_buf[0..old_meta_len] else null, metadata, performed_by, entry_book_id);
            }
        }

        if (owns_txn) try database.commit();
    }

    pub fn approve(database: db.Database, entry_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        var old_approval_buf: [16]u8 = undefined;
        var old_approval_len: usize = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id, approval_status FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (!std.mem.eql(u8, stmt.columnText(0).?, "draft")) return error.AlreadyPosted;
            book_id = stmt.columnInt64(1);
            if (stmt.columnText(2)) |s| {
                old_approval_len = @min(s.len, old_approval_buf.len);
                @memcpy(old_approval_buf[0..old_approval_len], s[0..old_approval_len]);
            }
        }

        {
            var creator_stmt = try database.prepare(
                \\SELECT performed_by FROM ledger_audit_log
                \\WHERE entity_type = 'entry' AND entity_id = ? AND action = 'create'
                \\ORDER BY id ASC LIMIT 1;
            );
            defer creator_stmt.finalize();
            try creator_stmt.bindInt(1, entry_id);
            if (try creator_stmt.step()) {
                if (creator_stmt.columnText(0)) |creator| {
                    if (std.mem.eql(u8, creator, performed_by)) return error.ApprovalRequired;
                }
            }
        }

        {
            var stmt = try database.prepare(
                \\UPDATE ledger_entries SET approval_status = 'approved',
                \\  approved_by = ?, approved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindText(1, performed_by);
            try stmt.bindInt(2, entry_id);
            _ = try stmt.step();
        }

        try audit.log(database, "entry", entry_id, "approve", "approval_status", old_approval_buf[0..old_approval_len], "approved", performed_by, book_id);
        if (owns_txn) try database.commit();
    }

    pub fn reject(database: db.Database, entry_id: i64, reason: []const u8, performed_by: []const u8) !void {
        if (reason.len == 0) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        var old_approval_buf: [16]u8 = undefined;
        var old_approval_len: usize = 0;
        {
            var stmt = try database.prepare("SELECT status, book_id, approval_status FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (!std.mem.eql(u8, stmt.columnText(0).?, "draft")) return error.AlreadyPosted;
            book_id = stmt.columnInt64(1);
            if (stmt.columnText(2)) |s| {
                old_approval_len = @min(s.len, old_approval_buf.len);
                @memcpy(old_approval_buf[0..old_approval_len], s[0..old_approval_len]);
            }
        }

        {
            var stmt = try database.prepare(
                \\UPDATE ledger_entries SET approval_status = 'rejected',
                \\  updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                \\WHERE id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, entry_id);
            _ = try stmt.step();
        }

        try audit.log(database, "entry", entry_id, "reject", "approval_status", old_approval_buf[0..old_approval_len], "rejected", performed_by, book_id);
        if (owns_txn) try database.commit();
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
    try std.testing.expectError(error.BookArchived, result);
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
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), line_id);
}

test "addLine adds credit line to draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try std.testing.expectEqual(@as(i64, 1), line_id);
}

test "addLine rejects line on non-draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.addLine(database, 1, 3, 500_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "addLine rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.addLine(database, 999, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "addLine rejects inactive account" {
    const database = try setupTestDb();
    defer database.close();

    try account_mod.Account.updateStatus(database, 1, .inactive, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.AccountInactive, result);
}

// ── post tests ──────────────────────────────────────────────────

test "post balanced entry succeeds" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.UnbalancedEntry, result);
}

test "post rejects entry with fewer than 2 lines" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.TooFewLines, result);
}

test "post rejects already posted entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    try Entry.post(database, 1, "admin");
    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "post rejects entry in closed period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "post rejects entry in locked period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    // Entry 2: debit Cash 500, credit AP 500
    _ = try Entry.createDraft(database, 1, "JE-002", "2026-01-16", "2026-01-16", null, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 1, 500_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 2, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.voidEntry(database, 1, "", "admin");
    try std.testing.expectError(error.VoidReasonRequired, result);
}

test "voidEntry writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const reversal_id = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", null, "admin");
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

test "reverse writes entry_type = 'reversal' on new entry and preserves original" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-TYPE", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    // Original should be entry_type = 'standard' before reverse.
    {
        var stmt = try database.prepare("SELECT entry_type FROM ledger_entries WHERE id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("standard", stmt.columnText(0).?);
    }

    const reversal_id = try Entry.reverse(database, 1, "Entry type check", "2026-01-31", null, "admin");

    // Original entry_type must remain 'standard' — reverse() only flips status.
    {
        var stmt = try database.prepare("SELECT entry_type FROM ledger_entries WHERE id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("standard", stmt.columnText(0).?);
    }

    // Reversal entry must be entry_type = 'reversal'.
    {
        var stmt = try database.prepare("SELECT entry_type FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, reversal_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("reversal", stmt.columnText(0).?);
    }
}

test "reverse rejects draft entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.reverse(database, 1, "Reason", "2026-01-31", null, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "reverse rejects empty reason" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.reverse(database, 1, "", "2026-01-31", null, "admin");
    try std.testing.expectError(error.ReverseReasonRequired, result);
}

test "reverse balance cache nets to zero" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    _ = try Entry.reverse(database, 1, "Reversal", "2026-01-31", null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, usd_amount, 0, "USD", fx, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, usd_amount, "USD", fx, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    const result = Entry.voidEntry(database, 1, "Double void", "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "reverse already-reversed entry rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Reversal", "2026-01-31", null, "admin");

    const result = Entry.reverse(database, 1, "Double reverse", "2026-01-31", null, "admin");
    try std.testing.expectError(error.InvalidTransition, result);
}

test "post 3-line entry: split payment" {
    const database = try setupTestDb();
    defer database.close();

    // Create tax payable account
    _ = try account_mod.Account.create(database, 1, "2100", "Tax Payable", .liability, false, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Split payment", 1, null, "admin");
    // Debit Cash 1000
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    // Credit AP 800
    _ = try Entry.addLine(database, 1, 2, 0, 800_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    // Credit Tax Payable 200
    _ = try Entry.addLine(database, 1, 3, 0, 200_000_000_00, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");

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
    const result = Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    try std.testing.expectError(error.CrossBookViolation, result);
}

test "addLine rejects zero debit and zero credit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    // Both debit and credit = 0 is caught by the Zig-layer XOR guard (Bug 5).
    // The schema CHECK is defense in depth but the Zig guard fires first and
    // returns a meaningful domain error instead of a generic SqliteStepFailed.
    const result = Entry.addLine(database, 1, 1, 0, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "addLine rejects both debit and credit positive" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.addLine(database, 1, 1, 100, 200, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "addLine rejects negative debit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.addLine(database, 1, 1, -100, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "addLine rejects negative credit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const result = Entry.addLine(database, 1, 1, 0, -100, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.InvalidAmount, result);
}

test "full lifecycle: create -> post -> void -> verify cache zero" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 5_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 5_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 2_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    // Void it
    try Entry.voidEntry(database, 1, "Wrong amount", "admin");

    // Post entry 2: debit Cash 2000, credit AP 2000
    _ = try Entry.createDraft(database, 1, "JE-002", "2026-01-16", "2026-01-16", null, 1, null, "admin");
    _ = try Entry.addLine(database, 2, 1, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 2, 2, 0, 2_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 800_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    _ = try Entry.addLine(database, 1, 3, 0, 200_000_000_00, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const rev_id = try Entry.reverse(database, 1, "Reversal", "2026-01-31", null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 700_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    _ = try Entry.addLine(database, 1, 3, 0, 300_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, usd_amount, 0, "USD", fx, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, usd_amount, "USD", fx, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.removeLine(database, line_id, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "removeLine writes audit log" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 565_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    _ = try Entry.reverse(database, 1, "Accrual reversal", "2026-01-31", null, "admin");

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
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    // Entry should still be posted
    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

// ── editLine tests ──────────────────────────────────────────────

test "editLine changes amount on draft line" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, line_id, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    var stmt = try database.prepare("SELECT debit_amount FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(0));
}

test "editLine changes account on draft line" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    // Change from Cash (1) to AP (2)
    try Entry.editLine(database, line_id, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    var stmt = try database.prepare("SELECT account_id FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2), stmt.columnInt64(0));
}

test "editLine changes currency and fx_rate" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, line_id, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, null, "admin");

    var stmt = try database.prepare("SELECT debit_amount, transaction_currency, fx_rate FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 10_000_000_000), stmt.columnInt64(0));
    try std.testing.expectEqualStrings("USD", stmt.columnText(1).?);
    try std.testing.expectEqual(@as(i64, 565_000_000_000), stmt.columnInt64(2));
}

test "editLine rejects posted entry line" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.editLine(database, line_id, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "editLine rejects inactive account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "3000", "Inactive Acct", .equity, false, "admin");
    try account_mod.Account.updateStatus(database, 3, .inactive, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    const result = Entry.editLine(database, line_id, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    try std.testing.expectError(error.AccountInactive, result);
}

test "editLine rejects nonexistent line" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.editLine(database, 999, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "editLine handles direct i64 boundary values safely" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-BOUNDARY", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, line_id, std.math.maxInt(i64), 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expectError(
        error.InvalidAmount,
        Entry.editLine(database, line_id, std.math.minInt(i64), 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin"),
    );
}

test "editLine writes rich audit log with field names and old/new values" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    // Change debit from 1000 to 2000 and account from 1 to 2
    try Entry.editLine(database, line_id, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    // Should have 2 audit entries: debit_amount + account_id
    {
        var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'entry_line' AND action = 'update' AND field_changed = 'debit_amount';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("debit_amount", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("100000000000", stmt.columnText(1).?);
        try std.testing.expectEqualStrings("200000000000", stmt.columnText(2).?);
    }
    {
        var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'entry_line' AND action = 'update' AND field_changed = 'account_id';");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("account_id", stmt.columnText(0).?);
        try std.testing.expectEqualStrings("1", stmt.columnText(1).?);
        try std.testing.expectEqualStrings("2", stmt.columnText(2).?);
    }
}

test "editLine no audit when values unchanged" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    // Edit with same values — no audit entries for update
    try Entry.editLine(database, line_id, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry_line' AND action = 'update';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "editLine currency change logged with old and new currency" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, line_id, 10_000_000_000, 0, "USD", 565_000_000_000, 1, null, null, "admin");

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE field_changed = 'transaction_currency';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("PHP", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("USD", stmt.columnText(1).?);
}

test "editLine then post: edited amount used in base computation" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 2_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    // Edit line 1 from 1000 to 2000 so it balances with line 2
    try Entry.editLine(database, line_id, 2_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT base_debit_amount FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2_000_000_000_00), stmt.columnInt64(0));
}

// ── Remaining business edge cases ───────────────────────────────

test "deleteDraft then verify no orphan audit pollution" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try Entry.deleteDraft(database, 1, "admin");

    // Create a new entry — should work (doc number freed)
    const id = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try std.testing.expect(id > 0);
}

test "remove all lines then try to post: rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const l1 = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    const l2 = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    try Entry.removeLine(database, l1, "admin");
    try Entry.removeLine(database, l2, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.TooFewLines, result);
}

test "void entry excluded from transaction_history view" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    // View only shows posted entries
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_transaction_history;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "post rejects entry when FX computation overflows i64" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    // Amount near i64 max with fx_rate > 1.0 will overflow
    const huge: i64 = std.math.maxInt(i64);
    _ = try Entry.addLine(database, 1, 1, huge, 0, "USD", 20_000_000_000, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, huge, "USD", 20_000_000_000, 2, null, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.AmountOverflow, result);
}

test "addLine handles direct i64 boundary values safely" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-BOUNDARY", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    const line_id = try Entry.addLine(database, 1, 1, std.math.maxInt(i64), 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try std.testing.expect(line_id > 0);
    try std.testing.expectError(
        error.InvalidAmount,
        Entry.addLine(database, 1, 2, std.math.minInt(i64), 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin"),
    );
}

test "post entries in two periods: cache independent per period" {
    const database = try setupTestDb();
    defer database.close();

    // Create Feb period
    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Post in Jan
    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    // Post in Feb
    _ = try Entry.createDraft(database, 1, "JE-002", "2026-02-15", "2026-02-15", null, 2, null, "admin");
    _ = try Entry.addLine(database, 2, 1, 500_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 2, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 2, "admin");

    // Jan cache: Cash debit 1000
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 1;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 1_000_000_000_00), stmt.columnInt64(0));
    }

    // Feb cache: Cash debit 500
    {
        var stmt = try database.prepare("SELECT debit_sum FROM ledger_account_balances WHERE account_id = 1 AND period_id = 2;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 500_000_000_00), stmt.columnInt64(0));
    }
}

// ── editDraft tests ────────────────────────────────────────────

test "editDraft changes document_number" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.editDraft(database, eid, "JE-999", "2026-01-15", "2026-01-15", null, null, 1, "admin");

    var stmt = try database.prepare("SELECT document_number FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("JE-999", stmt.columnText(0).?);
}

test "editDraft changes posting_date" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.editDraft(database, eid, "JE-001", "2026-01-15", "2026-01-20", null, null, 1, "admin");

    var stmt = try database.prepare("SELECT posting_date FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("2026-01-20", stmt.columnText(0).?);
}

test "editDraft changes description and metadata" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.editDraft(database, eid, "JE-001", "2026-01-15", "2026-01-15", "New desc", "{\"key\":\"val\"}", 1, "admin");

    var stmt = try database.prepare("SELECT description, metadata FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New desc", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("{\"key\":\"val\"}", stmt.columnText(1).?);
}

test "editDraft writes field-level audit for each changed field" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.editDraft(database, eid, "JE-002", "2026-01-20", "2026-01-25", "Desc", null, 1, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry' AND entity_id = ? AND action = 'update';");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    // Should have audited: document_number, transaction_date, posting_date, description = 4 fields
    try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
}

test "editDraft rejects posted entry" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    const result = Entry.editDraft(database, eid, "JE-002", "2026-01-15", "2026-01-15", null, null, 1, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "editDraft rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.editDraft(database, 999, "JE-001", "2026-01-15", "2026-01-15", null, null, 1, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "editDraft rejects posting_date outside period range" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.editDraft(database, eid, "JE-001", "2026-01-15", "2026-02-15", null, null, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "editDraft rejects empty document_number" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.editDraft(database, eid, "", "2026-01-15", "2026-01-15", null, null, 1, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "editDraft rejects duplicate document_number" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const eid2 = try Entry.createDraft(database, 1, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.editDraft(database, eid2, "JE-001", "2026-01-15", "2026-01-15", null, null, 1, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "editDraft no audit when nothing changed" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.editDraft(database, eid, "JE-001", "2026-01-15", "2026-01-15", null, null, 1, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry' AND entity_id = ? AND action = 'update';");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

// ── editPosted tests ───────────────────────────────────────────

test "editPosted changes description on posted entry" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try Entry.editPosted(database, eid, "Updated memo", null, "admin");

    var stmt = try database.prepare("SELECT description FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Updated memo", stmt.columnText(0).?);
}

test "editPosted writes audit for changed description" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Old", 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try Entry.editPosted(database, eid, "New", null, "admin");

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE entity_type = 'entry' AND field_changed = 'description' AND action = 'update';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Old", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("New", stmt.columnText(1).?);
}

test "editPosted rejects draft entry" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.editPosted(database, eid, "Memo", null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "editPosted rejects void entry" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");
    try Entry.voidEntry(database, eid, "Error", "admin");

    const result = Entry.editPosted(database, eid, "Memo", null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "editPosted rejects closed period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    const result = Entry.editPosted(database, eid, "Memo", null, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "editPosted rejects locked period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 1, .locked, "admin");

    const result = Entry.editPosted(database, eid, "Memo", null, "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

test "editPosted allows in soft_closed period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");

    try Entry.editPosted(database, eid, "Late correction memo", null, "admin");

    var stmt = try database.prepare("SELECT description FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Late correction memo", stmt.columnText(0).?);
}

test "editPosted rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.editPosted(database, 999, "Memo", null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── addLine counterparty_id tests ──────────────────────────────

test "addLine with counterparty_id stores counterparty" {
    const database = try setupTestDb();
    defer database.close();

    const subledger_mod = @import("subledger.zig");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const cid = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 2, cid, null, "admin");

    var stmt = try database.prepare("SELECT counterparty_id FROM ledger_entry_lines WHERE entry_id = ? AND line_number = 1;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, cid), stmt.columnInt64(0));
}

test "addLine with null counterparty_id stores null" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    var stmt = try database.prepare("SELECT counterparty_id FROM ledger_entry_lines WHERE entry_id = ? AND line_number = 1;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expect(stmt.columnText(0) == null);
}

// ── Cross-period reversal tests ────────────────────────────────

test "reverse: cross-period reversal posts to target period" {
    const database = try setupTestDb();
    defer database.close();

    // Create Feb period
    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Post in Jan
    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    // Reverse into Feb (cross-period)
    const rev_id = try Entry.reverse(database, eid, "Prior period correction", "2026-02-15", 2, "admin");

    // Verify reversal entry is in Feb period
    var stmt = try database.prepare("SELECT period_id FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, rev_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 2), stmt.columnInt64(0));
}

test "reverse: cross-period reversal rejects closed target period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    // Close Feb
    try period_mod.Period.transition(database, 2, .soft_closed, "admin");
    try period_mod.Period.transition(database, 2, .closed, "admin");

    const result = Entry.reverse(database, eid, "Correction", "2026-02-15", 2, "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "reverse: cross-period reversal rejects date outside target period" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    // Try to reverse into Feb but with a March date
    const result = Entry.reverse(database, eid, "Correction", "2026-03-15", 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

// ── Auto-rounding tests ────────────────────────────────────────

test "post: auto-rounds FX imbalance when rounding account set" {
    const database = try setupTestDb();
    defer database.close();

    // Create rounding account and set it
    _ = try account_mod.Account.create(database, 1, "9999", "FX Rounding", .expense, false, "admin");
    try book_mod.Book.setRoundingAccount(database, 1, 3, "admin");

    // Create entry with FX that will produce a small rounding difference
    const eid = try Entry.createDraft(database, 1, "FX-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    // 100.00 USD at fx 56.501 = 5650.1 PHP (base) — may truncate
    _ = try Entry.addLine(database, eid, 1, 10_000_000_000, 0, "USD", 56_501_000_000, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 10_000_000_000, "USD", 56_501_000_000, 2, null, null, "admin");

    // This should post — amounts should balance since same FX rate
    try Entry.post(database, eid, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "post: rejects large imbalance even with rounding account" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "9999", "FX Rounding", .expense, false, "admin");
    try book_mod.Book.setRoundingAccount(database, 1, 3, "admin");

    const eid = try Entry.createDraft(database, 1, "BAD-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 500_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    const result = Entry.post(database, eid, "admin");
    try std.testing.expectError(error.UnbalancedEntry, result);
}

// ── P0: Auto-rounding actually triggers rounding line ──────────

test "post: auto-rounding inserts rounding line when FX diff is small" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "9999", "FX Rounding", .expense, false, "admin");
    try book_mod.Book.setRoundingAccount(database, 1, 3, "admin");

    const eid = try Entry.createDraft(database, 1, "FX-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    // Line 1: 100 USD at 56.50 = base 5650.00000000 (565000000000)
    _ = try Entry.addLine(database, eid, 1, 10_000_000_000, 0, "USD", 56_500_000_000, 1, null, null, "admin");
    // Line 2: 100 USD at 56.50 but manually offset the rate slightly to create a 1-unit diff
    // 56.500000001 * 10^10 = 565000000010 — produces base = 5650.0000001 (565000000010 * 10000000000 / 10^10 = 56500000001)
    // Actually, computeBaseAmount = amount * fx_rate / FX_RATE_SCALE
    // = 10_000_000_000 * 56_500_000_001 / 10_000_000_000 = 56_500_000_001
    // So debit base = 10_000_000_000 * 56_500_000_000 / 10_000_000_000 = 56_500_000_000
    // credit base = 10_000_000_000 * 56_500_000_001 / 10_000_000_000 = 56_500_000_001
    // diff = 56_500_000_000 - 56_500_000_001 = -1 (within tolerance of 100)
    _ = try Entry.addLine(database, eid, 2, 0, 10_000_000_000, "USD", 56_500_000_001, 2, null, null, "admin");

    try Entry.post(database, eid, "admin");

    // Verify entry posted
    {
        var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, eid);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
    }

    // Verify a 3rd rounding line was inserted
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, eid);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }

    // Verify rounding line targets the rounding account (id=3)
    {
        var stmt = try database.prepare("SELECT account_id, description FROM ledger_entry_lines WHERE entry_id = ? AND line_number = 3;");
        defer stmt.finalize();
        try stmt.bindInt(1, eid);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i64, 3), stmt.columnInt64(0));
        try std.testing.expectEqualStrings("FX rounding adjustment", stmt.columnText(1).?);
    }
}

// ── P0: editLine with counterparty_id and description ──────────

test "editLine: changes description and audits it" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const lid = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, lid, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "Updated memo", "admin");

    var stmt = try database.prepare("SELECT description FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, lid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Updated memo", stmt.columnText(0).?);

    // Verify audit
    var audit_stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry_line' AND field_changed = 'description';");
    defer audit_stmt.finalize();
    _ = try audit_stmt.step();
    try std.testing.expect(audit_stmt.columnInt(0) > 0);
}

test "editLine: changes counterparty_id and audits it" {
    const database = try setupTestDb();
    defer database.close();

    const subledger_mod = @import("subledger.zig");
    const gid = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, 2, null, null, "admin");
    const cid = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", gid, "admin");

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const lid = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");

    try Entry.editLine(database, lid, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, cid, null, "admin");

    var stmt = try database.prepare("SELECT counterparty_id FROM ledger_entry_lines WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, lid);
    _ = try stmt.step();
    try std.testing.expectEqual(cid, stmt.columnInt64(0));
}

// ── P1: voidEntry in closed and locked period ──────────────────

test "voidEntry: rejects void in closed period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");

    const result = Entry.voidEntry(database, eid, "Error", "admin");
    try std.testing.expectError(error.PeriodClosed, result);
}

test "voidEntry: rejects void in locked period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try period_mod.Period.transition(database, 1, .locked, "admin");

    const result = Entry.voidEntry(database, eid, "Error", "admin");
    try std.testing.expectError(error.PeriodLocked, result);
}

// ── P1: reverse with nonexistent entry and period ──────────────

test "reverse: rejects nonexistent entry" {
    const database = try setupTestDb();
    defer database.close();

    const result = Entry.reverse(database, 999, "Reason", "2026-01-31", null, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "reverse: rejects nonexistent target period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    const result = Entry.reverse(database, eid, "Correction", "2026-02-15", 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── P1: same-period reverse verifies period_id ─────────────────

test "reverse: null target uses original period" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    const rev_id = try Entry.reverse(database, eid, "Reversal", "2026-01-31", null, "admin");

    var stmt = try database.prepare("SELECT period_id FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, rev_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

// ── P1: editDraft period change with audit ─────────────────────

test "editDraft: changing period_id audits it" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");

    // Change period to Feb and update posting_date accordingly
    try Entry.editDraft(database, eid, "JE-001", "2026-02-15", "2026-02-15", null, null, 2, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry' AND field_changed = 'period_id';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expect(stmt.columnInt(0) > 0);
}

// ── P1: editPosted no-op audit verification ────────────────────

test "editPosted: no audit when nothing changed" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", "Memo", 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    // Call editPosted with same values
    try Entry.editPosted(database, eid, "Memo", null, "admin");

    // Should have zero update audit entries for this editPosted
    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'entry' AND action = 'update' AND entity_id = ? AND field_changed = 'description';");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

// ── P1: Auto-rounding audit entry verification ──────────────────

test "post: auto-rounding line has audit entry" {
    const database = try setupTestDb();
    defer database.close();

    _ = try account_mod.Account.create(database, 1, "9999", "FX Rounding", .expense, false, "admin");
    try book_mod.Book.setRoundingAccount(database, 1, 3, "admin");

    const eid = try Entry.createDraft(database, 1, "FX-003", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 10_000_000_000, 0, "USD", 56_500_000_000, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 10_000_000_000, "USD", 56_500_000_001, 2, null, null, "admin");

    try Entry.post(database, eid, "admin");

    // Verify rounding line was inserted (precondition)
    {
        var check_stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
        defer check_stmt.finalize();
        try check_stmt.bindInt(1, eid);
        _ = try check_stmt.step();
        try std.testing.expectEqual(@as(i32, 3), check_stmt.columnInt(0));
    }

    // Verify audit entry exists for the auto-rounding line
    {
        var audit_stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_audit_log
            \\WHERE entity_type = 'entry_line' AND action = 'create'
            \\  AND new_value = 'FX rounding auto-post';
        );
        defer audit_stmt.finalize();
        _ = try audit_stmt.step();
        try std.testing.expect(audit_stmt.columnInt(0) >= 1);
    }
}

// ── P1: Stale-marking only affects future periods ───────────────

test "post: stale marking only affects future periods" {
    const database = try setupTestDb();
    defer database.close();

    // setupTestDb: book_id=1, Cash=1, AP=2, Jan 2026 period=1 (2026-01-01 to 2026-01-31)
    const feb_id = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    const dec_id = try period_mod.Period.create(database, 1, "Dec 2025", 12, 2025, "2025-12-01", "2025-12-31", "regular", "admin");

    // Post in Feb to create cache entries for both accounts in Feb
    const feb_entry = try Entry.createDraft(database, 1, "FEB-001", "2026-02-15", "2026-02-15", null, feb_id, null, "admin");
    _ = try Entry.addLine(database, feb_entry, 1, 100_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, feb_entry, 2, 0, 100_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, feb_entry, "admin");

    // Post in Dec to create cache entries for both accounts in Dec
    const dec_entry = try Entry.createDraft(database, 1, "DEC-001", "2025-12-15", "2025-12-15", null, dec_id, null, "admin");
    _ = try Entry.addLine(database, dec_entry, 1, 50_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, dec_entry, 2, 0, 50_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, dec_entry, "admin");

    // Clear all stale flags via direct SQL
    {
        var reset_stmt = try database.prepare("UPDATE ledger_account_balances SET is_stale = 0;");
        defer reset_stmt.finalize();
        _ = try reset_stmt.step();
    }

    // Post in Jan (period 1, end_date = 2026-01-31) — should mark Feb stale but not Dec
    const jan_entry = try Entry.createDraft(database, 1, "JAN-002", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try Entry.addLine(database, jan_entry, 1, 200_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, jan_entry, 2, 0, 200_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, jan_entry, "admin");

    // Feb cache should be stale (future period)
    {
        var feb_stmt = try database.prepare("SELECT is_stale FROM ledger_account_balances WHERE period_id = ? LIMIT 1;");
        defer feb_stmt.finalize();
        try feb_stmt.bindInt(1, feb_id);
        if (try feb_stmt.step()) {
            try std.testing.expectEqual(@as(i32, 1), feb_stmt.columnInt(0));
        }
    }

    // Dec cache should NOT be stale (past period)
    {
        var dec_stmt = try database.prepare("SELECT is_stale FROM ledger_account_balances WHERE period_id = ? LIMIT 1;");
        defer dec_stmt.finalize();
        try dec_stmt.bindInt(1, dec_id);
        if (try dec_stmt.step()) {
            try std.testing.expectEqual(@as(i32, 0), dec_stmt.columnInt(0));
        }
    }
}

test "reverse: 96-char document number produces 100-char REV- doc" {
    const database = try setupTestDb();
    defer database.close();

    const doc_96 = "A" ** 96;
    _ = try Entry.createDraft(database, 1, doc_96, "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const rev_id = try Entry.reverse(database, 1, "Correction", "2026-01-31", null, "admin");

    var stmt = try database.prepare("SELECT document_number FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, rev_id);
    _ = try stmt.step();
    const rev_doc = stmt.columnText(0).?;
    try std.testing.expectEqual(@as(usize, 100), rev_doc.len);
    try std.testing.expect(std.mem.startsWith(u8, rev_doc, "REV-"));
}

test "editDraft on void entry returns AlreadyPosted" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");
    try Entry.voidEntry(database, 1, "Error", "admin");

    const result = Entry.editDraft(database, 1, "JE-002", "2026-01-20", "2026-01-20", null, null, 1, "admin");
    try std.testing.expectError(error.AlreadyPosted, result);
}

// ── Approval Workflow Tests (20B) ─────────────────────────────

test "approve draft entry sets approval_status to approved" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.approve(database, 1, "manager");

    var stmt = try database.prepare("SELECT approval_status, approved_by, approved_at FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("approved", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("manager", stmt.columnText(1).?);
    try std.testing.expect(stmt.columnText(2) != null);
}

test "reject draft entry with reason sets approval_status to rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    try Entry.reject(database, 1, "Missing documentation", "manager");

    var stmt = try database.prepare("SELECT approval_status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("rejected", stmt.columnText(0).?);
}

test "post approved entry when book requires approval succeeds" {
    const database = try setupTestDb();
    defer database.close();

    try book_mod.Book.setRequireApproval(database, 1, true, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.approve(database, 1, "manager");
    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "post unapproved entry when book requires approval fails with ApprovalRequired" {
    const database = try setupTestDb();
    defer database.close();

    try book_mod.Book.setRequireApproval(database, 1, true, "admin");

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");

    const result = Entry.post(database, 1, "admin");
    try std.testing.expectError(error.ApprovalRequired, result);
}

test "post entry when book does NOT require approval succeeds regardless of approval_status" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

test "approve non-draft entry returns AlreadyPosted" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, 1, "admin");

    const result = Entry.approve(database, 1, "manager");
    try std.testing.expectError(error.AlreadyPosted, result);
}

test "self-approval rejected — same user who created cannot approve" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const result = Entry.approve(database, 1, "admin");
    try std.testing.expectError(error.ApprovalRequired, result);
    try Entry.approve(database, 1, "manager");
}

test "void rejects entry with open open items" {
    const subledger_mod = @import("subledger.zig");
    const open_item_mod = @import("open_item.zig");

    const database = try setupTestDb();
    defer database.close();

    const ar = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, ar, null, null, "admin");
    const customer = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", group_id, "admin");

    const eid = try Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, ar, customer, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    _ = try open_item_mod.createOpenItem(database, line_id, customer, 1_000_000_000_00, "2026-02-14", 1, "admin");

    const result = Entry.voidEntry(database, eid, "Wrong invoice", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "void succeeds after open item is closed" {
    const subledger_mod = @import("subledger.zig");
    const open_item_mod = @import("open_item.zig");

    const database = try setupTestDb();
    defer database.close();

    const ar = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, ar, null, null, "admin");
    const customer = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", group_id, "admin");

    const eid = try Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, ar, customer, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    const oi_id = try open_item_mod.createOpenItem(database, line_id, customer, 1_000_000_000_00, "2026-02-14", 1, "admin");
    try open_item_mod.allocatePayment(database, oi_id, 1_000_000_000_00, "admin");

    try Entry.voidEntry(database, eid, "Correction after payment", "admin");

    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, eid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("void", stmt.columnText(0).?);
}

test "void succeeds when entry has no open items" {
    const database = try setupTestDb();
    defer database.close();

    const eid = try Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");
    try Entry.voidEntry(database, eid, "No open items", "admin");
}

test "reverse rejects entry with open open items" {
    const subledger_mod = @import("subledger.zig");
    const open_item_mod = @import("open_item.zig");

    const database = try setupTestDb();
    defer database.close();

    const ar = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(database, 1, "Customers", "customer", 1, ar, null, null, "admin");
    const customer = try subledger_mod.SubledgerAccount.create(database, 1, "C001", "Acme", "customer", group_id, "admin");

    const eid = try Entry.createDraft(database, 1, "INV-REV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = try Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, ar, customer, null, "admin");
    _ = try Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try Entry.post(database, eid, "admin");

    _ = try open_item_mod.createOpenItem(database, line_id, customer, 1_000_000_000_00, "2026-02-14", 1, "admin");

    const result = Entry.reverse(database, eid, "Wrong invoice", "2026-01-31", null, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}
