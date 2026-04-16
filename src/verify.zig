const std = @import("std");
const db = @import("db.zig");
const export_mod = @import("export.zig");

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

pub const VerifySeverity = enum {
    @"error",
    warning,

    fn label(self: VerifySeverity) []const u8 {
        return switch (self) {
            .@"error" => "error",
            .warning => "warning",
        };
    }
};

pub const VerifyIssue = struct {
    severity: VerifySeverity,
    code: []const u8,
    primary_id: i64 = 0,
    related_id: i64 = 0,
    message_buf: [192]u8 = [_]u8{0} ** 192,
    message_len: usize = 0,

    fn message(self: *const VerifyIssue) []const u8 {
        return self.message_buf[0..self.message_len];
    }
};

const VerifyIssueList = std.ArrayListUnmanaged(VerifyIssue);

fn appendIssue(issues: *VerifyIssueList, severity: VerifySeverity, code: []const u8, primary_id: i64, related_id: i64, comptime fmt: []const u8, args: anytype) !void {
    var issue = VerifyIssue{
        .severity = severity,
        .code = code,
        .primary_id = primary_id,
        .related_id = related_id,
    };
    issue.message_len = (try std.fmt.bufPrint(&issue.message_buf, fmt, args)).len;
    try issues.append(std.heap.page_allocator, issue);
}

fn renderVerifyDetailedJson(summary: VerifyResult, issues: []const VerifyIssue, buf: []u8) ![]u8 {
    var pos: usize = 0;

    const open = "{\"summary\":{\"errors\":";
    if (pos + open.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + open.len], open);
    pos += open.len;
    pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{summary.errors})).len;

    const p1 = ",\"warnings\":";
    if (pos + p1.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + p1.len], p1);
    pos += p1.len;
    pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{summary.warnings})).len;

    const p2 = ",\"entries_checked\":";
    if (pos + p2.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + p2.len], p2);
    pos += p2.len;
    pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{summary.entries_checked})).len;

    const p3 = ",\"accounts_checked\":";
    if (pos + p3.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + p3.len], p3);
    pos += p3.len;
    pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{summary.accounts_checked})).len;

    const p4 = ",\"periods_checked\":";
    if (pos + p4.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + p4.len], p4);
    pos += p4.len;
    pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{summary.periods_checked})).len;

    const p5 = "},\"issues\":[";
    if (pos + p5.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[pos .. pos + p5.len], p5);
    pos += p5.len;

    for (issues, 0..) |issue, idx| {
        if (idx > 0) {
            if (pos >= buf.len) return error.BufferTooSmall;
            buf[pos] = ',';
            pos += 1;
        }
        const prefix = "{\"severity\":\"";
        if (pos + prefix.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + prefix.len], prefix);
        pos += prefix.len;
        pos += try export_mod.jsonString(buf[pos..], issue.severity.label());

        const c1 = "\",\"code\":\"";
        if (pos + c1.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + c1.len], c1);
        pos += c1.len;
        pos += try export_mod.jsonString(buf[pos..], issue.code);

        const c2 = "\",\"message\":\"";
        if (pos + c2.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + c2.len], c2);
        pos += c2.len;
        pos += try export_mod.jsonString(buf[pos..], issue.message());

        const c3 = "\",\"primary_id\":";
        if (pos + c3.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + c3.len], c3);
        pos += c3.len;
        pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{issue.primary_id})).len;

        const c4 = ",\"related_id\":";
        if (pos + c4.len > buf.len) return error.BufferTooSmall;
        @memcpy(buf[pos .. pos + c4.len], c4);
        pos += c4.len;
        pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{issue.related_id})).len;

        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '}';
        pos += 1;
    }

    if (pos + 2 > buf.len) return error.BufferTooSmall;
    buf[pos] = ']';
    buf[pos + 1] = '}';
    pos += 2;
    return buf[0..pos];
}

fn renderVerifyDetailedCsv(issues: []const VerifyIssue, buf: []u8) ![]u8 {
    var pos: usize = 0;
    const header = "severity,code,primary_id,related_id,message\n";
    if (header.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[0..header.len], header);
    pos = header.len;

    for (issues) |issue| {
        pos += try export_mod.csvField(buf[pos..], issue.severity.label());
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try export_mod.csvField(buf[pos..], issue.code);
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{issue.primary_id})).len;
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += (try std.fmt.bufPrint(buf[pos..], "{d}", .{issue.related_id})).len;
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = ',';
        pos += 1;
        pos += try export_mod.csvField(buf[pos..], issue.message());
        if (pos >= buf.len) return error.BufferTooSmall;
        buf[pos] = '\n';
        pos += 1;
    }
    return buf[0..pos];
}

pub fn verifyDetailed(database: db.Database, book_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var issues = VerifyIssueList{};
    defer issues.deinit(std.heap.page_allocator);

    // Verify book exists
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) == 0) return error.NotFound;
    }

    var summary = VerifyResult{
        .errors = 0,
        .warnings = 0,
        .entries_checked = 0,
        .accounts_checked = 0,
        .periods_checked = 0,
    };

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
            summary.entries_checked += 1;
            const entry_id = stmt.columnInt64(0);
            const debits = stmt.columnInt64(1);
            const credits = stmt.columnInt64(2);
            if (debits != credits) {
                summary.errors += 1;
                try appendIssue(&issues, .@"error", "UNBALANCED_ENTRY", entry_id, 0, "entry {d} has base debits {d} but credits {d}", .{ entry_id, debits, credits });
            }
        }
    }

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
            \\    WHERE e.book_id = ? AND e.status IN ('posted', 'reversed')
            \\      AND e.entry_type != 'opening'
            \\    GROUP BY el.account_id, e.period_id
            \\) computed ON computed.account_id = ab.account_id AND computed.period_id = ab.period_id
            \\WHERE ab.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);

        while (try stmt.step()) {
            summary.accounts_checked += 1;
            const account_id = stmt.columnInt64(0);
            const period_id = stmt.columnInt64(1);
            const cached_debit = stmt.columnInt64(2);
            const cached_credit = stmt.columnInt64(3);
            const real_debit = stmt.columnInt64(4);
            const real_credit = stmt.columnInt64(5);
            if (cached_debit != real_debit or cached_credit != real_credit) {
                summary.errors += 1;
                try appendIssue(&issues, .@"error", "STALE_ACCOUNT_BALANCE_CACHE", account_id, period_id, "account {d} period {d} cache debit/credit {d}/{d} differs from computed {d}/{d}", .{ account_id, period_id, cached_debit, cached_credit, real_debit, real_credit });
            }
        }
    }

    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const pc = stmt.columnInt(0);
        summary.periods_checked = if (pc >= 0) @as(u32, @intCast(pc)) else 0;
    }

    {
        var stmt = try database.prepare(
            \\SELECT p1.id, p2.id
            \\FROM ledger_periods p1
            \\JOIN ledger_periods p2 ON p1.book_id = p2.book_id AND p1.id < p2.id
            \\  AND p1.period_type = 'regular' AND p2.period_type = 'regular'
            \\  AND p1.start_date <= p2.end_date AND p2.start_date <= p1.end_date
            \\WHERE p1.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            const p1 = stmt.columnInt64(0);
            const p2 = stmt.columnInt64(1);
            summary.warnings += 1;
            try appendIssue(&issues, .warning, "OVERLAPPING_PERIODS", p1, p2, "period {d} overlaps period {d}", .{ p1, p2 });
        }
    }

    {
        var stmt = try database.prepare(
            \\SELECT e.id
            \\FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status IN ('posted', 'reversed', 'void')
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_audit_log al
            \\    WHERE al.entity_type = 'entry' AND al.entity_id = e.id
            \\      AND al.action = 'post'
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            const entry_id = stmt.columnInt64(0);
            summary.warnings += 1;
            try appendIssue(&issues, .warning, "MISSING_POST_AUDIT_RECORD", entry_id, 0, "entry {d} is missing its post audit log record", .{entry_id});
        }
    }

    {
        var stmt = try database.prepare(
            \\SELECT sg.id, sg.gl_account_id,
            \\  COALESCE(gl.gl_debit, 0) as gl_debit,
            \\  COALESCE(gl.gl_credit, 0) as gl_credit,
            \\  COALESCE(sl.sl_debit, 0) as sl_debit,
            \\  COALESCE(sl.sl_credit, 0) as sl_credit
            \\FROM ledger_subledger_groups sg
            \\LEFT JOIN (
            \\  SELECT ab.account_id, SUM(ab.debit_sum) as gl_debit, SUM(ab.credit_sum) as gl_credit
            \\  FROM ledger_account_balances ab
            \\  WHERE ab.book_id = ?
            \\  GROUP BY ab.account_id
            \\) gl ON gl.account_id = sg.gl_account_id
            \\LEFT JOIN (
            \\  SELECT sa.group_id,
            \\    SUM(el.base_debit_amount) as sl_debit,
            \\    SUM(el.base_credit_amount) as sl_credit
            \\  FROM ledger_entry_lines el
            \\  JOIN ledger_entries e ON e.id = el.entry_id AND e.status = 'posted'
            \\  JOIN ledger_subledger_accounts sa ON sa.id = el.counterparty_id
            \\  WHERE e.book_id = ?
            \\  GROUP BY sa.group_id
            \\) sl ON sl.group_id = sg.id
            \\WHERE sg.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);
        try stmt.bindInt(3, book_id);

        while (try stmt.step()) {
            const group_id = stmt.columnInt64(0);
            const gl_account_id = stmt.columnInt64(1);
            const gl_debit = stmt.columnInt64(2);
            const gl_credit = stmt.columnInt64(3);
            const sl_debit = stmt.columnInt64(4);
            const sl_credit = stmt.columnInt64(5);
            if (gl_debit != sl_debit or gl_credit != sl_credit) {
                summary.warnings += 1;
                try appendIssue(&issues, .warning, "SUBLEDGER_GL_MISMATCH", group_id, gl_account_id, "subledger group {d} does not reconcile to control account {d}: GL {d}/{d} vs subledger {d}/{d}", .{ group_id, gl_account_id, gl_debit, gl_credit, sl_debit, sl_credit });
            }
        }
    }

    {
        var stmt = try database.prepare(
            \\SELECT period_id, COUNT(*)
            \\FROM ledger_entries
            \\WHERE book_id = ? AND status = 'posted' AND entry_type = 'opening'
            \\GROUP BY period_id
            \\HAVING COUNT(*) > 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            const period_id = stmt.columnInt64(0);
            const count = stmt.columnInt64(1);
            summary.errors += 1;
            try appendIssue(&issues, .@"error", "DUPLICATE_OPENING_ENTRIES", period_id, 0, "period {d} has {d} posted opening entries", .{ period_id, count });
        }
    }

    {
        var stmt = try database.prepare(
            \\SELECT e.id, e.metadata
            \\FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.entry_type = 'opening' AND e.status = 'posted';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var check = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE id = ? AND book_id = ?;");
        defer check.finalize();
        while (try stmt.step()) {
            const entry_id = stmt.columnInt64(0);
            const meta = stmt.columnText(1) orelse {
                summary.warnings += 1;
                try appendIssue(&issues, .warning, "OPENING_ENTRY_MISSING_METADATA", entry_id, 0, "opening entry {d} is missing metadata", .{entry_id});
                continue;
            };
            const needle = "\"source_period_id\":";
            const idx = std.mem.indexOf(u8, meta, needle) orelse {
                summary.warnings += 1;
                try appendIssue(&issues, .warning, "OPENING_ENTRY_MISSING_SOURCE_PERIOD", entry_id, 0, "opening entry {d} is missing source_period_id metadata", .{entry_id});
                continue;
            };
            var tail = meta[idx + needle.len ..];
            while (tail.len > 0 and (tail[0] == ' ' or tail[0] == '\t')) : (tail = tail[1..]) {}
            var start: usize = 0;
            if (tail.len > 0 and tail[0] == '-') start = 1;
            var end: usize = start;
            while (end < tail.len and (tail[end] >= '0' and tail[end] <= '9')) : (end += 1) {}
            if (end == start) {
                summary.warnings += 1;
                try appendIssue(&issues, .warning, "OPENING_ENTRY_INVALID_SOURCE_PERIOD", entry_id, 0, "opening entry {d} has an unreadable source_period_id", .{entry_id});
                continue;
            }
            const src_id = std.fmt.parseInt(i64, tail[0..end], 10) catch {
                summary.warnings += 1;
                try appendIssue(&issues, .warning, "OPENING_ENTRY_INVALID_SOURCE_PERIOD", entry_id, 0, "opening entry {d} has an invalid source_period_id", .{entry_id});
                continue;
            };
            try check.bindInt(1, src_id);
            try check.bindInt(2, book_id);
            _ = try check.step();
            if (check.columnInt(0) == 0) {
                summary.errors += 1;
                try appendIssue(&issues, .@"error", "OPENING_ENTRY_ORPHAN_SOURCE_PERIOD", entry_id, src_id, "opening entry {d} references missing source period {d}", .{ entry_id, src_id });
            }
            check.reset();
            check.clearBindings();
        }
    }

    return switch (format) {
        .json => renderVerifyDetailedJson(summary, issues.items, buf),
        .csv => renderVerifyDetailedCsv(issues.items, buf),
    };
}

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

    // Check 2: Cache integrity — recompute from lines, compare with cache.
    // Opening entries are audit-trail markers that don't contribute to the
    // cache (by design — see Sprint C.1). Exclude them from the recomputation.
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
            \\    WHERE e.book_id = ? AND e.status IN ('posted', 'reversed')
            \\      AND e.entry_type != 'opening'
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
            \\JOIN ledger_accounts a ON a.id = el.account_id
            \\LEFT JOIN ledger_entries e ON e.id = el.entry_id
            \\WHERE a.book_id = ? AND e.id IS NULL;
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

    // Check 9: Closed periods should have zero revenue/expense balances
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.status IN ('closed', 'locked')
            \\  AND a.account_type IN ('revenue', 'expense')
            \\  AND (ab.debit_sum != ab.credit_sum);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const count = stmt.columnInt(0);
        if (count > 0) result.warnings += 1;
    }

    // Check 10: GL-SL reconciliation — subledger totals should match control account balances
    {
        var stmt = try database.prepare(
            \\SELECT sg.id, sg.gl_account_id,
            \\  COALESCE(gl.gl_debit, 0) as gl_debit,
            \\  COALESCE(gl.gl_credit, 0) as gl_credit,
            \\  COALESCE(sl.sl_debit, 0) as sl_debit,
            \\  COALESCE(sl.sl_credit, 0) as sl_credit
            \\FROM ledger_subledger_groups sg
            \\LEFT JOIN (
            \\  SELECT ab.account_id, SUM(ab.debit_sum) as gl_debit, SUM(ab.credit_sum) as gl_credit
            \\  FROM ledger_account_balances ab
            \\  WHERE ab.book_id = ?
            \\  GROUP BY ab.account_id
            \\) gl ON gl.account_id = sg.gl_account_id
            \\LEFT JOIN (
            \\  SELECT sa.group_id,
            \\    SUM(el.base_debit_amount) as sl_debit,
            \\    SUM(el.base_credit_amount) as sl_credit
            \\  FROM ledger_entry_lines el
            \\  JOIN ledger_entries e ON e.id = el.entry_id AND e.status = 'posted'
            \\  JOIN ledger_subledger_accounts sa ON sa.id = el.counterparty_id
            \\  WHERE e.book_id = ?
            \\  GROUP BY sa.group_id
            \\) sl ON sl.group_id = sg.id
            \\WHERE sg.book_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);
        try stmt.bindInt(3, book_id);

        while (try stmt.step()) {
            const gl_debit = stmt.columnInt64(2);
            const gl_credit = stmt.columnInt64(3);
            const sl_debit = stmt.columnInt64(4);
            const sl_credit = stmt.columnInt64(5);

            if (gl_debit != sl_debit or gl_credit != sl_credit) {
                result.warnings += 1;
            }
        }
    }

    // Check 11: Reversal pair integrity — reversed entries must have a matching reversal
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries
            \\WHERE book_id = ? AND status = 'reversed'
            \\  AND id NOT IN (SELECT reverses_entry_id FROM ledger_entries WHERE reverses_entry_id IS NOT NULL AND book_id = ?);
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.errors += 1;
    }

    // Check 12: Zero-total-activity entries (all amounts = 0)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.status = 'posted'
            \\  AND NOT EXISTS (
            \\    SELECT 1 FROM ledger_entry_lines el
            \\    WHERE el.entry_id = e.id AND (el.base_debit_amount > 0 OR el.base_credit_amount > 0)
            \\  );
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.warnings += 1;
    }

    // Check 13: Period date continuity — detect gaps in fiscal year coverage
    {
        var stmt = try database.prepare(
            \\SELECT p1.end_date, p2.start_date
            \\FROM ledger_periods p1
            \\JOIN ledger_periods p2 ON p2.book_id = p1.book_id
            \\  AND p2.year = p1.year AND p2.period_number = p1.period_number + 1
            \\  AND p2.period_type = 'regular' AND p1.period_type = 'regular'
            \\WHERE p1.book_id = ?
            \\  AND date(p2.start_date) > date(p1.end_date, '+1 day');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            result.warnings += 1;
        }
    }

    // Check 14: Duplicate opening entries within a period (Sprint F #12).
    // A period should carry at most one live (non-void/non-reversed) opening
    // entry. Re-closes produce revision-suffixed doc numbers and the prior
    // opening entry is voided — so seeing two posted openings in the same
    // period signals either a bypass of cascadeReopen or raw-SQL tampering.
    {
        var stmt = try database.prepare(
            \\SELECT period_id, COUNT(*)
            \\FROM ledger_entries
            \\WHERE book_id = ? AND status = 'posted' AND entry_type = 'opening'
            \\GROUP BY period_id
            \\HAVING COUNT(*) > 1;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            result.errors += 1;
        }
    }

    // Check 15: Orphan opening entries — every opening entry should reference
    // a real source period via its metadata. Opening entries are generated by
    // generateOpeningEntry which embeds {"opening_entry":true,"source_period_id":N}
    // in the metadata. If the source period no longer exists, the entry is
    // orphaned and its provenance chain is broken (Rule 11 violation).
    {
        var stmt = try database.prepare(
            \\SELECT e.id, e.metadata
            \\FROM ledger_entries e
            \\WHERE e.book_id = ? AND e.entry_type = 'opening' AND e.status = 'posted';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        // Hoist the period-existence check outside the loop. Preparing per-row
        // is an N+1 that re-parses the same statement for every opening entry.
        var check = try database.prepare("SELECT COUNT(*) FROM ledger_periods WHERE id = ? AND book_id = ?;");
        defer check.finalize();
        while (try stmt.step()) {
            const meta = stmt.columnText(1) orelse {
                result.warnings += 1;
                continue;
            };
            // Extract source_period_id via substring scan — metadata is small.
            // Tolerate optional whitespace and an optional minus sign between
            // the colon and the digits so manually-constructed metadata that
            // happens to be valid JSON still validates correctly.
            const needle = "\"source_period_id\":";
            const idx = std.mem.indexOf(u8, meta, needle) orelse {
                result.warnings += 1;
                continue;
            };
            var tail = meta[idx + needle.len ..];
            while (tail.len > 0 and (tail[0] == ' ' or tail[0] == '\t')) : (tail = tail[1..]) {}
            var start: usize = 0;
            if (tail.len > 0 and tail[0] == '-') start = 1;
            var end: usize = start;
            while (end < tail.len and (tail[end] >= '0' and tail[end] <= '9')) : (end += 1) {}
            if (end == start) {
                result.warnings += 1;
                continue;
            }
            const src_id = std.fmt.parseInt(i64, tail[0..end], 10) catch {
                result.warnings += 1;
                continue;
            };
            try check.bindInt(1, src_id);
            try check.bindInt(2, book_id);
            _ = try check.step();
            if (check.columnInt(0) == 0) result.errors += 1;
            check.reset();
            check.clearBindings();
        }
    }

    // Check 16: Negative entry_count in cache (defense-in-depth). Should be
    // impossible under normal operation since post/void are symmetric, but guards
    // against future concurrency changes or manual DB surgery.
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_account_balances
            \\WHERE book_id = ? AND entry_count < 0;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        if (stmt.columnInt(0) > 0) result.errors += 1;
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
const subledger_mod = @import("subledger.zig");

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
    try std.testing.expect(result.passed());
    try std.testing.expectEqual(@as(u32, 0), result.warnings);
}

test "verify: reversal entry has 'post' audit record (Bug 2 regression)" {
    const database = try setupTestDb();
    defer database.close();

    try postEntry(database, "JE-002", 1, 1_000_000_000_00, 3, 1_000_000_000_00);
    const reversal_id = try entry_mod.Entry.reverse(database, 1, "Bug 2 regression", "2026-01-15", null, "admin");

    // Explicit assertion that a 'post' audit action exists for the reversal.
    var stmt = try database.prepare(
        \\SELECT COUNT(*) FROM ledger_audit_log
        \\WHERE entity_type = 'entry' AND entity_id = ? AND action = 'post';
    );
    defer stmt.finalize();
    try stmt.bindInt(1, reversal_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
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

test "verify: GL-SL reconciliation passes when balanced" {
    const database = try setupTestDb();
    defer database.close();

    // Create AR account (account_id=6) as the GL control account
    _ = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");

    // Create subledger group linked to AR control account (gl_account_id=6)
    const group_id = try subledger_mod.SubledgerGroup.create(database, 1, "AR Customers", "customer", 1, 6, null, null, "admin");

    // Create subledger account (customer)
    const customer_id = try subledger_mod.SubledgerAccount.create(database, 1, "C-001", "Acme Corp", "customer", group_id, "admin");

    // Post entry: Debit AR (control account) with counterparty, Credit Revenue
    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 6, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 4, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const result = try verify(database, 1);
    try std.testing.expectEqual(@as(u32, 0), result.warnings);
    try std.testing.expect(result.passed());
}

test "verify: GL-SL reconciliation warns on mismatch" {
    const database = try setupTestDb();
    defer database.close();

    // Create AR account (account_id=6) as the GL control account
    _ = try account_mod.Account.create(database, 1, "1100", "AR", .asset, false, "admin");

    // Create subledger group linked to AR control account (gl_account_id=6)
    const group_id = try subledger_mod.SubledgerGroup.create(database, 1, "AR Customers", "customer", 1, 6, null, null, "admin");

    // Create subledger account (customer)
    const customer_id = try subledger_mod.SubledgerAccount.create(database, 1, "C-001", "Acme Corp", "customer", group_id, "admin");

    // Post entry: Debit AR with counterparty, Credit Revenue
    const eid = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 6, customer_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 4, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    // Corrupt GL balance: add extra debit to the control account cache
    // This simulates a raw SQL manipulation that bypasses the subledger
    try database.exec("UPDATE ledger_account_balances SET debit_sum = debit_sum + 50000000000 WHERE account_id = 6;");

    const result = try verify(database, 1);
    try std.testing.expect(result.warnings > 0);
}

test "verify: detects duplicate opening entries in same period (Check 14)" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Forge two opening entries in Feb 2026 via raw SQL. In normal flow,
    // cascadeReopen voids the prior opening before a new one is generated.
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, metadata, entry_type, status, period_id, book_id)
        \\VALUES ('OPEN-P2-A', '2026-02-01', '2026-02-01', '{"opening_entry":true,"source_period_id":1}', 'opening', 'posted', 2, 1);
    );
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, metadata, entry_type, status, period_id, book_id)
        \\VALUES ('OPEN-P2-B', '2026-02-01', '2026-02-01', '{"opening_entry":true,"source_period_id":1}', 'opening', 'posted', 2, 1);
    );

    const result = try verify(database, 1);
    try std.testing.expect(result.errors > 0);
}

test "verify: detects orphaned opening entry source_period_id (Check 15)" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, metadata, entry_type, status, period_id, book_id)
        \\VALUES ('OPEN-P2', '2026-02-01', '2026-02-01', '{"opening_entry":true,"source_period_id":999}', 'opening', 'posted', 2, 1);
    );

    const result = try verify(database, 1);
    try std.testing.expect(result.errors > 0);
}

test "verify: Check 15 tolerates whitespace after source_period_id colon" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    // Manually-constructed metadata with a space after the colon. The orphan
    // check must still detect that source_period_id 999 doesn't exist.
    try database.exec(
        \\INSERT INTO ledger_entries (document_number, transaction_date, posting_date, metadata, entry_type, status, period_id, book_id)
        \\VALUES ('OPEN-P2-WS', '2026-02-01', '2026-02-01', '{"opening_entry":true,"source_period_id": 999}', 'opening', 'posted', 2, 1);
    );

    const result = try verify(database, 1);
    try std.testing.expect(result.errors > 0);
}
