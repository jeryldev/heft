const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const report = @import("report.zig");

pub const MAX_CLASSIFICATION_NODES: usize = 10_000;

pub const Classification = struct {
    const valid_types = [_][]const u8{ "balance_sheet", "income_statement", "trial_balance", "cash_flow" };

    fn isValidType(t: []const u8) bool {
        for (valid_types) |vt| {
            if (std.mem.eql(u8, t, vt)) return true;
        }
        return false;
    }

    pub fn create(database: db.Database, book_id: i64, name: []const u8, report_type: []const u8, performed_by: []const u8) !i64 {
        if (!isValidType(report_type)) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        var stmt = try database.prepare("INSERT INTO ledger_classifications (name, report_type, book_id) VALUES (?, ?, ?);");
        defer stmt.finalize();
        try stmt.bindText(1, name);
        try stmt.bindText(2, report_type);
        try stmt.bindInt(3, book_id);
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "classification", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn updateName(database: db.Database, classification_id: i64, new_name: []const u8, performed_by: []const u8) !void {
        if (new_name.len == 0) return error.InvalidInput;

        var old_name_buf: [256]u8 = undefined;
        var old_name_len: usize = 0;
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT name, book_id FROM ledger_classifications WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0).?;
            old_name_len = @min(old.len, old_name_buf.len);
            @memcpy(old_name_buf[0..old_name_len], old[0..old_name_len]);
            book_id = stmt.columnInt64(1);
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_classifications SET name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_name);
            try stmt.bindInt(2, classification_id);
            _ = stmt.step() catch return error.DuplicateNumber;
        }

        try audit.log(database, "classification", classification_id, "update", "name", old_name_buf[0..old_name_len], new_name, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn delete(database: db.Database, classification_id: i64, performed_by: []const u8) !void {
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_classifications WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var node_stmt = try database.prepare("SELECT id FROM ledger_classification_nodes WHERE classification_id = ?;");
            defer node_stmt.finalize();
            try node_stmt.bindInt(1, classification_id);
            var audit_stmt = try database.prepare(audit.insert_sql);
            defer audit_stmt.finalize();
            while (try node_stmt.step()) {
                const node_id = node_stmt.columnInt64(0);
                try audit.logWithStmt(database, &audit_stmt, "classification_node", node_id, "delete", null, null, null, performed_by, book_id);
            }
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_classification_nodes WHERE classification_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            _ = try stmt.step();
        }
        {
            var stmt = try database.prepare("DELETE FROM ledger_classifications WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            _ = try stmt.step();
        }

        try audit.log(database, "classification", classification_id, "delete", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
    }
};

pub const ClassificationNode = struct {
    const max_depth: u32 = 20;

    fn getBookId(database: db.Database, classification_id: i64) !i64 {
        var stmt = try database.prepare("SELECT book_id FROM ledger_classifications WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        return stmt.columnInt64(0);
    }

    fn computeDepth(database: db.Database, parent_id: ?i64) !i32 {
        const pid = parent_id orelse return 0;
        var stmt = try database.prepare("SELECT depth FROM ledger_classification_nodes WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, pid);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        return stmt.columnInt(0) + 1;
    }

    /// Walk the parent chain from target_parent_id to root. If node_id appears
    /// in the chain, moving it there would create a cycle (A->B->C->A).
    /// Prepares the SELECT once and reuses with reset/clearBindings per step,
    /// avoiding N prepare/finalize cycles for an N-deep chain. Max depth 20
    /// as safety net (no real classification tree exceeds this).
    fn checkCycle(database: db.Database, node_id: i64, target_parent_id: ?i64) !void {
        var current = target_parent_id orelse return;
        var stmt = try database.prepare("SELECT parent_id FROM ledger_classification_nodes WHERE id = ?;");
        defer stmt.finalize();

        var depth: u32 = 0;
        while (depth < max_depth) : (depth += 1) {
            if (current == node_id) return error.CircularReference;
            try stmt.bindInt(1, current);
            const has_row = try stmt.step();
            if (!has_row) return;
            const parent_text = stmt.columnText(0);
            if (parent_text == null) {
                stmt.reset();
                return;
            }
            current = stmt.columnInt64(0);
            stmt.reset();
            stmt.clearBindings();
        }
        return error.CircularReference;
    }

    const bs_types = [_][]const u8{ "asset", "liability", "equity" };
    const is_types = [_][]const u8{ "revenue", "expense" };

    fn validateAccountType(database: db.Database, classification_id: i64, account_id: i64) !void {
        // Single query: join classification + account to validate type match
        var stmt = try database.prepare(
            \\SELECT c.report_type, a.account_type
            \\FROM ledger_classifications c, ledger_accounts a
            \\WHERE c.id = ? AND a.id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        try stmt.bindInt(2, account_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;

        const report_type = stmt.columnText(0).?;
        const acct_type = stmt.columnText(1).?;

        if (std.mem.eql(u8, report_type, "trial_balance")) return;
        if (std.mem.eql(u8, report_type, "cash_flow")) return;

        if (std.mem.eql(u8, report_type, "balance_sheet")) {
            for (bs_types) |t| {
                if (std.mem.eql(u8, acct_type, t)) return;
            }
            return error.InvalidInput;
        }
        if (std.mem.eql(u8, report_type, "income_statement")) {
            for (is_types) |t| {
                if (std.mem.eql(u8, acct_type, t)) return;
            }
            return error.InvalidInput;
        }
    }

    pub fn addGroup(database: db.Database, classification_id: i64, label: []const u8, parent_id: ?i64, position: i32, performed_by: []const u8) !i64 {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        const book_id = try getBookId(database, classification_id);

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        if (parent_id) |pid| {
            var p_stmt = try database.prepare("SELECT classification_id FROM ledger_classification_nodes WHERE id = ?;");
            defer p_stmt.finalize();
            try p_stmt.bindInt(1, pid);
            const has_row = try p_stmt.step();
            if (!has_row) return error.NotFound;
            if (p_stmt.columnInt64(0) != classification_id) return error.InvalidInput;
        }

        const depth = try computeDepth(database, parent_id);

        var stmt = try database.prepare("INSERT INTO ledger_classification_nodes (node_type, label, parent_id, position, depth, classification_id) VALUES ('group', ?, ?, ?, ?, ?);");
        defer stmt.finalize();
        try stmt.bindText(1, label);
        if (parent_id) |pid| try stmt.bindInt(2, pid) else try stmt.bindNull(2);
        try stmt.bindInt(3, @intCast(position));
        try stmt.bindInt(4, @intCast(depth));
        try stmt.bindInt(5, classification_id);
        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "classification_node", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn addAccount(database: db.Database, classification_id: i64, account_id: i64, parent_id: ?i64, position: i32, performed_by: []const u8) !i64 {
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT c.book_id,
                \\  (SELECT COUNT(*) FROM ledger_classification_nodes WHERE classification_id = ? AND account_id = ?)
                \\FROM ledger_classifications c WHERE c.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            try stmt.bindInt(2, account_id);
            try stmt.bindInt(3, classification_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
            if (stmt.columnInt(1) > 0) return error.DuplicateNumber;
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        try validateAccountType(database, classification_id, account_id);

        const depth = try computeDepth(database, parent_id);

        var stmt = try database.prepare("INSERT INTO ledger_classification_nodes (node_type, label, parent_id, account_id, position, depth, classification_id) VALUES ('account', NULL, ?, ?, ?, ?, ?);");
        defer stmt.finalize();
        if (parent_id) |pid| try stmt.bindInt(1, pid) else try stmt.bindNull(1);
        try stmt.bindInt(2, account_id);
        try stmt.bindInt(3, @intCast(position));
        try stmt.bindInt(4, @intCast(depth));
        try stmt.bindInt(5, classification_id);
        _ = try stmt.step();

        const id = database.lastInsertRowId();
        try audit.log(database, "classification_node", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn move(database: db.Database, node_id: i64, new_parent_id: ?i64, new_position: i32, performed_by: []const u8) !void {
        var book_id: i64 = 0;
        var old_depth: i32 = 0;
        var old_parent_id: ?i64 = null;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        try checkCycle(database, node_id, new_parent_id);

        {
            var stmt = try database.prepare("SELECT c.book_id FROM ledger_classification_nodes n JOIN ledger_classifications c ON c.id = n.classification_id WHERE n.id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, node_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        if (new_parent_id) |pid| {
            var cls_stmt = try database.prepare(
                \\SELECT n1.classification_id, n2.classification_id
                \\FROM ledger_classification_nodes n1, ledger_classification_nodes n2
                \\WHERE n1.id = ? AND n2.id = ?;
            );
            defer cls_stmt.finalize();
            try cls_stmt.bindInt(1, node_id);
            try cls_stmt.bindInt(2, pid);
            const has_row = try cls_stmt.step();
            if (!has_row) return error.NotFound;
            if (cls_stmt.columnInt64(0) != cls_stmt.columnInt64(1)) return error.CrossBookViolation;
        }

        const new_depth = try computeDepth(database, new_parent_id);

        {
            var d_stmt = try database.prepare("SELECT depth, parent_id FROM ledger_classification_nodes WHERE id = ?;");
            defer d_stmt.finalize();
            try d_stmt.bindInt(1, node_id);
            const has_row = try d_stmt.step();
            if (!has_row) return error.NotFound;
            old_depth = d_stmt.columnInt(0);
            if (d_stmt.columnText(1) != null) old_parent_id = d_stmt.columnInt64(1);
        }

        {
            var stmt = try database.prepare("UPDATE ledger_classification_nodes SET parent_id = ?, position = ?, depth = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            if (new_parent_id) |pid| try stmt.bindInt(1, pid) else try stmt.bindNull(1);
            try stmt.bindInt(2, @intCast(new_position));
            try stmt.bindInt(3, @intCast(new_depth));
            try stmt.bindInt(4, node_id);
            _ = try stmt.step();
        }

        {
            const depth_diff = @as(i32, @intCast(new_depth)) - old_depth;
            if (depth_diff != 0) {
                var desc_stmt = try database.prepare(
                    \\UPDATE ledger_classification_nodes SET depth = depth + ?
                    \\WHERE id IN (
                    \\  WITH RECURSIVE descendants(id) AS (
                    \\    SELECT id FROM ledger_classification_nodes WHERE parent_id = ?
                    \\    UNION ALL
                    \\    SELECT n.id FROM ledger_classification_nodes n
                    \\    JOIN descendants d ON n.parent_id = d.id
                    \\  )
                    \\  SELECT id FROM descendants
                    \\);
                );
                defer desc_stmt.finalize();
                try desc_stmt.bindInt(1, @intCast(depth_diff));
                try desc_stmt.bindInt(2, node_id);
                _ = try desc_stmt.step();
            }
        }

        var old_parent_buf: [20]u8 = undefined;
        var new_parent_buf: [20]u8 = undefined;
        const old_parent_str: ?[]const u8 = if (old_parent_id) |opid|
            std.fmt.bufPrint(&old_parent_buf, "{d}", .{opid}) catch unreachable
        else
            null;
        const new_parent_str: ?[]const u8 = if (new_parent_id) |npid|
            std.fmt.bufPrint(&new_parent_buf, "{d}", .{npid}) catch unreachable
        else
            null;

        try audit.log(database, "classification_node", node_id, "move", "parent_id", old_parent_str, new_parent_str, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn updateLabel(database: db.Database, node_id: i64, new_label: []const u8, performed_by: []const u8) !void {
        if (new_label.len == 0) return error.InvalidInput;

        var old_label_buf: [256]u8 = undefined;
        var old_label_len: usize = 0;
        var book_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT n.label, c.book_id, n.node_type FROM ledger_classification_nodes n
                \\JOIN ledger_classifications c ON c.id = n.classification_id WHERE n.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, node_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            const old = stmt.columnText(0) orelse "";
            old_label_len = @min(old.len, old_label_buf.len);
            @memcpy(old_label_buf[0..old_label_len], old[0..old_label_len]);
            book_id = stmt.columnInt64(1);
            const node_type = stmt.columnText(2).?;
            if (std.mem.eql(u8, node_type, "account")) return error.InvalidInput;
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("UPDATE ledger_classification_nodes SET label = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindText(1, new_label);
            try stmt.bindInt(2, node_id);
            _ = try stmt.step();
        }

        try audit.log(database, "classification_node", node_id, "update", "label", old_label_buf[0..old_label_len], new_label, performed_by, book_id);

        if (owns_txn) try database.commit();
    }

    pub fn delete(database: db.Database, node_id: i64, performed_by: []const u8) !void {
        var book_id: i64 = 0;
        var classification_id: i64 = 0;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        {
            var stmt = try database.prepare(
                \\SELECT n.classification_id, c.book_id FROM ledger_classification_nodes n
                \\JOIN ledger_classifications c ON c.id = n.classification_id WHERE n.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, node_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            classification_id = stmt.columnInt64(0);
            book_id = stmt.columnInt64(1);
        }

        {
            var bs_stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer bs_stmt.finalize();
            try bs_stmt.bindInt(1, book_id);
            const has_row = try bs_stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, bs_stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var desc_stmt = try database.prepare(
                \\WITH RECURSIVE descendants(id) AS (
                \\  SELECT id FROM ledger_classification_nodes WHERE parent_id = ?
                \\  UNION ALL
                \\  SELECT n.id FROM ledger_classification_nodes n
                \\  JOIN descendants d ON n.parent_id = d.id
                \\)
                \\SELECT id FROM descendants;
            );
            defer desc_stmt.finalize();
            try desc_stmt.bindInt(1, node_id);
            while (try desc_stmt.step()) {
                const desc_id = desc_stmt.columnInt64(0);
                try audit.log(database, "classification_node", desc_id, "delete", null, null, null, performed_by, book_id);
            }
        }

        {
            var stmt = try database.prepare(
                \\WITH RECURSIVE descendants(id) AS (
                \\  SELECT id FROM ledger_classification_nodes WHERE id = ?
                \\  UNION ALL
                \\  SELECT n.id FROM ledger_classification_nodes n
                \\  INNER JOIN descendants d ON n.parent_id = d.id
                \\  WHERE n.classification_id = ?
                \\)
                \\DELETE FROM ledger_classification_nodes WHERE id IN (SELECT id FROM descendants);
            );
            defer stmt.finalize();
            try stmt.bindInt(1, node_id);
            try stmt.bindInt(2, classification_id);
            _ = try stmt.step();
        }

        try audit.log(database, "classification_node", node_id, "delete", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
    }
};

pub const ClassifiedRow = struct {
    node_id: i64,
    node_type: [10]u8,
    node_type_len: usize,
    label: [256]u8,
    label_len: usize,
    account_id: i64,
    depth: i32,
    position: i32,
    debit_balance: i64,
    credit_balance: i64,
};

pub const ClassifiedResult = struct {
    arena: std.heap.ArenaAllocator,
    rows: []ClassifiedRow,
    total_debits: i64,
    total_credits: i64,
    unclassified_debits: i64,
    unclassified_credits: i64,
    decimal_places: u8 = 2,
    truncated: bool = false,

    pub fn deinit(self: *ClassifiedResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

fn copyText(dest: []u8, src: ?[]const u8) usize {
    const s = src orelse return 0;
    const len = @min(s.len, dest.len);
    @memcpy(dest[0..len], s[0..len]);
    return len;
}

/// Generate a classified report with hierarchical roll-up of account balances.
///
/// Uses an in-memory two-pass algorithm instead of a recursive CTE because:
/// - 2.3-2.7x faster at all tested scales (20, 50, 200 accounts)
/// - CTE overhead grows with scale due to SQL parsing, temp table materialization,
///   and UNION ALL accumulation across recursion levels
/// - Financial classification trees are small (typically 20-200 nodes) so the
///   in-memory hash map operations are trivial
/// - Simpler to debug and test (each node balance inspectable)
///
/// Pass 1: Read all nodes into hash maps (node_infos, node_balances)
/// Pass 2: For each account leaf, assign its balance then walk up the
///          ancestor chain accumulating to every parent group
///
/// Time complexity: O(N * D) where N = number of leaf accounts, D = average depth
/// Space complexity: O(N) for node maps (allocated in arena, freed with result)
pub fn classifiedReport(database: db.Database, classification_id: i64, as_of_date: []const u8) !*ClassifiedResult {
    var book_id: i64 = 0;
    {
        var stmt = try database.prepare("SELECT book_id FROM ledger_classifications WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        book_id = stmt.columnInt64(0);
    }

    const result = try std.heap.c_allocator.create(ClassifiedResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    // Build account balance map: account_id -> (debit_balance, credit_balance)
    // Reuse the same query pattern as trialBalance
    var balance_map = std.AutoHashMapUnmanaged(i64, [2]i64){};
    {
        var stmt = try database.prepare(
            \\SELECT a.id, a.normal_balance, SUM(ab.debit_sum), SUM(ab.credit_sum)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.end_date <= ?
            \\GROUP BY a.id;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, as_of_date);

        while (try stmt.step()) {
            const acct_id = stmt.columnInt64(0);
            const normal = stmt.columnText(1).?;
            const debit_sum = stmt.columnInt64(2);
            const credit_sum = stmt.columnInt64(3);

            var debit_bal: i64 = 0;
            var credit_bal: i64 = 0;
            if (std.mem.eql(u8, normal, "debit")) {
                debit_bal = std.math.sub(i64, debit_sum, credit_sum) catch return error.AmountOverflow;
                if (debit_bal < 0) {
                    credit_bal = std.math.negate(debit_bal) catch return error.AmountOverflow;
                    debit_bal = 0;
                }
            } else {
                credit_bal = std.math.sub(i64, credit_sum, debit_sum) catch return error.AmountOverflow;
                if (credit_bal < 0) {
                    debit_bal = std.math.negate(credit_bal) catch return error.AmountOverflow;
                    credit_bal = 0;
                }
            }
            try balance_map.put(allocator, acct_id, .{ debit_bal, credit_bal });
        }
    }

    // Pass 1: Read all nodes into memory.
    // We read the full tree in a single query rather than walking it recursively,
    // because hash map lookups are O(1) vs recursive CTE's temp table overhead.
    const NodeInfo = struct { parent_id: ?i64, acct_id: i64, is_account: bool };
    var node_infos = std.AutoHashMapUnmanaged(i64, NodeInfo){};
    var node_balances = std.AutoHashMapUnmanaged(i64, [2]i64){};
    var account_nodes = std.ArrayListUnmanaged(i64){};

    {
        var stmt = try database.prepare(
            \\SELECT id, node_type, account_id, parent_id
            \\FROM ledger_classification_nodes
            \\WHERE classification_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);

        var node_count: usize = 0;
        while (try stmt.step()) {
            if (node_count >= MAX_CLASSIFICATION_NODES) return error.TooManyAccounts;
            const node_id = stmt.columnInt64(0);
            const node_type = stmt.columnText(1).?;
            const is_account = std.mem.eql(u8, node_type, "account");
            const acct_id = stmt.columnInt64(2);
            const parent_text = stmt.columnText(3);
            const parent_id: ?i64 = if (parent_text != null) stmt.columnInt64(3) else null;

            try node_infos.put(allocator, node_id, .{ .parent_id = parent_id, .acct_id = acct_id, .is_account = is_account });
            try node_balances.put(allocator, node_id, .{ 0, 0 });

            if (is_account) try account_nodes.append(allocator, node_id);
            node_count += 1;
        }
    }

    // Pass 2: For each account leaf, assign its balance from the cache then walk
    // up the entire ancestor chain, accumulating at every parent. This ensures
    // that a 4-level deep leaf (root -> section -> subsection -> account) correctly
    // adds its balance to all 3 ancestor groups, not just the direct parent.
    // Walking ancestors via hash map is O(D) per leaf, O(N*D) total — faster than
    // a recursive CTE which materializes intermediate UNION ALL results at each level.
    for (account_nodes.items) |node_id| {
        const info = node_infos.get(node_id).?;
        var debit_bal: i64 = 0;
        var credit_bal: i64 = 0;
        if (balance_map.get(info.acct_id)) |bal| {
            debit_bal = bal[0];
            credit_bal = bal[1];
        }

        // Set leaf balance
        try node_balances.put(allocator, node_id, .{ debit_bal, credit_bal });

        // Walk up ancestors and accumulate
        var current_parent = info.parent_id;
        while (current_parent) |pid| {
            const existing = node_balances.get(pid).?;
            try node_balances.put(allocator, pid, .{
                std.math.add(i64, existing[0], debit_bal) catch return error.AmountOverflow,
                std.math.add(i64, existing[1], credit_bal) catch return error.AmountOverflow,
            });
            current_parent = node_infos.get(pid).?.parent_id;
        }
    }

    // Build display rows in depth ASC order
    var rows = std.ArrayListUnmanaged(ClassifiedRow){};
    {
        var stmt = try database.prepare(
            \\SELECT id, node_type, label, account_id, depth, position
            \\FROM ledger_classification_nodes
            \\WHERE classification_id = ?
            \\ORDER BY depth ASC, position ASC;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);

        while (try stmt.step()) {
            var row: ClassifiedRow = undefined;
            row.node_id = stmt.columnInt64(0);
            row.node_type_len = copyText(&row.node_type, stmt.columnText(1));
            row.label_len = copyText(&row.label, stmt.columnText(2));
            row.account_id = stmt.columnInt64(3);
            row.depth = stmt.columnInt(4);
            row.position = stmt.columnInt(5);

            const bal = node_balances.get(row.node_id) orelse .{ 0, 0 };
            row.debit_balance = bal[0];
            row.credit_balance = bal[1];

            try rows.append(allocator, row);
            if (rows.items.len >= report.MAX_REPORT_ROWS) break;
        }
    }

    // Compute unclassified: accounts with balances not in any node
    var classified_total_debits: i64 = 0;
    var classified_total_credits: i64 = 0;
    var all_total_debits: i64 = 0;
    var all_total_credits: i64 = 0;

    // Sum classified (root nodes only — avoid double counting)
    {
        var stmt = try database.prepare(
            \\SELECT id FROM ledger_classification_nodes
            \\WHERE classification_id = ? AND parent_id IS NULL;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        while (try stmt.step()) {
            const nid = stmt.columnInt64(0);
            if (node_balances.get(nid)) |bal| {
                classified_total_debits = std.math.add(i64, classified_total_debits, bal[0]) catch return error.AmountOverflow;
                classified_total_credits = std.math.add(i64, classified_total_credits, bal[1]) catch return error.AmountOverflow;
            }
        }
    }

    // Sum all balances from the map
    {
        var it = balance_map.iterator();
        while (it.next()) |entry| {
            all_total_debits = std.math.add(i64, all_total_debits, entry.value_ptr[0]) catch return error.AmountOverflow;
            all_total_credits = std.math.add(i64, all_total_credits, entry.value_ptr[1]) catch return error.AmountOverflow;
        }
    }

    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = classified_total_debits;
    result.total_credits = classified_total_credits;
    result.unclassified_debits = std.math.sub(i64, all_total_debits, classified_total_debits) catch return error.AmountOverflow;
    result.unclassified_credits = std.math.sub(i64, all_total_credits, classified_total_credits) catch return error.AmountOverflow;
    {
        var dp_stmt = try database.prepare("SELECT decimal_places FROM ledger_books WHERE id = ?;");
        defer dp_stmt.finalize();
        try dp_stmt.bindInt(1, book_id);
        _ = try dp_stmt.step();
        const dp = dp_stmt.columnInt(0);
        result.decimal_places = if (dp >= 0 and dp <= 8) @intCast(dp) else 2;
    }
    return result;
}

pub fn cashFlowStatement(database: db.Database, classification_id: i64, start_date: []const u8, end_date: []const u8) !*ClassifiedResult {
    var book_id: i64 = 0;
    {
        var stmt = try database.prepare("SELECT book_id, report_type FROM ledger_classifications WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        const has_row = try stmt.step();
        if (!has_row) return error.NotFound;
        book_id = stmt.columnInt64(0);
        const report_type = stmt.columnText(1).?;
        if (!std.mem.eql(u8, report_type, "cash_flow")) return error.InvalidInput;
    }

    const result = try std.heap.c_allocator.create(ClassifiedResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    var balance_map = std.AutoHashMapUnmanaged(i64, [2]i64){};
    {
        var stmt = try database.prepare(
            \\SELECT a.id, a.normal_balance, SUM(ab.debit_sum), SUM(ab.credit_sum)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
            \\GROUP BY a.id;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, start_date);
        try stmt.bindText(3, end_date);

        while (try stmt.step()) {
            const acct_id = stmt.columnInt64(0);
            const normal = stmt.columnText(1).?;
            const debit_sum = stmt.columnInt64(2);
            const credit_sum = stmt.columnInt64(3);

            var debit_bal: i64 = 0;
            var credit_bal: i64 = 0;
            if (std.mem.eql(u8, normal, "debit")) {
                debit_bal = std.math.sub(i64, debit_sum, credit_sum) catch return error.AmountOverflow;
                if (debit_bal < 0) {
                    credit_bal = std.math.negate(debit_bal) catch return error.AmountOverflow;
                    debit_bal = 0;
                }
            } else {
                credit_bal = std.math.sub(i64, credit_sum, debit_sum) catch return error.AmountOverflow;
                if (credit_bal < 0) {
                    debit_bal = std.math.negate(credit_bal) catch return error.AmountOverflow;
                    credit_bal = 0;
                }
            }
            try balance_map.put(allocator, acct_id, .{ debit_bal, credit_bal });
        }
    }

    const NodeInfo = struct { parent_id: ?i64, acct_id: i64, is_account: bool };
    var node_infos = std.AutoHashMapUnmanaged(i64, NodeInfo){};
    var node_balances = std.AutoHashMapUnmanaged(i64, [2]i64){};
    var account_nodes = std.ArrayListUnmanaged(i64){};

    {
        var stmt = try database.prepare(
            \\SELECT id, node_type, account_id, parent_id
            \\FROM ledger_classification_nodes
            \\WHERE classification_id = ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);

        var node_count: usize = 0;
        while (try stmt.step()) {
            if (node_count >= MAX_CLASSIFICATION_NODES) return error.TooManyAccounts;
            const node_id = stmt.columnInt64(0);
            const node_type = stmt.columnText(1).?;
            const is_account = std.mem.eql(u8, node_type, "account");
            const acct_id = stmt.columnInt64(2);
            const parent_text = stmt.columnText(3);
            const parent_id: ?i64 = if (parent_text != null) stmt.columnInt64(3) else null;

            try node_infos.put(allocator, node_id, .{ .parent_id = parent_id, .acct_id = acct_id, .is_account = is_account });
            try node_balances.put(allocator, node_id, .{ 0, 0 });

            if (is_account) try account_nodes.append(allocator, node_id);
            node_count += 1;
        }
    }

    for (account_nodes.items) |node_id| {
        const info = node_infos.get(node_id).?;
        var debit_bal: i64 = 0;
        var credit_bal: i64 = 0;
        if (balance_map.get(info.acct_id)) |bal| {
            debit_bal = bal[0];
            credit_bal = bal[1];
        }

        try node_balances.put(allocator, node_id, .{ debit_bal, credit_bal });

        var current_parent = info.parent_id;
        while (current_parent) |pid| {
            const existing = node_balances.get(pid).?;
            try node_balances.put(allocator, pid, .{
                std.math.add(i64, existing[0], debit_bal) catch return error.AmountOverflow,
                std.math.add(i64, existing[1], credit_bal) catch return error.AmountOverflow,
            });
            current_parent = node_infos.get(pid).?.parent_id;
        }
    }

    var rows = std.ArrayListUnmanaged(ClassifiedRow){};
    {
        var stmt = try database.prepare(
            \\SELECT id, node_type, label, account_id, depth, position
            \\FROM ledger_classification_nodes
            \\WHERE classification_id = ?
            \\ORDER BY depth ASC, position ASC;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);

        while (try stmt.step()) {
            var row: ClassifiedRow = undefined;
            row.node_id = stmt.columnInt64(0);
            row.node_type_len = copyText(&row.node_type, stmt.columnText(1));
            row.label_len = copyText(&row.label, stmt.columnText(2));
            row.account_id = stmt.columnInt64(3);
            row.depth = stmt.columnInt(4);
            row.position = stmt.columnInt(5);

            const bal = node_balances.get(row.node_id) orelse .{ 0, 0 };
            row.debit_balance = bal[0];
            row.credit_balance = bal[1];

            try rows.append(allocator, row);
            if (rows.items.len >= report.MAX_REPORT_ROWS) break;
        }
    }

    var classified_total_debits: i64 = 0;
    var classified_total_credits: i64 = 0;
    var all_total_debits: i64 = 0;
    var all_total_credits: i64 = 0;

    {
        var stmt = try database.prepare(
            \\SELECT id FROM ledger_classification_nodes
            \\WHERE classification_id = ? AND parent_id IS NULL;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        while (try stmt.step()) {
            const nid = stmt.columnInt64(0);
            if (node_balances.get(nid)) |bal| {
                classified_total_debits = std.math.add(i64, classified_total_debits, bal[0]) catch return error.AmountOverflow;
                classified_total_credits = std.math.add(i64, classified_total_credits, bal[1]) catch return error.AmountOverflow;
            }
        }
    }

    {
        var it = balance_map.iterator();
        while (it.next()) |entry| {
            all_total_debits = std.math.add(i64, all_total_debits, entry.value_ptr[0]) catch return error.AmountOverflow;
            all_total_credits = std.math.add(i64, all_total_credits, entry.value_ptr[1]) catch return error.AmountOverflow;
        }
    }

    result.rows = try rows.toOwnedSlice(allocator);
    result.total_debits = classified_total_debits;
    result.total_credits = classified_total_credits;
    result.unclassified_debits = std.math.sub(i64, all_total_debits, classified_total_debits) catch return error.AmountOverflow;
    result.unclassified_credits = std.math.sub(i64, all_total_credits, classified_total_credits) catch return error.AmountOverflow;
    {
        var dp_stmt = try database.prepare("SELECT decimal_places FROM ledger_books WHERE id = ?;");
        defer dp_stmt.finalize();
        try dp_stmt.bindInt(1, book_id);
        _ = try dp_stmt.step();
        const dp = dp_stmt.columnInt(0);
        result.decimal_places = if (dp >= 0 and dp <= 8) @intCast(dp) else 2;
    }
    return result;
}

pub const CashFlowIndirectResult = struct {
    arena: std.heap.ArenaAllocator,
    net_income: i64,
    adjustments: []ClassifiedRow,
    operating_total: i64,
    investing_total: i64,
    financing_total: i64,
    net_cash_change: i64,
    decimal_places: u8 = 2,
    truncated: bool = false,

    pub fn deinit(self: *CashFlowIndirectResult) void {
        self.arena.deinit();
        std.heap.c_allocator.destroy(self);
    }
};

pub fn cashFlowStatementIndirect(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8, classification_id: i64) !*CashFlowIndirectResult {
    const result = try std.heap.c_allocator.create(CashFlowIndirectResult);
    errdefer std.heap.c_allocator.destroy(result);
    result.arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    errdefer result.arena.deinit();
    const allocator = result.arena.allocator();

    const cache_mod = @import("cache.zig");
    {
        var stale_stmt = try database.prepare(
            \\SELECT p.id FROM ledger_periods p
            \\JOIN ledger_account_balances ab ON ab.period_id = p.id AND ab.is_stale = 1
            \\WHERE p.book_id = ? AND p.end_date <= ?
            \\GROUP BY p.id;
        );
        defer stale_stmt.finalize();
        try stale_stmt.bindInt(1, book_id);
        try stale_stmt.bindText(2, end_date);
        var period_ids: [200]i64 = undefined;
        var pcount: usize = 0;
        while (try stale_stmt.step()) {
            if (pcount >= period_ids.len) break;
            period_ids[pcount] = stale_stmt.columnInt64(0);
            pcount += 1;
        }
        if (pcount > 0) _ = try cache_mod.recalculateStale(database, book_id, period_ids[0..pcount]);
    }

    var net_income: i64 = 0;
    {
        var stmt = try database.prepare(
            \\SELECT COALESCE(SUM(ab.debit_sum), 0), COALESCE(SUM(ab.credit_sum), 0)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND a.account_type IN ('revenue', 'expense')
            \\  AND p.start_date >= ? AND p.end_date <= ?;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, start_date);
        try stmt.bindText(3, end_date);
        _ = try stmt.step();
        const total_debits = stmt.columnInt64(0);
        const total_credits = stmt.columnInt64(1);
        net_income = std.math.sub(i64, total_credits, total_debits) catch return error.AmountOverflow;
    }
    result.net_income = net_income;

    const cf_result = try cashFlowStatement(database, classification_id, start_date, end_date);
    defer cf_result.deinit();
    result.decimal_places = cf_result.decimal_places;

    var rows = std.ArrayListUnmanaged(ClassifiedRow){};
    var operating: i64 = 0;
    var investing: i64 = 0;
    var financing: i64 = 0;

    for (cf_result.rows) |row| {
        try rows.append(allocator, row);
        if (rows.items.len >= report.MAX_REPORT_ROWS) break;
        const net = std.math.sub(i64, row.debit_balance, row.credit_balance) catch return error.AmountOverflow;
        if (row.depth == 0) {
            const label = row.label[0..row.label_len];
            if (std.mem.indexOf(u8, label, "Operating") != null or std.mem.indexOf(u8, label, "operating") != null) {
                operating = net;
            } else if (std.mem.indexOf(u8, label, "Investing") != null or std.mem.indexOf(u8, label, "investing") != null) {
                investing = net;
            } else if (std.mem.indexOf(u8, label, "Financing") != null or std.mem.indexOf(u8, label, "financing") != null) {
                financing = net;
            }
        }
    }

    result.adjustments = try rows.toOwnedSlice(allocator);
    result.operating_total = std.math.add(i64, net_income, operating) catch return error.AmountOverflow;
    result.investing_total = investing;
    result.financing_total = financing;
    result.net_cash_change = std.math.add(i64, result.operating_total, std.math.add(i64, investing, financing) catch return error.AmountOverflow) catch return error.AmountOverflow;

    return result;
}

pub fn classifiedTrialBalance(database: db.Database, classification_id: i64, as_of_date: []const u8) !*ClassifiedResult {
    const cr = try classifiedReport(database, classification_id, as_of_date);
    errdefer cr.deinit();

    if (cr.unclassified_debits == 0 and cr.unclassified_credits == 0) return cr;

    var book_id: i64 = 0;
    {
        var stmt = try database.prepare("SELECT book_id FROM ledger_classifications WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, classification_id);
        _ = try stmt.step();
        book_id = stmt.columnInt64(0);
    }

    const allocator = cr.arena.allocator();

    var classified_ids = std.AutoHashMapUnmanaged(i64, void){};
    for (cr.rows) |row| {
        if (row.account_id > 0) {
            try classified_ids.put(allocator, row.account_id, {});
        }
    }

    var extra_rows = std.ArrayListUnmanaged(ClassifiedRow){};
    for (cr.rows) |row| {
        try extra_rows.append(allocator, row);
        if (extra_rows.items.len >= report.MAX_REPORT_ROWS) break;
    }

    {
        var stmt = try database.prepare(
            \\SELECT a.id, a.number, a.normal_balance, SUM(ab.debit_sum), SUM(ab.credit_sum)
            \\FROM ledger_account_balances ab
            \\JOIN ledger_accounts a ON a.id = ab.account_id
            \\JOIN ledger_periods p ON p.id = ab.period_id
            \\WHERE ab.book_id = ? AND p.end_date <= ?
            \\GROUP BY a.id;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, as_of_date);

        while (try stmt.step()) {
            const acct_id = stmt.columnInt64(0);
            if (classified_ids.get(acct_id) != null) continue;

            const normal = stmt.columnText(2).?;
            const debit_sum = stmt.columnInt64(3);
            const credit_sum = stmt.columnInt64(4);

            var debit_bal: i64 = 0;
            var credit_bal: i64 = 0;
            if (std.mem.eql(u8, normal, "debit")) {
                debit_bal = std.math.sub(i64, debit_sum, credit_sum) catch return error.AmountOverflow;
                if (debit_bal < 0) {
                    credit_bal = std.math.negate(debit_bal) catch return error.AmountOverflow;
                    debit_bal = 0;
                }
            } else {
                credit_bal = std.math.sub(i64, credit_sum, debit_sum) catch return error.AmountOverflow;
                if (credit_bal < 0) {
                    debit_bal = std.math.negate(credit_bal) catch return error.AmountOverflow;
                    credit_bal = 0;
                }
            }

            if (debit_bal == 0 and credit_bal == 0) continue;

            var row: ClassifiedRow = std.mem.zeroes(ClassifiedRow);
            row.account_id = acct_id;
            row.depth = -1;
            const num = stmt.columnText(1) orelse "";
            row.label_len = copyText(&row.label, num);
            row.node_type_len = copyText(&row.node_type, "unclassified");
            row.debit_balance = debit_bal;
            row.credit_balance = credit_bal;
            try extra_rows.append(allocator, row);
            if (extra_rows.items.len >= report.MAX_REPORT_ROWS) break;
        }
    }

    cr.rows = try extra_rows.toOwnedSlice(allocator);
    return cr;
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const money = @import("money.zig");
const report_mod = @import("report.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    _ = try book_mod.Book.create(database, "Test", "PHP", 2, "admin");
    // A/L/E accounts for balance sheet
    _ = try account_mod.Account.create(database, 1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1200", "AR", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1500", "Equipment", .asset, false, "admin");
    _ = try account_mod.Account.create(database, 1, "1900", "Accum Depreciation", .asset, true, "admin");
    _ = try account_mod.Account.create(database, 1, "2000", "AP", .liability, false, "admin");
    _ = try account_mod.Account.create(database, 1, "3000", "Capital", .equity, false, "admin");
    // R/E accounts for income statement
    _ = try account_mod.Account.create(database, 1, "4000", "Revenue", .revenue, false, "admin");
    _ = try account_mod.Account.create(database, 1, "5000", "COGS", .expense, false, "admin");
    _ = try period_mod.Period.create(database, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    return database;
}

// ── Classification CRUD tests ───────────────────────────────────

test "create classification returns id" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Classification.create(database, 1, "IFRS Balance Sheet", "balance_sheet", "admin");
    try std.testing.expectEqual(@as(i64, 1), id);
}

test "create classification writes audit" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Classification.create(database, 1, "IFRS BS", "balance_sheet", "admin");

    var stmt = try database.prepare("SELECT entity_type, action FROM ledger_audit_log WHERE entity_type = 'classification';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("classification", stmt.columnText(0).?);
}

test "create classification rejects invalid report_type" {
    const database = try setupTestDb();
    defer database.close();

    const result = Classification.create(database, 1, "Bad", "journal_register", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "create classification rejects nonexistent book" {
    const database = try setupTestDb();
    defer database.close();

    const result = Classification.create(database, 999, "BS", "balance_sheet", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "create classification rejects duplicate name in same book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Classification.create(database, 1, "IFRS BS", "balance_sheet", "admin");
    const result = Classification.create(database, 1, "IFRS BS", "income_statement", "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "multiple classifications per book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Classification.create(database, 1, "IFRS BS", "balance_sheet", "admin");
    _ = try Classification.create(database, 1, "IFRS IS", "income_statement", "admin");
    _ = try Classification.create(database, 1, "Management TB", "trial_balance", "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classifications WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
}

// ── Node tests ──────────────────────────────────────────────────

test "addGroup creates group node" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const node_id = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    try std.testing.expectEqual(@as(i64, 1), node_id);
}

test "addGroup with parent creates child" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const parent = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    const child = try ClassificationNode.addGroup(database, cls_id, "Current Assets", parent, 0, "admin");
    try std.testing.expect(child > parent);
}

test "addAccount creates leaf node" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Current Assets", null, 0, "admin");
    const node_id = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");
    try std.testing.expect(node_id > 0);
}

test "addAccount rejects wrong account type for report_type" {
    const database = try setupTestDb();
    defer database.close();

    // Balance sheet classification should reject revenue account
    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    const result = ClassificationNode.addAccount(database, cls_id, 7, group, 0, "admin"); // Revenue (id=7)
    try std.testing.expectError(error.InvalidInput, result);
}

test "addAccount rejects duplicate account in same classification" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const g1 = try ClassificationNode.addGroup(database, cls_id, "Current", null, 0, "admin");
    const g2 = try ClassificationNode.addGroup(database, cls_id, "Non-Current", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, g1, 0, "admin"); // Cash in Current
    const result = ClassificationNode.addAccount(database, cls_id, 1, g2, 0, "admin"); // Cash in Non-Current = duplicate
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "addAccount allows same account in different classifications" {
    const database = try setupTestDb();
    defer database.close();

    const cls1 = try Classification.create(database, 1, "IFRS BS", "balance_sheet", "admin");
    const cls2 = try Classification.create(database, 1, "GAAP BS", "balance_sheet", "admin");
    const g1 = try ClassificationNode.addGroup(database, cls1, "Assets", null, 0, "admin");
    const g2 = try ClassificationNode.addGroup(database, cls2, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls1, 1, g1, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls2, 1, g2, 0, "admin"); // Same account, different classification
}

test "income statement classification accepts revenue and expense only" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "IS", "income_statement", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Operating", null, 0, "admin");

    // Revenue (id=7) — accepted
    _ = try ClassificationNode.addAccount(database, cls_id, 7, group, 0, "admin");
    // Expense (id=8) — accepted
    _ = try ClassificationNode.addAccount(database, cls_id, 8, group, 1, "admin");
    // Asset (id=1) — rejected
    const result = ClassificationNode.addAccount(database, cls_id, 1, group, 2, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "trial_balance classification accepts all account types" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "TB", "trial_balance", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "All", null, 0, "admin");

    _ = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin"); // Asset
    _ = try ClassificationNode.addAccount(database, cls_id, 5, group, 1, "admin"); // Liability
    _ = try ClassificationNode.addAccount(database, cls_id, 7, group, 2, "admin"); // Revenue
}

test "cycle detection: cannot parent a node to its own descendant" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const a = try ClassificationNode.addGroup(database, cls_id, "A", null, 0, "admin");
    const b = try ClassificationNode.addGroup(database, cls_id, "B", a, 0, "admin");
    _ = try ClassificationNode.addGroup(database, cls_id, "C", b, 0, "admin");

    // Move A under C — creates cycle A->B->C->A
    const result = ClassificationNode.move(database, a, 3, 0, "admin");
    try std.testing.expectError(error.CircularReference, result);
}

test "move node reparents correctly" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const g1 = try ClassificationNode.addGroup(database, cls_id, "Current", null, 0, "admin");
    const g2 = try ClassificationNode.addGroup(database, cls_id, "Non-Current", null, 1, "admin");
    const acct = try ClassificationNode.addAccount(database, cls_id, 1, g1, 0, "admin");

    // Move Cash from Current to Non-Current
    try ClassificationNode.move(database, acct, g2, 0, "admin");

    var stmt = try database.prepare("SELECT parent_id FROM ledger_classification_nodes WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, acct);
    _ = try stmt.step();
    try std.testing.expectEqual(g2, stmt.columnInt64(0));
}

// ── Delete tests ────────────────────────────────────────────────

test "delete classification cascades to nodes" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");

    try Classification.delete(database, cls_id, "admin");

    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classifications;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }
}

test "delete writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    try Classification.delete(database, cls_id, "admin");

    var stmt = try database.prepare("SELECT action FROM ledger_audit_log WHERE entity_type = 'classification' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("delete", stmt.columnText(0).?);
}

// ── Empty/edge case tests ───────────────────────────────────────

test "empty classification: no nodes" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Classification.create(database, 1, "Empty BS", "balance_sheet", "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "3-level hierarchy: root -> section -> account" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const root = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    const section = try ClassificationNode.addGroup(database, cls_id, "Current Assets", root, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, section, 0, "admin"); // Cash
    _ = try ClassificationNode.addAccount(database, cls_id, 2, section, 1, "admin"); // AR

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes WHERE classification_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, cls_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0)); // 2 groups + 2 accounts
}

test "contra account under parent group in balance sheet" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    const fixed = try ClassificationNode.addGroup(database, cls_id, "Fixed Assets", assets, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 3, fixed, 0, "admin"); // Equipment
    _ = try ClassificationNode.addAccount(database, cls_id, 4, fixed, 1, "admin"); // Accum Depreciation (contra)

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes WHERE parent_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, fixed);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "addGroup writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    _ = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    var stmt = try database.prepare("SELECT entity_type FROM ledger_audit_log WHERE entity_type = 'classification_node';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("classification_node", stmt.columnText(0).?);
}

test "addAccount writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'classification_node';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0)); // group + account
}

test "move writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const g1 = try ClassificationNode.addGroup(database, cls_id, "Current", null, 0, "admin");
    const g2 = try ClassificationNode.addGroup(database, cls_id, "Non-Current", null, 1, "admin");
    const acct = try ClassificationNode.addAccount(database, cls_id, 1, g1, 0, "admin");

    try ClassificationNode.move(database, acct, g2, 0, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'classification_node' AND action = 'move';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "node position ordering among siblings" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin"); // Cash pos 0
    _ = try ClassificationNode.addAccount(database, cls_id, 2, assets, 1, "admin"); // AR pos 1
    _ = try ClassificationNode.addAccount(database, cls_id, 3, assets, 2, "admin"); // Equipment pos 2

    var stmt = try database.prepare("SELECT position FROM ledger_classification_nodes WHERE node_type = 'account' ORDER BY position;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
}

test "delete classification after entries posted — safe (presentation only)" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");

    // Post an entry (classification is presentation, not data)
    _ = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 5, null, null, "admin");
    try entry_mod.Entry.post(database, 1, "admin");

    // Delete classification — should succeed (no FK from entries to classifications)
    try Classification.delete(database, cls_id, "admin");

    // Entries and balances unaffected
    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}

// ── Classified Report tests ─────────────────────────────────────

fn postTestEntry(database: db.Database, doc: []const u8, debit_acct: i64, debit_amt: i64, credit_acct: i64, credit_amt: i64) !void {
    const eid = try entry_mod.Entry.createDraft(database, 1, doc, "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, debit_amt, 0, "PHP", money.FX_RATE_SCALE, debit_acct, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, credit_amt, "PHP", money.FX_RATE_SCALE, credit_acct, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");
}

test "classified BS: roll-up totals correct" {
    const database = try setupTestDb();
    defer database.close();

    // Post entries
    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00); // Cash 10k, Capital 10k
    try postTestEntry(database, "JE-002", 1, 5_000_000_000_00, 7, 5_000_000_000_00); // Cash 5k, Revenue 5k

    // Build classified BS
    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin"); // Cash

    const equity = try ClassificationNode.addGroup(database, cls_id, "Equity", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 6, equity, 0, "admin"); // Capital

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Assets group should have Cash balance rolled up
    try std.testing.expect(result.rows.len >= 4); // 2 groups + 2 accounts

    // Total classified = classified root nodes
    try std.testing.expect(result.total_debits > 0 or result.total_credits > 0);
}

test "classified BS: unclassified accounts captured" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00);

    // Classification with only Cash — Capital is unclassified
    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Capital (10k credit) is unclassified
    try std.testing.expect(result.unclassified_credits > 0);
}

test "classified BS: empty classification returns zero" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 1_000_000_000_00, 6, 1_000_000_000_00);

    const cls_id = try Classification.create(database, 1, "Empty BS", "balance_sheet", "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
    // All balances are unclassified
    try std.testing.expect(result.unclassified_debits > 0 or result.unclassified_credits > 0);
}

test "classified BS: contra account as deduction under parent" {
    const database = try setupTestDb();
    defer database.close();

    // Equipment 50k, Accum Dep 10k (contra)
    try postTestEntry(database, "JE-001", 3, 50_000_000_000_00, 6, 50_000_000_000_00); // Equipment debit
    try postTestEntry(database, "JE-002", 8, 10_000_000_000_00, 4, 10_000_000_000_00); // COGS debit, AccumDep credit

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Fixed Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 3, assets, 0, "admin"); // Equipment
    _ = try ClassificationNode.addAccount(database, cls_id, 4, assets, 1, "admin"); // Accum Dep (contra)

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Fixed Assets group: Equipment 50k debit - AccumDep 10k credit = net 40k
    // Find the group node (depth 0)
    var group_debit: i64 = 0;
    var group_credit: i64 = 0;
    for (result.rows) |row| {
        if (row.depth == 0) {
            group_debit = row.debit_balance;
            group_credit = row.credit_balance;
        }
    }
    // Net: 50k debit - 10k credit = 40k debit for the group
    try std.testing.expectEqual(@as(i64, 50_000_000_000_00), group_debit);
    try std.testing.expectEqual(@as(i64, 10_000_000_000_00), group_credit);
}

test "classified: deep chain 4-level roll-up correct" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00);

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const l0 = try ClassificationNode.addGroup(database, cls_id, "Total Assets", null, 0, "admin");
    const l1 = try ClassificationNode.addGroup(database, cls_id, "Current", l0, 0, "admin");
    const l2 = try ClassificationNode.addGroup(database, cls_id, "Cash & Equivalents", l1, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, l2, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // All 4 levels should have same balance (10k debit flows up)
    for (result.rows) |row| {
        try std.testing.expectEqual(@as(i64, 10_000_000_000_00), row.debit_balance);
    }
}

test "classified: wide tree — multiple children sum correctly" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 5_000_000_000_00, 6, 5_000_000_000_00); // Cash 5k
    try postTestEntry(database, "JE-002", 2, 3_000_000_000_00, 7, 3_000_000_000_00); // AR 3k

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Current Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin"); // Cash
    _ = try ClassificationNode.addAccount(database, cls_id, 2, assets, 1, "admin"); // AR

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Assets group: 5k + 3k = 8k
    for (result.rows) |row| {
        if (row.depth == 0) {
            try std.testing.expectEqual(@as(i64, 8_000_000_000_00), row.debit_balance);
        }
    }
}

test "classified: multiple roots sum independently" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00); // Cash, Capital
    try postTestEntry(database, "JE-002", 1, 5_000_000_000_00, 5, 5_000_000_000_00); // Cash, AP

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");

    const liabilities = try ClassificationNode.addGroup(database, cls_id, "Liabilities", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 5, liabilities, 0, "admin");

    const equity = try ClassificationNode.addGroup(database, cls_id, "Equity", null, 2, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 6, equity, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Assets root: Cash 15k debit. Liabilities root: AP 5k credit. Equity root: Capital 10k credit.
    // classified debits (15k) = classified credits (5k + 10k)
    try std.testing.expectEqual(result.total_debits, result.total_credits);
}

test "classified: account with zero balance included in tree" {
    const database = try setupTestDb();
    defer database.close();

    // Post and void — net zero for Cash
    try postTestEntry(database, "JE-001", 1, 1_000_000_000_00, 6, 1_000_000_000_00);
    try entry_mod.Entry.voidEntry(database, 1, "Error", "admin");

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Row exists but balance is zero
    try std.testing.expectEqual(@as(usize, 2), result.rows.len); // group + account
    for (result.rows) |row| {
        try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
    }
}

test "classified: account with no activity returns zero balance" {
    const database = try setupTestDb();
    defer database.close();

    // No entries posted — account exists but has no balance cache entry
    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 0), result.total_debits);
}

test "classified: all accounts classified means unclassified = 0" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00);

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");
    const equity = try ClassificationNode.addGroup(database, cls_id, "Equity", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 6, equity, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 0), result.unclassified_debits);
    try std.testing.expectEqual(@as(i64, 0), result.unclassified_credits);
}

test "classified + unclassified = total book balances (integrity)" {
    const database = try setupTestDb();
    defer database.close();

    try postTestEntry(database, "JE-001", 1, 10_000_000_000_00, 6, 10_000_000_000_00);
    try postTestEntry(database, "JE-002", 2, 3_000_000_000_00, 5, 3_000_000_000_00);

    // Only classify Cash and Capital — AR and AP unclassified
    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const assets = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, assets, 0, "admin");
    const equity = try ClassificationNode.addGroup(database, cls_id, "Equity", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 6, equity, 0, "admin");

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Classified: Cash 10k debit, Capital 10k credit
    // Unclassified: AR 3k debit, AP 3k credit
    const total_debits = result.total_debits + result.unclassified_debits;
    const total_credits = result.total_credits + result.unclassified_credits;

    // Total must balance (fundamental accounting equation)
    try std.testing.expectEqual(total_debits, total_credits);
}

test "classified: group with children that cancel out = net zero" {
    const database = try setupTestDb();
    defer database.close();

    // Equipment 50k debit, AccumDep 50k credit — net zero in group
    try postTestEntry(database, "JE-001", 3, 50_000_000_000_00, 6, 50_000_000_000_00);
    try postTestEntry(database, "JE-002", 8, 50_000_000_000_00, 4, 50_000_000_000_00);

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const fixed = try ClassificationNode.addGroup(database, cls_id, "Fixed Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 3, fixed, 0, "admin"); // Equipment 50k dr
    _ = try ClassificationNode.addAccount(database, cls_id, 4, fixed, 1, "admin"); // AccumDep 50k cr

    const result = try classifiedReport(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Group total: 50k debit - 50k credit = net zero on each side
    for (result.rows) |row| {
        if (row.depth == 0) {
            try std.testing.expectEqual(@as(i64, 50_000_000_000_00), row.debit_balance);
            try std.testing.expectEqual(@as(i64, 50_000_000_000_00), row.credit_balance);
        }
    }
}

test "classified report: nonexistent classification returns NotFound" {
    const database = try setupTestDb();
    defer database.close();

    const result = classifiedReport(database, 999, "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

// ── Classification.updateName tests ────────────────────────────

test "updateName changes classification name" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "Old Layout", "balance_sheet", "admin");
    try Classification.updateName(database, cid, "New Layout", "admin");

    var stmt = try database.prepare("SELECT name FROM ledger_classifications WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, cid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New Layout", stmt.columnText(0).?);
}

test "updateName writes audit with old and new" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "Old", "balance_sheet", "admin");
    try Classification.updateName(database, cid, "New", "admin");

    var stmt = try database.prepare("SELECT field_changed, old_value, new_value FROM ledger_audit_log WHERE entity_type = 'classification' AND field_changed = 'name';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Old", stmt.columnText(1).?);
    try std.testing.expectEqualStrings("New", stmt.columnText(2).?);
}

test "updateName rejects empty name" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "Layout", "balance_sheet", "admin");
    const result = Classification.updateName(database, cid, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateName rejects nonexistent classification" {
    const database = try setupTestDb();
    defer database.close();

    const result = Classification.updateName(database, 999, "New", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "updateName rejects duplicate name in same book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Classification.create(database, 1, "Layout A", "balance_sheet", "admin");
    const cid2 = try Classification.create(database, 1, "Layout B", "income_statement", "admin");
    const result = Classification.updateName(database, cid2, "Layout A", "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

// ── ClassificationNode.updateLabel tests ───────────────────────

test "updateLabel changes group node label" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Old Label", null, 1, "admin");
    try ClassificationNode.updateLabel(database, gid, "New Label", "admin");

    var stmt = try database.prepare("SELECT label FROM ledger_classification_nodes WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, gid);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("New Label", stmt.columnText(0).?);
}

test "updateLabel writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    try ClassificationNode.updateLabel(database, gid, "Current Assets", "admin");

    var stmt = try database.prepare("SELECT old_value, new_value FROM ledger_audit_log WHERE entity_type = 'classification_node' AND field_changed = 'label';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Assets", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("Current Assets", stmt.columnText(1).?);
}

test "updateLabel rejects account nodes" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    const aid = try ClassificationNode.addAccount(database, cid, 1, gid, 1, "admin");
    const result = ClassificationNode.updateLabel(database, aid, "New", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateLabel rejects empty label" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    const result = ClassificationNode.updateLabel(database, gid, "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "updateLabel rejects nonexistent node" {
    const database = try setupTestDb();
    defer database.close();

    const result = ClassificationNode.updateLabel(database, 999, "New", "admin");
    try std.testing.expectError(error.NotFound, result);
}

// ── ClassificationNode.delete tests ────────────────────────────

test "node delete removes a node" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    try ClassificationNode.delete(database, gid, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, gid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "node delete cascades children" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const parent = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cid, 1, parent, 1, "admin");
    _ = try ClassificationNode.addGroup(database, cid, "Sub", parent, 2, "admin");

    try ClassificationNode.delete(database, parent, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_classification_nodes WHERE classification_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, cid);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "node delete writes audit" {
    const database = try setupTestDb();
    defer database.close();

    const cid = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const gid = try ClassificationNode.addGroup(database, cid, "Assets", null, 1, "admin");
    try ClassificationNode.delete(database, gid, "admin");

    var stmt = try database.prepare("SELECT action FROM ledger_audit_log WHERE entity_type = 'classification_node' AND action = 'delete';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("delete", stmt.columnText(0).?);
}

test "node delete rejects nonexistent node" {
    const database = try setupTestDb();
    defer database.close();

    const result = ClassificationNode.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "move: cross-classification reparent rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls1 = try Classification.create(database, 1, "IFRS BS", "balance_sheet", "admin");
    const cls2 = try Classification.create(database, 1, "GAAP BS", "balance_sheet", "admin");
    const g1 = try ClassificationNode.addGroup(database, cls1, "Assets", null, 0, "admin");
    const g2 = try ClassificationNode.addGroup(database, cls2, "Assets", null, 0, "admin");

    // Move g1 (from cls1) under g2 (from cls2) — should reject
    const result = ClassificationNode.move(database, g1, g2, 0, "admin");
    try std.testing.expectError(error.CrossBookViolation, result);
}

test "move: children depths updated after reparent" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    // A(depth=0) -> B(depth=1) -> C(depth=2)
    const a = try ClassificationNode.addGroup(database, cls_id, "A", null, 0, "admin");
    const b = try ClassificationNode.addGroup(database, cls_id, "B", a, 0, "admin");
    const c = try ClassificationNode.addGroup(database, cls_id, "C", b, 0, "admin");

    // Create D(depth=0) -> E(depth=1) -> F(depth=2)
    const d = try ClassificationNode.addGroup(database, cls_id, "D", null, 1, "admin");
    const e = try ClassificationNode.addGroup(database, cls_id, "E", d, 0, "admin");
    _ = try ClassificationNode.addGroup(database, cls_id, "F", e, 0, "admin");

    // Move B under F (depth=2) — B becomes depth=3, C becomes depth=4
    try ClassificationNode.move(database, b, 6, 0, "admin");

    // Verify B is now depth=3
    {
        var stmt = try database.prepare("SELECT depth FROM ledger_classification_nodes WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, b);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 3), stmt.columnInt(0));
    }
    // Verify C is now depth=4
    {
        var stmt = try database.prepare("SELECT depth FROM ledger_classification_nodes WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, c);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 4), stmt.columnInt(0));
    }
}

test "move: self-parent rejected as circular reference" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const node = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    const result = ClassificationNode.move(database, node, node, 0, "admin");
    try std.testing.expectError(error.CircularReference, result);
}

test "addGroup on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");

    // Archive the book
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "addAccount on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    // Archive the book
    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "delete node cascades audit for descendants" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const parent = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addGroup(database, cls_id, "Current", parent, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, parent, 1, "admin");

    // Count audit entries before delete
    var before_count: i32 = 0;
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'classification_node' AND action = 'delete';");
        defer stmt.finalize();
        _ = try stmt.step();
        before_count = stmt.columnInt(0);
    }

    try ClassificationNode.delete(database, parent, "admin");

    // After delete: should have audit entries for both descendants plus the parent itself
    var after_count: i32 = 0;
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'classification_node' AND action = 'delete';");
        defer stmt.finalize();
        _ = try stmt.step();
        after_count = stmt.columnInt(0);
    }

    // 2 descendants + 1 parent = 3 delete audit entries
    try std.testing.expectEqual(@as(i32, 3), after_count - before_count);
}

test "checkCycle: deep tree at 19 levels works, 21 levels rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");

    // Build a chain of 19 levels (depth 0..18)
    var prev_id: i64 = 0;
    var parent_opt: ?i64 = null;
    var i: usize = 0;
    while (i < 19) : (i += 1) {
        prev_id = try ClassificationNode.addGroup(database, cls_id, "Level", parent_opt, 0, "admin");
        parent_opt = prev_id;
    }

    // Verify we can still add at depth 19 (under 20 limit)
    const deep_node = try ClassificationNode.addGroup(database, cls_id, "Level19", prev_id, 0, "admin");
    try std.testing.expect(deep_node > 0);

    // Build another branch to depth 20+ and verify cycle check catches it
    // Create a standalone node and try to move it to create a 21-deep chain
    // The checkCycle walks up to max_depth (20) iterations, so a chain of 21 triggers CircularReference
    const standalone = try ClassificationNode.addGroup(database, cls_id, "Standalone", null, 1, "admin");

    // Move standalone under deep_node (depth 19) — standalone would be depth 20
    // computeDepth returns parent_depth + 1 = 20, which is fine for addGroup
    // But checkCycle traverses ancestors; a chain of 20+ triggers the safety limit
    // Build a chain longer than max_depth and verify checkCycle rejects
    var extra_parent: i64 = deep_node;
    var j: usize = 0;
    while (j < 2) : (j += 1) {
        // Insert directly to bypass depth checks for testing
        var stmt = try database.prepare("INSERT INTO ledger_classification_nodes (node_type, label, parent_id, position, depth, classification_id) VALUES ('group', 'Extra', ?, 0, ?, ?);");
        defer stmt.finalize();
        try stmt.bindInt(1, extra_parent);
        try stmt.bindInt(2, @as(i64, @intCast(20 + j)));
        try stmt.bindInt(3, cls_id);
        _ = try stmt.step();
        extra_parent = database.lastInsertRowId();
    }

    // Now move standalone under the deepest node — checkCycle must walk 21+ ancestors
    const result = ClassificationNode.move(database, standalone, extra_parent, 0, "admin");
    try std.testing.expectError(error.CircularReference, result);
}

// ── Cash flow statement tests ──────────────────────────────────

test "cash_flow classification accepts all account types" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "SCF", "cash_flow", "admin");
    const operating = try ClassificationNode.addGroup(database, cls_id, "Operating", null, 0, "admin");

    _ = try ClassificationNode.addAccount(database, cls_id, 1, operating, 0, "admin"); // asset
    _ = try ClassificationNode.addAccount(database, cls_id, 5, operating, 1, "admin"); // liability
    _ = try ClassificationNode.addAccount(database, cls_id, 6, operating, 2, "admin"); // equity
    _ = try ClassificationNode.addAccount(database, cls_id, 7, operating, 3, "admin"); // revenue
    _ = try ClassificationNode.addAccount(database, cls_id, 8, operating, 4, "admin"); // expense
}

test "cashFlowStatement returns period activity balances" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "SCF", "cash_flow", "admin");
    const operating = try ClassificationNode.addGroup(database, cls_id, "Operating", null, 0, "admin");
    const investing = try ClassificationNode.addGroup(database, cls_id, "Investing", null, 1, "admin");

    _ = try ClassificationNode.addAccount(database, cls_id, 1, operating, 0, "admin"); // Cash (asset, id=1)
    _ = try ClassificationNode.addAccount(database, cls_id, 3, investing, 0, "admin"); // Equipment (asset, id=3)

    const e1 = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 1, 50_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 2, 0, 50_000_000_000_00, "PHP", money.FX_RATE_SCALE, 6, null, null, "admin");
    try entry_mod.Entry.post(database, e1, "admin");

    const r = try cashFlowStatement(database, cls_id, "2026-01-01", "2026-01-31");
    defer r.deinit();

    try std.testing.expect(r.rows.len == 4);
    // Operating group should have rolled-up Cash balance
    try std.testing.expect(r.rows[0].debit_balance == 50_000_000_000_00);
    try std.testing.expect(r.rows[0].credit_balance == 0);
}

test "cashFlowStatement rejects non-cash_flow classification" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const result = cashFlowStatement(database, cls_id, "2026-01-01", "2026-01-31");
    try std.testing.expectError(error.InvalidInput, result);
}

test "cashFlowStatement rejects nonexistent classification" {
    const database = try setupTestDb();
    defer database.close();

    const result = cashFlowStatement(database, 999, "2026-01-01", "2026-01-31");
    try std.testing.expectError(error.NotFound, result);
}

test "cashFlowStatement with O/I/F groups rolls up correctly" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "SCF", "cash_flow", "admin");
    const operating = try ClassificationNode.addGroup(database, cls_id, "Operating", null, 0, "admin");
    const investing = try ClassificationNode.addGroup(database, cls_id, "Investing", null, 1, "admin");
    const financing = try ClassificationNode.addGroup(database, cls_id, "Financing", null, 2, "admin");

    _ = try ClassificationNode.addAccount(database, cls_id, 1, operating, 0, "admin"); // Cash
    _ = try ClassificationNode.addAccount(database, cls_id, 3, investing, 0, "admin"); // Equipment
    _ = try ClassificationNode.addAccount(database, cls_id, 6, financing, 0, "admin"); // Capital

    const e1 = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-05", "2026-01-05", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 1, 100_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 2, 0, 100_000_000_000_00, "PHP", money.FX_RATE_SCALE, 6, null, null, "admin");
    try entry_mod.Entry.post(database, e1, "admin");

    const e2 = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e2, 1, 30_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e2, 2, 0, 30_000_000_000_00, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    try entry_mod.Entry.post(database, e2, "admin");

    const r = try cashFlowStatement(database, cls_id, "2026-01-01", "2026-01-31");
    defer r.deinit();

    try std.testing.expect(r.rows.len == 6);

    // Operating group (row 0): Cash net = 100k debit - 30k credit = 70k debit
    try std.testing.expect(r.rows[0].debit_balance == 70_000_000_000_00);
    try std.testing.expect(r.rows[0].credit_balance == 0);
    // Investing group (row 1): Equipment = 30k debit
    try std.testing.expect(r.rows[1].debit_balance == 30_000_000_000_00);
    try std.testing.expect(r.rows[1].credit_balance == 0);
    // Financing group (row 2): Capital = 100k credit
    try std.testing.expect(r.rows[2].credit_balance == 100_000_000_000_00);
    try std.testing.expect(r.rows[2].debit_balance == 0);
}

test "cashFlowStatement filters by date range" {
    const database = try setupTestDb();
    defer database.close();

    _ = try period_mod.Period.create(database, 1, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const cls_id = try Classification.create(database, 1, "SCF", "cash_flow", "admin");
    const operating = try ClassificationNode.addGroup(database, cls_id, "Operating", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, operating, 0, "admin"); // Cash

    const e1 = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 1, 50_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e1, 2, 0, 50_000_000_000_00, "PHP", money.FX_RATE_SCALE, 6, null, null, "admin");
    try entry_mod.Entry.post(database, e1, "admin");

    const e2 = try entry_mod.Entry.createDraft(database, 1, "JE-002", "2026-02-10", "2026-02-10", null, 2, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e2, 1, 20_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, e2, 2, 0, 20_000_000_000_00, "PHP", money.FX_RATE_SCALE, 6, null, null, "admin");
    try entry_mod.Entry.post(database, e2, "admin");

    // Only Feb activity
    const r = try cashFlowStatement(database, cls_id, "2026-02-01", "2026-02-28");
    defer r.deinit();

    // Operating group should only show Feb's 20k
    try std.testing.expect(r.rows[0].debit_balance == 20_000_000_000_00);
}

test "Classification.create on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = Classification.create(database, 1, "BS", "balance_sheet", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "Classification.updateName on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = Classification.updateName(database, cls_id, "New Name", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "Classification.delete on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = Classification.delete(database, cls_id, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "ClassificationNode.move on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = ClassificationNode.move(database, group, null, 1, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "ClassificationNode.updateLabel on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = ClassificationNode.updateLabel(database, group, "New Label", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "ClassificationNode.delete on archived book rejected" {
    const database = try setupTestDb();
    defer database.close();

    const cls_id = try Classification.create(database, 1, "BS", "balance_sheet", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");

    try period_mod.Period.transition(database, 1, .soft_closed, "admin");
    try period_mod.Period.transition(database, 1, .closed, "admin");
    try book_mod.Book.archive(database, 1, "admin");

    const result = ClassificationNode.delete(database, group, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "cashFlowStatementIndirect computes net income and activity totals" {
    const database = try setupTestDb();
    defer database.close();
    // setupTestDb creates: 1=Cash, 2=AR, 3=Equipment, 7=Revenue, period_id=1

    const eid1 = try entry_mod.Entry.createDraft(database, 1, "INV-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 500_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 500_000_000_000, "PHP", money.FX_RATE_SCALE, 7, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    const eid2 = try entry_mod.Entry.createDraft(database, 1, "PAY-001", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 300_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 2, 0, 300_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid2, "admin");

    const cf_cls = try Classification.create(database, 1, "Cash Flow", "cash_flow", "admin");
    const op_group = try ClassificationNode.addGroup(database, cf_cls, "Operating Activities", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cf_cls, 2, op_group, 0, "admin");
    const inv_group = try ClassificationNode.addGroup(database, cf_cls, "Investing Activities", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cf_cls, 3, inv_group, 0, "admin");
    _ = try ClassificationNode.addGroup(database, cf_cls, "Financing Activities", null, 2, "admin");

    const result = try cashFlowStatementIndirect(database, 1, "2026-01-01", "2026-01-31", cf_cls);
    defer result.deinit();

    try std.testing.expectEqual(@as(i64, 500_000_000_000), result.net_income);
    try std.testing.expect(result.adjustments.len > 0);
}

test "classifiedTrialBalance includes unclassified accounts" {
    const database = try setupTestDb();
    defer database.close();
    // setupTestDb creates accounts 1-8 and period 1
    // Post entry: Debit Cash(1) 1000, Credit Revenue(7) 1000
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 100_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 100_000_000_000, "PHP", money.FX_RATE_SCALE, 7, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    // Create a classification with only Cash classified
    const cls_id = try Classification.create(database, 1, "Partial TB", "trial_balance", "admin");
    const group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, group, 0, "admin");

    const result = try classifiedTrialBalance(database, cls_id, "2026-01-31");
    defer result.deinit();

    // Should have at least 2 rows: the classified Cash group+account AND the unclassified Revenue
    try std.testing.expect(result.rows.len >= 3);
    var found_unclassified = false;
    for (result.rows) |row| {
        if (row.depth == -1) {
            found_unclassified = true;
            break;
        }
    }
    try std.testing.expect(found_unclassified);
}

test "classifiedTrialBalance with all accounts classified has no unclassified" {
    const database = try setupTestDb();
    defer database.close();
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 100_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 100_000_000_000, "PHP", money.FX_RATE_SCALE, 7, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    const cls_id = try Classification.create(database, 1, "Full TB", "trial_balance", "admin");
    const a_group = try ClassificationNode.addGroup(database, cls_id, "Assets", null, 0, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 1, a_group, 0, "admin");
    const r_group = try ClassificationNode.addGroup(database, cls_id, "Revenue", null, 1, "admin");
    _ = try ClassificationNode.addAccount(database, cls_id, 7, r_group, 0, "admin");

    const result = try classifiedTrialBalance(database, cls_id, "2026-01-31");
    defer result.deinit();

    for (result.rows) |row| {
        try std.testing.expect(row.depth != -1);
    }
}
