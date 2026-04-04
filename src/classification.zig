const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");

pub const Classification = struct {
    const valid_types = [_][]const u8{ "balance_sheet", "income_statement", "trial_balance" };

    fn isValidType(t: []const u8) bool {
        for (valid_types) |vt| {
            if (std.mem.eql(u8, t, vt)) return true;
        }
        return false;
    }

    pub fn create(database: db.Database, book_id: i64, name: []const u8, report_type: []const u8, performed_by: []const u8) !i64 {
        if (!isValidType(report_type)) return error.InvalidInput;

        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) == 0) return error.NotFound;
        }

        try database.beginTransaction();
        errdefer database.rollback();

        var stmt = try database.prepare("INSERT INTO ledger_classifications (name, report_type, book_id) VALUES (?, ?, ?);");
        defer stmt.finalize();
        try stmt.bindText(1, name);
        try stmt.bindText(2, report_type);
        try stmt.bindInt(3, book_id);
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "classification", id, "create", null, null, null, performed_by, book_id);

        try database.commit();
        return id;
    }

    pub fn delete(database: db.Database, classification_id: i64, performed_by: []const u8) !void {
        var book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_classifications WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, classification_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        try database.beginTransaction();
        errdefer database.rollback();

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

        try database.commit();
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
            if (parent_text == null) { stmt.reset(); return; }
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
        const book_id = try getBookId(database, classification_id);
        const depth = try computeDepth(database, parent_id);

        try database.beginTransaction();
        errdefer database.rollback();

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

        try database.commit();
        return id;
    }

    pub fn addAccount(database: db.Database, classification_id: i64, account_id: i64, parent_id: ?i64, position: i32, performed_by: []const u8) !i64 {
        // Single query: get book_id + check duplicate in one pass
        var book_id: i64 = 0;
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

        // Validate account type matches report_type (1 query)
        try validateAccountType(database, classification_id, account_id);

        const depth = try computeDepth(database, parent_id);

        try database.beginTransaction();
        errdefer database.rollback();

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

        try database.commit();
        return id;
    }

    pub fn move(database: db.Database, node_id: i64, new_parent_id: ?i64, new_position: i32, performed_by: []const u8) !void {
        // Check cycle
        try checkCycle(database, node_id, new_parent_id);

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT c.book_id FROM ledger_classification_nodes n JOIN ledger_classifications c ON c.id = n.classification_id WHERE n.id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, node_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        const new_depth = try computeDepth(database, new_parent_id);

        try database.beginTransaction();
        errdefer database.rollback();

        {
            var stmt = try database.prepare("UPDATE ledger_classification_nodes SET parent_id = ?, position = ?, depth = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?;");
            defer stmt.finalize();
            if (new_parent_id) |pid| try stmt.bindInt(1, pid) else try stmt.bindNull(1);
            try stmt.bindInt(2, @intCast(new_position));
            try stmt.bindInt(3, @intCast(new_depth));
            try stmt.bindInt(4, node_id);
            _ = try stmt.step();
        }

        try audit.log(database, "classification_node", node_id, "move", "parent_id", null, null, performed_by, book_id);

        try database.commit();
    }
};

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

    const result = Classification.create(database, 1, "Bad", "cash_flow", "admin");
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
    _ = try entry_mod.Entry.addLine(database, 1, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, 1, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 5, null, "admin");
    try entry_mod.Entry.post(database, 1, "admin");

    // Delete classification — should succeed (no FK from entries to classifications)
    try Classification.delete(database, cls_id, "admin");

    // Entries and balances unaffected
    var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
}
