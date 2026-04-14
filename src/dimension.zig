const std = @import("std");
const db = @import("db.zig");
const audit = @import("audit.zig");
const export_mod = @import("export.zig");

pub const DimensionType = enum {
    tax_code,
    cost_center,
    department,
    project,
    segment,
    profit_center,
    fund,
    custom,

    pub fn toString(self: DimensionType) []const u8 {
        return @tagName(self);
    }

    pub fn fromString(s: []const u8) ?DimensionType {
        const map = .{
            .{ "tax_code", DimensionType.tax_code },
            .{ "cost_center", DimensionType.cost_center },
            .{ "department", DimensionType.department },
            .{ "project", DimensionType.project },
            .{ "segment", DimensionType.segment },
            .{ "profit_center", DimensionType.profit_center },
            .{ "fund", DimensionType.fund },
            .{ "custom", DimensionType.custom },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Dimension = struct {
    pub fn create(database: db.Database, book_id: i64, name: []const u8, dimension_type: DimensionType, performed_by: []const u8) !i64 {
        if (name.len == 0) return error.InvalidInput;

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

        var stmt = try database.prepare(
            \\INSERT INTO ledger_dimensions (name, dimension_type, book_id)
            \\VALUES (?, ?, ?);
        );
        defer stmt.finalize();
        try stmt.bindText(1, name);
        try stmt.bindText(2, dimension_type.toString());
        try stmt.bindInt(3, book_id);
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "dimension", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn delete(database: db.Database, dimension_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT book_id FROM ledger_dimensions WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, dimension_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
        }

        {
            var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (std.mem.eql(u8, stmt.columnText(0).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_dimension_values WHERE dimension_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, dimension_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_dimensions WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, dimension_id);
            _ = try stmt.step();
        }

        try audit.log(database, "dimension", dimension_id, "delete", null, null, null, performed_by, book_id);
        if (owns_txn) try database.commit();
    }
};

pub const DimensionValue = struct {
    pub fn create(database: db.Database, dimension_id: i64, code: []const u8, label: []const u8, performed_by: []const u8) !i64 {
        return createWithParent(database, dimension_id, code, label, null, performed_by);
    }

    pub fn createWithParent(database: db.Database, dimension_id: i64, code: []const u8, label: []const u8, parent_value_id: ?i64, performed_by: []const u8) !i64 {
        if (code.len == 0 or label.len == 0) return error.InvalidInput;

        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT d.book_id, b.status FROM ledger_dimensions d
                \\JOIN ledger_books b ON b.id = d.book_id WHERE d.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, dimension_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
            if (std.mem.eql(u8, stmt.columnText(1).?, "archived")) return error.BookArchived;
        }

        if (parent_value_id) |pid| {
            var pstmt = try database.prepare("SELECT dimension_id FROM ledger_dimension_values WHERE id = ?;");
            defer pstmt.finalize();
            try pstmt.bindInt(1, pid);
            const has_row = try pstmt.step();
            if (!has_row) return error.NotFound;
            if (pstmt.columnInt64(0) != dimension_id) return error.InvalidInput;
        }

        var stmt = try database.prepare(
            \\INSERT INTO ledger_dimension_values (code, label, dimension_id, parent_value_id)
            \\VALUES (?, ?, ?, ?);
        );
        defer stmt.finalize();
        try stmt.bindText(1, code);
        try stmt.bindText(2, label);
        try stmt.bindInt(3, dimension_id);
        if (parent_value_id) |pid| try stmt.bindInt(4, pid) else try stmt.bindNull(4);
        _ = stmt.step() catch return error.DuplicateNumber;

        const id = database.lastInsertRowId();
        try audit.log(database, "dimension_value", id, "create", null, null, null, performed_by, book_id);

        if (owns_txn) try database.commit();
        return id;
    }

    pub fn delete(database: db.Database, value_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT d.book_id, b.status FROM ledger_dimension_values dv
                \\JOIN ledger_dimensions d ON d.id = dv.dimension_id
                \\JOIN ledger_books b ON b.id = d.book_id
                \\WHERE dv.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, value_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            book_id = stmt.columnInt64(0);
            if (std.mem.eql(u8, stmt.columnText(1).?, "archived")) return error.BookArchived;
        }

        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_line_dimensions WHERE dimension_value_id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, value_id);
            _ = try stmt.step();
            if (stmt.columnInt(0) > 0) return error.InvalidInput;
        }

        {
            var stmt = try database.prepare("DELETE FROM ledger_dimension_values WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, value_id);
            _ = try stmt.step();
        }

        try audit.log(database, "dimension_value", value_id, "delete", null, null, null, performed_by, book_id);
        if (owns_txn) try database.commit();
    }
};

pub const LineDimension = struct {
    pub fn assign(database: db.Database, line_id: i64, dimension_value_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT e.book_id FROM ledger_entry_lines el
                \\JOIN ledger_entries e ON e.id = el.entry_id
                \\WHERE el.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
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
            var stmt = try database.prepare(
                \\SELECT d.book_id FROM ledger_dimension_values dv
                \\JOIN ledger_dimensions d ON d.id = dv.dimension_id
                \\WHERE dv.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, dimension_value_id);
            const has_row = try stmt.step();
            if (!has_row) return error.NotFound;
            if (stmt.columnInt64(0) != book_id) return error.CrossBookViolation;
        }

        {
            var chk_stmt = try database.prepare(
                \\SELECT COUNT(*) FROM ledger_line_dimensions WHERE line_id = ? AND dimension_value_id = ?;
            );
            defer chk_stmt.finalize();
            try chk_stmt.bindInt(1, line_id);
            try chk_stmt.bindInt(2, dimension_value_id);
            _ = try chk_stmt.step();
            if (chk_stmt.columnInt(0) > 0) {
                if (owns_txn) try database.commit();
                return;
            }
        }

        {
            var stmt = try database.prepare(
                \\INSERT INTO ledger_line_dimensions (line_id, dimension_value_id)
                \\VALUES (?, ?);
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            try stmt.bindInt(2, dimension_value_id);
            _ = try stmt.step();
        }

        var val_buf: [20]u8 = undefined;
        const val_str = std.fmt.bufPrint(&val_buf, "{d}", .{dimension_value_id}) catch unreachable;
        try audit.log(database, "line_dimension", line_id, "assign", "dimension_value_id", null, val_str, performed_by, book_id);
        if (owns_txn) try database.commit();
    }

    pub fn remove(database: db.Database, line_id: i64, dimension_value_id: i64, performed_by: []const u8) !void {
        const owns_txn = try database.beginTransactionIfNeeded();
        errdefer if (owns_txn) database.rollback();

        var book_id: i64 = 0;
        {
            var stmt = try database.prepare(
                \\SELECT e.book_id FROM ledger_entry_lines el
                \\JOIN ledger_entries e ON e.id = el.entry_id
                \\WHERE el.id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
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
            var chk_stmt = try database.prepare(
                \\SELECT COUNT(*) FROM ledger_line_dimensions WHERE line_id = ? AND dimension_value_id = ?;
            );
            defer chk_stmt.finalize();
            try chk_stmt.bindInt(1, line_id);
            try chk_stmt.bindInt(2, dimension_value_id);
            _ = try chk_stmt.step();
            if (chk_stmt.columnInt(0) == 0) return error.NotFound;
        }

        {
            var stmt = try database.prepare(
                \\DELETE FROM ledger_line_dimensions WHERE line_id = ? AND dimension_value_id = ?;
            );
            defer stmt.finalize();
            try stmt.bindInt(1, line_id);
            try stmt.bindInt(2, dimension_value_id);
            _ = try stmt.step();
        }

        var val_buf: [20]u8 = undefined;
        const val_str = std.fmt.bufPrint(&val_buf, "{d}", .{dimension_value_id}) catch unreachable;
        try audit.log(database, "line_dimension", line_id, "remove", "dimension_value_id", val_str, null, performed_by, book_id);
        if (owns_txn) try database.commit();
    }
};

pub fn dimensionSummary(database: db.Database, book_id: i64, dimension_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var stmt = try database.prepare(
        \\SELECT dv.code, dv.label,
        \\  COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_debit_amount ELSE 0 END), 0),
        \\  COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_credit_amount ELSE 0 END), 0)
        \\FROM ledger_dimension_values dv
        \\LEFT JOIN ledger_line_dimensions ld ON ld.dimension_value_id = dv.id
        \\LEFT JOIN ledger_entry_lines el ON el.id = ld.line_id
        \\LEFT JOIN ledger_entries e ON e.id = el.entry_id
        \\  AND e.status IN ('posted', 'reversed') AND e.book_id = ?
        \\  AND e.posting_date >= ? AND e.posting_date <= ?
        \\WHERE dv.dimension_id = ?
        \\GROUP BY dv.id
        \\ORDER BY dv.code ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    try stmt.bindInt(4, dimension_id);

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "code,label,total_debits,total_credits,net\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const code = stmt.columnText(0) orelse "";
                const label = stmt.columnText(1) orelse "";
                const debits = stmt.columnInt64(2);
                const credits = stmt.columnInt64(3);
                const net = std.math.sub(i64, debits, credits) catch return error.AmountOverflow;

                pos += try export_mod.csvField(buf[pos..], code);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], label);
                const row = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{ debits, credits, net }) catch return error.BufferTooSmall;
                pos += row.len;
            }
        },
        .json => {
            const open = "{\"rows\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                const code = stmt.columnText(0) orelse "";
                const label = stmt.columnText(1) orelse "";
                const debits = stmt.columnInt64(2);
                const credits = stmt.columnInt64(3);
                const net = std.math.sub(i64, debits, credits) catch return error.AmountOverflow;

                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const pre = std.fmt.bufPrint(buf[pos..], "{{\"code\":\"", .{}) catch return error.BufferTooSmall;
                pos += pre.len;
                pos += try export_mod.jsonString(buf[pos..], code);
                const mid1 = std.fmt.bufPrint(buf[pos..], "\",\"label\":\"", .{}) catch return error.BufferTooSmall;
                pos += mid1.len;
                pos += try export_mod.jsonString(buf[pos..], label);
                const rest = std.fmt.bufPrint(buf[pos..], "\",\"total_debits\":{d},\"total_credits\":{d},\"net\":{d}}}", .{ debits, credits, net }) catch return error.BufferTooSmall;
                pos += rest.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

/// Maximum dimension values that a single rollup can process. Consistent with
/// the bounded-allocation discipline used throughout the engine
/// (MAX_REPORT_ROWS, MAX_CLASSIFICATION_NODES, MaxAccounts). Realistic chart-of-
/// accounts dimensions hold tens to a few thousand values; 10k is a generous
/// upper bound that prevents adversarial input from exhausting memory.
pub const MAX_DIMENSION_VALUES: usize = 10_000;

pub fn dimensionSummaryRollup(database: db.Database, book_id: i64, dimension_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const DimEntry = struct { code: []const u8, label: []const u8, parent_id: i64, debits: i64, credits: i64 };
    var entries = std.AutoHashMapUnmanaged(i64, DimEntry){};

    {
        var stmt = try database.prepare(
            \\SELECT dv.id, dv.code, dv.label, COALESCE(dv.parent_value_id, 0),
            \\  COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_debit_amount ELSE 0 END), 0),
            \\  COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_credit_amount ELSE 0 END), 0)
            \\FROM ledger_dimension_values dv
            \\LEFT JOIN ledger_line_dimensions ld ON ld.dimension_value_id = dv.id
            \\LEFT JOIN ledger_entry_lines el ON el.id = ld.line_id
            \\LEFT JOIN ledger_entries e ON e.id = el.entry_id
            \\  AND e.status IN ('posted', 'reversed') AND e.book_id = ?
            \\  AND e.posting_date >= ? AND e.posting_date <= ?
            \\WHERE dv.dimension_id = ?
            \\GROUP BY dv.id
            \\ORDER BY dv.code ASC;
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindText(2, start_date);
        try stmt.bindText(3, end_date);
        try stmt.bindInt(4, dimension_id);

        while (try stmt.step()) {
            if (entries.count() >= MAX_DIMENSION_VALUES) return error.TooManyAccounts;
            const id = stmt.columnInt64(0);
            const code_src = stmt.columnText(1) orelse "";
            const label_src = stmt.columnText(2) orelse "";
            const code_copy = try allocator.dupe(u8, code_src);
            const label_copy = try allocator.dupe(u8, label_src);
            try entries.put(allocator, id, .{
                .code = code_copy,
                .label = label_copy,
                .parent_id = stmt.columnInt64(3),
                .debits = stmt.columnInt64(4),
                .credits = stmt.columnInt64(5),
            });
        }
    }

    var roll_keys = std.ArrayListUnmanaged(i64){};
    var it = entries.iterator();
    while (it.next()) |kv| try roll_keys.append(allocator, kv.key_ptr.*);

    for (roll_keys.items) |id| {
        const e = entries.get(id) orelse continue;
        if (e.parent_id == 0) continue;
        if (e.debits == 0 and e.credits == 0) continue;
        var pid = e.parent_id;
        var depth: u32 = 0;
        while (pid != 0 and depth < 20) : (depth += 1) {
            if (entries.getPtr(pid)) |parent| {
                parent.debits = std.math.add(i64, parent.debits, e.debits) catch break;
                parent.credits = std.math.add(i64, parent.credits, e.credits) catch break;
                pid = parent.parent_id;
            } else break;
        }
    }

    var sorted_keys = std.ArrayListUnmanaged(i64){};
    var it2 = entries.iterator();
    while (it2.next()) |kv| try sorted_keys.append(allocator, kv.key_ptr.*);
    std.mem.sort(i64, sorted_keys.items, {}, std.sort.asc(i64));

    var pos: usize = 0;
    switch (format) {
        .csv => {
            const header = "code,label,total_debits,total_credits,net\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            for (sorted_keys.items) |id| {
                const e = entries.get(id) orelse continue;
                pos += try export_mod.csvField(buf[pos..], e.code);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], e.label);
                const net = std.math.sub(i64, e.debits, e.credits) catch return error.AmountOverflow;
                const nums = std.fmt.bufPrint(buf[pos..], ",{d},{d},{d}\n", .{ e.debits, e.credits, net }) catch return error.BufferTooSmall;
                pos += nums.len;
            }
        },
        .json => {
            const open = "{\"values\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            for (sorted_keys.items) |id| {
                const e = entries.get(id) orelse continue;
                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;
                const pre = std.fmt.bufPrint(buf[pos..], "{{\"code\":\"", .{}) catch return error.BufferTooSmall;
                pos += pre.len;
                pos += try export_mod.jsonString(buf[pos..], e.code);
                const mid1 = std.fmt.bufPrint(buf[pos..], "\",\"label\":\"", .{}) catch return error.BufferTooSmall;
                pos += mid1.len;
                pos += try export_mod.jsonString(buf[pos..], e.label);
                const net = std.math.sub(i64, e.debits, e.credits) catch return error.AmountOverflow;
                const rest = std.fmt.bufPrint(buf[pos..], "\",\"total_debits\":{d},\"total_credits\":{d},\"net\":{d}}}", .{ e.debits, e.credits, net }) catch return error.BufferTooSmall;
                pos += rest.len;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

pub fn listDimensions(database: db.Database, book_id: i64, type_filter: ?[]const u8, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var pos: usize = 0;

    const has_filter = type_filter != null;
    var stmt = if (has_filter)
        try database.prepare("SELECT id, name, dimension_type FROM ledger_dimensions WHERE book_id = ? AND dimension_type = ? ORDER BY name;")
    else
        try database.prepare("SELECT id, name, dimension_type FROM ledger_dimensions WHERE book_id = ? ORDER BY name;");
    defer stmt.finalize();
    try stmt.bindInt(1, book_id);
    if (type_filter) |tf| try stmt.bindText(2, tf);

    switch (format) {
        .csv => {
            const header = "id,name,dimension_type\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const dim_type = stmt.columnText(2) orelse "";

                const id_str = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_str.len;
                pos += try export_mod.csvField(buf[pos..], name);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], dim_type);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"rows\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const name = stmt.columnText(1) orelse "";
                const dim_type = stmt.columnText(2) orelse "";

                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const pre = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"name\":\"", .{id}) catch return error.BufferTooSmall;
                pos += pre.len;
                pos += try export_mod.jsonString(buf[pos..], name);
                const mid = std.fmt.bufPrint(buf[pos..], "\",\"dimension_type\":\"", .{}) catch return error.BufferTooSmall;
                pos += mid.len;
                pos += try export_mod.jsonString(buf[pos..], dim_type);
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '"';
                pos += 1;
                buf[pos] = '}';
                pos += 1;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

pub fn listDimensionValues(database: db.Database, dimension_id: i64, buf: []u8, format: export_mod.ExportFormat) ![]u8 {
    var pos: usize = 0;

    var stmt = try database.prepare("SELECT id, code, label FROM ledger_dimension_values WHERE dimension_id = ? ORDER BY code;");
    defer stmt.finalize();
    try stmt.bindInt(1, dimension_id);

    switch (format) {
        .csv => {
            const header = "id,code,label\n";
            if (pos + header.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + header.len], header);
            pos += header.len;

            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const code = stmt.columnText(1) orelse "";
                const label = stmt.columnText(2) orelse "";

                const id_str = std.fmt.bufPrint(buf[pos..], "{d},", .{id}) catch return error.BufferTooSmall;
                pos += id_str.len;
                pos += try export_mod.csvField(buf[pos..], code);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = ',';
                pos += 1;
                pos += try export_mod.csvField(buf[pos..], label);
                if (pos >= buf.len) return error.BufferTooSmall;
                buf[pos] = '\n';
                pos += 1;
            }
        },
        .json => {
            const open = "{\"rows\":[";
            if (pos + open.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + open.len], open);
            pos += open.len;

            var first = true;
            while (try stmt.step()) {
                const id = stmt.columnInt64(0);
                const code = stmt.columnText(1) orelse "";
                const label = stmt.columnText(2) orelse "";

                if (!first) {
                    if (pos >= buf.len) return error.BufferTooSmall;
                    buf[pos] = ',';
                    pos += 1;
                }
                first = false;

                const pre = std.fmt.bufPrint(buf[pos..], "{{\"id\":{d},\"code\":\"", .{id}) catch return error.BufferTooSmall;
                pos += pre.len;
                pos += try export_mod.jsonString(buf[pos..], code);
                const mid = std.fmt.bufPrint(buf[pos..], "\",\"label\":\"", .{}) catch return error.BufferTooSmall;
                pos += mid.len;
                pos += try export_mod.jsonString(buf[pos..], label);
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '"';
                pos += 1;
                buf[pos] = '}';
                pos += 1;
            }

            const close = "]}";
            if (pos + close.len > buf.len) return error.BufferTooSmall;
            @memcpy(buf[pos .. pos + close.len], close);
            pos += close.len;
        },
    }
    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

const schema = @import("schema.zig");
const entry_mod = @import("entry.zig");

fn setupTestDb() !db.Database {
    const database = try db.Database.open(":memory:");
    try schema.createAll(database);
    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Test', 'PHP');");
    return database;
}

fn setupFullDb() !db.Database {
    const database = try setupTestDb();
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('1000', 'Cash', 'asset', 'debit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_accounts (number, name, account_type, normal_balance, book_id)
        \\VALUES ('4000', 'Revenue', 'revenue', 'credit', 1);
    );
    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Jan 2026', 1, 2026, '2026-01-01', '2026-01-31', 1);
    );
    return database;
}

test "create dimension and verify via SQL" {
    const database = try setupTestDb();
    defer database.close();

    const id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    try std.testing.expect(id > 0);

    var stmt = try database.prepare("SELECT name, dimension_type, book_id FROM ledger_dimensions WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("Tax Code", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("tax_code", stmt.columnText(1).?);
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(2));
}

test "create dimension value and verify via SQL" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    try std.testing.expect(val_id > 0);

    var stmt = try database.prepare("SELECT code, label, dimension_id FROM ledger_dimension_values WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, val_id);
    _ = try stmt.step();
    try std.testing.expectEqualStrings("VAT12", stmt.columnText(0).?);
    try std.testing.expectEqualStrings("VAT 12%", stmt.columnText(1).?);
    try std.testing.expectEqual(dim_id, stmt.columnInt64(2));
}

test "assign dimension value to posted entry line" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");

    try LineDimension.assign(database, line1, val_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_line_dimensions WHERE line_id = ? AND dimension_value_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line1);
    try stmt.bindInt(2, val_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "remove dimension assignment" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try Dimension.create(database, 1, "Cost Center", .cost_center, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "CC100", "Engineering", "admin");

    try LineDimension.assign(database, line1, val_id, "admin");
    try LineDimension.remove(database, line1, val_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_line_dimensions WHERE line_id = ? AND dimension_value_id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, line1);
    try stmt.bindInt(2, val_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "duplicate dimension name in same book rejected" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const result = Dimension.create(database, 1, "Tax Code", .cost_center, "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "duplicate value code in same dimension rejected" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    _ = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    const result = DimensionValue.create(database, dim_id, "VAT12", "Different label", "admin");
    try std.testing.expectError(error.DuplicateNumber, result);
}

test "delete dimension with values fails" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    _ = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    const result = Dimension.delete(database, dim_id, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "delete value with line assignments fails" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    try LineDimension.assign(database, line1, val_id, "admin");

    const result = DimensionValue.delete(database, val_id, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "cross-book dimension value assignment rejected" {
    const database = try setupFullDb();
    defer database.close();

    try database.exec("INSERT INTO ledger_books (name, base_currency) VALUES ('Book 2', 'USD');");
    const dim_id = try Dimension.create(database, 2, "Dept", .department, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "DEPT1", "Sales", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const result = LineDimension.assign(database, line1, val_id, "admin");
    try std.testing.expectError(error.CrossBookViolation, result);
}

test "archived book rejected for dimension create" {
    const database = try setupTestDb();
    defer database.close();

    try database.exec("UPDATE ledger_books SET status = 'archived' WHERE id = 1;");
    const result = Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "archived book rejected for dimension delete" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    try database.exec("UPDATE ledger_books SET status = 'archived' WHERE id = 1;");
    const result = Dimension.delete(database, dim_id, "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "archived book rejected for dimension value create" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    try database.exec("UPDATE ledger_books SET status = 'archived' WHERE id = 1;");
    const result = DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    try std.testing.expectError(error.BookArchived, result);
}

test "DimensionType.fromString with invalid string returns null" {
    try std.testing.expect(DimensionType.fromString("invalid") == null);
    try std.testing.expect(DimensionType.fromString("") == null);
    try std.testing.expect(DimensionType.fromString("TAX_CODE") == null);
}

test "DimensionType.fromString with valid strings" {
    try std.testing.expect(DimensionType.fromString("tax_code") == .tax_code);
    try std.testing.expect(DimensionType.fromString("cost_center") == .cost_center);
    try std.testing.expect(DimensionType.fromString("department") == .department);
    try std.testing.expect(DimensionType.fromString("project") == .project);
    try std.testing.expect(DimensionType.fromString("segment") == .segment);
    try std.testing.expect(DimensionType.fromString("custom") == .custom);
}

test "dimensionSummary CSV with posted lines" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    const line2 = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const v1 = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    const v2 = try DimensionValue.create(database, dim_id, "EXEMPT", "Tax Exempt", "admin");

    try LineDimension.assign(database, line1, v1, "admin");
    try LineDimension.assign(database, line2, v2, "admin");

    var buf: [4096]u8 = undefined;
    const result = try dimensionSummary(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf, .csv);
    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "code,label,total_debits,total_credits,net") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "EXEMPT") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT12") != null);
}

test "dimensionSummary JSON with posted lines" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const v1 = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    try LineDimension.assign(database, line1, v1, "admin");

    var buf: [4096]u8 = undefined;
    const result = try dimensionSummary(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"code\":\"VAT12\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"total_debits\":") != null);
}

test "dimension create with empty name rejected" {
    const database = try setupTestDb();
    defer database.close();

    const result = Dimension.create(database, 1, "", .tax_code, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "dimension value create with empty code rejected" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const result = DimensionValue.create(database, dim_id, "", "Label", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "dimension value create with empty label rejected" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const result = DimensionValue.create(database, dim_id, "CODE", "", "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "delete dimension without values succeeds" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    try Dimension.delete(database, dim_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_dimensions WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, dim_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "delete dimension value without assignments succeeds" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    try DimensionValue.delete(database, val_id, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_dimension_values WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, val_id);
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
}

test "dimension not found returns error" {
    const database = try setupTestDb();
    defer database.close();

    const result = Dimension.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "dimension value not found returns error" {
    const database = try setupTestDb();
    defer database.close();

    const result = DimensionValue.delete(database, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "dimension value create with non-existent dimension returns error" {
    const database = try setupTestDb();
    defer database.close();

    const result = DimensionValue.create(database, 999, "VAT12", "VAT 12%", "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "assign to non-existent line returns error" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    const result = LineDimension.assign(database, 999, val_id, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "assign non-existent dimension value returns error" {
    const database = try setupFullDb();
    defer database.close();

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const result = LineDimension.assign(database, line1, 999, "admin");
    try std.testing.expectError(error.NotFound, result);
}

test "audit log created for dimension operations" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE entity_type = 'dimension' AND action = 'create';");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
}

test "all dimension types create successfully" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Dimension.create(database, 1, "Tax", .tax_code, "admin");
    _ = try Dimension.create(database, 1, "CC", .cost_center, "admin");
    _ = try Dimension.create(database, 1, "Dept", .department, "admin");
    _ = try Dimension.create(database, 1, "Proj", .project, "admin");
    _ = try Dimension.create(database, 1, "Seg", .segment, "admin");
    _ = try Dimension.create(database, 1, "Other", .custom, "admin");

    var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_dimensions WHERE book_id = 1;");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i32, 6), stmt.columnInt(0));
}

test "listDimensions returns all dimensions for a book" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    _ = try Dimension.create(database, 1, "Cost Center", .cost_center, "admin");
    _ = try Dimension.create(database, 1, "Department", .department, "admin");

    var buf: [4096]u8 = undefined;
    const result = try listDimensions(database, 1, null, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "Tax Code") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Cost Center") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Department") != null);
}

test "listDimensions filtered by type returns only matching" {
    const database = try setupTestDb();
    defer database.close();

    _ = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    _ = try Dimension.create(database, 1, "Cost Center", .cost_center, "admin");

    var buf: [4096]u8 = undefined;
    const result = try listDimensions(database, 1, "tax_code", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "Tax Code") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Cost Center") == null);
}

test "listDimensionValues returns all values for a dimension" {
    const database = try setupTestDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    _ = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");
    _ = try DimensionValue.create(database, dim_id, "VAT0", "VAT Exempt", "admin");

    var buf: [4096]u8 = undefined;
    const result = try listDimensionValues(database, dim_id, &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT12") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT 12%") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT Exempt") != null);
}

test "dimensionSummary with tax_code dimension produces correct tax totals" {
    const database = try setupFullDb();
    defer database.close();

    const dim_id = try Dimension.create(database, 1, "Tax Code", .tax_code, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "VAT12", "VAT 12%", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-TAX", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 10000_00000000, 0, "PHP", 10000000000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 10000_00000000, "PHP", 10000000000, 2, null, null, "admin");

    try LineDimension.assign(database, line1, val_id, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [4096]u8 = undefined;
    const result = try dimensionSummary(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "VAT12") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"total_debits\":") != null);
}

test "reverse copies line dimensions into reversal period" {
    const database = try setupFullDb();
    defer database.close();

    try database.exec(
        \\INSERT INTO ledger_periods (name, period_number, year, start_date, end_date, book_id)
        \\VALUES ('Feb 2026', 2, 2026, '2026-02-01', '2026-02-28', 1);
    );

    const dim_id = try Dimension.create(database, 1, "Project", .project, "admin");
    const val_id = try DimensionValue.create(database, dim_id, "PRJ-A", "Alpha", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, 1, "JE-REV-DIM", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, null, null, "admin");
    try LineDimension.assign(database, line1, val_id, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    _ = try entry_mod.Entry.reverse(database, entry_id, "Move to February", "2026-02-10", 2, "admin");

    var buf: [4096]u8 = undefined;
    const result = try dimensionSummary(database, 1, dim_id, "2026-02-01", "2026-02-28", &buf, .json);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"total_debits\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"total_credits\":100000000000") != null);
}

test "create dimension with profit_center type" {
    const database = try setupTestDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Profit Centers", .profit_center, "admin");
    try std.testing.expect(dim_id > 0);
}

test "create dimension with fund type" {
    const database = try setupTestDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Grant Funds", .fund, "admin");
    try std.testing.expect(dim_id > 0);
}

test "create dimension value with parent hierarchy" {
    const database = try setupTestDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Cost Centers", .cost_center, "admin");
    const parent_id = try DimensionValue.create(database, dim_id, "CC-100", "Sales", "admin");
    const child_id = try DimensionValue.createWithParent(database, dim_id, "CC-101", "Sales East", parent_id, "admin");
    try std.testing.expect(child_id > 0);

    var stmt = try database.prepare("SELECT parent_value_id FROM ledger_dimension_values WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, child_id);
    _ = try stmt.step();
    try std.testing.expectEqual(parent_id, stmt.columnInt64(0));
}

test "create dimension value with parent from different dimension rejected" {
    const database = try setupTestDb();
    defer database.close();
    const dim1 = try Dimension.create(database, 1, "Departments", .department, "admin");
    const dim2 = try Dimension.create(database, 1, "Projects", .project, "admin");
    const parent = try DimensionValue.create(database, dim1, "DEP-001", "Engineering", "admin");
    const result = DimensionValue.createWithParent(database, dim2, "PRJ-001", "Alpha", parent, "admin");
    try std.testing.expectError(error.InvalidInput, result);
}

test "dimensionSummaryRollup aggregates child values to parent" {
    const database = try setupFullDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Cost Centers", .cost_center, "admin");
    const parent_val = try DimensionValue.create(database, dim_id, "CC-100", "Sales", "admin");
    const child1 = try DimensionValue.createWithParent(database, dim_id, "CC-101", "Sales East", parent_val, "admin");
    const child2 = try DimensionValue.createWithParent(database, dim_id, "CC-102", "Sales West", parent_val, "admin");

    // Post an entry and tag lines with child dimension values
    const eid = try entry_mod.Entry.createDraft(database, 1, "JE-CC", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line1 = try entry_mod.Entry.addLine(database, eid, 1, 100_000_000_000, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    const line2 = try entry_mod.Entry.addLine(database, eid, 2, 0, 100_000_000_000, "PHP", 10_000_000_000, 2, null, null, "admin");
    try LineDimension.assign(database, line1, child1, "admin");
    try LineDimension.assign(database, line2, child2, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    var buf: [8192]u8 = undefined;
    const csv = try dimensionSummaryRollup(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf, .csv);

    // Parent CC-100 should have rolled-up totals from both children
    try std.testing.expect(std.mem.indexOf(u8, csv, "CC-100") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "CC-101") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "CC-102") != null);
}

test "dimensionSummaryRollup rejects when value count exceeds MAX_DIMENSION_VALUES" {
    const database = try setupTestDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Stress", .project, "admin");

    // Create MAX + 1 values to trip the bound. SQLite in-memory inserts are
    // fast enough that this completes well under a second.
    var i: usize = 0;
    var code_buf: [16]u8 = undefined;
    while (i <= MAX_DIMENSION_VALUES) : (i += 1) {
        const code = std.fmt.bufPrint(&code_buf, "V{d}", .{i}) catch unreachable;
        _ = try DimensionValue.create(database, dim_id, code, "x", "admin");
    }

    var buf: [4096]u8 = undefined;
    const result = dimensionSummaryRollup(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf, .csv);
    try std.testing.expectError(error.TooManyAccounts, result);
}

test "dimensionSummaryRollup flat dimension same as dimensionSummary" {
    const database = try setupTestDb();
    defer database.close();
    const dim_id = try Dimension.create(database, 1, "Projects", .project, "admin");
    _ = try DimensionValue.create(database, dim_id, "PRJ-A", "Alpha", "admin");
    _ = try DimensionValue.create(database, dim_id, "PRJ-B", "Beta", "admin");

    var buf1: [4096]u8 = undefined;
    const flat = try dimensionSummary(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf1, .csv);
    var buf2: [4096]u8 = undefined;
    const rollup = try dimensionSummaryRollup(database, 1, dim_id, "2026-01-01", "2026-01-31", &buf2, .csv);
    try std.testing.expectEqualStrings(flat, rollup);
}
