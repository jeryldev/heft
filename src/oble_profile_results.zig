const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const classification_mod = @import("classification.zig");
const dimension_mod = @import("dimension.zig");
const budget_mod = @import("budget.zig");
const money = @import("money.zig");
const report_statements = @import("report_statements.zig");
const report_compare = @import("report_compare.zig");
const report_common = @import("report_common.zig");

const SummaryRow = struct {
    id: i64,
    parent_id: ?i64,
    code: []u8,
    label: []u8,
    debits: i64,
    credits: i64,
};

const BudgetAnalysisRow = struct {
    account_id: i64,
    account_number: []const u8,
    account_name: []const u8,
    budget: i64,
    actual_debit: i64,
    actual_credit: i64,
    actual_net: i64,
    variance: i64,
};

const StatementBoundary = struct {
    as_of_date: ?[]const u8 = null,
    start_date: ?[]const u8 = null,
    end_date: ?[]const u8 = null,
    projected_retained_earnings: bool = false,
};

const ComparativeBoundary = struct {
    current_as_of_date: ?[]const u8 = null,
    prior_as_of_date: ?[]const u8 = null,
    current_start_date: ?[]const u8 = null,
    current_end_date: ?[]const u8 = null,
    prior_start_date: ?[]const u8 = null,
    prior_end_date: ?[]const u8 = null,
    fy_start_date: ?[]const u8 = null,
};

const ExampleKeyPath = struct {
    container: []const u8,
    key: []const u8,
};

pub fn exportClassifiedReportPacketJson(database: db.Database, classification_id: i64, as_of_date: []const u8, buf: []u8) ![]u8 {
    const result = try classification_mod.classifiedReport(database, classification_id, as_of_date);
    defer result.deinit();

    const meta = try getClassificationMeta(database, classification_id);
    return writeClassificationPacket(
        buf,
        "classified_report",
        classification_id,
        meta.book_id,
        meta.report_type,
        as_of_date,
        null,
        null,
        result,
    );
}

pub fn exportClassifiedTrialBalancePacketJson(database: db.Database, classification_id: i64, as_of_date: []const u8, buf: []u8) ![]u8 {
    const result = try classification_mod.classifiedTrialBalance(database, classification_id, as_of_date);
    defer result.deinit();

    const meta = try getClassificationMeta(database, classification_id);
    return writeClassificationPacket(
        buf,
        "classified_trial_balance",
        classification_id,
        meta.book_id,
        meta.report_type,
        as_of_date,
        null,
        null,
        result,
    );
}

pub fn exportCashFlowStatementPacketJson(database: db.Database, classification_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8) ![]u8 {
    const result = try classification_mod.cashFlowStatement(database, classification_id, start_date, end_date);
    defer result.deinit();

    const meta = try getClassificationMeta(database, classification_id);
    return writeClassificationPacket(
        buf,
        "cash_flow_statement",
        classification_id,
        meta.book_id,
        meta.report_type,
        null,
        start_date,
        end_date,
        result,
    );
}

pub fn exportDimensionSummaryResultPacketJson(
    database: db.Database,
    book_id: i64,
    dimension_id: i64,
    start_date: []const u8,
    end_date: []const u8,
    rollup: bool,
    buf: []u8,
) ![]u8 {
    const meta = try getDimensionMeta(database, dimension_id);
    if (meta.book_id != book_id) return error.CrossBookViolation;

    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var rows = try collectDimensionRows(database, allocator, book_id, dimension_id, start_date, end_date);
    if (rollup) try applyDimensionRollup(&rows);

    var pos: usize = 0;
    try appendLiteral(buf, &pos, "{\"packet_kind\":\"");
    try appendLiteral(buf, &pos, if (rollup) "dimension_summary_rollup" else "dimension_summary");
    try appendLiteral(buf, &pos, "\",\"book_id\":\"");
    const book_ref = try std.fmt.bufPrint(buf[pos..], "book-{d}", .{book_id});
    pos += book_ref.len;
    try appendLiteral(buf, &pos, "\",\"dimension_id\":\"");
    const dimension_ref = try std.fmt.bufPrint(buf[pos..], "dimension-{d}", .{dimension_id});
    pos += dimension_ref.len;
    try appendLiteral(buf, &pos, "\",\"dimension_type\":\"");
    pos += try jsonString(buf[pos..], meta.dimension_type);
    try appendLiteral(buf, &pos, "\",\"start_date\":\"");
    pos += try jsonString(buf[pos..], start_date);
    try appendLiteral(buf, &pos, "\",\"end_date\":\"");
    pos += try jsonString(buf[pos..], end_date);
    const tail = try std.fmt.bufPrint(buf[pos..], "\",\"rollup\":{s},\"decimal_places\":{d},\"rows\":[", .{
        if (rollup) "true" else "false",
        meta.decimal_places,
    });
    pos += tail.len;

    for (rows.items, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"dimension_value_id\":\"");
        const value_ref = try std.fmt.bufPrint(buf[pos..], "dimension-value-{d}", .{row.id});
        pos += value_ref.len;
        try appendLiteral(buf, &pos, "\",\"parent_value_id\":");
        if (row.parent_id) |parent_id| {
            try appendLiteral(buf, &pos, "\"");
            const parent_ref = try std.fmt.bufPrint(buf[pos..], "dimension-value-{d}", .{parent_id});
            pos += parent_ref.len;
            try appendLiteral(buf, &pos, "\"");
        } else {
            try appendLiteral(buf, &pos, "null");
        }
        try appendLiteral(buf, &pos, ",\"code\":\"");
        pos += try jsonString(buf[pos..], row.code);
        try appendLiteral(buf, &pos, "\",\"label\":\"");
        pos += try jsonString(buf[pos..], row.label);
        try appendLiteral(buf, &pos, "\",\"total_debits\":\"");
        pos += try appendAmount(buf[pos..], row.debits, meta.decimal_places);
        try appendLiteral(buf, &pos, "\",\"total_credits\":\"");
        pos += try appendAmount(buf[pos..], row.credits, meta.decimal_places);
        const net = std.math.sub(i64, row.debits, row.credits) catch return error.AmountOverflow;
        try appendLiteral(buf, &pos, "\",\"net\":\"");
        pos += try appendAmount(buf[pos..], net, meta.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn exportBudgetAnalysisResultPacketJson(database: db.Database, budget_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8) ![]u8 {
    const meta = try getBudgetMeta(database, budget_id);

    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const rows = try collectBudgetAnalysisRows(database, allocator, budget_id, meta.book_id, start_date, end_date);

    var pos: usize = 0;
    const header = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"packet_kind\":\"budget_vs_actual\",\"book_id\":\"book-{d}\",\"budget_id\":\"budget-{d}\",\"start_date\":\"",
        .{ meta.book_id, budget_id },
    );
    pos += header.len;
    pos += try jsonString(buf[pos..], start_date);
    const mid = try std.fmt.bufPrint(buf[pos..], "\",\"end_date\":\"", .{});
    pos += mid.len;
    pos += try jsonString(buf[pos..], end_date);
    const tail = try std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"rows\":[", .{meta.decimal_places});
    pos += tail.len;

    for (rows.items, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"account_id\":\"");
        const account_ref = try std.fmt.bufPrint(buf[pos..], "acct-{d}", .{row.account_id});
        pos += account_ref.len;
        try appendLiteral(buf, &pos, "\",\"account_number\":\"");
        pos += try jsonString(buf[pos..], row.account_number);
        try appendLiteral(buf, &pos, "\",\"account_name\":\"");
        pos += try jsonString(buf[pos..], row.account_name);
        try appendLiteral(buf, &pos, "\",\"budget\":\"");
        pos += try appendAmount(buf[pos..], row.budget, meta.decimal_places);
        try appendLiteral(buf, &pos, "\",\"actual_debit\":\"");
        pos += try appendAmount(buf[pos..], row.actual_debit, meta.decimal_places);
        try appendLiteral(buf, &pos, "\",\"actual_credit\":\"");
        pos += try appendAmount(buf[pos..], row.actual_credit, meta.decimal_places);
        try appendLiteral(buf, &pos, "\",\"actual_net\":\"");
        pos += try appendAmount(buf[pos..], row.actual_net, meta.decimal_places);
        try appendLiteral(buf, &pos, "\",\"variance\":\"");
        pos += try appendAmount(buf[pos..], row.variance, meta.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

pub fn exportTrialBalanceResultPacketJson(database: db.Database, book_id: i64, as_of_date: []const u8, buf: []u8) ![]u8 {
    const result = try report_statements.trialBalance(database, book_id, as_of_date);
    defer result.deinit();
    return writeStatementPacket(buf, "trial_balance", book_id, .{ .as_of_date = as_of_date }, result);
}

pub fn exportTrialBalanceMovementResultPacketJson(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8) ![]u8 {
    const result = try report_statements.trialBalanceMovement(database, book_id, start_date, end_date);
    defer result.deinit();
    return writeStatementPacket(buf, "trial_balance_movement", book_id, .{
        .start_date = start_date,
        .end_date = end_date,
    }, result);
}

pub fn exportIncomeStatementResultPacketJson(database: db.Database, book_id: i64, start_date: []const u8, end_date: []const u8, buf: []u8) ![]u8 {
    const result = try report_statements.incomeStatement(database, book_id, start_date, end_date);
    defer result.deinit();
    return writeStatementPacket(buf, "income_statement", book_id, .{
        .start_date = start_date,
        .end_date = end_date,
    }, result);
}

pub fn exportBalanceSheetResultPacketJson(database: db.Database, book_id: i64, as_of_date: []const u8, buf: []u8) ![]u8 {
    const result = try report_statements.balanceSheetAutoWithProjectedRE(database, book_id, as_of_date);
    defer result.deinit();
    return writeStatementPacket(buf, "balance_sheet", book_id, .{
        .as_of_date = as_of_date,
        .projected_retained_earnings = true,
    }, result);
}

pub fn exportTrialBalanceComparativeResultPacketJson(database: db.Database, book_id: i64, current_date: []const u8, prior_date: []const u8, buf: []u8) ![]u8 {
    const result = try report_compare.trialBalanceComparative(database, book_id, current_date, prior_date);
    defer result.deinit();
    return writeComparativePacket(buf, "trial_balance_comparative", book_id, .{
        .current_as_of_date = current_date,
        .prior_as_of_date = prior_date,
    }, result);
}

pub fn exportTrialBalanceMovementComparativeResultPacketJson(
    database: db.Database,
    book_id: i64,
    current_start_date: []const u8,
    current_end_date: []const u8,
    prior_start_date: []const u8,
    prior_end_date: []const u8,
    buf: []u8,
) ![]u8 {
    const result = try report_compare.trialBalanceMovementComparative(
        database,
        book_id,
        current_start_date,
        current_end_date,
        prior_start_date,
        prior_end_date,
    );
    defer result.deinit();
    return writeComparativePacket(buf, "trial_balance_movement_comparative", book_id, .{
        .current_start_date = current_start_date,
        .current_end_date = current_end_date,
        .prior_start_date = prior_start_date,
        .prior_end_date = prior_end_date,
    }, result);
}

pub fn exportIncomeStatementComparativeResultPacketJson(
    database: db.Database,
    book_id: i64,
    current_start_date: []const u8,
    current_end_date: []const u8,
    prior_start_date: []const u8,
    prior_end_date: []const u8,
    buf: []u8,
) ![]u8 {
    const result = try report_compare.incomeStatementComparative(
        database,
        book_id,
        current_start_date,
        current_end_date,
        prior_start_date,
        prior_end_date,
    );
    defer result.deinit();
    return writeComparativePacket(buf, "income_statement_comparative", book_id, .{
        .current_start_date = current_start_date,
        .current_end_date = current_end_date,
        .prior_start_date = prior_start_date,
        .prior_end_date = prior_end_date,
    }, result);
}

pub fn exportBalanceSheetComparativeResultPacketJson(
    database: db.Database,
    book_id: i64,
    current_date: []const u8,
    prior_date: []const u8,
    fy_start_date: []const u8,
    buf: []u8,
) ![]u8 {
    const result = try report_compare.balanceSheetComparative(database, book_id, current_date, prior_date, fy_start_date);
    defer result.deinit();
    return writeComparativePacket(buf, "balance_sheet_comparative", book_id, .{
        .current_as_of_date = current_date,
        .prior_as_of_date = prior_date,
        .fy_start_date = fy_start_date,
    }, result);
}

pub fn exportEquityChangesResultPacketJson(
    database: db.Database,
    book_id: i64,
    start_date: []const u8,
    end_date: []const u8,
    fy_start_date: []const u8,
    buf: []u8,
) ![]u8 {
    const result = try report_compare.equityChanges(database, book_id, start_date, end_date, fy_start_date);
    defer result.deinit();
    return writeEquityPacket(buf, book_id, start_date, end_date, fy_start_date, result);
}

const ClassificationMeta = struct {
    book_id: i64,
    report_type: []const u8,
};

fn writeClassificationPacket(
    buf: []u8,
    packet_kind: []const u8,
    classification_id: i64,
    book_id: i64,
    report_type: []const u8,
    as_of_date: ?[]const u8,
    start_date: ?[]const u8,
    end_date: ?[]const u8,
    result: *classification_mod.ClassifiedResult,
) ![]u8 {
    var pos: usize = 0;
    const header = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"packet_kind\":\"{s}\",\"classification_id\":\"classification-{d}\",\"book_id\":\"book-{d}\",\"report_type\":\"",
        .{ packet_kind, classification_id, book_id },
    );
    pos += header.len;
    pos += try jsonString(buf[pos..], report_type);
    if (as_of_date) |date| {
        try appendLiteral(buf, &pos, "\",\"as_of_date\":\"");
        pos += try jsonString(buf[pos..], date);
    } else {
        try appendLiteral(buf, &pos, "\",\"start_date\":\"");
        pos += try jsonString(buf[pos..], start_date orelse return error.InvalidInput);
        try appendLiteral(buf, &pos, "\",\"end_date\":\"");
        pos += try jsonString(buf[pos..], end_date orelse return error.InvalidInput);
    }
    const tail = try std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"total_debits\":\"", .{result.decimal_places});
    pos += tail.len;
    pos += try appendAmount(buf[pos..], result.total_debits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"total_credits\":\"");
    pos += try appendAmount(buf[pos..], result.total_credits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"unclassified_debits\":\"");
    pos += try appendAmount(buf[pos..], result.unclassified_debits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"unclassified_credits\":\"");
    pos += try appendAmount(buf[pos..], result.unclassified_credits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"rows\":[");

    for (result.rows, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"node_id\":\"");
        const node_ref = try std.fmt.bufPrint(buf[pos..], "classification-node-{d}", .{row.node_id});
        pos += node_ref.len;
        try appendLiteral(buf, &pos, "\",\"node_type\":\"");
        pos += try jsonString(buf[pos..], row.node_type[0..row.node_type_len]);
        const mid = try std.fmt.bufPrint(buf[pos..], "\",\"depth\":{d},\"position\":{d},\"label\":\"", .{ row.depth, row.position });
        pos += mid.len;
        pos += try jsonString(buf[pos..], row.label[0..row.label_len]);
        try appendLiteral(buf, &pos, "\",\"account_id\":");
        if (row.account_id != 0) {
            try appendLiteral(buf, &pos, "\"");
            const account_ref = try std.fmt.bufPrint(buf[pos..], "acct-{d}", .{row.account_id});
            pos += account_ref.len;
            try appendLiteral(buf, &pos, "\"");
        } else {
            try appendLiteral(buf, &pos, "null");
        }
        try appendLiteral(buf, &pos, ",\"debit\":\"");
        pos += try appendAmount(buf[pos..], row.debit_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"credit\":\"");
        pos += try appendAmount(buf[pos..], row.credit_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

fn writeStatementPacket(
    buf: []u8,
    packet_kind: []const u8,
    book_id: i64,
    boundary: StatementBoundary,
    result: *report_common.ReportResult,
) ![]u8 {
    var pos: usize = 0;
    const header = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"packet_kind\":\"{s}\",\"book_id\":\"book-{d}\"",
        .{ packet_kind, book_id },
    );
    pos += header.len;
    if (boundary.as_of_date) |date| {
        try appendLiteral(buf, &pos, ",\"as_of_date\":\"");
        pos += try jsonString(buf[pos..], date);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.start_date) |date| {
        try appendLiteral(buf, &pos, ",\"start_date\":\"");
        pos += try jsonString(buf[pos..], date);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.end_date) |date| {
        try appendLiteral(buf, &pos, ",\"end_date\":\"");
        pos += try jsonString(buf[pos..], date);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.projected_retained_earnings) {
        try appendLiteral(buf, &pos, ",\"projected_retained_earnings\":true");
    }
    const tail = try std.fmt.bufPrint(buf[pos..], ",\"decimal_places\":{d},\"total_debits\":\"", .{result.decimal_places});
    pos += tail.len;
    pos += try appendAmount(buf[pos..], result.total_debits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"total_credits\":\"");
    pos += try appendAmount(buf[pos..], result.total_credits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"rows\":[");

    for (result.rows, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"account_id\":\"");
        const account_ref = try std.fmt.bufPrint(buf[pos..], "acct-{d}", .{row.account_id});
        pos += account_ref.len;
        try appendLiteral(buf, &pos, "\",\"account_number\":\"");
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        try appendLiteral(buf, &pos, "\",\"account_name\":\"");
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        try appendLiteral(buf, &pos, "\",\"account_type\":\"");
        pos += try jsonString(buf[pos..], row.account_type[0..row.account_type_len]);
        try appendLiteral(buf, &pos, "\",\"debit\":\"");
        pos += try appendAmount(buf[pos..], row.debit_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"credit\":\"");
        pos += try appendAmount(buf[pos..], row.credit_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

fn writeComparativePacket(
    buf: []u8,
    packet_kind: []const u8,
    book_id: i64,
    boundary: ComparativeBoundary,
    result: *report_common.ComparativeReportResult,
) ![]u8 {
    var pos: usize = 0;
    const header = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"packet_kind\":\"{s}\",\"book_id\":\"book-{d}\"",
        .{ packet_kind, book_id },
    );
    pos += header.len;

    if (boundary.current_as_of_date) |value| {
        try appendLiteral(buf, &pos, ",\"current_as_of_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.prior_as_of_date) |value| {
        try appendLiteral(buf, &pos, ",\"prior_as_of_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.current_start_date) |value| {
        try appendLiteral(buf, &pos, ",\"current_start_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.current_end_date) |value| {
        try appendLiteral(buf, &pos, ",\"current_end_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.prior_start_date) |value| {
        try appendLiteral(buf, &pos, ",\"prior_start_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.prior_end_date) |value| {
        try appendLiteral(buf, &pos, ",\"prior_end_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }
    if (boundary.fy_start_date) |value| {
        try appendLiteral(buf, &pos, ",\"fy_start_date\":\"");
        pos += try jsonString(buf[pos..], value);
        try appendLiteral(buf, &pos, "\"");
    }

    const tail = try std.fmt.bufPrint(
        buf[pos..],
        ",\"decimal_places\":{d},\"current_total_debits\":\"",
        .{result.decimal_places},
    );
    pos += tail.len;
    pos += try appendAmount(buf[pos..], result.current_total_debits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"current_total_credits\":\"");
    pos += try appendAmount(buf[pos..], result.current_total_credits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"prior_total_debits\":\"");
    pos += try appendAmount(buf[pos..], result.prior_total_debits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"prior_total_credits\":\"");
    pos += try appendAmount(buf[pos..], result.prior_total_credits, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"rows\":[");

    for (result.rows, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"account_id\":\"");
        const account_ref = try std.fmt.bufPrint(buf[pos..], "acct-{d}", .{row.account_id});
        pos += account_ref.len;
        try appendLiteral(buf, &pos, "\",\"account_number\":\"");
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        try appendLiteral(buf, &pos, "\",\"account_name\":\"");
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        try appendLiteral(buf, &pos, "\",\"account_type\":\"");
        pos += try jsonString(buf[pos..], row.account_type[0..row.account_type_len]);
        try appendLiteral(buf, &pos, "\",\"current_debit\":\"");
        pos += try appendAmount(buf[pos..], row.current_debit, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"current_credit\":\"");
        pos += try appendAmount(buf[pos..], row.current_credit, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"prior_debit\":\"");
        pos += try appendAmount(buf[pos..], row.prior_debit, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"prior_credit\":\"");
        pos += try appendAmount(buf[pos..], row.prior_credit, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"variance_debit\":\"");
        pos += try appendAmount(buf[pos..], row.variance_debit, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"variance_credit\":\"");
        pos += try appendAmount(buf[pos..], row.variance_credit, result.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

fn writeEquityPacket(
    buf: []u8,
    book_id: i64,
    start_date: []const u8,
    end_date: []const u8,
    fy_start_date: []const u8,
    result: *report_common.EquityResult,
) ![]u8 {
    var pos: usize = 0;
    const header = try std.fmt.bufPrint(
        buf[pos..],
        "{{\"packet_kind\":\"equity_changes\",\"book_id\":\"book-{d}\",\"start_date\":\"",
        .{book_id},
    );
    pos += header.len;
    pos += try jsonString(buf[pos..], start_date);
    try appendLiteral(buf, &pos, "\",\"end_date\":\"");
    pos += try jsonString(buf[pos..], end_date);
    try appendLiteral(buf, &pos, "\",\"fy_start_date\":\"");
    pos += try jsonString(buf[pos..], fy_start_date);
    const tail = try std.fmt.bufPrint(buf[pos..], "\",\"decimal_places\":{d},\"net_income\":\"", .{result.decimal_places});
    pos += tail.len;
    pos += try appendAmount(buf[pos..], result.net_income, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"total_opening\":\"");
    pos += try appendAmount(buf[pos..], result.total_opening, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"total_closing\":\"");
    pos += try appendAmount(buf[pos..], result.total_closing, result.decimal_places);
    try appendLiteral(buf, &pos, "\",\"rows\":[");

    for (result.rows, 0..) |row, i| {
        if (i > 0) try appendLiteral(buf, &pos, ",");
        try appendLiteral(buf, &pos, "{\"account_id\":\"");
        const account_ref = try std.fmt.bufPrint(buf[pos..], "acct-{d}", .{row.account_id});
        pos += account_ref.len;
        try appendLiteral(buf, &pos, "\",\"account_number\":\"");
        pos += try jsonString(buf[pos..], row.account_number[0..row.account_number_len]);
        try appendLiteral(buf, &pos, "\",\"account_name\":\"");
        pos += try jsonString(buf[pos..], row.account_name[0..row.account_name_len]);
        try appendLiteral(buf, &pos, "\",\"opening_balance\":\"");
        pos += try appendAmount(buf[pos..], row.opening_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"period_activity\":\"");
        pos += try appendAmount(buf[pos..], row.period_activity, result.decimal_places);
        try appendLiteral(buf, &pos, "\",\"closing_balance\":\"");
        pos += try appendAmount(buf[pos..], row.closing_balance, result.decimal_places);
        try appendLiteral(buf, &pos, "\"}");
    }

    try appendLiteral(buf, &pos, "]}");
    return buf[0..pos];
}

fn getClassificationMeta(database: db.Database, classification_id: i64) !ClassificationMeta {
    var stmt = try database.prepare(
        \\SELECT c.book_id, c.report_type
        \\FROM ledger_classifications c
        \\WHERE c.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, classification_id);
    if (!try stmt.step()) return error.NotFound;
    return .{
        .book_id = stmt.columnInt64(0),
        .report_type = stmt.columnText(1).?,
    };
}

const DimensionMeta = struct {
    book_id: i64,
    dimension_type: []const u8,
    decimal_places: u8,
};

fn getDimensionMeta(database: db.Database, dimension_id: i64) !DimensionMeta {
    var stmt = try database.prepare(
        \\SELECT d.book_id, d.dimension_type, b.decimal_places
        \\FROM ledger_dimensions d
        \\JOIN ledger_books b ON b.id = d.book_id
        \\WHERE d.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, dimension_id);
    if (!try stmt.step()) return error.NotFound;
    const dp = stmt.columnInt(2);
    return .{
        .book_id = stmt.columnInt64(0),
        .dimension_type = stmt.columnText(1).?,
        .decimal_places = if (dp >= 0 and dp <= 8) @intCast(dp) else 2,
    };
}

fn collectDimensionRows(
    database: db.Database,
    allocator: std.mem.Allocator,
    book_id: i64,
    dimension_id: i64,
    start_date: []const u8,
    end_date: []const u8,
) !std.ArrayListUnmanaged(SummaryRow) {
    var rows = std.ArrayListUnmanaged(SummaryRow){};
    var stmt = try database.prepare(
        \\SELECT dv.id,
        \\       dv.parent_value_id,
        \\       dv.code,
        \\       dv.label,
        \\       COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_debit_amount ELSE 0 END), 0),
        \\       COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN el.base_credit_amount ELSE 0 END), 0)
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
        try rows.append(allocator, .{
            .id = stmt.columnInt64(0),
            .parent_id = if (stmt.columnText(1) != null) stmt.columnInt64(1) else null,
            .code = try allocator.dupe(u8, stmt.columnText(2) orelse ""),
            .label = try allocator.dupe(u8, stmt.columnText(3) orelse ""),
            .debits = stmt.columnInt64(4),
            .credits = stmt.columnInt64(5),
        });
    }
    return rows;
}

fn applyDimensionRollup(rows: *std.ArrayList(SummaryRow)) !void {
    var map = std.AutoHashMap(i64, usize).init(std.heap.c_allocator);
    defer map.deinit();
    for (rows.items, 0..) |row, idx| try map.put(row.id, idx);

    for (rows.items) |row| {
        if (row.parent_id == null) continue;
        if (row.debits == 0 and row.credits == 0) continue;
        var current = row.parent_id;
        var depth: u32 = 0;
        while (current) |pid| : (depth += 1) {
            if (depth >= 20) break;
            const idx = map.get(pid) orelse break;
            rows.items[idx].debits = std.math.add(i64, rows.items[idx].debits, row.debits) catch return error.AmountOverflow;
            rows.items[idx].credits = std.math.add(i64, rows.items[idx].credits, row.credits) catch return error.AmountOverflow;
            current = rows.items[idx].parent_id;
        }
    }
}

const BudgetMeta = struct {
    book_id: i64,
    decimal_places: u8,
};

fn getBudgetMeta(database: db.Database, budget_id: i64) !BudgetMeta {
    var stmt = try database.prepare(
        \\SELECT bud.book_id, b.decimal_places
        \\FROM ledger_budgets bud
        \\JOIN ledger_books b ON b.id = bud.book_id
        \\WHERE bud.id = ?;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    if (!try stmt.step()) return error.NotFound;
    const dp = stmt.columnInt(1);
    return .{
        .book_id = stmt.columnInt64(0),
        .decimal_places = if (dp >= 0 and dp <= 8) @intCast(dp) else 2,
    };
}

fn collectBudgetAnalysisRows(
    database: db.Database,
    allocator: std.mem.Allocator,
    budget_id: i64,
    book_id: i64,
    start_date: []const u8,
    end_date: []const u8,
) !std.ArrayListUnmanaged(BudgetAnalysisRow) {
    var rows = std.ArrayListUnmanaged(BudgetAnalysisRow){};
    var stmt = try database.prepare(
        \\SELECT a.id, a.number, a.name,
        \\  COALESCE(budget.total_budget, 0) as budget_amount,
        \\  COALESCE(actual.actual_debit, 0) as actual_debit,
        \\  COALESCE(actual.actual_credit, 0) as actual_credit,
        \\  a.normal_balance
        \\FROM ledger_accounts a
        \\LEFT JOIN (
        \\  SELECT bl.account_id, SUM(bl.amount) as total_budget
        \\  FROM ledger_budget_lines bl
        \\  JOIN ledger_periods p ON p.id = bl.period_id
        \\  WHERE bl.budget_id = ? AND p.start_date >= ? AND p.end_date <= ?
        \\  GROUP BY bl.account_id
        \\) budget ON budget.account_id = a.id
        \\LEFT JOIN (
        \\  SELECT ab.account_id, SUM(ab.debit_sum) as actual_debit, SUM(ab.credit_sum) as actual_credit
        \\  FROM ledger_account_balances ab
        \\  JOIN ledger_periods p ON p.id = ab.period_id
        \\  WHERE ab.book_id = ? AND p.start_date >= ? AND p.end_date <= ?
        \\  GROUP BY ab.account_id
        \\) actual ON actual.account_id = a.id
        \\WHERE a.book_id = ? AND (budget.total_budget IS NOT NULL OR actual.actual_debit IS NOT NULL)
        \\ORDER BY a.number ASC;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, budget_id);
    try stmt.bindText(2, start_date);
    try stmt.bindText(3, end_date);
    try stmt.bindInt(4, book_id);
    try stmt.bindText(5, start_date);
    try stmt.bindText(6, end_date);
    try stmt.bindInt(7, book_id);

    while (try stmt.step()) {
        const actual_debit = stmt.columnInt64(4);
        const actual_credit = stmt.columnInt64(5);
        const is_credit_normal = if (stmt.columnText(6)) |nb| std.mem.eql(u8, nb, "credit") else false;
        const actual_net = if (is_credit_normal)
            std.math.sub(i64, actual_credit, actual_debit) catch return error.AmountOverflow
        else
            std.math.sub(i64, actual_debit, actual_credit) catch return error.AmountOverflow;
        const budget_amt = stmt.columnInt64(3);
        const variance = std.math.sub(i64, actual_net, budget_amt) catch return error.AmountOverflow;
        try rows.append(allocator, .{
            .account_id = stmt.columnInt64(0),
            .account_number = try allocator.dupe(u8, stmt.columnText(1) orelse ""),
            .account_name = try allocator.dupe(u8, stmt.columnText(2) orelse ""),
            .budget = budget_amt,
            .actual_debit = actual_debit,
            .actual_credit = actual_credit,
            .actual_net = actual_net,
            .variance = variance,
        });
    }

    return rows;
}

fn appendLiteral(buf: []u8, pos: *usize, literal: []const u8) !void {
    if (pos.* + literal.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[pos.* .. pos.* + literal.len], literal);
    pos.* += literal.len;
}

fn jsonString(buf: []u8, text: []const u8) !usize {
    var pos: usize = 0;
    for (text) |c| {
        switch (c) {
            '"', '\\' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = c;
                pos += 2;
            },
            '\n' => {
                if (pos + 2 > buf.len) return error.BufferTooSmall;
                buf[pos] = '\\';
                buf[pos + 1] = 'n';
                pos += 2;
            },
            else => {
                if (pos + 1 > buf.len) return error.BufferTooSmall;
                buf[pos] = c;
                pos += 1;
            },
        }
    }
    return pos;
}

fn appendAmount(buf: []u8, amount: i64, dp: u8) !usize {
    const rendered = try money.formatDecimal(buf, amount, dp);
    return rendered.len;
}

test "OBLE results: classified report packet exports canonical JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Classified Result Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const equity_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-1", "2026-01-01", "2026-01-01", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 1_500_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_500_00_000_000, "USD", money.FX_RATE_SCALE, equity_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const classification_id = try classification_mod.Classification.create(database, book_id, "Balance Sheet", "balance_sheet", "admin");
    const group_id = try classification_mod.ClassificationNode.addGroup(database, classification_id, "Assets", null, 1, "admin");
    _ = try classification_mod.ClassificationNode.addAccount(database, classification_id, cash_id, group_id, 1, "admin");

    var buf: [256 * 1024]u8 = undefined;
    const json = try exportClassifiedReportPacketJson(database, classification_id, "2026-01-31", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"packet_kind\":\"classified_report\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"classification_id\":\"classification-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"node_id\":\"classification-node-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"debit\":\"1500.00\"") != null);
}

test "OBLE results: dimension summary and rollup packets export canonical JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Dimension Result Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-1", "2026-01-10", "2026-01-10", null, period_id, null, "admin");
    const line_id = try entry_mod.Entry.addLine(database, entry_id, 1, 100_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 100_00_000_000, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    const dim_id = try dimension_mod.Dimension.create(database, book_id, "Tax Code", .tax_code, "admin");
    const parent_id = try dimension_mod.DimensionValue.create(database, dim_id, "VAT", "VAT", "admin");
    const child_id = try dimension_mod.DimensionValue.createWithParent(database, dim_id, "VAT12", "VAT 12%", parent_id, "admin");
    try dimension_mod.LineDimension.assign(database, line_id, child_id, "admin");

    var flat_buf: [256 * 1024]u8 = undefined;
    var roll_buf: [256 * 1024]u8 = undefined;
    const flat_json = try exportDimensionSummaryResultPacketJson(database, book_id, dim_id, "2026-01-01", "2026-01-31", false, &flat_buf);
    const roll_json = try exportDimensionSummaryResultPacketJson(database, book_id, dim_id, "2026-01-01", "2026-01-31", true, &roll_buf);

    try std.testing.expect(std.mem.indexOf(u8, flat_json, "\"packet_kind\":\"dimension_summary\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, flat_json, "\"dimension_value_id\":\"dimension-value-2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, flat_json, "\"net\":\"100.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, roll_json, "\"packet_kind\":\"dimension_summary_rollup\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, roll_json, "\"dimension_value_id\":\"dimension-value-1\"") != null);
}

test "OBLE results: budget analysis packet exports canonical JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Budget Result Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const equity_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const budget_id = try budget_mod.Budget.create(database, book_id, "FY2026 Plan", 2026, "admin");
    _ = try budget_mod.BudgetLine.set(database, budget_id, cash_id, jan_id, 1_000_00_000_000, "admin");
    try budget_mod.Budget.transition(database, budget_id, .approved, "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-1", "2026-01-01", "2026-01-01", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 1_200_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 1_200_00_000_000, "USD", money.FX_RATE_SCALE, equity_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var buf: [256 * 1024]u8 = undefined;
    const json = try exportBudgetAnalysisResultPacketJson(database, budget_id, "2026-01-01", "2026-01-31", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"packet_kind\":\"budget_vs_actual\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"account_id\":\"acct-1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"budget\":\"1000.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"actual_net\":\"1200.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"variance\":\"200.00\"") != null);
}

test "OBLE results: statement, comparative, and equity packets export canonical JSON" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Statement Result Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const revenue_id = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const jan_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_id = try period_mod.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");

    const open_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-1", "2026-01-01", "2026-01-01", null, jan_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 1, 1_000_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, open_id, 2, 0, 1_000_00_000_000, "USD", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(database, open_id, "admin");

    const sale_id = try entry_mod.Entry.createDraft(database, book_id, "SALE-1", "2026-02-10", "2026-02-10", null, feb_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 1, 200_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, sale_id, 2, 0, 200_00_000_000, "USD", money.FX_RATE_SCALE, revenue_id, null, null, "admin");
    try entry_mod.Entry.post(database, sale_id, "admin");

    var statement_buf: [256 * 1024]u8 = undefined;
    var comparative_buf: [256 * 1024]u8 = undefined;
    var equity_buf: [256 * 1024]u8 = undefined;
    const statement_example = try loadExampleJson(std.testing.allocator, "docs/oble/examples/statement-result.json");
    defer std.testing.allocator.free(statement_example);
    const comparative_example = try loadExampleJson(std.testing.allocator, "docs/oble/examples/comparative-statement-result.json");
    defer std.testing.allocator.free(comparative_example);
    const equity_example = try loadExampleJson(std.testing.allocator, "docs/oble/examples/equity-result.json");
    defer std.testing.allocator.free(equity_example);

    const tb_json = try exportTrialBalanceResultPacketJson(database, book_id, "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, tb_json, "\"packet_kind\":\"trial_balance\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, tb_json, "\"total_debits\":\"1200.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, tb_json, "\"total_credits\":\"1200.00\"") != null);
    try expectPacketMatchesExampleShape(tb_json, statement_example, &.{
        .{ .container = "", .key = "packet_kind" },
        .{ .container = "", .key = "book_id" },
        .{ .container = "", .key = "as_of_date" },
        .{ .container = "", .key = "decimal_places" },
        .{ .container = "", .key = "total_debits" },
        .{ .container = "", .key = "total_credits" },
        .{ .container = "rows", .key = "account_id" },
        .{ .container = "rows", .key = "account_number" },
        .{ .container = "rows", .key = "account_name" },
        .{ .container = "rows", .key = "account_type" },
        .{ .container = "rows", .key = "debit" },
        .{ .container = "rows", .key = "credit" },
    });

    const is_json = try exportIncomeStatementResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, is_json, "\"packet_kind\":\"income_statement\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, is_json, "\"credit\":\"200.00\"") != null);

    const bs_json = try exportBalanceSheetResultPacketJson(database, book_id, "2026-02-28", &statement_buf);
    try std.testing.expect(std.mem.indexOf(u8, bs_json, "\"packet_kind\":\"balance_sheet\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bs_json, "\"projected_retained_earnings\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, bs_json, "\"total_debits\":\"1200.00\"") != null);

    const cmp_json = try exportTrialBalanceComparativeResultPacketJson(database, book_id, "2026-02-28", "2026-01-31", &comparative_buf);
    try std.testing.expect(std.mem.indexOf(u8, cmp_json, "\"packet_kind\":\"trial_balance_comparative\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cmp_json, "\"current_total_debits\":\"1200.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cmp_json, "\"prior_total_debits\":\"1000.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cmp_json, "\"variance_debit\":\"200.00\"") != null);
    try expectPacketMatchesExampleShape(cmp_json, comparative_example, &.{
        .{ .container = "", .key = "packet_kind" },
        .{ .container = "", .key = "book_id" },
        .{ .container = "", .key = "current_as_of_date" },
        .{ .container = "", .key = "prior_as_of_date" },
        .{ .container = "", .key = "decimal_places" },
        .{ .container = "", .key = "current_total_debits" },
        .{ .container = "", .key = "current_total_credits" },
        .{ .container = "", .key = "prior_total_debits" },
        .{ .container = "", .key = "prior_total_credits" },
        .{ .container = "rows", .key = "account_id" },
        .{ .container = "rows", .key = "account_number" },
        .{ .container = "rows", .key = "account_name" },
        .{ .container = "rows", .key = "account_type" },
        .{ .container = "rows", .key = "current_debit" },
        .{ .container = "rows", .key = "current_credit" },
        .{ .container = "rows", .key = "prior_debit" },
        .{ .container = "rows", .key = "prior_credit" },
        .{ .container = "rows", .key = "variance_debit" },
        .{ .container = "rows", .key = "variance_credit" },
    });

    const eq_json = try exportEquityChangesResultPacketJson(database, book_id, "2026-02-01", "2026-02-28", "2026-01-01", &equity_buf);
    try std.testing.expect(std.mem.indexOf(u8, eq_json, "\"packet_kind\":\"equity_changes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, eq_json, "\"net_income\":\"200.00\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, eq_json, "\"total_opening\":\"1000.00\"") != null);
    try expectPacketMatchesExampleShape(eq_json, equity_example, &.{
        .{ .container = "", .key = "packet_kind" },
        .{ .container = "", .key = "book_id" },
        .{ .container = "", .key = "start_date" },
        .{ .container = "", .key = "end_date" },
        .{ .container = "", .key = "fy_start_date" },
        .{ .container = "", .key = "decimal_places" },
        .{ .container = "", .key = "net_income" },
        .{ .container = "", .key = "total_opening" },
        .{ .container = "", .key = "total_closing" },
        .{ .container = "rows", .key = "account_id" },
        .{ .container = "rows", .key = "account_number" },
        .{ .container = "rows", .key = "account_name" },
        .{ .container = "rows", .key = "opening_balance" },
        .{ .container = "rows", .key = "period_activity" },
        .{ .container = "rows", .key = "closing_balance" },
    });
}

test "OBLE results: statement packet exporters fail cleanly on tiny buffer" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Tiny Buffer Book", "USD", 2, "admin");
    const cash_id = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital_id = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const entry_id = try entry_mod.Entry.createDraft(database, book_id, "OPEN-1", "2026-01-01", "2026-01-01", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 1, 100_00_000_000, 0, "USD", money.FX_RATE_SCALE, cash_id, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, entry_id, 2, 0, 100_00_000_000, "USD", money.FX_RATE_SCALE, capital_id, null, null, "admin");
    try entry_mod.Entry.post(database, entry_id, "admin");

    var tiny_buf: [32]u8 = undefined;
    try std.testing.expectError(error.NoSpaceLeft, exportTrialBalanceResultPacketJson(database, book_id, "2026-01-31", &tiny_buf));
}

fn expectPacketMatchesExampleShape(actual_json: []const u8, example_json: []const u8, required_keys: []const ExampleKeyPath) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const actual = try std.json.parseFromSlice(std.json.Value, allocator, actual_json, .{});
    const example = try std.json.parseFromSlice(std.json.Value, allocator, example_json, .{});

    try expectObjectHasKeys(actual.value, example.value, required_keys);
}

fn expectObjectHasKeys(actual: std.json.Value, example: std.json.Value, required_keys: []const ExampleKeyPath) !void {
    const actual_object = actual.object;
    const example_object = example.object;

    for (required_keys) |required| {
        if (required.container.len == 0) {
            try std.testing.expect(actual_object.get(required.key) != null);
            try std.testing.expect(example_object.get(required.key) != null);
            continue;
        }

        const actual_container = actual_object.get(required.container) orelse {
            try std.testing.expect(false);
            return;
        };
        const example_container = example_object.get(required.container) orelse {
            try std.testing.expect(false);
            return;
        };
        try expectArrayFirstObjectHasKey(actual_container, required.key);
        try expectArrayFirstObjectHasKey(example_container, required.key);
    }
}

fn expectArrayFirstObjectHasKey(value: std.json.Value, key: []const u8) !void {
    const array = value.array;
    try std.testing.expect(array.items.len > 0);
    try std.testing.expect(array.items[0].object.get(key) != null);
}

fn loadExampleJson(allocator: std.mem.Allocator, relative_path: []const u8) ![]u8 {
    return try std.fs.cwd().readFileAlloc(allocator, relative_path, 64 * 1024);
}
