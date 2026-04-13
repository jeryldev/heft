const std = @import("std");
const heft = @import("heft");
const abi_common = @import("abi_common.zig");
const abi_buffers = @import("abi_buffers.zig");

const ReportSeed = struct {
    database: heft.db.Database,
    book_id: i64,
    customer_group_id: i64,
    cash_id: i64,
    ar_id: i64,
    revenue_id: i64,
    expense_id: i64,
};

const BudgetWorkload = struct {
    database: heft.db.Database,
    budget_id: i64,
};

const OpenItemWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    counterparty_id: i64,
    seed_line_id: i64,
    seed_amount: i64,
};

const ClassificationWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    balance_sheet_id: i64,
    income_statement_id: i64,
    trial_balance_id: i64,
    cash_flow_id: i64,
};

const DimensionWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    dimension_id: i64,
};

const AbiBufferWorkload = struct {
    database: heft.db.Database,
    handle: abi_common.LedgerDB,
    book_id: i64,
    counterparty_id: i64,
    dimension_id: i64,
};

pub fn runBudgetSuiteBench(sink: *usize, iterations: usize, entry_count: usize, counterparty_count: usize) !void {
    var setup_timer = try std.time.Timer.start();
    var workload = try setupBudgetWorkload(entry_count, counterparty_count);
    const setup_ns = setup_timer.read();
    defer workload.database.close();
    try printSplitBench("budget_seed", 1, setup_ns, 0);
    try runBench(BudgetWorkload, sink, "budget_vs_actual", iterations, setup_ns, &workload, benchBudgetVsActual);
}

pub fn runOpenItemSuiteBench(sink: *usize, iterations: usize, entry_count: usize, counterparty_count: usize) !void {
    var setup_timer = try std.time.Timer.start();
    var workload = try setupOpenItemWorkload(entry_count, counterparty_count);
    const setup_ns = setup_timer.read();
    defer workload.database.close();
    try printSplitBench("open_item_seed", 1, setup_ns, 0);
    try runBench(OpenItemWorkload, sink, "open_items_active", iterations, setup_ns, &workload, benchOpenItemsActive);
    try runBench(OpenItemWorkload, sink, "open_items_all", iterations, setup_ns, &workload, benchOpenItemsAll);
    try runBench(OpenItemWorkload, sink, "open_item_allocate", iterations, setup_ns, &workload, benchOpenItemAllocate);
}

pub fn runClassificationSuiteBench(sink: *usize, iterations: usize, entry_count: usize, counterparty_count: usize) !void {
    var setup_timer = try std.time.Timer.start();
    var workload = try setupClassificationWorkload(entry_count, counterparty_count);
    const setup_ns = setup_timer.read();
    defer workload.database.close();
    try printSplitBench("classification_seed", 1, setup_ns, 0);
    try runBench(ClassificationWorkload, sink, "classified_balance_sheet", iterations, setup_ns, &workload, benchClassifiedBalanceSheet);
    try runBench(ClassificationWorkload, sink, "classified_income_statement", iterations, setup_ns, &workload, benchClassifiedIncomeStatement);
    try runBench(ClassificationWorkload, sink, "classified_trial_balance", iterations, setup_ns, &workload, benchClassifiedTrialBalance);
    try runBench(ClassificationWorkload, sink, "cash_flow_direct", iterations, setup_ns, &workload, benchCashFlowDirect);
    try runBench(ClassificationWorkload, sink, "cash_flow_indirect", iterations, setup_ns, &workload, benchCashFlowIndirect);
}

pub fn runDimensionSuiteBench(sink: *usize, iterations: usize, entry_count: usize, counterparty_count: usize) !void {
    var setup_timer = try std.time.Timer.start();
    var workload = try setupDimensionWorkload(entry_count, counterparty_count);
    const setup_ns = setup_timer.read();
    defer workload.database.close();
    try printSplitBench("dimension_seed", 1, setup_ns, 0);
    try runBench(DimensionWorkload, sink, "dimension_summary", iterations, setup_ns, &workload, benchDimensionSummary);
    try runBench(DimensionWorkload, sink, "dimension_rollup", iterations, setup_ns, &workload, benchDimensionRollup);
    try runBench(DimensionWorkload, sink, "list_dimensions", iterations, setup_ns, &workload, benchListDimensions);
    try runBench(DimensionWorkload, sink, "list_dimension_values", iterations, setup_ns, &workload, benchListDimensionValues);
}

pub fn runAbiBufferSuiteBench(sink: *usize, iterations: usize, entry_count: usize, counterparty_count: usize) !void {
    var setup_timer = try std.time.Timer.start();
    var workload = try setupAbiBufferWorkload(entry_count, counterparty_count);
    const setup_ns = setup_timer.read();
    defer workload.database.close();
    try printSplitBench("abi_buffer_seed", 1, setup_ns, 0);
    try runBench(AbiBufferWorkload, sink, "abi_list_entries", iterations, setup_ns, &workload, benchAbiListEntries);
    try runBench(AbiBufferWorkload, sink, "abi_list_audit_log", iterations, setup_ns, &workload, benchAbiListAuditLog);
    try runBench(AbiBufferWorkload, sink, "abi_list_transactions", iterations, setup_ns, &workload, benchAbiListTransactions);
    try runBench(AbiBufferWorkload, sink, "abi_dimension_summary", iterations, setup_ns, &workload, benchAbiDimensionSummary);
    try runBench(AbiBufferWorkload, sink, "abi_list_open_items", iterations, setup_ns, &workload, benchAbiListOpenItems);
    try runBench(AbiBufferWorkload, sink, "abi_export_journal_entries", iterations, setup_ns, &workload, benchAbiExportJournalEntries);
}

fn runBench(comptime T: type, sink: *usize, label: []const u8, iterations: usize, setup_ns: u64, workload: *T, comptime runner: fn (*usize, *T) anyerror!void) !void {
    var timer = try std.time.Timer.start();
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        try runner(sink, workload);
    }
    try printSplitBench(label, iterations, setup_ns, timer.read());
}

fn printSplitBench(label: []const u8, iterations: usize, setup_ns: u64, op_ns: u64) !void {
    const count = @as(u64, @intCast(if (iterations == 0) 1 else iterations));
    const setup_avg_ms = @as(f64, @floatFromInt(setup_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
    const op_avg_ms = @as(f64, @floatFromInt(op_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
    const total_ns = setup_ns + op_ns;
    const ops_per_sec = if (op_ns == 0)
        0.0
    else
        (@as(f64, @floatFromInt(iterations)) * @as(f64, @floatFromInt(std.time.ns_per_s))) / @as(f64, @floatFromInt(op_ns));
    const total_ms = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
    std.debug.print(
        "{s:24} total={d:.3}ms setup_avg={d:.3}ms op_avg={d:.3}ms op_only_ops/s={d:.2}\n",
        .{ label, total_ms, setup_avg_ms, op_avg_ms, ops_per_sec },
    );
}

fn setupReportSeed(entry_count: usize, counterparty_count: usize) !ReportSeed {
    const database = try heft.db.Database.open(":memory:");
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Bench Feature Suites", "PHP", 2, "bench");
    const cash_id = try heft.account.Account.create(database, book_id, "1000", "Cash", .asset, false, "bench");
    const ar_id = try heft.account.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "bench");
    const revenue_id = try heft.account.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "bench");
    const expense_id = try heft.account.Account.create(database, book_id, "5000", "Expense", .expense, false, "bench");

    const customer_group_id = try heft.subledger.SubledgerGroup.create(database, book_id, "Customers", "customer", 1, ar_id, null, null, "bench");

    const cp_count = @max(counterparty_count, 1);
    const customer_ids = try std.heap.page_allocator.alloc(i64, cp_count);
    defer std.heap.page_allocator.free(customer_ids);

    var idx: usize = 0;
    while (idx < cp_count) : (idx += 1) {
        var number_buf: [16]u8 = undefined;
        var name_buf: [32]u8 = undefined;
        const number = try std.fmt.bufPrint(&number_buf, "C{d:0>4}", .{idx + 1});
        const name = try std.fmt.bufPrint(&name_buf, "Customer {d}", .{idx + 1});
        customer_ids[idx] = try heft.subledger.SubledgerAccount.create(database, book_id, number, name, "customer", customer_group_id, "bench");
    }

    var period_ids: [12]i64 = undefined;
    var month: usize = 0;
    while (month < period_ids.len) : (month += 1) {
        const month_number = month + 1;
        var name_buf: [24]u8 = undefined;
        var start_buf: [16]u8 = undefined;
        var end_buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "P{d}", .{month_number});
        const start = try std.fmt.bufPrint(&start_buf, "2026-{d:0>2}-01", .{month_number});
        const end = try std.fmt.bufPrint(&end_buf, "2026-{d:0>2}-28", .{month_number});
        period_ids[month] = try heft.period.Period.create(database, book_id, name, @intCast(month_number), 2026, start, end, "regular", "bench");
    }

    idx = 0;
    while (idx < entry_count) : (idx += 1) {
        const month_index = idx % period_ids.len;
        const month_number = month_index + 1;
        const customer_id = customer_ids[idx % cp_count];
        const day = (idx % 28) + 1;

        var doc_buf: [32]u8 = undefined;
        var date_buf: [16]u8 = undefined;
        const doc = try std.fmt.bufPrint(&doc_buf, "INV-{d:0>6}", .{idx + 1});
        const date = try std.fmt.bufPrint(&date_buf, "2026-{d:0>2}-{d:0>2}", .{ month_number, day });
        const amount = @as(i64, @intCast(100 + (idx % 25))) * heft.money.AMOUNT_SCALE;

        const invoice_id = try heft.entry.Entry.createDraft(database, book_id, doc, date, date, null, period_ids[month_index], null, "bench");
        _ = try heft.entry.Entry.addLine(database, invoice_id, 1, amount, 0, "PHP", heft.money.FX_RATE_SCALE, ar_id, customer_id, null, "bench");
        _ = try heft.entry.Entry.addLine(database, invoice_id, 2, 0, amount, "PHP", heft.money.FX_RATE_SCALE, revenue_id, null, null, "bench");
        try heft.entry.Entry.post(database, invoice_id, "bench");

        if (idx % 3 == 0) {
            var pay_doc_buf: [32]u8 = undefined;
            const pay_doc = try std.fmt.bufPrint(&pay_doc_buf, "PAY-{d:0>6}", .{idx + 1});
            const pay_amount = @divTrunc(amount, 2);
            const payment_id = try heft.entry.Entry.createDraft(database, book_id, pay_doc, date, date, null, period_ids[month_index], null, "bench");
            _ = try heft.entry.Entry.addLine(database, payment_id, 1, pay_amount, 0, "PHP", heft.money.FX_RATE_SCALE, cash_id, null, null, "bench");
            _ = try heft.entry.Entry.addLine(database, payment_id, 2, 0, pay_amount, "PHP", heft.money.FX_RATE_SCALE, ar_id, customer_id, null, "bench");
            try heft.entry.Entry.post(database, payment_id, "bench");
        }

        if (idx % 5 == 0) {
            var exp_doc_buf: [32]u8 = undefined;
            const exp_doc = try std.fmt.bufPrint(&exp_doc_buf, "EXP-{d:0>6}", .{idx + 1});
            const exp_amount = @as(i64, @intCast(40 + (idx % 11))) * heft.money.AMOUNT_SCALE;
            const expense_entry = try heft.entry.Entry.createDraft(database, book_id, exp_doc, date, date, null, period_ids[month_index], null, "bench");
            _ = try heft.entry.Entry.addLine(database, expense_entry, 1, exp_amount, 0, "PHP", heft.money.FX_RATE_SCALE, expense_id, null, null, "bench");
            _ = try heft.entry.Entry.addLine(database, expense_entry, 2, 0, exp_amount, "PHP", heft.money.FX_RATE_SCALE, cash_id, null, null, "bench");
            try heft.entry.Entry.post(database, expense_entry, "bench");
        }
    }

    return .{
        .database = database,
        .book_id = book_id,
        .customer_group_id = customer_group_id,
        .cash_id = cash_id,
        .ar_id = ar_id,
        .revenue_id = revenue_id,
        .expense_id = expense_id,
    };
}

fn setupBudgetWorkload(entry_count: usize, counterparty_count: usize) !BudgetWorkload {
    const seed = try setupReportSeed(entry_count, counterparty_count);
    const budget_id = try heft.budget.Budget.create(seed.database, seed.book_id, "Bench FY2026", 2026, "bench");

    var stmt = try seed.database.prepare("SELECT id, period_number FROM ledger_periods WHERE book_id = ? ORDER BY start_date ASC;");
    defer stmt.finalize();
    try stmt.bindInt(1, seed.book_id);
    while (try stmt.step()) {
        const period_id = stmt.columnInt64(0);
        const period_number = stmt.columnInt(1);
        const revenue_budget = @as(i64, 180 + period_number) * heft.money.AMOUNT_SCALE;
        const expense_budget = @as(i64, 75 + period_number) * heft.money.AMOUNT_SCALE;
        _ = try heft.budget.BudgetLine.set(seed.database, budget_id, seed.revenue_id, period_id, revenue_budget, "bench");
        _ = try heft.budget.BudgetLine.set(seed.database, budget_id, seed.expense_id, period_id, expense_budget, "bench");
    }

    return .{ .database = seed.database, .budget_id = budget_id };
}

fn setupOpenItemWorkload(entry_count: usize, counterparty_count: usize) !OpenItemWorkload {
    const seed = try setupReportSeed(entry_count, counterparty_count);
    var customer_stmt = try seed.database.prepare("SELECT id FROM ledger_subledger_accounts WHERE group_id = ? ORDER BY number ASC LIMIT 1;");
    defer customer_stmt.finalize();
    try customer_stmt.bindInt(1, seed.customer_group_id);
    if (!try customer_stmt.step()) return error.NotFound;
    const customer_id = customer_stmt.columnInt64(0);

    var line_stmt = try seed.database.prepare(
        \\SELECT el.id, el.base_debit_amount
        \\FROM ledger_entry_lines el
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\WHERE e.book_id = ? AND el.counterparty_id = ? AND el.base_debit_amount > 0
        \\ORDER BY e.id ASC
        \\LIMIT 64;
    );
    defer line_stmt.finalize();
    try line_stmt.bindInt(1, seed.book_id);
    try line_stmt.bindInt(2, customer_id);

    var created: usize = 0;
    var seed_line_id: i64 = 0;
    var seed_amount: i64 = 0;
    while (try line_stmt.step()) : (created += 1) {
        const line_id = line_stmt.columnInt64(0);
        const amount = line_stmt.columnInt64(1);
        if (seed_line_id == 0) {
            seed_line_id = line_id;
            seed_amount = amount;
        }
        const open_item_id = try heft.open_item.createOpenItem(seed.database, line_id, customer_id, amount, "2026-12-31", seed.book_id, "bench");
        if (created % 3 == 0) {
            try heft.open_item.allocatePayment(seed.database, open_item_id, @divTrunc(amount, 2), "bench");
        } else if (created % 5 == 0) {
            try heft.open_item.allocatePayment(seed.database, open_item_id, amount, "bench");
        }
    }

    return .{
        .database = seed.database,
        .book_id = seed.book_id,
        .counterparty_id = customer_id,
        .seed_line_id = seed_line_id,
        .seed_amount = seed_amount,
    };
}

fn setupClassificationWorkload(entry_count: usize, counterparty_count: usize) !ClassificationWorkload {
    const seed = try setupReportSeed(entry_count, counterparty_count);

    const bs_id = try heft.classification.Classification.create(seed.database, seed.book_id, "Bench BS", "balance_sheet", "bench");
    const bs_assets = try heft.classification.ClassificationNode.addGroup(seed.database, bs_id, "Assets", null, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, bs_id, seed.cash_id, bs_assets, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, bs_id, seed.ar_id, bs_assets, 2, "bench");

    const is_id = try heft.classification.Classification.create(seed.database, seed.book_id, "Bench IS", "income_statement", "bench");
    const is_revenue = try heft.classification.ClassificationNode.addGroup(seed.database, is_id, "Revenue", null, 1, "bench");
    const is_expense = try heft.classification.ClassificationNode.addGroup(seed.database, is_id, "Expenses", null, 2, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, is_id, seed.revenue_id, is_revenue, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, is_id, seed.expense_id, is_expense, 1, "bench");

    const tb_id = try heft.classification.Classification.create(seed.database, seed.book_id, "Bench TB", "trial_balance", "bench");
    const tb_assets = try heft.classification.ClassificationNode.addGroup(seed.database, tb_id, "Assets", null, 1, "bench");
    const tb_revenue = try heft.classification.ClassificationNode.addGroup(seed.database, tb_id, "Revenue", null, 2, "bench");
    const tb_expense = try heft.classification.ClassificationNode.addGroup(seed.database, tb_id, "Expenses", null, 3, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, tb_id, seed.cash_id, tb_assets, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, tb_id, seed.ar_id, tb_assets, 2, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, tb_id, seed.revenue_id, tb_revenue, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, tb_id, seed.expense_id, tb_expense, 1, "bench");

    const cf_id = try heft.classification.Classification.create(seed.database, seed.book_id, "Bench CF", "cash_flow", "bench");
    const cf_operating = try heft.classification.ClassificationNode.addGroup(seed.database, cf_id, "Operating Activities", null, 1, "bench");
    _ = try heft.classification.ClassificationNode.addGroup(seed.database, cf_id, "Investing Activities", null, 2, "bench");
    _ = try heft.classification.ClassificationNode.addGroup(seed.database, cf_id, "Financing Activities", null, 3, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, cf_id, seed.revenue_id, cf_operating, 1, "bench");
    _ = try heft.classification.ClassificationNode.addAccount(seed.database, cf_id, seed.expense_id, cf_operating, 2, "bench");

    return .{
        .database = seed.database,
        .book_id = seed.book_id,
        .balance_sheet_id = bs_id,
        .income_statement_id = is_id,
        .trial_balance_id = tb_id,
        .cash_flow_id = cf_id,
    };
}

fn setupDimensionWorkload(entry_count: usize, counterparty_count: usize) !DimensionWorkload {
    const seed = try setupReportSeed(entry_count, counterparty_count);
    const dimension_id = try heft.dimension.Dimension.create(seed.database, seed.book_id, "Departments", .department, "bench");
    const ops_id = try heft.dimension.DimensionValue.create(seed.database, dimension_id, "OPS", "Operations", "bench");
    const sales_id = try heft.dimension.DimensionValue.createWithParent(seed.database, dimension_id, "SAL", "Sales", ops_id, "bench");
    const admin_id = try heft.dimension.DimensionValue.create(seed.database, dimension_id, "ADM", "Admin", "bench");

    var stmt = try seed.database.prepare(
        \\SELECT el.id
        \\FROM ledger_entry_lines el
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\WHERE e.book_id = ? AND e.status = 'posted'
        \\ORDER BY el.id ASC
        \\LIMIT 96;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, seed.book_id);

    var idx: usize = 0;
    while (try stmt.step()) : (idx += 1) {
        const line_id = stmt.columnInt64(0);
        const value_id = if (idx % 3 == 0)
            ops_id
        else if (idx % 3 == 1)
            sales_id
        else
            admin_id;
        try heft.dimension.LineDimension.assign(seed.database, line_id, value_id, "bench");
    }

    return .{
        .database = seed.database,
        .book_id = seed.book_id,
        .dimension_id = dimension_id,
    };
}

fn setupAbiBufferWorkload(entry_count: usize, counterparty_count: usize) !AbiBufferWorkload {
    var open_items = try setupOpenItemWorkload(entry_count, counterparty_count);
    const dimension_id = try heft.dimension.Dimension.create(open_items.database, open_items.book_id, "ABI Departments", .department, "bench");
    const north_id = try heft.dimension.DimensionValue.create(open_items.database, dimension_id, "NOR", "North", "bench");
    const south_id = try heft.dimension.DimensionValue.create(open_items.database, dimension_id, "SOU", "South", "bench");

    var stmt = try open_items.database.prepare(
        \\SELECT el.id
        \\FROM ledger_entry_lines el
        \\JOIN ledger_entries e ON e.id = el.entry_id
        \\WHERE e.book_id = ? AND e.status = 'posted'
        \\ORDER BY el.id ASC
        \\LIMIT 48;
    );
    defer stmt.finalize();
    try stmt.bindInt(1, open_items.book_id);

    var idx: usize = 0;
    while (try stmt.step()) : (idx += 1) {
        const value_id = if (idx % 2 == 0) north_id else south_id;
        try heft.dimension.LineDimension.assign(open_items.database, stmt.columnInt64(0), value_id, "bench");
    }

    return .{
        .database = open_items.database,
        .handle = .{ .sqlite = open_items.database },
        .book_id = open_items.book_id,
        .counterparty_id = open_items.counterparty_id,
        .dimension_id = dimension_id,
    };
}

fn benchBudgetVsActual(sink: *usize, workload: *BudgetWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.budget.budgetVsActual(workload.database, workload.budget_id, "2026-01-01", "2026-12-31", &buf, .json);
    sink.* +%= payload.len;
}

fn benchOpenItemsActive(sink: *usize, workload: *OpenItemWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.open_item.listOpenItems(workload.database, workload.counterparty_id, false, &buf, .json);
    sink.* +%= payload.len;
}

fn benchOpenItemsAll(sink: *usize, workload: *OpenItemWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.open_item.listOpenItems(workload.database, workload.counterparty_id, true, &buf, .json);
    sink.* +%= payload.len;
}

fn benchOpenItemAllocate(sink: *usize, workload: *OpenItemWorkload) !void {
    const open_item_id = try heft.open_item.createOpenItem(
        workload.database,
        workload.seed_line_id,
        workload.counterparty_id,
        workload.seed_amount,
        "2026-12-31",
        workload.book_id,
        "bench",
    );
    const allocation = @divTrunc(workload.seed_amount, 2);
    try heft.open_item.allocatePayment(workload.database, open_item_id, allocation, "bench");
    sink.* +%= @as(usize, @intCast(@mod(allocation, 10_000)));
}

fn benchClassifiedBalanceSheet(sink: *usize, workload: *ClassificationWorkload) !void {
    const result = try heft.classification.classifiedReport(workload.database, workload.balance_sheet_id, "2026-12-31");
    defer result.deinit();
    sink.* +%= result.rows.len;
    sink.* +%= @as(usize, @intCast(@max(result.total_debits, 0) % 10_000));
}

fn benchClassifiedIncomeStatement(sink: *usize, workload: *ClassificationWorkload) !void {
    const result = try heft.classification.classifiedReport(workload.database, workload.income_statement_id, "2026-12-31");
    defer result.deinit();
    sink.* +%= result.rows.len;
    sink.* +%= @as(usize, @intCast(@max(result.total_credits, 0) % 10_000));
}

fn benchClassifiedTrialBalance(sink: *usize, workload: *ClassificationWorkload) !void {
    const result = try heft.classification.classifiedTrialBalance(workload.database, workload.trial_balance_id, "2026-12-31");
    defer result.deinit();
    sink.* +%= result.rows.len;
    sink.* +%= @as(usize, @intCast(@max(result.unclassified_debits, 0) % 10_000));
}

fn benchCashFlowDirect(sink: *usize, workload: *ClassificationWorkload) !void {
    const result = try heft.classification.cashFlowStatement(workload.database, workload.cash_flow_id, "2026-01-01", "2026-12-31");
    defer result.deinit();
    sink.* +%= result.rows.len;
    sink.* +%= @as(usize, @intCast(@max(result.total_debits, 0) % 10_000));
}

fn benchCashFlowIndirect(sink: *usize, workload: *ClassificationWorkload) !void {
    const result = try heft.classification.cashFlowStatementIndirect(workload.database, workload.book_id, "2026-01-01", "2026-12-31", workload.cash_flow_id);
    defer result.deinit();
    sink.* +%= result.adjustments.len;
    sink.* +%= @as(usize, @intCast(@max(result.net_cash_change, 0) % 10_000));
}

fn benchDimensionSummary(sink: *usize, workload: *DimensionWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.dimension.dimensionSummary(workload.database, workload.book_id, workload.dimension_id, "2026-01-01", "2026-12-31", &buf, .json);
    sink.* +%= payload.len;
}

fn benchDimensionRollup(sink: *usize, workload: *DimensionWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.dimension.dimensionSummaryRollup(workload.database, workload.book_id, workload.dimension_id, "2026-01-01", "2026-12-31", &buf, .json);
    sink.* +%= payload.len;
}

fn benchListDimensions(sink: *usize, workload: *DimensionWorkload) !void {
    var buf: [64 * 1024]u8 = undefined;
    const payload = try heft.dimension.listDimensions(workload.database, workload.book_id, null, &buf, .json);
    sink.* +%= payload.len;
}

fn benchListDimensionValues(sink: *usize, workload: *DimensionWorkload) !void {
    var buf: [64 * 1024]u8 = undefined;
    const payload = try heft.dimension.listDimensionValues(workload.database, workload.dimension_id, &buf, .json);
    sink.* +%= payload.len;
}

fn benchAbiListEntries(sink: *usize, workload: *AbiBufferWorkload) !void {
    var buf: [512 * 1024]u8 = undefined;
    const len = abi_buffers.ledger_list_entries(&workload.handle, workload.book_id, "posted", "2026-01-01", "2026-12-31", null, 0, 500, 0, &buf, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}

fn benchAbiListAuditLog(sink: *usize, workload: *AbiBufferWorkload) !void {
    var buf: [512 * 1024]u8 = undefined;
    const len = abi_buffers.ledger_list_audit_log(&workload.handle, workload.book_id, null, null, "2026-01-01", "2026-12-31", 0, 500, 0, &buf, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}

fn benchAbiListTransactions(sink: *usize, workload: *AbiBufferWorkload) !void {
    var buf: [512 * 1024]u8 = undefined;
    const len = abi_buffers.ledger_list_transactions(&workload.handle, workload.book_id, 0, workload.counterparty_id, "2026-01-01", "2026-12-31", 0, 500, 0, &buf, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}

fn benchAbiDimensionSummary(sink: *usize, workload: *AbiBufferWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const len = abi_buffers.ledger_dimension_summary(&workload.handle, workload.book_id, workload.dimension_id, "2026-01-01", "2026-12-31", &buf, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}

fn benchAbiListOpenItems(sink: *usize, workload: *AbiBufferWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const len = abi_buffers.ledger_list_open_items(&workload.handle, workload.counterparty_id, true, &buf, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}

fn benchAbiExportJournalEntries(sink: *usize, workload: *AbiBufferWorkload) !void {
    const buf = try std.heap.c_allocator.alloc(u8, 2 * 1024 * 1024);
    defer std.heap.c_allocator.free(buf);
    const len = abi_buffers.ledger_export_journal_entries(&workload.handle, workload.book_id, "2026-01-01", "2026-12-31", buf.ptr, @intCast(buf.len), 1);
    if (len < 0) return error.InvalidInput;
    sink.* +%= @as(usize, @intCast(len));
}
