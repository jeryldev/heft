const std = @import("std");
const heft = @import("heft");
const feature_suites = @import("bench_feature_suites.zig");

const Scenario = enum {
    all,
    trial_balance,
    general_ledger,
    aged_subledger,
    statement_suite,
    comparative_suite,
    export_suite,
    budget_suite,
    open_item_suite,
    classification_suite,
    dimension_suite,
    abi_buffer_suite,
    query_scale,
    seed_report,
    seed_close,
    seed_revalue,
    close_period,
    close_direct,
    close_income_summary,
    close_allocated,
    recalculate_stale,
    post_generated_entry,
    generate_opening_entry,
    revalue,
};

const Config = struct {
    scenario: Scenario = .all,
    read_iterations: usize = 30,
    write_iterations: usize = 10,
    report_entries: usize = 2_000,
    counterparty_count: usize = 64,
    close_entries: usize = 1_000,
    revalue_entries: usize = 500,
};

const ReportWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    customer_group_id: i64,
};

const CloseWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    period_id: i64,
};

const CacheWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    period_id: i64,
};

const PostWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    period_id: i64,
    draft_entry_id: i64,
};

const OpeningWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    closed_period_id: i64,
};

const CloseMode = enum {
    direct,
    income_summary,
    allocated,
};

const RevalueWorkload = struct {
    database: heft.db.Database,
    book_id: i64,
    period_id: i64,
};

var bench_sink: usize = 0;

pub fn main() !void {
    const config = try parseArgs();
    std.debug.print(
        "Heft benchmark\nscenario={s} read_iterations={d} write_iterations={d} report_entries={d} counterparties={d} close_entries={d} revalue_entries={d}\n\n",
        .{
            @tagName(config.scenario),
            config.read_iterations,
            config.write_iterations,
            config.report_entries,
            config.counterparty_count,
            config.close_entries,
            config.revalue_entries,
        },
    );

    if (config.scenario == .all or config.scenario == .trial_balance or config.scenario == .general_ledger or config.scenario == .aged_subledger or config.scenario == .statement_suite or config.scenario == .comparative_suite or config.scenario == .export_suite) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupReportWorkload(config.report_entries, config.counterparty_count);
        const setup_ns = setup_timer.read();
        defer workload.database.close();
        try printSplitBench("report_seed", 1, setup_ns, 0);

        if (config.scenario == .all or config.scenario == .trial_balance) {
            try runReadBench("trial_balance", config.read_iterations, setup_ns, &workload, benchTrialBalance);
        }
        if (config.scenario == .all or config.scenario == .general_ledger) {
            try runReadBench("general_ledger", config.read_iterations, setup_ns, &workload, benchGeneralLedger);
        }
        if (config.scenario == .all or config.scenario == .aged_subledger) {
            try runReadBench("aged_subledger", config.read_iterations, setup_ns, &workload, benchAgedSubledger);
        }
        if (config.scenario == .all or config.scenario == .statement_suite) {
            try runStatementSuiteBench(config.read_iterations, setup_ns, &workload);
        }
        if (config.scenario == .all or config.scenario == .comparative_suite) {
            try runComparativeSuiteBench(config.read_iterations, setup_ns, &workload);
        }
        if (config.scenario == .all or config.scenario == .export_suite) {
            try runExportSuiteBench(config.read_iterations, setup_ns, &workload);
        }
        std.debug.print("\n", .{});
    }

    if (config.scenario == .all or config.scenario == .budget_suite) {
        try feature_suites.runBudgetSuiteBench(&bench_sink, config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .all or config.scenario == .open_item_suite) {
        try feature_suites.runOpenItemSuiteBench(&bench_sink, config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .all or config.scenario == .classification_suite) {
        try feature_suites.runClassificationSuiteBench(&bench_sink, config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .all or config.scenario == .dimension_suite) {
        try feature_suites.runDimensionSuiteBench(&bench_sink, config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .all or config.scenario == .abi_buffer_suite) {
        try feature_suites.runAbiBufferSuiteBench(&bench_sink, config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .query_scale) {
        try runQueryScaleBench(config.read_iterations, config.report_entries, config.counterparty_count);
        std.debug.print("\n", .{});
    }

    if (config.scenario == .seed_report) {
        try runSeedBench("seed_report", config.write_iterations, config.report_entries, config.counterparty_count, setupReportSeedOnly);
    }

    if (config.scenario == .seed_close) {
        try runSeedBench("seed_close_direct", config.write_iterations, config.close_entries, 0, setupCloseSeedDirectOnly);
        try runSeedBench("seed_close_income_summary", config.write_iterations, config.close_entries, 0, setupCloseSeedIncomeSummaryOnly);
        try runSeedBench("seed_close_allocated", config.write_iterations, config.close_entries, 0, setupCloseSeedAllocatedOnly);
    }

    if (config.scenario == .seed_revalue) {
        try runSeedBench("seed_revalue", config.write_iterations, config.revalue_entries, 0, setupRevalueSeedOnly);
    }

    if (config.scenario == .all or config.scenario == .close_period) {
        try runCloseBench("close_direct", .direct, config.write_iterations, config.close_entries);
        try runCloseBench("close_income_summary", .income_summary, config.write_iterations, config.close_entries);
        try runCloseBench("close_allocated", .allocated, config.write_iterations, config.close_entries);
    } else if (config.scenario == .close_direct) {
        try runCloseBench("close_direct", .direct, config.write_iterations, config.close_entries);
    } else if (config.scenario == .close_income_summary) {
        try runCloseBench("close_income_summary", .income_summary, config.write_iterations, config.close_entries);
    } else if (config.scenario == .close_allocated) {
        try runCloseBench("close_allocated", .allocated, config.write_iterations, config.close_entries);
    }

    if (config.scenario == .all or config.scenario == .revalue) {
        try runRevalueBench(config.write_iterations, config.revalue_entries);
    }

    if (config.scenario == .all or config.scenario == .recalculate_stale) {
        try runCacheBench(config.write_iterations, config.close_entries);
    }

    if (config.scenario == .all or config.scenario == .post_generated_entry) {
        try runPostBench(config.write_iterations, config.close_entries);
    }

    if (config.scenario == .all or config.scenario == .generate_opening_entry) {
        try runOpeningBench(config.write_iterations, config.close_entries);
    }

    std.debug.print("\nbench_sink={d}\n", .{bench_sink});
}

fn parseArgs() !Config {
    var config = Config{};
    const allocator = std.heap.page_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--scenario")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.scenario = parseScenario(args[i]) orelse return error.InvalidInput;
        } else if (std.mem.eql(u8, arg, "--read-iterations")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.read_iterations = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--write-iterations")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.write_iterations = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--report-entries")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.report_entries = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--counterparties")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.counterparty_count = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--close-entries")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.close_entries = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--revalue-entries")) {
            i += 1;
            if (i >= args.len) return error.InvalidInput;
            config.revalue_entries = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            std.process.exit(0);
        } else {
            return error.InvalidInput;
        }
    }

    return config;
}

fn parseScenario(raw: []const u8) ?Scenario {
    inline for (comptime std.meta.fields(Scenario)) |field| {
        if (std.mem.eql(u8, raw, field.name)) {
            return @field(Scenario, field.name);
        }
    }
    return null;
}

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build bench -- [options]
        \\
        \\  --scenario all|trial_balance|general_ledger|aged_subledger|statement_suite|comparative_suite|export_suite|budget_suite|open_item_suite|classification_suite|dimension_suite|abi_buffer_suite|query_scale|seed_report|seed_close|seed_revalue|close_period|close_direct|close_income_summary|close_allocated|recalculate_stale|post_generated_entry|generate_opening_entry|revalue
        \\  --read-iterations N
        \\  --write-iterations N
        \\  --report-entries N
        \\  --counterparties N
        \\  --close-entries N
        \\  --revalue-entries N
        \\
    , .{});
}

fn runReadBench(label: []const u8, iterations: usize, setup_ns: u64, workload: *ReportWorkload, comptime runner: fn (*ReportWorkload) anyerror!void) !void {
    var timer = try std.time.Timer.start();
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        try runner(workload);
    }
    try printSplitBench(label, iterations, setup_ns, timer.read());
}

fn runCloseBench(label: []const u8, mode: CloseMode, iterations: usize, entry_count: usize) !void {
    var setup_ns: u64 = 0;
    var op_ns: u64 = 0;
    var phase_totals = heft.close.ClosePeriodProfile{};
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupCloseWorkload(mode, entry_count);
        setup_ns += setup_timer.read();
        defer workload.database.close();
        var op_timer = try std.time.Timer.start();
        try benchClosePeriod(&workload, &phase_totals);
        op_ns += op_timer.read();
    }
    try printSplitBench(label, iterations, setup_ns, op_ns);
    try printCloseProfile(label, iterations, phase_totals);
}

fn runRevalueBench(iterations: usize, entry_count: usize) !void {
    var setup_ns: u64 = 0;
    var op_ns: u64 = 0;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupRevalueWorkload(entry_count);
        setup_ns += setup_timer.read();
        defer workload.database.close();
        var op_timer = try std.time.Timer.start();
        try benchRevalue(&workload);
        op_ns += op_timer.read();
    }
    try printSplitBench("revalue", iterations, setup_ns, op_ns);
}

fn runCacheBench(iterations: usize, entry_count: usize) !void {
    var setup_ns: u64 = 0;
    var elapsed_ns: u64 = 0;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupCacheWorkload(entry_count);
        setup_ns += setup_timer.read();
        defer workload.database.close();
        var timer = try std.time.Timer.start();
        try benchRecalculateStale(&workload);
        elapsed_ns += timer.read();
    }
    try printSplitBench("recalculate_stale", iterations, setup_ns, elapsed_ns);
}

fn runPostBench(iterations: usize, entry_count: usize) !void {
    var setup_ns: u64 = 0;
    var elapsed_ns: u64 = 0;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupGeneratedPostWorkload(entry_count);
        setup_ns += setup_timer.read();
        defer workload.database.close();
        var timer = try std.time.Timer.start();
        try benchPostGeneratedEntry(&workload);
        elapsed_ns += timer.read();
    }
    try printSplitBench("post_generated_entry", iterations, setup_ns, elapsed_ns);
}

fn runOpeningBench(iterations: usize, entry_count: usize) !void {
    var setup_ns: u64 = 0;
    var elapsed_ns: u64 = 0;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var setup_timer = try std.time.Timer.start();
        var workload = try setupOpeningWorkload(entry_count);
        setup_ns += setup_timer.read();
        defer workload.database.close();
        var timer = try std.time.Timer.start();
        try benchGenerateOpeningEntry(&workload);
        elapsed_ns += timer.read();
    }
    try printSplitBench("generate_opening_entry", iterations, setup_ns, elapsed_ns);
}

fn printBench(label: []const u8, iterations: usize, elapsed_ns: u64) !void {
    const avg_ns = elapsed_ns / @as(u64, @intCast(if (iterations == 0) 1 else iterations));
    const total_ms = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
    const avg_ms = @as(f64, @floatFromInt(avg_ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
    const ops_per_sec = if (elapsed_ns == 0)
        0.0
    else
        (@as(f64, @floatFromInt(iterations)) * @as(f64, @floatFromInt(std.time.ns_per_s))) / @as(f64, @floatFromInt(elapsed_ns));

    std.debug.print("{s:16} total={d:.3}ms avg={d:.3}ms ops/s={d:.2}\n", .{ label, total_ms, avg_ms, ops_per_sec });
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

fn runSeedBench(label: []const u8, iterations: usize, primary_size: usize, secondary_size: usize, comptime setup_fn: fn (usize, usize) anyerror!void) !void {
    var elapsed_ns: u64 = 0;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var timer = try std.time.Timer.start();
        try setup_fn(primary_size, secondary_size);
        elapsed_ns += timer.read();
    }
    try printBench(label, iterations, elapsed_ns);
}

fn runStatementSuiteBench(iterations: usize, setup_ns: u64, workload: *ReportWorkload) !void {
    try runReadBench("income_statement", iterations, setup_ns, workload, benchIncomeStatement);
    try runReadBench("trial_balance_movement", iterations, setup_ns, workload, benchTrialBalanceMovement);
    try runReadBench("balance_sheet_auto", iterations, setup_ns, workload, benchBalanceSheetAuto);
    try runReadBench("balance_sheet_projected_re", iterations, setup_ns, workload, benchBalanceSheetProjectedRE);
}

fn runComparativeSuiteBench(iterations: usize, setup_ns: u64, workload: *ReportWorkload) !void {
    try runReadBench("tb_comparative", iterations, setup_ns, workload, benchTrialBalanceComparative);
    try runReadBench("is_comparative", iterations, setup_ns, workload, benchIncomeStatementComparative);
    try runReadBench("bs_comparative", iterations, setup_ns, workload, benchBalanceSheetComparative);
    try runReadBench("tb_movement_comparative", iterations, setup_ns, workload, benchTrialBalanceMovementComparative);
    try runReadBench("equity_changes", iterations, setup_ns, workload, benchEquityChanges);
}

fn runExportSuiteBench(iterations: usize, setup_ns: u64, workload: *ReportWorkload) !void {
    try runReadBench("export_chart_of_accounts", iterations, setup_ns, workload, benchExportChartOfAccounts);
    try runReadBench("export_journal_entries", iterations, setup_ns, workload, benchExportJournalEntries);
    try runReadBench("export_audit_trail", iterations, setup_ns, workload, benchExportAuditTrail);
    try runReadBench("export_periods", iterations, setup_ns, workload, benchExportPeriods);
    try runReadBench("export_subledger", iterations, setup_ns, workload, benchExportSubledger);
    try runReadBench("export_book_metadata", iterations, setup_ns, workload, benchExportBookMetadata);
}

fn runQueryScaleBench(iterations: usize, base_entries: usize, counterparty_count: usize) !void {
    const scales = [_]usize{ 1, 5, 10 };
    for (scales) |scale| {
        const entry_count = base_entries * scale;
        var setup_timer = try std.time.Timer.start();
        var workload = try setupReportWorkload(entry_count, counterparty_count);
        const setup_ns = setup_timer.read();
        defer workload.database.close();

        var label_buf: [64]u8 = undefined;
        const tb_label = try std.fmt.bufPrint(&label_buf, "trial_balance_{d}", .{entry_count});
        try runReadBench(tb_label, iterations, setup_ns, &workload, benchTrialBalance);

        const gl_label = try std.fmt.bufPrint(&label_buf, "general_ledger_{d}", .{entry_count});
        try runReadBench(gl_label, iterations, setup_ns, &workload, benchGeneralLedger);

        const aged_label = try std.fmt.bufPrint(&label_buf, "aged_subledger_{d}", .{entry_count});
        try runReadBench(aged_label, iterations, setup_ns, &workload, benchAgedSubledger);
    }
}

fn setupReportWorkload(entry_count: usize, counterparty_count: usize) !ReportWorkload {
    const database = try heft.db.Database.open(":memory:");
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Bench Reports", "PHP", 2, "bench");
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

    return .{ .database = database, .book_id = book_id, .customer_group_id = customer_group_id };
}

fn setupReportSeedOnly(entry_count: usize, counterparty_count: usize) !void {
    var workload = try setupReportWorkload(entry_count, counterparty_count);
    workload.database.close();
}

fn benchTrialBalance(workload: *ReportWorkload) !void {
    const result = try heft.report.trialBalance(workload.database, workload.book_id, "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_debits, 10_000)));
}

fn benchGeneralLedger(workload: *ReportWorkload) !void {
    const result = try heft.report.generalLedger(workload.database, workload.book_id, "2026-01-01", "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_debits, 10_000)));
}

fn benchAgedSubledger(workload: *ReportWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.query_mod.agedSubledger(workload.database, workload.book_id, workload.customer_group_id, "2026-12-31", .asc, 500, 0, &buf, .json);
    bench_sink +%= payload.len;
}

fn benchIncomeStatement(workload: *ReportWorkload) !void {
    const result = try heft.report.incomeStatement(workload.database, workload.book_id, "2026-01-01", "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_credits, 10_000)));
}

fn benchTrialBalanceMovement(workload: *ReportWorkload) !void {
    const result = try heft.report.trialBalanceMovement(workload.database, workload.book_id, "2026-01-01", "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_debits, 10_000)));
}

fn benchBalanceSheetAuto(workload: *ReportWorkload) !void {
    const result = try heft.report.balanceSheetAuto(workload.database, workload.book_id, "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_credits, 10_000)));
}

fn benchBalanceSheetProjectedRE(workload: *ReportWorkload) !void {
    const result = try heft.report.balanceSheetAutoWithProjectedRE(workload.database, workload.book_id, "2026-12-31");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_credits, 10_000)));
}

fn benchTrialBalanceComparative(workload: *ReportWorkload) !void {
    const result = try heft.report.trialBalanceComparative(workload.database, workload.book_id, "2026-12-31", "2026-06-30");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.current_total_debits, 10_000)));
}

fn benchIncomeStatementComparative(workload: *ReportWorkload) !void {
    const result = try heft.report.incomeStatementComparative(workload.database, workload.book_id, "2026-07-01", "2026-12-31", "2026-01-01", "2026-06-30");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.current_total_credits, 10_000)));
}

fn benchBalanceSheetComparative(workload: *ReportWorkload) !void {
    const result = try heft.report.balanceSheetComparative(workload.database, workload.book_id, "2026-12-31", "2026-06-30", "2026-01-01");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.current_total_debits, 10_000)));
}

fn benchTrialBalanceMovementComparative(workload: *ReportWorkload) !void {
    const result = try heft.report.trialBalanceMovementComparative(workload.database, workload.book_id, "2026-07-01", "2026-12-31", "2026-01-01", "2026-06-30");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.current_total_debits, 10_000)));
}

fn benchEquityChanges(workload: *ReportWorkload) !void {
    const result = try heft.report.equityChanges(workload.database, workload.book_id, "2026-01-01", "2026-12-31", "2026-01-01");
    defer result.deinit();
    bench_sink +%= result.rows.len;
    bench_sink +%= @as(usize, @intCast(@mod(result.total_closing, 10_000)));
}

fn benchExportChartOfAccounts(workload: *ReportWorkload) !void {
    var buf: [256 * 1024]u8 = undefined;
    const payload = try heft.export_mod.exportChartOfAccounts(workload.database, workload.book_id, &buf, .json);
    bench_sink +%= payload.len;
}

fn benchExportJournalEntries(workload: *ReportWorkload) !void {
    const buf = try std.heap.c_allocator.alloc(u8, 2 * 1024 * 1024);
    defer std.heap.c_allocator.free(buf);
    const payload = try heft.export_mod.exportJournalEntries(workload.database, workload.book_id, "2026-01-01", "2026-12-31", buf, .json);
    bench_sink +%= payload.len;
}

fn benchExportAuditTrail(workload: *ReportWorkload) !void {
    const buf = try std.heap.c_allocator.alloc(u8, 2 * 1024 * 1024);
    defer std.heap.c_allocator.free(buf);
    const payload = try heft.export_mod.exportAuditTrail(workload.database, workload.book_id, "2026-01-01", "2026-12-31", buf, .json);
    bench_sink +%= payload.len;
}

fn benchExportPeriods(workload: *ReportWorkload) !void {
    var buf: [128 * 1024]u8 = undefined;
    const payload = try heft.export_mod.exportPeriods(workload.database, workload.book_id, &buf, .json);
    bench_sink +%= payload.len;
}

fn benchExportSubledger(workload: *ReportWorkload) !void {
    var buf: [512 * 1024]u8 = undefined;
    const payload = try heft.export_mod.exportSubledger(workload.database, workload.book_id, &buf, .json);
    bench_sink +%= payload.len;
}

fn benchExportBookMetadata(workload: *ReportWorkload) !void {
    var buf: [64 * 1024]u8 = undefined;
    const payload = try heft.export_mod.exportBookMetadata(workload.database, workload.book_id, &buf, .json);
    bench_sink +%= payload.len;
}

fn setupCloseWorkload(mode: CloseMode, entry_count: usize) !CloseWorkload {
    const database = try heft.db.Database.open(":memory:");
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Bench Close", "PHP", 2, "bench");
    const cash_id = try heft.account.Account.create(database, book_id, "1000", "Cash", .asset, false, "bench");
    const revenue_id = try heft.account.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "bench");
    const expense_id = try heft.account.Account.create(database, book_id, "5000", "Expense", .expense, false, "bench");
    const retained_earnings_id = try heft.account.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "bench");
    try heft.book.Book.setRetainedEarningsAccount(database, book_id, retained_earnings_id, "bench");

    switch (mode) {
        .direct => {},
        .income_summary => {
            const income_summary_id = try heft.account.Account.create(database, book_id, "3200", "Income Summary", .equity, false, "bench");
            try heft.book.Book.setIncomeSummaryAccount(database, book_id, income_summary_id, "bench");
        },
        .allocated => {
            try heft.book.Book.setEntityType(database, book_id, .partnership, "bench");
            const partner_a = try heft.account.Account.create(database, book_id, "3300", "Partner A Capital", .equity, false, "bench");
            const partner_b = try heft.account.Account.create(database, book_id, "3310", "Partner B Capital", .equity, false, "bench");
            const partner_c = try heft.account.Account.create(database, book_id, "3320", "Partner C Capital", .equity, false, "bench");
            _ = try heft.book.Book.addEquityAllocation(database, book_id, partner_a, "Partner A", 5000, "2026-01-01", "bench");
            _ = try heft.book.Book.addEquityAllocation(database, book_id, partner_b, "Partner B", 3000, "2026-01-01", "bench");
            _ = try heft.book.Book.addEquityAllocation(database, book_id, partner_c, "Partner C", 2000, "2026-01-01", "bench");
        },
    }

    const jan_id = try heft.period.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "bench");
    _ = try heft.period.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "bench");

    var idx: usize = 0;
    while (idx < entry_count) : (idx += 1) {
        const amount = @as(i64, @intCast(200 + (idx % 17))) * heft.money.AMOUNT_SCALE;

        var rev_doc_buf: [32]u8 = undefined;
        const rev_doc = try std.fmt.bufPrint(&rev_doc_buf, "REV-{d:0>6}", .{idx + 1});
        const revenue_entry = try heft.entry.Entry.createDraft(database, book_id, rev_doc, "2026-01-15", "2026-01-15", null, jan_id, null, "bench");
        _ = try heft.entry.Entry.addLine(database, revenue_entry, 1, amount, 0, "PHP", heft.money.FX_RATE_SCALE, cash_id, null, null, "bench");
        _ = try heft.entry.Entry.addLine(database, revenue_entry, 2, 0, amount, "PHP", heft.money.FX_RATE_SCALE, revenue_id, null, null, "bench");
        try heft.entry.Entry.post(database, revenue_entry, "bench");

        var exp_doc_buf: [32]u8 = undefined;
        const exp_doc = try std.fmt.bufPrint(&exp_doc_buf, "CST-{d:0>6}", .{idx + 1});
        const expense_amount = @divTrunc(amount, 2);
        const expense_entry = try heft.entry.Entry.createDraft(database, book_id, exp_doc, "2026-01-20", "2026-01-20", null, jan_id, null, "bench");
        _ = try heft.entry.Entry.addLine(database, expense_entry, 1, expense_amount, 0, "PHP", heft.money.FX_RATE_SCALE, expense_id, null, null, "bench");
        _ = try heft.entry.Entry.addLine(database, expense_entry, 2, 0, expense_amount, "PHP", heft.money.FX_RATE_SCALE, cash_id, null, null, "bench");
        try heft.entry.Entry.post(database, expense_entry, "bench");
    }

    return .{ .database = database, .book_id = book_id, .period_id = jan_id };
}

fn setupCloseSeedDirectOnly(entry_count: usize, _: usize) !void {
    var workload = try setupCloseWorkload(.direct, entry_count);
    workload.database.close();
}

fn setupCloseSeedIncomeSummaryOnly(entry_count: usize, _: usize) !void {
    var workload = try setupCloseWorkload(.income_summary, entry_count);
    workload.database.close();
}

fn setupCloseSeedAllocatedOnly(entry_count: usize, _: usize) !void {
    var workload = try setupCloseWorkload(.allocated, entry_count);
    workload.database.close();
}

fn benchClosePeriod(workload: *CloseWorkload, phase_totals: *heft.close.ClosePeriodProfile) !void {
    var profile = heft.close.ClosePeriodProfile{};
    try heft.close.closePeriodProfiled(workload.database, workload.book_id, workload.period_id, "bench", &profile);
    phase_totals.preflight_ns += profile.preflight_ns;
    phase_totals.recalculate_stale_ns += profile.recalculate_stale_ns;
    phase_totals.load_accounts_ns += profile.load_accounts_ns;
    phase_totals.close_entries_ns += profile.close_entries_ns;
    phase_totals.opening_entry_ns += profile.opening_entry_ns;
    phase_totals.transitions_ns += profile.transitions_ns;

    var stmt = try workload.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND entry_type = 'closing';");
    defer stmt.finalize();
    try stmt.bindInt(1, workload.book_id);
    _ = try stmt.step();
    bench_sink +%= @as(usize, @intCast(stmt.columnInt(0)));
}

fn printCloseProfile(label: []const u8, iterations: usize, profile: heft.close.ClosePeriodProfile) !void {
    const count = @as(u64, @intCast(if (iterations == 0) 1 else iterations));
    std.debug.print(
        "  {s}_phases preflight={d:.3}ms stale={d:.3}ms load={d:.3}ms close={d:.3}ms opening={d:.3}ms transitions={d:.3}ms\n",
        .{
            label,
            @as(f64, @floatFromInt(profile.preflight_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
            @as(f64, @floatFromInt(profile.recalculate_stale_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
            @as(f64, @floatFromInt(profile.load_accounts_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
            @as(f64, @floatFromInt(profile.close_entries_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
            @as(f64, @floatFromInt(profile.opening_entry_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
            @as(f64, @floatFromInt(profile.transitions_ns / count)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
        },
    );
}

fn setupCacheWorkload(entry_count: usize) !CacheWorkload {
    const workload = try setupCloseWorkload(.direct, entry_count);
    try heft.close.closePeriod(workload.database, workload.book_id, workload.period_id, "bench");

    var stale_stmt = try workload.database.prepare(
        \\UPDATE ledger_account_balances
        \\SET is_stale = 1, stale_since = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        \\WHERE book_id = ? AND period_id = ?;
    );
    defer stale_stmt.finalize();
    try stale_stmt.bindInt(1, workload.book_id);
    try stale_stmt.bindInt(2, workload.period_id);
    _ = try stale_stmt.step();

    return .{
        .database = workload.database,
        .book_id = workload.book_id,
        .period_id = workload.period_id,
    };
}

fn benchRecalculateStale(workload: *CacheWorkload) !void {
    const fixed = try heft.cache.recalculateStale(workload.database, workload.book_id, &.{workload.period_id});
    bench_sink +%= fixed;
}

fn setupGeneratedPostWorkload(entry_count: usize) !PostWorkload {
    const database = try heft.db.Database.open(":memory:");
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Bench Post", "PHP", 2, "bench");
    const retained_earnings_id = try heft.account.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "bench");
    try heft.book.Book.setRetainedEarningsAccount(database, book_id, retained_earnings_id, "bench");

    const jan_id = try heft.period.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "bench");

    const revenue_count: usize = @max(entry_count / 2, 1);
    const expense_count: usize = @max(entry_count - revenue_count, 1);
    const total_accounts = revenue_count + expense_count;
    const account_ids = try std.heap.page_allocator.alloc(i64, total_accounts);
    defer std.heap.page_allocator.free(account_ids);

    var idx: usize = 0;
    while (idx < revenue_count) : (idx += 1) {
        var num_buf: [16]u8 = undefined;
        var name_buf: [32]u8 = undefined;
        const number = try std.fmt.bufPrint(&num_buf, "4{d:0>3}", .{idx});
        const name = try std.fmt.bufPrint(&name_buf, "Revenue {d}", .{idx + 1});
        account_ids[idx] = try heft.account.Account.create(database, book_id, number, name, .revenue, false, "bench");
    }
    idx = 0;
    while (idx < expense_count) : (idx += 1) {
        const slot = revenue_count + idx;
        var num_buf: [16]u8 = undefined;
        var name_buf: [32]u8 = undefined;
        const number = try std.fmt.bufPrint(&num_buf, "5{d:0>3}", .{idx});
        const name = try std.fmt.bufPrint(&name_buf, "Expense {d}", .{idx + 1});
        account_ids[slot] = try heft.account.Account.create(database, book_id, number, name, .expense, false, "bench");
    }

    const entry_id = try heft.entry.Entry.createDraftAs(database, book_id, "BENCH-CLOSE", "2026-01-31", "2026-01-31", null, jan_id, "{\"closing_entry\":true,\"method\":\"bench\"}", .closing, "bench");
    var appender = try heft.entry.Entry.LineAppender.init(database, entry_id, book_id, "bench");
    defer appender.deinit();

    var line_num: i32 = 1;
    var re_debit_total: i64 = 0;
    var re_credit_total: i64 = 0;

    idx = 0;
    while (idx < revenue_count) : (idx += 1) {
        const amount = @as(i64, @intCast(200 + (idx % 17))) * heft.money.AMOUNT_SCALE;
        _ = try appender.add(line_num, amount, 0, "PHP", heft.money.FX_RATE_SCALE, account_ids[idx], null, null);
        line_num += 1;
        re_credit_total = std.math.add(i64, re_credit_total, amount) catch return error.AmountOverflow;
    }

    idx = 0;
    while (idx < expense_count) : (idx += 1) {
        const amount = @as(i64, @intCast(100 + (idx % 13))) * heft.money.AMOUNT_SCALE;
        _ = try appender.add(line_num, 0, amount, "PHP", heft.money.FX_RATE_SCALE, account_ids[revenue_count + idx], null, null);
        line_num += 1;
        re_debit_total = std.math.add(i64, re_debit_total, amount) catch return error.AmountOverflow;
    }

    if (re_debit_total > 0) {
        _ = try appender.add(line_num, re_debit_total, 0, "PHP", heft.money.FX_RATE_SCALE, retained_earnings_id, null, null);
        line_num += 1;
    }
    if (re_credit_total > 0) {
        _ = try appender.add(line_num, 0, re_credit_total, "PHP", heft.money.FX_RATE_SCALE, retained_earnings_id, null, null);
    }

    return .{
        .database = database,
        .book_id = book_id,
        .period_id = jan_id,
        .draft_entry_id = entry_id,
    };
}

fn benchPostGeneratedEntry(workload: *PostWorkload) !void {
    try heft.entry.Entry.post(workload.database, workload.draft_entry_id, "bench");

    var stmt = try workload.database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
    defer stmt.finalize();
    try stmt.bindInt(1, workload.draft_entry_id);
    _ = try stmt.step();
    if (stmt.columnText(0)) |status| bench_sink +%= status.len;
}

fn setupOpeningWorkload(entry_count: usize) !OpeningWorkload {
    const workload = try setupCloseWorkload(.direct, entry_count);
    try heft.close.closePeriod(workload.database, workload.book_id, workload.period_id, "bench");

    var void_stmt = try workload.database.prepare(
        \\UPDATE ledger_entries
        \\SET status = 'void', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        \\WHERE book_id = ? AND period_id = (
        \\  SELECT id FROM ledger_periods
        \\  WHERE book_id = ? AND start_date > '2026-01-31'
        \\  ORDER BY start_date ASC LIMIT 1
        \\) AND entry_type = 'opening';
    );
    defer void_stmt.finalize();
    try void_stmt.bindInt(1, workload.book_id);
    try void_stmt.bindInt(2, workload.book_id);
    _ = try void_stmt.step();

    return .{
        .database = workload.database,
        .book_id = workload.book_id,
        .closed_period_id = workload.period_id,
    };
}

fn benchGenerateOpeningEntry(workload: *OpeningWorkload) !void {
    try heft.close.generateOpeningEntry(workload.database, workload.book_id, workload.closed_period_id, "2026-01-31", "PHP", "bench");

    var stmt = try workload.database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE book_id = ? AND entry_type = 'opening' AND status = 'posted';");
    defer stmt.finalize();
    try stmt.bindInt(1, workload.book_id);
    _ = try stmt.step();
    bench_sink +%= @as(usize, @intCast(stmt.columnInt(0)));
}

fn setupRevalueWorkload(entry_count: usize) !RevalueWorkload {
    const database = try heft.db.Database.open(":memory:");
    try heft.schema.createAll(database);

    const book_id = try heft.book.Book.create(database, "Bench FX", "PHP", 2, "bench");
    const cash_php = try heft.account.Account.create(database, book_id, "1000", "Cash PHP", .asset, false, "bench");
    const cash_usd = try heft.account.Account.create(database, book_id, "1010", "Cash USD", .asset, false, "bench");
    const fx_gl = try heft.account.Account.create(database, book_id, "7000", "FX Gain Loss", .expense, false, "bench");
    try heft.book.Book.setFxGainLossAccount(database, book_id, fx_gl, "bench");

    const jan_id = try heft.period.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "bench");
    _ = try heft.period.Period.create(database, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "bench");

    var idx: usize = 0;
    while (idx < entry_count) : (idx += 1) {
        const usd_amount = @as(i64, @intCast(50 + (idx % 20))) * heft.money.AMOUNT_SCALE;
        const php_amount = try heft.money.computeBaseAmount(usd_amount, 565_000_000_000);

        var doc_buf: [32]u8 = undefined;
        const doc = try std.fmt.bufPrint(&doc_buf, "FX-{d:0>6}", .{idx + 1});
        const entry_id = try heft.entry.Entry.createDraft(database, book_id, doc, "2026-01-15", "2026-01-15", null, jan_id, null, "bench");
        _ = try heft.entry.Entry.addLine(database, entry_id, 1, usd_amount, 0, "USD", 565_000_000_000, cash_usd, null, null, "bench");
        _ = try heft.entry.Entry.addLine(database, entry_id, 2, 0, php_amount, "PHP", heft.money.FX_RATE_SCALE, cash_php, null, null, "bench");
        try heft.entry.Entry.post(database, entry_id, "bench");
    }

    return .{ .database = database, .book_id = book_id, .period_id = jan_id };
}

fn setupRevalueSeedOnly(entry_count: usize, _: usize) !void {
    var workload = try setupRevalueWorkload(entry_count);
    workload.database.close();
}

fn benchRevalue(workload: *RevalueWorkload) !void {
    const rates = [_]heft.revaluation.CurrencyRate{.{ .currency = "USD", .new_rate = 572_500_000_000 }};
    const result = try heft.revaluation.revalueForexBalances(workload.database, workload.book_id, workload.period_id, &rates, "bench");
    bench_sink +%= @as(usize, @intCast(@max(result.entry_id, 0)));
    bench_sink +%= @as(usize, @intCast(@max(result.reversal_id, 0)));
}
