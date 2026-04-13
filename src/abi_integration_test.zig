const std = @import("std");
const heft = @import("heft");
const abi = @import("main.zig");

const VERSION = heft.version.VERSION;
const SCHEMA_VERSION = heft.schema.SCHEMA_VERSION;
const ledger_open = abi.ledger_open;
const ledger_close = abi.ledger_close;
const ledger_version = abi.ledger_version;
const ledger_create_book = abi.ledger_create_book;
const ledger_create_account = abi.ledger_create_account;
const ledger_create_period = abi.ledger_create_period;
const ledger_update_account_status = abi.ledger_update_account_status;
const ledger_transition_period = abi.ledger_transition_period;
const ledger_set_rounding_account = abi.ledger_set_rounding_account;
const ledger_set_fx_gain_loss_account = abi.ledger_set_fx_gain_loss_account;
const ledger_set_retained_earnings_account = abi.ledger_set_retained_earnings_account;
const ledger_set_equity_close_target = abi.ledger_set_equity_close_target;
const ledger_set_dividends_drawings_account = abi.ledger_set_dividends_drawings_account;
const ledger_set_current_year_earnings_account = abi.ledger_set_current_year_earnings_account;
const ledger_set_income_summary_account = abi.ledger_set_income_summary_account;
const ledger_set_opening_balance_account = abi.ledger_set_opening_balance_account;
const ledger_set_suspense_account = abi.ledger_set_suspense_account;
const ledger_validate_opening_balance = abi.ledger_validate_opening_balance;
const ledger_bulk_create_periods = abi.ledger_bulk_create_periods;
const ledger_create_draft = abi.ledger_create_draft;
const ledger_add_line = abi.ledger_add_line;
const ledger_edit_draft = abi.ledger_edit_draft;
const ledger_edit_posted = abi.ledger_edit_posted;
const ledger_post_entry = abi.ledger_post_entry;
const ledger_void_entry = abi.ledger_void_entry;
const ledger_reverse_entry = abi.ledger_reverse_entry;
const ledger_remove_line = abi.ledger_remove_line;
const ledger_delete_draft = abi.ledger_delete_draft;
const ledger_edit_line = abi.ledger_edit_line;
const ledger_trial_balance = abi.ledger_trial_balance;
const ledger_income_statement = abi.ledger_income_statement;
const ledger_trial_balance_movement = abi.ledger_trial_balance_movement;
const ledger_balance_sheet = abi.ledger_balance_sheet;
const ledger_balance_sheet_auto = abi.ledger_balance_sheet_auto;
const ledger_set_fy_start_month = abi.ledger_set_fy_start_month;
const ledger_set_entity_type = abi.ledger_set_entity_type;
const ledger_translate_report = abi.ledger_translate_report;
const ledger_general_ledger = abi.ledger_general_ledger;
const ledger_account_ledger = abi.ledger_account_ledger;
const ledger_journal_register = abi.ledger_journal_register;
const ledger_free_ledger_result = abi.ledger_free_ledger_result;
const ledger_free_result = abi.ledger_free_result;
const ledger_trial_balance_comparative = abi.ledger_trial_balance_comparative;
const ledger_income_statement_comparative = abi.ledger_income_statement_comparative;
const ledger_balance_sheet_comparative = abi.ledger_balance_sheet_comparative;
const ledger_trial_balance_movement_comparative = abi.ledger_trial_balance_movement_comparative;
const ledger_free_comparative_result = abi.ledger_free_comparative_result;
const ledger_equity_changes = abi.ledger_equity_changes;
const ledger_free_equity_result = abi.ledger_free_equity_result;
const ledger_result_row_count = abi.ledger_result_row_count;
const ledger_result_total_debits = abi.ledger_result_total_debits;
const ledger_result_total_credits = abi.ledger_result_total_credits;
const ledger_create_subledger_group = abi.ledger_create_subledger_group;
const ledger_create_subledger_account = abi.ledger_create_subledger_account;
const ledger_create_classification = abi.ledger_create_classification;
const ledger_add_group_node = abi.ledger_add_group_node;
const ledger_add_account_node = abi.ledger_add_account_node;
const ledger_move_node = abi.ledger_move_node;
const ledger_classified_report = abi.ledger_classified_report;
const ledger_cash_flow_statement = abi.ledger_cash_flow_statement;
const ledger_free_classified_result = abi.ledger_free_classified_result;
const ledger_delete_classification = abi.ledger_delete_classification;
const ledger_verify = abi.ledger_verify;
const ledger_archive_book = abi.ledger_archive_book;
const ledger_close_period = abi.ledger_close_period;
const ledger_revalue_forex_balances = abi.ledger_revalue_forex_balances;
const ledger_last_error = abi.ledger_last_error;
const ledger_update_book_name = abi.ledger_update_book_name;
const ledger_update_account_name = abi.ledger_update_account_name;
const ledger_set_account_parent = abi.ledger_set_account_parent;
const ledger_set_account_monetary = abi.ledger_set_account_monetary;
const ledger_update_classification_name = abi.ledger_update_classification_name;
const ledger_update_node_label = abi.ledger_update_node_label;
const ledger_delete_node = abi.ledger_delete_node;
const ledger_update_subledger_group_name = abi.ledger_update_subledger_group_name;
const ledger_delete_subledger_group = abi.ledger_delete_subledger_group;
const ledger_update_subledger_account_name = abi.ledger_update_subledger_account_name;
const ledger_delete_subledger_account = abi.ledger_delete_subledger_account;
const ledger_update_subledger_account_status = abi.ledger_update_subledger_account_status;
const ledger_get_book = abi.ledger_get_book;
const ledger_list_books = abi.ledger_list_books;
const ledger_get_account = abi.ledger_get_account;
const ledger_list_accounts = abi.ledger_list_accounts;
const ledger_get_period = abi.ledger_get_period;
const ledger_list_periods = abi.ledger_list_periods;
const ledger_get_entry = abi.ledger_get_entry;
const ledger_list_entries = abi.ledger_list_entries;
const ledger_list_entry_lines = abi.ledger_list_entry_lines;
const ledger_list_classifications = abi.ledger_list_classifications;
const ledger_list_subledger_groups = abi.ledger_list_subledger_groups;
const ledger_list_subledger_accounts = abi.ledger_list_subledger_accounts;
const ledger_list_audit_log = abi.ledger_list_audit_log;
const ledger_subledger_report = abi.ledger_subledger_report;
const ledger_counterparty_ledger = abi.ledger_counterparty_ledger;
const ledger_list_transactions = abi.ledger_list_transactions;
const ledger_subledger_reconciliation = abi.ledger_subledger_reconciliation;
const ledger_aged_subledger = abi.ledger_aged_subledger;
const ledger_edit_line_full = abi.ledger_edit_line_full;
const ledger_create_dimension = abi.ledger_create_dimension;
const ledger_delete_dimension = abi.ledger_delete_dimension;
const ledger_create_dimension_value = abi.ledger_create_dimension_value;
const ledger_delete_dimension_value = abi.ledger_delete_dimension_value;
const ledger_assign_line_dimension = abi.ledger_assign_line_dimension;
const ledger_remove_line_dimension = abi.ledger_remove_line_dimension;
const ledger_dimension_summary = abi.ledger_dimension_summary;
const ledger_list_dimensions = abi.ledger_list_dimensions;
const ledger_list_dimension_values = abi.ledger_list_dimension_values;
const ledger_describe_schema = abi.ledger_describe_schema;
const ledger_approve_entry = abi.ledger_approve_entry;
const ledger_reject_entry = abi.ledger_reject_entry;
const ledger_set_require_approval = abi.ledger_set_require_approval;
const ledger_create_budget = abi.ledger_create_budget;
const ledger_delete_budget = abi.ledger_delete_budget;
const ledger_set_budget_line = abi.ledger_set_budget_line;
const ledger_budget_vs_actual = abi.ledger_budget_vs_actual;
const ledger_batch_post = abi.ledger_batch_post;
const ledger_batch_void = abi.ledger_batch_void;
const ledger_recalculate_balances = abi.ledger_recalculate_balances;
const ledger_export_chart_of_accounts = abi.ledger_export_chart_of_accounts;
const ledger_export_journal_entries = abi.ledger_export_journal_entries;
const ledger_export_audit_trail = abi.ledger_export_audit_trail;
const ledger_export_periods = abi.ledger_export_periods;
const ledger_export_subledger = abi.ledger_export_subledger;
const ledger_export_book_metadata = abi.ledger_export_book_metadata;
const ledger_transition_budget = abi.ledger_transition_budget;
const ledger_create_open_item = abi.ledger_create_open_item;
const ledger_allocate_payment = abi.ledger_allocate_payment;
const ledger_list_open_items = abi.ledger_list_open_items;
const ledger_cash_flow_indirect = abi.ledger_cash_flow_indirect;
const ledger_free_cash_flow_indirect = abi.ledger_free_cash_flow_indirect;
const ledger_classified_trial_balance = abi.ledger_classified_trial_balance;
const ledger_dimension_summary_rollup = abi.ledger_dimension_summary_rollup;

fn cleanupTestFile(name: [*:0]const u8) void {
    const cwd = std.fs.cwd();
    const base = std.mem.span(name);
    cwd.deleteFile(base) catch {};
    // WAL mode creates -wal and -shm sidecar files
    var wal_buf: [256]u8 = undefined;
    var shm_buf: [256]u8 = undefined;
    const wal_name = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{base}) catch return;
    const shm_name = std.fmt.bufPrint(&shm_buf, "{s}-shm", .{base}) catch return;
    cwd.deleteFile(wal_name) catch {};
    cwd.deleteFile(shm_name) catch {};
}

const AbiFeatureScenario = struct {
    book_id: i64,
    dec_2025_id: i64,
    jan_2026_id: i64,
    feb_2026_id: i64,
    cash_id: i64,
    ar_id: i64,
    capital_id: i64,
    retained_earnings_id: i64,
    income_summary_id: i64,
    opening_balance_id: i64,
    revenue_id: i64,
    customer_group_id: i64,
    customer_id: i64,
    cash_flow_classification_id: i64,
    trial_balance_classification_id: i64,
    dimension_id: i64,
    dimension_value_id: i64,
    invoice_entry_id: i64,
    invoice_line_id: i64,
};

fn setupAbiFeatureScenario(handle: *abi.LedgerDB) !AbiFeatureScenario {
    const book_id = ledger_create_book(handle, "Feature Test", "PHP", 2, "admin");
    try std.testing.expect(book_id > 0);

    const cash_id = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    const ar_id = ledger_create_account(handle, book_id, "1100", "Accounts Receivable", "asset", 0, "admin");
    const capital_id = ledger_create_account(handle, book_id, "3000", "Capital", "equity", 0, "admin");
    const retained_earnings_id = ledger_create_account(handle, book_id, "3100", "Retained Earnings", "equity", 0, "admin");
    const income_summary_id = ledger_create_account(handle, book_id, "3200", "Income Summary", "equity", 0, "admin");
    const opening_balance_id = ledger_create_account(handle, book_id, "3300", "Opening Balance Equity", "equity", 0, "admin");
    const revenue_id = ledger_create_account(handle, book_id, "4000", "Revenue", "revenue", 0, "admin");
    try std.testing.expect(cash_id > 0 and ar_id > 0 and capital_id > 0 and retained_earnings_id > 0 and income_summary_id > 0 and opening_balance_id > 0 and revenue_id > 0);

    try std.testing.expect(ledger_set_retained_earnings_account(handle, book_id, retained_earnings_id, "admin"));
    try std.testing.expect(ledger_set_income_summary_account(handle, book_id, income_summary_id, "admin"));
    try std.testing.expect(ledger_set_opening_balance_account(handle, book_id, opening_balance_id, "admin"));
    try std.testing.expect(ledger_validate_opening_balance(handle, book_id));

    const dec_2025_id = ledger_create_period(handle, book_id, "Dec 2025", 12, 2025, "2025-12-01", "2025-12-31", "regular", "admin");
    const jan_2026_id = ledger_create_period(handle, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const feb_2026_id = ledger_create_period(handle, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    try std.testing.expect(dec_2025_id > 0 and jan_2026_id > 0 and feb_2026_id > 0);

    const customer_group_id = ledger_create_subledger_group(handle, book_id, "AR Customers", "customer", 1, ar_id, "admin");
    const customer_id = ledger_create_subledger_account(handle, book_id, "C001", "Customer ABC", "customer", customer_group_id, "admin");
    try std.testing.expect(customer_group_id > 0 and customer_id > 0);

    const trial_balance_classification_id = ledger_create_classification(handle, book_id, "Trial Balance", "trial_balance", "admin");
    const tb_assets = ledger_add_group_node(handle, trial_balance_classification_id, "Assets", 0, 1, "admin");
    const tb_equity = ledger_add_group_node(handle, trial_balance_classification_id, "Equity", 0, 2, "admin");
    const tb_revenue = ledger_add_group_node(handle, trial_balance_classification_id, "Revenue", 0, 3, "admin");
    _ = ledger_add_account_node(handle, trial_balance_classification_id, cash_id, tb_assets, 1, "admin");
    _ = ledger_add_account_node(handle, trial_balance_classification_id, ar_id, tb_assets, 2, "admin");
    _ = ledger_add_account_node(handle, trial_balance_classification_id, capital_id, tb_equity, 1, "admin");
    _ = ledger_add_account_node(handle, trial_balance_classification_id, retained_earnings_id, tb_equity, 2, "admin");
    _ = ledger_add_account_node(handle, trial_balance_classification_id, revenue_id, tb_revenue, 1, "admin");

    const cash_flow_classification_id = ledger_create_classification(handle, book_id, "Cash Flow", "cash_flow", "admin");
    const cf_operating = ledger_add_group_node(handle, cash_flow_classification_id, "Operating Activities", 0, 1, "admin");
    _ = ledger_add_account_node(handle, cash_flow_classification_id, cash_id, cf_operating, 1, "admin");

    const dimension_id = ledger_create_dimension(handle, book_id, "VAT", "tax_code", "admin");
    const dimension_value_id = ledger_create_dimension_value(handle, dimension_id, "VAT12", "VAT 12%", "admin");
    try std.testing.expect(dimension_id > 0 and dimension_value_id > 0);

    const opening_entry_id = ledger_create_draft(handle, book_id, "OB-2025-001", "2025-12-01", "2025-12-01", "Opening balance", dec_2025_id, null, "admin");
    _ = ledger_add_line(handle, opening_entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, 0, null, "admin");
    _ = ledger_add_line(handle, opening_entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, capital_id, 0, null, "admin");
    try std.testing.expect(ledger_post_entry(handle, opening_entry_id, "admin"));

    const invoice_entry_id = ledger_create_draft(handle, book_id, "INV-2026-001", "2026-01-10", "2026-01-10", "Invoice", jan_2026_id, null, "admin");
    const invoice_line_id = ledger_add_line(handle, invoice_entry_id, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, ar_id, customer_id, "Receivable", "admin");
    _ = ledger_add_line(handle, invoice_entry_id, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, revenue_id, 0, "Revenue", "admin");
    try std.testing.expect(invoice_line_id > 0);
    try std.testing.expect(ledger_post_entry(handle, invoice_entry_id, "admin"));
    try std.testing.expect(ledger_assign_line_dimension(handle, invoice_line_id, dimension_value_id, "admin"));

    const payment_entry_id = ledger_create_draft(handle, book_id, "RCPT-2026-001", "2026-01-20", "2026-01-20", "Payment", jan_2026_id, null, "admin");
    _ = ledger_add_line(handle, payment_entry_id, 1, 2_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, 0, "Cash receipt", "admin");
    _ = ledger_add_line(handle, payment_entry_id, 2, 0, 2_000_000_000_00, "PHP", 10_000_000_000, ar_id, customer_id, "AR settlement", "admin");
    try std.testing.expect(ledger_post_entry(handle, payment_entry_id, "admin"));

    return .{
        .book_id = book_id,
        .dec_2025_id = dec_2025_id,
        .jan_2026_id = jan_2026_id,
        .feb_2026_id = feb_2026_id,
        .cash_id = cash_id,
        .ar_id = ar_id,
        .capital_id = capital_id,
        .retained_earnings_id = retained_earnings_id,
        .income_summary_id = income_summary_id,
        .opening_balance_id = opening_balance_id,
        .revenue_id = revenue_id,
        .customer_group_id = customer_group_id,
        .customer_id = customer_id,
        .cash_flow_classification_id = cash_flow_classification_id,
        .trial_balance_classification_id = trial_balance_classification_id,
        .dimension_id = dimension_id,
        .dimension_value_id = dimension_value_id,
        .invoice_entry_id = invoice_entry_id,
        .invoice_line_id = invoice_line_id,
    };
}

test "ledger_version returns 0.0.1" {
    const v = std.mem.span(ledger_version());
    try std.testing.expectEqualStrings(VERSION, v);
}

test "ledger_version matches build.zig.zon package version" {
    const zon = try std.fs.cwd().readFileAlloc(std.testing.allocator, "build.zig.zon", 64 * 1024);
    defer std.testing.allocator.free(zon);
    const needle = ".version = \"" ++ VERSION ++ "\"";
    try std.testing.expect(std.mem.indexOf(u8, zon, needle) != null);
}

test "header schema version matches engine schema version" {
    const header = try std.fs.cwd().readFileAlloc(std.testing.allocator, "include/heft.h", 128 * 1024);
    defer std.testing.allocator.free(header);
    var buf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&buf, "#define HEFT_SCHEMA_VERSION    {d}", .{heft.schema.SCHEMA_VERSION}) catch unreachable;
    try std.testing.expect(std.mem.indexOf(u8, header, needle) != null);
}

test "header error codes match public mapError contract" {
    const header = try std.fs.cwd().readFileAlloc(std.testing.allocator, "include/heft.h", 128 * 1024);
    defer std.testing.allocator.free(header);
    const required = [_][]const u8{
        "HEFT_INVALID_INPUT                  = 2",
        "HEFT_BUFFER_TOO_SMALL               = 25",
        "HEFT_PERIOD_NOT_IN_BALANCE          = 32",
        "HEFT_EQUITY_ALLOCATION_TOTAL_INVALID = 36",
        "HEFT_SQLITE_BIND_FAILED             = 94",
    };
    for (required) |needle| {
        try std.testing.expect(std.mem.indexOf(u8, header, needle) != null);
    }
}

test "ledger_open returns non-null, ledger_close cleans up" {
    defer cleanupTestFile("test-open-close.ledger");
    const handle = ledger_open("test-open-close.ledger");
    try std.testing.expect(handle != null);
    if (handle) |h| ledger_close(h);
}

test "ledger_open creates all 16 schema tables" {
    defer cleanupTestFile("test-schema-main.ledger");
    const handle = ledger_open("test-schema-main.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const expected_tables = [_][]const u8{
            "ledger_books",
            "ledger_accounts",
            "ledger_periods",
            "ledger_classifications",
            "ledger_classification_nodes",
            "ledger_subledger_groups",
            "ledger_subledger_accounts",
            "ledger_entries",
            "ledger_entry_lines",
            "ledger_account_balances",
            "ledger_dimensions",
            "ledger_dimension_values",
            "ledger_line_dimensions",
            "ledger_budgets",
            "ledger_budget_lines",
            "ledger_audit_log",
        };

        for (expected_tables) |table_name| {
            var stmt = try h.sqlite.prepare(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;",
            );
            defer stmt.finalize();
            try stmt.bindText(1, table_name);
            _ = try stmt.step();
            try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
        }
    }
}

test "ledger_open enables WAL mode" {
    defer cleanupTestFile("test-wal.ledger");
    const handle = ledger_open("test-wal.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA journal_mode;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqualStrings("wal", stmt.columnText(0).?);
    }
}

test "ledger_open enables foreign keys" {
    defer cleanupTestFile("test-fk.ledger");
    const handle = ledger_open("test-fk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA foreign_keys;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 1), stmt.columnInt(0));
    }
}

test "ledger_open sets schema version on new file" {
    defer cleanupTestFile("test-version.ledger");
    const handle = ledger_open("test-version.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(SCHEMA_VERSION, stmt.columnInt(0));
    }
}

test "ledger_open rejects future schema version" {
    defer cleanupTestFile("test-future-version.ledger");

    const h1 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const raw_db = try heft.db.Database.open("test-future-version.ledger");
    try raw_db.exec("PRAGMA user_version = 999;");
    raw_db.close();

    const h2 = ledger_open("test-future-version.ledger");
    try std.testing.expect(h2 == null);
}

test "ledger_open preserves schema version on reopen" {
    defer cleanupTestFile("test-reopen-version.ledger");

    const h1 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    const h2 = ledger_open("test-reopen-version.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| {
        defer ledger_close(h);

        var stmt = try h.sqlite.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(SCHEMA_VERSION, stmt.columnInt(0));
    }
}

test "ledger_open returns null for invalid path" {
    const handle = ledger_open("/no/such/dir/bad.ledger");
    try std.testing.expect(handle == null);
}

test "ledger_open is idempotent on existing file" {
    defer cleanupTestFile("test-idempotent.ledger");
    const h1 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h1 != null);
    if (h1) |h| ledger_close(h);

    // Open same file again — schema uses IF NOT EXISTS
    const h2 = ledger_open("test-idempotent.ledger");
    try std.testing.expect(h2 != null);
    if (h2) |h| ledger_close(h);
}

// ── Sprint 2: C ABI integration tests ──────────────────────────

test "C ABI: full lifecycle book -> account -> period -> transition" {
    defer cleanupTestFile("test-cabi-lifecycle.ledger");
    const handle = ledger_open("test-cabi-lifecycle.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(acct_id > 0);
        const rounding_acct = ledger_create_account(h, book_id, "7000", "Rounding", "expense", 0, "admin");
        try std.testing.expect(rounding_acct > 0);

        const period_id = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(period_id > 0);

        try std.testing.expect(ledger_transition_period(h, period_id, "soft_closed", "admin"));
        try std.testing.expect(ledger_set_rounding_account(h, book_id, rounding_acct, "admin"));
        try std.testing.expect(ledger_update_account_status(h, acct_id, "archived", "admin"));
    }
}

test "C ABI: bulk create periods via C boundary" {
    defer cleanupTestFile("test-cabi-bulk.ledger");
    const handle = ledger_open("test-cabi-bulk.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        try std.testing.expect(ledger_bulk_create_periods(h, book_id, 2026, 1, "monthly", "admin"));

        var stmt = try h.sqlite.prepare("SELECT COUNT(*) FROM ledger_periods WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 12), stmt.columnInt(0));
    }
}

test "C ABI: null handle returns error values" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_book(null, "Test", "PHP", 2, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_account(null, 1, "1000", "Cash", "asset", 0, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_period(null, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin"));
    try std.testing.expect(!ledger_update_account_status(null, 1, "inactive", "admin"));
    try std.testing.expect(!ledger_transition_period(null, 1, "soft_closed", "admin"));
    try std.testing.expect(!ledger_set_rounding_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_bulk_create_periods(null, 1, 2026, 1, "monthly", "admin"));
    try std.testing.expect(!ledger_archive_book(null, 1, "admin"));
}

test "C ABI: null handle sets InvalidInput in last_error" {
    _ = ledger_create_book(null, "Test", "PHP", 2, "admin");
    try std.testing.expectEqual(@as(i32, 2), ledger_last_error());

    var buf: [256]u8 = undefined;
    _ = ledger_get_book(null, 1, &buf, buf.len, 1);
    try std.testing.expectEqual(@as(i32, 2), ledger_last_error());

    _ = ledger_trial_balance(null, 1, "2026-01-31");
    try std.testing.expectEqual(@as(i32, 2), ledger_last_error());
}

test "C ABI: invalid account_type string returns -1" {
    defer cleanupTestFile("test-cabi-bad-type.ledger");
    const handle = ledger_open("test-cabi-bad-type.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const result = ledger_create_account(h, book_id, "1000", "Cash", "invalid_type", 0, "admin");
        try std.testing.expectEqual(@as(i64, -1), result);
    }
}

test "C ABI: invalid granularity string returns false" {
    defer cleanupTestFile("test-cabi-bad-gran.ledger");
    const handle = ledger_open("test-cabi-bad-gran.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        try std.testing.expect(!ledger_bulk_create_periods(h, book_id, 2026, 1, "weekly", "admin"));
    }
}

test "C ABI: invalid account status string returns false" {
    defer cleanupTestFile("test-cabi-bad-acct-status.ledger");
    const handle = ledger_open("test-cabi-bad-acct-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const acct_id = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(!ledger_update_account_status(h, acct_id, "deleted", "admin"));
    }
}

test "C ABI: invalid period status string returns false" {
    defer cleanupTestFile("test-cabi-bad-period-status.ledger");
    const handle = ledger_open("test-cabi-bad-period-status.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_transition_period(h, 1, "deleted", "admin"));
    }
}

test "C ABI: archive book via C boundary" {
    defer cleanupTestFile("test-cabi-archive.ledger");
    const handle = ledger_open("test-cabi-archive.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        try std.testing.expect(ledger_archive_book(h, book_id, "admin"));

        var stmt = try h.sqlite.prepare("SELECT status FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
    }
}

test "C ABI: archive book with open periods returns false" {
    defer cleanupTestFile("test-cabi-archive-fail.ledger");
    const handle = ledger_open("test-cabi-archive-fail.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        try std.testing.expect(!ledger_archive_book(h, book_id, "admin"));
    }
}

// ── Sprint 3 C ABI integration tests ───────────────────────────

test "C ABI: full posting lifecycle through C boundary" {
    defer cleanupTestFile("test-cabi-posting.ledger");
    const handle = ledger_open("test-cabi-posting.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(entry_id > 0);

        const line1 = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        try std.testing.expect(line1 > 0);

        const line2 = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        try std.testing.expect(line2 > 0);

        try std.testing.expect(ledger_post_entry(h, entry_id, "admin"));

        var stmt = try h.sqlite.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("posted", stmt.columnText(0).?);
    }
}

test "C ABI: void entry through C boundary" {
    defer cleanupTestFile("test-cabi-void.ledger");
    const handle = ledger_open("test-cabi-void.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        try std.testing.expect(ledger_void_entry(h, entry_id, "Error", "admin"));

        var stmt = try h.sqlite.prepare(
            \\SELECT COUNT(*) FROM ledger_audit_log
            \\WHERE entity_type = 'entry' AND entity_id = ? AND action = 'void';
        );
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        _ = try stmt.step();
        try std.testing.expect(stmt.columnInt(0) >= 1);
    }
}

test "C ABI: reverse entry through C boundary" {
    defer cleanupTestFile("test-cabi-reverse.ledger");
    const handle = ledger_open("test-cabi-reverse.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "FY2026", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const reversal_id = ledger_reverse_entry(h, entry_id, "Accrual reversal", "2026-01-31", 0, "admin");
        try std.testing.expect(reversal_id > 0);

        var stmt = try h.sqlite.prepare(
            \\SELECT COUNT(*) FROM ledger_audit_log
            \\WHERE entity_type = 'entry' AND entity_id IN (?, ?) AND action IN ('reverse', 'create', 'post');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, entry_id);
        try stmt.bindInt(2, reversal_id);
        _ = try stmt.step();
        try std.testing.expect(stmt.columnInt(0) >= 3);
    }
}

test "C ABI: null handle returns error for Sprint 3 exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_draft(null, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_line(null, 1, 1, 100, 0, "PHP", 10_000_000_000, 1, 0, null, "admin"));
    try std.testing.expect(!ledger_post_entry(null, 1, "admin"));
    try std.testing.expect(!ledger_void_entry(null, 1, "Error", "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_reverse_entry(null, 1, "Reason", "2026-01-31", 0, "admin"));
    try std.testing.expect(!ledger_remove_line(null, 1, "admin"));
    try std.testing.expect(!ledger_delete_draft(null, 1, "admin"));
    try std.testing.expect(!ledger_edit_line(null, 1, 100, 0, "PHP", 10_000_000_000, 1, "admin"));
}

test "C ABI: edit line through C boundary" {
    defer cleanupTestFile("test-cabi-editline.ledger");
    const handle = ledger_open("test-cabi-editline.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line_id = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");

        try std.testing.expect(ledger_edit_line(h, line_id, 2_000_000_000_00, 0, "PHP", 10_000_000_000, 1, "admin"));
    }
}

test "C ABI: remove line and delete draft through C boundary" {
    defer cleanupTestFile("test-cabi-draft-ops.ledger");
    const handle = ledger_open("test-cabi-draft-ops.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line_id = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");

        try std.testing.expect(ledger_remove_line(h, line_id, "admin"));
        try std.testing.expect(ledger_delete_draft(h, entry_id, "admin"));
    }
}

// ── Sprint 4 C ABI: Report tests ───────────────────────────────

test "C ABI: trial balance through C boundary" {
    defer cleanupTestFile("test-cabi-tb.ledger");
    const handle = ledger_open("test-cabi-tb.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_trial_balance(h, book_id, "2026-01-31");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expect(ledger_result_row_count(r) >= 2);
            try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: income statement through C boundary" {
    defer cleanupTestFile("test-cabi-is.ledger");
    const handle = ledger_open("test-cabi-is.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "4000", "Revenue", "revenue", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_income_statement(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expectEqual(@as(i64, 5_000_000_000_00), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: balance sheet through C boundary" {
    defer cleanupTestFile("test-cabi-bs.ledger");
    const handle = ledger_open("test-cabi-bs.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const result = ledger_balance_sheet(h, book_id, "2026-01-31", "2026-01-01");
        try std.testing.expect(result != null);

        if (result) |r| {
            defer ledger_free_result(r);
            try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        }
    }
}

test "C ABI: null handle returns null/error for all exports" {
    try std.testing.expect(ledger_trial_balance(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_income_statement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_trial_balance_movement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_balance_sheet(null, 1, "2026-01-31", "2026-01-01") == null);
    try std.testing.expect(ledger_general_ledger(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_account_ledger(null, 1, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_journal_register(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expectEqual(@as(i64, -1), ledger_create_subledger_group(null, 1, "X", "customer", 1, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_subledger_account(null, 1, "X", "X", "customer", 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_classification(null, 1, "X", "balance_sheet", "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_group_node(null, 1, "X", 0, 0, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_add_account_node(null, 1, 1, 0, 0, "admin"));
    try std.testing.expect(!ledger_move_node(null, 1, 0, 0, "admin"));
    try std.testing.expect(!ledger_delete_classification(null, 1, "admin"));
    try std.testing.expect(ledger_classified_report(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_cash_flow_statement(null, 1, "2026-01-01", "2026-01-31") == null);
    try std.testing.expect(ledger_trial_balance_comparative(null, 1, "2026-01-31", "2025-12-31") == null);
    try std.testing.expect(ledger_income_statement_comparative(null, 1, "2026-01-01", "2026-01-31", "2025-01-01", "2025-12-31") == null);
    try std.testing.expect(ledger_balance_sheet_comparative(null, 1, "2026-01-31", "2025-12-31", "2026-01-01") == null);
    try std.testing.expect(ledger_trial_balance_movement_comparative(null, 1, "2026-01-01", "2026-01-31", "2025-01-01", "2025-12-31") == null);
}

test "C ABI: free null classified result is safe" {
    ledger_free_classified_result(null);
}

test "C ABI: classified report through C boundary" {
    defer cleanupTestFile("test-cabi-cls.ledger");
    const handle = ledger_open("test-cabi-cls.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const cls_id = ledger_create_classification(h, book_id, "BS", "balance_sheet", "admin");
        try std.testing.expect(cls_id > 0);

        const group = ledger_add_group_node(h, cls_id, "Assets", 0, 0, "admin");
        try std.testing.expect(group > 0);

        _ = ledger_add_account_node(h, cls_id, 1, group, 0, "admin");

        const result = ledger_classified_report(h, cls_id, "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| ledger_free_classified_result(r);
    }
}

test "C ABI: cash flow statement through C boundary" {
    defer cleanupTestFile("test-cabi-cfs.ledger");
    const handle = ledger_open("test-cabi-cfs.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const cls_id = ledger_create_classification(h, book_id, "SCF", "cash_flow", "admin");
        try std.testing.expect(cls_id > 0);

        const operating = ledger_add_group_node(h, cls_id, "Operating", 0, 0, "admin");
        try std.testing.expect(operating > 0);

        _ = ledger_add_account_node(h, cls_id, 1, operating, 0, "admin");

        const result = ledger_cash_flow_statement(h, cls_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| {
            try std.testing.expect(r.rows.len == 2);
            try std.testing.expect(r.rows[0].debit_balance == 10_000_000_000_00);
            ledger_free_classified_result(r);
        }
    }
}

test "C ABI: free null results is safe" {
    ledger_free_result(null);
    ledger_free_ledger_result(null);
    ledger_free_comparative_result(null);
}

test "C ABI: GL through C boundary" {
    defer cleanupTestFile("test-cabi-gl.ledger");
    const handle = ledger_open("test-cabi-gl.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        const gl = ledger_general_ledger(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(gl != null);
        if (gl) |r| ledger_free_ledger_result(r);

        const al = ledger_account_ledger(h, book_id, 1, "2026-01-01", "2026-01-31");
        try std.testing.expect(al != null);
        if (al) |r| ledger_free_ledger_result(r);

        const jr = ledger_journal_register(h, book_id, "2026-01-01", "2026-01-31");
        try std.testing.expect(jr != null);
        if (jr) |r| ledger_free_ledger_result(r);
    }
}

test "C ABI: ledger_edit_draft changes header fields" {
    defer cleanupTestFile("test-cabi-editdraft.ledger");
    const handle = ledger_open("test-cabi-editdraft.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(entry_id > 0);

        // Edit the draft header
        try std.testing.expect(ledger_edit_draft(h, entry_id, "JE-999", "2026-01-20", "2026-01-25", "Updated desc", null, 1, "admin"));

        // Verify via null handle rejection
        try std.testing.expect(!ledger_edit_draft(null, entry_id, "JE-999", "2026-01-20", "2026-01-25", null, null, 1, "admin"));
    }
}

test "C ABI: ledger_edit_posted changes description on posted entry" {
    defer cleanupTestFile("test-cabi-editposted.ledger");
    const handle = ledger_open("test-cabi-editposted.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        // Edit description on posted entry
        try std.testing.expect(ledger_edit_posted(h, entry_id, "Updated memo", null, "admin"));

        // Null handle rejection
        try std.testing.expect(!ledger_edit_posted(null, entry_id, "Memo", null, "admin"));
    }
}

test "C ABI: ledger_free_classified_result with null is safe" {
    ledger_free_classified_result(null);
}

test "C ABI: ledger_update_book_name" {
    defer cleanupTestFile("test-cabi-updatename.ledger");
    const handle = ledger_open("test-cabi-updatename.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Old", "PHP", 2, "admin");
        try std.testing.expect(ledger_update_book_name(h, 1, "New", "admin"));
        try std.testing.expect(!ledger_update_book_name(null, 1, "New", "admin"));
    }
}

test "C ABI: ledger_update_account_name" {
    defer cleanupTestFile("test-cabi-acctname.ledger");
    const handle = ledger_open("test-cabi-acctname.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        try std.testing.expect(ledger_update_account_name(h, 1, "Petty Cash", "admin"));
    }
}

test "C ABI: ledger_set_account_monetary" {
    defer cleanupTestFile("test-cabi-monetary.ledger");
    const handle = ledger_open("test-cabi-monetary.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1500", "Equipment", "asset", 0, "admin");
        try std.testing.expect(ledger_set_account_monetary(h, 1, 0, "admin"));
    }
}

test "C ABI: ledger_set_account_parent" {
    defer cleanupTestFile("test-cabi-parent.ledger");
    const handle = ledger_open("test-cabi-parent.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Current Assets", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "1001", "Cash", "asset", 0, "admin");
        try std.testing.expect(ledger_set_account_parent(h, 2, 1, "admin"));
        try std.testing.expect(ledger_set_account_parent(h, 2, 0, "admin"));
    }
}

test "C ABI: ledger_set_fy_start_month" {
    defer cleanupTestFile("test-cabi-fy.ledger");
    const handle = ledger_open("test-cabi-fy.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "India", "INR", 2, "admin");
        try std.testing.expect(ledger_set_fy_start_month(h, 1, 4, "admin"));
    }
}

test "C ABI: ledger_set_entity_type" {
    defer cleanupTestFile("test-cabi-entity.ledger");
    const handle = ledger_open("test-cabi-entity.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Juan dela Cruz", "PHP", 2, "admin");
        try std.testing.expect(ledger_set_entity_type(h, 1, "sole_proprietorship", "admin"));
        // Invalid entity type string must return false
        try std.testing.expect(!ledger_set_entity_type(h, 1, "nonsense", "admin"));
        // Null handle returns false
        try std.testing.expect(!ledger_set_entity_type(null, 1, "corporation", "admin"));
    }
}

test "C ABI: ledger_balance_sheet_auto" {
    defer cleanupTestFile("test-cabi-bsauto.ledger");
    const handle = ledger_open("test-cabi-bsauto.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        try std.testing.expect(ledger_add_line(h, eid, 1, 1000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin") > 0);
        try std.testing.expect(ledger_add_line(h, eid, 2, 0, 1000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin") > 0);
        try std.testing.expect(ledger_post_entry(h, eid, "admin"));
        const result = ledger_balance_sheet_auto(h, 1, "2026-01-31");
        try std.testing.expect(result != null);
        if (result) |r| {
            defer r.deinit();
            try std.testing.expectEqual(r.total_debits, r.total_credits);
        }
    }
}

test "C ABI: ledger_translate_report" {
    defer cleanupTestFile("test-cabi-translate.ledger");
    const handle = ledger_open("test-cabi-translate.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "3000", "Capital", "equity", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        _ = ledger_add_line(h, eid, 1, 5650_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, eid, 2, 0, 5650_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, eid, "admin");
        const tb = try heft.report.trialBalance(h.sqlite, 1, "2026-01-31");
        defer tb.deinit();
        const translated = ledger_translate_report(tb, 180_000_000, 175_000_000);
        try std.testing.expect(translated != null);
        if (translated) |t| {
            defer t.deinit();
            try std.testing.expectEqual(tb.rows.len, t.rows.len);
        }
    }
}

test "C ABI: ledger_delete_node" {
    defer cleanupTestFile("test-cabi-delnode.ledger");
    const handle = ledger_open("test-cabi-delnode.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        const cid = ledger_create_classification(h, 1, "BS", "balance_sheet", "admin");
        const gid = ledger_add_group_node(h, cid, "Assets", 0, 1, "admin");
        try std.testing.expect(gid > 0);
        try std.testing.expect(ledger_delete_node(h, gid, "admin"));
    }
}

test "C ABI: ledger_get_book returns bytes" {
    defer cleanupTestFile("test-cabi-getbook.ledger");
    const handle = ledger_open("test-cabi-getbook.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        var buf: [4096]u8 = undefined;
        const len = ledger_get_book(h, 1, &buf, 4096, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Test\"") != null);
    }
}

test "C ABI: ledger_list_books returns paginated JSON" {
    defer cleanupTestFile("test-cabi-listbooks.ledger");
    const handle = ledger_open("test-cabi-listbooks.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Book A", "PHP", 2, "admin");
        _ = ledger_create_book(h, "Book B", "USD", 2, "admin");
        var buf: [8192]u8 = undefined;
        const len = ledger_list_books(h, null, 0, 100, 0, &buf, 8192, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":2") != null);
    }
}

test "C ABI: ledger_list_accounts with filter" {
    defer cleanupTestFile("test-cabi-listaccts.ledger");
    const handle = ledger_open("test-cabi-listaccts.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, 1, "2000", "AP", "liability", 0, "admin");
        var buf: [8192]u8 = undefined;
        const len = ledger_list_accounts(h, 1, "asset", null, null, 0, 100, 0, &buf, 8192, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, json, "Cash") != null);
    }
}

test "C ABI: ledger_list_entries with date range" {
    defer cleanupTestFile("test-cabi-listentries.ledger");
    const handle = ledger_open("test-cabi-listentries.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
        _ = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        var buf: [16384]u8 = undefined;
        const len = ledger_list_entries(h, 1, null, "2026-01-01", "2026-01-31", null, 0, 100, 0, &buf, 16384, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "JE-001") != null);
    }
}

test "C ABI: ledger_list_audit_log" {
    defer cleanupTestFile("test-cabi-listaudit.ledger");
    const handle = ledger_open("test-cabi-listaudit.ledger");
    if (handle) |h| {
        defer ledger_close(h);
        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        var buf: [32768]u8 = undefined;
        const len = ledger_list_audit_log(h, 1, null, null, null, null, 0, 100, 0, &buf, 32768, 1);
        try std.testing.expect(len > 0);
    }
}

test "C ABI: null handle returns -1 for all query exports" {
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_get_book(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_books(null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_account(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_accounts(null, 1, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_period(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_periods(null, 1, 0, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_get_entry(null, 1, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_entries(null, 1, null, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_entry_lines(null, 1, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_classifications(null, 1, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_subledger_groups(null, 1, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_subledger_accounts(null, 1, 0, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_audit_log(null, 1, null, null, null, null, 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_subledger_report(null, 1, 0, null, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_counterparty_ledger(null, 1, 1, 0, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_list_transactions(null, 1, 0, 0, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_subledger_reconciliation(null, 1, 1, "2026-01-31", &buf, 1024, 1));
    try std.testing.expectEqual(@as(i32, -1), ledger_aged_subledger(null, 1, 0, "2026-01-31", 0, 100, 0, &buf, 1024, 1));
}

test "C ABI: null handle returns false for all CRUD exports" {
    try std.testing.expect(!ledger_update_book_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_account_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_set_account_parent(null, 1, 2, "admin"));
    try std.testing.expect(!ledger_set_account_monetary(null, 1, 0, "admin"));
    try std.testing.expect(!ledger_set_fy_start_month(null, 1, 4, "admin"));
    try std.testing.expect(ledger_balance_sheet_auto(null, 1, "2026-01-31") == null);
    try std.testing.expect(ledger_translate_report(null, 10_000_000_000, 10_000_000_000) == null);
    try std.testing.expect(!ledger_update_classification_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_node_label(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_delete_node(null, 1, "admin"));
    try std.testing.expect(!ledger_update_subledger_group_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_delete_subledger_group(null, 1, "admin"));
    try std.testing.expect(!ledger_update_subledger_account_name(null, 1, "X", "admin"));
    try std.testing.expect(!ledger_update_subledger_account_status(null, 1, "inactive", "admin"));
    try std.testing.expect(!ledger_delete_subledger_account(null, 1, "admin"));
    try std.testing.expect(!ledger_edit_line_full(null, 1, 100, 0, "PHP", 10_000_000_000, 1, 0, null, "admin"));
}

test "C ABI: ledger_last_error returns error code after failure" {
    defer cleanupTestFile("test-cabi-lasterr.ledger");
    const handle = ledger_open("test-cabi-lasterr.ledger");
    if (handle) |h| {
        defer ledger_close(h);

        // Create book succeeds — error should be 0
        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);

        // Try to create book with invalid currency — should fail and set error
        const bad = ledger_create_book(h, "Bad", "XX", 2, "admin");
        try std.testing.expectEqual(@as(i64, -1), bad);
        const err = ledger_last_error();
        try std.testing.expectEqual(@as(i32, 2), err);
    }
}

test "C ABI: ledger_last_error after post failure" {
    defer cleanupTestFile("test-cabi-lasterr2.ledger");
    const handle = ledger_open("test-cabi-lasterr2.ledger");
    if (handle) |h| {
        defer ledger_close(h);

        _ = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, 1, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_period(h, 1, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const eid = ledger_create_draft(h, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        // Only 1 line — post should fail with TooFewLines
        _ = ledger_add_line(h, eid, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        const posted = ledger_post_entry(h, eid, "admin");
        try std.testing.expect(!posted);
        const err = ledger_last_error();
        try std.testing.expectEqual(@as(i32, 16), err);
    }
}

test "C ABI: ledger_open null path returns null" {
    const handle = ledger_open(null);
    try std.testing.expect(handle == null);
    try std.testing.expect(ledger_last_error() > 0);
}

test "C ABI: ledger_verify null out params returns false" {
    defer cleanupTestFile("test-verify-null.ledger");
    const h = ledger_open("test-verify-null.ledger");
    try std.testing.expect(h != null);
    defer if (h) |handle| ledger_close(handle);
    if (h) |handle| {
        const book_id = ledger_create_book(handle, "Test", "PHP", 2, "admin");
        try std.testing.expect(book_id > 0);
        try std.testing.expect(!ledger_verify(handle, book_id, null, null));
    }
}

test "C ABI: ledger_verify clean book passes" {
    defer cleanupTestFile("test-verify-clean.ledger");
    const h = ledger_open("test-verify-clean.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1001", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "4001", "Revenue", "revenue", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(h, entry_id, 1, 100_000_000, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(h, entry_id, 2, 0, 100_000_000, "PHP", 10_000_000_000, 2, 0, null, "admin");
    _ = ledger_post_entry(h, entry_id, "admin");
    var errors: u32 = 99;
    var warnings: u32 = 99;
    const passed = ledger_verify(h, book_id, &errors, &warnings);
    try std.testing.expect(passed);
    try std.testing.expectEqual(@as(u32, 0), errors);
}

test "C ABI: ledger_edit_line_full happy path" {
    defer cleanupTestFile("test-editfull.ledger");
    const h = ledger_open("test-editfull.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1001", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "4001", "Revenue", "revenue", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = ledger_add_line(h, entry_id, 1, 100_000_000, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    try std.testing.expect(line_id > 0);
    const ok = ledger_edit_line_full(h, line_id, 200_000_000, 0, "PHP", 10_000_000_000, 1, 0, "Office supplies", "admin");
    try std.testing.expect(ok);
}

test "C ABI: invalid format value returns error" {
    defer cleanupTestFile("test-badfmt.ledger");
    const h = ledger_open("test-badfmt.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    var buf: [1024]u8 = undefined;
    const result = ledger_get_book(h, book_id, &buf, 1024, 99);
    try std.testing.expectEqual(@as(i32, -1), result);
    try std.testing.expectEqual(@as(i32, 2), ledger_last_error());
}

// ── Sprint 12: System account designation + parameter gap tests ──

test "C ABI: null handle returns false for system account exports" {
    try std.testing.expect(!ledger_set_fx_gain_loss_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_retained_earnings_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_income_summary_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_opening_balance_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_set_suspense_account(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_validate_opening_balance(null, 1));
}

test "C ABI: ledger_create_draft with description and metadata" {
    defer cleanupTestFile("test-cabi-draft-desc.ledger");
    const h = ledger_open("test-cabi-draft-desc.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-100", "2026-01-15", "2026-01-15", "Office supplies purchase", 1, "{\"dept\":\"ops\"}", "admin");
    try std.testing.expect(entry_id > 0);
    var buf: [8192]u8 = undefined;
    const len = ledger_get_entry(h, entry_id, book_id, &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "Office supplies purchase") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "ops") != null);
}

test "C ABI: ledger_add_line with description" {
    defer cleanupTestFile("test-cabi-line-desc.ledger");
    const h = ledger_open("test-cabi-line-desc.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "5000", "Supplies", "expense", 0, "admin");
    _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const entry_id = ledger_create_draft(h, book_id, "JE-200", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    const line_id = ledger_add_line(h, entry_id, 1, 500_000_000, 0, "PHP", 10_000_000_000, 1, 0, "Pens and paper", "admin");
    try std.testing.expect(line_id > 0);
    var buf: [8192]u8 = undefined;
    const len = ledger_list_entry_lines(h, entry_id, &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "Pens and paper") != null);
}

test "C ABI: ledger_reverse_entry with target_period_id" {
    defer cleanupTestFile("test-cabi-rev-period.ledger");
    const h = ledger_open("test-cabi-rev-period.ledger") orelse unreachable;
    defer ledger_close(h);
    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(h, book_id, "2000", "AP", "liability", 0, "admin");
    const p1 = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const p2 = ledger_create_period(h, book_id, "Feb 2026", 2, 2026, "2026-02-01", "2026-02-28", "regular", "admin");
    try std.testing.expect(p1 > 0);
    try std.testing.expect(p2 > 0);
    const entry_id = ledger_create_draft(h, book_id, "JE-300", "2026-01-15", "2026-01-15", null, p1, null, "admin");
    _ = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
    _ = ledger_post_entry(h, entry_id, "admin");
    const reversal_id = ledger_reverse_entry(h, entry_id, "Period correction", "2026-02-01", p2, "admin");
    try std.testing.expect(reversal_id > 0);
}

test "C ABI: system account designation happy path" {
    defer cleanupTestFile("test-cabi-sysacct.ledger");
    const h = ledger_open("test-cabi-sysacct.ledger") orelse unreachable;
    defer ledger_close(h);

    const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
    try std.testing.expect(book_id > 0);

    const expense_acct = ledger_create_account(h, book_id, "6900", "FX Rounding", "expense", 0, "admin");
    const equity_acct = ledger_create_account(h, book_id, "3100", "Retained Earnings", "equity", 0, "admin");
    const equity_acct2 = ledger_create_account(h, book_id, "3200", "Income Summary", "equity", 0, "admin");
    const equity_acct3 = ledger_create_account(h, book_id, "3300", "Opening Balance", "equity", 0, "admin");
    const asset_acct = ledger_create_account(h, book_id, "8000", "Suspense", "asset", 0, "admin");

    try std.testing.expect(expense_acct > 0);
    try std.testing.expect(equity_acct > 0);
    try std.testing.expect(equity_acct2 > 0);
    try std.testing.expect(equity_acct3 > 0);
    try std.testing.expect(asset_acct > 0);

    try std.testing.expect(ledger_set_fx_gain_loss_account(h, book_id, expense_acct, "admin"));
    try std.testing.expect(ledger_set_retained_earnings_account(h, book_id, equity_acct, "admin"));
    try std.testing.expect(ledger_set_income_summary_account(h, book_id, equity_acct2, "admin"));
    try std.testing.expect(ledger_set_opening_balance_account(h, book_id, equity_acct3, "admin"));
    try std.testing.expect(ledger_set_suspense_account(h, book_id, asset_acct, "admin"));

    var buf: [4096]u8 = undefined;
    const len = ledger_get_book(h, book_id, &buf, 4096, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];

    try std.testing.expect(std.mem.indexOf(u8, json, "\"fx_gain_loss_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"retained_earnings_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"income_summary_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"opening_balance_account_id\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"suspense_account_id\":") != null);
}

// ── Sprint 14: Period closing C ABI tests ──

test "C ABI: null handle returns false for ledger_close_period" {
    try std.testing.expect(!ledger_close_period(null, 1, 1, "admin"));
}

// ── Sprint 15: FX revaluation C ABI tests ──

test "C ABI: null handle returns -1 for ledger_revalue_forex_balances" {
    try std.testing.expectEqual(@as(i64, -1), ledger_revalue_forex_balances(null, 1, 1, "[{\"currency\":\"USD\",\"rate\":57000000000}]", "admin"));
}

// ── Sprint 16B: Equity changes C ABI tests ──

test "C ABI: null handle returns null for ledger_equity_changes" {
    try std.testing.expect(ledger_equity_changes(null, 1, "2026-01-01", "2026-01-31", "2026-01-01") == null);
}

test "C ABI: free null equity result is safe" {
    ledger_free_equity_result(null);
}

// ── Sprint 17: Dimension C ABI tests ──

test "C ABI: null handle returns error for dimension exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_dimension(null, 1, "Tax", "tax_code", "admin"));
    try std.testing.expect(!ledger_delete_dimension(null, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_create_dimension_value(null, 1, "VAT12", "VAT 12%", "admin"));
    try std.testing.expect(!ledger_delete_dimension_value(null, 1, "admin"));
    try std.testing.expect(!ledger_assign_line_dimension(null, 1, 1, "admin"));
    try std.testing.expect(!ledger_remove_line_dimension(null, 1, 1, "admin"));
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_dimension_summary(null, 1, 1, "2026-01-01", "2026-01-31", &buf, 1024, 1));
}

test "C ABI: invalid dimension_type string returns -1" {
    defer cleanupTestFile("test-cabi-bad-dimtype.ledger");
    const handle = ledger_open("test-cabi-bad-dimtype.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        const result = ledger_create_dimension(h, book_id, "Bad", "invalid_type", "admin");
        try std.testing.expectEqual(@as(i64, -1), result);
    }
}

test "C ABI: dimension full lifecycle through C boundary" {
    defer cleanupTestFile("test-cabi-dim-lifecycle.ledger");
    const handle = ledger_open("test-cabi-dim-lifecycle.ledger");
    try std.testing.expect(handle != null);

    if (handle) |h| {
        defer ledger_close(h);

        const book_id = ledger_create_book(h, "Test", "PHP", 2, "admin");
        _ = ledger_create_account(h, book_id, "1000", "Cash", "asset", 0, "admin");
        _ = ledger_create_account(h, book_id, "4000", "Revenue", "revenue", 0, "admin");
        _ = ledger_create_period(h, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

        const dim_id = ledger_create_dimension(h, book_id, "Tax Code", "tax_code", "admin");
        try std.testing.expect(dim_id > 0);

        const val_id = ledger_create_dimension_value(h, dim_id, "VAT12", "VAT 12%", "admin");
        try std.testing.expect(val_id > 0);

        const entry_id = ledger_create_draft(h, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
        const line1 = ledger_add_line(h, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
        _ = ledger_add_line(h, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");
        _ = ledger_post_entry(h, entry_id, "admin");

        try std.testing.expect(ledger_assign_line_dimension(h, line1, val_id, "admin"));

        var buf: [4096]u8 = undefined;
        const len = ledger_dimension_summary(h, book_id, dim_id, "2026-01-01", "2026-01-31", &buf, 4096, 1);
        try std.testing.expect(len > 0);
        const json = buf[0..@intCast(len)];
        try std.testing.expect(std.mem.indexOf(u8, json, "VAT12") != null);

        try std.testing.expect(ledger_remove_line_dimension(h, line1, val_id, "admin"));
        try std.testing.expect(ledger_delete_dimension_value(h, val_id, "admin"));
        try std.testing.expect(ledger_delete_dimension(h, dim_id, "admin"));
    }
}

// ── Sprint 18B: Budget C ABI tests ──

test "C ABI: null handle returns error for budget exports" {
    try std.testing.expectEqual(@as(i64, -1), ledger_create_budget(null, 1, "FY2026", 2026, "admin"));
    try std.testing.expect(!ledger_delete_budget(null, 1, "admin"));
    try std.testing.expectEqual(@as(i64, -1), ledger_set_budget_line(null, 1, 1, 1, 100, "admin"));
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_budget_vs_actual(null, 1, "2026-01-01", "2026-01-31", &buf, 1024, 1));
}

// ── Sprint 19: Batch C ABI tests ──

test "C ABI: null handle returns false for batch exports" {
    var succeeded: u32 = 99;
    var failed: u32 = 99;
    try std.testing.expect(!ledger_batch_post(null, "[1,2]", "admin", &succeeded, &failed));
    try std.testing.expect(!ledger_batch_void(null, "[1,2]", "Error", "admin", &succeeded, &failed));
}

test "C ABI: batch post and void lifecycle" {
    defer cleanupTestFile("test-cabi-batch.ledger");
    const handle = ledger_open("test-cabi-batch.ledger") orelse unreachable;
    defer ledger_close(handle);

    _ = ledger_create_book(handle, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, 1, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, 1, "2000", "AP", "liability", 0, "admin");
    _ = ledger_create_period(handle, 1, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    const e1 = ledger_create_draft(handle, 1, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, e1, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, e1, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    const e2 = ledger_create_draft(handle, 1, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, e2, 1, 2_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, e2, 2, 0, 2_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    var succeeded: u32 = 0;
    var failed: u32 = 0;

    var ids_json_buf: [65]u8 = undefined;
    const ids_json = std.fmt.bufPrint(ids_json_buf[0..64], "[{d},{d}]", .{ e1, e2 }) catch unreachable;
    ids_json_buf[ids_json.len] = 0;
    const ids_z: [*:0]const u8 = ids_json_buf[0..ids_json.len :0];

    const post_ok = ledger_batch_post(handle, ids_z, "admin", &succeeded, &failed);
    try std.testing.expect(post_ok);
    try std.testing.expectEqual(@as(u32, 2), succeeded);
    try std.testing.expectEqual(@as(u32, 0), failed);

    const void_ok = ledger_batch_void(handle, ids_z, "Batch correction", "admin", &succeeded, &failed);
    try std.testing.expect(void_ok);
    try std.testing.expectEqual(@as(u32, 2), succeeded);
    try std.testing.expectEqual(@as(u32, 0), failed);
}

test "C ABI: ledger_describe_schema null handle returns -1" {
    var buf: [64]u8 = undefined;
    const result = ledger_describe_schema(null, &buf, 64, 0);
    try std.testing.expectEqual(@as(i32, -1), result);
}

test "C ABI: ledger_describe_schema happy path returns > 0" {
    defer cleanupTestFile("test-describe-schema.ledger");
    const handle = ledger_open("test-describe-schema.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    var buf: [65536]u8 = undefined;
    const csv_len = ledger_describe_schema(handle, &buf, 65536, 0);
    try std.testing.expect(csv_len > 0);

    const json_len = ledger_describe_schema(handle, &buf, 65536, 1);
    try std.testing.expect(json_len > 0);
}

test "C ABI: ledger_approve_entry null handle returns false" {
    const result = ledger_approve_entry(null, 1, "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_reject_entry null handle returns false" {
    const result = ledger_reject_entry(null, 1, "bad", "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_set_require_approval null handle returns false" {
    const result = ledger_set_require_approval(null, 1, 1, "admin");
    try std.testing.expect(!result);
}

test "C ABI: ledger_recalculate_balances null handle returns -1" {
    const result = ledger_recalculate_balances(null, 1);
    try std.testing.expectEqual(@as(i32, -1), result);
}

test "C ABI: comprehensive lifecycle — book, accounts, period, draft, lines, approve, post, TB, verify, close, describe" {
    defer cleanupTestFile("test-cabi-comprehensive.ledger");
    const handle = ledger_open("test-cabi-comprehensive.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Comprehensive", "PHP", 2, "admin");
    try std.testing.expect(book_id > 0);

    const cash_id = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    try std.testing.expect(cash_id > 0);
    const rev_id = ledger_create_account(handle, book_id, "4000", "Revenue", "revenue", 0, "admin");
    try std.testing.expect(rev_id > 0);
    const equity_id = ledger_create_account(handle, book_id, "3000", "Capital", "equity", 0, "admin");
    try std.testing.expect(equity_id > 0);

    const period_id = ledger_create_period(handle, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    try std.testing.expect(period_id > 0);

    try std.testing.expect(ledger_set_require_approval(handle, book_id, 1, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", "Service income", period_id, null, "admin");
    try std.testing.expect(entry_id > 0);

    const l1 = ledger_add_line(handle, entry_id, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, cash_id, 0, null, "admin");
    try std.testing.expect(l1 > 0);
    const l2 = ledger_add_line(handle, entry_id, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, rev_id, 0, null, "admin");
    try std.testing.expect(l2 > 0);

    try std.testing.expect(ledger_approve_entry(handle, entry_id, "manager"));

    try std.testing.expect(ledger_post_entry(handle, entry_id, "admin"));

    const tb = ledger_trial_balance(handle, book_id, "2026-01-31");
    try std.testing.expect(tb != null);
    if (tb) |r| {
        defer ledger_free_result(r);
        try std.testing.expect(ledger_result_row_count(r) >= 2);
        try std.testing.expectEqual(ledger_result_total_debits(r), ledger_result_total_credits(r));
        try std.testing.expectEqual(@as(i64, 5_000_000_000_00), ledger_result_total_debits(r));
    }

    var err_count: u32 = 0;
    var warn_count: u32 = 0;
    try std.testing.expect(ledger_verify(handle, book_id, &err_count, &warn_count));
    try std.testing.expectEqual(@as(u32, 0), err_count);

    try std.testing.expect(ledger_transition_period(handle, period_id, "soft_closed", "admin"));
    try std.testing.expect(ledger_transition_period(handle, period_id, "closed", "admin"));

    var desc_buf: [65536]u8 = undefined;
    const json_len = ledger_describe_schema(handle, &desc_buf, 65536, 1);
    try std.testing.expect(json_len > 0);
    const csv_len = ledger_describe_schema(handle, &desc_buf, 65536, 0);
    try std.testing.expect(csv_len > 0);
}

test "C ABI: ledger_reject_entry rejects draft with reason" {
    defer cleanupTestFile("test-cabi-reject.ledger");
    const handle = ledger_open("test-cabi-reject.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, book_id, "2000", "AP", "liability", 0, "admin");
    _ = ledger_create_period(handle, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try std.testing.expect(ledger_set_require_approval(handle, book_id, 1, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, entry_id, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, 0, null, "admin");
    _ = ledger_add_line(handle, entry_id, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, 2, 0, null, "admin");

    try std.testing.expect(ledger_reject_entry(handle, entry_id, "Incorrect amounts", "manager"));

    try std.testing.expect(!ledger_post_entry(handle, entry_id, "admin"));
}

test "C ABI: ledger_revalue_forex_balances with valid rates" {
    defer cleanupTestFile("test-cabi-revalue.ledger");
    const handle = ledger_open("test-cabi-revalue.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "FX Book", "PHP", 2, "admin");
    const cash_php = ledger_create_account(handle, book_id, "1000", "Cash PHP", "asset", 0, "admin");
    const cash_usd = ledger_create_account(handle, book_id, "1001", "Cash USD", "asset", 0, "admin");
    const fx_gl = ledger_create_account(handle, book_id, "5000", "FX Gain/Loss", "expense", 0, "admin");
    _ = ledger_create_period(handle, book_id, "Jan", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    try std.testing.expect(ledger_set_fx_gain_loss_account(handle, book_id, fx_gl, "admin"));

    const entry_id = ledger_create_draft(handle, book_id, "JE-001", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = ledger_add_line(handle, entry_id, 1, 10_000_000_000, 0, "USD", 565_000_000_000, cash_usd, 0, null, "admin");
    _ = ledger_add_line(handle, entry_id, 2, 0, 565_000_000_000, "PHP", 10_000_000_000, cash_php, 0, null, "admin");
    try std.testing.expect(ledger_post_entry(handle, entry_id, "admin"));

    const reval_entry_id = ledger_revalue_forex_balances(handle, book_id, 1, "[{\"currency\":\"USD\",\"rate\":570000000000}]", "admin");
    try std.testing.expect(reval_entry_id > 0);
}

test "C ABI: export wrappers null handle returns -1" {
    var buf: [4096]u8 = undefined;
    try std.testing.expectEqual(@as(i32, -1), ledger_export_chart_of_accounts(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_journal_entries(null, 1, "2026-01-01", "2026-12-31", &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_audit_trail(null, 1, "2026-01-01", "2026-12-31", &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_periods(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_subledger(null, 1, &buf, 4096, 0));
    try std.testing.expectEqual(@as(i32, -1), ledger_export_book_metadata(null, 1, &buf, 4096, 0));
    try std.testing.expect(!ledger_transition_budget(null, 1, "approved", "admin"));
}

test "C ABI: export chart of accounts via C boundary" {
    defer cleanupTestFile("test-cabi-export-coa.ledger");
    const handle = ledger_open("test-cabi-export-coa.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Export Test", "PHP", 2, "admin");
    _ = ledger_create_account(handle, book_id, "1000", "Cash", "asset", 0, "admin");
    _ = ledger_create_account(handle, book_id, "4000", "Revenue", "revenue", 0, "admin");

    var buf: [8192]u8 = undefined;
    const csv_len = ledger_export_chart_of_accounts(handle, book_id, &buf, 8192, 0);
    try std.testing.expect(csv_len > 0);
    const csv = buf[0..@intCast(csv_len)];
    try std.testing.expect(std.mem.indexOf(u8, csv, "1000") != null);

    const json_len = ledger_export_chart_of_accounts(handle, book_id, &buf, 8192, 1);
    try std.testing.expect(json_len > 0);
    const json = buf[0..@intCast(json_len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "\"number\":\"1000\"") != null);
}

test "C ABI: export audit trail via C boundary" {
    defer cleanupTestFile("test-cabi-export-audit.ledger");
    const handle = ledger_open("test-cabi-export-audit.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    _ = ledger_create_book(handle, "Audit Test", "PHP", 2, "admin");

    var buf: [8192]u8 = undefined;
    const len = ledger_export_audit_trail(handle, 1, "2020-01-01", "2030-12-31", &buf, 8192, 1);
    try std.testing.expect(len > 0);
    const json = buf[0..@intCast(len)];
    try std.testing.expect(std.mem.indexOf(u8, json, "create") != null);
}

test "C ABI: round 2 feature coverage happy paths" {
    defer cleanupTestFile("test-cabi-feature-coverage.ledger");
    const handle = ledger_open("test-cabi-feature-coverage.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const s = try setupAbiFeatureScenario(handle);

    const open_item_id = ledger_create_open_item(handle, s.invoice_line_id, s.customer_id, 5_000_000_000_00, "2026-02-15", s.book_id, "admin");
    try std.testing.expect(open_item_id > 0);
    try std.testing.expect(ledger_allocate_payment(handle, open_item_id, 2_000_000_000_00, "admin"));

    var buf: [16384]u8 = undefined;

    const open_items_len = ledger_list_open_items(handle, s.customer_id, false, &buf, 16384, 1);
    try std.testing.expect(open_items_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(open_items_len)], "\"remaining_amount\":300000000000") != null);

    const cf_indirect = ledger_cash_flow_indirect(handle, s.book_id, "2026-01-01", "2026-01-31", s.cash_flow_classification_id);
    try std.testing.expect(cf_indirect != null);
    if (cf_indirect) |result| {
        defer ledger_free_cash_flow_indirect(result);
        try std.testing.expect(result.adjustments.len > 0);
    }

    const classified_tb = ledger_classified_trial_balance(handle, s.trial_balance_classification_id, "2026-01-31");
    try std.testing.expect(classified_tb != null);
    if (classified_tb) |result| {
        defer ledger_free_classified_result(result);
        try std.testing.expect(result.rows.len > 0);
    }

    const dim_rollup_len = ledger_dimension_summary_rollup(handle, s.book_id, s.dimension_id, "2026-01-01", "2026-01-31", &buf, 16384, 1);
    try std.testing.expect(dim_rollup_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(dim_rollup_len)], "VAT12") != null);

    const counterparty_ledger_len = ledger_counterparty_ledger(handle, s.book_id, s.customer_id, 0, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 16384, 1);
    try std.testing.expect(counterparty_ledger_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(counterparty_ledger_len)], "INV-2026-001") != null);

    const transactions_len = ledger_list_transactions(handle, s.book_id, 0, s.customer_id, "2026-01-01", "2026-01-31", 0, 100, 0, &buf, 16384, 1);
    try std.testing.expect(transactions_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(transactions_len)], "RCPT-2026-001") != null);

    const subledger_recon_len = ledger_subledger_reconciliation(handle, s.book_id, s.customer_group_id, "2026-01-31", &buf, 16384, 1);
    try std.testing.expect(subledger_recon_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(subledger_recon_len)], "\"difference\":0") != null);

    const aged_len = ledger_aged_subledger(handle, s.book_id, s.customer_group_id, "2026-01-31", 0, 100, 0, &buf, 16384, 1);
    try std.testing.expect(aged_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(aged_len)], "Customer ABC") != null);

    const export_journal_len = ledger_export_journal_entries(handle, s.book_id, "2025-12-01", "2026-12-31", &buf, 16384, 1);
    try std.testing.expect(export_journal_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(export_journal_len)], "INV-2026-001") != null);

    const export_periods_len = ledger_export_periods(handle, s.book_id, &buf, 16384, 1);
    try std.testing.expect(export_periods_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(export_periods_len)], "Jan 2026") != null);

    const export_subledger_len = ledger_export_subledger(handle, s.book_id, &buf, 16384, 1);
    try std.testing.expect(export_subledger_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(export_subledger_len)], "Customer ABC") != null);

    const export_metadata_len = ledger_export_book_metadata(handle, s.book_id, &buf, 16384, 1);
    try std.testing.expect(export_metadata_len > 0);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..@intCast(export_metadata_len)], "Feature Test") != null);

    const is_comp = ledger_income_statement_comparative(handle, s.book_id, "2026-01-01", "2026-01-31", "2025-12-01", "2025-12-31");
    try std.testing.expect(is_comp != null);
    if (is_comp) |result| {
        defer ledger_free_comparative_result(result);
        try std.testing.expect(result.rows.len > 0);
    }

    const bs_comp = ledger_balance_sheet_comparative(handle, s.book_id, "2026-01-31", "2025-12-31", "2026-01-01");
    try std.testing.expect(bs_comp != null);
    if (bs_comp) |result| {
        defer ledger_free_comparative_result(result);
        try std.testing.expect(result.rows.len > 0);
    }

    const tbm_comp = ledger_trial_balance_movement_comparative(handle, s.book_id, "2026-01-01", "2026-01-31", "2025-12-01", "2025-12-31");
    try std.testing.expect(tbm_comp != null);
    if (tbm_comp) |result| {
        defer ledger_free_comparative_result(result);
        try std.testing.expect(result.rows.len > 0);
    }
}

test "C ABI: ledger_transition_budget via C boundary" {
    defer cleanupTestFile("test-cabi-budget-trans.ledger");
    const handle = ledger_open("test-cabi-budget-trans.ledger") orelse return error.TestUnexpectedResult;
    defer ledger_close(handle);

    const book_id = ledger_create_book(handle, "Budget Test", "PHP", 2, "admin");
    const budget_id = heft.budget.Budget.create(handle.sqlite, book_id, "FY2026", 2026, "admin") catch return error.TestUnexpectedResult;
    try std.testing.expect(ledger_transition_budget(handle, budget_id, "approved", "admin"));
    try std.testing.expect(!ledger_transition_budget(handle, budget_id, "draft", "admin"));
}
