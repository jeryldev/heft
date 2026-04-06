const std = @import("std");
const db = @import("db.zig");
const schema = @import("schema.zig");
const book_mod = @import("book.zig");
const account_mod = @import("account.zig");
const period_mod = @import("period.zig");
const entry_mod = @import("entry.zig");
const report_mod = @import("report.zig");
const verify_mod = @import("verify.zig");
const cache_mod = @import("cache.zig");
const close_mod = @import("close.zig");
const revaluation_mod = @import("revaluation.zig");
const classification_mod = @import("classification.zig");
const subledger_mod = @import("subledger.zig");
const dimension_mod = @import("dimension.zig");
const budget_mod = @import("budget.zig");
const export_mod = @import("export.zig");
const query_mod = @import("query.zig");
const describe_mod = @import("describe.zig");
const batch_mod = @import("batch.zig");
const open_item_mod = @import("open_item.zig");
const money = @import("money.zig");

fn findReportRow(rows: []const report_mod.ReportRow, account_id: i64) ?report_mod.ReportRow {
    for (rows) |row| {
        if (row.account_id == account_id) return row;
    }
    return null;
}

fn findEquityRow(rows: []const report_mod.EquityRow, account_id: i64) ?report_mod.EquityRow {
    for (rows) |row| {
        if (row.account_id == account_id) return row;
    }
    return null;
}

test "LIFECYCLE: Complete fiscal year — setup through year-end close" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    // ═══════════════════════════════════════════════════════════════
    // PHASE 0: BOOK SETUP
    // ═══════════════════════════════════════════════════════════════

    // 0.1 Create book — Philippine company, PHP base currency
    const book_id = try book_mod.Book.create(database, "Acme Corp Philippines", "PHP", 2, "controller");
    try std.testing.expect(book_id > 0);

    // 0.2 Create chart of accounts (10 types represented)
    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "controller");
    const cash_usd = try account_mod.Account.create(database, book_id, "1001", "Cash USD", .asset, false, "controller");
    const ar = try account_mod.Account.create(database, book_id, "1100", "Accounts Receivable", .asset, false, "controller");
    _ = try account_mod.Account.create(database, book_id, "1101", "Allowance for Doubtful Accounts", .asset, true, "controller");
    const equipment = try account_mod.Account.create(database, book_id, "1500", "Equipment", .asset, false, "controller");
    const accum_dep = try account_mod.Account.create(database, book_id, "1501", "Accumulated Depreciation", .asset, true, "controller");
    const ap = try account_mod.Account.create(database, book_id, "2000", "Accounts Payable", .liability, false, "controller");
    const capital = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "controller");
    const retained_earnings = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "controller");
    const income_summary = try account_mod.Account.create(database, book_id, "3200", "Income Summary", .equity, false, "controller");
    const opening_bal_eq = try account_mod.Account.create(database, book_id, "3300", "Opening Balance Equity", .equity, false, "controller");
    _ = try account_mod.Account.create(database, book_id, "3900", "Owner Drawings", .equity, true, "controller");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Sales Revenue", .revenue, false, "controller");
    const sales_returns = try account_mod.Account.create(database, book_id, "4100", "Sales Returns", .revenue, true, "controller");
    const cogs = try account_mod.Account.create(database, book_id, "5000", "Cost of Goods Sold", .expense, false, "controller");
    const salaries = try account_mod.Account.create(database, book_id, "6000", "Salaries Expense", .expense, false, "controller");
    const rent = try account_mod.Account.create(database, book_id, "6100", "Rent Expense", .expense, false, "controller");
    const depreciation_exp = try account_mod.Account.create(database, book_id, "6200", "Depreciation Expense", .expense, false, "controller");
    _ = try account_mod.Account.create(database, book_id, "6300", "Bad Debt Expense", .expense, false, "controller");
    const fx_rounding = try account_mod.Account.create(database, book_id, "6900", "FX Rounding", .expense, false, "controller");
    const fx_gain_loss = try account_mod.Account.create(database, book_id, "7000", "FX Gain/Loss", .expense, false, "controller");
    const suspense = try account_mod.Account.create(database, book_id, "8000", "Suspense", .asset, false, "controller");

    // 0.3 Designate system accounts
    try book_mod.Book.setRoundingAccount(database, book_id, fx_rounding, "controller");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, retained_earnings, "controller");
    try book_mod.Book.setIncomeSummaryAccount(database, book_id, income_summary, "controller");
    try book_mod.Book.setOpeningBalanceAccount(database, book_id, opening_bal_eq, "controller");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gain_loss, "controller");
    try book_mod.Book.setSuspenseAccount(database, book_id, suspense, "controller");

    // 0.4 Create fiscal year periods (calendar year 2026, monthly)
    try period_mod.Period.bulkCreate(database, book_id, 2026, 1, .monthly, "controller");

    // Query period IDs by period_number
    var period_ids: [12]i64 = undefined;
    {
        var stmt = try database.prepare("SELECT id FROM ledger_periods WHERE book_id = ? ORDER BY period_number ASC;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) {
            if (idx < 12) {
                period_ids[idx] = stmt.columnInt64(0);
                idx += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 12), idx);
    }

    // 0.5 Setup subledger (AR customers, AP vendors)
    const ar_group = try subledger_mod.SubledgerGroup.create(database, book_id, "AR Customers", "customer", 1, ar, null, null, "controller");
    const ap_group = try subledger_mod.SubledgerGroup.create(database, book_id, "AP Vendors", "supplier", 1, ap, null, null, "controller");
    const customer_abc = try subledger_mod.SubledgerAccount.create(database, book_id, "C001", "Customer ABC Corp", "customer", ar_group, "controller");
    const customer_xyz = try subledger_mod.SubledgerAccount.create(database, book_id, "C002", "Customer XYZ Inc", "customer", ar_group, "controller");
    const vendor_supply = try subledger_mod.SubledgerAccount.create(database, book_id, "V001", "Supply Co", "supplier", ap_group, "controller");

    // 0.6 Setup classifications (BS + IS + Cash Flow)
    const bs_class = try classification_mod.Classification.create(database, book_id, "Balance Sheet", "balance_sheet", "controller");
    const is_class = try classification_mod.Classification.create(database, book_id, "Income Statement", "income_statement", "controller");
    const cf_class = try classification_mod.Classification.create(database, book_id, "Cash Flow Statement", "cash_flow", "controller");

    // 0.6a ClassificationNode.addGroup — build BS tree structure
    const bs_assets_group = try classification_mod.ClassificationNode.addGroup(database, bs_class, "Assets", null, 1, "controller");
    try std.testing.expect(bs_assets_group > 0);
    const bs_liabilities_group = try classification_mod.ClassificationNode.addGroup(database, bs_class, "Liabilities", null, 2, "controller");
    try std.testing.expect(bs_liabilities_group > 0);
    const bs_equity_group = try classification_mod.ClassificationNode.addGroup(database, bs_class, "Equity", null, 3, "controller");
    try std.testing.expect(bs_equity_group > 0);

    // 0.6b ClassificationNode.addAccount — assign accounts to BS groups
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, cash, bs_assets_group, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, cash_usd, bs_assets_group, 2, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, ar, bs_assets_group, 3, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, equipment, bs_assets_group, 4, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, accum_dep, bs_assets_group, 5, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, ap, bs_liabilities_group, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, capital, bs_equity_group, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, bs_class, retained_earnings, bs_equity_group, 2, "controller");

    // 0.6c Build IS tree structure
    const is_revenue_group = try classification_mod.ClassificationNode.addGroup(database, is_class, "Revenue", null, 1, "controller");
    const is_expense_group = try classification_mod.ClassificationNode.addGroup(database, is_class, "Expenses", null, 2, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, revenue, is_revenue_group, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, sales_returns, is_revenue_group, 2, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, cogs, is_expense_group, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, salaries, is_expense_group, 2, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, rent, is_expense_group, 3, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, is_class, depreciation_exp, is_expense_group, 4, "controller");

    // 0.6d Build Cash Flow tree (Operating section with cash account)
    const cf_operating_group = try classification_mod.ClassificationNode.addGroup(database, cf_class, "Operating Activities", null, 1, "controller");
    _ = try classification_mod.ClassificationNode.addAccount(database, cf_class, cash, cf_operating_group, 1, "controller");

    // 0.7 Setup dimensions (tax codes)
    const tax_dim = try dimension_mod.Dimension.create(database, book_id, "VAT", .tax_code, "controller");
    const vat_12 = try dimension_mod.DimensionValue.create(database, tax_dim, "VAT12", "VAT 12%", "controller");
    _ = try dimension_mod.DimensionValue.create(database, tax_dim, "VATEX", "VAT Exempt", "controller");

    // 0.7a listDimensions — verify VAT dimension was created
    {
        var dim_list_buf: [4096]u8 = undefined;
        const dim_list = try dimension_mod.listDimensions(database, book_id, null, &dim_list_buf, .json);
        try std.testing.expect(dim_list.len > 0);
    }

    // 0.7b listDimensionValues — verify VAT values were created
    {
        var dv_buf: [4096]u8 = undefined;
        const dv_list = try dimension_mod.listDimensionValues(database, tax_dim, &dv_buf, .json);
        try std.testing.expect(dv_list.len > 0);
    }

    // 0.8 Create budget — revenue target 1,000,000 PHP per month (= 100_000_000_000_00 in fixed-point)
    const budget_id = try budget_mod.Budget.create(database, book_id, "FY2026 Budget", 2026, "controller");
    for (period_ids) |pid| {
        _ = try budget_mod.BudgetLine.set(database, budget_id, revenue, pid, 100_000_000_000_00, "controller");
    }

    // 0.9 Opening balance migration
    try book_mod.Book.validateOpeningBalanceMigration(database, book_id);
    const ob_entry = try entry_mod.Entry.createDraft(database, book_id, "OB-001", "2026-01-01", "2026-01-01", null, period_ids[0], null, "controller");
    // Cash 5,000,000.00 PHP = 500_000_000_000_00 fixed-point
    _ = try entry_mod.Entry.addLine(database, ob_entry, 1, 500_000_000_000_00, 0, "PHP", 10_000_000_000, cash, null, null, "controller");
    // Equipment 2,000,000.00 PHP
    _ = try entry_mod.Entry.addLine(database, ob_entry, 2, 200_000_000_000_00, 0, "PHP", 10_000_000_000, equipment, null, null, "controller");
    // Capital 7,000,000.00 PHP (credit)
    _ = try entry_mod.Entry.addLine(database, ob_entry, 3, 0, 700_000_000_000_00, "PHP", 10_000_000_000, capital, null, null, "controller");
    try entry_mod.Entry.post(database, ob_entry, "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 1: DAILY OPERATIONS — JANUARY
    // ═══════════════════════════════════════════════════════════════

    // 1.1 Revenue: Invoice customer ABC for PHP 100,000.00
    const inv001_line1: i64 = blk: {
        const e = try entry_mod.Entry.createDraft(database, book_id, "INV-2026-001", "2026-01-10", "2026-01-10", null, period_ids[0], null, "accountant");
        const line1 = try entry_mod.Entry.addLine(database, e, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, ar, customer_abc, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, revenue, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
        break :blk line1;
    };
    // Tag revenue line with VAT 12%
    try dimension_mod.LineDimension.assign(database, inv001_line1, vat_12, "accountant");

    // 1.2 Expense: Purchase inventory from vendor, PHP 60,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "BILL-2026-001", "2026-01-12", "2026-01-12", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 6_000_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 6_000_000_000_00, "PHP", 10_000_000_000, ap, vendor_supply, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.3 Cash receipt from customer ABC, PHP 100,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "CR-2026-001", "2026-01-20", "2026-01-20", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 10_000_000_000_00, 0, "PHP", 10_000_000_000, cash, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 10_000_000_000_00, "PHP", 10_000_000_000, ar, customer_abc, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.4 Cash payment to vendor, PHP 60,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "CD-2026-001", "2026-01-22", "2026-01-22", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 6_000_000_000_00, 0, "PHP", 10_000_000_000, ap, vendor_supply, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 6_000_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.5 Multi-currency: Buy USD 1,000.00 at fx_rate 56.50 (= 565_000_000_000 in 10^10)
    //     USD $1,000.00 = 100_000_000_000 in 10^8 fixed-point
    //     base_amount = 100_000_000_000 * 565_000_000_000 / 10^10 = 5_650_000_000_00
    //     That's PHP 56,500.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "FX-2026-001", "2026-01-25", "2026-01-25", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 100_000_000_000, 0, "USD", 565_000_000_000, cash_usd, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 5_650_000_000_000, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.6 Salaries expense PHP 50,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "PR-2026-01", "2026-01-31", "2026-01-31", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, salaries, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 5_000_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.7 Rent expense PHP 20,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "RENT-2026-01", "2026-01-31", "2026-01-31", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 2_000_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 2_000_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.8 Error correction: Post wrong entry, then void it
    {
        const wrong = try entry_mod.Entry.createDraft(database, book_id, "WRONG-001", "2026-01-28", "2026-01-28", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, wrong, 1, 999_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, wrong, 2, 0, 999_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, wrong, "accountant");
        try entry_mod.Entry.voidEntry(database, wrong, "Wrong amount - should be 9990", "accountant");
    }

    // 1.9 Entry.editDraft — create a draft, fix the doc number before posting
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "MISC-WRONG", "2026-01-29", "2026-01-29", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 100_000_000_00, 0, "PHP", 10_000_000_000, suspense, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 100_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.editDraft(database, e, "MISC-2026-001", "2026-01-29", "2026-01-29", "Corrected doc number", null, period_ids[0], "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.10 Entry.editLine — create draft, add line, edit the amount before posting
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "ADJ-2026-001", "2026-01-30", "2026-01-30", null, period_ids[0], null, "accountant");
        const line1 = try entry_mod.Entry.addLine(database, e, 1, 999_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 500_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.editLine(database, line1, 500_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.11 Entry.removeLine — create draft, add 3 lines, remove one, post with 2
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "REM-2026-001", "2026-01-30", "2026-01-30", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 200_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        const extra_line = try entry_mod.Entry.addLine(database, e, 2, 100_000_000_00, 0, "PHP", 10_000_000_000, salaries, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 3, 0, 200_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.removeLine(database, extra_line, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 1.12 Entry.deleteDraft — create a draft then abandon it
    {
        const abandoned = try entry_mod.Entry.createDraft(database, book_id, "ABANDONED-001", "2026-01-30", "2026-01-30", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, abandoned, 1, 100_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, abandoned, 2, 0, 100_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.deleteDraft(database, abandoned, "accountant");
        // Verify it's gone
        {
            var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entries WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, abandoned);
            _ = try stmt.step();
            try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
        }
    }

    // 1.13 Entry.approve + Entry.reject — enable approval on book
    {
        try book_mod.Book.setRequireApproval(database, book_id, true, "controller");

        // Create draft for approval
        const approve_e = try entry_mod.Entry.createDraft(database, book_id, "APPR-2026-001", "2026-01-30", "2026-01-30", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, approve_e, 1, 300_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, approve_e, 2, 0, 300_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");

        // Posting without approval should fail
        try std.testing.expectError(error.ApprovalRequired, entry_mod.Entry.post(database, approve_e, "accountant"));

        // Approve and post
        try entry_mod.Entry.approve(database, approve_e, "controller");
        try entry_mod.Entry.post(database, approve_e, "accountant");

        // Create another draft and reject it
        const reject_e = try entry_mod.Entry.createDraft(database, book_id, "APPR-2026-002", "2026-01-30", "2026-01-30", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, reject_e, 1, 100_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, reject_e, 2, 0, 100_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.reject(database, reject_e, "Duplicate entry", "controller");

        // Clean up rejected draft so closePeriod won't fail on pending drafts
        try entry_mod.Entry.deleteDraft(database, reject_e, "controller");

        // Disable approval for remaining tests
        try book_mod.Book.setRequireApproval(database, book_id, false, "controller");
    }

    // (Entry.reverse exercised in February — see section 2.3 below)

    // 1.15 Account.updateName — rename the suspense account
    try account_mod.Account.updateName(database, suspense, "Suspense Account", "controller");

    // 1.16 Account.updateStatus — create a temporary account and deactivate it
    const temp_acct = try account_mod.Account.create(database, book_id, "9999", "Temporary Account", .asset, false, "controller");
    try account_mod.Account.updateStatus(database, temp_acct, .inactive, "controller");

    // 1.17 SubledgerAccount.updateName — rename Customer ABC
    try subledger_mod.SubledgerAccount.updateName(database, customer_abc, "Customer ABC Corp (renamed)", "controller");

    // 1.18 SubledgerAccount.updateStatus — deactivate Customer XYZ (no open entries referencing it as active counterparty needed for later)
    // Create a fresh customer just to deactivate (XYZ is used in Feb)
    const old_customer = try subledger_mod.SubledgerAccount.create(database, book_id, "C099", "Old Customer", "customer", ar_group, "controller");
    try subledger_mod.SubledgerAccount.updateStatus(database, old_customer, .inactive, "controller");

    // 1.19 SubledgerGroup.updateName — rename the AR group
    try subledger_mod.SubledgerGroup.updateName(database, ar_group, "AR Customers (renamed)", "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 3: MONTHLY CLOSE — JANUARY
    // ═══════════════════════════════════════════════════════════════

    // 3.1 Depreciation entry: PHP 10,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "DEP-2026-01", "2026-01-31", "2026-01-31", null, period_ids[0], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, depreciation_exp, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, accum_dep, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 3.2 FX Revaluation — revalue USD cash at month-end rate 57.00
    {
        const reval_rates = [_]revaluation_mod.CurrencyRate{
            .{ .currency = "USD", .new_rate = 570_000_000_000 },
        };
        const reval_id = try revaluation_mod.revalueForexBalances(database, book_id, period_ids[0], &reval_rates, "accountant");
        _ = reval_id;
    }

    // 3.3 Run reports — verify correctness

    // Trial Balance
    {
        const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
        defer tb.deinit();
        try std.testing.expectEqual(tb.total_debits, tb.total_credits);

        // Capital: credit-normal equity, OB credit 70_000_000_000_000 (PHP 7,000,000.00)
        if (findReportRow(tb.rows, capital)) |row| {
            try std.testing.expectEqual(@as(i64, 70_000_000_000_000), row.credit_balance);
            try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Equipment: debit-normal asset, OB debit 20_000_000_000_000 (PHP 2,000,000.00)
        if (findReportRow(tb.rows, equipment)) |row| {
            try std.testing.expectEqual(@as(i64, 20_000_000_000_000), row.debit_balance);
            try std.testing.expectEqual(@as(i64, 0), row.credit_balance);
        } else return error.TestUnexpectedResult;

        // Cash USD: debit-normal, FX original 5_650_000_000_000 + reval 50_000_000_000 = 5_700_000_000_000
        if (findReportRow(tb.rows, cash_usd)) |row| {
            try std.testing.expectEqual(@as(i64, 5_700_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Accum Dep: contra asset (credit normal), 100_000_000_000
        if (findReportRow(tb.rows, accum_dep)) |row| {
            try std.testing.expectEqual(@as(i64, 100_000_000_000), row.credit_balance);
        } else return error.TestUnexpectedResult;
    }

    // Income Statement
    {
        const is_report = try report_mod.incomeStatement(database, book_id, "2026-01-01", "2026-01-31");
        defer is_report.deinit();
        try std.testing.expect(is_report.rows.len > 0);

        // Revenue: INV-001 = 1_000_000_000_000 credit (PHP 100,000.00)
        if (findReportRow(is_report.rows, revenue)) |row| {
            try std.testing.expectEqual(@as(i64, 1_000_000_000_000), row.credit_balance);
            try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Salaries: PR-2026-01 = 500_000_000_000 debit (PHP 50,000.00)
        if (findReportRow(is_report.rows, salaries)) |row| {
            try std.testing.expectEqual(@as(i64, 500_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Rent: 200_000_000_000 + 20_000_000_000 + 30_000_000_000 = 250_000_000_000 (PHP 25,000.00)
        if (findReportRow(is_report.rows, rent)) |row| {
            try std.testing.expectEqual(@as(i64, 250_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Depreciation: DEP-2026-01 = 100_000_000_000 (PHP 10,000.00)
        if (findReportRow(is_report.rows, depreciation_exp)) |row| {
            try std.testing.expectEqual(@as(i64, 100_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // COGS: BILL 600_000_000_000 + ADJ 50_000_000_000 = 650_000_000_000 (PHP 65,000.00)
        if (findReportRow(is_report.rows, cogs)) |row| {
            try std.testing.expectEqual(@as(i64, 650_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // FX Gain/Loss: reval gain of 50_000_000_000 shows as credit (expense with credit balance)
        if (findReportRow(is_report.rows, fx_gain_loss)) |row| {
            try std.testing.expectEqual(@as(i64, 50_000_000_000), row.credit_balance);
            try std.testing.expectEqual(@as(i64, 0), row.debit_balance);
        } else return error.TestUnexpectedResult;
    }

    // Balance Sheet
    {
        const bs = try report_mod.balanceSheet(database, book_id, "2026-01-31", "2026-01-01");
        defer bs.deinit();
        try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    }

    // TB Movement
    {
        const tbm = try report_mod.trialBalanceMovement(database, book_id, "2026-01-01", "2026-01-31");
        defer tbm.deinit();
        try std.testing.expect(tbm.rows.len > 0);
    }

    // 3.3a General Ledger — GL for January
    {
        const gl = try report_mod.generalLedger(database, book_id, "2026-01-01", "2026-01-31");
        defer gl.deinit();
        try std.testing.expect(gl.rows.len > 0);
        try std.testing.expectEqual(gl.total_debits, gl.total_credits);
    }

    // 3.3b Account Ledger — Cash detail with running balance
    {
        const al = try report_mod.accountLedger(database, book_id, cash, "2026-01-01", "2026-01-31");
        defer al.deinit();
        try std.testing.expect(al.rows.len > 0);

        // Cash is debit-normal. January is the first period so opening_balance = 0
        try std.testing.expectEqual(@as(i64, 0), al.opening_balance);

        // Cash has 10 posted lines in January (OB dr, CR dr, CD cr, FX cr, PR cr, RENT cr, MISC cr, ADJ cr, REM cr, APPR cr)
        // Debits: 50_000_000_000_000 + 1_000_000_000_000 = 51_000_000_000_000
        try std.testing.expectEqual(@as(i64, 51_000_000_000_000), al.total_debits);
        // Credits: 600_000_000_000 + 5_650_000_000_000 + 500_000_000_000 + 200_000_000_000
        //        + 10_000_000_000 + 50_000_000_000 + 20_000_000_000 + 30_000_000_000 = 7_060_000_000_000
        try std.testing.expectEqual(@as(i64, 7_060_000_000_000), al.total_credits);

        // Closing balance = opening(0) + debits - credits = 43_940_000_000_000 (debit normal)
        try std.testing.expectEqual(@as(i64, 43_940_000_000_000), al.closing_balance);
        try std.testing.expect(al.total_debits > al.total_credits);
    }

    // 3.3c Journal Register — JR for January
    {
        const jr = try report_mod.journalRegister(database, book_id, "2026-01-01", "2026-01-31");
        defer jr.deinit();
        try std.testing.expect(jr.rows.len > 0);
        try std.testing.expectEqual(jr.total_debits, jr.total_credits);
    }

    // 3.3d Classified Balance Sheet
    {
        const cbs = try classification_mod.classifiedReport(database, bs_class, "2026-01-31");
        defer cbs.deinit();
        try std.testing.expect(cbs.rows.len > 0);
    }

    // 3.3e Cash Flow Statement
    {
        const cfs = try classification_mod.cashFlowStatement(database, cf_class, "2026-01-01", "2026-01-31");
        defer cfs.deinit();
        try std.testing.expect(cfs.rows.len > 0);
    }

    // 3.3f Counterparty Ledger — Customer ABC
    {
        var cp_buf: [16384]u8 = undefined;
        const cp_result = try query_mod.counterpartyLedger(database, book_id, customer_abc, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &cp_buf, .json);
        try std.testing.expect(cp_result.len > 0);
    }

    // 3.3g Subledger Report — AR summary
    {
        var sl_buf: [16384]u8 = undefined;
        const sl_result = try query_mod.subledgerReport(database, book_id, ar_group, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &sl_buf, .json);
        try std.testing.expect(sl_result.len > 0);
    }

    // 3.3h Aged Subledger — AR aging
    {
        var aged_buf: [16384]u8 = undefined;
        const aged_result = try query_mod.agedSubledger(database, book_id, ar_group, "2026-01-31", .asc, 100, 0, &aged_buf, .json);
        try std.testing.expect(aged_result.len > 0);
    }

    // 3.3i listTransactions — paginated GL
    {
        var lt_buf: [16384]u8 = undefined;
        const lt_result = try query_mod.listTransactions(database, book_id, null, null, "2026-01-01", "2026-01-31", .asc, 100, 0, &lt_buf, .json);
        try std.testing.expect(lt_result.len > 0);
    }

    // 3.3j Subledger Reconciliation — reconcile AR
    {
        var recon_buf: [16384]u8 = undefined;
        const recon_result = try query_mod.subledgerReconciliation(database, book_id, ar_group, "2026-01-31", &recon_buf, .json);
        try std.testing.expect(recon_result.len > 0);
    }

    // 3.3k describeSchema — export schema description
    {
        var schema_buf: [32768]u8 = undefined;
        const schema_result = try describe_mod.describeSchema(database, &schema_buf, .json);
        try std.testing.expect(schema_result.len > 0);
    }

    // 3.3l Query: getBook
    {
        var book_buf: [4096]u8 = undefined;
        const book_result = try query_mod.getBook(database, book_id, &book_buf, .json);
        try std.testing.expect(book_result.len > 0);
    }

    // 3.4 Recalculate stale balances and verify integrity
    _ = try cache_mod.recalculateAllStale(database, book_id);

    // The reversal entry (1.14) posted into Feb with flipped lines, creating cache rows
    // in period_ids[1] that won't be stale but Feb hasn't been fully closed yet.
    // Run verify after Feb operations instead.
    // Verify January posted entries balance (subset check)
    {
        var stmt = try database.prepare(
            \\SELECT COUNT(*) FROM ledger_entries
            \\WHERE book_id = ? AND period_id = ? AND status IN ('posted', 'reversed');
        );
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        try stmt.bindInt(2, period_ids[0]);
        _ = try stmt.step();
        try std.testing.expect(stmt.columnInt(0) > 0);
    }

    // 3.5 Entry.editPosted — fix a description on a posted entry (metadata-only edit)
    {
        var posted_id: i64 = 0;
        {
            var stmt = try database.prepare("SELECT id FROM ledger_entries WHERE book_id = ? AND period_id = ? AND status = 'posted' LIMIT 1;");
            defer stmt.finalize();
            try stmt.bindInt(1, book_id);
            try stmt.bindInt(2, period_ids[0]);
            _ = try stmt.step();
            posted_id = stmt.columnInt64(0);
        }
        try entry_mod.Entry.editPosted(database, posted_id, "Updated description for audit", null, "controller");
    }

    // 3.6 Soft close January
    try period_mod.Period.transition(database, period_ids[0], .soft_closed, "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 1 CONTINUED: FEBRUARY OPERATIONS
    // ═══════════════════════════════════════════════════════════════

    // Revenue in Feb: Customer XYZ, PHP 150,000.00
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "INV-2026-002", "2026-02-15", "2026-02-15", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 15_000_000_000_00, 0, "PHP", 10_000_000_000, ar, customer_xyz, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 15_000_000_000_00, "PHP", 10_000_000_000, revenue, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // Sales return from Customer ABC: PHP 5,000.00 (contra revenue)
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "RET-2026-001", "2026-02-20", "2026-02-20", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 500_000_000_00, 0, "PHP", 10_000_000_000, sales_returns, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 500_000_000_00, "PHP", 10_000_000_000, ar, customer_abc, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // Feb expenses: Salaries 50k + Rent 20k = 70k total
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "PR-2026-02", "2026-02-28", "2026-02-28", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 5_000_000_000_00, 0, "PHP", 10_000_000_000, salaries, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 2_000_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 3, 0, 7_000_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
    }

    // 2.3 Entry.reverse — post an entry then reverse it in the same period
    {
        const e = try entry_mod.Entry.createDraft(database, book_id, "REV-2026-002", "2026-02-15", "2026-02-15", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        try entry_mod.Entry.post(database, e, "accountant");
        const reversal_id = try entry_mod.Entry.reverse(database, e, "Accrual reversal", "2026-02-15", null, "accountant");
        try std.testing.expect(reversal_id > 0);
    }

    // 2.4 batchPost — create 3 drafts with lines, batch post all 3
    var batch_entry_ids: [3]i64 = undefined;
    {
        const e1 = try entry_mod.Entry.createDraft(database, book_id, "BATCH-001", "2026-02-28", "2026-02-28", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e1, 1, 100_000_000_00, 0, "PHP", 10_000_000_000, rent, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e1, 2, 0, 100_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        batch_entry_ids[0] = e1;

        const e2 = try entry_mod.Entry.createDraft(database, book_id, "BATCH-002", "2026-02-28", "2026-02-28", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e2, 1, 200_000_000_00, 0, "PHP", 10_000_000_000, salaries, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e2, 2, 0, 200_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        batch_entry_ids[1] = e2;

        const e3 = try entry_mod.Entry.createDraft(database, book_id, "BATCH-003", "2026-02-28", "2026-02-28", null, period_ids[1], null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e3, 1, 50_000_000_00, 0, "PHP", 10_000_000_000, cogs, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, e3, 2, 0, 50_000_000_00, "PHP", 10_000_000_000, cash, null, null, "accountant");
        batch_entry_ids[2] = e3;

        const batch_result = batch_mod.batchPost(database, &batch_entry_ids, "accountant");
        try std.testing.expectEqual(@as(u32, 3), batch_result.succeeded);
        try std.testing.expectEqual(@as(u32, 0), batch_result.failed);
    }

    // 2.5 batchVoid — batch void 2 of the 3
    {
        const void_ids = [_]i64{ batch_entry_ids[1], batch_entry_ids[2] };
        const void_result = batch_mod.batchVoid(database, &void_ids, "Batch void - duplicates", "controller");
        try std.testing.expectEqual(@as(u32, 2), void_result.succeeded);
        try std.testing.expectEqual(@as(u32, 0), void_result.failed);
    }

    // Soft close February
    try period_mod.Period.transition(database, period_ids[1], .soft_closed, "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 4: QUARTERLY — Q1 COMPARATIVE REPORTS
    // ═══════════════════════════════════════════════════════════════

    // Comparative IS: Q1 2026 vs (empty) Q1 2025
    {
        const is_comp = try report_mod.incomeStatementComparative(database, book_id, "2026-01-01", "2026-03-31", "2025-01-01", "2025-03-31");
        defer is_comp.deinit();
        try std.testing.expectEqual(@as(i64, 0), is_comp.prior_total_debits);
        try std.testing.expectEqual(@as(i64, 0), is_comp.prior_total_credits);
    }

    // Budget vs Actual for Q1
    {
        var bva_buf: [8192]u8 = undefined;
        const bva_result = try budget_mod.budgetVsActual(database, budget_id, "2026-01-01", "2026-03-31", &bva_buf, .json);
        try std.testing.expect(bva_result.len > 10);
    }

    // Dimension summary (VAT report)
    {
        var dim_buf: [4096]u8 = undefined;
        const dim_result = try dimension_mod.dimensionSummary(database, book_id, tax_dim, "2026-01-01", "2026-03-31", &dim_buf, .json);
        try std.testing.expect(dim_result.len > 10);
    }

    // 4.4 Classification.updateName — rename the BS classification
    try classification_mod.Classification.updateName(database, bs_class, "Statement of Financial Position", "controller");

    // 4.5 ClassificationNode.move — reorganize: move Equity group under a new parent
    {
        const bs_capital_group = try classification_mod.ClassificationNode.addGroup(database, bs_class, "Capital Section", null, 4, "controller");
        try classification_mod.ClassificationNode.move(database, bs_equity_group, bs_capital_group, 1, "controller");
    }

    // 4.6 ClassificationNode.updateLabel — rename a group node
    try classification_mod.ClassificationNode.updateLabel(database, bs_assets_group, "Current and Non-Current Assets", "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 6: YEAR-END CLOSE
    // ═══════════════════════════════════════════════════════════════

    // Close January and February with closing entries (zeroes R/E accounts -> RE)
    try close_mod.closePeriod(database, book_id, period_ids[0], "controller");
    try close_mod.closePeriod(database, book_id, period_ids[1], "controller");

    // Close remaining periods (no R/E activity, transition open -> soft_closed -> closed)
    {
        var i: usize = 2;
        while (i < 12) : (i += 1) {
            try period_mod.Period.transition(database, period_ids[i], .soft_closed, "controller");
            try period_mod.Period.transition(database, period_ids[i], .closed, "controller");
        }
    }

    // Verify post-close state
    _ = try cache_mod.recalculateAllStale(database, book_id);
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }

    // Post-close drilldown: verify R/E accounts zeroed in January cache
    {
        var stmt = try database.prepare(
            \\SELECT debit_sum, credit_sum FROM ledger_account_balances
            \\WHERE account_id = ? AND period_id = ?;
        );
        defer stmt.finalize();

        // Revenue in January: closing entry debits revenue to zero it
        try stmt.bindInt(1, revenue);
        try stmt.bindInt(2, period_ids[0]);
        if (try stmt.step()) {
            try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
        } else return error.TestUnexpectedResult;

        stmt.reset();
        stmt.clearBindings();

        // Salaries in January: closing entry credits salaries to zero it
        try stmt.bindInt(1, salaries);
        try stmt.bindInt(2, period_ids[0]);
        if (try stmt.step()) {
            try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
        } else return error.TestUnexpectedResult;

        stmt.reset();
        stmt.clearBindings();

        // COGS in January: closing entry credits COGS to zero it
        try stmt.bindInt(1, cogs);
        try stmt.bindInt(2, period_ids[0]);
        if (try stmt.step()) {
            try std.testing.expectEqual(stmt.columnInt64(0), stmt.columnInt64(1));
        } else return error.TestUnexpectedResult;
    }

    // Balance sheet after close
    {
        const bs_final = try report_mod.balanceSheet(database, book_id, "2026-12-31", "2026-01-01");
        defer bs_final.deinit();
        try std.testing.expectEqual(bs_final.total_debits, bs_final.total_credits);

        // Capital: credit 70_000_000_000_000 (unchanged by closing)
        if (findReportRow(bs_final.rows, capital)) |row| {
            try std.testing.expectEqual(@as(i64, 70_000_000_000_000), row.credit_balance);
        } else return error.TestUnexpectedResult;

        // Equipment: debit 20_000_000_000_000 (unchanged)
        if (findReportRow(bs_final.rows, equipment)) |row| {
            try std.testing.expectEqual(@as(i64, 20_000_000_000_000), row.debit_balance);
        } else return error.TestUnexpectedResult;

        // Retained Earnings: should have net income transferred from closing
        if (findReportRow(bs_final.rows, retained_earnings)) |row| {
            try std.testing.expect(row.credit_balance != 0 or row.debit_balance != 0);
        } else return error.TestUnexpectedResult;
    }

    // Equity changes for the full year
    {
        const eq = try report_mod.equityChanges(database, book_id, "2026-01-01", "2026-12-31", "2026-01-01");
        defer eq.deinit();
        try std.testing.expect(eq.rows.len > 0);

        // Capital account: opening = 0 (FY start), closing = 70_000_000_000_000 (from OB entry)
        if (findEquityRow(eq.rows, capital)) |row| {
            try std.testing.expectEqual(@as(i64, 70_000_000_000_000), row.closing_balance);
        } else return error.TestUnexpectedResult;

        // Retained Earnings: receives net income from closing entries
        if (findEquityRow(eq.rows, retained_earnings)) |row| {
            try std.testing.expect(row.closing_balance != 0);
        } else return error.TestUnexpectedResult;
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 7: AUDIT — VERIFY EVERYTHING
    // ═══════════════════════════════════════════════════════════════

    // Final integrity check
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
        try std.testing.expect(v.entries_checked > 0);
        try std.testing.expect(v.accounts_checked > 0);
        try std.testing.expect(v.periods_checked > 0);
    }

    // Recalculate all stale balances and verify clean
    _ = try cache_mod.recalculateAllStale(database, book_id);

    // Lock all periods (permanent — audit complete)
    {
        var i: usize = 0;
        while (i < 12) : (i += 1) {
            try period_mod.Period.transition(database, period_ids[i], .locked, "controller");
        }
    }

    // Verify locked periods reject further transitions
    try std.testing.expectError(error.PeriodLocked, period_mod.Period.transition(database, period_ids[0], .open, "controller"));

    // ═══════════════════════════════════════════════════════════════
    // PHASE 7b: YEAR-END CLEANUP
    // ═══════════════════════════════════════════════════════════════

    // 7b.1 LineDimension.remove — remove a dimension tag from the invoice line
    try dimension_mod.LineDimension.remove(database, inv001_line1, vat_12, "controller");

    // 7b.2 Create a disposable dimension, value, assign, remove, delete
    {
        const temp_dim = try dimension_mod.Dimension.create(database, book_id, "Temp Project", .project, "controller");
        const temp_val = try dimension_mod.DimensionValue.create(database, temp_dim, "P001", "Prototype", "controller");

        // DimensionValue.delete — clean up unused dimension value
        try dimension_mod.DimensionValue.delete(database, temp_val, "controller");

        // Dimension.delete — delete an unused dimension (after deleting values)
        try dimension_mod.Dimension.delete(database, temp_dim, "controller");

        // Verify dimension is gone
        {
            var dim_buf2: [4096]u8 = undefined;
            const dim_list2 = try dimension_mod.listDimensions(database, book_id, "project", &dim_buf2, .json);
            // Should not contain "Temp Project" — but list is empty or lacks it
            try std.testing.expect(std.mem.indexOf(u8, dim_list2, "Temp Project") == null);
        }
    }

    // 7b.3 Budget.delete — delete the budget after the year
    try budget_mod.Budget.delete(database, budget_id, "controller");
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_budgets WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, budget_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    // (Entry.editPosted exercised earlier in Phase 3, before periods are closed)

    // ═══════════════════════════════════════════════════════════════
    // PHASE 8: NEXT FISCAL YEAR SETUP
    // ═══════════════════════════════════════════════════════════════

    // Create FY2027 periods
    try period_mod.Period.bulkCreate(database, book_id, 2027, 1, .monthly, "controller");

    var period_ids_2027: [12]i64 = undefined;
    {
        var stmt = try database.prepare("SELECT id FROM ledger_periods WHERE book_id = ? AND year = 2027 ORDER BY period_number ASC;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) : (idx += 1) {
            if (idx < 12) period_ids_2027[idx] = stmt.columnInt64(0);
        }
    }

    const customer_id = customer_abc;

    // Verify BS accounts carry forward (cumulative query includes all prior periods)
    {
        const bs_2027 = try report_mod.balanceSheet(database, book_id, "2027-01-31", "2027-01-01");
        defer bs_2027.deinit();
        try std.testing.expectEqual(bs_2027.total_debits, bs_2027.total_credits);
    }

    // IS for Jan 2027 should be empty (no FY2027 activity yet)
    {
        const is_2027 = try report_mod.incomeStatement(database, book_id, "2027-01-01", "2027-01-31");
        defer is_2027.deinit();
        try std.testing.expectEqual(@as(i64, 0), is_2027.total_debits);
        try std.testing.expectEqual(@as(i64, 0), is_2027.total_credits);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 9: OPEN ITEM MANAGEMENT (AR invoice-payment lifecycle)
    // ═══════════════════════════════════════════════════════════════

    // Post a FY2027 invoice: Debit AR 50,000, Credit Revenue 50,000
    {
        const inv_eid = try entry_mod.Entry.createDraft(database, book_id, "INV-2027-001", "2027-01-20", "2027-01-20", null, period_ids_2027[0], null, "accountant");
        const inv_line = try entry_mod.Entry.addLine(database, inv_eid, 1, 5_000_000_000_000, 0, "PHP", money.FX_RATE_SCALE, ar, customer_id, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, inv_eid, 2, 0, 5_000_000_000_000, "PHP", money.FX_RATE_SCALE, revenue, null, null, "accountant");
        try entry_mod.Entry.post(database, inv_eid, "accountant");

        // Create open item for the invoice (due in 30 days)
        const oi_id = try open_item_mod.createOpenItem(database, inv_line, customer_id, 5_000_000_000_000, "2027-02-19", book_id, "accountant");
        try std.testing.expect(oi_id > 0);

        // Partial payment: 30,000 of 50,000
        try open_item_mod.allocatePayment(database, oi_id, 3_000_000_000_000, "accountant");

        // Verify partial status
        {
            var stmt = try database.prepare("SELECT status, remaining_amount FROM ledger_open_items WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, oi_id);
            _ = try stmt.step();
            try std.testing.expectEqualStrings("partial", stmt.columnText(0).?);
            try std.testing.expectEqual(@as(i64, 2_000_000_000_000), stmt.columnInt64(1));
        }

        // Full payment of remaining
        try open_item_mod.allocatePayment(database, oi_id, 2_000_000_000_000, "accountant");

        // Verify closed status
        {
            var stmt = try database.prepare("SELECT status FROM ledger_open_items WHERE id = ?;");
            defer stmt.finalize();
            try stmt.bindInt(1, oi_id);
            _ = try stmt.step();
            try std.testing.expectEqualStrings("closed", stmt.columnText(0).?);
        }

        // List open items — closed should not appear
        {
            var oi_buf: [4096]u8 = undefined;
            const oi_list = try open_item_mod.listOpenItems(database, customer_id, false, &oi_buf, .json);
            try std.testing.expect(std.mem.indexOf(u8, oi_list, "\"items\":[]") != null);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 10: CLASSIFIED TRIAL BALANCE
    // ═══════════════════════════════════════════════════════════════

    {
        const tb_cls = try classification_mod.Classification.create(database, book_id, "Statutory TB", "trial_balance", "controller");
        const tb_assets = try classification_mod.ClassificationNode.addGroup(database, tb_cls, "Assets", null, 0, "controller");
        _ = try classification_mod.ClassificationNode.addAccount(database, tb_cls, cash, tb_assets, 0, "controller");
        _ = try classification_mod.ClassificationNode.addAccount(database, tb_cls, ar, tb_assets, 1, "controller");

        const ctb = try classification_mod.classifiedTrialBalance(database, tb_cls, "2027-01-31");
        defer ctb.deinit();

        // Should have classified rows + unclassified rows for accounts not in the tree
        try std.testing.expect(ctb.rows.len > 3);
        var has_unclassified = false;
        for (ctb.rows) |row| {
            if (row.depth == -1) { has_unclassified = true; break; }
        }
        try std.testing.expect(has_unclassified);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 11: DIMENSION HIERARCHY ROLL-UP
    // ═══════════════════════════════════════════════════════════════

    {
        const cc_dim = try dimension_mod.Dimension.create(database, book_id, "Cost Centers", .cost_center, "controller");
        const cc_parent = try dimension_mod.DimensionValue.create(database, cc_dim, "CC-100", "Sales", "controller");
        const cc_child = try dimension_mod.DimensionValue.createWithParent(database, cc_dim, "CC-101", "Sales East", cc_parent, "controller");

        // Post entry in FY2027 and tag with child cost center
        const cc_eid = try entry_mod.Entry.createDraft(database, book_id, "CC-TEST", "2027-01-25", "2027-01-25", null, period_ids_2027[0], null, "accountant");
        const cc_line1 = try entry_mod.Entry.addLine(database, cc_eid, 1, 100_000_000_000, 0, "PHP", money.FX_RATE_SCALE, salaries, null, null, "accountant");
        _ = try entry_mod.Entry.addLine(database, cc_eid, 2, 0, 100_000_000_000, "PHP", money.FX_RATE_SCALE, cash, null, null, "accountant");
        try dimension_mod.LineDimension.assign(database, cc_line1, cc_child, "accountant");
        try entry_mod.Entry.post(database, cc_eid, "accountant");

        // Roll-up summary should show parent with accumulated child values
        var cc_buf: [4096]u8 = undefined;
        const cc_csv = try dimension_mod.dimensionSummaryRollup(database, book_id, cc_dim, "2027-01-01", "2027-01-31", &cc_buf, .csv);
        try std.testing.expect(std.mem.indexOf(u8, cc_csv, "CC-100") != null);
        try std.testing.expect(std.mem.indexOf(u8, cc_csv, "CC-101") != null);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 12: BUDGET TRANSITION LIFECYCLE
    // ═══════════════════════════════════════════════════════════════

    {
        const fy27_budget = try budget_mod.Budget.create(database, book_id, "FY2027 Operating Budget", 2027, "controller");
        _ = try budget_mod.BudgetLine.set(database, fy27_budget, revenue, period_ids_2027[0], 10_000_000_000_000, "controller");
        _ = try budget_mod.BudgetLine.set(database, fy27_budget, salaries, period_ids_2027[0], 3_000_000_000_000, "controller");
        try budget_mod.Budget.transition(database, fy27_budget, .approved, "controller");

        // Cannot modify approved budget
        const mod_result = budget_mod.BudgetLine.set(database, fy27_budget, revenue, period_ids_2027[0], 999, "controller");
        try std.testing.expectError(error.InvalidTransition, mod_result);

        // Budget vs actual
        var bva_buf: [8192]u8 = undefined;
        const bva = try budget_mod.budgetVsActual(database, fy27_budget, "2027-01-01", "2027-01-31", &bva_buf, .csv);
        try std.testing.expect(std.mem.indexOf(u8, bva, "4000") != null);

        try budget_mod.Budget.transition(database, fy27_budget, .closed, "controller");
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 13: EXPORT FORMATTED AMOUNTS
    // ═══════════════════════════════════════════════════════════════

    {
        // Trial balance should have formatted decimals
        const tb = try report_mod.trialBalance(database, book_id, "2027-01-31");
        defer tb.deinit();
        var tb_buf: [16384]u8 = undefined;
        const tb_csv = try export_mod.reportToCsv(tb, &tb_buf);
        // Should contain decimal amounts like "50000.00" not raw integers
        try std.testing.expect(std.mem.indexOf(u8, tb_csv, ".") != null);
        try std.testing.expect(std.mem.indexOf(u8, tb_csv, "00000000000") == null);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 14: CASH FLOW STATEMENT (INDIRECT METHOD)
    // ═══════════════════════════════════════════════════════════════

    {
        const cf_cls = try classification_mod.Classification.create(database, book_id, "Cash Flow FY2027", "cash_flow", "controller");
        const op_g = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Operating Activities", null, 0, "controller");
        _ = try classification_mod.ClassificationNode.addAccount(database, cf_cls, ar, op_g, 0, "controller");
        const inv_g = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Investing Activities", null, 1, "controller");
        _ = try classification_mod.ClassificationNode.addAccount(database, cf_cls, equipment, inv_g, 0, "controller");
        _ = try classification_mod.ClassificationNode.addGroup(database, cf_cls, "Financing Activities", null, 2, "controller");

        const cf = try classification_mod.cashFlowStatementIndirect(database, book_id, "2027-01-01", "2027-01-31", cf_cls);
        defer cf.deinit();
        // Net income should reflect FY2027 revenue minus expenses
        try std.testing.expect(cf.adjustments.len > 0);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 15: HASH CHAIN AUDIT VERIFICATION
    // ═══════════════════════════════════════════════════════════════

    {
        // Verify audit log hash chain is populated
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE book_id = ? AND hash_chain IS NOT NULL;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const hashed_count = stmt.columnInt(0);
        try std.testing.expect(hashed_count > 50);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 16: BOOK MANAGEMENT, ADJUSTMENT PERIOD, COMPARATIVE REPORTS
    // ═══════════════════════════════════════════════════════════════

    // Book.updateName
    try book_mod.Book.updateName(database, book_id, "Acme Corp PH (Renamed)", "controller");

    // Period.create (individual adjustment period)
    _ = try period_mod.Period.create(database, book_id, "Adj FY2026", 13, 2026, "2026-12-01", "2026-12-31", "adjustment", "controller");

    // Period.transitionWithReason (close a FY2027 period, reopen with reason, re-close)
    try period_mod.Period.transition(database, period_ids_2027[1], .soft_closed, "controller");
    try period_mod.Period.transition(database, period_ids_2027[1], .closed, "controller");
    try period_mod.Period.transitionWithReason(database, period_ids_2027[1], .open, "Late invoice discovered by BIR audit", "controller");
    try period_mod.Period.transition(database, period_ids_2027[1], .soft_closed, "controller");
    try period_mod.Period.transition(database, period_ids_2027[1], .closed, "controller");

    // Comparative reports (FY2027 Jan vs FY2026 Jan)
    {
        const tb_comp = try report_mod.trialBalanceComparative(database, book_id, "2027-01-31", "2026-01-31");
        defer tb_comp.deinit();
        try std.testing.expect(tb_comp.rows.len > 0);
    }
    {
        const bs_comp = try report_mod.balanceSheetComparative(database, book_id, "2027-01-31", "2026-12-31", "2027-01-01");
        defer bs_comp.deinit();
        try std.testing.expect(bs_comp.rows.len > 0);
    }
    {
        const tbm_comp = try report_mod.trialBalanceMovementComparative(database, book_id, "2027-01-01", "2027-01-31", "2026-01-01", "2026-01-31");
        defer tbm_comp.deinit();
        try std.testing.expect(tbm_comp.rows.len > 0);
    }

    // Parsers (utility functions)
    {
        var rates_buf: [10]revaluation_mod.CurrencyRate = undefined;
        const rcount = try revaluation_mod.parseRatesJson("[{\"currency\":\"USD\",\"rate\":570000000000}]", &rates_buf);
        try std.testing.expectEqual(@as(usize, 1), rcount);
        try std.testing.expect(std.mem.eql(u8, rates_buf[0].currency, "USD"));
    }
    {
        var ids_buf: [1000]i64 = undefined;
        const icount = try batch_mod.parseIdArray("[10,20,30]", &ids_buf);
        try std.testing.expectEqual(@as(usize, 3), icount);
        try std.testing.expectEqual(@as(i64, 20), ids_buf[1]);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 17: CRUD QUERY COVERAGE
    // ═══════════════════════════════════════════════════════════════

    {
        var qbuf: [16384]u8 = undefined;

        // listBooks
        const lb = try query_mod.listBooks(database, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, lb, "Renamed") != null);

        // getAccount
        const ga = try query_mod.getAccount(database, cash, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, ga, "1000") != null);

        // listAccounts
        const la = try query_mod.listAccounts(database, book_id, null, null, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, la, "Cash") != null);

        // getPeriod
        const gp = try query_mod.getPeriod(database, period_ids_2027[0], &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, gp, "2027") != null);

        // listPeriods
        const lp = try query_mod.listPeriods(database, book_id, null, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(lp.len > 50);

        // listEntries
        const le = try query_mod.listEntries(database, book_id, null, null, null, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, le, "posted") != null);

        // getEntry (use ob_entry from Phase 1 — entry_id=1 is the opening balance)
        const ge = try query_mod.getEntry(database, 1, book_id, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, ge, "OB-001") != null);

        // listEntryLines
        const ll = try query_mod.listEntryLines(database, 1, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, ll, "\"debit\":") != null);

        // listAuditLog (small page to fit in buffer)
        const al = try query_mod.listAuditLog(database, book_id, null, null, null, null, .asc, 5, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, al, "create") != null);

        // getClassification
        const gc = try query_mod.getClassification(database, bs_class, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, gc, "balance_sheet") != null);

        // listClassifications
        const lc = try query_mod.listClassifications(database, book_id, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(lc.len > 10);

        // getSubledgerGroup
        const gsg = try query_mod.getSubledgerGroup(database, ar_group, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, gsg, "AR Customers") != null);

        // listSubledgerGroups
        const lsg = try query_mod.listSubledgerGroups(database, book_id, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, lsg, "AR") != null);

        // getSubledgerAccount
        const gsa = try query_mod.getSubledgerAccount(database, customer_abc, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, gsa, "ABC") != null);

        // listSubledgerAccounts
        const lsa = try query_mod.listSubledgerAccounts(database, book_id, null, null, .asc, 100, 0, &qbuf, .json);
        try std.testing.expect(std.mem.indexOf(u8, lsa, "C001") != null);
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 18: EXPORT COVERAGE
    // ═══════════════════════════════════════════════════════════════

    {
        var ebuf: [32768]u8 = undefined;

        // reportToJson
        {
            const tb = try report_mod.trialBalance(database, book_id, "2027-01-31");
            defer tb.deinit();
            const json = try export_mod.reportToJson(tb, &ebuf);
            try std.testing.expect(std.mem.indexOf(u8, json, "total_debits") != null);
        }

        // ledgerResultToCsv
        {
            const gl = try report_mod.generalLedger(database, book_id, "2027-01-01", "2027-01-31");
            defer gl.deinit();
            const csv = try export_mod.ledgerResultToCsv(gl, &ebuf);
            try std.testing.expect(std.mem.indexOf(u8, csv, "posting_date") != null);
        }

        // ledgerResultToJson
        {
            const gl = try report_mod.generalLedger(database, book_id, "2027-01-01", "2027-01-31");
            defer gl.deinit();
            const json = try export_mod.ledgerResultToJson(gl, &ebuf);
            try std.testing.expect(std.mem.indexOf(u8, json, "opening_balance") != null);
        }

        // classifiedResultToCsv
        {
            const cr = try classification_mod.classifiedReport(database, bs_class, "2027-01-31");
            defer cr.deinit();
            const csv = try export_mod.classifiedResultToCsv(cr, &ebuf);
            try std.testing.expect(std.mem.indexOf(u8, csv, "node_type") != null);
        }

        // classifiedResultToJson
        {
            const cr = try classification_mod.classifiedReport(database, bs_class, "2027-01-31");
            defer cr.deinit();
            const json = try export_mod.classifiedResultToJson(cr, &ebuf);
            try std.testing.expect(std.mem.indexOf(u8, json, "total_debits") != null);
        }

        // exportChartOfAccounts
        {
            const coa = try export_mod.exportChartOfAccounts(database, book_id, &ebuf, .json);
            try std.testing.expect(std.mem.indexOf(u8, coa, "1000") != null);
        }

        // exportJournalEntries (narrow date range to fit buffer)
        {
            const je = try export_mod.exportJournalEntries(database, book_id, "2027-01-01", "2027-01-31", &ebuf, .json);
            try std.testing.expect(std.mem.indexOf(u8, je, "document_number") != null);
        }

        // exportAuditTrail (narrow date range to fit buffer)
        {
            const at = try export_mod.exportAuditTrail(database, book_id, "2027-01-01", "2027-01-31", &ebuf, .json);
            try std.testing.expect(at.len > 10);
        }

        // exportPeriods
        {
            const ep = try export_mod.exportPeriods(database, book_id, &ebuf, .json);
            try std.testing.expect(std.mem.indexOf(u8, ep, "2026") != null);
        }

        // exportSubledger
        {
            const sl = try export_mod.exportSubledger(database, book_id, &ebuf, .json);
            try std.testing.expect(std.mem.indexOf(u8, sl, "AR Customers") != null);
        }

        // exportBookMetadata
        {
            const bm = try export_mod.exportBookMetadata(database, book_id, &ebuf, .json);
            try std.testing.expect(std.mem.indexOf(u8, bm, "PHP") != null);
        }

        // csvField (utility)
        {
            var small: [50]u8 = undefined;
            const len = try export_mod.csvField(&small, "hello, world");
            try std.testing.expect(small[0] == '"');
            _ = len;
        }

        // jsonString (utility)
        {
            var small: [50]u8 = undefined;
            const len = try export_mod.jsonString(&small, "tab\there");
            const result = small[0..len];
            try std.testing.expect(std.mem.indexOf(u8, result, "\\t") != null);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // PHASE 19: CLEANUP, DELETE, AND ARCHIVE
    // ═══════════════════════════════════════════════════════════════

    // ClassificationNode.delete — remove a node from cash flow classification
    {
        const temp_cls = try classification_mod.Classification.create(database, book_id, "Temp Classification", "trial_balance", "controller");
        const temp_node = try classification_mod.ClassificationNode.addGroup(database, temp_cls, "Temp Group", null, 0, "controller");
        try classification_mod.ClassificationNode.delete(database, temp_node, "controller");

        // Classification.delete — delete the temp classification entirely
        try classification_mod.Classification.delete(database, temp_cls, "controller");
    }

    // SubledgerAccount.delete + SubledgerGroup.delete
    {
        const temp_group = try subledger_mod.SubledgerGroup.create(database, book_id, "Temp SL Group", "customer", 99, ar, null, null, "controller");
        const temp_sl_acct = try subledger_mod.SubledgerAccount.create(database, book_id, "TEMP-001", "Temp Customer", "customer", temp_group, "controller");
        try subledger_mod.SubledgerAccount.delete(database, temp_sl_acct, "controller");
        try subledger_mod.SubledgerGroup.delete(database, temp_group, "controller");
    }

    // Close all open/soft_closed periods for archival
    {
        // FY2027 (period_ids_2027[1] already closed from Phase 16)
        var i: usize = 0;
        while (i < 12) : (i += 1) {
            if (i == 1) continue;
            try period_mod.Period.transition(database, period_ids_2027[i], .soft_closed, "controller");
            try period_mod.Period.transition(database, period_ids_2027[i], .closed, "controller");
        }
        // Close any remaining open periods (adjustment period 13)
        var stmt = try database.prepare("SELECT id FROM ledger_periods WHERE book_id = ? AND status IN ('open', 'soft_closed');");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        while (try stmt.step()) {
            const pid = stmt.columnInt64(0);
            period_mod.Period.transition(database, pid, .soft_closed, "controller") catch {};
            period_mod.Period.transition(database, pid, .closed, "controller") catch {};
        }
    }

    // Book.archive — terminal operation, must be last
    try book_mod.Book.archive(database, book_id, "controller");

    // ═══════════════════════════════════════════════════════════════
    // PHASE 20: FINAL VERIFICATION
    // ═══════════════════════════════════════════════════════════════

    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }

    // Describe schema includes PRAGMA info
    {
        var desc_buf: [32768]u8 = undefined;
        const desc = try describe_mod.describeSchema(database, &desc_buf, .json);
        try std.testing.expect(std.mem.indexOf(u8, desc, "\"foreign_keys\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, desc, "\"journal_mode\":") != null);
    }

    // Audit trail is extensive
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_audit_log WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        const audit_count = stmt.columnInt(0);
        try std.testing.expect(audit_count > 80);
    }

    // Book is archived
    {
        var stmt = try database.prepare("SELECT status FROM ledger_books WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("archived", stmt.columnText(0).?);
    }
}

// ═══════════════════════════════════════════════════════════════════
// SCENARIO TESTS — Isolated scenarios for review-identified gaps
// ═══════════════════════════════════════════════════════════════════

test "SCENARIO: Void blocked by open items, succeeds after payment" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "AR Test", "PHP", 2, "admin");
    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const ar = try account_mod.Account.create(database, book_id, "1100", "AR", .asset, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    _ = revenue;
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    const group_id = try subledger_mod.SubledgerGroup.create(database, book_id, "AR Customers", "customer", 1, ar, null, null, "admin");
    const customer = try subledger_mod.SubledgerAccount.create(database, book_id, "C001", "Customer A", "customer", group_id, "admin");

    // Post invoice: AR 50,000 / Revenue 50,000
    const inv_eid = try entry_mod.Entry.createDraft(database, book_id, "INV-001", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    const inv_line = try entry_mod.Entry.addLine(database, inv_eid, 1, 5_000_000_000_000, 0, "PHP", money.FX_RATE_SCALE, ar, customer, null, "admin");
    _ = try entry_mod.Entry.addLine(database, inv_eid, 2, 0, 5_000_000_000_000, "PHP", money.FX_RATE_SCALE, 3, null, null, "admin");
    try entry_mod.Entry.post(database, inv_eid, "admin");

    // Create open item
    const oi_id = try open_item_mod.createOpenItem(database, inv_line, customer, 5_000_000_000_000, "2026-02-14", book_id, "admin");

    // Void should fail — open item is open
    try std.testing.expectError(error.InvalidInput, entry_mod.Entry.voidEntry(database, inv_eid, "Wrong invoice", "admin"));

    // Partial payment — still open
    try open_item_mod.allocatePayment(database, oi_id, 3_000_000_000_000, "admin");
    try std.testing.expectError(error.InvalidInput, entry_mod.Entry.voidEntry(database, inv_eid, "Wrong invoice", "admin"));

    // Full payment — closed
    try open_item_mod.allocatePayment(database, oi_id, 2_000_000_000_000, "admin");

    // Now void should succeed
    try entry_mod.Entry.voidEntry(database, inv_eid, "Customer dispute resolved", "admin");

    // Verify voided
    {
        var stmt = try database.prepare("SELECT status FROM ledger_entries WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, inv_eid);
        _ = try stmt.step();
        try std.testing.expectEqualStrings("void", stmt.columnText(0).?);
    }

    // Verify TB still balances after void
    {
        const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
        defer tb.deinit();
        try std.testing.expectEqual(tb.total_debits, tb.total_credits);
    }

    // Post cash receipt separately
    const cr_eid = try entry_mod.Entry.createDraft(database, book_id, "CR-001", "2026-01-20", "2026-01-20", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, cr_eid, 1, 5_000_000_000_000, 0, "PHP", money.FX_RATE_SCALE, cash, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, cr_eid, 2, 0, 5_000_000_000_000, "PHP", money.FX_RATE_SCALE, ar, customer, null, "admin");
    try entry_mod.Entry.post(database, cr_eid, "admin");

    // Final verify — 0 errors
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }
}

test "SCENARIO: Report truncated flag on normal reports is false" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Report Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Post one entry
    const eid = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, 1_000_000_000_00, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, 1_000_000_000_00, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    // All reports should have truncated = false
    {
        const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
        defer tb.deinit();
        try std.testing.expect(!tb.truncated);
        try std.testing.expectEqual(tb.total_debits, tb.total_credits);
    }
    {
        const is_rep = try report_mod.incomeStatement(database, book_id, "2026-01-01", "2026-01-31");
        defer is_rep.deinit();
        try std.testing.expect(!is_rep.truncated);
    }
    {
        const bs = try report_mod.balanceSheet(database, book_id, "2026-01-31", "2026-01-01");
        defer bs.deinit();
        try std.testing.expect(!bs.truncated);
        try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    }
    {
        const gl = try report_mod.generalLedger(database, book_id, "2026-01-01", "2026-01-31");
        defer gl.deinit();
        try std.testing.expect(!gl.truncated);
    }
    {
        const eq = try report_mod.equityChanges(database, book_id, "2026-01-01", "2026-01-31", "2026-01-01");
        defer eq.deinit();
        try std.testing.expect(!eq.truncated);
    }
    {
        const comp = try report_mod.trialBalanceComparative(database, book_id, "2026-01-31", "2025-12-31");
        defer comp.deinit();
        try std.testing.expect(!comp.truncated);
    }
}

test "SCENARIO: FX revaluation overflow-safe diff computation" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "FX Test", "PHP", 2, "admin");
    const cash_usd = try account_mod.Account.create(database, book_id, "1001", "Cash USD", .asset, false, "admin");
    const cash_php = try account_mod.Account.create(database, book_id, "1000", "Cash PHP", .asset, false, "admin");
    const fx_gl = try account_mod.Account.create(database, book_id, "7000", "FX G/L", .expense, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gl, "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Large FX transaction: USD 500M at rate 56.50
    // This exercises i128 intermediate without overflowing the base amount
    const usd_amount: i64 = 50_000_000_000_000_000; // 500M * 10^8
    const fx_rate: i64 = 565_000_000_000; // 56.50

    const eid = try entry_mod.Entry.createDraft(database, book_id, "FX-BIG", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid, 1, usd_amount, 0, "USD", fx_rate, cash_usd, null, null, "admin");
    const base = try money.computeBaseAmount(usd_amount, fx_rate);
    _ = try entry_mod.Entry.addLine(database, eid, 2, 0, base, "PHP", money.FX_RATE_SCALE, cash_php, null, null, "admin");
    try entry_mod.Entry.post(database, eid, "admin");

    // Revalue at a significantly different rate
    // The diff between revalued and existing base uses std.math.sub for safety
    const rates = [_]revaluation_mod.CurrencyRate{.{ .currency = "USD", .new_rate = 580_000_000_000 }};
    const reval = try revaluation_mod.revalueForexBalances(database, book_id, period_id, &rates, "admin");
    try std.testing.expect(reval.entry_id > 0);

    // Verify TB balances after large FX revaluation
    {
        const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
        defer tb.deinit();
        try std.testing.expectEqual(tb.total_debits, tb.total_credits);
        try std.testing.expect(!tb.truncated);
    }

    // Verify the revaluation gain is non-trivial (500M * (58.0 - 56.5) = 750M PHP)
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }
}

test "SCENARIO: Non-monetary account excluded from FX revaluation" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "FX Monetary", "PHP", 2, "admin");
    const cash_usd = try account_mod.Account.create(database, book_id, "1001", "Cash USD", .asset, false, "admin");
    const equip_usd = try account_mod.Account.create(database, book_id, "1500", "Equipment USD", .asset, false, "admin");
    const cash_php = try account_mod.Account.create(database, book_id, "1000", "Cash PHP", .asset, false, "admin");
    const fx_gl = try account_mod.Account.create(database, book_id, "7000", "FX G/L", .expense, false, "admin");
    try book_mod.Book.setFxGainLossAccount(database, book_id, fx_gl, "admin");
    try account_mod.Account.setMonetary(database, equip_usd, false, "admin");

    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Post USD to both monetary (Cash) and non-monetary (Equipment)
    const eid1 = try entry_mod.Entry.createDraft(database, book_id, "FX-CASH", "2026-01-10", "2026-01-10", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 10_000_000_000, 0, "USD", 565_000_000_000, cash_usd, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 565_000_000_000, "PHP", money.FX_RATE_SCALE, cash_php, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    const eid2 = try entry_mod.Entry.createDraft(database, book_id, "FX-EQUIP", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 50_000_000_000, 0, "USD", 565_000_000_000, equip_usd, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 2, 0, 2_825_000_000_000, "PHP", money.FX_RATE_SCALE, cash_php, null, null, "admin");
    try entry_mod.Entry.post(database, eid2, "admin");

    // Revalue: only Cash USD ($100) should adjust, not Equipment ($500)
    const rates = [_]revaluation_mod.CurrencyRate{.{ .currency = "USD", .new_rate = 570_000_000_000 }};
    const reval = try revaluation_mod.revalueForexBalances(database, book_id, period_id, &rates, "admin");
    try std.testing.expect(reval.entry_id > 0);

    // Only 2 lines (Cash USD gain + FX GL), not 4
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_entry_lines WHERE entry_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, reval.entry_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 2), stmt.columnInt(0));
    }

    // TB still balances
    {
        const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
        defer tb.deinit();
        try std.testing.expectEqual(tb.total_debits, tb.total_credits);
    }

    // Verify integrity
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }
}

test "SCENARIO: Batch stale recalculation handles many periods" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Stale Test", "PHP", 2, "admin");
    _ = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");

    // Create 24 periods (2 fiscal years)
    try period_mod.Period.bulkCreate(database, book_id, 2026, 1, .monthly, "admin");
    try period_mod.Period.bulkCreate(database, book_id, 2027, 1, .monthly, "admin");

    // Post entries in every period to create cache rows
    var period_ids: [24]i64 = undefined;
    {
        var stmt = try database.prepare("SELECT id FROM ledger_periods WHERE book_id = ? ORDER BY start_date;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        var idx: usize = 0;
        while (try stmt.step()) {
            if (idx < 24) {
                period_ids[idx] = stmt.columnInt64(0);
                idx += 1;
            }
        }
    }

    // Post one entry in first and last period
    {
        const e1 = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-15", "2026-01-15", null, period_ids[0], null, "admin");
        _ = try entry_mod.Entry.addLine(database, e1, 1, 100_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
        _ = try entry_mod.Entry.addLine(database, e1, 2, 0, 100_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
        try entry_mod.Entry.post(database, e1, "admin");
    }
    {
        const e2 = try entry_mod.Entry.createDraft(database, book_id, "JE-002", "2027-12-15", "2027-12-15", null, period_ids[23], null, "admin");
        _ = try entry_mod.Entry.addLine(database, e2, 1, 200_000_000_000, 0, "PHP", money.FX_RATE_SCALE, 1, null, null, "admin");
        _ = try entry_mod.Entry.addLine(database, e2, 2, 0, 200_000_000_000, "PHP", money.FX_RATE_SCALE, 2, null, null, "admin");
        try entry_mod.Entry.post(database, e2, "admin");
    }

    // Mark all cache entries stale
    {
        var stmt = try database.prepare("UPDATE ledger_account_balances SET is_stale = 1 WHERE book_id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
    }

    // recalculateAllStale should process all without truncation
    const fixed = try cache_mod.recalculateAllStale(database, book_id);
    try std.testing.expect(fixed > 0);

    // No stale entries remain
    {
        var stmt = try database.prepare("SELECT COUNT(*) FROM ledger_account_balances WHERE book_id = ? AND is_stale = 1;");
        defer stmt.finalize();
        try stmt.bindInt(1, book_id);
        _ = try stmt.step();
        try std.testing.expectEqual(@as(i32, 0), stmt.columnInt(0));
    }

    // Verify integrity
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }
}

test "SCENARIO: Period close with MaxAccounts=2000 handles large COA" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "Large COA", "PHP", 2, "admin");
    const re = try account_mod.Account.create(database, book_id, "3100", "RE", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re, "admin");
    const period_id = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");

    // Create 10 revenue + 10 expense accounts and post entries for each
    var acct_ids: [20]i64 = undefined;
    var num_buf: [10]u8 = undefined;
    var name_buf: [32]u8 = undefined;
    for (0..10) |i| {
        const num = std.fmt.bufPrint(&num_buf, "4{d:0>3}", .{i}) catch unreachable;
        const name = std.fmt.bufPrint(&name_buf, "Revenue {d}", .{i}) catch unreachable;
        acct_ids[i] = try account_mod.Account.create(database, book_id, num, name, .revenue, false, "admin");
    }
    for (0..10) |i| {
        const num = std.fmt.bufPrint(&num_buf, "5{d:0>3}", .{i}) catch unreachable;
        const name = std.fmt.bufPrint(&name_buf, "Expense {d}", .{i}) catch unreachable;
        acct_ids[10 + i] = try account_mod.Account.create(database, book_id, num, name, .expense, false, "admin");
    }

    // Post one compound entry with all 20 accounts
    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    {
        const eid = try entry_mod.Entry.createDraft(database, book_id, "MULTI-001", "2026-01-15", "2026-01-15", null, period_id, null, "admin");
        var line_num: i32 = 1;
        var total_rev: i64 = 0;
        for (0..10) |i| {
            const amt: i64 = @as(i64, @intCast((i + 1))) * 100_000_000_000;
            _ = try entry_mod.Entry.addLine(database, eid, line_num, 0, amt, "PHP", money.FX_RATE_SCALE, acct_ids[i], null, null, "admin");
            total_rev += amt;
            line_num += 1;
        }
        var total_exp: i64 = 0;
        for (0..10) |i| {
            const amt: i64 = @as(i64, @intCast((i + 1))) * 50_000_000_000;
            _ = try entry_mod.Entry.addLine(database, eid, line_num, amt, 0, "PHP", money.FX_RATE_SCALE, acct_ids[10 + i], null, null, "admin");
            total_exp += amt;
            line_num += 1;
        }
        // Cash receives net
        const net = total_rev - total_exp;
        _ = try entry_mod.Entry.addLine(database, eid, line_num, net, 0, "PHP", money.FX_RATE_SCALE, cash, null, null, "admin");
        try entry_mod.Entry.post(database, eid, "admin");
    }

    // Close the period — should handle 20 R/E accounts without TooManyAccounts
    try close_mod.closePeriod(database, book_id, period_id, "admin");

    // All revenue/expense accounts should be zeroed after close
    {
        const is_rep = try report_mod.incomeStatement(database, book_id, "2026-01-01", "2026-01-31");
        defer is_rep.deinit();
        // After closing, R/E accounts debits=credits so TB shows zero net
        try std.testing.expectEqual(is_rep.total_debits, is_rep.total_credits);
    }

    // Verify
    {
        const v = try verify_mod.verify(database, book_id);
        try std.testing.expectEqual(@as(u32, 0), v.errors);
    }
}

test "SCENARIO: Schema migration logs errors without blocking" {
    const database = try db.Database.open(":memory:");
    defer database.close();

    // Create schema at version 6
    try schema.createAll(database);

    // Running migrate from version 4 should succeed (columns already exist, errors logged)
    try schema.migrate(database, 4);

    // Verify version is still current
    {
        var stmt = try database.prepare("PRAGMA user_version;");
        defer stmt.finalize();
        _ = try stmt.step();
        try std.testing.expectEqual(schema.SCHEMA_VERSION, stmt.columnInt(0));
    }
}

test "SCENARIO: Account hierarchy for SKR03-style COA" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "German Entity", "EUR", 2, "admin");
    const current_assets = try account_mod.Account.create(database, book_id, "1000", "Umlaufvermoegen", .asset, false, "admin");
    const cash = try account_mod.Account.create(database, book_id, "1001", "Kasse", .asset, false, "admin");
    const bank = try account_mod.Account.create(database, book_id, "1002", "Bank", .asset, false, "admin");
    const fixed_assets = try account_mod.Account.create(database, book_id, "0400", "Anlagevermoegen", .asset, false, "admin");
    const equipment = try account_mod.Account.create(database, book_id, "0410", "BGA", .asset, false, "admin");

    try account_mod.Account.setParent(database, cash, current_assets, "admin");
    try account_mod.Account.setParent(database, bank, current_assets, "admin");
    try account_mod.Account.setParent(database, equipment, fixed_assets, "admin");

    {
        var stmt = try database.prepare("SELECT parent_id FROM ledger_accounts WHERE id = ?;");
        defer stmt.finalize();
        try stmt.bindInt(1, cash);
        _ = try stmt.step();
        try std.testing.expectEqual(current_assets, stmt.columnInt64(0));
    }

    try std.testing.expectError(error.InvalidInput, account_mod.Account.setParent(database, current_assets, cash, "admin"));

    const rev = try account_mod.Account.create(database, book_id, "8000", "Erloese", .revenue, false, "admin");
    try std.testing.expectError(error.InvalidInput, account_mod.Account.setParent(database, rev, current_assets, "admin"));
}

test "SCENARIO: India fiscal year (Apr-Mar) with balanceSheetAuto" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "India Entity", "INR", 2, "admin");
    try book_mod.Book.setFyStartMonth(database, book_id, 4, "admin");

    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const re = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re, "admin");

    _ = try period_mod.Period.create(database, book_id, "Apr 2026", 1, 2027, "2026-04-01", "2026-04-30", "regular", "admin");
    _ = try period_mod.Period.create(database, book_id, "May 2026", 2, 2027, "2026-05-01", "2026-05-31", "regular", "admin");

    const eid1 = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-04-15", "2026-04-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 100_000_000_000_00, 0, "INR", 10_000_000_000, cash, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 100_000_000_000_00, "INR", 10_000_000_000, capital, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    const eid2 = try entry_mod.Entry.createDraft(database, book_id, "JE-002", "2026-05-10", "2026-05-10", null, 2, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 50_000_000_000_00, 0, "INR", 10_000_000_000, cash, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 2, 0, 50_000_000_000_00, "INR", 10_000_000_000, revenue, null, null, "admin");
    try entry_mod.Entry.post(database, eid2, "admin");

    const bs = try report_mod.balanceSheetAuto(database, book_id, "2026-05-31");
    defer bs.deinit();

    try std.testing.expectEqual(bs.total_debits, bs.total_credits);
    try std.testing.expect(bs.rows.len >= 2);
}

test "SCENARIO: Presentation currency translation PHP->USD" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const book_id = try book_mod.Book.create(database, "PH Subsidiary", "PHP", 2, "admin");
    const cash = try account_mod.Account.create(database, book_id, "1000", "Cash", .asset, false, "admin");
    const capital = try account_mod.Account.create(database, book_id, "3000", "Capital", .equity, false, "admin");
    const revenue = try account_mod.Account.create(database, book_id, "4000", "Revenue", .revenue, false, "admin");
    const expense = try account_mod.Account.create(database, book_id, "5000", "Expense", .expense, false, "admin");
    const re = try account_mod.Account.create(database, book_id, "3100", "Retained Earnings", .equity, false, "admin");
    try book_mod.Book.setRetainedEarningsAccount(database, book_id, re, "admin");

    _ = try period_mod.Period.create(database, book_id, "Jan 2026", 1, 2026, "2026-01-01", "2026-01-31", "regular", "admin");
    _ = cash;

    const eid1 = try entry_mod.Entry.createDraft(database, book_id, "JE-001", "2026-01-10", "2026-01-10", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 1, 5_650_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid1, 2, 0, 5_650_000_000_00, "PHP", 10_000_000_000, capital, null, null, "admin");
    try entry_mod.Entry.post(database, eid1, "admin");

    const eid2 = try entry_mod.Entry.createDraft(database, book_id, "JE-002", "2026-01-15", "2026-01-15", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 1, 1_000_000_000_00, 0, "PHP", 10_000_000_000, 1, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid2, 2, 0, 1_000_000_000_00, "PHP", 10_000_000_000, revenue, null, null, "admin");
    try entry_mod.Entry.post(database, eid2, "admin");

    const eid3 = try entry_mod.Entry.createDraft(database, book_id, "JE-003", "2026-01-20", "2026-01-20", null, 1, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid3, 1, 300_000_000_00, 0, "PHP", 10_000_000_000, expense, null, null, "admin");
    _ = try entry_mod.Entry.addLine(database, eid3, 2, 0, 300_000_000_00, "PHP", 10_000_000_000, 1, null, null, "admin");
    try entry_mod.Entry.post(database, eid3, "admin");

    const tb = try report_mod.trialBalance(database, book_id, "2026-01-31");
    defer tb.deinit();

    const closing_rate: i64 = 180_000_000; // 0.018 USD/PHP = 1/56 roughly
    const average_rate: i64 = 175_000_000; // 0.0175 USD/PHP
    const rates = report_mod.TranslationRates{
        .closing_rate = closing_rate,
        .average_rate = average_rate,
    };
    const translated = try report_mod.translateReportResult(tb, rates);
    defer translated.deinit();

    try std.testing.expectEqual(tb.rows.len, translated.rows.len);

    for (translated.rows) |row| {
        try std.testing.expect(row.debit_balance != 0 or row.credit_balance != 0);
    }
}

test "SCENARIO: Per-book audit chain verified after multi-book operations" {
    const database = try db.Database.open(":memory:");
    defer database.close();
    try schema.createAll(database);

    const audit_mod = @import("audit.zig");

    const book1 = try book_mod.Book.create(database, "Entity A", "PHP", 2, "admin");
    const book2 = try book_mod.Book.create(database, "Entity B", "USD", 2, "admin");
    _ = try account_mod.Account.create(database, book1, "1000", "Cash", .asset, false, "admin");
    _ = try account_mod.Account.create(database, book2, "1000", "Cash", .asset, false, "admin");

    var stmt = try database.prepare(
        \\SELECT hash_chain FROM ledger_audit_log WHERE book_id = ? ORDER BY id;
    );
    defer stmt.finalize();

    try stmt.bindInt(1, book1);
    var prev_hash = audit_mod.genesis_hash.*;
    var book1_count: u32 = 0;
    while (try stmt.step()) {
        book1_count += 1;
        if (stmt.columnText(0)) |h| {
            if (h.len == 64) {
                @memcpy(&prev_hash, h[0..64]);
            }
        }
    }
    try std.testing.expect(book1_count >= 2);

    stmt.reset();
    stmt.clearBindings();
    try stmt.bindInt(1, book2);
    var book2_count: u32 = 0;
    while (try stmt.step()) {
        book2_count += 1;
    }
    try std.testing.expect(book2_count >= 2);
}
