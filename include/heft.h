/*
 * Heft — The Embedded Accounting Engine
 * Copyright (C) 2026 Jeryl Donato Estopace
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Thread safety:
 *   NOT thread-safe. One LedgerDB handle per thread. Do not share handles
 *   across threads. SQLite compiled with SQLITE_THREADSAFE=0 (no mutexes).
 *   If you need concurrent access, use one handle per thread with separate
 *   .ledger files, or serialize access with an external mutex.
 *
 * Error handling:
 *   Functions returning int64_t return -1 on error.
 *   Functions returning bool return false on error.
 *   Functions returning pointers return NULL on error.
 *   Call ledger_last_error() after any failure for the error code.
 *
 * Buffer functions (ledger_list_*, ledger_get_*, ledger_export_*):
 *   Write JSON or CSV into a caller-provided buffer.
 *   Return the number of bytes written (NOT null-terminated).
 *   Return -1 if the buffer is too small or on error.
 *   Caller must use the returned length to slice the buffer.
 *
 * Report functions (ledger_trial_balance, ledger_balance_sheet, etc.):
 *   Return heap-allocated result pointers. Caller MUST free with the
 *   matching ledger_free_* function or memory will leak.
 *
 * Numeric conventions:
 *   Amounts:  int64_t scaled by 10^8  (10,000.50 = 1000050000000)
 *   FX rates: int64_t scaled by 10^10 (1.0 = 10000000000)
 *   Dates:    "YYYY-MM-DD" (10 chars, NOT null-terminated in buffers)
 *   Timestamps: "YYYY-MM-DDTHH:MM:SSZ" (UTC)
 */

#ifndef HEFT_H
#define HEFT_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Engine limits (static allocation bounds) */
#define HEFT_MAX_REPORT_ROWS   50000  /* max rows in any report result */
#define HEFT_MAX_ACCOUNTS      2000   /* max accounts per close/revalue operation */
#define HEFT_AMOUNT_SCALE      100000000LL      /* 10^8: multiply to convert decimal to int */
#define HEFT_FX_RATE_SCALE     10000000000LL    /* 10^10: multiply to convert FX rate to int */
#define HEFT_SCHEMA_VERSION    9

/* Opaque handle — one per .ledger file */
typedef struct LedgerDB LedgerDB;

/* Opaque result types — caller must free via corresponding ledger_free_* */
typedef struct ReportResult ReportResult;
typedef struct LedgerResult LedgerResult;
typedef struct ComparativeReportResult ComparativeReportResult;
typedef struct EquityResult EquityResult;
typedef struct ClassifiedResult ClassifiedResult;
typedef struct CashFlowIndirectResult CashFlowIndirectResult;

/* Error codes returned by ledger_last_error()
 * Source of truth: main.zig mapError() — keep in sync */
enum {
    HEFT_OK                             = 0,
    HEFT_NOT_FOUND                      = 1,
    HEFT_INVALID_INPUT                  = 2,
    HEFT_PERIOD_CLOSED                  = 3,
    HEFT_PERIOD_LOCKED                  = 4,
    HEFT_ALREADY_POSTED                 = 5,
    HEFT_UNBALANCED_ENTRY               = 6,
    HEFT_DUPLICATE_NUMBER               = 7,
    HEFT_INVALID_TRANSITION             = 8,
    HEFT_ACCOUNT_INACTIVE               = 9,
    HEFT_MISSING_COUNTERPARTY           = 10,
    HEFT_INVALID_COUNTERPARTY           = 11,
    HEFT_AMOUNT_OVERFLOW                = 12,
    HEFT_VOID_REASON_REQUIRED           = 13,
    HEFT_REVERSE_REASON_REQUIRED        = 14,
    HEFT_CIRCULAR_REFERENCE             = 15,
    HEFT_TOO_FEW_LINES                  = 16,
    HEFT_SCHEMA_VERSION_MISMATCH        = 17,
    HEFT_OUT_OF_MEMORY                  = 18,
    HEFT_INVALID_AMOUNT                 = 20,
    HEFT_BOOK_ARCHIVED                  = 21,
    HEFT_CROSS_BOOK_VIOLATION           = 22,
    HEFT_INVALID_FX_RATE                = 23,
    HEFT_INVALID_DECIMAL_PLACES         = 24,
    HEFT_BUFFER_TOO_SMALL               = 25,
    HEFT_RETAINED_EARNINGS_REQUIRED     = 26,
    HEFT_FX_GAIN_LOSS_REQUIRED          = 27,
    HEFT_OPENING_BALANCE_REQUIRED       = 28,
    HEFT_INCOME_SUMMARY_REQUIRED        = 29,
    HEFT_APPROVAL_REQUIRED              = 30,
    HEFT_TOO_MANY_ACCOUNTS              = 31,
    HEFT_SQLITE_OPEN_FAILED             = 90,
    HEFT_SQLITE_EXEC_FAILED             = 91,
    HEFT_SQLITE_PREPARE_FAILED          = 92,
    HEFT_SQLITE_STEP_FAILED             = 93,
    HEFT_SQLITE_BIND_FAILED             = 94,
    HEFT_UNKNOWN                        = 99,
};

/* ── Lifecycle ─────────────────────────────────────────────── */

const char* ledger_version(void);
LedgerDB*   ledger_open(const char* path);
void        ledger_close(LedgerDB* handle);
int32_t     ledger_last_error(void);

/* ── Books ─────────────────────────────────────────────────── */

int64_t ledger_create_book(LedgerDB* h, const char* name, const char* base_currency, int32_t decimal_places, const char* performed_by);
bool    ledger_update_book_name(LedgerDB* h, int64_t book_id, const char* new_name, const char* performed_by);
bool    ledger_archive_book(LedgerDB* h, int64_t book_id, const char* performed_by);
bool    ledger_set_rounding_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_set_fx_gain_loss_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_set_retained_earnings_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_set_income_summary_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_set_opening_balance_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_set_suspense_account(LedgerDB* h, int64_t book_id, int64_t account_id, const char* performed_by);
bool    ledger_validate_opening_balance(LedgerDB* h, int64_t book_id);
bool    ledger_set_require_approval(LedgerDB* h, int64_t book_id, int32_t require, const char* performed_by);
bool    ledger_set_fy_start_month(LedgerDB* h, int64_t book_id, int32_t month, const char* performed_by);
bool    ledger_set_entity_type(LedgerDB* h, int64_t book_id, const char* entity_type, const char* performed_by);

/* ── Accounts ──────────────────────────────────────────────── */

int64_t ledger_create_account(LedgerDB* h, int64_t book_id, const char* number, const char* name, const char* account_type, int32_t is_contra, const char* performed_by);
bool    ledger_update_account_name(LedgerDB* h, int64_t account_id, const char* new_name, const char* performed_by);
bool    ledger_update_account_status(LedgerDB* h, int64_t account_id, const char* new_status, const char* performed_by);
bool    ledger_set_account_monetary(LedgerDB* h, int64_t account_id, int32_t is_monetary, const char* performed_by);
bool    ledger_set_account_parent(LedgerDB* h, int64_t account_id, int64_t parent_id, const char* performed_by);

/* ── Periods ───────────────────────────────────────────────── */

int64_t ledger_create_period(LedgerDB* h, int64_t book_id, const char* name, int32_t period_number, int32_t year, const char* start_date, const char* end_date, const char* period_type, const char* performed_by);
bool    ledger_bulk_create_periods(LedgerDB* h, int64_t book_id, int32_t year, int32_t start_month, const char* granularity, const char* performed_by);
bool    ledger_transition_period(LedgerDB* h, int64_t period_id, const char* new_status, const char* performed_by);
bool    ledger_close_period(LedgerDB* h, int64_t book_id, int64_t period_id, const char* performed_by);

/* ── Journal Entries ───────────────────────────────────────── */

int64_t ledger_create_draft(LedgerDB* h, int64_t book_id, const char* document_number, const char* transaction_date, const char* posting_date, const char* description, int64_t period_id, const char* metadata, const char* performed_by);
bool    ledger_edit_draft(LedgerDB* h, int64_t entry_id, const char* document_number, const char* transaction_date, const char* posting_date, const char* description, const char* metadata, int64_t period_id, const char* performed_by);
bool    ledger_edit_posted(LedgerDB* h, int64_t entry_id, const char* description, const char* metadata, const char* performed_by);
int64_t ledger_add_line(LedgerDB* h, int64_t entry_id, int32_t line_number, int64_t debit_amount, int64_t credit_amount, const char* transaction_currency, int64_t fx_rate, int64_t account_id, int64_t counterparty_id, const char* description, const char* performed_by);
bool    ledger_edit_line(LedgerDB* h, int64_t line_id, int64_t debit_amount, int64_t credit_amount, const char* transaction_currency, int64_t fx_rate, int64_t account_id, const char* performed_by);
bool    ledger_edit_line_full(LedgerDB* h, int64_t line_id, int64_t debit_amount, int64_t credit_amount, const char* currency, int64_t fx_rate, int64_t account_id, int64_t counterparty_id, const char* description, const char* performed_by);
bool    ledger_remove_line(LedgerDB* h, int64_t line_id, const char* performed_by);
bool    ledger_delete_draft(LedgerDB* h, int64_t entry_id, const char* performed_by);
bool    ledger_post_entry(LedgerDB* h, int64_t entry_id, const char* performed_by);
bool    ledger_void_entry(LedgerDB* h, int64_t entry_id, const char* reason, const char* performed_by);
int64_t ledger_reverse_entry(LedgerDB* h, int64_t entry_id, const char* reason, const char* reversal_date, int64_t target_period_id, const char* performed_by);

/* ── Approval Workflow ────────────────────────────────────── */

bool    ledger_approve_entry(LedgerDB* h, int64_t entry_id, const char* performed_by);
bool    ledger_reject_entry(LedgerDB* h, int64_t entry_id, const char* reason, const char* performed_by);

/* ── Batch Operations ─────────────────────────────────────── */

bool    ledger_batch_post(LedgerDB* h, const char* entry_ids_json, const char* performed_by, uint32_t* out_succeeded, uint32_t* out_failed);
bool    ledger_batch_void(LedgerDB* h, const char* entry_ids_json, const char* reason, const char* performed_by, uint32_t* out_succeeded, uint32_t* out_failed);

/* ── Subledger ────────────────────────────────────────────── */

int64_t ledger_create_subledger_group(LedgerDB* h, int64_t book_id, const char* name, const char* group_type, int32_t group_number, int64_t gl_account_id, const char* performed_by);
bool    ledger_update_subledger_group_name(LedgerDB* h, int64_t group_id, const char* new_name, const char* performed_by);
bool    ledger_delete_subledger_group(LedgerDB* h, int64_t group_id, const char* performed_by);
int64_t ledger_create_subledger_account(LedgerDB* h, int64_t book_id, const char* number, const char* name, const char* account_type, int64_t group_id, const char* performed_by);
bool    ledger_update_subledger_account_name(LedgerDB* h, int64_t account_id, const char* new_name, const char* performed_by);
bool    ledger_update_subledger_account_status(LedgerDB* h, int64_t account_id, const char* new_status, const char* performed_by);
bool    ledger_delete_subledger_account(LedgerDB* h, int64_t account_id, const char* performed_by);

/* ── Classifications ──────────────────────────────────────── */

int64_t ledger_create_classification(LedgerDB* h, int64_t book_id, const char* name, const char* report_type, const char* performed_by);
bool    ledger_update_classification_name(LedgerDB* h, int64_t classification_id, const char* new_name, const char* performed_by);
bool    ledger_delete_classification(LedgerDB* h, int64_t classification_id, const char* performed_by);
int64_t ledger_add_group_node(LedgerDB* h, int64_t classification_id, const char* label, int64_t parent_id, int32_t position, const char* performed_by);
int64_t ledger_add_account_node(LedgerDB* h, int64_t classification_id, int64_t account_id, int64_t parent_id, int32_t position, const char* performed_by);
bool    ledger_move_node(LedgerDB* h, int64_t node_id, int64_t new_parent_id, int32_t new_position, const char* performed_by);
bool    ledger_update_node_label(LedgerDB* h, int64_t node_id, const char* new_label, const char* performed_by);
bool    ledger_delete_node(LedgerDB* h, int64_t node_id, const char* performed_by);

/* ── Dimensions ───────────────────────────────────────────── */

int64_t ledger_create_dimension(LedgerDB* h, int64_t book_id, const char* name, const char* dimension_type, const char* performed_by);
bool    ledger_delete_dimension(LedgerDB* h, int64_t dimension_id, const char* performed_by);
int64_t ledger_create_dimension_value(LedgerDB* h, int64_t dimension_id, const char* code, const char* label, const char* performed_by);
bool    ledger_delete_dimension_value(LedgerDB* h, int64_t value_id, const char* performed_by);
bool    ledger_assign_line_dimension(LedgerDB* h, int64_t line_id, int64_t dimension_value_id, const char* performed_by);
bool    ledger_remove_line_dimension(LedgerDB* h, int64_t line_id, int64_t dimension_value_id, const char* performed_by);

/* ── Budgets ──────────────────────────────────────────────── */

int64_t ledger_create_budget(LedgerDB* h, int64_t book_id, const char* name, int32_t fiscal_year, const char* performed_by);
bool    ledger_delete_budget(LedgerDB* h, int64_t budget_id, const char* performed_by);
int64_t ledger_set_budget_line(LedgerDB* h, int64_t budget_id, int64_t account_id, int64_t period_id, int64_t amount, const char* performed_by);
bool    ledger_transition_budget(LedgerDB* h, int64_t budget_id, const char* target_status, const char* performed_by);

/* ── Open Items (AR/AP) ───────────────────────────────────── */

int64_t ledger_create_open_item(LedgerDB* h, int64_t entry_line_id, int64_t counterparty_id, int64_t original_amount, const char* due_date, int64_t book_id, const char* performed_by);
bool    ledger_allocate_payment(LedgerDB* h, int64_t open_item_id, int64_t amount, const char* performed_by);

/* ── Reports (heap-allocated — caller MUST call matching free) */

ReportResult*            ledger_trial_balance(LedgerDB* h, int64_t book_id, const char* as_of_date);
ReportResult*            ledger_trial_balance_movement(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ReportResult*            ledger_income_statement(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ReportResult*            ledger_balance_sheet(LedgerDB* h, int64_t book_id, const char* as_of_date, const char* fy_start_date);
ReportResult*            ledger_balance_sheet_auto(LedgerDB* h, int64_t book_id, const char* as_of_date);
ReportResult*            ledger_translate_report(ReportResult* source, int64_t closing_rate, int64_t average_rate);
EquityResult*            ledger_equity_changes(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date, const char* fy_start_date);
LedgerResult*            ledger_general_ledger(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
LedgerResult*            ledger_account_ledger(LedgerDB* h, int64_t book_id, int64_t account_id, const char* start_date, const char* end_date);
LedgerResult*            ledger_journal_register(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ComparativeReportResult* ledger_trial_balance_comparative(LedgerDB* h, int64_t book_id, const char* current_date, const char* prior_date);
ComparativeReportResult* ledger_trial_balance_movement_comparative(LedgerDB* h, int64_t book_id, const char* cur_start, const char* cur_end, const char* prior_start, const char* prior_end);
ComparativeReportResult* ledger_income_statement_comparative(LedgerDB* h, int64_t book_id, const char* cur_start, const char* cur_end, const char* prior_start, const char* prior_end);
ComparativeReportResult* ledger_balance_sheet_comparative(LedgerDB* h, int64_t book_id, const char* current_date, const char* prior_date, const char* fy_start);
ClassifiedResult*        ledger_classified_report(LedgerDB* h, int64_t classification_id, const char* as_of_date);
ClassifiedResult*        ledger_classified_trial_balance(LedgerDB* h, int64_t classification_id, const char* as_of_date);
ClassifiedResult*        ledger_cash_flow_statement(LedgerDB* h, int64_t classification_id, const char* start_date, const char* end_date);
CashFlowIndirectResult*  ledger_cash_flow_indirect(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date, int64_t classification_id);

/* ── Report Result Accessors ──────────────────────────────── */

int32_t ledger_result_row_count(ReportResult* r);
int64_t ledger_result_total_debits(ReportResult* r);
int64_t ledger_result_total_credits(ReportResult* r);

/* ── Report Memory Management ─────────────────────────────── */

void ledger_free_result(ReportResult* r);
void ledger_free_ledger_result(LedgerResult* r);
void ledger_free_comparative_result(ComparativeReportResult* r);
void ledger_free_equity_result(EquityResult* r);
void ledger_free_classified_result(ClassifiedResult* r);
void ledger_free_cash_flow_indirect(CashFlowIndirectResult* r);

/* ── Multi-Currency ────────────────────────────────────────── */

int64_t ledger_revalue_forex_balances(LedgerDB* h, int64_t book_id, int64_t period_id, const char* rates_json, const char* performed_by);

/* ── Verification ──────────────────────────────────────────── */

bool    ledger_verify(LedgerDB* h, int64_t book_id, uint32_t* out_errors, uint32_t* out_warnings);
int32_t ledger_recalculate_balances(LedgerDB* h, int64_t book_id);

/* ── Queries (JSON/CSV into caller buffer, returns bytes written or -1) */

int32_t ledger_get_book(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_account(LedgerDB* h, int64_t account_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_period(LedgerDB* h, int64_t period_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_entry(LedgerDB* h, int64_t entry_id, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_books(LedgerDB* h, const char* status_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_accounts(LedgerDB* h, int64_t book_id, const char* type_filter, const char* status_filter, const char* name_search, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_periods(LedgerDB* h, int64_t book_id, int32_t year_filter, const char* status_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_entries(LedgerDB* h, int64_t book_id, const char* status_filter, const char* start_date, const char* end_date, const char* doc_search, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_entry_lines(LedgerDB* h, int64_t entry_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_classifications(LedgerDB* h, int64_t book_id, const char* type_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_subledger_groups(LedgerDB* h, int64_t book_id, const char* type_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_subledger_accounts(LedgerDB* h, int64_t book_id, int64_t group_filter, const char* name_search, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_audit_log(LedgerDB* h, int64_t book_id, const char* entity_type, const char* action, const char* start_date, const char* end_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_open_items(LedgerDB* h, int64_t counterparty_id, bool include_closed, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_dimensions(LedgerDB* h, int64_t book_id, const char* type_filter, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_dimension_values(LedgerDB* h, int64_t dimension_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_transactions(LedgerDB* h, int64_t book_id, int64_t account_filter, int64_t counterparty_filter, const char* start_date, const char* end_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);

/* ── Subledger Reports (JSON/CSV into caller buffer) ──────── */

int32_t ledger_subledger_report(LedgerDB* h, int64_t book_id, int64_t group_id, const char* name_search, const char* start_date, const char* end_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_counterparty_ledger(LedgerDB* h, int64_t book_id, int64_t counterparty_id, int64_t account_filter, const char* start_date, const char* end_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_subledger_reconciliation(LedgerDB* h, int64_t book_id, int64_t group_id, const char* as_of_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_aged_subledger(LedgerDB* h, int64_t book_id, int64_t group_id, const char* as_of_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_dimension_summary(LedgerDB* h, int64_t book_id, int64_t dimension_id, const char* start_date, const char* end_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_dimension_summary_rollup(LedgerDB* h, int64_t book_id, int64_t dimension_id, const char* start_date, const char* end_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_budget_vs_actual(LedgerDB* h, int64_t budget_id, const char* start_date, const char* end_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_describe_schema(LedgerDB* h, uint8_t* buf, int32_t buf_len, int32_t format);

/* ── Export (full-book JSON/CSV into caller buffer) ────────── */

int32_t ledger_export_chart_of_accounts(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_export_journal_entries(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_export_audit_trail(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_export_periods(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_export_subledger(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_export_book_metadata(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);

#ifdef __cplusplus
}
#endif

#endif /* HEFT_H */
