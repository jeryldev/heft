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

/* Error codes returned by ledger_last_error() */
enum {
    HEFT_OK = 0,
    HEFT_NOT_FOUND = 1,
    HEFT_ALREADY_POSTED = 2,
    HEFT_UNBALANCED_ENTRY = 3,
    HEFT_PERIOD_CLOSED = 4,
    HEFT_PERIOD_LOCKED = 5,
    HEFT_INVALID_INPUT = 6,
    HEFT_DUPLICATE_NUMBER = 7,
    HEFT_BOOK_ARCHIVED = 8,
    HEFT_ACCOUNT_INACTIVE = 9,
    HEFT_INVALID_TRANSITION = 10,
    HEFT_TOO_FEW_LINES = 11,
    HEFT_AMOUNT_OVERFLOW = 12,
    HEFT_SQLITE_ERROR = 13,
    HEFT_OUT_OF_MEMORY = 14,
    HEFT_VOID_REASON_REQUIRED = 15,
    HEFT_REVERSE_REASON_REQUIRED = 16,
    HEFT_UNKNOWN = 99,
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
bool    ledger_set_require_approval(LedgerDB* h, int64_t book_id, int32_t require, const char* performed_by);
bool    ledger_set_fy_start_month(LedgerDB* h, int64_t book_id, int32_t month, const char* performed_by);

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
int64_t ledger_add_line(LedgerDB* h, int64_t entry_id, int32_t line_number, int64_t debit_amount, int64_t credit_amount, const char* transaction_currency, int64_t fx_rate, int64_t account_id, int64_t counterparty_id, const char* description, const char* performed_by);
bool    ledger_remove_line(LedgerDB* h, int64_t line_id, const char* performed_by);
bool    ledger_edit_line(LedgerDB* h, int64_t line_id, int64_t debit_amount, int64_t credit_amount, const char* performed_by);
bool    ledger_edit_line_full(LedgerDB* h, int64_t line_id, int64_t debit_amount, int64_t credit_amount, const char* currency, int64_t fx_rate, int64_t account_id, int64_t counterparty_id, const char* description, const char* performed_by);
bool    ledger_delete_draft(LedgerDB* h, int64_t entry_id, const char* performed_by);
bool    ledger_post_entry(LedgerDB* h, int64_t entry_id, const char* performed_by);
bool    ledger_void_entry(LedgerDB* h, int64_t entry_id, const char* reason, const char* performed_by);
int64_t ledger_reverse_entry(LedgerDB* h, int64_t entry_id, const char* reason, const char* reversal_date, int64_t target_period_id, const char* performed_by);

/* ── Reports ───────────────────────────────────────────────── */

ReportResult*            ledger_trial_balance(LedgerDB* h, int64_t book_id, const char* as_of_date);
ReportResult*            ledger_trial_balance_movement(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ReportResult*            ledger_income_statement(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ReportResult*            ledger_balance_sheet(LedgerDB* h, int64_t book_id, const char* as_of_date, const char* fy_start_date);
ReportResult*            ledger_balance_sheet_auto(LedgerDB* h, int64_t book_id, const char* as_of_date);
EquityResult*            ledger_equity_changes(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date, const char* fy_start_date);
LedgerResult*            ledger_general_ledger(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
LedgerResult*            ledger_account_ledger(LedgerDB* h, int64_t book_id, int64_t account_id, const char* start_date, const char* end_date);
LedgerResult*            ledger_journal_register(LedgerDB* h, int64_t book_id, const char* start_date, const char* end_date);
ComparativeReportResult* ledger_trial_balance_comparative(LedgerDB* h, int64_t book_id, const char* current_date, const char* prior_date);
ComparativeReportResult* ledger_income_statement_comparative(LedgerDB* h, int64_t book_id, const char* cur_start, const char* cur_end, const char* prior_start, const char* prior_end);
ComparativeReportResult* ledger_balance_sheet_comparative(LedgerDB* h, int64_t book_id, const char* current_date, const char* prior_date, const char* fy_start);
ReportResult*            ledger_translate_report(ReportResult* source, int64_t closing_rate, int64_t average_rate);

/* ── Report Memory Management ─────────────────────────────── */

void ledger_free_result(ReportResult* r);
void ledger_free_ledger_result(LedgerResult* r);
void ledger_free_comparative_result(ComparativeReportResult* r);
void ledger_free_equity_result(EquityResult* r);
void ledger_free_classified_result(ClassifiedResult* r);
void ledger_free_cash_flow_indirect(CashFlowIndirectResult* r);

/* ── Multi-Currency ────────────────────────────────────────── */

bool ledger_revalue_forex_balances(LedgerDB* h, int64_t book_id, int64_t period_id, const char* rates_json, const char* performed_by);

/* ── Verification ──────────────────────────────────────────── */

bool ledger_verify(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len);
bool ledger_recalculate_balances(LedgerDB* h, int64_t book_id);

/* ── Queries (JSON/CSV output to caller buffer) ────────────── */

int32_t ledger_list_books(LedgerDB* h, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_accounts(LedgerDB* h, int64_t book_id, const char* type_filter, const char* status_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_periods(LedgerDB* h, int64_t book_id, int32_t year_filter, const char* status_filter, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_entries(LedgerDB* h, int64_t book_id, const char* status_filter, const char* start_date, const char* end_date, int32_t sort_order, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_entry_lines(LedgerDB* h, int64_t entry_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_list_audit_log(LedgerDB* h, int64_t book_id, const char* entity_type, int64_t entity_id, int32_t limit, int32_t offset, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_book(LedgerDB* h, int64_t book_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_account(LedgerDB* h, int64_t account_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_entry(LedgerDB* h, int64_t entry_id, uint8_t* buf, int32_t buf_len, int32_t format);
int32_t ledger_get_period(LedgerDB* h, int64_t period_id, uint8_t* buf, int32_t buf_len, int32_t format);

#ifdef __cplusplus
}
#endif

#endif /* HEFT_H */
