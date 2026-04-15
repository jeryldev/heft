pub const indexes = [_][*:0]const u8{
    \\CREATE INDEX IF NOT EXISTS idx_entries_book_status_date
    \\  ON ledger_entries (book_id, status, posting_date, id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_lines_account
    \\  ON ledger_entry_lines (account_id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_lines_entry
    \\  ON ledger_entry_lines (entry_id, line_number);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_lines_counterparty
    \\  ON ledger_entry_lines (counterparty_id)
    \\  WHERE counterparty_id IS NOT NULL;
    ,
    \\CREATE INDEX IF NOT EXISTS idx_subledger_accounts_group_number
    \\  ON ledger_subledger_accounts (group_id, number);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_class_nodes_tree
    \\  ON ledger_classification_nodes (classification_id, parent_id, position);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_accounts_book_number
    \\  ON ledger_accounts (book_id, number);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_periods_book_dates
    \\  ON ledger_periods (book_id, start_date, end_date);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_audit_log_entity
    \\  ON ledger_audit_log (entity_type, entity_id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_audit_log_book_date
    \\  ON ledger_audit_log (book_id, performed_at);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_entries_book_period
    \\  ON ledger_entries (book_id, period_id, status);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_balances_period
    \\  ON ledger_account_balances (period_id, account_id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_balances_account_period
    \\  ON ledger_account_balances (account_id, period_id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_entries_reverses
    \\  ON ledger_entries (reverses_entry_id)
    \\  WHERE reverses_entry_id IS NOT NULL;
    ,
    \\CREATE INDEX IF NOT EXISTS idx_line_dimensions_value
    \\  ON ledger_line_dimensions (dimension_value_id);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_open_items_counterparty
    \\  ON ledger_open_items (counterparty_id, status, due_date);
    ,
    \\CREATE INDEX IF NOT EXISTS idx_equity_allocations_book_dates
    \\  ON ledger_equity_allocations (book_id, effective_date, end_date);
    ,
};
