pub const views = [_][*:0]const u8{
    \\CREATE VIEW IF NOT EXISTS ledger_transaction_history AS
    \\SELECT
    \\  l.id AS line_id, l.line_number,
    \\  l.debit_amount, l.credit_amount,
    \\  l.base_debit_amount, l.base_credit_amount,
    \\  l.fx_rate, l.transaction_currency,
    \\  l.description AS line_description,
    \\  l.quantity, l.unit_type,
    \\  l.account_id, l.entry_id, l.counterparty_id,
    \\  e.document_number, e.transaction_date, e.posting_date,
    \\  e.description AS entry_description, e.status AS entry_status,
    \\  e.metadata, e.posted_at, e.posted_by,
    \\  e.reverses_entry_id, e.void_reason, e.reversed_reason,
    \\  e.book_id, e.period_id,
    \\  a.number AS account_number, a.name AS account_name,
    \\  a.account_type, a.normal_balance, a.is_contra,
    \\  a.status AS account_status,
    \\  p.name AS period_name, p.period_number,
    \\  p.year AS period_year, p.start_date AS period_start_date,
    \\  p.end_date AS period_end_date, p.period_type,
    \\  p.status AS period_status,
    \\  sa.number AS counterparty_number, sa.name AS counterparty_name,
    \\  sa.type AS counterparty_type,
    \\  sg.id AS subledger_group_id, sg.name AS subledger_group_name,
    \\  sg.gl_account_id AS control_account_id
    \\FROM ledger_entry_lines l
    \\INNER JOIN ledger_entries e ON e.id = l.entry_id
    \\INNER JOIN ledger_accounts a ON a.id = l.account_id
    \\INNER JOIN ledger_periods p ON p.id = e.period_id
    \\LEFT JOIN ledger_subledger_accounts sa ON sa.id = l.counterparty_id
    \\LEFT JOIN ledger_subledger_groups sg ON sg.id = sa.group_id
    \\WHERE e.status = 'posted';
    ,
};
