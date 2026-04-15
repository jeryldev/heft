pub const tables = [_][*:0]const u8{
    \\CREATE TABLE IF NOT EXISTS ledger_books (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  base_currency TEXT NOT NULL
    \\    CHECK (length(base_currency) = 3),
    \\  decimal_places INTEGER NOT NULL DEFAULT 2
    \\    CHECK (decimal_places BETWEEN 0 AND 8),
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'archived')),
    \\  rounding_account_id INTEGER,
    \\  fx_gain_loss_account_id INTEGER,
    \\  retained_earnings_account_id INTEGER,
    \\  income_summary_account_id INTEGER,
    \\  opening_balance_account_id INTEGER,
    \\  suspense_account_id INTEGER,
    \\  require_approval INTEGER NOT NULL DEFAULT 0
    \\    CHECK (require_approval IN (0, 1)),
    \\  fy_start_month INTEGER NOT NULL DEFAULT 1
    \\    CHECK (fy_start_month BETWEEN 1 AND 12),
    \\  entity_type TEXT NOT NULL DEFAULT 'corporation'
    \\    CHECK (entity_type IN ('corporation', 'sole_proprietorship', 'partnership',
    \\                            'llc', 'nonprofit', 'cooperative', 'fund',
    \\                            'government', 'other')),
    \\  dividends_drawings_account_id INTEGER,
    \\  current_year_earnings_account_id INTEGER,
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_accounts (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  number TEXT NOT NULL
    \\    CHECK (length(number) BETWEEN 1 AND 50),
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  account_type TEXT NOT NULL
    \\    CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    \\  normal_balance TEXT NOT NULL
    \\    CHECK (normal_balance IN ('debit', 'credit')),
    \\  is_contra INTEGER NOT NULL DEFAULT 0
    \\    CHECK (is_contra IN (0, 1)),
    \\  is_monetary INTEGER NOT NULL DEFAULT 1
    \\    CHECK (is_monetary IN (0, 1)),
    \\  parent_id INTEGER REFERENCES ledger_accounts(id),
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'inactive', 'archived')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, number)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_periods (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 100),
    \\  period_number INTEGER NOT NULL
    \\    CHECK (period_number BETWEEN 1 AND 16),
    \\  year INTEGER NOT NULL,
    \\  start_date TEXT NOT NULL
    \\    CHECK (length(start_date) = 10 AND date(start_date) IS NOT NULL),
    \\  end_date TEXT NOT NULL
    \\    CHECK (length(end_date) = 10 AND date(end_date) IS NOT NULL),
    \\  period_type TEXT NOT NULL DEFAULT 'regular'
    \\    CHECK (period_type IN ('regular', 'adjustment')),
    \\  status TEXT NOT NULL DEFAULT 'open'
    \\    CHECK (status IN ('open', 'soft_closed', 'closed', 'locked')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, period_number, year),
    \\  CHECK (end_date >= start_date)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_classifications (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  report_type TEXT NOT NULL
    \\    CHECK (report_type IN ('balance_sheet', 'income_statement', 'trial_balance', 'cash_flow')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_classification_nodes (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  node_type TEXT NOT NULL
    \\    CHECK (node_type IN ('group', 'account')),
    \\  label TEXT
    \\    CHECK (label IS NULL OR length(label) <= 255),
    \\  parent_id INTEGER REFERENCES ledger_classification_nodes(id),
    \\  account_id INTEGER REFERENCES ledger_accounts(id),
    \\  position INTEGER NOT NULL DEFAULT 0,
    \\  depth INTEGER NOT NULL DEFAULT 0,
    \\  classification_id INTEGER NOT NULL REFERENCES ledger_classifications(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  CHECK ((node_type = 'group' AND label IS NOT NULL) OR (node_type = 'account' AND account_id IS NOT NULL))
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_groups (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier')),
    \\  group_number INTEGER NOT NULL,
    \\  number_range_start TEXT
    \\    CHECK (number_range_start IS NULL OR length(number_range_start) <= 50),
    \\  number_range_end TEXT
    \\    CHECK (number_range_end IS NULL OR length(number_range_end) <= 50),
    \\  gl_account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, type, group_number),
    \\  CHECK (number_range_start IS NULL OR number_range_end IS NULL OR number_range_start <= number_range_end)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_subledger_accounts (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  number TEXT NOT NULL
    \\    CHECK (length(number) BETWEEN 1 AND 50),
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  type TEXT NOT NULL
    \\    CHECK (type IN ('customer', 'supplier', 'both')),
    \\  group_id INTEGER NOT NULL REFERENCES ledger_subledger_groups(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  status TEXT NOT NULL DEFAULT 'active'
    \\    CHECK (status IN ('active', 'inactive', 'archived')),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, number)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_entries (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  document_number TEXT NOT NULL
    \\    CHECK (length(document_number) BETWEEN 1 AND 100),
    \\  transaction_date TEXT NOT NULL
    \\    CHECK (length(transaction_date) = 10 AND date(transaction_date) IS NOT NULL),
    \\  posting_date TEXT NOT NULL
    \\    CHECK (length(posting_date) = 10 AND date(posting_date) IS NOT NULL),
    \\  description TEXT
    \\    CHECK (description IS NULL OR length(description) <= 1000),
    \\  status TEXT NOT NULL DEFAULT 'draft'
    \\    CHECK (status IN ('draft', 'posted', 'reversed', 'void')),
    \\  void_reason TEXT
    \\    CHECK (void_reason IS NULL OR length(void_reason) <= 500),
    \\  reversed_reason TEXT
    \\    CHECK (reversed_reason IS NULL OR length(reversed_reason) <= 500),
    \\  reverses_entry_id INTEGER REFERENCES ledger_entries(id),
    \\  posted_at TEXT,
    \\  posted_by TEXT
    \\    CHECK (posted_by IS NULL OR length(posted_by) <= 100),
    \\  metadata TEXT
    \\    CHECK (metadata IS NULL OR length(metadata) <= 10000),
    \\  entry_type TEXT NOT NULL DEFAULT 'standard'
    \\    CHECK (entry_type IN ('standard', 'opening', 'closing', 'reversal', 'adjusting')),
    \\  approval_status TEXT NOT NULL DEFAULT 'none'
    \\    CHECK (approval_status IN ('none', 'pending', 'approved', 'rejected')),
    \\  approved_by TEXT
    \\    CHECK (approved_by IS NULL OR length(approved_by) <= 100),
    \\  approved_at TEXT,
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, document_number),
    \\  CHECK (reverses_entry_id IS NULL OR reverses_entry_id != id)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_entry_lines (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  line_number INTEGER NOT NULL,
    \\  debit_amount INTEGER NOT NULL DEFAULT 0,
    \\  credit_amount INTEGER NOT NULL DEFAULT 0,
    \\  base_debit_amount INTEGER NOT NULL DEFAULT 0,
    \\  base_credit_amount INTEGER NOT NULL DEFAULT 0,
    \\  fx_rate INTEGER NOT NULL DEFAULT 10000000000
    \\    CHECK (fx_rate > 0),
    \\  transaction_currency TEXT NOT NULL
    \\    CHECK (length(transaction_currency) = 3),
    \\  description TEXT
    \\    CHECK (description IS NULL OR length(description) <= 1000),
    \\  quantity INTEGER,
    \\  unit_type TEXT
    \\    CHECK (unit_type IS NULL OR length(unit_type) <= 50),
    \\  counterparty_id INTEGER REFERENCES ledger_subledger_accounts(id),
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  entry_id INTEGER NOT NULL REFERENCES ledger_entries(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (entry_id, line_number),
    \\  CHECK ((debit_amount > 0 AND credit_amount = 0)
    \\      OR (debit_amount = 0 AND credit_amount > 0))
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_account_balances (
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  debit_sum INTEGER NOT NULL DEFAULT 0,
    \\  credit_sum INTEGER NOT NULL DEFAULT 0,
    \\  balance INTEGER NOT NULL DEFAULT 0,
    \\  entry_count INTEGER NOT NULL DEFAULT 0,
    \\  is_stale INTEGER NOT NULL DEFAULT 0
    \\    CHECK (is_stale IN (0, 1)),
    \\  stale_since TEXT,
    \\  last_recalculated_at TEXT,
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  PRIMARY KEY (account_id, period_id)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_dimensions (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 100),
    \\  dimension_type TEXT NOT NULL
    \\    CHECK (dimension_type IN ('tax_code', 'cost_center', 'department', 'project', 'segment', 'profit_center', 'fund', 'custom')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_dimension_values (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  code TEXT NOT NULL
    \\    CHECK (length(code) BETWEEN 1 AND 50),
    \\  label TEXT NOT NULL
    \\    CHECK (length(label) BETWEEN 1 AND 255),
    \\  dimension_id INTEGER NOT NULL REFERENCES ledger_dimensions(id),
    \\  parent_value_id INTEGER REFERENCES ledger_dimension_values(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (dimension_id, code)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_line_dimensions (
    \\  line_id INTEGER NOT NULL REFERENCES ledger_entry_lines(id),
    \\  dimension_value_id INTEGER NOT NULL REFERENCES ledger_dimension_values(id),
    \\  PRIMARY KEY (line_id, dimension_value_id)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_budgets (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  name TEXT NOT NULL
    \\    CHECK (length(name) BETWEEN 1 AND 255),
    \\  fiscal_year INTEGER NOT NULL,
    \\  status TEXT NOT NULL DEFAULT 'draft'
    \\    CHECK (status IN ('draft', 'approved', 'closed')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (book_id, name)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_budget_lines (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  amount INTEGER NOT NULL DEFAULT 0,
    \\  budget_id INTEGER NOT NULL REFERENCES ledger_budgets(id),
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  period_id INTEGER NOT NULL REFERENCES ledger_periods(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (budget_id, account_id, period_id)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_audit_log (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  entity_type TEXT NOT NULL
    \\    CHECK (length(entity_type) BETWEEN 1 AND 50),
    \\  entity_id INTEGER NOT NULL,
    \\  action TEXT NOT NULL
    \\    CHECK (length(action) BETWEEN 1 AND 50),
    \\  field_changed TEXT
    \\    CHECK (field_changed IS NULL OR length(field_changed) <= 100),
    \\  old_value TEXT
    \\    CHECK (old_value IS NULL OR length(old_value) <= 4000),
    \\  new_value TEXT
    \\    CHECK (new_value IS NULL OR length(new_value) <= 4000),
    \\  performed_by TEXT NOT NULL
    \\    CHECK (length(performed_by) BETWEEN 1 AND 100),
    \\  performed_at TEXT NOT NULL
    \\    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  hash_chain TEXT
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_open_items (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  entry_line_id INTEGER NOT NULL REFERENCES ledger_entry_lines(id),
    \\  counterparty_id INTEGER NOT NULL REFERENCES ledger_subledger_accounts(id),
    \\  original_amount INTEGER NOT NULL CHECK (original_amount > 0),
    \\  remaining_amount INTEGER NOT NULL CHECK (remaining_amount >= 0),
    \\  due_date TEXT CHECK (due_date IS NULL OR length(due_date) = 10),
    \\  status TEXT NOT NULL DEFAULT 'open'
    \\    CHECK (status IN ('open', 'partial', 'closed')),
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  UNIQUE (entry_line_id)
    \\);
    ,
    \\CREATE TABLE IF NOT EXISTS ledger_equity_allocations (
    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
    \\  book_id INTEGER NOT NULL REFERENCES ledger_books(id),
    \\  account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
    \\  allocation_name TEXT NOT NULL
    \\    CHECK (length(allocation_name) BETWEEN 1 AND 100),
    \\  allocation_type TEXT NOT NULL DEFAULT 'percentage'
    \\    CHECK (allocation_type IN ('percentage', 'ratio', 'equal', 'fixed_amount')),
    \\  allocation_value INTEGER NOT NULL
    \\    CHECK (allocation_value >= 0),
    \\  effective_date TEXT NOT NULL
    \\    CHECK (length(effective_date) = 10),
    \\  end_date TEXT
    \\    CHECK (end_date IS NULL OR length(end_date) = 10),
    \\  inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    \\  CHECK (end_date IS NULL OR end_date >= effective_date)
    \\);
    ,
};
