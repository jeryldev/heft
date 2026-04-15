pub const triggers = [_][*:0]const u8{
    \\CREATE TRIGGER IF NOT EXISTS protect_audit_log_delete
    \\BEFORE DELETE ON ledger_audit_log
    \\BEGIN
    \\  SELECT RAISE(ABORT, 'audit log is immutable: DELETE not allowed');
    \\END;
    ,
    \\CREATE TRIGGER IF NOT EXISTS protect_audit_log_update
    \\BEFORE UPDATE ON ledger_audit_log
    \\BEGIN
    \\  SELECT RAISE(ABORT, 'audit log is immutable: UPDATE not allowed');
    \\END;
    ,
};
