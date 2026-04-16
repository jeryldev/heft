const std = @import("std");
const heft = @import("heft");

pub const VERSION = heft.version.VERSION;

pub const LedgerDB = struct {
    sqlite: heft.db.Database,
};

threadlocal var last_error_code: i32 = 0;
threadlocal var last_error_message_buf: [256]u8 = [_]u8{0} ** 256;
threadlocal var last_error_name_buf: [64]u8 = [_]u8{0} ** 64;

pub fn ledgerLastError() i32 {
    return last_error_code;
}

pub fn ledgerLastErrorMessage() [*:0]const u8 {
    return @ptrCast(&last_error_message_buf);
}

pub fn ledgerLastErrorName() [*:0]const u8 {
    return @ptrCast(&last_error_name_buf);
}

fn copyZ(dest: []u8, src: []const u8) void {
    if (dest.len == 0) return;
    const copy_len = @min(src.len, dest.len - 1);
    if (copy_len > 0) @memcpy(dest[0..copy_len], src[0..copy_len]);
    dest[copy_len] = 0;
    if (copy_len + 1 < dest.len) @memset(dest[copy_len + 1 ..], 0);
}

pub fn errorCodeName(code: i32) []const u8 {
    return switch (code) {
        0 => "HEFT_OK",
        1 => "HEFT_NOT_FOUND",
        2 => "HEFT_INVALID_INPUT",
        3 => "HEFT_PERIOD_CLOSED",
        4 => "HEFT_PERIOD_LOCKED",
        5 => "HEFT_ALREADY_POSTED",
        6 => "HEFT_UNBALANCED_ENTRY",
        7 => "HEFT_DUPLICATE_NUMBER",
        8 => "HEFT_INVALID_TRANSITION",
        9 => "HEFT_ACCOUNT_INACTIVE",
        10 => "HEFT_MISSING_COUNTERPARTY",
        11 => "HEFT_INVALID_COUNTERPARTY",
        12 => "HEFT_AMOUNT_OVERFLOW",
        13 => "HEFT_VOID_REASON_REQUIRED",
        14 => "HEFT_REVERSE_REASON_REQUIRED",
        15 => "HEFT_CIRCULAR_REFERENCE",
        16 => "HEFT_TOO_FEW_LINES",
        17 => "HEFT_SCHEMA_VERSION_MISMATCH",
        18 => "HEFT_OUT_OF_MEMORY",
        20 => "HEFT_INVALID_AMOUNT",
        21 => "HEFT_BOOK_ARCHIVED",
        22 => "HEFT_CROSS_BOOK_VIOLATION",
        23 => "HEFT_INVALID_FX_RATE",
        24 => "HEFT_INVALID_DECIMAL_PLACES",
        25 => "HEFT_BUFFER_TOO_SMALL",
        26 => "HEFT_RETAINED_EARNINGS_REQUIRED",
        27 => "HEFT_FX_GAIN_LOSS_REQUIRED",
        28 => "HEFT_OPENING_BALANCE_REQUIRED",
        29 => "HEFT_INCOME_SUMMARY_REQUIRED",
        30 => "HEFT_APPROVAL_REQUIRED",
        31 => "HEFT_TOO_MANY_ACCOUNTS",
        32 => "HEFT_PERIOD_NOT_IN_BALANCE",
        33 => "HEFT_NO_NEXT_PERIOD",
        34 => "HEFT_CANNOT_REOPEN_CASCADE",
        35 => "HEFT_EQUITY_ALLOCATION_REQUIRED",
        36 => "HEFT_EQUITY_ALLOCATION_TOTAL_INVALID",
        37 => "HEFT_SUSPENSE_NOT_CLEAR",
        38 => "HEFT_EQUITY_CLOSE_TARGET_REQUIRED",
        39 => "HEFT_TOO_MANY_IMPORT_IDS",
        40 => "HEFT_PAYLOAD_TOO_LARGE",
        41 => "HEFT_PERIOD_HAS_DRAFTS",
        90 => "HEFT_SQLITE_OPEN_FAILED",
        91 => "HEFT_SQLITE_EXEC_FAILED",
        92 => "HEFT_SQLITE_PREPARE_FAILED",
        93 => "HEFT_SQLITE_STEP_FAILED",
        94 => "HEFT_SQLITE_BIND_FAILED",
        else => "HEFT_UNKNOWN",
    };
}

pub fn errorCodeMessage(code: i32) []const u8 {
    return switch (code) {
        0 => "no error",
        1 => "record not found",
        2 => "invalid input",
        3 => "period is closed",
        4 => "period is locked",
        5 => "entry is already posted",
        6 => "entry is not balanced",
        7 => "duplicate number",
        8 => "invalid status transition",
        9 => "account is inactive",
        10 => "counterparty is required for this account",
        11 => "invalid counterparty for this account",
        12 => "amount overflow",
        13 => "void reason is required",
        14 => "reverse reason is required",
        15 => "circular reference detected",
        16 => "entry must contain at least two lines",
        17 => "schema version mismatch",
        18 => "out of memory",
        20 => "invalid amount",
        21 => "book is archived",
        22 => "cross-book reference is not allowed",
        23 => "invalid FX rate",
        24 => "invalid decimal places",
        25 => "buffer too small",
        26 => "retained earnings account is required before close",
        27 => "FX gain/loss account is required",
        28 => "opening balance account is required",
        29 => "income summary account is required",
        30 => "approval is required before posting",
        31 => "too many accounts for this operation",
        32 => "period is not in balance",
        33 => "next period does not exist",
        34 => "cannot reopen because a later period depends on this close",
        35 => "equity allocation is required",
        36 => "equity allocation total is invalid",
        37 => "suspense account is not clear",
        38 => "equity close target is required",
        39 => "too many import identifiers",
        40 => "payload exceeds configured limit",
        41 => "period cannot close while draft entries still exist",
        90 => "failed to open SQLite database",
        91 => "SQLite execution failed",
        92 => "SQLite prepare failed",
        93 => "SQLite step failed",
        94 => "SQLite bind failed",
        else => "unknown error",
    };
}

pub fn setError(code: i32) void {
    last_error_code = code;
    copyZ(last_error_name_buf[0..], errorCodeName(code));
    copyZ(last_error_message_buf[0..], errorCodeMessage(code));
}

pub fn setErrorMessage(code: i32, message: []const u8) void {
    last_error_code = code;
    copyZ(last_error_name_buf[0..], errorCodeName(code));
    copyZ(last_error_message_buf[0..], message);
}

pub fn invalidHandleBool() bool {
    setErrorMessage(mapError(error.InvalidInput), "invalid ledger handle");
    return false;
}

pub fn invalidHandleI64() i64 {
    setErrorMessage(mapError(error.InvalidInput), "invalid ledger handle");
    return -1;
}

pub fn invalidHandleI32() i32 {
    setErrorMessage(mapError(error.InvalidInput), "invalid ledger handle");
    return -1;
}

pub fn mapError(err: anyerror) i32 {
    return switch (err) {
        error.NotFound => 1,
        error.InvalidInput => 2,
        error.PeriodClosed => 3,
        error.PeriodLocked => 4,
        error.AlreadyPosted => 5,
        error.UnbalancedEntry => 6,
        error.DuplicateNumber => 7,
        error.InvalidTransition => 8,
        error.AccountInactive => 9,
        error.MissingCounterparty => 10,
        error.InvalidCounterparty => 11,
        error.AmountOverflow => 12,
        error.VoidReasonRequired => 13,
        error.ReverseReasonRequired => 14,
        error.CircularReference => 15,
        error.TooFewLines => 16,
        error.SchemaVersionMismatch => 17,
        error.OutOfMemory => 18,
        // 19 intentionally reserved to preserve ABI numbering continuity.
        error.InvalidAmount => 20,
        error.BookArchived => 21,
        error.CrossBookViolation => 22,
        error.InvalidFxRate => 23,
        error.InvalidDecimalPlaces => 24,
        error.BufferTooSmall => 25,
        error.NoSpaceLeft => 25,
        error.RetainedEarningsAccountRequired => 26,
        error.EquityCloseTargetRequired => 38,
        error.FxGainLossAccountRequired => 27,
        error.OpeningBalanceAccountRequired => 28,
        error.IncomeSummaryAccountRequired => 29,
        error.ApprovalRequired => 30,
        error.TooManyAccounts => 31,
        error.PeriodNotInBalance => 32,
        error.NoNextPeriod => 33,
        error.CannotReopenCascade => 34,
        error.EquityAllocationRequired => 35,
        error.EquityAllocationTotalInvalid => 36,
        error.SuspenseNotClear => 37,
        error.TooManyImportIds => 39,
        error.PayloadTooLarge => 40,
        error.PeriodHasDrafts => 41,
        error.SqliteOpenFailed => 90,
        error.SqliteExecFailed => 91,
        error.SqlitePrepareFailed => 92,
        error.SqliteStepFailed => 93,
        error.SqliteBindFailed => 94,
        else => 99,
    };
}

pub fn safeBuf(buf: ?[*]u8, buf_len: i32) ?[]u8 {
    const b = buf orelse return null;
    if (buf_len <= 0) return null;
    const len: usize = @intCast(buf_len);
    return b[0..len];
}

pub fn safeIntCast(val: usize) i32 {
    if (val > @as(usize, @intCast(std.math.maxInt(i32)))) return std.math.maxInt(i32);
    return @intCast(val);
}
