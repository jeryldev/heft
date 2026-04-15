const std = @import("std");
const heft = @import("heft");

pub const VERSION = heft.version.VERSION;

pub const LedgerDB = struct {
    sqlite: heft.db.Database,
};

threadlocal var last_error_code: i32 = 0;

pub fn ledgerLastError() i32 {
    return last_error_code;
}

pub fn setError(code: i32) void {
    last_error_code = code;
}

pub fn invalidHandleBool() bool {
    setError(mapError(error.InvalidInput));
    return false;
}

pub fn invalidHandleI64() i64 {
    setError(mapError(error.InvalidInput));
    return -1;
}

pub fn invalidHandleI32() i32 {
    setError(mapError(error.InvalidInput));
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
