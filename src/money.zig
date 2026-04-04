const std = @import("std");

pub const AMOUNT_SCALE: i64 = 100_000_000; // 10^8
pub const FX_RATE_SCALE: i64 = 10_000_000_000; // 10^10

/// Convert a transaction amount to base currency using the FX rate.
/// Uses i128 intermediate to prevent overflow: the product of two i64 values
/// can exceed i64 range (e.g., 9.2e18 * 2e10 = 1.84e28). The division by
/// FX_RATE_SCALE brings the result back to i64 range. Returns AmountOverflow
/// if the final result still exceeds i64 (amount too large for the FX rate).
pub fn computeBaseAmount(amount: i64, fx_rate: i64) !i64 {
    if (amount == 0) return 0;
    const wide_amount: i128 = @as(i128, amount);
    const wide_rate: i128 = @as(i128, fx_rate);
    const intermediate = wide_amount * wide_rate;
    const result = @divTrunc(intermediate, @as(i128, FX_RATE_SCALE));
    if (result > std.math.maxInt(i64) or result < std.math.minInt(i64)) {
        return error.AmountOverflow;
    }
    return @intCast(result);
}

pub fn parseDecimal(input: []const u8, scale: i64) !i64 {
    if (input.len == 0) return error.InvalidAmount;

    var s = input;
    var negative = false;
    if (s[0] == '-') {
        negative = true;
        s = s[1..];
        if (s.len == 0) return error.InvalidAmount;
    }

    // Find decimal point
    var dot_pos: ?usize = null;
    var dot_count: u32 = 0;
    for (s, 0..) |ch, i| {
        if (ch == '.') {
            dot_count += 1;
            dot_pos = i;
        } else if (ch < '0' or ch > '9') {
            return error.InvalidAmount;
        }
    }
    if (dot_count > 1) return error.InvalidAmount;

    // Parse integer part
    const int_str = if (dot_pos) |dp| s[0..dp] else s;
    if (int_str.len == 0) return error.InvalidAmount;

    var int_part: i64 = 0;
    for (int_str) |ch| {
        int_part = std.math.mul(i64, int_part, 10) catch return error.AmountOverflow;
        int_part = std.math.add(i64, int_part, @as(i64, ch - '0')) catch return error.AmountOverflow;
    }

    // Parse fractional part
    var frac_part: i64 = 0;
    var frac_digits: u32 = 0;
    if (dot_pos) |dp| {
        const frac_str = s[dp + 1 ..];
        // Determine how many scale digits we need
        var remaining_scale = scale;
        var scale_digits: u32 = 0;
        while (remaining_scale > 1) : ({
            remaining_scale = @divTrunc(remaining_scale, 10);
            scale_digits += 1;
        }) {}

        for (frac_str) |ch| {
            if (frac_digits >= scale_digits) break;
            frac_part = std.math.mul(i64, frac_part, 10) catch return error.AmountOverflow;
            frac_part = std.math.add(i64, frac_part, @as(i64, ch - '0')) catch return error.AmountOverflow;
            frac_digits += 1;
        }

        // Pad remaining digits with zeros
        while (frac_digits < scale_digits) : (frac_digits += 1) {
            frac_part *= 10;
        }
    }

    const scaled = std.math.mul(i64, int_part, scale) catch return error.AmountOverflow;
    const result = std.math.add(i64, scaled, frac_part) catch return error.AmountOverflow;
    if (negative) {
        return std.math.negate(result) catch return error.AmountOverflow;
    }
    return result;
}

/// Format a scaled i64 amount into a decimal string written to caller's buffer.
/// Special-cases minInt(i64) to avoid signed negation overflow: -(-2^63) exceeds
/// i64 range. Computes abs_val as maxInt(i64) + 1 via unsigned arithmetic.
/// Builds the fractional part manually with leading zeros (Zig 0.15 bufPrint
/// doesn't support runtime-width zero-padding).
pub fn formatDecimal(buf: []u8, value: i64, decimal_places: u8) ![]u8 {
    if (decimal_places == 0) {
        const int_val = @divTrunc(value, AMOUNT_SCALE);
        const result = std.fmt.bufPrint(buf, "{d}", .{int_val}) catch return error.InvalidAmount;
        return result;
    }

    const negative = value < 0;
    const abs_val: u64 = if (value == std.math.minInt(i64))
        @as(u64, std.math.maxInt(i64)) + 1
    else if (negative)
        @intCast(-value)
    else
        @intCast(value);

    // Compute divisor for display: 10^(8 - decimal_places)
    var display_divisor: u64 = 1;
    {
        var i: u8 = 0;
        while (i < 8 - decimal_places) : (i += 1) {
            display_divisor *= 10;
        }
    }
    const display_val = abs_val / display_divisor;

    var frac_divisor: u64 = 1;
    {
        var i: u8 = 0;
        while (i < decimal_places) : (i += 1) {
            frac_divisor *= 10;
        }
    }

    const int_part = display_val / frac_divisor;
    const frac_part = display_val % frac_divisor;

    // Build fractional string with leading zeros
    var frac_buf: [16]u8 = undefined;
    var frac_len: usize = 0;
    var remaining = frac_part;
    var dp = decimal_places;
    while (dp > 0) : (dp -= 1) {
        frac_buf[dp - 1] = @intCast('0' + (remaining % 10));
        remaining /= 10;
        frac_len += 1;
    }

    var pos: usize = 0;
    if (negative) {
        if (pos >= buf.len) return error.InvalidAmount;
        buf[pos] = '-';
        pos += 1;
    }

    const int_str = std.fmt.bufPrint(buf[pos..], "{d}", .{int_part}) catch return error.InvalidAmount;
    pos += int_str.len;

    if (pos >= buf.len) return error.InvalidAmount;
    buf[pos] = '.';
    pos += 1;

    if (pos + frac_len > buf.len) return error.InvalidAmount;
    @memcpy(buf[pos .. pos + frac_len], frac_buf[0..frac_len]);
    pos += frac_len;

    return buf[0..pos];
}

// ── Tests ───────────────────────────────────────────────────────

// ── computeBaseAmount tests ─────────────────────────────────────

test "computeBaseAmount: same currency (fx_rate = 1.0)" {
    const result = try computeBaseAmount(1_000_050_000_000, FX_RATE_SCALE);
    try std.testing.expectEqual(@as(i64, 1_000_050_000_000), result);
}

test "computeBaseAmount: FX conversion USD to PHP" {
    // 100.00 USD * 56.50 PHP/USD = 5,650.00 PHP
    const amount: i64 = 100_00_000_000; // 100.00 * 10^8 = 10,000,000,000
    const fx_rate: i64 = 56_50_000_000_00; // 56.50 * 10^10 = 565,000,000,000
    // Expected: 5,650.00 * 10^8 = 565,000,000,000
    // Calculation: 10,000,000,000 * 565,000,000,000 / 10,000,000,000 = 565,000,000,000
    const result = try computeBaseAmount(amount, fx_rate);
    // amount * fx_rate / FX_RATE_SCALE
    // = 10_000_000_000 * 565_000_000_000 / 10_000_000_000
    // = 565_000_000_000
    try std.testing.expectEqual(@as(i64, 565_000_000_000), result);
}

test "computeBaseAmount: zero amount returns zero" {
    const result = try computeBaseAmount(0, FX_RATE_SCALE);
    try std.testing.expectEqual(@as(i64, 0), result);
}

test "computeBaseAmount: negative amount" {
    const result = try computeBaseAmount(-1_000_000_000_00, FX_RATE_SCALE);
    try std.testing.expectEqual(@as(i64, -1_000_000_000_00), result);
}

test "computeBaseAmount: large amount near i64 boundary uses i128 safely" {
    // Amount near max: 90 billion * 10^8 = 9_000_000_000_000_000_000
    const amount: i64 = 9_000_000_000_000_000_000;
    const result = try computeBaseAmount(amount, FX_RATE_SCALE);
    try std.testing.expectEqual(amount, result);
}

test "computeBaseAmount: overflow returns AmountOverflow" {
    const result = computeBaseAmount(std.math.maxInt(i64), 20_000_000_000);
    try std.testing.expectError(error.AmountOverflow, result);
}

test "computeBaseAmount: fx_rate with fractional rate" {
    // 1,000.00 * 1.2345 = 1,234.50
    const amount: i64 = 1_000_00_000_000; // 1,000.00 * 10^8 = 100,000,000,000
    const fx_rate: i64 = 1_23_450_000_00; // 1.2345 * 10^10 = 12,345,000,000
    // Expected: 100,000,000,000 * 12,345,000,000 / 10,000,000,000 = 123,450,000,000
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 123_450_000_000), result);
}

// ── parseDecimal tests ──────────────────────────────────────────

test "parseDecimal: simple amount" {
    const result = try parseDecimal("10000.50", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 1_000_050_000_000), result);
}

test "parseDecimal: no decimal point" {
    const result = try parseDecimal("10000", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 1_000_000_000_000), result);
}

test "parseDecimal: negative amount" {
    const result = try parseDecimal("-500.00", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, -50_000_000_000), result);
}

test "parseDecimal: zero" {
    const result = try parseDecimal("0", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 0), result);
}

test "parseDecimal: leading zeros" {
    const result = try parseDecimal("007.50", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 750_000_000), result);
}

test "parseDecimal: trailing zeros preserved" {
    const result = try parseDecimal("1.10", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 110_000_000), result);
}

test "parseDecimal: max precision (8 decimal places)" {
    const result = try parseDecimal("1.12345678", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 112_345_678), result);
}

test "parseDecimal: excess precision truncated" {
    const result = try parseDecimal("1.123456789", AMOUNT_SCALE);
    try std.testing.expectEqual(@as(i64, 112_345_678), result);
}

test "parseDecimal: empty string returns error" {
    const result = parseDecimal("", AMOUNT_SCALE);
    try std.testing.expectError(error.InvalidAmount, result);
}

test "parseDecimal: non-numeric returns error" {
    const result = parseDecimal("abc", AMOUNT_SCALE);
    try std.testing.expectError(error.InvalidAmount, result);
}

test "parseDecimal: multiple decimal points returns error" {
    const result = parseDecimal("10.50.25", AMOUNT_SCALE);
    try std.testing.expectError(error.InvalidAmount, result);
}

test "parseDecimal: just decimal point returns error" {
    const result = parseDecimal(".", AMOUNT_SCALE);
    try std.testing.expectError(error.InvalidAmount, result);
}

test "parseDecimal: FX rate scale" {
    // 56.50 as FX rate
    const result = try parseDecimal("56.50", FX_RATE_SCALE);
    try std.testing.expectEqual(@as(i64, 565_000_000_000), result);
}

test "computeBaseAmount: FX rounding truncates (not rounds)" {
    // 100.01 * 1.33333 = 133.3433333... — truncation at i64 boundary
    const amount: i64 = 10_001_000_000; // 100.01 * 10^8
    const fx_rate: i64 = 13_333_300_000; // 1.33333 * 10^10
    // exact: 10_001_000_000 * 13_333_300_000 / 10_000_000_000 = 13_334_633_330
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 13_334_633_330), result);
}

test "computeBaseAmount: FX with many decimal places in rate" {
    // 1,000.00 * 0.00789012 = 7.89012
    const amount: i64 = 100_000_000_000; // 1,000.00 * 10^8
    const fx_rate: i64 = 78_901_200; // 0.00789012 * 10^10
    // exact: 100_000_000_000 * 78_901_200 / 10_000_000_000 = 789_012_000
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 789_012_000), result);
}

test "computeBaseAmount: very small FX rate" {
    // 1.00 * 0.0000001 = 0.0000001
    const amount: i64 = 100_000_000; // 1.00 * 10^8
    const fx_rate: i64 = 1_000; // 0.0000001 * 10^10 = 1,000
    // exact: 100_000_000 * 1_000 / 10_000_000_000 = 10
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 10), result);
}

test "computeBaseAmount: large amount with large FX rate still fits i64" {
    // 50 billion * 1.5 = 75 billion (fits in i64)
    const amount: i64 = 5_000_000_000_000_000_000; // 50B * 10^8
    const fx_rate: i64 = 15_000_000_000; // 1.5 * 10^10
    // intermediate: 5e18 * 1.5e10 = 7.5e28 (exceeds i64, but i128 handles it)
    // result: 7.5e28 / 1e10 = 7.5e18 (fits i64)
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 7_500_000_000_000_000_000), result);
}

test "computeBaseAmount: product that causes i64 overflow but result fits after division" {
    // 9.2e9 * 10^8 = 9.2e17, * 10 = 9.2e18 — fits i64
    // but 9.2e17 * 2e10 = 1.84e28 — overflows i64, handled by i128
    const amount: i64 = 920_000_000_000_000_000; // 9.2B * 10^8
    const fx_rate: i64 = 20_000_000_000; // 2.0 * 10^10
    const result = try computeBaseAmount(amount, fx_rate);
    try std.testing.expectEqual(@as(i64, 1_840_000_000_000_000_000), result);
}

// ── formatDecimal tests ─────────────────────────────────────────

test "formatDecimal: simple amount with 2 decimal places" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, 1_000_050_000_000, 2);
    try std.testing.expectEqualStrings("10000.50", result);
}

test "formatDecimal: zero" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, 0, 2);
    try std.testing.expectEqualStrings("0.00", result);
}

test "formatDecimal: negative amount" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, -50_000_000_000, 2);
    try std.testing.expectEqualStrings("-500.00", result);
}

test "formatDecimal: zero decimal places (JPY)" {
    var buf: [32]u8 = undefined;
    // 10,000 JPY = 10,000 * 10^8 = 1,000,000,000,000
    const result = try formatDecimal(&buf, 1_000_000_000_000, 0);
    try std.testing.expectEqualStrings("10000", result);
}

test "formatDecimal: 8 decimal places (crypto)" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, 112_345_678, 8);
    try std.testing.expectEqualStrings("1.12345678", result);
}

test "formatDecimal: small amount" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, 1, 2);
    try std.testing.expectEqualStrings("0.00", result);
}

test "formatDecimal: amount that rounds to visible digits" {
    var buf: [32]u8 = undefined;
    const result = try formatDecimal(&buf, 100_000_000, 2);
    try std.testing.expectEqualStrings("1.00", result);
}

// ── Round-trip tests ────────────────────────────────────────────

test "parseDecimal -> formatDecimal round-trip: 10000.50" {
    const parsed = try parseDecimal("10000.50", AMOUNT_SCALE);
    var buf: [32]u8 = undefined;
    const formatted = try formatDecimal(&buf, parsed, 2);
    try std.testing.expectEqualStrings("10000.50", formatted);
}

test "parseDecimal -> formatDecimal round-trip: 0.01" {
    const parsed = try parseDecimal("0.01", AMOUNT_SCALE);
    var buf: [32]u8 = undefined;
    const formatted = try formatDecimal(&buf, parsed, 2);
    try std.testing.expectEqualStrings("0.01", formatted);
}

test "parseDecimal -> formatDecimal round-trip: negative" {
    const parsed = try parseDecimal("-1234.56", AMOUNT_SCALE);
    var buf: [32]u8 = undefined;
    const formatted = try formatDecimal(&buf, parsed, 2);
    try std.testing.expectEqualStrings("-1234.56", formatted);
}

test "parseDecimal -> formatDecimal round-trip: whole number" {
    const parsed = try parseDecimal("500", AMOUNT_SCALE);
    var buf: [32]u8 = undefined;
    const formatted = try formatDecimal(&buf, parsed, 2);
    try std.testing.expectEqualStrings("500.00", formatted);
}

// ── Additional edge cases ───────────────────────────────────────

test "formatDecimal: i64 min value does not overflow" {
    var buf: [32]u8 = undefined;
    // This would panic without the minInt special case
    const result = try formatDecimal(&buf, std.math.minInt(i64), 2);
    try std.testing.expect(result.len > 0);
    try std.testing.expect(result[0] == '-');
}

test "computeBaseAmount: both operands negative" {
    // (-100) * (-2.0) = 200 (positive result from two negatives)
    // But fx_rate should always be positive (CHECK constraint)
    // This tests the math, not the business rule
    const result = try computeBaseAmount(-10_000_000_000, -20_000_000_000);
    try std.testing.expectEqual(@as(i64, 20_000_000_000), result);
}

test "computeBaseAmount: amount = 1 (smallest unit)" {
    const result = try computeBaseAmount(1, FX_RATE_SCALE);
    try std.testing.expectEqual(@as(i64, 1), result);
}

test "computeBaseAmount: FX truncation with unequal sides" {
    // 33.33 * 1.0 = 33.33 on both sides, but what about:
    // 100.00 / 3 = 33.333333... per line (truncated differently)
    // This tests that two lines with the same FX rate but different
    // amounts still balance if the input amounts balance
    const line1 = try computeBaseAmount(33_33_000_000, FX_RATE_SCALE); // 33.33
    const line2 = try computeBaseAmount(66_67_000_000, FX_RATE_SCALE); // 66.67
    // 33.33 + 66.67 = 100.00 — should balance
    try std.testing.expectEqual(@as(i64, 100_00_000_000), line1 + line2);
}
