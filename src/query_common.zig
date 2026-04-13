const std = @import("std");

pub const SortOrder = enum { asc, desc };

pub const DEFAULT_LIMIT: i32 = 100;
pub const MAX_LIMIT: i32 = 1000;

pub fn clampLimit(limit: i32) i32 {
    if (limit <= 0) return DEFAULT_LIMIT;
    if (limit > MAX_LIMIT) return MAX_LIMIT;
    return limit;
}

pub fn clampOffset(offset: i32) i32 {
    if (offset < 0) return 0;
    return offset;
}

pub fn writeJsonMeta(buf: []u8, total: i64, limit: i32, offset: i32) !usize {
    const has_more = @as(i64, offset) + @as(i64, limit) < total;
    const s = std.fmt.bufPrint(buf, "{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"has_more\":{s},\"rows\":[", .{
        total, limit, offset, if (has_more) "true" else "false",
    }) catch return error.BufferTooSmall;
    return s.len;
}

pub fn writeCsvMeta(buf: []u8, total: i64, limit: i32, offset: i32) !usize {
    const s = std.fmt.bufPrint(buf, "# total={d},limit={d},offset={d}\n", .{
        total, limit, offset,
    }) catch return error.BufferTooSmall;
    return s.len;
}
