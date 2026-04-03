const std = @import("std");

pub const db = @import("db.zig");
pub const schema = @import("schema.zig");

comptime {
    _ = db;
    _ = schema;
}
