const std = @import("std");

pub const VERSION = "0.1.0";

test "VERSION matches build.zig.zon package version" {
    const zon = try std.fs.cwd().readFileAlloc(std.testing.allocator, "build.zig.zon", 64 * 1024);
    defer std.testing.allocator.free(zon);
    const needle = ".version = \"" ++ VERSION ++ "\"";
    try std.testing.expect(std.mem.indexOf(u8, zon, needle) != null);
}
