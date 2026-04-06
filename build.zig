const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("heft", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    mod.addCSourceFile(.{
        .file = b.path("vendor/sqlite3.c"),
        .flags = &.{
            "-DSQLITE_DQS=0",
            "-DSQLITE_THREADSAFE=0",
            "-DSQLITE_OMIT_DEPRECATED",
            "-DSQLITE_DEFAULT_MEMSTATUS=0",
            "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
            "-DSQLITE_OMIT_SHARED_CACHE",
        },
    });
    mod.addIncludePath(b.path("vendor/"));
    mod.link_libc = true;

    const main_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "heft", .module = mod },
        },
    });

    const static_lib = b.addLibrary(.{
        .name = "heft",
        .root_module = main_mod,
        .linkage = .static,
    });
    b.installArtifact(static_lib);

    const shared_lib = b.addLibrary(.{
        .name = "heft",
        .root_module = main_mod,
        .linkage = .dynamic,
    });
    b.installArtifact(shared_lib);

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const main_tests = b.addTest(.{ .root_module = main_mod });
    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_main_tests.step);
}
