const std = @import("std");

const sqlite_flags = &.{
    "-DSQLITE_DQS=0",
    "-DSQLITE_THREADSAFE=0",
    "-DSQLITE_OMIT_DEPRECATED",
    "-DSQLITE_OMIT_PROGRESS_CALLBACK",
    "-DSQLITE_DEFAULT_MEMSTATUS=0",
    "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
    "-DSQLITE_OMIT_SHARED_CACHE",
    "-DSQLITE_ENABLE_STAT4",
};

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
        .flags = sqlite_flags,
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

    const abi_test_mod = b.createModule(.{
        .root_source_file = b.path("src/abi_integration_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "heft", .module = mod },
        },
    });

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/benchmark.zig"),
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

    const bench_exe = b.addExecutable(.{
        .name = "heft-bench",
        .root_module = bench_mod,
    });

    const example_mod = b.createModule(.{
        .root_source_file = b.path("examples/basic_heft.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "heft", .module = mod },
        },
    });

    const example_exe = b.addExecutable(.{
        .name = "heft-example-basic",
        .root_module = example_mod,
    });

    const oble_roundtrip_mod = b.createModule(.{
        .root_source_file = b.path("examples/oble_roundtrip.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "heft", .module = mod },
        },
    });

    const oble_roundtrip_exe = b.addExecutable(.{
        .name = "heft-example-oble-roundtrip",
        .root_module = oble_roundtrip_mod,
    });

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const abi_tests = b.addTest(.{ .root_module = abi_test_mod });
    const run_abi_tests = b.addRunArtifact(abi_tests);

    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench.addArgs(args);

    const run_example = b.addRunArtifact(example_exe);
    const run_oble_roundtrip = b.addRunArtifact(oble_roundtrip_exe);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_abi_tests.step);

    const test_mod_step = b.step("test-mod", "Run module tests");
    test_mod_step.dependOn(&run_mod_tests.step);

    const test_abi_step = b.step("test-abi", "Run ABI integration tests");
    test_abi_step.dependOn(&run_abi_tests.step);

    const bench_step = b.step("bench", "Run lightweight performance benchmarks");
    bench_step.dependOn(&run_bench.step);

    const example_step = b.step("example-basic", "Run the basic Heft example");
    example_step.dependOn(&run_example.step);

    const oble_roundtrip_step = b.step("example-oble-roundtrip", "Run the OBLE round-trip example");
    oble_roundtrip_step.dependOn(&run_oble_roundtrip.step);

    const safe_mod = b.addModule("heft-safe", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    safe_mod.addCSourceFile(.{
        .file = b.path("vendor/sqlite3.c"),
        .flags = sqlite_flags,
    });
    safe_mod.addIncludePath(b.path("vendor/"));
    safe_mod.link_libc = true;

    const safe_main_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
        .imports = &.{
            .{ .name = "heft", .module = safe_mod },
        },
    });

    const safe_lib = b.addLibrary(.{
        .name = "heft-safe",
        .root_module = safe_main_mod,
        .linkage = .static,
    });

    const check_step = b.step("check", "Build in ReleaseSafe to verify no UB");
    check_step.dependOn(&safe_lib.step);
}
