# Release Checklist

This checklist is for preparing a public release of Heft.

## Version and contract

- update version in `build.zig.zon` if needed
- confirm `src/version.zig` matches package version
- confirm `include/heft.h` matches public ABI behavior
- confirm `HEFT_SCHEMA_VERSION` matches schema version
- confirm release notes under `docs/releases/` match the target tag
- confirm `docs/stability.md` still reflects current expectations

## Code quality gate

- run `zig fmt`
- run `zig build test`
- run `zig build check`
- run `zig build bench`
- or run `bash scripts/quality-gate.sh`

## Build reproducibility gate

- record the exact Zig toolchain version used for the release
- record the vendored SQLite version in the release notes if it changed
- build from a clean worktree
- note whether symbols were stripped for release artifacts
- avoid shipping production builds with debug logging enabled

## Benchmark gate

Recommended smoke scenarios:

- `zig build bench -- --scenario query_scale --read-iterations 3 --report-entries 500 --counterparties 32`
- `zig build bench -- --scenario close_period --write-iterations 3 --close-entries 120`
- `zig build bench -- --scenario revalue --write-iterations 3 --revalue-entries 80`

Review:

- setup time vs operation time
- obvious query-scale regressions
- obvious close/revalue regressions

## Docs gate

- `README.md` reflects current scope
- `docs/architecture.md` is current
- `docs/designations.md` is current
- `docs/lifecycle.md` is current
- `docs/embedding.md` is current
- `docs/benchmarks.md` is current

## ABI gate

- C ABI tests pass
- error codes are in sync
- result ownership rules are documented
- thread-safety warning is present

## Accounting policy gate

- designation-dependent workflows are documented
- entity-type close behavior is documented
- opening carry-forward behavior is documented
- reopen cascade behavior is documented
- suspense/open-item controls are documented

## Release framing

- decide release number
- decide stability statement
- decide whether breaking changes are expected in the next cycle
- record benchmark baseline for the release
- ensure release notes exist for the target version

## Suggested first public release position

If releasing before `1.0`, present Heft as:

- embedded accounting engine
- SQLite-backed
- Zig-first with C ABI
- strong on accounting invariants and lifecycle
- still evolving in API and packaging
