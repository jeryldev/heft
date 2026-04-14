#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${HEFT_CACHE_DIR:-/tmp/heft-zig-cache}"
GLOBAL_CACHE_DIR="${HEFT_GLOBAL_CACHE_DIR:-/tmp/heft-zig-global-cache}"

cd "$ROOT"

echo "[1/6] zig fmt"
zig_files=()
while IFS= read -r file; do
  zig_files+=("$file")
done < <(find src -type f -name '*.zig' | sort)
zig fmt --check "${zig_files[@]}" build.zig

echo "[2/6] zig build check"
zig build check --cache-dir "$CACHE_DIR" --global-cache-dir "$GLOBAL_CACHE_DIR"

echo "[3/6] zig build test"
zig build test --cache-dir "$CACHE_DIR" --global-cache-dir "$GLOBAL_CACHE_DIR"

echo "[4/6] benchmark smoke"
zig build -Doptimize=ReleaseFast bench --cache-dir "$CACHE_DIR" --global-cache-dir "$GLOBAL_CACHE_DIR" -- --scenario query_scale --read-iterations 3 --report-entries 500 --counterparties 32
zig build -Doptimize=ReleaseFast bench --cache-dir "$CACHE_DIR" --global-cache-dir "$GLOBAL_CACHE_DIR" -- --scenario close_period --write-iterations 3 --close-entries 120
zig build -Doptimize=ReleaseFast bench --cache-dir "$CACHE_DIR" --global-cache-dir "$GLOBAL_CACHE_DIR" -- --scenario abi_buffer_suite --read-iterations 3 --report-entries 500 --counterparties 32

echo "[5/6] OBLE validation"
bash scripts/validate-oble.sh

echo "[6/6] done"
