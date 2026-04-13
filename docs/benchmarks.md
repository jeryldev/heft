# Benchmarks

Heft includes an in-memory benchmark harness driven by:

- `zig build bench`

## Benchmark philosophy

The benchmark surface is intended to answer three different questions:

1. How expensive is workload setup and fixture generation?
2. How expensive is the actual operation under test?
3. How does performance scale with larger datasets?

Those are intentionally separated now.

## Main scenario groups

### Read scenarios

- `trial_balance`
- `general_ledger`
- `aged_subledger`

These run against one seeded reporting workload and print:

- setup time
- operation time

### Seed scenarios

- `seed_report`
- `seed_close`
- `seed_revalue`

These measure data generation explicitly.

### Close scenarios

- `close_direct`
- `close_income_summary`
- `close_allocated`
- `close_period`

These print both:

- setup vs operation time
- internal close-phase timing

### Close component scenarios

- `recalculate_stale`
- `post_generated_entry`
- `generate_opening_entry`

These isolate individual pieces of close-related behavior.

### Statement scenarios

- `statement_suite`

This runs the main financial statements on one seeded workload:

- income statement
- trial balance movement
- balance sheet
- balance sheet with projected retained earnings

### Comparative scenarios

- `comparative_suite`

This runs the comparative reporting family on one seeded workload:

- trial balance comparative
- income statement comparative
- balance sheet comparative
- trial balance movement comparative
- equity changes

### Export scenarios

- `export_suite`

This runs the main export surfaces on one seeded workload:

- chart of accounts
- journal entries
- audit trail
- periods
- subledger
- book metadata

### Budget scenarios

- `budget_suite`

This runs `budgetVsActual` on a seeded reporting workload with generated budget lines.

### Open item scenarios

- `open_item_suite`

This runs open-item listing for both active-only and mixed-status views.

### Classification scenarios

- `classification_suite`

This runs the classification-driven reporting family on one seeded workload:

- classified balance sheet
- classified income statement
- classified trial balance
- direct cash flow
- indirect cash flow

### Dimension scenarios

- `dimension_suite`

This runs the dimension analysis family on one seeded workload:

- dimension summary
- dimension summary rollup
- dimension listing
- dimension value listing

### ABI buffer scenarios

- `abi_buffer_suite`

This runs key C ABI buffer endpoints to measure marshaling plus engine cost:

- list entries
- list audit log
- list transactions
- dimension summary
- list open items
- export journal entries

### Scale scenarios

- `query_scale`

This runs the main read/report surfaces at multiple dataset sizes.

## Current interpretation of results

The main lessons from the current harness are:

- fixture generation cost is large and must not be confused with engine speed
- `closePeriod()` internals are relatively fast in `ReleaseFast`
- the main statement, comparative, export, budget, open-item, classification, and dimension families now have direct benchmark coverage
- key C ABI buffer endpoints now have direct benchmark coverage too
- report/query scaling is still a more meaningful optimization frontier than period close

## Example commands

```bash
zig build bench
zig build bench -- --scenario seed_report --write-iterations 3 --report-entries 5000 --counterparties 128
zig build bench -- --scenario close_period --write-iterations 5 --close-entries 2000
zig build bench -- --scenario statement_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario comparative_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario export_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario budget_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario open_item_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario classification_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario dimension_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario abi_buffer_suite --read-iterations 10 --report-entries 2000 --counterparties 128
zig build bench -- --scenario query_scale --read-iterations 10 --report-entries 2000 --counterparties 128
```

## How to use benchmark output

Use the output to answer:

- is setup dominating the total?
- is operation time scaling acceptably?
- are changes improving the target operation or only the fixture path?

Do not compare benchmark runs casually unless:

- optimization mode is the same
- scenario parameters are the same
- setup and operation output are both considered

## Recommended release benchmark gate

For release confidence, a small benchmark smoke gate is useful:

- `query_scale`
- `close_period`
- `revalue`

The goal is not strict perf budgeting yet. The goal is to catch obvious
regressions and preserve measurement discipline.
