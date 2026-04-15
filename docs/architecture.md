# Heft Architecture

Heft is an embedded accounting engine built around one core idea:

- the book of account is the boundary for accounting policy, invariants, and lifecycle

That boundary is implemented with SQLite transactions, explicit audit logging,
book-scoped designations, and deterministic fixed-point arithmetic.

## System shape

The major layers are:

- storage and schema
- accounting domain modules
- read/query/report modules
- C ABI wrappers
- benchmark and test surfaces

The storage layer is SQLite with:

- foreign keys enabled
- integer money and FX storage
- explicit schema versioning
- views and indexes for reporting
- append-only audit log protection

The domain layer is organized around accounting objects:

- `book.zig`
- `account.zig`
- `period.zig`
- `entry.zig`
- `close.zig`
- `revaluation.zig`
- `open_item.zig`
- `subledger.zig`
- `classification.zig`
- `dimension.zig`
- `budget.zig`
- `batch.zig`
- `verify.zig`

The read layer is split between:

- reports returning heap-allocated typed results
- buffer-oriented query/export functions returning JSON or CSV payloads

The public integration surfaces are:

- Zig API via `src/root.zig`
- C ABI via `include/heft.h`

## Design principles

Heft is intentionally opinionated in a few places.

- Everything important happens in journal entries.
- Books carry accounting policy through explicit designations.
- Period state matters and is enforced.
- Amounts and rates are integers, never floats.
- Mutations write audit records in the same transaction.
- Derived caches exist for speed, but verification exists to re-check them.
- Journal history and audit history are append-only. Some configuration
  entities can still be deleted, but those deletions are themselves audited.

## Book as policy boundary

A book is not only a container for accounts and periods. It is also the place
where accounting behavior is configured.

Examples of book-scoped policy:

- base currency
- decimal places
- rounding account
- FX gain/loss account
- retained earnings / equity close target
- income summary account
- opening balance account
- suspense account
- dividends/drawings account
- fiscal-year start month
- entity type
- approval requirements

This is the core architectural choice behind Heft. The posting engine stays
generic, while book designations decide how lifecycle operations behave.

## Core data flow

The normal lifecycle looks like this:

1. Create a book and chart of accounts.
2. Create periods.
3. Configure designations required by the intended lifecycle.
4. Create draft entries.
5. Add and edit lines while the entry is still draft.
6. Post the entry, which computes base amounts, validates invariants, updates
   per-period balance cache, and writes audit records.
7. Query reports and subledger views.
8. Close periods, generate opening carry-forward entries, and reopen with
   cascade handling when needed.

## Important storage conventions

- amounts are `i64` scaled by `10^8`
- FX rates are `i64` scaled by `10^10`
- dates are `YYYY-MM-DD`
- timestamps are UTC ISO 8601 text
- one `LedgerDB` handle per thread

## Caching model

Heft maintains `ledger_account_balances` as a per-account, per-period balance
cache. This is not the source of truth. It is a derived optimization layer.

The source of truth remains:

- journal entries
- journal entry lines
- related book/period/account metadata

The cache exists to make period-oriented reporting and close workflows fast.

Supporting mechanisms:

- stale marking when future periods may be affected
- targeted recalculation of stale periods
- verification routines that compare cache state to recomputed truth

One tradeoff is worth naming directly: when a post backdates into an earlier
period, Heft marks later cached periods stale so reports and close flows cannot
silently rely on invalid derived balances. That is the right correctness choice,
but sequential close work across many periods can trigger repeated stale
recalculation if you close one period at a time. In practice that is a lifecycle
cost, not a posting hot-path cost, and it is one reason the benchmark surface
measures `recalculate_stale`, `close_period`, and opening-entry generation
separately.

## Reporting model

Reporting is intentionally split into two styles:

- typed result reports for major statements and ledgers
- buffer-based query/export endpoints for embeddings that want JSON or CSV

Key report families:

- trial balance
- balance sheet
- income statement
- movement and comparative reports
- general ledger
- journal register
- equity changes
- classified reports and cash flow
- subledger reconciliation and aging

## Close and reopen model

Close is book-driven and period-aware.

- regular periods can transition through open, soft-closed, closed, and locked
- close can be direct, income-summary based, or allocation based depending on
  book designations and entity type
- year-end can sweep dividends/drawings
- opening balances are carried to the next period through a real opening entry
- reopen cascades by voiding stale closing/opening entries

## Verification and safety

The engine has explicit safety features beyond schema constraints.

- posting-time domain validation
- cross-book checks
- status-transition rules
- period-state enforcement
- control-account and counterparty enforcement
- open-item lifecycle protection
- verification of cache and accounting invariants

## Threading and deployment model

Heft is designed as an embedded engine, not a network service.

- use one handle per thread
- do not share a handle across threads
- treat the `.ledger` file as the accounting database boundary
- compose Heft with your own UI, service layer, or application logic

## Why the architecture looks this way

Heft is trying to be a reliable accounting core for applications, not a generic
ORM schema and not a hosted accounting platform.

That is why it favors:

- explicit accounting lifecycle rules
- book-scoped policy via designations
- deterministic storage conventions
- direct SQL plus tests over framework indirection
