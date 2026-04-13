# Embedding Heft

Heft is designed to be embedded inside another application.

You can use it from:

- Zig
- C
- any language that can call the C ABI

## Integration model

The host application is expected to provide:

- application UI
- authentication and authorization
- business-specific metadata and workflow
- network/service layer if needed
- deployment and file management

Heft provides:

- ledger storage
- accounting lifecycle rules
- reporting/query/export surfaces
- audit logging

## Basic embedding flow

1. Open a ledger handle.
2. Create a book.
3. Create accounts and periods.
4. Configure book designations.
5. Create and post entries.
6. Query reports and subledger views.
7. Run close/reopen/revalue operations as needed.

## Zig entry point

Use `src/root.zig` as the primary Zig surface.

Main modules are exported there, including:

- `book`
- `account`
- `period`
- `entry`
- `report`
- `query_mod`
- `close`
- `revaluation`
- `open_item`

## C ABI entry point

Use [include/heft.h](/Users/jeryldev/code/zig_projects/heft/include/heft.h).

The C ABI gives you:

- handle lifecycle
- mutation operations
- buffer-based read APIs
- heap-allocated report APIs
- error-code retrieval

## Handle rules

Important:

- one `LedgerDB` handle per thread
- handles are not thread-safe
- do not share the same handle across threads

## Memory rules

There are two read styles in the C ABI.

### Buffer APIs

Examples:

- `ledger_list_*`
- `ledger_get_*`
- `ledger_export_*`

These write JSON or CSV into a caller-provided buffer and return the number of
bytes written.

### Heap result APIs

Examples:

- `ledger_trial_balance`
- `ledger_general_ledger`
- `ledger_balance_sheet`

These return heap-allocated results and must be freed with the matching
`ledger_free_*` function.

## Error handling

General rule:

- integer returns use `-1` on failure
- boolean returns use `false` on failure
- pointer returns use `NULL` on failure

After failure, call:

- `ledger_last_error()`

## Suggested host responsibilities

Your application should usually own:

- user identity mapping to `performed_by`
- chart-of-accounts UX
- designation setup UX
- entry creation screens
- workflow approval UX
- document attachments
- external tax/regulatory logic beyond the ledger core

## Recommended startup flow

For a new embedded deployment:

1. create/open the ledger
2. create the book
3. load or create the chart
4. configure required designations
5. create periods
6. optionally create subledger groups/accounts
7. begin posting and reporting

## Recommended safety checks

Before exposing a book for production use, validate:

- required designations are assigned
- periods exist
- account statuses are correct
- approval configuration is intentional
- opening-balance migration rules are satisfied if used

## Good first embedded workflows

For a first integration, start with:

- create book
- create accounts
- create period
- create draft
- add lines
- post entry
- run trial balance
- run general ledger

Then add:

- subledger/open items
- close/reopen
- revaluation
- dimensions
- budgets
