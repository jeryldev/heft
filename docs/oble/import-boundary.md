# OBLE Import Boundary

Status: Draft

## Purpose

This note explains why OBLE import is currently implemented as a Zig-facing
integration boundary in Heft rather than a fully exposed C ABI surface.

## Short answer

Heft can already import the implemented OBLE packet set:

- core book/account/period/entry packets
- reversal-pair packets
- counterparty/open-item packets
- policy-profile packets

But that import layer is still intentionally Zig-first.

The export side is much easier to expose safely through the C ABI because it is
stateless and buffer-oriented.

The import side is different.

## Why import is harder than export

OBLE import is not just a string-to-struct parse.

It also needs:

- logical ID mapping between OBLE identifiers and live Heft row IDs
- sequencing across dependent packets
- lifecycle-safe reconstruction of state
- decisions about when derived state should be imported directly versus rebuilt
  by the engine

Examples:

- a counterparty must exist before an imported line can safely reference it
- an open item must bind to a real imported entry line
- a policy profile can be imported directly because it is user-authored
  configuration
- a close/reopen packet is different because much of it is lifecycle-derived
  engine state that is safer to reconstruct than to replay blindly

## Why there is no C ABI import yet

The current C ABI style in Heft is:

- handle-based
- buffer-oriented
- mostly stateless per call

That fits export very well.

Import would likely need a more explicit session model, such as:

- create import session
- import packet A
- import packet B
- resolve references
- finalize session

Until that design is stable, exposing one-off `ledger_oble_import_*` calls
through C would create a misleading surface:

- too stateful to be honest as single-shot calls
- not explicit enough about sequencing and dependency rules
- likely to be revised once the import session model is clearer

## Current recommendation

For now:

- use Zig APIs for OBLE import
- use C ABI functions for OBLE export

Heft now has an explicit Zig-facing import-session boundary:

- `src/oble_import_session.zig`

That session model makes sequencing honest instead of hiding it:

- import core bundle
- import dependent profile packets
- reuse one logical-ID mapping context across the whole import flow

It also now exposes the imported logical-ID map back to Zig callers, so a
consumer can resolve:

- imported books
- accounts
- periods
- entries
- lines
- counterparties
- open items

without guessing row IDs after import.

## Import-session rules

The current Zig session boundary assumes deterministic sequencing.

### Safe order

The stable order today is:

1. import core bundle
2. import prerequisite profile collections such as counterparties
3. import dependent entries
4. import profile bundles that depend on those entries and counterparties
5. import safe user-authored policy packets

### Current failure modes

The session intentionally fails fast when packet order is wrong:

- unresolved logical references return `error.NotFound`
- duplicate logical IDs in the same session return `error.DuplicateNumber`
- invalid lifecycle or malformed payload state returns `error.InvalidInput`

This is preferable to silently guessing or auto-repairing packet order.

This keeps the standards boundary real without freezing an immature C import
surface too early.

## What would justify a C ABI import surface later

A C ABI import boundary becomes worthwhile once Heft has:

- a stable import session model
- explicit packet ordering rules
- deterministic conflict and duplicate handling
- broader fixture-driven conformance coverage
- clear guidance on which lifecycle-derived packets may be replayed directly

## Practical claim

Today, Heft can honestly claim:

- OBLE export is available in Zig and C
- OBLE import is available in Zig
- a real import-session boundary now exists in Zig for the implemented packet set
- the import surface is real, tested, and round-tripped for the implemented
  packets
- the C import boundary is intentionally deferred, not forgotten
