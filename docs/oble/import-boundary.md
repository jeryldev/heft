# OBLE Import Boundary

Status: Draft

## Purpose

This note explains the current OBLE import boundary in Heft and why the public
C ABI import surface is still intentionally narrower than the Zig one.

## Short answer

Heft can import the implemented OBLE packet set:

- core book/account/period/entry packets
- reversal-pair packets
- counterparty/open-item packets
- policy-profile packets

It can also import the safe user-authored parts of richer Zig-first bundles:

- FX bundles import the foreign-currency entry and report whether a
  revaluation packet was present
- policy/lifecycle bundles import the policy profile and report whether close
  and revaluation packets were present

That import layer is still strongest in Zig.

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

## What C ABI import exists now

Heft now exposes a minimal C ABI import-session handle for the stable import
packets:

- create import session
- import core bundle
- import entry
- import reversal pair
- import counterparties
- import counterparty profile bundle
- import policy profile
- import book snapshot
- resolve imported logical IDs back to live row IDs

This keeps the statefulness honest instead of pretending import is a stateless
single-call operation.

## Why the C ABI import surface is still intentionally small

The current C ABI style in Heft is still mostly:

- handle-based
- buffer-oriented
- mostly stateless per call

That fits export very well.

Import needed a more explicit session model, such as:

- create import session
- import packet A
- import packet B
- resolve references
- finalize session

That session model now exists.

What Heft still avoids is exposing every conceivable packet and lifecycle
reconstruction path through the C ABI before the semantics are stable enough.

Blindly exposing one-off `ledger_oble_import_*` calls for all packet types
would still create a misleading surface:

- too stateful to be honest as single-shot calls
- not explicit enough about sequencing and dependency rules
- likely to be revised once the import session model is clearer

## Current recommendation

For now:

- prefer Zig APIs for the richest OBLE import workflows
- use C ABI functions for OBLE export
- use the C ABI import session for the stable packet set when embedding from C

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

The current C ABI import session follows the same sequencing rules for the
stable packet set. It is not a separate semantics layer.

### Safe order

The stable order today is:

1. import core bundle
2. import prerequisite profile collections such as counterparties
3. import dependent entries
4. import profile bundles that depend on those entries and counterparties
5. import safe user-authored policy packets

For richer Zig-only bundle imports, the rule is slightly more precise:

- import the safe user-authored portion
- surface whether derived lifecycle packets were present
- let the engine reconstruct those derived lifecycle effects explicitly later

### Current failure modes

The session intentionally fails fast when packet order is wrong:

- unresolved logical references return `error.NotFound`
- duplicate logical IDs in the same session return `error.DuplicateNumber`
- invalid lifecycle or malformed payload state returns `error.InvalidInput`

At the C boundary these surface through the usual ABI contract:

- `-1`, `false`, or `NULL` return values
- followed by `ledger_last_error()`

This is preferable to silently guessing or auto-repairing packet order.

This keeps the standards boundary real without freezing a too-broad C import
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
- OBLE import is available in Zig and minimally available in C
- a real import-session boundary now exists in Zig for the implemented packet set
- the Zig session can now import the safe portion of lifecycle-rich bundles and
  report the presence of derived packets that still require engine
  reconstruction
- a real import-session boundary now exists in C for the stable import packets
- the import surface is real, tested, and round-tripped for the implemented
  packets
- the wider C import boundary is intentionally deferred, not forgotten
