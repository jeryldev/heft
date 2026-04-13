# OBLE-0002 Lifecycle and Invariants

Status: Draft

## Purpose

This document defines the minimum lifecycle semantics and accounting
invariants for OBLE core entities.

The goal is not to force every implementation to expose identical APIs.

The goal is to ensure that two conforming implementations agree on when an
entry is economically effective, how it can stop being effective, and which
core invariants must hold regardless of storage engine or user interface.

## Lifecycle entities

The lifecycle model in this draft applies primarily to:

- `Entry`
- `Line`
- `Period`

## Entry lifecycle states

Minimum entry states:

- `draft`
- `posted`
- `void`
- `reversed`

### Draft

A `draft` entry is mutable and not yet economically effective.

A draft entry:

- may have incomplete lines
- may be unbalanced
- must not affect balances, reports, or audit-derived totals as a posted entry

### Posted

A `posted` entry is economically effective.

A posted entry:

- is balanced
- belongs to a valid book and period
- contributes to ledger totals
- is no longer freely mutable

### Void

A `void` entry is explicitly canceled in place.

Void semantics:

- the original entry remains identifiable
- the entry no longer contributes economic effect
- the reason for voiding should be recoverable or auditable

### Reversed

A `reversed` entry has been offset by a separate reversing entry.

Reverse semantics:

- the original entry remains part of the audit trail
- a distinct reversal entry carries the offsetting financial effect
- the link between original and reversal should be recoverable

OBLE does not require one exact internal storage strategy for reversal, but it
does require the economic relationship to be explicit.

## Minimum lifecycle transitions

Required transitions:

- `draft -> posted`
- `posted -> void`
- `posted -> reversed`

Recommended restrictions:

- `void` should be terminal
- `reversed` should be terminal
- direct mutation of a posted entry should be disallowed or tightly controlled

OBLE does not require every implementation to support every convenience path,
but the economic meaning of these state changes must remain stable.

## Posting invariants

Before an entry may become `posted`, the following must hold:

1. The entry belongs to one valid `Book`.
2. The entry references one valid `Period`.
3. The period is in a state that allows posting.
4. The entry contains at least two lines.
5. Every line references an account in the same book.
6. The entry balances exactly.
7. Each line has one explicit economic orientation.

The last rule means a conforming model must not allow one line to carry both an
economically positive debit and credit at the same time.

## Period lifecycle

Minimum period states:

- `open`
- `soft_closed`
- `closed`
- `locked`

### Open

Posting is allowed.

### Soft closed

The period is operationally closed, but the implementation may allow controlled
administrative reopening or transition.

### Closed

Normal posting is not allowed.

### Locked

The period is immutable for ordinary lifecycle operations.

## Period invariants

Conforming implementations must treat periods as first-class lifecycle
boundaries.

At minimum:

1. A posted entry belongs to exactly one period.
2. The posting date and period relationship must be explicit.
3. A period state must influence whether posting, voiding, reversing, or close
   flows are allowed.

## Close semantics

OBLE core recognizes period close as a lifecycle operation even if different
implementations realize it differently.

Minimum expectations:

- a close operation is period-scoped
- it may generate derived entries
- it changes what future lifecycle operations are permitted
- it must preserve auditability

This draft does not yet standardize one universal close algorithm.

That belongs to a later refinement or profile because jurisdictions and entity
types differ. What is standardized here is that close is a real lifecycle event
with accounting consequences, not just a UI flag.

## Reopen semantics

If an implementation supports reopening a previously closed period:

- reopening must be explicit
- downstream derived state must remain internally coherent
- stale derived entries or carry-forward artifacts should not survive silently

OBLE does not yet require one exact reopen cascade algorithm, but it does
require that reopen not leave the ledger in a contradictory lifecycle state.

## Audit invariant

Every lifecycle mutation with accounting significance must be recoverable.

Allowed implementation strategies:

- append-only audit log
- row history
- event stream
- equivalent recoverable mutation log

The core requirement is not a specific table shape.

The core requirement is that the implementation can answer:

- what changed
- when it changed
- which entity changed
- the relevant old and new lifecycle state when applicable

## Balance invariants

The fundamental accounting invariants are:

1. Posted entries balance exactly.
2. Aggregate debits and credits remain equal across the posted ledger domain.
3. Void and reverse operations must preserve aggregate ledger consistency.
4. Reporting totals must be derivable from posted lifecycle state without
   ambiguity.

## Implementation freedom

OBLE intentionally leaves room for implementation choices in:

- caches
- derived balance tables
- audit storage layout
- approval workflows
- draft editing behavior
- reopen cascades

Those choices are implementation detail as long as the observable lifecycle
semantics remain conformant.

## Mapping note for Heft

Heft already expresses most of this lifecycle directly:

- draft/post/void/reverse entry states
- open/soft_closed/closed/locked periods
- close and reopen behavior
- explicit audit logging
- exact balance enforcement

That makes Heft a strong reference implementation candidate for this draft, but
the draft should remain valid even for systems that do not share Heft's exact
schema or cache strategy.
