# OBLE-0007 Close and Reopen Profile

Status: Draft

## Purpose

This document defines a first profile for period close and reopen behavior in
OBLE.

The OBLE core already recognizes that:

- periods are lifecycle boundaries
- close is a real accounting event
- reopen is not just a UI convenience

This profile goes one step further and defines the minimum observable semantics
for implementations that choose to support close and reopen workflows.

## Why this is a profile

Close is universal in accounting, but not every implementation realizes it the
same way.

Differences appear in:

- whether close generates journal entries
- whether current earnings are projected or materialized
- whether close is soft, hard, or both
- whether reopen requires cascading invalidation of derived state

That makes close too important to ignore, but too opinionated to treat as
unqualified OBLE core in the first version.

So OBLE should standardize it as a profile.

## Scope

This profile covers:

- close preconditions
- close effects
- derived opening-entry behavior
- reopen expectations
- auditability requirements

This profile does not yet attempt to standardize:

- one universal tax close process
- entity-specific equity allocation policy
- every jurisdiction-specific year-end rule

## Close-capable implementation

An implementation may claim this profile if it supports:

- explicit period close behavior
- explicit period reopen behavior
- internally coherent regeneration or invalidation of derived state

## Close preconditions

Before a period may be closed, the implementation should ensure at minimum:

1. The period belongs to a valid book.
2. The period is in a close-eligible state.
3. No disallowed draft or incomplete posting state remains in the period.
4. The ledger for the close domain is internally balanced.
5. Any policy-required control conditions are satisfied.

Examples of policy-required control conditions include:

- suspense balances cleared
- required close designations configured
- close target allocation rules satisfied

These conditions may vary by implementation, but the implementation must define
them explicitly if it claims this profile.

## Close effects

A close operation must do more than flip a period flag.

At minimum, a conforming profile implementation must ensure that close has
observable accounting consequences.

Those consequences may include:

- posting one or more derived closing entries
- transitioning the period state
- creating or refreshing carry-forward state for the next period

If the implementation does not materialize close effects as entries, it must
still expose an auditable, deterministic close result.

## Derived closing entries

A profile implementation may generate derived entries during close.

If it does:

- those entries must be identifiable as derived close activity
- they must remain auditable
- their economic role must be distinguishable from ordinary user-authored
  entries

Recommended metadata:

- `entry_type = closing`
- explicit close reason or origin markers

## Opening carry-forward

If an implementation materializes opening balances for the next period:

- the opening carry-forward must be derivable from the closed state of the
  prior period
- the relationship between the closed period and generated opening state must
  be recoverable

Recommended metadata:

- `entry_type = opening`
- link to the source closed period

## Reopen semantics

Reopening is not just permission to post again.

If a period is reopened after close:

- stale derived close state must not silently remain authoritative
- stale opening carry-forward state in later periods must be invalidated,
  voided, or regenerated
- the reopen event must be explicit and auditable

This is one of the most important lifecycle guarantees in the profile.

Without it, a system can appear open again while still depending on invalid
derived balances from a previous close.

## Reopen cascade expectations

If reopening one period can invalidate derived state in later periods, the
implementation should define one of these behaviors explicitly:

- disallow reopen until later periods are addressed
- cascade invalidation forward
- void stale derived entries and require regeneration

Different implementations may choose different strategies.

What OBLE standardizes here is that the strategy must be explicit and internally
coherent.

## Audit requirements

A profile implementation must be able to recover:

- that a close happened
- which period was closed
- what derived state was created, if any
- that a reopen happened
- what stale derived state was invalidated or replaced

The audit mechanism may vary by implementation, but the lifecycle history must
not be lost.

## Presentation versus materialization

Some systems project current earnings or carry-forward state at report time.
Others materialize them as real entries.

This profile allows both, with an important condition:

- the implementation must not blur projected presentation with materially posted
  accounting state

In other words:

- projected balances are presentation behavior
- closing entries and opening entries are lifecycle behavior

The implementation must keep that distinction clear.

## Conformance claim

An implementation may claim `OBLE Close/Reopen Profile` if:

- it supports explicit close behavior
- it supports explicit reopen behavior
- derived close state is auditable
- stale carry-forward or close artifacts do not remain silently authoritative
  after reopen

## Mapping note for Heft

Heft is especially relevant to this profile because it already models:

- explicit period states
- close-derived entries
- opening-entry generation
- reopen cascade behavior
- invalidation of stale derived state

That makes Heft a strong reference implementation candidate for this profile,
while OBLE remains free to describe the behavior in implementation-neutral
terms rather than adopting one exact schema or algorithm.
