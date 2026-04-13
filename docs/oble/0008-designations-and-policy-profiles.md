# OBLE-0008 Designations and Policy Profiles

Status: Draft

## Purpose

This document defines how OBLE should think about policy-bearing ledger
configuration such as designated accounts and entity-specific lifecycle rules.

The core idea is simple:

- some accounting semantics are universal
- some accounting behavior is determined by book-level policy

OBLE should standardize the boundary between those two things instead of
pretending every engine has the same close, equity, suspense, or revaluation
configuration.

## Why this matters

Many accounting engines contain special accounts or policy switches that drive:

- close behavior
- opening balance behavior
- FX revaluation behavior
- suspense enforcement
- equity allocation behavior

These are real accounting semantics, but they are not all part of the irreducible
double-entry core.

If OBLE tries to push all of them into the core model, it becomes bloated.

If OBLE ignores them entirely, it cannot describe serious accounting engines.

So OBLE should treat them as policy profiles.

## Designation

A `Designation` is a book-level policy binding that gives special semantic
meaning to an account or accounting role.

Examples:

- retained earnings target
- income summary account
- FX gain/loss account
- suspense account
- opening balance account
- dividends/drawings account

## Policy profile

A `PolicyProfile` is a declared set of accounting behaviors or required
designations attached to a book or implementation profile.

Examples:

- simple close policy
- income-summary close policy
- allocation-driven equity close policy
- suspense-enforced policy
- revaluation-capable policy

## Core principle

OBLE should not require every implementation to have the same designations.

But OBLE should allow implementations to declare:

- which designations exist
- which are required for a given profile
- which lifecycle operations depend on them

This keeps policy explicit without forcing one engine's internal assumptions on
every conforming system.

## Recommended model

At the profile layer, a book may expose:

- `designations`
- `policy_profiles`

### Example designation shape

```json
{
  "name": "retained_earnings_account",
  "account_id": "acct-3100"
}
```

### Example policy profile shape

```json
{
  "name": "income_summary_close",
  "required_designations": [
    "retained_earnings_account",
    "income_summary_account"
  ]
}
```

## Designations are not accounts themselves

This distinction matters.

A designation is:

- policy meaning attached to an account

It is not:

- a new primitive ledger entity
- a replacement for account type

For example:

- `retained_earnings_account` designates how a close policy targets an equity
  account
- it does not create a sixth fundamental account type

## Relationship to close and reopen

Some lifecycle profiles depend on designations.

Examples:

- a close profile may require a retained earnings target
- an income-summary close profile may also require an income summary account
- an opening-balance workflow may require an opening balance account

This is why designations belong near policy profiles rather than in OBLE core.

## Suspense and control semantics

Some engines require special treatment for suspense or control accounts.

OBLE should not assume every implementation enforces suspense the same way.

But a policy profile may declare:

- that suspense is used
- that suspense must be cleared before close
- that certain workflows are invalid while suspense carries a nonzero balance

## Entity-specific policy

Some close and equity behaviors depend on entity type or organizational form.

Examples:

- corporation-style retained earnings close
- partnership or LLC allocation-driven close
- nonprofit fund-balance behavior

OBLE should not hardcode all of these in the core model.

Instead, a policy profile should be able to declare:

- required designations
- required allocation structures
- additional close constraints

## Conformance implications

An implementation should not claim policy behavior implicitly.

If it supports designation-driven workflows, it should be able to state:

- which designations exist
- which profiles use them
- which operations fail when required designations are missing

This makes policy-bearing behavior portable enough to reason about, even if not
every engine supports the same profiles.

## Import guidance

This profile is a good candidate for direct import because it mostly describes
user-authored or administrator-authored configuration rather than derived
lifecycle state.

That means implementations can usually import:

- entity type
- fiscal-year start month
- approval requirement
- designation bindings

as configuration updates on top of an already-imported book and chart of
accounts.

The main requirement is that referenced designation accounts already exist in
the target book or are resolvable from imported account identifiers.

## What should remain outside the core

These should usually remain in profiles or extensions rather than core:

- exact close target layout
- suspense clearing rules
- equity allocation formulas
- jurisdiction-specific retained earnings policy
- implementation-specific account naming conventions

## Mapping note for Heft

Heft is a strong motivating example for this draft because it already uses
book-level designations in a disciplined way.

Examples in Heft include:

- retained earnings account
- income summary account
- FX gain/loss account
- suspense account
- opening balance account
- dividends/drawings account

That makes Heft especially good evidence that designations are not random
application details. They are policy semantics.

At the same time, Heft also shows why they should not all be elevated into OBLE
core without qualification.

They belong in an explicit policy layer.
