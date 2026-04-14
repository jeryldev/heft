# OBLE-0005 Heft Mapping

Status: Draft

## Purpose

This document explains how Heft maps to the current OBLE drafts.

It is not part of the neutral standard itself.

Its purpose is to:

- show that OBLE is grounded in running software
- clarify which Heft concepts fit directly into OBLE core
- clarify which Heft concepts are extensions or implementation detail

## Mapping philosophy

Heft should be treated as a reference implementation candidate, not as the
definition of the spec.

So this document maps:

- Heft concept
- OBLE concept
- exact match, extension match, or implementation-specific detail

## Core model mapping

### Book

Heft `Book` maps directly to OBLE `Book`.

Direct matches:

- name
- base currency
- decimal places
- status

Heft-specific additions that are not automatically core OBLE:

- designation-bearing system accounts
- entity type
- fiscal-year configuration details beyond the minimal core

### Account

Heft `Account` maps directly to OBLE `Account`.

Direct matches:

- account number
- name
- account type
- normal balance
- contra flag
- status
- parent account relationship

### Period

Heft `Period` maps directly to OBLE `Period`.

Direct matches:

- start and end date
- status
- period number
- year

### Entry

Heft `Entry` maps directly to OBLE `Entry`.

Direct matches:

- draft/post/void/reversed status
- transaction date
- posting date
- document number
- description
- metadata
- reversal linkage

### Line

Heft `EntryLine` maps directly to OBLE `Line`.

Direct matches:

- account reference
- debit and credit orientation
- transaction currency
- FX rate
- base amounts
- optional description

Heft's ten account behaviors are represented in OBLE core as:

- five base account types: `asset`, `liability`, `equity`, `revenue`, `expense`
- plus `is_contra` to capture the contra variants without exploding the type taxonomy

## Lifecycle mapping

Heft already implements the lifecycle assumed by current OBLE drafts:

- draft creation
- line mutation before posting
- exact-balance enforcement on post
- void
- reverse
- period close
- reopen cascade behavior

This makes Heft especially useful for drafting OBLE lifecycle semantics.

## Counterparty and subledger mapping

Heft uses:

- subledger groups
- subledger accounts
- line-level counterparty references
- open items
- allocation

This maps naturally to the OBLE counterparty/subledger extension, with one
important naming adjustment:

- OBLE should speak in terms of generic counterparties and roles
- Heft currently uses subledger-account terminology for those parties

## Designations in Heft

One of Heft's strongest design ideas is designation-based accounting policy per
book.

Examples include:

- retained earnings account
- income summary account
- FX gain/loss account
- suspense account
- opening balance account

These are important, but they should not automatically be part of OBLE core.

Why:

- they are policy-bearing configuration
- some jurisdictions or engines may realize close and revaluation differently
- they fit better as a profile or extension than as a universal minimum

## What should likely stay outside OBLE core

From Heft, these should remain outside the minimum OBLE core unless later
evidence justifies them:

- designation-specific close policy
- approval workflow configuration
- classification trees
- dimensions
- budgets
- benchmark methodology
- ABI-specific buffer contracts

## Where Heft can drive the spec

Heft is especially valuable in shaping:

- lifecycle invariants
- reverse versus void semantics
- period-aware posting and close behavior
- counterparties and open items
- multi-currency exactness

## Where Heft should not overfit the spec

OBLE should resist inheriting Heft-specific details such as:

- exact table names
- exact cache-table structure
- exact audit-log schema
- exact C ABI naming
- one required account-designation layout

## Current practical interpretation

If someone asked today how to think about the relationship:

- Heft is a serious implementation
- OBLE is the extracted semantics layer
- the mapping document is the bridge between them

That bridge is useful because it keeps the spec honest without collapsing the
spec into product documentation.
