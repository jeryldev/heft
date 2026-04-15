# OBLE Profile Matrix

Status: Draft

## Purpose

This document groups the current OBLE drafts into practical interoperability
profiles.

The goal is to answer:

- what a minimal OBLE implementation should support
- which Heft behaviors belong to core versus profile layers
- what packet sets are stable enough to talk about together

## Profile levels

### Core

The smallest useful interoperable ledger surface:

- books
- accounts
- periods
- entries
- lines
- lifecycle states
- balance invariants

### Profiles

Portable layers that build on the core for richer accounting behavior:

- counterparties and open items
- multi-currency
- close/reopen lifecycle
- policy/designations
- classifications/report structures
- dimensions/analytics
- budgets/planning

### Runtime

Implementation details that are outside OBLE itself:

- storage engine
- caches
- indexes
- buffer plumbing
- ABI layout details

## Current profile set

| Profile | Related drafts | Current packet examples | Heft status |
| --- | --- | --- | --- |
| `OBLE Core` | `0001`, `0002`, parts of `0004` | `core-book.json`, `core-accounts.json`, `core-periods.json`, `core-entry-posted.json`, `book-snapshot.json` | Strong |
| `Counterparty / Open Item` | `0003`, parts of `0004` | `counterparties.json`, `counterparty-open-item.json`, `book-snapshot.json` | Strong |
| `Multi-Currency` | `0006` | core entry export with foreign-currency lines, revaluation packet export | Emerging |
| `Close / Reopen` | `0007` | close/reopen profile export | Emerging |
| `Policy / Designations` | `0008` | `policy-profile.json`, `book-snapshot.json` | Strong |
| `Classifications / Report Structures` | `0009` | classification profile bundle (Zig-first) | Emerging |
| `Dimensions / Analytics` | `0010` | dimension profile bundle (Zig-first) | Emerging |
| `Budgets / Planning` | `0011` | budget profile bundle (Zig-first) | Emerging |

## Public-boundary posture

The current public-boundary posture in Heft is intentionally asymmetric:

- Zig API: richest import and export surface
- C ABI: strong export surface, conservative import surface

That means:

- `OBLE Core` has public export in Zig and C, and import in Zig and C
- `Counterparty / Open Item` has public export in Zig and C, and stable
  session-based import in Zig and C
- `Policy / Designations` has public export in Zig and C, and safe
  user-authored import in Zig and C
- Zig import sessions can also consume richer `Policy / Designations` lifecycle
  bundles by importing the safe policy layer and reporting the presence of
  derived close/revaluation packets
- Zig import sessions can consume `Multi-Currency` bundles by importing the
  foreign-currency entry and reporting whether revaluation replay remains
  pending
- `Multi-Currency` and `Close / Reopen` remain export-first at the public
  boundary

This is deliberate. The goal is to freeze the safer user-authored packet
imports first and keep lifecycle-derived import replay conservative.

## Profile definitions

## 1. OBLE Core

This profile includes:

- `Book`
- `Account`
- `Period`
- `Entry`
- `Line`
- lifecycle states
- posted-entry balance invariants
- canonical identifiers and dates

This is the profile another engine would need first in order to claim real
OBLE compatibility.

### Minimum expectations

- import/export core packet shapes
- preserve book/account/period/entry identity
- preserve debit/credit orientation
- preserve line ordering
- enforce or validate balance invariants

## 2. Counterparty / Open Item

This profile adds:

- counterparties
- subledger grouping concepts
- line-level counterparty references
- open items
- settlement/allocation semantics

### Minimum expectations

- import/export counterparties
- import/export open-item packet(s)
- preserve counterparty linkage on entry lines
- preserve remaining/open/partial/closed state

## 3. Multi-Currency

This profile adds:

- transaction currency
- FX rate
- base debit/credit amounts
- explicit exact arithmetic expectations
- revaluation packet(s)

### Minimum expectations

- import/export line currency data
- preserve base-amount semantics
- preserve exact-scaled amount behavior
- export revaluation behavior where implemented

## 4. Close / Reopen

This profile adds:

- close-generated state
- opening carry-forward behavior
- reopen semantics
- lifecycle-derived packet exports

### Minimum expectations

- export close/reopen profile state honestly
- distinguish user-authored from lifecycle-derived packets
- avoid importing derived packets naively where the engine should reconstruct
  them instead

## 5. Policy / Designations

This profile adds:

- designation bindings
- fiscal-year start behavior
- entity-type behavior
- approval requirements

### Minimum expectations

- export book policy profile
- import safe user-authored policy state
- preserve designation identity without conflating it with core ledger state

## 6. Classifications / Report Structures

This profile adds:

- classification definitions
- hierarchical report nodes
- account bindings for report structure
- report intent such as balance sheet or cash flow

### Minimum expectations

- import/export classification metadata
- preserve hierarchical node ordering
- preserve account binding identity
- keep report structure distinct from chart-of-accounts identity

## 7. Dimensions / Analytics

This profile adds:

- dimension definitions
- dimension values
- line-level analytical assignments

### Minimum expectations

- import/export dimensions and values
- preserve line assignment semantics
- keep analytical tags separate from balancing semantics

## 8. Budgets / Planning

This profile adds:

- budget definitions
- budget lines
- budget lifecycle state

### Minimum expectations

- import/export budget metadata
- preserve account/period planning line intent
- keep planning state distinct from posted ledger history

## Stable versus emerging areas

### Strongest today

- `OBLE Core`
- `Counterparty / Open Item`
- `Policy / Designations`

These are the areas where Heft already has the most complete semantics plus
real export/import coverage.

### Still expanding

- `Multi-Currency`
- `Close / Reopen`
- `Classifications / Report Structures`
- `Dimensions / Analytics`
- `Budgets / Planning`

These already have meaningful export coverage, but they still need broader
packet breadth and stronger interoperability posture before they should be
treated as fully mature profiles.

## How Heft should use this matrix

The intended interpretation is:

- `Heft` remains the engine
- `OBLE Core` and `OBLE Profiles` become clearer semantic boundaries inside it

That means:

- native Heft APIs can stay richer than the profile surface
- profile-aligned export/import paths should keep growing
- conformance claims should be made profile by profile, not all-or-nothing

## Practical conformance language

Good claims:

- "Heft implements OBLE Core."
- "Heft implements the OBLE Counterparty/Open Item profile."
- "Heft implements the OBLE Policy/Designations profile."
- "Heft has emerging support for the OBLE Multi-Currency and Close/Reopen profiles."
- "OBLE defines draft profiles for classifications, dimensions, and budgets that are not yet widely implemented."

Weaker claims that should be avoided for now:

- "Heft fully implements all of OBLE."
- "OBLE is finished."

## Immediate next steps

1. Use this matrix in [Heft to OBLE Conformance](heft-conformance.md)
2. Use it when describing packet stability in docs and release notes
3. Use it to decide which modules and APIs should be labeled next as:
   - core
   - profile
   - runtime
