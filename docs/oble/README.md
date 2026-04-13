# OBLE Drafts

OBLE stands for Open Bookkeeping Ledger Exchange.

This folder contains the early working drafts for extracting an open ledger
standard from the accounting semantics that Heft already implements.

Current draft set:

1. [OBLE-0000 Vision](</Users/jeryldev/code/zig_projects/heft/docs/oble/0000-vision.md>)
2. [OBLE-0001 Core Model](</Users/jeryldev/code/zig_projects/heft/docs/oble/0001-core-model.md>)
3. [OBLE-0002 Lifecycle and Invariants](</Users/jeryldev/code/zig_projects/heft/docs/oble/0002-lifecycle-invariants.md>)
4. [OBLE-0003 Counterparties and Subledgers](</Users/jeryldev/code/zig_projects/heft/docs/oble/0003-counterparties-subledgers.md>)
5. [OBLE-0004 Serialization and Conformance](</Users/jeryldev/code/zig_projects/heft/docs/oble/0004-serialization-conformance.md>)
6. [OBLE-0005 Heft Mapping](</Users/jeryldev/code/zig_projects/heft/docs/oble/0005-heft-mapping.md>)
7. [OBLE-0006 Multi-Currency Semantics](</Users/jeryldev/code/zig_projects/heft/docs/oble/0006-multi-currency-semantics.md>)
8. [OBLE-0007 Close and Reopen Profile](</Users/jeryldev/code/zig_projects/heft/docs/oble/0007-close-reopen-profile.md>)
9. [OBLE-0008 Designations and Policy Profiles](</Users/jeryldev/code/zig_projects/heft/docs/oble/0008-designations-and-policy-profiles.md>)
10. [Conformance Checklist](</Users/jeryldev/code/zig_projects/heft/docs/oble/conformance-checklist.md>)
11. [Examples](</Users/jeryldev/code/zig_projects/heft/docs/oble/examples/README.md>)

Intended reading order:

- `0000` explains why OBLE exists.
- `0001` defines the smallest useful ledger core.
- `0002` defines lifecycle semantics and invariants.
- `0003` adds counterparties, subledgers, and open items as the first extension.
- `0004` defines the first serialization and conformance story.
- `0005` explains how Heft maps to the drafts without making Heft the spec.
- `0006` defines the minimum multi-currency semantics needed for real exchange.
- `0007` defines a first profile for close and reopen lifecycle behavior.
- `0008` defines how book-level designations and policy profiles fit around the core.
- `conformance-checklist` gives a practical first-pass claim matrix.
- `examples/` provides concrete JSON payloads for review and future fixtures.

These drafts are intentionally small, implementation-neutral, and incomplete.

They should be treated as working documents for extracting stable semantics,
not as frozen standards yet.
