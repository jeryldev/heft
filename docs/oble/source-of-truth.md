# OBLE Source of Truth

Status: Transitional

## What "canonical split" means

A canonical split means:

- the standalone `oble` repository becomes the source of truth for the
  standard
- the `Heft` repository stops treating `docs/oble/` as the primary standards
  home
- `Heft` keeps implementation code, conformance tests, and any temporary
  vendored OBLE artifacts needed for validation

In other words:

- `OBLE` owns the standard text, examples, schemas, and profile definitions
- `Heft` owns the engine and proves conformance against those artifacts

## Current status

Right now, `Heft` still contains a local copy of the OBLE draft materials in
`docs/oble/`.

That copy should now be treated as a vendored snapshot, not the long-term
canonical home of the standard.

The standalone OBLE draft package already exists locally and is intended to
become the canonical standards repo once it is published to its permanent
remote.

## What stays in Heft after the split

Even after the canonical split, `Heft` should keep:

- OBLE implementation code
- import/export boundaries
- conformance tests
- semantic verification
- reconstruction helpers
- documentation about how `Heft` maps to OBLE

## What changes after the split

After the standalone `oble` repo is published:

1. `docs/oble/README.md` in `Heft` should become a short pointer and status
   document
2. the full standard text should live in the `oble` repo
3. `Heft` validation should consume canonical OBLE artifacts from that repo or
   from a synced snapshot of it

## Practical interpretation

Until the external repo is published:

- use `docs/oble/` in `Heft` as a working local copy
- treat the standalone `oble` package as the intended future source of truth
- avoid making new readers guess which copy is meant to win
