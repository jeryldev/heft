# OBLE Source of Truth

Status: Canonical repo published

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

`Heft` still contains a local copy of the OBLE draft materials in `docs/oble/`.

That copy should now be treated as a vendored snapshot, not the long-term
canonical home of the standard.

The canonical OBLE repository is now:

- [github.com/jeryldev/oble](https://github.com/jeryldev/oble)

The canonical local checkout used in this workspace is:

- `/Users/jeryldev/code/zig_projects/oble`

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

The intended local snapshot workflow is now:

1. sync the snapshot with
   `bash scripts/sync-oble.sh /Users/jeryldev/code/zig_projects/oble`
2. validate the synced snapshot with `bash scripts/validate-oble.sh`

`Heft` can also validate an external OBLE source directly without first
refreshing the local snapshot:

```bash
OBLE_SOURCE=/Users/jeryldev/code/zig_projects/oble bash scripts/validate-oble.sh
```

## Practical interpretation

Now that the external repo exists:

- use the standalone `oble` repo as the source of truth
- treat `docs/oble/` in `Heft` as a synced local snapshot
- prefer refreshing the local copy with `scripts/sync-oble.sh` instead of
  editing both repos independently
