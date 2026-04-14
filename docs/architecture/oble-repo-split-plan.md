# OBLE Repo Split Plan

Status: Implemented locally, published remotely

## Purpose

This document describes how to separate `OBLE` from `Heft` once the current
draft packet is stable enough to stand on its own.

The goal is:

- make `OBLE` visibly neutral
- keep `Heft` as the reference implementation
- avoid turning the standards repo into a runtime dependency
- preserve conformance, examples, and validation workflows

## Recommendation

Create a separate `oble` repository, but not as a runtime package first.

That split has now happened locally and the standalone repository has been
published at:

- [github.com/jeryldev/oble](https://github.com/jeryldev/oble)

The remaining work is no longer "create the split", but "finish reducing the
Heft-side snapshot over time."

The original decision rule for splitting was:

- the current draft packet feels stable enough to publish as its own unit
- the current profile matrix is good enough to explain what is core, profile,
  and still draft
- `Heft` can keep validating against shared OBLE artifacts without ambiguity

## What moves to the `oble` repo

The `oble` repo should contain the standard itself:

- numbered spec drafts
- glossary and introduction
- examples
- JSON schemas
- conformance checklist
- profile matrix
- import-boundary guidance
- future fixture bundles
- future reference conformance reports

From the current `Heft` repo, that means the future `oble` repo should receive:

- `docs/oble/0000-vision.md`
- `docs/oble/0001-core-model.md`
- `docs/oble/0002-lifecycle-invariants.md`
- `docs/oble/0003-counterparties-subledgers.md`
- `docs/oble/0004-serialization-conformance.md`
- `docs/oble/0005-heft-mapping.md`
- `docs/oble/0006-multi-currency-semantics.md`
- `docs/oble/0007-close-reopen-profile.md`
- `docs/oble/0008-designations-and-policy-profiles.md`
- `docs/oble/introduction.md`
- `docs/oble/glossary.md`
- `docs/oble/conformance-checklist.md`
- `docs/oble/profile-matrix.md`
- `docs/oble/import-boundary.md`
- `docs/oble/examples/`
- `docs/oble/schema/`

## What stays in `Heft`

The `Heft` repo should remain the engine and the reference implementation.

That includes:

- all runtime code
- all Zig modules
- all C ABI surfaces
- all importer/exporter implementations
- all conformance test code
- all semantic verification code
- all reconstruction helpers
- all engine architecture docs
- `Heft` release notes and stability notes

That means these stay in `Heft`:

- `src/oble_core.zig`
- `src/oble_profile_counterparty.zig`
- `src/oble_profile_policy.zig`
- `src/oble_profile_fx.zig`
- `src/oble_import.zig`
- `src/oble_import_session.zig`
- `src/oble_export.zig`
- `src/oble_reconstruction.zig`
- `src/oble_semantic_verify.zig`
- `src/oble_conformance_test.zig`
- C ABI exports/import session code
- `docs/architecture/*`
- `docs/embedding.md`
- `docs/architecture/heft-vs-oble.md`

## What should remain duplicated only temporarily

During the transition, some OBLE files may exist in both repos for a short
time so that links and CI keep working.

That should be temporary only.

The long-term goal is:

- `oble` repo is the source of truth for the standard
- `Heft` repo links to it and validates against it

## How `Heft` should consume OBLE after the split

`Heft` should not treat `OBLE` as a runtime library dependency first.

Instead, `Heft` should consume:

- schemas
- examples
- fixture bundles
- conformance metadata

That can happen in one of three ways:

1. vendored snapshot directory
2. git submodule
3. sync script that copies canonical OBLE artifacts into a local test fixture
   directory

The safest first choice is a vendored snapshot or sync script.

That keeps:

- CI simple
- release packaging simple
- runtime dependency graph clean

## Recommended first split shape

Create a new `oble` repo with this top-level layout:

- `spec/`
- `examples/`
- `schema/`
- `profiles/`
- `conformance/`
- `README.md`

Suggested mapping:

- `docs/oble/0000-*.md` -> `spec/`
- `docs/oble/examples/` -> `examples/`
- `docs/oble/schema/` -> `schema/`
- `docs/oble/profile-matrix.md` -> `profiles/`
- `docs/oble/conformance-checklist.md` -> `conformance/`

## Versioning recommendation

Keep versioning separate:

- `Heft` uses engine release versions like `0.1.1`
- `OBLE` uses draft/spec versions like `draft-0`, `draft-1`, or `0.1-draft`

Avoid implying that a `Heft` release and an `OBLE` draft version are the same
thing.

A good compatibility statement looks like:

- `Heft 0.1.1 implements OBLE draft-0 core and the current counterparty,
  policy, and FX profiles.`

## Migration plan

Recommended sequence:

1. freeze the current OBLE packet set enough to publish externally
2. create the `oble` repo
3. copy the current docs/examples/schema layer into it
4. publish the standalone repo
5. leave a short pointer in `Heft/docs/oble/README.md`
6. update `Heft` validation scripts to consume canonical OBLE artifacts
7. keep `Heft` conformance docs focused on implementation status rather than
   holding the full standard text forever

During the transition, a practical local workflow is:

- sync a vendored snapshot into `Heft` with
  `bash scripts/sync-oble.sh /Users/jeryldev/code/zig_projects/oble`
- validate it with `bash scripts/validate-oble.sh`

## Non-goals

The split should not:

- make `Heft` runtime depend on an `oble` package
- force all `Heft` APIs to become spec-shaped immediately
- freeze every draft packet prematurely
- move engine architecture docs out of the `Heft` repo

## Decision rule

Split OBLE into its own repo when this statement feels true:

`Another implementer could read the OBLE repo without reading Heft and still
understand the standard well enough to start building against it.`
