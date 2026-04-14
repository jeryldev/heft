# OBLE Drafts

Status: Vendored snapshot

OBLE stands for Open Bookkeeping Ledger Exchange.

This folder is a vendored local snapshot of the canonical OBLE repository.

Canonical home:

- [github.com/jeryldev/oble](https://github.com/jeryldev/oble)
- canonical license: Apache-2.0

What this folder is for:

- local validation inside the `Heft` repo
- fixture and schema sync for conformance work
- local reference when working on `Heft` without leaving the engine repo

What to read first:

- in the canonical repo:
  - [spec/introduction.md](https://github.com/jeryldev/oble/blob/main/spec/introduction.md)
  - [spec/0000-vision.md](https://github.com/jeryldev/oble/blob/main/spec/0000-vision.md)
  - [profiles/profile-matrix.md](https://github.com/jeryldev/oble/blob/main/profiles/profile-matrix.md)
  - [conformance/conformance-checklist.md](https://github.com/jeryldev/oble/blob/main/conformance/conformance-checklist.md)

Inside `Heft`, the most relevant companion docs are:

- [Source of Truth](source-of-truth.md)
- [Heft vs OBLE](../architecture/heft-vs-oble.md)
- [OBLE Repo Split Plan](../architecture/oble-repo-split-plan.md)

Snapshot workflow:

- refresh this vendored copy with
  `bash scripts/sync-oble.sh /Users/jeryldev/code/zig_projects/oble`
- then validate it with `bash scripts/validate-oble.sh`

The current migration recommendation is documented in
[../architecture/oble-repo-split-plan.md](../architecture/oble-repo-split-plan.md).

The source-of-truth transition is documented in
[source-of-truth.md](source-of-truth.md).
