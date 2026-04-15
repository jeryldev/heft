# OBLE-0010 Dimensions and Analytics

Status: Draft

## Purpose

This draft defines an optional OBLE profile for analytical dimensions attached
to ledger lines.

The goal is to make analytical tagging portable without turning dimensions into
core ledger identity.

## Why this is a profile

Dimensions are powerful, but they are not required for double-entry itself.

A valid ledger can exist without them.

They belong in a profile because:

- many systems support them differently
- they refine meaning rather than define the core journal structure
- analytical slicing should not be confused with account identity

## Core ideas

This profile introduces:

- `Dimension`
- `DimensionValue`
- `LineDimensionAssignment`

These structures let a line carry additional analytical meaning such as:

- department
- project
- tax code
- cost center
- product line

## Dimension

A `Dimension` defines one analytical axis.

Suggested fields:

- `id`
- `book_id`
- `name`
- `dimension_type`
- `status`

### `dimension_type`

Examples:

- `department`
- `project`
- `tax_code`
- `cost_center`
- `custom`

Custom values should remain allowed even when implementations ship with
well-known types.

## Dimension value

A `DimensionValue` defines one value within a dimension.

Suggested fields:

- `id`
- `dimension_id`
- `code`
- `label`
- `status`

## Line assignment

A `LineDimensionAssignment` binds a posted or draft line to one dimension
value.

Suggested fields:

- `line_id`
- `dimension_value_id`

This draft does not require assignments to be denormalized into line payloads,
but implementations may expose either:

- explicit assignment packets
- inline line metadata
- or both

## Invariants

At minimum:

- dimension definitions belong to one book
- values belong to one dimension
- assignments bind existing lines to existing values
- dimensions must not violate line/account/book ownership boundaries
- analytical tags must not alter balance semantics

## Relationship to reports

This profile is about analytical structure and assignment.

It does not yet standardize:

- every summary output
- every rollup rendering
- every pivot/report transport format

It should make it possible for another implementation to preserve analytical
tags and then derive its own summaries correctly.

## Suggested packet shapes

### `dimensions`

Dimension definitions for a book.

### `dimension-values`

Values for one or more dimensions.

### `line-dimension-assignments`

Assignments linking lines to analytical values.

### `dimension-profile-bundle`

A grouped bundle containing:

- dimensions
- values
- assignments

## What this profile is not

This profile is not:

- a replacement for account segmentation in the core ledger
- a generic BI standard
- a full reporting standard for all analytical summaries

It is a portable way to preserve analytical tagging semantics.

## Relationship to Heft

Heft already has:

- dimension definitions
- dimension values
- line assignment support
- dimension summaries
- rollup reporting

So Heft is a strong candidate implementation target for this profile once the
packet shapes are formalized.

## Current draft posture

This profile should currently be treated as:

- motivated by real engine behavior
- useful for richer accounting interoperability
- not yet implemented as a stable OBLE packet family

## Immediate next steps

1. define a minimal example with one tax-code dimension and one assigned line
2. decide whether assignments should be exported inline, separately, or both
3. decide whether rollup summaries belong in this profile or in a later analytics profile
