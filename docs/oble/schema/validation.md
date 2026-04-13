# OBLE Schema Validation

Status: Draft

This document explains how to validate the current OBLE example payloads
against the draft JSON Schemas.

## What is being validated

The current schema/example mapping is recorded in:

- [example-map.json](example-map.json)

That mapping currently covers:

- `core-book.json`
- `core-accounts.json`
- `core-periods.json`
- `core-entry-posted.json`
- `reversal-pair.json`
- `counterparty-open-item.json`

## Why there is no bundled validator yet

The repo currently ships:

- OBLE draft schemas
- OBLE example payloads

But it does not yet vendor one required JSON Schema validator tool.

That is intentional for now. The draft is still evolving, and it is better to
keep the validation process explicit than to pretend the repo has one canonical
toolchain before we are ready to support it.

## Recommended validation approaches

Any validator that supports JSON Schema draft 2020-12 should work.

Examples:

- `ajv-cli`
- `check-jsonschema`
- any equivalent validator that can resolve local schema references

## Example with ajv-cli

If you have `ajv-cli` installed, validate a single example like this:

```bash
ajv validate \
  -s docs/oble/schema/book.schema.json \
  -d docs/oble/examples/core-book.json
```

Validate the entry example:

```bash
ajv validate \
  -s docs/oble/schema/entry.schema.json \
  -d docs/oble/examples/core-entry-posted.json
```

Validate the extension examples:

```bash
ajv validate \
  -s docs/oble/schema/reversal-pair.schema.json \
  -d docs/oble/examples/reversal-pair.json

ajv validate \
  -s docs/oble/schema/counterparty-open-item.schema.json \
  -d docs/oble/examples/counterparty-open-item.json
```

## Validation expectation

At this stage, validation means:

- the examples conform to the current schema drafts
- local schema references resolve correctly
- the examples and schemas evolve together

It does not yet mean:

- full semantic conformance of an accounting engine
- proof that an implementation satisfies all OBLE lifecycle invariants

That later level should come from fixture-driven conformance tests, not just
schema validation.

## Next step

Once OBLE stabilizes a bit more, the next useful move is:

- add one small validator script or CI job
- use `example-map.json` as the source of truth for what gets validated
