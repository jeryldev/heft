# OBLE JSON Schemas

Status: Draft

This folder contains early JSON Schema drafts for the current OBLE example
payloads.

The goal is modest:

- make the current examples machine-checkable
- create a foundation for future conformance tooling
- avoid overcommitting the spec before the drafts settle

Current scope:

- `book.schema.json`
- `account.schema.json`
- `accounts.schema.json`
- `period.schema.json`
- `periods.schema.json`
- `entry.schema.json`
- `counterparties.schema.json`
- `policy-profile.schema.json`
- `classification-profile.schema.json`
- `dimension-profile.schema.json`
- `budget-profile.schema.json`
- `book-snapshot.schema.json`
- `reversal-pair.schema.json`
- `counterparty-open-item.schema.json`
- `example-map.json`
- `validation.md`

These schemas intentionally validate the current draft examples and core field
shapes. They should be treated as early draft artifacts, not final normative
contracts yet.

Suggested mapping to the example payloads:

- `examples/core-book.json` -> `book.schema.json`
- `examples/core-accounts.json` -> `accounts.schema.json`
- `examples/core-periods.json` -> `periods.schema.json`
- `examples/core-entry-posted.json` -> `entry.schema.json`
- `examples/counterparties.json` -> `counterparties.schema.json`
- `examples/policy-profile.json` -> `policy-profile.schema.json`
- `examples/classification-profile.json` -> `classification-profile.schema.json`
- `examples/dimension-profile.json` -> `dimension-profile.schema.json`
- `examples/budget-profile.json` -> `budget-profile.schema.json`
- `examples/book-snapshot.json` -> `book-snapshot.schema.json`
- `examples/reversal-pair.json` -> `reversal-pair.schema.json`
- `examples/counterparty-open-item.json` -> `counterparty-open-item.schema.json`

See also:

- [Validation Guide](validation.md)
- [Example Map](example-map.json)
- [`scripts/validate-oble.sh`](../../../scripts/validate-oble.sh)
