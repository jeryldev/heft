# Heft Examples

These examples are intentionally small and practical.

They are meant to help a new user:

- open a ledger
- create a book
- create accounts and periods
- post a basic journal entry
- query a report
- see where OBLE export fits in

## Available examples

### `basic_heft.zig`

Creates an in-memory ledger, posts a simple sale, prints a trial balance, and
exports the OBLE core bundle for the resulting book.

Run it from the repo root with:

```bash
zig build example-basic
```

This example is intentionally small enough to read in one sitting.
