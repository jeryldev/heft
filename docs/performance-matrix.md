# Performance Matrix

This matrix tracks benchmark coverage across Heft's feature surface.

## Status meanings

- `Covered`: there is a direct benchmark scenario for the feature or feature family.
- `Partially Covered`: the feature shares benchmarked primitives, but does not yet have a dedicated scenario of its own.
- `Pending`: no direct benchmark coverage yet.

## Current matrix

| Feature area | Status | Coverage surface |
| --- | --- | --- |
| Trial balance | Covered | `trial_balance`, `query_scale` |
| General ledger | Covered | `general_ledger`, `query_scale` |
| Aged subledger | Covered | `aged_subledger`, `query_scale` |
| Income statement | Covered | `statement_suite` |
| Trial balance movement | Covered | `statement_suite` |
| Balance sheet | Covered | `statement_suite` |
| Balance sheet with projected retained earnings | Covered | `statement_suite` |
| Comparative reports | Covered | `comparative_suite` |
| Equity changes | Covered | `comparative_suite` |
| Chart of accounts export | Covered | `export_suite` |
| Journal entry export | Covered | `export_suite` |
| Audit trail export | Covered | `export_suite` |
| Period export | Covered | `export_suite` |
| Subledger export | Covered | `export_suite` |
| Book metadata export | Covered | `export_suite` |
| Period close | Covered | `close_period`, phase profile |
| Close variants | Covered | `close_direct`, `close_income_summary`, `close_allocated` |
| Cache stale recalculation | Covered | `recalculate_stale` |
| Generated posting path | Covered | `post_generated_entry` |
| Opening entry generation | Covered | `generate_opening_entry` |
| FX revaluation | Covered | `revalue` |
| Fixture generation / seeding | Covered | `seed_report`, `seed_close`, `seed_revalue` |
| Classification tree maintenance | Pending | No dedicated benchmark yet |
| Classified balance sheet / income statement | Covered | `classification_suite` |
| Cash flow statement | Covered | `classification_suite` |
| Indirect cash flow statement | Covered | `classification_suite` |
| Classified trial balance | Covered | `classification_suite` |
| Dimensions and dimension summaries | Covered | `dimension_suite` |
| Budgets / budget vs actual | Covered | `budget_suite` |
| Open item listing / allocation | Covered | `open_item_suite` covers listing and direct allocation |
| Buffer query endpoints beyond aged subledger | Covered | `abi_buffer_suite` |
| C ABI marshalling overhead | Partially Covered | `abi_buffer_suite` covers key buffer endpoints and one ABI export path |

## What this means for release readiness

Heft now has direct benchmark coverage for the core accounting lifecycle, the main financial statements, comparative reporting, and the export surface most likely to matter for embedded usage.

The remaining benchmark gaps are concentrated in secondary and support surfaces:

- classification tree maintenance
- complete C ABI coverage beyond the current key buffer/export paths

Those are good candidates for the next benchmark wave, but they are no longer blocking a first public release if the goal is to ship the ledger core with honest performance notes.
