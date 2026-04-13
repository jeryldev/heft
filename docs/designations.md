# Book Designations

Heft uses a designation-based approach per book of account.

This means accounting behavior is not globally hardcoded. Instead, a book
chooses specific accounts to serve specific accounting roles.

That is one of the central design decisions in Heft.

## Why designations exist

Designations let the engine stay generic while still supporting real accounting
lifecycle behavior.

Without designations, the engine would have to either:

- hardcode account numbers and account semantics
- push critical accounting decisions into application code
- or avoid lifecycle features like close, revalue, suspense enforcement, and
  carry-forward entirely

With designations, the engine can say:

- this book closes equity here
- this book routes FX revaluation here
- this book requires approval before posting
- this book uses income summary
- this book treats suspense as a control account

## Main designations

### Retained earnings / equity close target

Used by close logic when the entity type closes net income into a single equity
account.

Relevant APIs:

- `ledger_set_retained_earnings_account`
- `ledger_set_equity_close_target`

### Income summary account

Optional intermediate account used for multi-step close behavior.

Relevant API:

- `ledger_set_income_summary_account`

### FX gain/loss account

Required for FX revaluation workflows.

Relevant API:

- `ledger_set_fx_gain_loss_account`

### Rounding account

Optional account used when small FX/base rounding differences are auto-posted.

Relevant API:

- `ledger_set_rounding_account`

### Opening balance account

Used for opening-balance migration and validation workflows.

Relevant API:

- `ledger_set_opening_balance_account`
- `ledger_validate_opening_balance`

### Suspense account

Used as a control point so close can refuse to proceed while unresolved suspense
balances remain.

Relevant API:

- `ledger_set_suspense_account`

### Dividends / drawings account

Used for year-end close behavior where owner/partner distributions are swept
appropriately.

Relevant API:

- `ledger_set_dividends_drawings_account`

### Current year earnings account

Intermediate equity account for specific equity lifecycle approaches.

Relevant API:

- `ledger_set_current_year_earnings_account`

### Fiscal-year start month

Defines the fiscal-year boundary used by lifecycle operations.

Relevant API:

- `ledger_set_fy_start_month`

### Entity type

Determines which close behavior is appropriate.

Examples:

- corporation
- sole proprietorship
- partnership
- LLC
- nonprofit

Relevant API:

- `ledger_set_entity_type`

## How designations affect behavior

Designations are not cosmetic metadata. They change engine behavior.

Examples:

- close logic changes depending on whether a book uses direct close,
  income-summary close, or allocation-based close
- revaluation requires a designated FX gain/loss account
- balance migration validation depends on the opening balance account
- period close can fail if the suspense account is not cleared
- year-end sweep behavior depends on the dividends/drawings designation

## Required vs optional

Some designations are only needed when a workflow uses them.

Examples:

- FX gain/loss is only required for revaluation
- opening balance account is only required for opening-balance migration
- income summary is only required if the book chooses that close path

Other designations are effectively required for a normal accounting lifecycle.

Examples:

- an equity close target for entity types that close into one account
- entity type if you want close behavior aligned to legal/economic form

## Entity type and allocations

Partnerships and LLCs use allocation-driven close behavior instead of a single
retained-earnings target.

That means:

- entity type matters
- active equity allocations must exist
- active equity allocations must total exactly 100%

This is why designations in Heft are not only “which account is special” but
also “what kind of accounting entity is this book”.

## Why this is a per-book decision

Different books may need different policy, even inside one application.

Examples:

- one book may use direct close
- another may use income summary
- another may be a partnership using equity allocations
- one may require approvals
- another may not

Putting designations on the book lets one engine support all of those without
forking the accounting rules in application code.

## Suggested setup order

1. Create the book.
2. Create the chart of accounts.
3. Set entity type and fiscal-year start.
4. Designate key lifecycle accounts.
5. Create periods.
6. Start posting.

## Release note for users

If you are embedding Heft, think of book designations as part of your accounting
policy configuration, not just as optional metadata.
