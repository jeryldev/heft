# Why Heft

Heft exists for products that need real accounting behavior without adopting a
full accounting suite or rebuilding ledger correctness from scratch.

Most teams that say "we just need a ledger" eventually discover that the hard
part is not storing rows. The hard part is keeping accounting rules, lifecycle
rules, audit history, and reporting behavior consistent as the system grows.

## The problem Heft is solving

A generic database schema or a few journal tables usually do not give you:

- double-entry enforcement
- period-aware posting rules
- append-only audit history
- fixed-point money math
- multi-currency handling
- subledger and open-item behavior
- close and reopen lifecycle behavior
- accounting-aware report surfaces

Those rules get rebuilt piecemeal in application code, and over time they
become harder to reason about, harder to test, and harder to migrate.

Heft exists to make that core accounting layer explicit, embeddable, and
reusable.

## What Heft gives you

Heft is:

- an embedded accounting engine
- backed by SQLite
- built in Zig
- exposed through both a Zig API and a C ABI

It is designed to give applications a small, deterministic ledger runtime with:

- transactional posting
- fixed-point arithmetic
- append-only audit posture
- accounting periods and lifecycle controls
- subledger and counterparty support
- dimensions, classifications, and budgets
- export and interchange surfaces through OBLE

## Why not just use a SaaS accounting API

Hosted accounting systems are useful, but they are a different product shape.

They are usually:

- networked and multi-tenant
- proprietary in data model and lifecycle behavior
- harder to embed deeply into a product
- awkward when you need local or portable ledger state

Heft is for teams that want the ledger core inside their own application or
runtime, not behind someone else's product boundary.

## Why not just design a schema yourself

You can design ledger tables yourself, but the tables are only the start.

The real complexity lives in:

- invariants
- balance maintenance
- lifecycle transitions
- reversals and voids
- audit behavior
- report semantics
- long-term migration and interoperability

Heft gives you a coherent engine for that layer instead of asking every product
team to rediscover it independently.

## Why SQLite matters

SQLite gives Heft a practical deployment shape:

- single-file storage
- transactional durability
- low operational overhead
- easy embedding
- easy backup and portability

That makes Heft useful for products that want a serious ledger engine without
also operating a separate accounting service.

## Why the C ABI matters

Heft is Zig-native, but it is not Zig-only.

The C ABI makes it realistic to use Heft from:

- Elixir
- Python
- Node.js
- Go
- Ruby
- other native or hosted runtimes

That boundary is important because the point of Heft is not just elegant Zig
code. The point is a reusable accounting core that other systems can actually
consume.

## Why Heft and OBLE are separate

Heft is the engine.

OBLE is the exchange language.

Heft exists so systems can run accounting well.
OBLE exists so systems can move accounting meaning well.

Keeping them separate gives you:

- an embeddable implementation
- a neutral standard
- better migration and interoperability paths

## The short version

Heft is for teams that need:

- embedded accounting behavior
- strong invariants
- portable local ledger state
- a real engine instead of ad hoc ledger tables

It is best thought of as:

**SQLite for accounting, with lifecycle and invariants included.**
