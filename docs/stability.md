# Stability Statement

Heft `0.1.1` is the current public prerelease line for the embedded accounting
engine.

## What is stable enough to use now

- the SQLite-backed ledger core
- double-entry posting and posting validation
- period lifecycle and close flows
- retained earnings, income summary, FX, and other designation-based book policy
- reporting, comparative reporting, and export surfaces
- subledger, open-item, dimension, and budget primitives
- the C ABI ownership model and error-code contract

## What is still expected to evolve before `1.0`

- API shape and naming at the Zig surface
- packaging and distribution ergonomics
- benchmark breadth for lower-priority features
- documentation depth and example coverage
- CI and release automation details

## Compatibility expectations for `0.x`

Before `1.0`, Heft should be treated as a fast-moving library:

- schema migrations are supported within the shipped engine
- public behavior should remain intentional and tested
- breaking API changes are still possible across minor releases
- benchmark baselines may improve as query and ABI paths are refined

## What `1.0` should mean later

The `1.0` milestone should imply:

- stronger compatibility guarantees for the Zig and C ABI surfaces
- more explicit migration and deprecation policy
- fuller benchmark coverage across the whole feature surface
- release automation and documentation that are routine rather than aspirational
