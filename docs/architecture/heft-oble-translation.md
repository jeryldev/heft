# Heft to OBLE Terminology

Status: Draft

## Purpose

This note explains how `Heft` native terms relate to `OBLE` portable terms.

`Heft` uses some implementation-oriented accounting language that is more
native to the engine than to the standard. `OBLE` uses the more portable public
vocabulary.

The goal is not to claim the terms are identical.

The goal is to make the translation explicit so readers do not confuse:

- engine-native accounting terminology
- portable standards terminology
- storage-level naming

## Quick mapping

| Heft native term | OBLE portable term | Notes |
| --- | --- | --- |
| `SubledgerAccount` | `Counterparty` | In Heft, the party record still lives in a subledger/control-account architecture. In OBLE, the portable noun is `Counterparty`. |
| `SubledgerGroup` | `SubledgerGroup` / control-account relationship | The concept is close in both, but OBLE centers counterparties first. |
| `SubledgerAccount.type` | `Counterparty.role` | This is the most important terminology drift to keep in mind. |
| `counterparty_id` on entry lines | `counterparty_id` | This is already aligned between Heft and OBLE. |
| open-item workflow | `OpenItem` profile semantics | Strong conceptual alignment. |
| `book snapshot` export | snapshot bundle | A snapshot is a bundle flavor, not a separate profile. |
| result exports | result packets | Heft exports many report/result surfaces as OBLE packets. |

## Why Heft still says `subledger`

This is intentional in some parts of the engine.

In accounting architecture, a subledger is the control-account-linked domain
that lets party-level activity reconcile to a GL bucket such as:

- accounts receivable
- accounts payable

So `Heft` keeps `subledger` to describe:

- control-account relationships
- reconciliation
- aging
- AR/AP-style workflows

## Why OBLE prefers `counterparty`

`OBLE` is trying to standardize portable meaning across different engines and
domains.

Not every implementation has:

- separate customer and supplier tables
- a visible subledger-group primitive
- the same internal naming as Heft

So the standard leads with:

- `Counterparty`
- `role`
- `OpenItem`

and then describes subledger-style control-account relationships as part of the
profile semantics.

## Recommended reading rule

When reading `Heft` docs and code:

- think `counterparty` when you see `SubledgerAccount`
- think `counterparty role` when you see subledger `type`
- think `control-account architecture` when you see `subledger`

That will make the Heft ↔ OBLE mapping much easier to follow.

## Preferred public vocabulary

For outward-facing docs and integrations, prefer:

- `counterparty`
- `role`
- `open item`
- `subledger` only when discussing control-account relationships,
  reconciliation, or AR/AP architecture

For internal/storage code, native Heft terminology may remain more
subledger-oriented where changing it would add churn without improving the
public surface.
