# OBLE-0019: Audit Trail and Provenance Result Packets

Status: Draft

## Purpose

This draft defines export-first result packets for portable audit-trail and
provenance inspection.

These packets are meant for:

- inspection
- compliance review
- browser tooling
- cross-engine provenance visibility

They are not meant to replay native engine audit internals as authoritative
history in another ledger.

## Audit Trail Result

The audit-trail result packet expresses:

- the book being inspected
- the reporting date range
- immutable audit records emitted by the engine
- actor and timestamp information
- optional chain/hash material when the engine exposes it

Recommended packet kind:

- `audit_trail`

## Provenance posture

The audit-trail result packet is export-first.

It can carry portable provenance facts, but it is not a promise that every
native engine audit mechanism is identical across implementations.

Engines may expose richer internal provenance than what the packet standardizes.
