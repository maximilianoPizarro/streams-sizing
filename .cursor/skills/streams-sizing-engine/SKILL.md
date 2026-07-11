---
name: streams-sizing-engine
description: >-
  Change the streams-sizing analytical engine (Andy Yuen model). Use when editing
  engine/sizing-engine.mjs, trace fields, fixtures, golden tests, sync-engine,
  or calibration margins — without altering core net/disk/partition formulas.
---

# streams-sizing engine

## Canonical paths

| Path | Role |
|------|------|
| `engine/sizing-engine.mjs` | Source of truth |
| `docs/assets/js/sizing-engine.mjs` | Browser copy — sync only |
| `scripts/sync-engine.mjs` | `npm run sync-engine` |
| `docs/fixtures/*.json` | Golden inputs + expected outputs |
| `docs/assets/fixtures/` | Copy of fixtures for static site |
| `tests/sizing-engine.test.mjs` | Node test runner |

## Workflow checklist

1. Edit **only** `engine/sizing-engine.mjs`.
2. Run `node scripts/sync-engine.mjs` (or `npm run sync-engine`).
3. Copy fixtures: `cp docs/fixtures/*.json docs/assets/fixtures/`.
4. Update golden values in fixtures when disk/margin defaults change intentionally.
5. Add or extend tests in `tests/sizing-engine.test.mjs`.
6. Run `npm test` — all tests must pass before claiming done.
7. Bump cache-bust on engine import in `docs/assets/js/app.js` (`sizing-engine.mjs?v=N`).

## Rules (do not break)

- **Do not alter** core formulas for `writes`, `netWrite`, `netRead`, `diskIO`, `partitions`, `controllers` unless explicitly requested.
- Allowed changes: margins (`maxUtil`, `safetyFactor`, `amplificationFactor`), disk headroom (`diskCapacityHeadroom`, `diskSegmentOverhead`), optional inputs with backward-compatible defaults, warnings, trace exposure, experimental estimates (CPU, RAM cache).
- New optional inputs must default to prior behaviour (except documented breaking changes like disk headroom 1.25 + 0.05).
- Keep `ENGINE_VERSION` in sync with meaningful calibration releases.
- Do **not** overwrite or port from `legacy/spring-boot` Java at runtime — reference only for verification.

## Trace conventions

Expose audit fields in `result.trace`: `amplificationFactor`, `capacityHeadroom`, `segmentOverhead`, `kraftMetadataMBps`, `duplexMode`, `diskIOBoostFromRam`, partition density, etc.

## Acceptance

- `npm test` green (22+ tests).
- Export/import of fixture JSON reproduces same sizing.
- Brokers unchanged for `fixture-light` / `fixture-heavy` unless formula intent changed.

## Anti-patterns

- Editing `docs/assets/js/sizing-engine.mjs` directly without sync.
- Inventing benchmark numbers for experimental CPU — mark `experimental: true`.
- Applying RAM page-cache boost when `ramPerBrokerGB` is unset (use default path only).
