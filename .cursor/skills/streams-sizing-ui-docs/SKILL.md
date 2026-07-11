---
name: streams-sizing-ui-docs
description: >-
  Evolve the streams-sizing GitHub Pages wizard, PatternFly-like CSS, methodology/
  verification docs, walkthrough screenshots, and cache-bust. Use for UI, Jekyll docs,
  parameter help, Results layout, or brand assets in docs/.
---

# streams-sizing UI & docs

## Canonical paths

| Path | Role |
|------|------|
| `docs/index.md` | Calculator page (`calculator: true`) |
| `docs/assets/js/app.js` | Wizard steps, Results, export/import |
| `docs/assets/css/site.css` | PatternFly-inspired layout |
| `docs/_layouts/default.html` | Masthead, meta/OG, script cache-bust |
| `docs/_layouts/doc.html` | Markdown doc wrapper |
| `docs/methodology.md`, `docs/verification.md`, `docs/usage-walkthrough.md` | Docs |
| `docs/assets/brand/` | Logo, favicon, OG image |
| `docs/assets/screenshots/walkthrough/` | Walkthrough PNGs |

## UI conventions

- Wizard steps: Workload → Durability → Consumers → Platform → Results.
- Parameter help: explain **what** and **default source** (`fixture-light` JSON); no “validate with load simulation” boilerplate.
- Show **Total cluster** (brokers + controllers + optional RHAF/integrations).
- Expose trace highlights: `amplificationFactor`, disk headroom, warnings, experimental compute CPU vs subscription cores.
- Mixed retention help: **X% = share of write volume (MB/s), not topic count**.
- `duplexMode`: full vs half with clear tooltips.

## Docs (Jekyll)

- Markdown pages use `layout: doc` — content must not be wrapped in raw HTML blocks (kramdown bug).
- After substantive UI changes, update walkthrough screenshots and `docs/usage-walkthrough.md`.

## Cache-bust

After `app.js` or synced engine changes:

1. Bump `?v=N` on `app.js` in `docs/_layouts/default.html`.
2. Bump `sizing-engine.mjs?v=N` import inside `app.js`.

## Brand

- Masthead logo: `docs/assets/brand/red-hat-logo-on-dark.svg` — do not modify artwork.
- Favicon: `node scripts/generate-favicon.mjs` after SVG change.
- OG: `docs/assets/brand/og-streams-sizing.png`; meta tags in `default.html`.

## Checklist before PR

- [ ] `node scripts/sync-engine.mjs`
- [ ] `npm test`
- [ ] Cache-bust versions bumped
- [ ] Methodology updated if semantics changed
- [ ] Walkthrough screenshots if wizard layout changed materially

## Anti-patterns

- Default Inter/Roboto-only stacks without Red Hat Text where brand applies.
- Card-heavy hero on landing — keep calculator-first, minimal chrome.
- Editing `docs/assets/js/sizing-engine.mjs` without sync from `engine/`.
