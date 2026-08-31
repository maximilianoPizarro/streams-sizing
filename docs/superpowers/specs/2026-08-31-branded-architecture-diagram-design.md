# Branded architecture diagram (no Mermaid)

**Date:** 2026-08-31  
**Status:** Approved (user: scope C, renderer B, single diagram — eliminate Mermaid)

## Goal

Replace the Mermaid architecture preview with **one** HTML/CSS diagram that uses official Red Hat brand assets where available and always shows **quantities** from the sizing result. Print/PDF uses the same markup.

## Behavior

| Condition | Layout |
|-----------|--------|
| `includeDr !== true` | Single site panel (OpenShift + Streams + clients/RHAF/integrations as applicable) |
| `includeDr === true` | Dual site: Site A (active) ↔ MirrorMaker 2 ↔ Site B (replica); Site B mirrors broker/controller counts from the sized cluster |

## Node content

Each node card:

- Optional logo (`<img>` from `docs/assets/brand/…`) when an official asset exists in-repo
- Product / role label (official product names; no invented logos)
- Quantities from result/input, e.g.:
  - Brokers: `N × vCPU · Gi · GB PVC`
  - Controllers: `N × vCPU · Gi`
  - RHAF / Camel / Quarkus / MM2: `instances × vCPU` (and Gi when present)

## Brand assets

| Product | Asset policy |
|---------|----------------|
| Red Hat corporate | `docs/assets/brand/red-hat-logo-on-dark.svg` or standard logo SVG in brand/ |
| OpenShift | Official SVG under `docs/assets/brand/` (from public RHDC managed-files) |
| Streams, Camel, Keycloak, Quarkus, Apicurio, Bridge, Console, Cruise Control, MM2 | Text card until Brand Portal SVG/PNG is added; map file names in a small logo registry |

Do not recreate or distort Red Hat marks.

## API (`engine/architecture-diagram.mjs`)

- Primary: `architectureDiagramFromScenario(scenario, opts?)` returns:
  - `format: 'html'`
  - `diagram: string` (HTML fragment, safe escaped text)
  - `summary: object` (unchanged fields + `layout: 'single' | 'dual'`)
- Remove Mermaid/PlantUML builders and `format: 'mermaid' | 'plantuml'` options.
- Sync to `docs/assets/js/` via `scripts/sync-engine.mjs`.

## UI (`docs/assets/js/app.js`)

- Inject `arch.diagram` into `#architecture-preview` (no Mermaid CDN).
- Remove Mermaid load/render, `.mmd` download, and Mermaid source `<details>`.
- Keep a single architecture print sheet.

## CSS

- New rules under `.streams-arch-*` for cards, site panels, MM2 bridge, logos, print page break.

## Tests

- Update `tests/architecture-diagram.test.mjs`: assert `format === 'html'`, dual layout when `includeDr`, quantity strings present, no `flowchart` Mermaid.
- Sync-engine still copies `architecture-diagram.mjs`.

## Out of scope

- Pixel-perfect clone of the one-off GenerateImage PNG
- Authenticated Brand Portal downloads in CI
- Changing sizing math
