# Branded Architecture Diagram Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Mermaid with a single HTML/CSS architecture diagram that shows official logos (when present) and sizing quantities; dual-site when `includeDr`.

**Architecture:** `engine/architecture-diagram.mjs` builds a data model + escaped HTML fragment. UI injects it into Results/print. Brand SVGs live under `docs/assets/brand/`. Sync copies the engine module to `docs/assets/js/`.

**Tech Stack:** Vanilla ES modules, existing PatternFly-like CSS, Node test runner.

**Spec:** `docs/superpowers/specs/2026-08-31-branded-architecture-diagram-design.md`

---

## File map

| File | Responsibility |
|------|----------------|
| `engine/architecture-diagram.mjs` | HTML diagram builder + summary |
| `docs/assets/js/architecture-diagram.mjs` | Synced copy |
| `docs/assets/js/app.js` | Inject HTML; remove Mermaid |
| `docs/assets/css/site.css` | Arch diagram layout + print |
| `docs/assets/brand/*.svg` | Logos (OpenShift + Red Hat) |
| `docs/_layouts/default.html` | Cache-bust |
| `docs/local-print-preview.html` | Cache-bust |
| `tests/architecture-diagram.test.mjs` | HTML/dual/quantity assertions |
| `scripts/sync-engine.mjs` | Unchanged behavior (verify copy) |

---

### Task 1: Failing tests for HTML diagram API

**Files:**
- Modify: `tests/architecture-diagram.test.mjs`

- [ ] **Step 1:** Rewrite tests to expect `format === 'html'`, `summary.layout`, dual when `includeDr`, quantity substrings (`brokerNodes`, `×`), and absence of `flowchart`.
- [ ] **Step 2:** Run `node --test tests/architecture-diagram.test.mjs` — expect FAIL.
- [ ] **Step 3:** Commit: `test: expect HTML architecture diagram instead of Mermaid`

---

### Task 2: Implement HTML architecture builder

**Files:**
- Modify: `engine/architecture-diagram.mjs`

- [ ] **Step 1:** Replace Mermaid/PlantUML with `buildHtml(title, input, result, summary)` and logo registry (relative paths under `assets/brand/`).
- [ ] **Step 2:** Single vs dual layout; escape all dynamic text.
- [ ] **Step 3:** Run architecture tests — expect PASS.
- [ ] **Step 4:** Commit: `feat: render branded HTML architecture diagram with quantities`

---

### Task 3: Wire UI + CSS; remove Mermaid

**Files:**
- Modify: `docs/assets/js/app.js`
- Modify: `docs/assets/css/site.css`
- Modify: `docs/_layouts/default.html`, `docs/local-print-preview.html` (cache-bust)
- Run: `node scripts/sync-engine.mjs`

- [ ] **Step 1:** Sync engine; inject `arch.diagram` into preview; delete Mermaid CDN/helpers and `.mmd` download.
- [ ] **Step 2:** Add `.streams-arch-*` CSS for screen + print.
- [ ] **Step 3:** Ensure OpenShift SVG is under `docs/assets/brand/` (copy from refs if needed).
- [ ] **Step 4:** Bump `app.js` and `architecture-diagram.mjs` query versions.
- [ ] **Step 5:** `npm test`; smoke local print preview.
- [ ] **Step 6:** Commit: `feat: show branded arch diagram in Results; drop Mermaid`

---

### Task 4: Docs touch

**Files:**
- Modify: `README.md` or `docs/usage-walkthrough.md` (one line: architecture is HTML branded, not Mermaid)

- [ ] **Step 1:** Update mention of Mermaid architecture if present.
- [ ] **Step 2:** Commit: `docs: note branded HTML architecture diagram`
