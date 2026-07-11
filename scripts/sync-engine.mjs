import { readFileSync, writeFileSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const destDir = join(root, 'docs', 'assets', 'js');
mkdirSync(destDir, { recursive: true });

for (const name of ['sizing-engine.mjs', 'architecture-diagram.mjs']) {
  const src = join(root, 'engine', name);
  const dest = join(destDir, name);
  writeFileSync(dest, readFileSync(src, 'utf8'));
  console.log(`Synced engine/${name} -> docs/assets/js/${name}`);
}

const fxSrc = join(root, 'docs', 'fixtures');
const fxDest = join(root, 'docs', 'assets', 'fixtures');
mkdirSync(fxDest, { recursive: true });
for (const f of readdirSync(fxSrc).filter((n) => n.endsWith('.json'))) {
  writeFileSync(join(fxDest, f), readFileSync(join(fxSrc, f), 'utf8'));
}
console.log('Synced fixtures -> docs/assets/fixtures/');
