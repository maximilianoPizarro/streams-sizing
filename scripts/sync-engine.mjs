import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const src = join(root, 'engine', 'sizing-engine.mjs');
const dest = join(root, 'docs', 'assets', 'js', 'sizing-engine.mjs');

mkdirSync(dirname(dest), { recursive: true });
writeFileSync(dest, readFileSync(src, 'utf8'));
console.log('Synced engine -> docs/assets/js/sizing-engine.mjs');
