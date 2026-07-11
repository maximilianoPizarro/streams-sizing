/**
 * Generate favicon PNG sizes from docs/assets/brand/favicon.svg
 * Run: node scripts/generate-favicon.mjs
 */
import sharp from 'sharp';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const svg = readFileSync(join(root, 'docs/assets/brand/favicon.svg'));
const outDir = join(root, 'docs/assets/brand');

for (const size of [16, 32]) {
  await sharp(svg, { density: 300 })
    .resize(size, size, { fit: 'contain', background: { r: 21, g: 21, b: 21, alpha: 1 } })
    .png()
    .toFile(join(outDir, `favicon-${size}.png`));
}

await sharp(svg, { density: 300 })
  .resize(32, 32, { fit: 'contain', background: { r: 21, g: 21, b: 21, alpha: 1 } })
  .toFile(join(outDir, 'favicon.ico'));

console.log('Generated favicon-16.png, favicon-32.png, favicon.ico');
