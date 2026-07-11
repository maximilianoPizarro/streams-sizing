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
    .resize(size, size, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
    .png()
    .toFile(join(outDir, `favicon-${size}.png`));
}

await sharp(svg, { density: 300 })
  .resize(32, 32, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
  .toFile(join(outDir, 'favicon.ico'));

console.log('Generated favicon-16.png, favicon-32.png, favicon.ico');
