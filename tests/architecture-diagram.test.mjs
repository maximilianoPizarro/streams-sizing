import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sizeKafkaCluster } from '../engine/sizing-engine.mjs';
import { architectureDiagramFromScenario } from '../engine/architecture-diagram.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

test('architecture diagram Mermaid from fixture-light', () => {
  const fx = JSON.parse(
    readFileSync(join(root, 'docs/fixtures/fixture-light.json'), 'utf8')
  );
  const result = sizeKafkaCluster(fx.input);
  const { diagram, summary, format } = architectureDiagramFromScenario(
    { name: fx.id, input: fx.input, result },
    { format: 'mermaid' }
  );
  assert.equal(format, 'mermaid');
  assert.equal(summary.brokers, result.brokerNodes);
  assert.match(diagram, /flowchart TB/);
  assert.match(diagram, /Kafka brokers/);
  assert.match(diagram, /KRaft controllers/);
});

test('synced docs assets match engine sources', () => {
  for (const name of ['sizing-engine.mjs', 'architecture-diagram.mjs']) {
    const a = readFileSync(join(root, 'engine', name), 'utf8');
    const b = readFileSync(join(root, 'docs/assets/js', name), 'utf8');
    assert.equal(a, b, `${name} out of sync — run npm run sync-engine`);
  }
});
