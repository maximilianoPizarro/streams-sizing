import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sizeKafkaCluster } from '../engine/sizing-engine.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

function loadFixture(name) {
  const path = join(root, 'docs', 'fixtures', `${name}.json`);
  return JSON.parse(readFileSync(path, 'utf8'));
}

test('fixture-light: year-0 throughput and storage', () => {
  const fx = loadFixture('fixture-light');
  const result = sizeKafkaCluster(fx.input);
  assert.equal(result.ingressMBps, fx.expected.ingressMBps);
  assert.equal(result.dailyDiskUsageGB, fx.expected.dailyDiskUsageGB);
  assert.ok(result.brokerNodes >= fx.expected.brokerNodesMin);
  assert.equal(result.controllerNodes, fx.expected.controllerNodes);
  assert.equal(result.platform, 'openshift');
  assert.ok(result.platformDetails.kafkaNodePools);
});

test('fixture-light: year-3 projection', () => {
  const fx = loadFixture('fixture-light');
  const input = { ...fx.input, ...fx.expected.year3 };
  const result = sizeKafkaCluster(input);
  assert.equal(result.ingressMBps, fx.expected.year3.ingressMBps);
  assert.equal(result.dailyDiskUsageGB, fx.expected.year3.dailyDiskUsageGB);
  assert.equal(
    result.totalDiskStorageGB,
    fx.expected.year3.totalDiskStorageMixedRetentionGB
  );
});

test('fixture-light: mixed retention effective days', () => {
  const fx = loadFixture('fixture-light');
  const days = fx.input.retentionDays * (1 - 0.2) + fx.input.extendedRetentionDays * 0.2;
  assert.equal(Math.round(days * 10) / 10, fx.expected.mixedRetentionEffectiveDays);
  const result = sizeKafkaCluster(fx.input);
  assert.equal(result.retentionEffectiveDays, days);
});

test('fixture-light: trace is reproducible', () => {
  const fx = loadFixture('fixture-light');
  const a = sizeKafkaCluster(fx.input);
  const b = sizeKafkaCluster(fx.input);
  assert.deepEqual(a.trace, b.trace);
});

test('fixture-heavy: high throughput storage and bottleneck', () => {
  const fx = loadFixture('fixture-heavy');
  const result = sizeKafkaCluster(fx.input);
  assert.equal(result.ingressMBps, fx.expected.ingressMBps);
  assert.equal(result.dailyDiskUsageGB, fx.expected.dailyDiskUsageGB);
  assert.equal(result.bindingConstraint, fx.expected.bindingConstraint);
  assert.ok(result.brokerNodes >= fx.expected.brokerNodesMin);
  assert.ok(
    result.brokerNodes > fx.expected.architectureReferenceBrokers,
    'Bottleneck sizing should exceed architecture reference broker count'
  );
});

test('fixture-heavy: failover subscription policy', () => {
  const fx = loadFixture('fixture-heavy');
  const result = sizeKafkaCluster(fx.input);
  assert.equal(result.subscriptionPolicy, 'failoverExcluded');
  assert.equal(
    result.subscriptionCoresReported,
    result.subscriptionFailoverExcluded
  );
  assert.equal(
    result.subscriptionFailoverExcluded,
    (result.brokerNodes - 1) * result.vcpusPerBroker
  );
});

test('platform adapters: openshift vs rhel', () => {
  const fx = loadFixture('fixture-light');
  const ocp = sizeKafkaCluster({ ...fx.input, platform: 'openshift' });
  const rhel = sizeKafkaCluster({ ...fx.input, platform: 'rhel' });
  assert.equal(ocp.brokerNodes, rhel.brokerNodes);
  assert.ok(ocp.platformDetails.kafkaNodePools);
  assert.ok(rhel.platformDetails.topology);
});

test('RHAF estimates included', () => {
  const fx = loadFixture('fixture-light');
  const result = sizeKafkaCluster(fx.input);
  assert.ok(result.rhaf);
  assert.ok(result.rhaf.components.length >= 5);
});
