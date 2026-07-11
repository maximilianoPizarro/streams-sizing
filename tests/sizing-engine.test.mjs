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

test('fixture-example-aggregate: production ~500 MB/s golden', () => {
  const fx = loadFixture('fixture-example-aggregate');
  const result = sizeKafkaCluster(fx.input);
  assert.equal(result.ingressMBps, fx.expected.ingressMBps);
  assert.equal(result.dailyDiskUsageGB, fx.expected.dailyDiskUsageGB);
  assert.equal(result.totalDiskStorageGB, fx.expected.totalDiskStorageGB);
  assert.equal(result.brokerNodes, fx.expected.brokerNodes);
  assert.equal(result.controllerNodes, fx.expected.controllerNodes);
  assert.equal(result.bindingConstraint, fx.expected.bindingConstraint);
  assert.equal(result.diskPerBrokerGB, fx.expected.diskPerBrokerGB);
  assert.equal(
    result.subscriptionCoresReported,
    fx.expected.subscriptionCoresReported
  );
});

test('fixture-example-aggregate: export/import is reproducible', () => {
  const fx = loadFixture('fixture-example-aggregate');
  const first = sizeKafkaCluster(fx.input);
  const scenario = {
    schemaVersion: 1,
    name: fx.id,
    input: fx.input,
    result: first,
  };
  const second = sizeKafkaCluster(scenario.input);
  assert.deepEqual(second.trace, first.trace);
  assert.equal(second.brokerNodes, first.brokerNodes);
  assert.equal(second.totalDiskStorageGB, first.totalDiskStorageGB);
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

test('clusterTotals aggregates brokers and controllers', () => {
  const fx = loadFixture('fixture-example-aggregate');
  const result = sizeKafkaCluster(fx.input);
  const t = result.clusterTotals;
  assert.equal(t.brokerNodes, result.brokerNodes);
  assert.equal(t.controllerNodes, result.controllerNodes);
  assert.equal(t.nodes, result.brokerNodes + result.controllerNodes);
  assert.equal(
    t.vcpus,
    result.brokerNodes * result.vcpusPerBroker +
      result.controllerNodes * result.vcpusPerController
  );
  assert.equal(
    t.memoryGi,
    result.brokerNodes * result.memPerBrokerGB +
      result.controllerNodes * result.memPerControllerGB
  );
  assert.equal(
    t.diskGB,
    result.brokerNodes * result.diskPerBrokerGB +
      result.controllerNodes * result.diskPerControllerGB
  );
  assert.equal(t.kafkaDataDiskGB, result.totalDiskStorageGB);
  assert.ok(result.rhaf.totals.instances > 0);
  assert.equal(
    t.withRhaf.vcpus,
    t.vcpus + result.rhaf.totals.vcpus
  );
  assert.equal(result.integrations, null);
});

test('Camel integrations sized when clientAccessPattern=camel', () => {
  const fx = loadFixture('fixture-light');
  const result = sizeKafkaCluster({
    ...fx.input,
    clientAccessPattern: 'camel',
    camelIntegrations: 3,
    quarkusRuntimes: 0,
  });
  assert.ok(result.integrations);
  assert.equal(result.integrations.pattern, 'camel');
  const camel = result.integrations.components.find((c) =>
    c.name.includes('Camel')
  );
  assert.ok(camel);
  assert.equal(camel.estimate.instances, 3);
  assert.equal(result.integrations.totals.instances, 3);
  assert.ok(result.clusterTotals.withIntegrations.vcpus > result.clusterTotals.withRhaf.vcpus);
});

test('External Quarkus clients sized outside OpenShift', () => {
  const fx = loadFixture('fixture-light');
  const result = sizeKafkaCluster({
    ...fx.input,
    clientAccessPattern: 'external',
    quarkusRuntimes: 4,
  });
  assert.ok(result.integrations);
  assert.equal(result.integrations.pattern, 'external');
  const q = result.integrations.components.find((c) =>
    c.name.includes('outside OpenShift')
  );
  assert.ok(q);
  assert.equal(q.estimate.instances, 4);
  assert.ok(result.integrations.notes.some((n) => n.includes('external listeners')));
});

test('camelAndExternal sizes both Camel and Quarkus', () => {
  const fx = loadFixture('fixture-example-aggregate');
  const result = sizeKafkaCluster({
    ...fx.input,
    clientAccessPattern: 'camelAndExternal',
    camelIntegrations: 2,
    quarkusRuntimes: 2,
  });
  assert.equal(result.integrations.components.length, 2);
  assert.equal(result.integrations.totals.instances, 4);
  assert.ok(result.clusterTotals.withIntegrations);
});
