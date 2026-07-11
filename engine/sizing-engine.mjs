/**
 * Red Hat Streams for Apache Kafka sizing engine.
 * Ported from Andy Yuen's analytical model (SizingServiceImpl) and extended for
 * KRaft, OpenShift/RHEL, mixed retention, and RHAF add-on estimates.
 *
 * @see docs/methodology.md
 */

export const ENGINE_VERSION = '1.0.0';

export const DEFAULTS = {
  safetyFactor: 1.6,
  vcpusPerBroker: 8,
  vcpuIncrement: 2,
  memPerBroker: 32,
  vcpusPerController: 4,
  memPerController: 16,
  diskPerController: 100,
  maxUtil: 0.65,
  netSpeedGbps: 1.0,
  replicas: 3,
  diskThroughputMBps: 125,
  controllerFailuresTolerated: 1,
  jvmHeapGbMin: 5,
  jvmHeapGbMax: 8,
};

/**
 * @typedef {'openshift' | 'rhel'} Platform
 */

/**
 * @typedef {Object} SizingInput
 * @property {Platform} platform
 * @property {number} messageRate - messages per second
 * @property {number} messageSizeBytes
 * @property {number} replicas
 * @property {number} netSpeedGbps
 * @property {number} diskThroughputMBps
 * @property {number} maxUtil - 0.01..1.0
 * @property {number} consumerGroups
 * @property {number} laggingConsumers
 * @property {number} retentionDays
 * @property {number} [extendedRetentionDays]
 * @property {number} [extendedRetentionPercent] - 0..100
 * @property {number} [annualGrowthRatePercent] - e.g. 8 for 8%
 * @property {number} [projectionYears] - default 0 (current year)
 * @property {number} controllerFailuresTolerated - 1 or 2
 * @property {number} [topicThroughputMBps]
 * @property {number} [producerThroughputMBps]
 * @property {number} [consumerThroughputMBps]
 * @property {boolean} [includeRhaf]
 * @property {'corePairs' | 'failoverExcluded'} [subscriptionPolicy]
 */

function ceil(n) {
  return Math.ceil(n);
}

function effectiveMessageRate(input) {
  const years = input.projectionYears ?? 0;
  const growth = input.annualGrowthRatePercent ?? 0;
  if (years <= 0 || growth === 0) {
    return input.messageRate;
  }
  return input.messageRate * (1 + growth / 100) ** years;
}

function effectiveRetentionDays(input) {
  const std = input.retentionDays;
  const extDays = input.extendedRetentionDays ?? 0;
  const extPct = (input.extendedRetentionPercent ?? 0) / 100;
  if (extDays <= 0 || extPct <= 0) {
    return std;
  }
  return std * (1 - extPct) + extDays * extPct;
}

function controllerNodeCount(input, brokerNodes) {
  const tolerated = input.controllerFailuresTolerated ?? 1;
  const base = tolerated === 2 ? 5 : 3;
  return base > 3 || brokerNodes > 50 ? 5 : 3;
}

/**
 * Core analytical sizing (Andy Yuen model).
 * @param {SizingInput} input
 * @param {typeof DEFAULTS} defaults
 */
export function sizeKafkaCluster(input, defaults = DEFAULTS) {
  const trace = {};
  const rate = effectiveMessageRate(input);
  trace.messageRateEffective = rate;
  trace.projectionYears = input.projectionYears ?? 0;
  trace.annualGrowthRatePercent = input.annualGrowthRatePercent ?? 0;

  const writesMB = (rate * input.messageSizeBytes) / 1_000_000;
  trace.writesMBps = writesMB;

  const rf = input.replicas;
  const netWrite = rf * writesMB;
  const netRead = (input.consumerGroups + rf - 1) * writesMB;
  const diskIO = (rf + input.laggingConsumers) * writesMB;
  trace.netWriteMBps = netWrite;
  trace.netReadMBps = netRead;
  trace.diskReadWriteMBps = diskIO;

  const netCapacityMBps = (input.netSpeedGbps / 8) * 1000;
  const netUtilisation = Math.max(netWrite, netRead) / netCapacityMBps;
  trace.netUtilisation = netUtilisation;

  let vcpusPerBroker = defaults.vcpusPerBroker;
  if (input.netSpeedGbps > 3.0 && netUtilisation > 0.3) {
    vcpusPerBroker += defaults.vcpuIncrement;
    trace.vcpuBumpForHighSpeedNetwork = true;
  }
  trace.vcpusPerBroker = vcpusPerBroker;

  const diskUtilisation = diskIO / input.diskThroughputMBps;
  trace.diskUtilisation = diskUtilisation;

  const maxUtilisation = Math.max(diskUtilisation, netUtilisation);
  trace.maxUtilisation = maxUtilisation;

  const brokersNeeded = maxUtilisation / input.maxUtil;
  trace.brokersNeededRaw = brokersNeeded;

  const brokersByBottleneck = Math.max(
    brokersNeeded * defaults.safetyFactor,
    rf + 1
  );
  trace.brokersNeededByBottleneck = brokersByBottleneck;

  const brokerNodes = ceil(brokersByBottleneck);
  const controllerNodes = controllerNodeCount(input, brokerNodes);
  trace.controllerNodes = controllerNodes;

  const retentionEffectiveDays = effectiveRetentionDays(input);
  trace.retentionEffectiveDays = retentionEffectiveDays;
  trace.extendedRetentionPercent = input.extendedRetentionPercent ?? 0;

  const dailyDiskUsageGB = ceil((writesMB * 86400) / 1000 * rf);
  const totalDiskStorageGB = ceil(dailyDiskUsageGB * retentionEffectiveDays);
  trace.dailyDiskUsageGB = dailyDiskUsageGB;
  trace.totalDiskStorageGB = totalDiskStorageGB;

  const diskPerBrokerGB = ceil((totalDiskStorageGB / brokerNodes) * 1.1);
  trace.diskPerBrokerOverheadFactor = 1.1;

  let producersNeeded = 0;
  let consumersNeeded = 0;
  let partitions = 0;
  const topicTp = input.topicThroughputMBps ?? 0;
  const prodTp = input.producerThroughputMBps ?? 0;
  const consTp = input.consumerThroughputMBps ?? 0;
  if (topicTp > 0 && prodTp > 0 && consTp > 0) {
    producersNeeded = ceil(topicTp / prodTp);
    consumersNeeded = ceil(topicTp / consTp);
    partitions = ceil(Math.max(producersNeeded, consumersNeeded));
    trace.producersNeeded = producersNeeded;
    trace.consumersNeeded = consumersNeeded;
  }

  const bindingConstraint =
    diskUtilisation >= netUtilisation ? 'disk' : 'network';
  trace.bindingConstraint = bindingConstraint;

  const subscriptionCorePairs = Math.floor((brokerNodes * vcpusPerBroker) / 2);
  const subscriptionFailoverExcluded = (brokerNodes - 1) * vcpusPerBroker;
  const policy = input.subscriptionPolicy ?? 'corePairs';

  const clusterTotals = {
    nodes: brokerNodes + controllerNodes,
    brokerNodes,
    controllerNodes,
    vcpus: brokerNodes * vcpusPerBroker + controllerNodes * defaults.vcpusPerController,
    memoryGi: brokerNodes * defaults.memPerBroker + controllerNodes * defaults.memPerController,
    diskGB:
      brokerNodes * diskPerBrokerGB + controllerNodes * defaults.diskPerController,
    kafkaDataDiskGB: totalDiskStorageGB,
    subscriptionCoresReported:
      policy === 'failoverExcluded'
        ? subscriptionFailoverExcluded
        : subscriptionCorePairs,
  };

  const platformResult = buildPlatformResult(input.platform, {
    brokerNodes,
    controllerNodes,
    vcpusPerBroker,
    memPerBroker: defaults.memPerBroker,
    diskPerBrokerGB,
    vcpusPerController: defaults.vcpusPerController,
    memPerController: defaults.memPerController,
    diskPerController: defaults.diskPerController,
  });

  const rhaf = input.includeRhaf !== false
    ? estimateRhaf(input, { brokerNodes, writesMB, rf })
    : null;

  if (rhaf) {
    const comps = rhaf.components ?? [];
    rhaf.totals = {
      instances: comps.reduce((s, c) => s + (c.estimate.instances ?? 0), 0),
      vcpus: comps.reduce(
        (s, c) => s + (c.estimate.instances ?? 0) * (c.estimate.vcpuEach ?? 0),
        0
      ),
      memoryGi: comps.reduce(
        (s, c) => s + (c.estimate.instances ?? 0) * (c.estimate.memoryGiEach ?? 0),
        0
      ),
    };
    clusterTotals.withRhaf = {
      vcpus: clusterTotals.vcpus + rhaf.totals.vcpus,
      memoryGi: clusterTotals.memoryGi + rhaf.totals.memoryGi,
      nodes: clusterTotals.nodes + rhaf.totals.instances,
    };
  }

  return {
    engineVersion: ENGINE_VERSION,
    platform: input.platform,
    ingressMBps: round(writesMB, 2),
    dailyDiskUsageGB,
    totalDiskStorageGB,
    brokerNodes,
    controllerNodes,
    diskPerBrokerGB,
    memPerBrokerGB: defaults.memPerBroker,
    vcpusPerBroker,
    vcpusPerController: defaults.vcpusPerController,
    memPerControllerGB: defaults.memPerController,
    diskPerControllerGB: defaults.diskPerController,
    subscriptionCorePairs,
    subscriptionFailoverExcluded,
    subscriptionCoresReported: clusterTotals.subscriptionCoresReported,
    subscriptionPolicy: policy,
    partitions,
    topicThroughputMBps: topicTp,
    bindingConstraint,
    retentionEffectiveDays,
    jvmHeapRecommendationGb: `${defaults.jvmHeapGbMin}-${defaults.jvmHeapGbMax}`,
    clusterTotals,
    platformDetails: platformResult,
    rhaf,
    trace,
  };
}

function round(n, d = 2) {
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

function buildPlatformResult(platform, spec) {
  if (platform === 'openshift') {
    return {
      deploymentTarget: 'OpenShift',
      kafkaNodePools: [
        {
          role: 'broker',
          nodes: spec.brokerNodes,
          resources: {
            cpuRequest: `${spec.vcpusPerBroker * 1000}m`,
            memoryRequestGi: spec.memPerBroker,
            memoryLimitGi: spec.memPerBroker,
            pvcSizeGi: spec.diskPerBrokerGB,
          },
          scheduling: {
            dedicatedWorkers: true,
            taintsTolerations: true,
            nodeAffinity: true,
          },
        },
        {
          role: 'controller',
          nodes: spec.controllerNodes,
          resources: {
            cpuRequest: `${spec.vcpusPerController * 1000}m`,
            memoryRequestGi: spec.memPerController,
            memoryLimitGi: spec.memPerController,
            pvcSizeGi: spec.diskPerController,
          },
        },
      ],
      storage: {
        type: 'block',
        accessMode: 'ReadWriteOnce',
        filesystem: 'xfs',
        recommendations: ['ODF (Ceph RBD) for production', 'LSO (local NVMe) for dev/low-latency'],
      },
      operators: [
        'Cluster Operator',
        'Entity Operator (Topic + User)',
        'Cruise Control (recommended production)',
      ],
      notes: [
        'Separate KafkaNodePool for broker and controller roles (KRaft).',
        'Set terminationGracePeriodSeconds >= 60 for brokers.',
      ],
    };
  }

  return {
    deploymentTarget: 'RHEL',
    topology: {
      brokerHosts: spec.brokerNodes,
      controllerHosts: spec.controllerNodes,
    },
    resourcesPerBroker: {
      vcpus: spec.vcpusPerBroker,
      memoryGi: spec.memPerBroker,
      diskGi: spec.diskPerBrokerGB,
    },
    resourcesPerController: {
      vcpus: spec.vcpusPerController,
      memoryGi: spec.memPerController,
      diskGi: spec.diskPerController,
    },
    storage: {
      type: 'block',
      filesystem: 'xfs or ext4',
      notes: ['Local block storage; avoid NFS for Kafka data directories.'],
    },
    notes: [
      'KRaft mode with dedicated controller quorum.',
      'See Streams for Apache Kafka 3.2 RHEL documentation for install paths.',
    ],
  };
}

/**
 * RHAF complementary component estimates (non-Kafka core).
 */
function estimateRhaf(input, { brokerNodes, writesMB, rf }) {
  const drEnabled = (input.projectionYears ?? 0) >= 0;
  return {
    disclaimer:
      'Orientative sizing for RHAF components. Validate against product documentation and workload.',
    components: [
      {
        name: 'Apicurio Registry',
        role: 'Schema registry',
        estimate: { instances: 2, vcpuEach: 1, memoryGiEach: 2 },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_build_of_apicurio_registry/',
      },
      {
        name: 'Streams for Apache Kafka HTTP Bridge',
        role: 'REST clients',
        estimate: { instances: 2, vcpuEach: 1, memoryGiEach: 1 },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_http_bridge/',
      },
      {
        name: 'MirrorMaker 2',
        role: 'DR / cluster replication',
        estimate: {
          instances: drEnabled ? 2 : 0,
          vcpuEach: 2,
          memoryGiEach: 4,
          note: 'Size connectors per replicated topic throughput.',
        },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/disaster_recovery_using_mirrormaker_2/',
      },
      {
        name: 'Cruise Control',
        role: 'Rebalancing',
        estimate: { instances: 1, vcpuEach: 1, memoryGiEach: 2 },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift/',
      },
      {
        name: 'Streams Console',
        role: 'Operations UI',
        estimate: { instances: 1, vcpuEach: 1, memoryGiEach: 2 },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_console/',
      },
      {
        name: 'Red Hat build of Keycloak',
        role: 'OAuth/OIDC for clients and Console',
        estimate: { instances: 2, vcpuEach: 2, memoryGiEach: 4 },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/',
      },
    ],
    kafkaExporter: {
      enabled: true,
      vcpu: 0.5,
      memoryGi: 0.5,
      note: 'Deployed with Kafka CR; monitor consumer lag.',
    },
    referenceThroughputMBps: round(writesMB * rf, 2),
    referenceBrokerCount: brokerNodes,
  };
}

export function exportScenario(name, input, result) {
  return {
    schemaVersion: 1,
    exportedAt: new Date().toISOString(),
    name,
    input,
    result,
  };
}

export function importScenario(json) {
  if (!json || typeof json !== 'object') {
    throw new Error('Invalid scenario JSON');
  }
  if (!json.input || !json.input.platform) {
    throw new Error('Scenario missing input.platform');
  }
  return json;
}
