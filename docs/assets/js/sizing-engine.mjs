/**
 * Red Hat Streams for Apache Kafka sizing engine.
 * Ported from Andy Yuen's analytical model (SizingServiceImpl) and extended for
 * KRaft, OpenShift/RHEL, mixed retention, and RHAF add-on estimates.
 *
 * @see docs/methodology.md
 */

export const ENGINE_VERSION = '1.1.1';

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
  diskCapacityHeadroom: 1.25,
  diskSegmentOverhead: 0.05,
  cacheFraction: 0.5,
  maxPartitionsPerBroker: 4000,
  kraftMetadataMBpsPerController: 2,
  cpuPerMBps: 0.002,
  cpuBaseCores: 2,
};

const COMPRESSION_CPU_FACTORS = {
  none: 1.0,
  lz4: 1.15,
  snappy: 1.2,
  zstd: 1.35,
  gzip: 1.5,
};

/**
 * @typedef {'openshift' | 'rhel'} Platform
 * @typedef {'full' | 'half'} DuplexMode
 * @typedef {'none' | 'lz4' | 'snappy' | 'zstd' | 'gzip'} CompressionType
 */

function ceil(n) {
  return Math.ceil(n);
}

function round(n, d = 2) {
  const f = 10 ** d;
  return Math.round(n * f) / f;
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
  return round(std * (1 - extPct) + extDays * extPct, 2);
}

function controllerNodeCount(input, brokerNodes) {
  const tolerated = input.controllerFailuresTolerated ?? 1;
  const base = tolerated === 2 ? 5 : 3;
  return base > 3 || brokerNodes > 50 ? 5 : 3;
}

function isEnabledFlag(value, defaultEnabled = true) {
  if (value === undefined || value === null) return defaultEnabled;
  if (value === false || value === 0 || value === '0' || value === 'false') return false;
  return true;
}

function netPressureMBps(netWrite, netRead, duplexMode) {
  return duplexMode === 'half' ? netWrite + netRead : Math.max(netWrite, netRead);
}

function compressionFactor(type) {
  return COMPRESSION_CPU_FACTORS[type ?? 'none'] ?? 1.0;
}

function effectiveDiskIO(input, writesMB, rf, defaults) {
  let diskIO = (rf + input.laggingConsumers) * writesMB;
  let diskIOBoostFromRam = 0;
  const ramGb = input.ramPerBrokerGB ?? defaults.memPerBroker;
  const cacheFraction = input.cacheFraction ?? defaults.cacheFraction;
  const cacheableRetentionGB = ramGb * cacheFraction;
  const lagWindowSec = input.lagReadWindowSec ?? 300;
  const lagVolumeGB =
    input.laggingConsumers > 0
      ? (input.laggingConsumers * writesMB * lagWindowSec) / 1000
      : 0;
  const applyRamModel = input.ramPerBrokerGB != null;
  if (
    applyRamModel &&
    input.laggingConsumers > 0 &&
    lagVolumeGB > cacheableRetentionGB &&
    cacheableRetentionGB > 0
  ) {
    const boostFactor = Math.min(lagVolumeGB / cacheableRetentionGB, 2.0);
    if (boostFactor > 1) {
      diskIOBoostFromRam = diskIO * (boostFactor - 1);
      diskIO += diskIOBoostFromRam;
    }
  }
  return { diskIO, diskIOBoostFromRam, cacheableRetentionGB, ramGb, lagVolumeGB };
}

function estimateComputeCpu(input, netWrite, netRead, defaults) {
  const compressionType = input.compressionType ?? 'none';
  const tlsEnabled = input.tlsEnabled ?? false;
  const tlsFactor = tlsEnabled ? 1.2 : 1.0;
  const compFactor = compressionFactor(compressionType);
  const netTotal = netWrite + netRead;
  const cores =
    defaults.cpuBaseCores + netTotal * defaults.cpuPerMBps * compFactor * tlsFactor;
  return {
    cpuCoresPerBroker: round(cores, 2),
    experimental: true,
    compressionType,
    tlsEnabled,
    compressionFactor: compFactor,
    tlsFactor,
  };
}

function partitionDensityCheck(input, partitions, brokerNodes, rf, defaults) {
  const totalPartitions = input.totalPartitions ?? partitions;
  if (!totalPartitions || totalPartitions <= 0 || brokerNodes <= 0) {
    return { density: 0, warnings: [], brokersForPartitions: brokerNodes };
  }
  const density = (totalPartitions * rf) / brokerNodes;
  const maxPerBroker = input.maxPartitionsPerBroker ?? defaults.maxPartitionsPerBroker;
  const warnings = [];
  let brokersForPartitions = brokerNodes;
  if (density > maxPerBroker) {
    warnings.push(
      `Partition density ${Math.round(density)} per broker exceeds recommended ${maxPerBroker} (totalPartitions×RF/brokers).`
    );
    if (input.enforcePartitionLimit) {
      brokersForPartitions = ceil((totalPartitions * rf) / maxPerBroker);
    }
  }
  return { density: round(density, 1), warnings, brokersForPartitions, totalPartitions };
}

/**
 * Core analytical sizing (Andy Yuen model).
 * @param {import('./sizing-engine.mjs').SizingInput} input
 * @param {typeof DEFAULTS} defaults
 */
export function sizeKafkaCluster(input, defaults = DEFAULTS) {
  const trace = {};
  const warnings = [];
  const rate = effectiveMessageRate(input);
  trace.messageRateEffective = rate;
  trace.projectionYears = input.projectionYears ?? 0;
  trace.annualGrowthRatePercent = input.annualGrowthRatePercent ?? 0;

  const writesMB = (rate * input.messageSizeBytes) / 1_000_000;
  trace.writesMBps = round(writesMB, 4);

  const rf = input.replicas;
  const netWrite = rf * writesMB;
  const netRead = (input.consumerGroups + rf - 1) * writesMB;
  trace.netWriteMBps = round(netWrite, 4);
  trace.netReadMBps = round(netRead, 4);

  const duplexMode = input.duplexMode ?? 'full';
  trace.duplexMode = duplexMode;

  const {
    diskIO,
    diskIOBoostFromRam,
    cacheableRetentionGB,
    ramGb,
  } = effectiveDiskIO(input, writesMB, rf, defaults);
  trace.diskReadWriteMBps = round(diskIO, 4);
  trace.diskIOBoostFromRam = round(diskIOBoostFromRam, 4);
  trace.ramPerBrokerGB = ramGb;
  trace.cacheableRetentionGB = round(cacheableRetentionGB, 2);

  const netCapacityMBps = (input.netSpeedGbps / 8) * 1000;
  let netPressure = netPressureMBps(netWrite, netRead, duplexMode);
  trace.netPressureMBps = round(netPressure, 4);

  let controllerNodesEstimate = controllerNodeCount(input, rf + 1);
  const kraftMetadataMBps =
    controllerNodesEstimate * (input.kraftMetadataMBpsPerController ?? defaults.kraftMetadataMBpsPerController);
  netPressure += kraftMetadataMBps;
  trace.kraftMetadataMBps = kraftMetadataMBps;

  const netUtilisation = netPressure / netCapacityMBps;
  trace.netUtilisation = round(netUtilisation, 6);

  let vcpusPerBroker = defaults.vcpusPerBroker;
  if (input.netSpeedGbps > 3.0 && netUtilisation > 0.3) {
    vcpusPerBroker += defaults.vcpuIncrement;
    trace.vcpuBumpForHighSpeedNetwork = true;
  }
  trace.vcpusPerBroker = vcpusPerBroker;

  const diskUtilisation = diskIO / input.diskThroughputMBps;
  trace.diskUtilisation = round(diskUtilisation, 6);

  const maxUtilisation = Math.max(diskUtilisation, netUtilisation);
  trace.maxUtilisation = round(maxUtilisation, 6);

  const maxUtil = input.maxUtil;
  const safetyFactor = input.safetyFactor ?? defaults.safetyFactor;
  trace.maxUtil = maxUtil;
  trace.safetyFactor = safetyFactor;
  trace.amplificationFactor = round((1 / maxUtil) * safetyFactor, 4);

  const brokersNeeded = maxUtilisation / maxUtil;
  trace.brokersNeededRaw = round(brokersNeeded, 6);

  let brokersByBottleneck = Math.max(brokersNeeded * safetyFactor, rf + 1);
  trace.brokersNeededByBottleneck = round(brokersByBottleneck, 4);

  let brokerNodes = ceil(brokersByBottleneck);
  // controllerNodes finalized after optional partition-driven broker bump

  const retentionEffectiveDays = effectiveRetentionDays(input);
  trace.retentionEffectiveDays = retentionEffectiveDays;
  trace.extendedRetentionPercent = input.extendedRetentionPercent ?? 0;

  const dailyDiskUsageGB = ceil((writesMB * 86400) / 1000 * rf);
  const totalDiskStorageGB = ceil(dailyDiskUsageGB * retentionEffectiveDays);
  trace.dailyDiskUsageGB = dailyDiskUsageGB;
  trace.totalDiskStorageGB = totalDiskStorageGB;

  const capacityHeadroom = input.diskCapacityHeadroom ?? defaults.diskCapacityHeadroom;
  const segmentOverhead = input.diskSegmentOverhead ?? defaults.diskSegmentOverhead;
  trace.capacityHeadroom = capacityHeadroom;
  trace.segmentOverhead = segmentOverhead;

  const diskPerBrokerGB = ceil(
    (totalDiskStorageGB / brokerNodes) * capacityHeadroom * (1 + segmentOverhead)
  );

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

  const partCheck = partitionDensityCheck(input, partitions, brokerNodes, rf, defaults);
  trace.partitionsPerBroker = partCheck.density;
  if (partCheck.warnings.length) {
    warnings.push(...partCheck.warnings);
  }
  if (partCheck.brokersForPartitions > brokerNodes) {
    brokerNodes = partCheck.brokersForPartitions;
    trace.brokersAdjustedForPartitions = brokerNodes;
  }

  const controllerNodes = controllerNodeCount(input, brokerNodes);
  trace.controllerNodes = controllerNodes;

  const bindingConstraint =
    diskUtilisation >= netUtilisation ? 'disk' : 'network';
  trace.bindingConstraint = bindingConstraint;

  const computeCpu = estimateComputeCpu(input, netWrite, netRead, defaults);

  const subscriptionCorePairs = Math.floor((brokerNodes * vcpusPerBroker) / 2);
  const subscriptionFailoverExcluded = (brokerNodes - 1) * vcpusPerBroker;
  const policy = input.subscriptionPolicy ?? 'corePairs';

  const finalDiskPerBrokerGB = ceil(
    (totalDiskStorageGB / brokerNodes) * capacityHeadroom * (1 + segmentOverhead)
  );

  const clusterTotals = {
    nodes: brokerNodes + controllerNodes,
    brokerNodes,
    controllerNodes,
    vcpus: brokerNodes * vcpusPerBroker + controllerNodes * defaults.vcpusPerController,
    memoryGi: brokerNodes * defaults.memPerBroker + controllerNodes * defaults.memPerController,
    diskGB:
      brokerNodes * finalDiskPerBrokerGB + controllerNodes * defaults.diskPerController,
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
    diskPerBrokerGB: finalDiskPerBrokerGB,
    vcpusPerController: defaults.vcpusPerController,
    memPerController: defaults.memPerController,
    diskPerController: defaults.diskPerController,
  });

  const rhaf = isEnabledFlag(input.includeRhaf, true)
    ? estimateRhaf(input, { brokerNodes, writesMB, rf })
    : null;

  if (rhaf) {
    const comps = rhaf.components ?? [];
    rhaf.totals = sumComponentTotals(comps);
    clusterTotals.withRhaf = {
      vcpus: clusterTotals.vcpus + rhaf.totals.vcpus,
      memoryGi: clusterTotals.memoryGi + rhaf.totals.memoryGi,
      nodes: clusterTotals.nodes + rhaf.totals.instances,
    };
  }

  const integrations = estimateIntegrations(input, { writesMB });
  if (integrations) {
    integrations.totals = sumComponentTotals(integrations.components);
    const baseVcpus = clusterTotals.withRhaf?.vcpus ?? clusterTotals.vcpus;
    const baseMem = clusterTotals.withRhaf?.memoryGi ?? clusterTotals.memoryGi;
    const baseNodes = clusterTotals.withRhaf?.nodes ?? clusterTotals.nodes;
    clusterTotals.withIntegrations = {
      vcpus: baseVcpus + integrations.totals.vcpus,
      memoryGi: baseMem + integrations.totals.memoryGi,
      nodes: baseNodes + integrations.totals.instances,
    };
  }

  const economizeSuggestions = buildEconomizeSuggestions(input, {
    brokerNodes,
    controllerNodes,
    vcpusPerBroker,
    vcpusPerController: defaults.vcpusPerController,
    totalDiskStorageGB,
    retentionEffectiveDays,
    bindingConstraint,
    subscriptionCorePairs,
    subscriptionFailoverExcluded,
    policy,
    rhaf,
    clusterTotals,
  });

  return {
    engineVersion: ENGINE_VERSION,
    platform: input.platform,
    ingressMBps: round(writesMB, 2),
    dailyDiskUsageGB,
    totalDiskStorageGB,
    brokerNodes,
    controllerNodes,
    diskPerBrokerGB: finalDiskPerBrokerGB,
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
    clientAccessPattern: input.clientAccessPattern ?? 'inCluster',
    amplificationFactor: trace.amplificationFactor,
    computeCpuEstimate: computeCpu,
    warnings,
    economizeSuggestions,
    clusterTotals,
    platformDetails: platformResult,
    rhaf,
    integrations,
    trace,
  };
}

/**
 * Contextual cost/licensing economization hints grounded in Streams for Apache Kafka
 * OpenShift guidance (dedicated broker/controller NodePools; operators/controllers
 * are not the primary subscription driver — broker vCPU is).
 */
function buildEconomizeSuggestions(input, ctx) {
  const tips = [];

  tips.push({
    id: 'openshift-semantics',
    title: 'How to read Total cluster on OpenShift',
    detail:
      'Nodes / vCPU / memory are Kafka broker + controller pod requests (KafkaNodePool replicas), not OpenShift worker or infra node SKUs. Pack those pods onto dedicated worker nodes with affinity/taints; do not schedule Kafka on control-plane/infra nodes.',
    source: 'Streams for Apache Kafka 3.2 — Deploying on OpenShift (KafkaNodePool roles broker|controller)',
  });

  tips.push({
    id: 'subscription-scope',
    title: 'Subscription cores track broker capacity',
    detail: `Reported subscription figure is ${ctx.clusterTotals.subscriptionCoresReported} (${ctx.policy}). Controllers (${ctx.controllerNodes} × ${ctx.vcpusPerController} vCPU) and Cluster/Entity Operator cores are outside the broker licensing line used here. Align reporting with your Red Hat entitlement (core pairs vs failover-excluded).`,
    source: 'Streams release notes — subscription limits / operator cores; product methodology',
  });

  const extPct = Number(input.extendedRetentionPercent ?? 0);
  const extDays = Number(input.extendedRetentionDays ?? 0);
  if (extDays > 0 && extPct > 0) {
    tips.push({
      id: 'mixed-retention',
      title: 'Trim mixed retention to cut disk',
      detail: `Effective retention is ${ctx.retentionEffectiveDays} days (${input.retentionDays}d + ${extPct}% at ${extDays}d). Lowering extendedRetentionPercent or extendedRetentionDays cuts totalDiskStorageGB (~${ctx.totalDiskStorageGB} GB) without changing broker count unless disk was the bottleneck.`,
      lever: 'Durability → Volume on extended retention (%) / Extended retention (days)',
    });
  } else if (Number(input.retentionDays ?? 0) > 3) {
    tips.push({
      id: 'retention',
      title: 'Shorten standard retention when compliance allows',
      detail: `Retention ${input.retentionDays}d drives ~${ctx.totalDiskStorageGB} GB cluster data (RF included). Reducing days is the largest storage economizer; broker count stays ${ctx.brokerNodes} unless disk I/O was binding.`,
      lever: 'Durability → Standard retention (days)',
    });
  }

  if (isEnabledFlag(input.includeRhaf, true)) {
    tips.push({
      id: 'rhaf-optional',
      title: 'Disable RHAF add-ons you will not deploy',
      detail:
        'Apicurio, Bridge, Console, Keycloak, etc. add OpenShift pods and storage. They are not Streams broker cores, but they do consume cluster capacity. Set includeRhaf false when sizing Kafka-only.',
      lever: 'Durability → Include RHAF complementary components',
    });
  }

  if (input.includeDr === true) {
    tips.push({
      id: 'dr-mm2',
      title: 'MirrorMaker 2 only when DR is real',
      detail:
        'includeDr sizes MirrorMaker 2 workers for cross-cluster replication. Leave it off unless DR is in scope; MM2 adds Connect workers and network load, not a full second Kafka cluster by itself.',
      lever: 'Durability → Include DR (MirrorMaker 2)',
    });
  }

  if (Number(input.consumerGroups ?? 0) > 6 && ctx.bindingConstraint === 'network') {
    tips.push({
      id: 'consumer-fanout',
      title: 'Revisit overstated consumer groups',
      detail: `Binding constraint is network with ${input.consumerGroups} groups. Over-counting independent groups inflates netRead and can force extra brokers (now ${ctx.brokerNodes}). Use peak concurrent independent groups, not topic count.`,
      lever: 'Consumers → Consumer groups',
    });
  }

  if (ctx.policy === 'failoverExcluded' && ctx.subscriptionFailoverExcluded > ctx.subscriptionCorePairs) {
    tips.push({
      id: 'subscription-policy',
      title: 'Compare subscription reporting policies',
      detail: `failoverExcluded=${ctx.subscriptionFailoverExcluded} vs corePairs=${ctx.subscriptionCorePairs}. Choose the policy that matches your contract; this does not change physical broker sizing.`,
      lever: 'Durability → Subscription core policy',
    });
  }

  tips.push({
    id: 'worker-packing',
    title: 'Right-size workers; never move Kafka to infra',
    detail:
      'Economize OpenShift by packing broker/controller pods onto fewer large workers (or dedicated machine sets), not by placing them on infra/control-plane. Production guidance: separate broker and controller NodePools; dual-role only for non-prod.',
    source: 'Streams for Apache Kafka 3.2 Overview — Node pools / role separation',
  });

  tips.push({
    id: 'storage-class',
    title: 'Match storage class to cost/latency',
    detail:
      'Per-broker PVC size already includes capacity headroom. Prefer LSO/local NVMe when latency and cost matter; use ODF/Ceph RBD when you need shared block HA. Over-provisioned storage class is a common silent cost.',
    source: 'Streams deploying guide — persistent storage / JBOD',
  });

  return tips;
}

function sumComponentTotals(comps) {
  return {
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

function estimateRhaf(input, { brokerNodes, writesMB, rf }) {
  const drEnabled = input.includeDr === true;
  const components = [
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
        name: 'Cruise Control',
        role: 'Rebalancing (orientative add-on footprint; also listed as recommended operator)',
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
  ];
  if (drEnabled) {
    components.splice(2, 0, {
      name: 'MirrorMaker 2',
      role: 'DR / cluster replication',
      estimate: {
        instances: 2,
        vcpuEach: 2,
        memoryGiEach: 4,
        note: 'Size connectors per replicated topic throughput.',
      },
      docs: 'https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/disaster_recovery_using_mirrormaker_2/',
    });
  }
  return {
    disclaimer:
      'Orientative sizing for RHAF complementary components (not Streams broker subscription cores).',
    components,
    kafkaExporter: {
      enabled: true,
      vcpu: 0.5,
      memoryGi: 0.5,
      note: 'Deployed with Kafka CR; monitor consumer lag. Not included in RHAF subtotal above.',
    },
    referenceThroughputMBps: round(writesMB * rf, 2),
    referenceBrokerCount: brokerNodes,
  };
}

function estimateIntegrations(input, { writesMB }) {
  const pattern = input.clientAccessPattern ?? 'inCluster';
  if (pattern === 'inCluster') {
    return null;
  }

  const wantsCamel = pattern === 'camel' || pattern === 'camelAndExternal';
  const wantsExternal = pattern === 'external' || pattern === 'camelAndExternal';
  const throughputBump = writesMB > 50 ? 1 : 0;

  const components = [];
  const notes = [];

  if (wantsCamel) {
    const requested = input.camelIntegrations ?? 0;
    const instances = Math.max(2, ceil(requested > 0 ? requested : 2));
    components.push({
      name: 'Red Hat build of Apache Camel (Quarkus)',
      role: 'Integration routes / connectors to Kafka',
      estimate: {
        instances,
        vcpuEach: 2 + throughputBump,
        memoryGiEach: 2 + throughputBump * 2,
        note: 'HA baseline ≥2 pods. Scale with route count and transformation complexity.',
      },
      docs: 'https://docs.redhat.com/en/documentation/red_hat_build_of_apache_camel/',
    });
    notes.push(
      'Camel sizing is for integration runtimes (often Camel for Quarkus), not Kafka brokers.'
    );
  }

  if (wantsExternal || wantsCamel) {
    const requested = input.quarkusRuntimes ?? 0;
    const defaultCount = wantsExternal ? 2 : 0;
    const useCount = requested > 0 ? requested : defaultCount;
    if (useCount > 0 || wantsExternal) {
      const instances = Math.max(2, ceil(useCount > 0 ? useCount : 2));
      components.push({
        name: wantsExternal
          ? 'Quarkus Kafka clients (outside OpenShift)'
          : 'Quarkus Kafka runtimes (additional)',
        role: wantsExternal
          ? 'Produce/consume Kafka from outside the OpenShift cluster'
          : 'Additional Quarkus apps talking to Kafka (non-Camel)',
        estimate: {
          instances,
          vcpuEach: 1 + throughputBump,
          memoryGiEach: 1 + throughputBump,
          note: wantsExternal
            ? 'Runs outside OpenShift; ensure external Kafka listeners, TLS, and network capacity.'
            : 'Optional extra Quarkus services beyond Camel integrations.',
        },
        docs: 'https://docs.redhat.com/en/documentation/red_hat_build_of_quarkus/',
      });
    }
  }

  if (wantsExternal) {
    notes.push(
      'External access: configure Kafka external listeners (LoadBalancer/NodePort/Routes), certificates, and firewall rules.'
    );
    notes.push(
      'External consumer groups still count in the Kafka consumerGroups input for broker network sizing.'
    );
  }

  if (components.length === 0) {
    return null;
  }

  return {
    pattern,
    disclaimer:
      'Orientative sizing for Camel integrations and Quarkus runtimes. Not Streams subscription cores.',
    components,
    notes,
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
