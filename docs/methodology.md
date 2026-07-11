---
layout: doc
title: Methodology
---

# Sizing methodology

**streams-sizing** implements a constraint-based analytical model for [Red Hat Streams for Apache Kafka 3.2](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2), derived from [Andy Yuen's kafka-sizing](https://github.com/AndyYuen/kafka-sizing) and extended for KRaft, OpenShift/RHEL, mixed retention, and RHAF add-ons.

## Core formulas

Given message rate `R` (msg/s), average size `S` (bytes), replication factor `RF`:

```
writesMBps = R × S / 1,000,000
netWrite   = RF × writesMBps
netRead    = (consumerGroups + RF − 1) × writesMBps
diskIO     = (RF + laggingConsumers) × writesMBps   (+ optional RAM/cache boost)

netCapacityMBps = (netSpeedGbps / 8) × 1000
netPressure     = max(netWrite, netRead)   [full duplex]
                = netWrite + netRead       [half duplex]
netPressure    += kraftMetadataMBps        (controllers × 2 MB/s default)

netUtilisation  = netPressure / netCapacityMBps
diskUtilisation = diskIO / diskThroughputMBps

brokersNeeded = max(netUtilisation, diskUtilisation) / maxUtil
brokerNodes   = ceil(max(brokersNeeded × safetyFactor, RF + 1))
```

### Safety margins (distinct roles)

| Parameter | Role |
|-----------|------|
| `maxUtil` | Sustained utilisation ceiling on the binding constraint (default 0.65) |
| `safetyFactor` | Headroom for peaks, protocol overhead, broker imbalance (default 1.6) |

Combined amplification (trace JSON):

```
amplificationFactor = (1 / maxUtil) × safetyFactor
```

Example: `maxUtil=0.7`, `safetyFactor=1.6` → `amplificationFactor ≈ 2.29×` peak utilisation before the RF+1 floor.

These margins are **not** double-counting the same risk: `maxUtil` sizes for steady-state headroom; `safetyFactor` adds burst/ops margin on top.

### Storage

```
dailyDiskGB = ceil(writesMBps × 86,400 / 1,000 × RF)
effectiveRetentionDays = stdDays × (1 − X) + extDays × X   # mixed retention
totalDiskGB = ceil(dailyDiskGB × effectiveRetentionDays)
diskPerBrokerGB = ceil(totalDiskGB / brokerNodes × capacityHeadroom × (1 + segmentOverhead))
```

Defaults: `capacityHeadroom = 1.25` (~80% max disk use), `segmentOverhead = 0.05` (index/segment rotation, auditable separately).

**Mixed retention:** `X%` is the share of **write volume (MB/s)**, not topic count, kept on extended retention.

### Growth projection

```
R_eff = R × (1 + growth%/100)^years
```

### Partitions (optional)

When topic/producer/consumer throughputs are all &gt; 0:

```
partitions = ceil(max(topicTP / producerTP, topicTP / consumerTP))
partitionsPerBroker = totalPartitions × RF / brokerNodes
```

If `partitionsPerBroker` exceeds ~4000 (configurable), a warning is emitted; optional `enforcePartitionLimit` increases broker count.

### RAM / page cache (optional)

When `ramPerBrokerGB` is set explicitly, lag read volume (5 min window per lagging consumer) is compared to `ram × cacheFraction`. If lag exceeds cache, `diskIO` is boosted (capped 2×).

### Compute CPU (experimental)

Separate from subscription cores (licensing):

```
cpuCoresPerBroker ≈ baseCores + (netWrite + netRead) × cpuPerMBps × compressionFactor × tlsFactor
```

Marked experimental until validated against benchmarks.

### Controllers (KRaft)

- 1 failure tolerated → 3 controllers
- 2 failures tolerated → 5 controllers
- 5 controllers also when broker count &gt; 50

ZooKeeper is **not** used in default outputs (legacy mode only in Spring Boot branch).

## Subscription cores

Two policies (selectable):

| Policy | Formula |
|--------|---------|
| Core pairs | `(brokerNodes × vCPU) ÷ 2` |
| Failover excluded | `(brokerNodes − 1) × vCPU` |

Controller nodes are not counted toward Streams subscription cores.

## How to read OpenShift totals

**Total cluster nodes / vCPU / memory** = sum of Kafka **broker + controller pod** requests from `KafkaNodePool` replicas — not the count or size of OpenShift **worker** or **infra** nodes.

Official Streams for Apache Kafka 3.2 guidance:

- Production: separate node pools for `broker` and `controller` roles ([Overview — Node pools](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/streams_for_apache_kafka_on_openshift_overview/kafka-components_str)).
- Dual-role nodes are for development/test, not typical production.
- Place Kafka on dedicated **worker** capacity (affinity/taints). Control-plane / **infra** nodes are for platform services, not broker workloads.
- Operator and (historically ZooKeeper) cores are treated separately from broker subscription accounting in Streams subscription guidance; this calculator reports subscription from **broker** vCPU only.

## Economizing (without breaking the model)

Levers that usually lower cost or reported entitlement:

| Lever | Effect |
|-------|--------|
| Shorter retention / less mixed retention | Cuts PVC / data volume; brokers unchanged if network-bound |
| `includeRhaf: false` | Removes add-on pod footprint when RHAF is out of scope |
| `includeRhaf` / `includeDr` | Toggle RHAF add-ons and MirrorMaker 2 in Durability |
| Subscription policy (core pairs vs failover excluded) | Changes **reported** cores, not physical size |
| Pack pods on right-sized workers / LSO vs ODF | OpenShift platform cost; not Kafka formula |

Sample scenario: `docs/fixtures/fixture-economize-light.json` (3-day retention, no RHAF).

## Architecture diagrams (separate module)

Topology diagrams are **not** part of the sizing formulas. Use
[`engine/architecture-diagram.mjs`](../engine/architecture-diagram.mjs) to turn a scenario
JSON / sizing result into Mermaid (or PlantUML):

```js
import { sizeKafkaCluster } from './sizing-engine.mjs';
import { architectureDiagramFromScenario } from './architecture-diagram.mjs';

const result = sizeKafkaCluster(input);
const { diagram } = architectureDiagramFromScenario({ input, result }, { format: 'mermaid' });
```

The Results step exposes Copy / Download `.mmd`. Sync with `npm run sync-engine`.

## Platform outputs

### OpenShift

- Separate `KafkaNodePool` for broker and controller roles
- CPU/memory requests, PVC size per broker
- Block storage (RWO), XFS; ODF or LSO guidance
- Operators: Cluster, Entity, Cruise Control

### RHEL

- Broker and controller host counts
- Same CPU/RAM/disk numbers; local block storage on XFS/ext4

## RHAF add-ons

Orientative estimates for production architectures:

- Apicurio Registry, HTTP Bridge, MirrorMaker 2, Cruise Control, Streams Console, Red Hat build of Keycloak

See product documentation for authoritative sizing.

## Camel integrations & Quarkus runtimes

Optional **client access patterns** (do not change broker math unless you also raise `consumerGroups`):

| Pattern | When to use | What is sized |
|---------|-------------|----------------|
| `inCluster` | Apps only inside the platform | No Camel/Quarkus runtime estimate |
| `camel` | Client wants **Apache Camel** mediation | Red Hat build of Apache Camel (Quarkus): `max(2, camelIntegrations)` pods |
| `external` | Clients consume/produce **outside OpenShift** | Quarkus Kafka clients: `max(2, quarkusRuntimes)` pods + external listener notes |
| `camelAndExternal` | Both | Camel + external Quarkus runtimes |

Resource baselines (orientative; +1 vCPU when ingress &gt; 50 MB/s):

- Camel: 2 vCPU / 2–4 Gi per instance
- Quarkus: 1–2 vCPU / 1–2 Gi per instance

These runtimes are **not** Streams subscription cores. External groups must still be counted in Kafka `consumerGroups` for network sizing.

## References

- [Streams for Apache Kafka 3.2 Reference](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2#Reference)
- [Minimum sizing (OpenShift dev)](https://access.redhat.com/solutions/4205851)
- [Kafka configuration tuning](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.0/html-single/kafka_configuration_tuning/index)
- [Strimzi deploying guide](https://strimzi.io/docs/operators/latest/deploying.html)
- [Red Hat build of Apache Camel](https://docs.redhat.com/en/documentation/red_hat_build_of_apache_camel/)
- [Red Hat build of Quarkus](https://docs.redhat.com/en/documentation/red_hat_build_of_quarkus/)
