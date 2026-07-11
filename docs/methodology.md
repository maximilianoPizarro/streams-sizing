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
diskIO     = (RF + laggingConsumers) × writesMBps

netCapacityMBps = (netSpeedGbps / 8) × 1000
netUtilisation  = max(netWrite, netRead) / netCapacityMBps
diskUtilisation = diskIO / diskThroughputMBps

brokersNeeded = max(netUtilisation, diskUtilisation) / maxUtil
brokerNodes   = ceil(max(brokersNeeded × safetyFactor, RF + 1))
```

Default `safetyFactor = 1.6` (protocol overhead, imbalance, peaks).

### Storage

```
dailyDiskGB = ceil(writesMBps × 86,400 / 1,000 × RF)
effectiveRetentionDays = stdDays × (1 − X) + extDays × X   # mixed retention
totalDiskGB = ceil(dailyDiskGB × effectiveRetentionDays)
diskPerBrokerGB = ceil(totalDiskGB / brokerNodes × 1.1)
```

### Growth projection

```
R_eff = R × (1 + growth%/100)^years
```

### Partitions (optional)

When topic/producer/consumer throughputs are all &gt; 0:

```
partitions = ceil(max(topicTP / producerTP, topicTP / consumerTP))
```

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
