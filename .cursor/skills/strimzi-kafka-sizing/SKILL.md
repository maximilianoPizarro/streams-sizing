---
name: strimzi-kafka-sizing
description: >-
  Strimzi and Red Hat Streams for Apache Kafka 3.2 sizing guidance. Use when
  translating streams-sizing results to NodePools, storage, RF, KRaft controllers,
  partition limits, MirrorMaker/RHAF, or deciding Camel vs external clients.
---

# Strimzi / Streams sizing guidance

## Product context

- Target: **Red Hat Streams for Apache Kafka 3.2** on OpenShift or RHEL, **KRaft** (no ZooKeeper).
- Calculator: analytical model from Andy Yuen — planning estimate, not a substitute for load testing.
- Strimzi CRs: `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, MirrorMaker 2 for DR.

## Mapping calculator → Strimzi

| Calculator output | Strimzi / ops |
|-------------------|---------------|
| `brokerNodes` | `KafkaNodePool` role `broker`, replicas |
| `controllerNodes` | `KafkaNodePool` role `controller` (KRaft) |
| `diskPerBrokerGB` | JBOD/persistent volume per broker + headroom already in model |
| `replicas` (RF) | `spec.kafka.replicas` / topic `replication.factor` |
| `partitionsPerBroker` | Stay under ~4000/broker; split topics or add brokers |
| `rhaf` / MirrorMaker | Only when `includeDr === true`; MM2 tasks + connect replicas |
| Integrations (Camel/Quarkus) | Separate deployments — not Kafka broker pods |

## NodePool patterns

```yaml
# Brokers and controllers in separate pools (Streams 3.2 / Strimzi 0.40+)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: brokers
  labels:
    strimzi.io/cluster: my-cluster
spec:
  replicas: <brokerNodes>
  roles: [broker]
  storage:
    type: persistent-claim
    size: <diskPerBrokerGB>Gi
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: controllers
spec:
  replicas: <controllerNodes>
  roles: [controller]
  storage:
    type: persistent-claim
    size: 32Gi   # metadata log; calculator focuses on broker disk
```

## When to use integrations sizing

| Pattern | Use |
|---------|-----|
| `inCluster` | Producers/consumers as pods in same OpenShift cluster |
| `camel` | Camel K routes / integrations hitting Kafka |
| `externalOpenShift` | Quarkus/Java clients outside cluster (VPN, DMZ) |
| `camelAndExternal` | Both — sums component instances |

## Partition & RF guidance

- RF=3 typical production; RF=2 only with explicit risk acceptance.
- High partition count with low throughput → warning in calculator; enforce with `enforcePartitionLimit: true` to bump brokers.
- KRaft: 3 or 5 controllers for quorum; match `controllerFailuresTolerated` input.

## DR / RHAF

- Enable DR sizing only when MirrorMaker 2 replication is planned (`includeDr: true`).
- Network duplex: use `half` for constrained VPN/overlay paths.

## Anti-patterns

- Sizing Connect/Camel pods as Kafka brokers.
- Using subscription vCPU from licensing line as JVM heap or CPU limit without review.
- Assuming calculator disk includes non-Kafka PVCs (OS logs, metrics sidecars) — add ops margin separately.

## References in repo

- `docs/methodology.md` — formulas and margins
- `docs/verification.md` — fixture golden values
- `docs/fixtures/fixture-example-aggregate.json` — production-scale example
