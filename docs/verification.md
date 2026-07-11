---
layout: doc
title: Verification
---

# Verification

Every result includes a **JSON trace** of intermediate variables so Solution Architects can audit the calculation.

## Automated tests

```bash
npm test
```

Golden fixtures (anonymized workloads):

| Fixture | Description |
|---------|-------------|
| `docs/fixtures/fixture-light.json` | ~1400 msg/s, 8 KB, RF=3, mixed retention → **4 brokers** |
| `docs/fixtures/fixture-economize-light.json` | Same ingress, 3-day retention, no RHAF → lower disk |
| `docs/fixtures/fixture-heavy.json` | ~500 MB/s ingress, **high consumer fan-out** → **16 brokers** |
| `docs/fixtures/fixture-example-aggregate.json` | ~500 MB/s prod example (low fan-out) → **10 brokers**, 388800 GB |

The GitHub Pages UI loads copies under `docs/assets/fixtures/` (Jekyll excludes `docs/fixtures/` from the site build). Keep both trees in sync with `npm run sync-engine`.

Tests assert throughput, storage, KRaft controller count, platform adapters, RHAF output, architecture Mermaid export, and export/import reproducibility.

Reproduce any scenario:

```bash
npm test
# or load a fixture in the UI and Export scenario (JSON)
```

## Manual verification (fixture-light)

**Year 0 inputs:** 1400 msg/s, 8000 bytes, RF=3

```
writesMBps = 1400 × 8000 / 1e6 = 11.2
dailyDiskGB = ceil(11.2 × 86400 / 1000 × 3) = 2904
```

**Year 3** (8% growth): rate ≈ 1763.6 msg/s → 14.11 MB/s → daily ≈ 3657 GB (RF=3).

**Mixed retention** (7d / 45d, 20% extended):

```
effectiveDays = 7 × 0.8 + 45 × 0.2 = 14.6
```

## Load simulation (recommended)

Validate estimates with Kafka perf tools before production:

- `kafka-producer-perf-test`
- `kafka-consumer-perf-test`

See [Developing Kafka client applications](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/developing_kafka_client_applications/index).

## Export / import

On the Results step, export a scenario JSON. Re-import to reproduce the same inputs and compare traces after engine updates.

## Legacy Spring Boot

The original calculator logic lives on branch `legacy/spring-boot` (Andy Yuen model + ZooKeeper outputs). Compare traces when migrating from [kafkasizing.azurewebsites.net](https://kafkasizing.azurewebsites.net/size).
