---
layout: doc
title: Usage walkthrough
---

# Usage walkthrough

Step-by-step example using an **anonymized high-throughput production workload** (~500 MB/s peak ingress, 8 KiB average message, 72 h retention, RF=3, OpenShift).

## Example inputs

| Parameter | Value |
|-----------|-------|
| Platform | Red Hat OpenShift |
| Message rate | 61,035 msg/s |
| Message size | 8,192 bytes (8 KiB) |
| Replication factor | 3 |
| Network | 25 Gbps |
| Disk throughput | 400 MB/s |
| Retention | 3 days (72 h) |
| Consumer groups | 3 |
| Subscription policy | Failover excluded |
| Client access pattern | In-cluster only (optional Camel / external Quarkus) |

Load this scenario from the Results step: **Load fixture: aggregate example**, or open  
`/streams-sizing/?fixture=fixture-example-aggregate&step=4`

## Step 1 — Platform

Select **Red Hat OpenShift** for Strimzi `KafkaNodePool`, PVCs, and operator-based deployment.

![Step 1 — Platform]({{ '/assets/screenshots/walkthrough/step-01-platform.png' | relative_url }})

## Step 2 — Workload

Enter peak ingress and infrastructure limits. At 61,035 msg/s × 8 KiB the engine computes **~500 MB/s** ingress. Each field includes guidance on what it measures.

![Step 2 — Workload]({{ '/assets/screenshots/walkthrough/step-02-workload.png' | relative_url }})

## Step 3 — Durability & storage

Set retention (72 h), controller quorum tolerance, and subscription core policy for licensing estimates.

![Step 3 — Durability & storage]({{ '/assets/screenshots/walkthrough/step-03-durability.png' | relative_url }})

## Step 4 — Consumers, partitions & client access

Define consumer groups and optional partition throughputs. Choose the **client access pattern** when the architecture uses **Apache Camel** integrations and/or **direct Kafka clients outside OpenShift** (Quarkus runtimes).

![Step 4 — Consumers & partitions]({{ '/assets/screenshots/walkthrough/step-04-consumers.png' | relative_url }})

## Step 5 — Results

Review **Total cluster** (nodes, vCPU, memory, disk, subscription cores), then the role breakdown, RHAF add-ons, optional Camel/Quarkus integrations, and the verification trace.

![Step 5 — Results]({{ '/assets/screenshots/walkthrough/step-05-results.png' | relative_url }})

Expected for this fixture: **10 brokers + 3 controllers**, ~**388.8 TB** Kafka data, **90** subscription cores (failover excluded).

Compare the trace with [Verification]({{ '/verification.html' | relative_url }}) and export JSON for audit.
