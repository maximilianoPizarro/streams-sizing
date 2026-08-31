# streams-sizing

Capacity planning calculator for **Red Hat Streams for Apache Kafka** on OpenShift or RHEL.

Live site (GitHub Pages): `https://maximilianopizarro.github.io/streams-sizing/`

See [Usage walkthrough](docs/usage-walkthrough.md) for a step-by-step example with screenshots (anonymized high-throughput workload).

## Features

- Constraint-based broker sizing (network, disk, replication, safety factor)
- KRaft controller quorum (3 or 5 nodes)
- OpenShift (`KafkaNodePool`) vs RHEL host outputs
- Mixed retention, growth projection, optional partition estimate
- RHAF add-on estimates (Registry, Bridge, MirrorMaker 2, Console, Keycloak)
- Export/import scenarios (JSON), including optional planning metadata
- Browser **Print / Save as PDF** from Results (paged sheets: cover, subscription math, tables, architecture)
- Verifiable calculation trace on every result
- Economize suggestions + optional RHAF / MirrorMaker 2 toggles
- Architecture HTML diagrams from scenario JSON (`engine/architecture-diagram.mjs`) — branded logos + quantities; dual-site when DR is enabled

## Analytical model

Ported from [Andy Yuen's kafka-sizing](https://github.com/AndyYuen/kafka-sizing) and extended for Streams 3.2 / Strimzi. See [docs/methodology.md](docs/methodology.md).

Legacy Spring Boot + ZooKeeper UI: branch [`legacy/spring-boot`](https://github.com/maximilianPizarro/streams-sizing/tree/legacy/spring-boot) (original [Azure deployment](https://kafkasizing.azurewebsites.net/size)).

## Verify

```bash
npm test
node scripts/sync-engine.mjs   # copy engine + architecture-diagram + fixtures → docs/assets/
```

Golden fixtures: `docs/fixtures/fixture-light.json`, `fixture-economize-light.json`, `fixture-heavy.json`, `fixture-example-aggregate.json`.

Planning samples (msg/s TPS, 72h retention, dual-site DR): `fixture-entry-10pct-50tps.json`, `fixture-peak-500-to-3k-tps.json`. Load in the wizard via `?fixture=<name>&step=4` or the Planning package controls on Results.

## Local preview (Jekyll)

```bash
cd docs
bundle install   # optional: gem install jekyll
bundle exec jekyll serve
```

Or open `docs/index.md` output after `jekyll build` in `docs/_site/`.

**Note:** ES modules require HTTP (GitHub Pages or `jekyll serve`), not `file://`.

### Local print / PDF smoke test

Without Jekyll, serve `docs/` over HTTP and open the print helper:

```bash
cd docs
python -m http.server 8765
# then open:
# http://127.0.0.1:8765/local-print-preview.html?fixture=fixture-entry-10pct-50tps&step=4
```

Use the Results **Print** control (or the browser print dialog) to Save as PDF. Sheets are laid out for A4 with cover, subscription cores, capacity tables, and architecture.

## GitHub Pages

1. Repository **Settings → Pages**
2. Source: **Deploy from branch**
3. Branch: `main`, folder: `/docs`
4. Site URL: `https://<user>.github.io/streams-sizing/`

Ensure `baseurl` in [docs/_config.yml](docs/_config.yml) matches the repo name.

## Branding

Official product logo: [docs/assets/brand/](docs/assets/brand/) ([Red Hat brand standards](https://www.redhat.com/en/about/brand/standards/product-logos)).

## Disclaimer

This tool provides **educated estimates**. Validate with `kafka-producer-perf-test`, `kafka-consumer-perf-test`, and production metrics before purchasing or deploying.

## License

Apache License 2.0 (see [LICENSE](LICENSE)). Analytical model attribution: Andy Yuen / kafka-sizing.
