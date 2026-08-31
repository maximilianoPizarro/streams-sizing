/**
 * Architecture diagram module (separate from sizing math).
 * Builds an HTML fragment from a sizing scenario / sizeKafkaCluster() result
 * for Results preview and print — official brand logos when assets exist,
 * always with quantities. Dual-site when includeDr.
 *
 * Does not change broker/controller formulas; only visualizes topology.
 */

/** Filenames under docs/assets/brand/ (resolved via opts.assetBase in the UI). */
const LOGO_FILES = {
  redhat: 'logo-red-hat-standard.svg',
  openshift: 'Logo-Red_Hat-OpenShift-A-Standard-RGB.svg',
};

/**
 * @param {{ input?: object, result?: object, name?: string }} scenario
 * @param {{ title?: string, assetBase?: string }} [opts]
 *   assetBase — absolute or directory URL ending with / for brand assets (browser).
 * @returns {{ format: 'html', diagram: string, summary: object }}
 */
export function architectureDiagramFromScenario(scenario, opts = {}) {
  const input = scenario?.input ?? {};
  const result = scenario?.result ?? scenario;
  if (!result || result.brokerNodes == null) {
    throw new Error('architectureDiagramFromScenario requires a sizing result with brokerNodes');
  }

  const title =
    opts.title ??
    scenario?.name ??
    `streams-sizing ${result.platform ?? input.platform ?? 'cluster'}`;

  const dual = input.includeDr === true;
  const summary = {
    platform: result.platform ?? input.platform,
    brokers: result.brokerNodes,
    controllers: result.controllerNodes,
    ingressMBps: result.ingressMBps,
    clientAccessPattern: result.clientAccessPattern ?? input.clientAccessPattern ?? 'inCluster',
    includeRhaf: Boolean(result.rhaf),
    includeDr: dual,
    integrations: result.integrations?.pattern ?? null,
    layout: dual ? 'dual' : 'single',
  };

  const assetBase = normalizeAssetBase(opts.assetBase);
  const diagram = buildHtml(title, result, summary, assetBase);

  return { format: 'html', diagram, summary };
}

function normalizeAssetBase(base) {
  if (!base) return '';
  return base.endsWith('/') ? base : `${base}/`;
}

function logoUrl(assetBase, key) {
  const file = LOGO_FILES[key];
  if (!file) return null;
  return assetBase ? `${assetBase}${file}` : `assets/brand/${file}`;
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function card({ title, qty, logoSrc, logoAlt, modifier = '' }) {
  const logo = logoSrc
    ? `<img class="streams-arch__logo" src="${esc(logoSrc)}" alt="${esc(logoAlt ?? '')}" loading="lazy" />`
    : '';
  const mod = modifier ? ` ${modifier}` : '';
  return `<div class="streams-arch__card${mod}">
    ${logo}
    <div class="streams-arch__card-body">
      <div class="streams-arch__card-title">${esc(title)}</div>
      ${qty ? `<div class="streams-arch__card-qty">${esc(qty)}</div>` : ''}
    </div>
  </div>`;
}

function qtyRole(n, vcpu, memGi, diskGb) {
  let s = `${n} × ${vcpu} vCPU`;
  if (memGi != null) s += ` · ${memGi} Gi`;
  if (diskGb != null) s += ` · ${diskGb} GB PVC`;
  return s;
}

function qtyInstances(est) {
  if (!est) return '';
  let s = `${est.instances}× ${est.vcpuEach} vCPU`;
  if (est.memoryGiEach != null) s += ` · ${est.memoryGiEach} Gi`;
  return s;
}

function buildClientCards(summary, result) {
  const parts = [];
  const pattern = summary.clientAccessPattern;
  if (pattern === 'inCluster' || !pattern) {
    parts.push(card({ title: 'In-cluster producers / consumers', qty: 'Kafka protocol' }));
  }
  if (pattern === 'camel' || pattern === 'camelAndExternal') {
    const camel = result.integrations?.components?.find((c) => /Camel/i.test(c.name));
    parts.push(
      card({
        title: camel?.name ?? 'Red Hat build of Apache Camel',
        qty: camel ? qtyInstances(camel.estimate) : undefined,
      })
    );
  }
  if (pattern === 'external' || pattern === 'camelAndExternal') {
    const quarkus = result.integrations?.components?.find((c) => /Quarkus/i.test(c.name));
    parts.push(
      card({
        title: quarkus?.name ?? 'External Quarkus clients',
        qty: quarkus
          ? qtyInstances(quarkus.estimate)
          : 'Outside OpenShift · external listener',
      })
    );
  }
  return parts.join('\n');
}

function buildKafkaCore(result, { replica = false } = {}) {
  const streamsTitle = replica
    ? 'Streams for Apache Kafka (replica)'
    : 'Red Hat Streams for Apache Kafka';
  return `<div class="streams-arch__kafka">
    <div class="streams-arch__kafka-label">${esc(streamsTitle)}</div>
    ${card({
      title: 'KRaft controllers',
      qty: qtyRole(
        result.controllerNodes,
        result.vcpusPerController,
        result.memPerControllerGB,
        result.diskPerControllerGB
      ),
    })}
    ${card({
      title: 'Kafka brokers',
      qty: qtyRole(
        result.brokerNodes,
        result.vcpusPerBroker,
        result.memPerBrokerGB,
        result.diskPerBrokerGB
      ),
      modifier: 'streams-arch__card--broker',
    })}
  </div>`;
}

function buildRhafCards(result, { omitMirrorMaker = false } = {}) {
  const components = (result.rhaf?.components ?? []).filter(
    (c) => !(omitMirrorMaker && /MirrorMaker/i.test(c.name))
  );
  if (!components.length) return '';
  const cards = components
    .map((c) => card({ title: c.name, qty: qtyInstances(c.estimate) }))
    .join('\n');
  return `<div class="streams-arch__rhaf">
    <div class="streams-arch__section-label">RHAF complementary</div>
    <div class="streams-arch__rhaf-grid">${cards}</div>
  </div>`;
}

function buildSitePanel({
  label,
  result,
  summary,
  assetBase,
  showClients,
  showRhaf,
  omitMirrorMaker,
  replica,
}) {
  const ocp = logoUrl(assetBase, 'openshift');
  return `<section class="streams-arch__site${replica ? ' streams-arch__site--replica' : ''}">
    <header class="streams-arch__site-header">
      ${
        ocp
          ? `<img class="streams-arch__logo streams-arch__logo--product" src="${esc(ocp)}" alt="Red Hat OpenShift" loading="lazy" />`
          : ''
      }
      <h3 class="streams-arch__site-title">${esc(label)}</h3>
    </header>
    <div class="streams-arch__site-body">
      ${showClients ? `<div class="streams-arch__clients">${buildClientCards(summary, result)}</div>` : ''}
      ${buildKafkaCore(result, { replica })}
      ${showRhaf ? buildRhafCards(result, { omitMirrorMaker }) : ''}
    </div>
  </section>`;
}

function buildHtml(title, result, summary, assetBase) {
  const rh = logoUrl(assetBase, 'redhat');
  const dual = summary.layout === 'dual';

  const mm2 = result.rhaf?.components?.find((c) => /MirrorMaker/i.test(c.name));
  const bridge = dual
    ? `<div class="streams-arch__bridge" aria-label="MirrorMaker 2 disaster recovery">
        ${card({
          title: 'MirrorMaker 2',
          qty: mm2
            ? `${qtyInstances(mm2.estimate)} · fiber DR`
            : 'Cross-site replication · fiber DR',
          modifier: 'streams-arch__card--bridge',
        })}
      </div>`
    : '';

  const siteA = buildSitePanel({
    label: dual ? 'Site A — Active' : 'Cluster',
    result,
    summary,
    assetBase,
    showClients: true,
    showRhaf: Boolean(result.rhaf),
    omitMirrorMaker: dual,
    replica: false,
  });

  const siteB = dual
    ? buildSitePanel({
        label: 'Site B — Replica',
        result,
        summary,
        assetBase,
        showClients: false,
        showRhaf: false,
        omitMirrorMaker: false,
        replica: true,
      })
    : '';

  const meta = `Ingress ${result.ingressMBps} MB/s · binding ${result.bindingConstraint} · subscription ${result.subscriptionCoresReported} cores (${result.subscriptionPolicy})`;

  return `<div class="streams-arch" data-layout="${esc(summary.layout)}">
  <header class="streams-arch__header">
    ${
      rh
        ? `<img class="streams-arch__logo streams-arch__logo--mast" src="${esc(rh)}" alt="Red Hat" loading="lazy" />`
        : ''
    }
    <div class="streams-arch__header-text">
      <div class="streams-arch__title">${esc(title)}</div>
      <div class="streams-arch__subtitle">OpenShift · Streams for Apache Kafka${dual ? ' · MirrorMaker 2' : ''}${summary.includeRhaf ? ' · RHAF' : ''}</div>
    </div>
  </header>
  <div class="streams-arch__canvas${dual ? ' streams-arch__canvas--dual' : ''}">
    ${siteA}
    ${bridge}
    ${siteB}
  </div>
  <footer class="streams-arch__footer">${esc(meta)}</footer>
</div>`;
}
