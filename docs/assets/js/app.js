import {
  sizeKafkaCluster,
  exportScenario,
  importScenario,
  DEFAULTS,
} from './sizing-engine.mjs';

const STEPS = [
  { id: 'platform', title: 'Platform' },
  { id: 'workload', title: 'Workload' },
  { id: 'durability', title: 'Durability & storage' },
  { id: 'consumers', title: 'Consumers & partitions' },
  { id: 'results', title: 'Results' },
];

/** Defaults match docs/fixtures/fixture-light.json (repo sample). */
const state = {
  step: 0,
  input: {
    platform: 'openshift',
    messageRate: 1400,
    messageSizeBytes: 8000,
    replicas: 3,
    netSpeedGbps: 10,
    diskThroughputMBps: 400,
    maxUtil: 0.65,
    consumerGroups: 12,
    laggingConsumers: 0,
    retentionDays: 7,
    extendedRetentionDays: 45,
    extendedRetentionPercent: 20,
    annualGrowthRatePercent: 8,
    projectionYears: 0,
    controllerFailuresTolerated: 1,
    topicThroughputMBps: 11,
    producerThroughputMBps: 2,
    consumerThroughputMBps: 2,
    includeRhaf: true,
    subscriptionPolicy: 'corePairs',
  },
  result: null,
};

const navEl = document.getElementById('wizard-nav');
const bodyEl = document.getElementById('wizard-body');
const btnBack = document.getElementById('btn-back');
const btnNext = document.getElementById('btn-next');

function renderNav() {
  navEl.innerHTML = STEPS.map((s, i) => {
    let cls = 'streams-wizard__step';
    if (i === state.step) cls += ' is-current';
    else if (i < state.step) cls += ' is-complete';
    return `<li class="${cls}"><span class="streams-wizard__step-num">${i + 1}.</span> ${s.title}</li>`;
  }).join('');
}

function field(name, label, type = 'number', opts = {}) {
  const val = state.input[name];
  const { min, max, step, help, options } = opts;
  if (options) {
    return `
      <div class="streams-field">
        <label for="${name}">${label}</label>
        <select id="${name}" name="${name}">
          ${options.map(([v, t]) => `<option value="${v}" ${String(val) === String(v) ? 'selected' : ''}>${t}</option>`).join('')}
        </select>
        ${help ? `<small>${help}</small>` : ''}
      </div>`;
  }
  return `
    <div class="streams-field">
      <label for="${name}">${label}</label>
      <input type="${type}" id="${name}" name="${name}" value="${val ?? ''}"
        ${min != null ? `min="${min}"` : ''} ${max != null ? `max="${max}"` : ''} ${step != null ? `step="${step}"` : ''} />
      ${help ? `<small>${help}</small>` : ''}
    </div>`;
}

function renderPlatformStep() {
  return `
    <h2>Deployment platform</h2>
    <p class="streams-step-intro">
      Choose where Streams for Apache Kafka will run. The analytical formulas are the same;
      only the output shape changes (OpenShift <code>KafkaNodePool</code> / PVCs vs RHEL host counts).
      Defaults below follow the repository sample fixture — change them to your environment.
    </p>
    <div class="streams-platform-cards" role="radiogroup" aria-label="Platform">
      ${['openshift', 'rhel'].map((p) => `
        <button type="button" class="streams-platform-card ${state.input.platform === p ? 'is-selected' : ''}"
          data-platform="${p}" aria-pressed="${state.input.platform === p}">
          <h3>${p === 'openshift' ? 'Red Hat OpenShift' : 'Red Hat Enterprise Linux'}</h3>
          <p>${p === 'openshift'
            ? 'Cluster Operator, separate broker/controller NodePools, block PVCs (ODF/LSO).'
            : 'Dedicated RHEL hosts for brokers and KRaft controllers, local block disks.'}</p>
        </button>`).join('')}
    </div>`;
}

function renderWorkloadStep() {
  return `
    <h2>Workload</h2>
    <p class="streams-step-intro">
      These values drive ingress throughput (MB/s), network and disk pressure, and growth projections.
      Use <strong>peak sustained</strong> rates, not averages, unless you intentionally size for average load.
    </p>
    <div class="streams-field-grid">
      ${field('messageRate', 'Target message rate (msgs/s)', 'number', {
        min: 1,
        help: 'How many messages per second the cluster must accept at the sizing point (usually peak). Ingress MB/s = rate × size / 1,000,000.',
      })}
      ${field('messageSizeBytes', 'Average message size (bytes)', 'number', {
        min: 1,
        help: 'Mean payload size including typical headers. Example: 8000 ≈ 8 KB (decimal). Larger messages raise MB/s and storage faster than rate alone.',
      })}
      ${field('replicas', 'Replication factor (RF)', 'number', {
        min: 1,
        max: 7,
        help: 'Copies of each partition (leaders + followers). Production default is usually 3. Multiplies write traffic and disk usage by RF.',
      })}
      ${field('netSpeedGbps', 'Network adapter speed (Gbps)', 'number', {
        min: 0.1,
        step: 0.1,
        help: 'NIC speed available to each broker (full-duplex). Used to compute network capacity: (Gbps / 8) × 1000 MB/s.',
      })}
      ${field('diskThroughputMBps', 'Max disk throughput (MB/s)', 'number', {
        min: 1,
        help: 'Sustainable sequential read/write per broker disk path. Typical: ~125 HDD, ~400+ SSD/NVMe. Underestimating this under-sizes brokers.',
      })}
      ${field('maxUtil', 'Max utilisation target (0.01–1.00)', 'number', {
        min: 0.01,
        max: 1,
        step: 0.01,
        help: 'Headroom target for the binding constraint (network or disk). 0.65 means size so peak utilisation stays near 65% before the safety factor.',
      })}
      ${field('annualGrowthRatePercent', 'Annual growth rate (%)', 'number', {
        min: 0,
        step: 0.1,
        help: 'Compound annual growth applied to message rate when Projection horizon &gt; 0. Example: 8 with 3 years → rate × 1.08³.',
      })}
      ${field('projectionYears', 'Projection horizon (years)', 'number', {
        min: 0,
        max: 10,
        help: '0 = size for current rate. 3 = size for year-3 projected rate using the annual growth above.',
      })}
    </div>`;
}

function renderDurabilityStep() {
  return `
    <h2>Durability & storage</h2>
    <p class="streams-step-intro">
      Retention drives total disk. Mixed retention models a share of volume kept longer than the standard policy.
      Controller count is independent of broker count (KRaft quorum).
    </p>
    <div class="streams-field-grid">
      ${field('retentionDays', 'Standard retention (days)', 'number', {
        min: 1,
        help: 'Default topic retention in days for most of the data. Total disk ≈ daily growth × effective retention days.',
      })}
      ${field('extendedRetentionDays', 'Extended retention (days)', 'number', {
        min: 0,
        help: 'Longer retention window for a subset of topics/data (compliance, replay). Set 0 to disable mixed retention.',
      })}
      ${field('extendedRetentionPercent', 'Volume on extended retention (%)', 'number', {
        min: 0,
        max: 100,
        help: 'Share of volume kept for the extended window. Effective days = std×(1−X) + ext×X, where X is this percent / 100.',
      })}
      ${field('controllerFailuresTolerated', 'Controller failures tolerated', 'number', {
        options: [
          [1, '1 failure → 3 controllers'],
          [2, '2 failures → 5 controllers'],
        ],
        help: 'KRaft quorum size. Controllers do not count toward Streams subscription cores. Clusters with &gt;50 brokers also use 5 controllers.',
      })}
      ${field('subscriptionPolicy', 'Subscription core policy', 'number', {
        options: [
          ['corePairs', 'Core pairs: (brokers × vCPU) ÷ 2'],
          ['failoverExcluded', 'Failover excluded: (brokers − 1) × vCPU'],
        ],
        help: 'How subscription cores are reported. Core pairs is the classic pairing model; failover excluded omits one broker as spare capacity.',
      })}
    </div>`;
}

function renderConsumersStep() {
  return `
    <h2>Consumers & partitions</h2>
    <p class="streams-step-intro">
      Consumer fan-out is often the network bottleneck. Partition fields are optional: leave at 0 to skip partition estimation.
    </p>
    <div class="streams-field-grid">
      ${field('consumerGroups', 'Consumer groups', 'number', {
        min: 0,
        help: 'Independent consumer groups reading the same topics at peak. Raises net-read: (groups + RF − 1) × writes. Use the peak concurrent group count.',
      })}
      ${field('laggingConsumers', 'Lagging consumers', 'number', {
        min: 0,
        help: 'Consumers reading older data from disk instead of page cache. Adds disk I/O: (RF + lagging) × writes. 0 = best case; worst case ≈ groups + (RF − 1).',
      })}
      ${field('topicThroughputMBps', 'Topic throughput (MB/s)', 'number', {
        min: 0,
        help: 'Optional. Aggregate throughput of the topic used for partition sizing. Set 0 to skip the partition estimate.',
      })}
      ${field('producerThroughputMBps', 'Slowest producer throughput (MB/s)', 'number', {
        min: 0,
        help: 'Optional. Throughput of the slowest producer client. Partitions ≈ ceil(topicTP / producerTP) when all three optional fields are &gt; 0.',
      })}
      ${field('consumerThroughputMBps', 'Slowest consumer throughput (MB/s)', 'number', {
        min: 0,
        help: 'Optional. Throughput of the slowest consumer. Partitions also consider ceil(topicTP / consumerTP); final partitions = max of both.',
      })}
    </div>`;
}

function formatGb(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)} PB`;
  if (n >= 1000) return `${(n / 1000).toFixed(2)} TB`;
  return `${n} GB`;
}

function renderResultsStep() {
  const r = state.result;
  if (!r) return '<p>Calculating…</p>';

  const pd = r.platformDetails;
  const t = r.clusterTotals;
  const poolRows = pd.kafkaNodePools
    ? pd.kafkaNodePools.map((p) => `
        <tr><th>${p.role} nodes</th><td>${p.nodes} × ${p.resources.cpuRequest} CPU, ${p.resources.memoryRequestGi} Gi RAM, ${p.resources.pvcSizeGi} Gi disk</td></tr>`).join('')
    : `
        <tr><th>Broker hosts</th><td>${pd.topology.brokerHosts}</td></tr>
        <tr><th>Controller hosts</th><td>${pd.topology.controllerHosts}</td></tr>
        <tr><th>Resources per broker</th><td>${pd.resourcesPerBroker.vcpus} vCPU, ${pd.resourcesPerBroker.memoryGi} Gi RAM, ${formatGb(pd.resourcesPerBroker.diskGi)} disk</td></tr>
        <tr><th>Resources per controller</th><td>${pd.resourcesPerController.vcpus} vCPU, ${pd.resourcesPerController.memoryGi} Gi RAM, ${formatGb(pd.resourcesPerController.diskGi)} disk</td></tr>`;

  const rhafRows = (r.rhaf?.components ?? []).map((c) => `
    <tr>
      <th>${c.name}</th>
      <td>${c.estimate.instances} instance(s), ${c.estimate.vcpuEach} vCPU, ${c.estimate.memoryGiEach} Gi RAM — ${c.role}</td>
    </tr>`).join('');

  const withRhaf = t.withRhaf
    ? `<tr><th>Grand total with RHAF</th><td>${t.withRhaf.nodes} instances/nodes · ${t.withRhaf.vcpus} vCPU · ${t.withRhaf.memoryGi} Gi RAM</td></tr>`
    : '';

  return `
    <div class="streams-results">
      <p class="streams-step-intro">
        Results for the inputs above. If you kept the defaults, this matches the repository
        <code>fixture-light</code> sample — replace inputs with your workload and recalculate.
        Export JSON to keep an auditable, reproducible scenario.
      </p>

      <h2>Total cluster</h2>
      <table class="streams-results-table streams-results-table--total">
        <tr><th>Nodes (brokers + controllers)</th><td><strong>${t.nodes}</strong> (${t.brokerNodes} brokers + ${t.controllerNodes} controllers)</td></tr>
        <tr><th>Total vCPU</th><td><strong>${t.vcpus}</strong></td></tr>
        <tr><th>Total memory</th><td><strong>${t.memoryGi} Gi</strong></td></tr>
        <tr><th>Total provisioned disk</th><td><strong>${formatGb(t.diskGB)}</strong> (broker PVCs/hosts + controller disks)</td></tr>
        <tr><th>Kafka data volume</th><td>${formatGb(t.kafkaDataDiskGB)} across cluster (RF × retention; before per-broker 10% overhead)</td></tr>
        <tr><th>Subscription cores</th><td><strong>${t.subscriptionCoresReported}</strong> (${r.subscriptionPolicy})</td></tr>
        <tr><th>Ingress / binding</th><td>${r.ingressMBps} MB/s · ${r.bindingConstraint}</td></tr>
        ${withRhaf}
      </table>

      <h2>Breakdown (${pd.deploymentTarget})</h2>
      <table class="streams-results-table">
        <tr><th>Broker nodes</th><td>${r.brokerNodes} × ${r.vcpusPerBroker} vCPU, ${r.memPerBrokerGB} Gi RAM, ${formatGb(r.diskPerBrokerGB)} disk</td></tr>
        <tr><th>Controller nodes (KRaft)</th><td>${r.controllerNodes} × ${r.vcpusPerController} vCPU, ${r.memPerControllerGB} Gi RAM, ${formatGb(r.diskPerControllerGB)} disk</td></tr>
        <tr><th>Daily storage (RF included)</th><td>${formatGb(r.dailyDiskUsageGB)}</td></tr>
        <tr><th>Total storage (effective retention)</th><td>${formatGb(r.totalDiskStorageGB)} (${r.retentionEffectiveDays} days effective)</td></tr>
        <tr><th>Disk per broker</th><td>${formatGb(r.diskPerBrokerGB)}</td></tr>
        <tr><th>Core pairs (alternate)</th><td>${r.subscriptionCorePairs}</td></tr>
        <tr><th>Failover-excluded cores</th><td>${r.subscriptionFailoverExcluded}</td></tr>
        <tr><th>Partitions (if estimated)</th><td>${r.partitions || '—'}</td></tr>
        ${poolRows}
      </table>

      <h2>RHAF complementary components</h2>
      <p><small>${r.rhaf?.disclaimer ?? ''}</small></p>
      <table class="streams-results-table">
        ${rhafRows}
        ${r.rhaf?.totals ? `<tr><th>RHAF subtotal</th><td>${r.rhaf.totals.instances} instances · ${r.rhaf.totals.vcpus} vCPU · ${r.rhaf.totals.memoryGi} Gi RAM</td></tr>` : ''}
      </table>

      <h2>Verification trace</h2>
      <pre class="streams-trace">${JSON.stringify(r.trace, null, 2)}</pre>

      <div class="streams-actions">
        <button type="button" class="streams-btn streams-btn--secondary" id="btn-export">Export scenario (JSON)</button>
        <label class="streams-btn streams-btn--secondary">
          Import scenario
          <input type="file" id="import-file" accept="application/json" hidden />
        </label>
        <button type="button" class="streams-btn streams-btn--link" id="btn-load-light">Load fixture: light</button>
        <button type="button" class="streams-btn streams-btn--link" id="btn-load-example">Load fixture: aggregate example</button>
      </div>
    </div>`;
}

function renderBody() {
  const stepId = STEPS[state.step].id;
  switch (stepId) {
    case 'platform': bodyEl.innerHTML = renderPlatformStep(); bindPlatformCards(); break;
    case 'workload': bodyEl.innerHTML = renderWorkloadStep(); bindFields(); break;
    case 'durability': bodyEl.innerHTML = renderDurabilityStep(); bindFields(); break;
    case 'consumers': bodyEl.innerHTML = renderConsumersStep(); bindFields(); break;
    case 'results':
      state.result = sizeKafkaCluster(state.input, DEFAULTS);
      bodyEl.innerHTML = renderResultsStep();
      bindResultsActions();
      break;
    default: break;
  }
  btnBack.disabled = state.step === 0;
  btnNext.textContent = state.step === STEPS.length - 1 ? 'Recalculate' : 'Next';
}

function bindPlatformCards() {
  bodyEl.querySelectorAll('[data-platform]').forEach((el) => {
    el.addEventListener('click', () => {
      state.input.platform = el.dataset.platform;
      renderBody();
    });
  });
}

function bindFields() {
  bodyEl.querySelectorAll('input, select').forEach((el) => {
    el.addEventListener('change', () => {
      const { name, value, type } = el;
      if (type === 'number') {
        state.input[name] = value === '' ? 0 : Number(value);
      } else if (name === 'subscriptionPolicy' || name === 'platform') {
        state.input[name] = value;
      } else {
        state.input[name] = Number(value);
      }
    });
  });
}

function bindResultsActions() {
  document.getElementById('btn-export')?.addEventListener('click', () => {
    const blob = new Blob(
      [JSON.stringify(exportScenario('custom', state.input, state.result), null, 2)],
      { type: 'application/json' }
    );
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'streams-sizing-scenario.json';
    a.click();
  });

  document.getElementById('import-file')?.addEventListener('change', async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const json = importScenario(JSON.parse(await file.text()));
    state.input = { ...state.input, ...json.input };
    state.step = STEPS.length - 1;
    renderNav();
    renderBody();
  });

  document.getElementById('btn-load-light')?.addEventListener('click', () => loadFixture('fixture-light'));
  document.getElementById('btn-load-heavy')?.addEventListener('click', () => loadFixture('fixture-heavy'));
  document.getElementById('btn-load-example')?.addEventListener('click', () => loadFixture('fixture-example-aggregate'));
}

async function loadFixture(name, targetStep = null) {
  const url = new URL(`../fixtures/${name}.json`, import.meta.url);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Fixture not found: ${name}`);
  }
  const fx = await res.json();
  state.input = { ...state.input, ...fx.input };
  state.step = targetStep ?? STEPS.length - 1;
  renderNav();
  renderBody();
}

btnBack.addEventListener('click', () => {
  if (state.step > 0) {
    state.step -= 1;
    renderNav();
    renderBody();
  }
});

btnNext.addEventListener('click', () => {
  if (state.step < STEPS.length - 1) {
    state.step += 1;
    renderNav();
    renderBody();
  } else {
    state.result = sizeKafkaCluster(state.input, DEFAULTS);
    renderBody();
  }
});

async function init() {
  const demoParams = new URLSearchParams(window.location.search);
  const demoFixture = demoParams.get('fixture');
  const demoStep = demoParams.get('step');
  try {
    if (demoFixture) {
      await loadFixture(
        demoFixture,
        demoStep != null && demoStep !== '' ? Number(demoStep) : null
      );
      return;
    }
  } catch (err) {
    console.error(err);
    bodyEl.innerHTML = `<p class="streams-field"><strong>Could not load fixture.</strong> ${err.message}</p>`;
  }
  renderNav();
  renderBody();
}

init();
