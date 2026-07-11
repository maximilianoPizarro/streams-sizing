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
    extendedRetentionPercent: 0,
    annualGrowthRatePercent: 0,
    projectionYears: 0,
    controllerFailuresTolerated: 1,
    topicThroughputMBps: 0,
    producerThroughputMBps: 0,
    consumerThroughputMBps: 0,
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
    let cls = 'pf-v5-c-wizard__nav-item';
    if (i === state.step) cls += ' is-current';
    else if (i < state.step) cls += ' is-complete';
    return `<li class="${cls}" role="listitem">${i + 1}. ${s.title}</li>`;
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
    <h2 class="pf-v5-c-title pf-m-2xl">Deployment platform</h2>
    <p>Select where Streams for Apache Kafka will run. The analytical model is the same; outputs differ (NodePools vs hosts).</p>
    <div class="streams-platform-cards" role="radiogroup" aria-label="Platform">
      ${['openshift', 'rhel'].map((p) => `
        <button type="button" class="streams-platform-card ${state.input.platform === p ? 'is-selected' : ''}"
          data-platform="${p}" aria-pressed="${state.input.platform === p}">
          <h3>${p === 'openshift' ? 'Red Hat OpenShift' : 'Red Hat Enterprise Linux'}</h3>
          <p>${p === 'openshift'
            ? 'Strimzi operators, KafkaNodePool, PVCs, dedicated workers.'
            : 'KRaft on RHEL hosts, local block storage, no Kubernetes.'}</p>
        </button>`).join('')}
    </div>`;
}

function renderWorkloadStep() {
  return `
    <h2 class="pf-v5-c-title pf-m-2xl">Workload</h2>
    <div class="streams-field-grid">
      ${field('messageRate', 'Target message rate (msgs/s)', 'number', { min: 1, help: 'Ingress rate the cluster must handle.' })}
      ${field('messageSizeBytes', 'Average message size (bytes)', 'number', { min: 1, help: 'Use 8000 for 8 KB average (decimal KB).' })}
      ${field('replicas', 'Replication factor', 'number', { min: 1, max: 7 })}
      ${field('netSpeedGbps', 'Network adapter speed (Gbps)', 'number', { min: 0.1, step: 0.1, help: 'Full-duplex assumed.' })}
      ${field('diskThroughputMBps', 'Max disk throughput (MB/s)', 'number', { min: 1, help: '~125 HDD, 400+ SSD/NVMe.' })}
      ${field('maxUtil', 'Max utilisation (0.01–1.00)', 'number', { min: 0.01, max: 1, step: 0.01 })}
      ${field('annualGrowthRatePercent', 'Annual growth rate (%)', 'number', { min: 0, step: 0.1 })}
      ${field('projectionYears', 'Projection horizon (years)', 'number', { min: 0, max: 10, help: '0 = current year; 3 = year-3 projection.' })}
    </div>`;
}

function renderDurabilityStep() {
  return `
    <h2 class="pf-v5-c-title pf-m-2xl">Durability & storage</h2>
    <div class="streams-field-grid">
      ${field('retentionDays', 'Standard retention (days)', 'number', { min: 1 })}
      ${field('extendedRetentionDays', 'Extended retention (days)', 'number', { min: 0, help: '0 = disabled.' })}
      ${field('extendedRetentionPercent', 'Volume on extended retention (%)', 'number', { min: 0, max: 100 })}
      ${field('controllerFailuresTolerated', 'Controller failures tolerated', 'number', {
        options: [[1, '1 failure → 3 controllers'], [2, '2 failures → 5 controllers']],
      })}
      ${field('subscriptionPolicy', 'Subscription core policy', 'number', {
        options: [
          ['corePairs', 'Core pairs: (brokers × vCPU) ÷ 2'],
          ['failoverExcluded', 'Failover excluded: (brokers − 1) × vCPU'],
        ],
      })}
    </div>`;
}

function renderConsumersStep() {
  return `
    <h2 class="pf-v5-c-title pf-m-2xl">Consumers & partitions</h2>
    <div class="streams-field-grid">
      ${field('consumerGroups', 'Consumer groups', 'number', { min: 0 })}
      ${field('laggingConsumers', 'Lagging consumers', 'number', { min: 0, help: '0 = best case (page cache hits). Worst case ≈ consumerGroups + (RF − 1).' })}
      ${field('topicThroughputMBps', 'Topic throughput (MB/s)', 'number', { min: 0, help: 'Optional; 0 skips partition estimate.' })}
      ${field('producerThroughputMBps', 'Slowest producer (MB/s)', 'number', { min: 0 })}
      ${field('consumerThroughputMBps', 'Slowest consumer (MB/s)', 'number', { min: 0 })}
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
  const poolRows = pd.kafkaNodePools
    ? pd.kafkaNodePools.map((p) => `
        <tr><th>${p.role} nodes</th><td>${p.nodes} × ${p.resources.cpuRequest} CPU, ${p.resources.memoryRequestGi} Gi RAM, ${p.resources.pvcSizeGi} Gi disk</td></tr>`).join('')
    : `
        <tr><th>Broker hosts</th><td>${pd.topology.brokerHosts}</td></tr>
        <tr><th>Controller hosts</th><td>${pd.topology.controllerHosts}</td></tr>`;

  const rhafRows = (r.rhaf?.components ?? []).map((c) => `
    <tr>
      <th>${c.name}</th>
      <td>${c.estimate.instances} instance(s), ${c.estimate.vcpuEach} vCPU, ${c.estimate.memoryGiEach} Gi RAM — ${c.role}</td>
    </tr>`).join('');

  return `
    <div class="streams-results">
      <h2>Sizing summary (${pd.deploymentTarget})</h2>
      <table class="streams-results-table">
        <tr><th>Ingress throughput</th><td>${r.ingressMBps} MB/s</td></tr>
        <tr><th>Binding constraint</th><td>${r.bindingConstraint}</td></tr>
        <tr><th>Broker nodes</th><td>${r.brokerNodes}</td></tr>
        <tr><th>Controller nodes (KRaft)</th><td>${r.controllerNodes}</td></tr>
        <tr><th>Daily storage (RF included)</th><td>${formatGb(r.dailyDiskUsageGB)}</td></tr>
        <tr><th>Total storage (effective retention)</th><td>${formatGb(r.totalDiskStorageGB)} (${r.retentionEffectiveDays} days effective)</td></tr>
        <tr><th>Disk per broker</th><td>${formatGb(r.diskPerBrokerGB)}</td></tr>
        <tr><th>Subscription cores (${r.subscriptionPolicy})</th><td>${r.subscriptionCoresReported}</td></tr>
        <tr><th>Core pairs (alternate)</th><td>${r.subscriptionCorePairs}</td></tr>
        <tr><th>Partitions (if estimated)</th><td>${r.partitions || '—'}</td></tr>
        ${poolRows}
      </table>

      <h2>RHAF complementary components</h2>
      <p><small>${r.rhaf?.disclaimer ?? ''}</small></p>
      <table class="streams-results-table">${rhafRows}</table>

      <h2>Verification trace</h2>
      <pre class="streams-trace">${JSON.stringify(r.trace, null, 2)}</pre>

      <div class="streams-actions">
        <button type="button" class="pf-v5-c-button pf-m-secondary" id="btn-export">Export scenario (JSON)</button>
        <label class="pf-v5-c-button pf-m-secondary">
          Import scenario
          <input type="file" id="import-file" accept="application/json" hidden />
        </label>
        <button type="button" class="pf-v5-c-button pf-m-link" id="btn-load-light">Load fixture: light</button>
        <button type="button" class="pf-v5-c-button pf-m-link" id="btn-load-heavy">Load fixture: heavy</button>
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
}

async function loadFixture(name) {
  const res = await fetch(`assets/fixtures/${name}.json`);
  const fx = await res.json();
  state.input = { ...state.input, ...fx.input };
  state.step = STEPS.length - 1;
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

renderNav();
renderBody();
