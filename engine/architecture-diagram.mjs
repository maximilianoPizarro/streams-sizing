/**
 * Architecture diagram module (separate from sizing math).
 * Builds Mermaid (and optional PlantUML) views from a sizing scenario /
 * sizeKafkaCluster() result — for docs, export, or the Results step.
 *
 * Does not change broker/controller formulas; only visualizes topology.
 */

/**
 * @param {{ input?: object, result?: object, name?: string }} scenario
 *   Accepts exportScenario() shape or { input, result } after sizeKafkaCluster.
 * @param {{ format?: 'mermaid' | 'plantuml', title?: string }} [opts]
 * @returns {{ format: string, diagram: string, summary: object }}
 */
export function architectureDiagramFromScenario(scenario, opts = {}) {
  const format = opts.format ?? 'mermaid';
  const input = scenario?.input ?? {};
  const result = scenario?.result ?? scenario;
  if (!result || result.brokerNodes == null) {
    throw new Error('architectureDiagramFromScenario requires a sizing result with brokerNodes');
  }

  const title =
    opts.title ??
    scenario?.name ??
    `streams-sizing ${result.platform ?? input.platform ?? 'cluster'}`;

  const summary = {
    platform: result.platform ?? input.platform,
    brokers: result.brokerNodes,
    controllers: result.controllerNodes,
    ingressMBps: result.ingressMBps,
    clientAccessPattern: result.clientAccessPattern ?? input.clientAccessPattern ?? 'inCluster',
    includeRhaf: Boolean(result.rhaf),
    includeDr: input.includeDr === true,
    integrations: result.integrations?.pattern ?? null,
  };

  const diagram =
    format === 'plantuml'
      ? buildPlantuml(title, input, result, summary)
      : buildMermaid(title, input, result, summary);

  return { format, diagram, summary };
}

function esc(s) {
  return String(s).replace(/[[\](){}|]/g, ' ');
}

function buildMermaid(title, input, result, summary) {
  const lines = [
    '%%{init: {"theme": "neutral"}}%%',
    'flowchart TB',
    `  subgraph cluster["${esc(title)}"]`,
    '    direction TB',
    `    CTRL["KRaft controllers\\n${result.controllerNodes} × ${result.vcpusPerController} vCPU / ${result.memPerControllerGB} Gi"]`,
    `    BRK["Kafka brokers\\n${result.brokerNodes} × ${result.vcpusPerBroker} vCPU / ${result.memPerBrokerGB} Gi\\n${result.diskPerBrokerGB} GB PVC each"]`,
    '    CTRL -.->|metadata| BRK',
    '  end',
  ];

  const pattern = summary.clientAccessPattern;
  if (pattern === 'inCluster' || !pattern) {
    lines.push('  APPS["In-cluster producers / consumers"] -->|Kafka protocol| BRK');
  }
  if (pattern === 'camel' || pattern === 'camelAndExternal') {
    lines.push('  CAMEL["Camel for Quarkus integrations"] -->|Kafka| BRK');
  }
  if (pattern === 'external' || pattern === 'camelAndExternal') {
    lines.push('  EXT["External Quarkus clients\\n(outside OpenShift)"] -->|listener| BRK');
  }

  if (result.rhaf?.components?.length) {
    lines.push('  subgraph rhaf["RHAF complementary"]');
    lines.push('    direction LR');
    result.rhaf.components.forEach((c, i) => {
      const id = `RHAF${i}`;
      lines.push(
        `    ${id}["${esc(c.name)}\\n${c.estimate.instances}× ${c.estimate.vcpuEach} vCPU"]`
      );
      lines.push(`    ${id} -.-> BRK`);
    });
    lines.push('  end');
  }

  if (input.includeDr === true) {
    lines.push('  MM2["MirrorMaker 2"] -->|replicate| BRK');
    lines.push('  MM2 -->|to| DR["DR / remote cluster"]');
  }

  lines.push(
    `  NOTE["Ingress ${result.ingressMBps} MB/s · binding ${result.bindingConstraint}\\nSubscription cores ${result.subscriptionCoresReported} (${result.subscriptionPolicy})"]`
  );
  lines.push('  BRK --- NOTE');

  return `${lines.join('\n')}\n`;
}

function buildPlantuml(title, input, result, summary) {
  const lines = [
    '@startuml',
    `title ${esc(title)}`,
    'skinparam componentStyle rectangle',
    `package "Kafka (${summary.platform})" {`,
    `  component "Controllers\\n${result.controllerNodes}" as CTRL`,
    `  component "Brokers\\n${result.brokerNodes}" as BRK`,
    '  CTRL ..> BRK : metadata',
    '}',
  ];
  if (summary.clientAccessPattern === 'camel' || summary.clientAccessPattern === 'camelAndExternal') {
    lines.push('component "Camel" as CAMEL');
    lines.push('CAMEL --> BRK');
  }
  if (summary.clientAccessPattern === 'external' || summary.clientAccessPattern === 'camelAndExternal') {
    lines.push('component "External clients" as EXT');
    lines.push('EXT --> BRK');
  }
  if (input.includeDr === true) {
    lines.push('component "MirrorMaker 2" as MM2');
    lines.push('MM2 --> BRK');
  }
  lines.push('@enduml');
  return `${lines.join('\n')}\n`;
}

/**
 * Convenience: size + diagram in one call (keeps diagram module usable standalone
 * when caller already has a result).
 */
export function mermaidFromSizingResult(result, input = {}, opts = {}) {
  return architectureDiagramFromScenario({ input, result }, { ...opts, format: 'mermaid' });
}
