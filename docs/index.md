---
layout: default
title: Calculator
calculator: true
og_title: "Streams for Apache Kafka — Capacity Planning Calculator"
og_description: "Constraint-based sizing tool for Red Hat Streams for Apache Kafka on OpenShift or RHEL (KRaft)."
---

<div class="streams-hero">
  <p class="streams-hero__eyebrow">Capacity planning</p>
  <h1 class="streams-hero__title">Size your Kafka cluster</h1>
  <p class="streams-hero__lead">
    Constraint-based estimate for Red Hat Streams for Apache Kafka on OpenShift or RHEL (KRaft).
    Results include broker/controller topology, storage, subscription cores, RHAF add-ons,
    and optional Camel / Quarkus integration runtimes (including clients outside OpenShift).
  </p>
</div>

<div class="streams-disclaimer">
  <p class="streams-disclaimer__title">Start from the repo example, then replace with your data</p>
  <p class="streams-disclaimer__text">
    The form is pre-filled with the anonymized <strong>fixture-light</strong> example from this repository
    (<code>docs/fixtures/fixture-light.json</code>). Leaving the defaults reproduces that sample sizing.
    Replace every value with your peak workload, retention, network, disk, and consumer topology before using the result for planning.
    You can also load other fixtures or import/export a scenario JSON from the Results step.
  </p>
</div>

<div id="calculator-app" class="streams-calculator" aria-live="polite">
  <div class="streams-wizard">
    <ul class="streams-wizard__nav" id="wizard-nav"></ul>
    <div class="streams-wizard__body" id="wizard-body"></div>
    <footer class="streams-wizard__footer">
      <button type="button" class="streams-btn streams-btn--secondary" id="btn-back" disabled>Back</button>
      <button type="button" class="streams-btn streams-btn--primary" id="btn-next">Next</button>
    </footer>
  </div>
</div>
