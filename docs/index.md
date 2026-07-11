---
layout: default
title: Calculator
calculator: true
---

<div class="streams-hero">
  <p class="streams-hero__eyebrow">Capacity planning</p>
  <h1 class="streams-hero__title">Size your Kafka cluster</h1>
  <p class="streams-hero__lead">
    Constraint-based estimate for Red Hat Streams for Apache Kafka on OpenShift or RHEL (KRaft).
    Results include broker/controller topology, storage, subscription cores, and RHAF add-ons.
  </p>
</div>

<div id="calculator-app" class="streams-calculator" aria-live="polite">
  <div class="pf-v5-c-wizard">
    <ol class="pf-v5-c-wizard__nav" id="wizard-nav" role="list"></ol>
    <div class="pf-v5-c-wizard__main">
      <div class="pf-v5-c-wizard__main-body" id="wizard-body"></div>
      <footer class="pf-v5-c-wizard__footer streams-wizard-footer">
        <button type="button" class="pf-v5-c-button pf-m-secondary" id="btn-back" disabled>Back</button>
        <button type="button" class="pf-v5-c-button pf-m-primary" id="btn-next">Next</button>
      </footer>
    </div>
  </div>
</div>

<div class="streams-disclaimer pf-v5-c-alert pf-m-inline pf-m-info">
  <div class="pf-v5-c-alert__icon"></div>
  <div class="pf-v5-c-alert__title">Estimate only</div>
  <div class="pf-v5-c-alert__description">
    The most accurate sizing method is simulating target load with
    <code>kafka-producer-perf-test</code> and <code>kafka-consumer-perf-test</code>.
    See <a href="{{ '/verification.html' | relative_url }}">Verification</a>.
  </div>
</div>
