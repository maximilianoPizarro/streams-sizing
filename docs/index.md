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
  <div class="streams-wizard">
    <ol class="streams-wizard__nav" id="wizard-nav" role="list"></ol>
    <div class="streams-wizard__body" id="wizard-body"></div>
    <footer class="streams-wizard__footer">
      <button type="button" class="streams-btn streams-btn--secondary" id="btn-back" disabled>Back</button>
      <button type="button" class="streams-btn streams-btn--primary" id="btn-next">Next</button>
    </footer>
  </div>
</div>

<div class="streams-disclaimer">
  <p class="streams-disclaimer__title">Estimate only</p>
  <p class="streams-disclaimer__text">
    The most accurate sizing method is simulating target load with
    <code>kafka-producer-perf-test</code> and <code>kafka-consumer-perf-test</code>.
    See <a href="{{ '/verification.html' | relative_url }}">Verification</a>.
  </p>
</div>
