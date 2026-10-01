# A12 — Adaptive Opportunity Clock + Microstructure Intelligence

Status: ENGINEERED / NOT YET PRODUCTION-DEPLOYED
Branch: gorila-crypto-intelligence

## Objective

Upgrade A6 from a descriptive clock to a self-supervised reaction-time learning
system while preserving the scientific boundary between:

    capture -> PIT information set -> learning -> OOS evidence -> promotion -> execution

The learner never writes to the immutable event ledger and never changes the
Quality Gate.

## Intelligence model

The Opportunity Clock is modeled as a discrete-time survival process.

For a leader event and target pair, the learner estimates:

- conditional hazard of target reaction in each interval;
- cumulative probability of reaction by 100/250/500/1000/2000/5000 ms;
- survival probability through each horizon;
- expected reaction time.

This is richer than a binary directional model because the central object is the
time remaining in the opportunity.

A hierarchical learner combines:

- global market model across BTC/ETH/SOL;
- pair-specific residual model for BTC->ETH, ETH->BTC, etc.;
- exponentially adaptive normalization;
- diagonal-Adagrad online logistic updates.

Labels are self-generated only when the future target event becomes observable.
Unresolved opportunities are right-censored at 5 seconds.

## Microstructure feature stack

The first feature contract uses only fields already present in the Crypto ledger:

- top-of-book spread;
- bid/ask depth imbalance;
- microprice displacement;
- aggressor-signed trade flow;
- trade intensity;
- realized short-horizon volatility;
- transport age;
- leader shock magnitude;
- leader-vs-target differences.

When available, depthUpdate is also parsed. The system does not pretend
bookTicker is a full L2 book.

## Temporal correctness

The intelligence runner consumes events in immutable ledger_seq order.
Features for a decision use only information already received at the detection
time. The learner therefore has no access to the target reaction until that
reaction arrives in the ledger.

A one-second leader shock is measured against the last trade available at least
one second earlier in receive time. It is not derived from event-time ordering.

Warm restart behavior:

1. restore the last learner state;
2. replay a bounded recent ledger window with learning disabled;
3. rebuild microstructure state;
4. continue from the last committed ledger sequence;
5. preserve pending opportunities through the serialized learner state.

## Self-learning loop

    live ledger
      -> microstructure state
      -> leader shock
      -> opportunity detected
      -> target response clock starts
      -> reaction / censoring
      -> delayed online update
      -> updated hazard surface
      -> next opportunity

This is online adaptation, not uncontrolled self-modification. Parameters,
feature schema and model version remain versioned and persisted.

## Persistence

The new worker persists:

- model state;
- last processed ledger sequence;
- active capture session;
- pending opportunities;
- every training example with exact opportunity id;
- replay fingerprint lineage.

A unique opportunity id makes training-event application idempotent.

## Deployment boundary

The learner is intentionally a separate Render service. A learner code deploy
must not restart gorila-crypto-cleanroom-capture and therefore must not reset the
7-day prospective cohort.

render-crypto-intelligence.yaml links the worker to the same Postgres database.

## Promotion boundary

Nothing in A12 authorizes:

- production forecast promotion;
- execution;
- automatic model replacement of the preregistered research model;
- using future outcomes to rewrite historical features.

The learner first accumulates a substantial prospective sample. Only after
Quality Gate, walk-forward/OOS, calibration, cost/slippage and stress gates can
any candidate be considered for promotion.

## Next engineering extension

The next microstructure expansion is to add normalized 100 ms/1 s depth-event
features and an event-intensity state, then test whether the additional
microstructure information adds incremental predictive information after PIT
controls and costs.
