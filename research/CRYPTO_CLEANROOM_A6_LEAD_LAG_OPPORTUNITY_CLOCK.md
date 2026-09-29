# Gorila Crypto Cleanroom — A6 Lead/Lag + Opportunity Clock Shadow

Date: 2026-09-29
Status: IMPLEMENTED
Branch: gorila-crypto-cleanroom

## Research objective

A6 turns the replayable event ledger into a descriptive market-microstructure
laboratory without turning the results into forecasts or trading signals.

The engine separates two clocks:

1. Market clock:
   target event_time minus leader event_time.

2. Information clock:
   target received_time minus leader received_time.

The second clock is the one relevant to what the live system could actually have
observed after detecting the leader event.

## Lead/Lag engine

The current research layer:
- consumes only immutable ledger rows;
- extracts trade prices from Binance trade events;
- requires the leader shock's historical reference to have been received no later
  than the leader event detection time;
- computes target response at explicitly configured delays;
- reports descriptive mean/median signed response, correlation, hit rate and
  median market/information lag;
- scans ordered symbol pairs only, never self-pairs.

The current default parameters are research defaults, not fitted hyperparameters:
- lookback: 1 second;
- shock threshold: 5 bps;
- response delays: 100, 250, 500, 1000, 2000, 5000 ms;
- response horizon: 10 seconds;
- reaction threshold: 2 bps;
- convergence fraction: 70 percent of leader shock.

These parameters are hypotheses and must not be optimized on the same OOS sample
used to evaluate economic value.

## Opportunity Clock

For each leader impulse and target pair the shadow engine records:
- detection event time;
- detection receive time;
- target baseline available at detection;
- first reaction;
- convergence;
- market-time lags;
- information-time lags;
- maximum favorable excursion before convergence/expiry;
- maximum adverse excursion before convergence/expiry;
- terminal state:
  - CONVERGED
  - PARTIALLY_RESOLVED
  - EXPIRED

The lifecycle is therefore:

DETECTED -> OPEN -> PARTIALLY_RESOLVED -> CONVERGED
                                                   -> EXPIRED

These states are research observations, not orders.

## Statistical safety

A6 does not produce:
- P(UP);
- forecast scores;
- trade signals;
- execution instructions;
- model promotion;
- claims of predictive alpha.

Trade-level observations can be strongly dependent because successive market events
overlap in time. Consequently A6 descriptive statistics are not treated as
independent-sample significance tests. Later OOS validation must use event/session
blocking, overlap-aware resampling and explicit transaction costs/slippage.

## Data integrity assumptions

The current Binance Spot market streams expose:
- trade event time E, trade time T and trade identifier fields on aggregate-trade
  messages;
- best bid/ask plus order-book updateId on bookTicker;
- diff-depth event time E and update interval U/u at 100ms or 1000ms.

The current adapter preserves these distinctions rather than manufacturing a
provider timestamp for bookTicker.

## Persistence

A6 adds:
- crypto_lead_lag_shadow
- crypto_opportunity_shadow

Both are keyed to the exact replay fingerprint, allowing later evidence to bind
the descriptive analysis to a precise immutable data slice.

## Safety gate

A6 is shadow-only. No background worker is started and the Crypto application
does not expose any forecast or execution endpoint.

## Next gate

A7 — forecast integration, but only after the shadow engine has sufficient replay
data and passes integrity/OOS design review. No automatic promotion is permitted.
