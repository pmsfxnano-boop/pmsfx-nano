# Gorila Argentum — Batch 11: Drift

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`
Estado: IMPLEMENTADO + PERSISTENTE + CI VALIDADO

## Capas implementadas

### Data/distribution drift
PSI, KS, mean-shift z y std-ratio sobre ventanas referencia/actual, con estados `OK/WARN/ALERT/INSUFFICIENT_DATA`.

### Prediction drift
Las predicciones Shadow asentadas se monitorizan sobre `probability_up` mediante la misma lógica de ventanas.

### Realized-vs-predicted drift
Se calcula:
- prediction gap referencia vs. actual;
- delta de Brier;
- severidad `OK/WARN/ALERT`.

Los diagnósticos quedan expuestos dentro de Control Room y Audit.

## Persistencia

Los snapshots de data drift se almacenan en `drift_snapshots`. El Shadow ledger alimenta los diagnósticos de prediction/outcome drift.

## Gate operativo

Un `ALERT` crítico produce `HALTED` en el circuit breaker de investigación. Drift no promueve modelos y no ejecuta trading.

## Evidencia

El checkpoint `aec9102f…` tiene Hardening CI en **success**. Render E2E aún no está declarado.
