# Gorila Argentum — Batch 13: Control Room

Estado: IMPLEMENTADO + PERSISTENTE + CI VALIDADA.

## Estado operativo expuesto

- runtime mode;
- backend efectivo y durabilidad;
- promotion gate;
- source health;
- data drift;
- prediction drift;
- realized-vs-predicted drift;
- Shadow ledger;
- learning;
- recalibration;
- circuit breaker.

## Circuit breaker de investigación

Estados:
- `NORMAL`
- `DEGRADED`
- `HALTED`

HALTED cuando existe:
- almacenamiento no durable;
- data drift ALERT;
- source health crítico;
- prediction/outcome drift ALERT.

Cuando está HALTED, el endpoint de creación de predicciones Shadow rechaza nuevas predicciones con HTTP 409 y conserva los motivos.

Esto no es un mecanismo de trading. Es una barrera operacional para el runtime de investigación.

## Promoción

La Promotion Gate sigue separada del circuit breaker y permanece bloqueada con la evidencia V2 vigente.
