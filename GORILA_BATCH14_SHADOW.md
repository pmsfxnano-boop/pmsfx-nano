# Gorila Argentum — Batch 14: Shadow

Estado: IMPLEMENTADO + PERSISTENTE + CI VALIDADA.

## Flujo

prediction → shadow_predictions → horizon → observed price → shadow_outcomes → diagnostics

Cada registro conserva símbolo, versión, probabilidad, horizonte, régimen, entry price, feature hash y timestamps.

Settlement:
- dirección realizada;
- return_pct;
- accuracy;
- Brier;
- log-loss.

## Seguridad

- no hay broker;
- no hay order endpoint;
- no hay ejecución real;
- Shadow prediction queda bloqueado si el circuit breaker está HALTED;
- settlement exige alcanzar el horizonte;
- settlement due puede resolver usando observaciones persistidas con `event_time`.

## E2E

La rama está CI-validada. El E2E contra Render continúa pendiente de despliegue y persistencia Postgres real.
