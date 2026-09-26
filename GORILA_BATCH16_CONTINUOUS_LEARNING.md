# Gorila Argentum — Batch 16: Continuous Learning

Estado: IMPLEMENTADO COMO CICLO DE CANDIDATOS + PERSISTENCIA.

## Ciclo

observations → causal feature dataset → purged walk-forward → candidate fit → current-state score → learning_runs

Features:
- r1
- r3
- r5
- vol5
- vol20
- z20

Las labels miran sólo hacia delante por horizon_days. Cada dataset recibe SHA-256.

## Recalibración

Se añadió `gorila_argentum/calibration.py` con:
- calibración logit-intercept;
- solución por bisección acotada;
- evaluación Brier/log-loss/prediction gap;
- candidate gate;
- persistencia en `calibration_runs`.

La aplicación automática está deshabilitada y exige Promotion + durability.

## Runtime tick

`scripts/gorila_runtime_tick.py` ejecuta, con Postgres:
1. learning para los símbolos core;
2. settlement Shadow due;
3. Promotion evaluation;
4. recalibración candidata;
5. Audit snapshot.

El tick falla cerrado con `durable_storage_required` si no hay Postgres.

## Invariantes

- no reemplazo automático del modelo;
- no trading;
- no relajación del Promotion Gate;
- no reutilización de labels futuras;
- candidatos persistidos;
- trazabilidad mediante hashes/audit.

## Operación pendiente

El runtime tick está listo para Render Cron. No se declara activo hasta que el servicio Render tenga el nuevo deploy, Postgres configurado y el workspace autorizado.
