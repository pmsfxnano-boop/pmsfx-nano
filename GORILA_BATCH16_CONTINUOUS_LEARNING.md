# Gorila Argentum — Batch 16: Continuous Learning

Fecha: 2026-09-26 UTC
Rama: gorila-argentum-v0-hardening

## Estado

IMPLEMENTADO COMO CICLO DE CANDIDATOS + PERSISTENCIA.

Batch 16 existe como ciclo reproducible:

observations → causal feature dataset → purged walk-forward → candidate fit → current-state score → learning_runs

## Dataset

Para cada símbolo se construyen features exclusivamente con observaciones anteriores al timestamp de cada muestra:

- retorno 1 período;
- retorno 3 períodos;
- retorno 5 períodos;
- volatilidad 5;
- volatilidad 20;
- z-score de 20.

La etiqueta mira únicamente hacia delante mediante horizon_days.

Cada dataset recibe un SHA-256 para detectar cambios de universo/datos.

## Validación

El candidato pasa por purged_walk_forward con purge igual al horizonte.

Criterio interno de candidato:

- OOS accuracy >= 0.55;
- Brier skill >= 0.

Un candidato puede ser rechazado sin afectar al predictor vigente.

## Invariantes

- Nunca se reemplaza automáticamente el modelo vigente.
- Todo candidato queda en learning_runs.
- La promoción continúa BLOCKED hasta Batch 15.
- La probabilidad final se calcula sobre el estado más reciente disponible, no sobre una fila que ya utiliza una etiqueta futura.
- No se mezclan labels futuros con features del mismo timestamp.

## API

- POST /api/learning/run?symbol=GGAL&horizon_days=5
- GET /api/learning?symbol=GGAL&limit=20

## Operación

El ciclo está preparado para ejecución recurrente por scheduler/cron. La rama contiene el código y los tests, pero el runtime Render todavía no se ha verificado con estos commits.

La habilitación de Continuous Learning no implica promoción ni trading.