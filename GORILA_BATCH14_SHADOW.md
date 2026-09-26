# Gorila Argentum — Batch 14: Shadow

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`

## Estado

**IMPLEMENTADO + PERSISTENTE + CI CONFIGURADA; EJECUCIÓN NO OBSERVADA INDEPENDIENTEMENTE.**

Batch 14 añade un ledger de investigación Shadow. No coloca órdenes, no conecta un broker y no modifica la compuerta de promoción.

### Continuidad con el trabajo previo

La rama ya contenía un especialista GGAL + riesgo soberano aislado (`ggal_shadow.py` / `ggal_shadow_app.py`, incorporado el 21-09-2026). El nuevo ledger es una capa genérica y persistente; no reemplaza ni borra ese especialista.

## Flujo ejecutado

`prediction → shadow_predictions → observed price → shadow_outcomes → summary`

Cada registro Shadow conserva:

- símbolo;
- versión de modelo;
- probabilidad UP;
- dirección;
- horizonte;
- régimen;
- precio de entrada;
- hash de features opcional;
- estado `OPEN/SETTLED`;
- timestamps.

El settlement calcula:

- dirección realizada;
- retorno porcentual;
- acierto direccional cuando existe movimiento;
- Brier score;
- log-loss.

Se añadieron:

- `POST /api/shadow/prediction`
- `POST /api/shadow/{prediction_id}/settle`
- `GET /api/shadow`
- `GET /api/shadow/summary`

El dashboard incorpora **SHADOW LEDGER — BATCH 14** y el Control Room identifica el ledger como implementado.

## Invariantes

1. El endpoint se limita a persistir y evaluar señales Shadow.
2. No existe endpoint de orden ni ejecución.
3. El predictor productivo sigue bloqueado.
4. Un prediction solo puede cerrarse una vez.
5. `observed_at` debe alcanzar el horizonte de la predicción; el ledger rechaza settlement prematuro.

## Validación

Se añadió `tests/test_shadow.py` para validación de inputs, métricas y roundtrip SQLite de predicción → settlement → summary.

La CI del repositorio fue ampliada para ejecutar este test junto al conjunto de hardening. En esta sesión no se cuenta con una ejecución de CI observable desde el conector, por lo que no se declara el test como ejecutado.

## No declarado como cerrado

Render todavía no contiene estos commits. Por lo tanto, no se declara E2E en el servicio desplegado.

Tampoco se afirma que exista ejecución real de mercado, broker connectivity, paper orders o promoción automática.
