# Gorila Argentum — Batch 11: Drift

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`
Estado: IMPLEMENTADO + PERSISTENTE + CI VALIDADO
Promoción predictiva: NO
Modo runtime conocido: RESEARCH

## Continuidad

El plan de producción recuperado ubica **Batch 11 — Drift** inmediatamente después de **Batch 10 — Outcome Engine**. Batch 10 permanece cerrado y su compuerta estadística del predictor sigue bloqueada.

Este documento registra únicamente trabajo realmente realizado en esta rama. No redefine ni inventa etapas históricas.

## Implementación ejecutada

Se añadió `gorila_argentum/drift.py` con:

- limpieza de valores finitos;
- PSI (Population Stability Index) con bins definidos desde la referencia;
- estadístico KS sin dependencia externa;
- desplazamiento de media estandarizado;
- razón de desviaciones estándar;
- clasificación `OK / WARN / ALERT`;
- evaluación de ventanas rodantes referencia vs. actual.

Se añadió el endpoint:

`GET /api/drift/{symbol}/{field}?current_size=30&reference_size=90`

También se añadió `GET /api/drift?limit=100` para leer snapshots persistidos.

Cada ingestión calcula Drift sobre los seis símbolos core (`GGAL,BMA,YPFD,PAMP,TGSU2,CEPU`) con referencia de 90 observaciones y ventana actual de 30, y persiste el resultado en `drift_snapshots`.

El endpoint de cálculo directo no modifica la lógica del predictor V0.

## Pruebas

Se añadió `tests/test_drift.py` cubriendo:

1. distribución idéntica -> sin alerta;
2. desplazamiento fuerte -> `ALERT`;
3. datos insuficientes -> `INSUFFICIENT_DATA`;
4. referencia constante con cambio de nivel -> `ALERT`.

La CI `Gorila Argentum Hardening CI` del commit `3dfe7e582657832add5b33a0d4729ed3937d5c00` terminó en **success**:

- Unit tests: success.
- Compile research scripts: success.

## Corrección durante la ejecución

La primera fixture de “sin drift” era defectuosa porque comparaba ventanas con composiciones diferentes y producía un falso `WARN`. Se descartó esa fixture y se sustituyó por dos muestras con distribución idéntica. La nueva prueba pasó.

## Alcance científico real

Este Batch 11 implementa **data/distribution drift** sobre series almacenadas.

Todavía no se declara implementado aquí un monitor completo de:

- prediction drift persistido;
- realized-vs-predicted drift;
- recalibración automática;
- kill-switch automático.

No se inventan esos resultados. Quedan como trabajo posterior de las capas de monitorización/control.

## Runtime

El servicio Render existente `gorila-argentum-research` sigue configurado sobre `gorila-argentum-v0-hardening`, pero el último deploy verificable es el commit anterior `2a8aa06a1be9951da0e95a810b68e782b2b4d393`.

El intento de disparar el nuevo deploy quedó bloqueado por la herramienta de Render al exigir selección explícita de workspace. Por lo tanto:

**No se declara el endpoint de Drift como desplegado y probado en producción.**

La evidencia actual de validación del nuevo código es la CI del repositorio. La prueba incluye persistencia SQLite de snapshots y cobertura del dashboard.

## Regla de promoción

El drift monitor es infraestructura de diagnóstico. No habilita por sí mismo ninguna promoción del predictor. El predictor de ranking permanece bloqueado por la evidencia V2 de Batch 10.
