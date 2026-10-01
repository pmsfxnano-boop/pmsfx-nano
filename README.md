# Cryptonita

Repositorio dedicado exclusivamente a infraestructura cuantitativa Crypto.

## Alcance

El árbol activo contiene:

- Binance y Kraken market data.
- Ledger prospectivo durable.
- Replay, calidad, PIT/OOS y gates de validación.
- Runtime de captura aislado y observabilidad.
- `gorila_core` únicamente cuando es dependencia directa de Crypto.

## Ejecución

La captura Binance utiliza:

    uvicorn gorila_crypto.app:app --host 0.0.0.0 --port $PORT

La dependencia de runtime está en `requirements-gorila.txt`.

## Regla de dominio

Toda nueva funcionalidad debe permanecer dentro del dominio Crypto y respetar los contratos de ingestión, evidencia y validación existentes.
