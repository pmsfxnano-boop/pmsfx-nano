# Gorila Crypto

Repositorio dedicado exclusivamente al subsistema cuantitativo Crypto de Gorila.

## Alcance

El árbol activo contiene únicamente infraestructura de mercado cripto, investigación y validación Crypto, incluyendo:

- Binance y Kraken market data.
- Ledger prospectivo durable.
- Replay, calidad, PIT/OOS y gates de validación.
- Runtime de captura aislado y observabilidad.
- gorila_core compartido sólo cuando es dependencia directa de Crypto.

No contiene integraciones ajenas al dominio Crypto.

## Ejecución

La captura Binance utiliza:

    uvicorn gorila_crypto.app:app --host 0.0.0.0 --port $PORT

La dependencia de runtime está en requirements-gorila.txt.

## Regla de aislamiento

Cualquier nueva funcionalidad que no sea Crypto debe mantenerse fuera de este árbol.
