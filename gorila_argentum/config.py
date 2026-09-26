from dataclasses import dataclass
import os
from typing import Tuple

DEFAULT_SYMBOLS = tuple(s.strip() for s in os.getenv("GORILA_SYMBOLS","GGAL,BMA,BBAR,YPFD,PAMP,TGSU2,CEPU,TRAN,TXAR,ALUA,LOMA,BYMA,COME,IRSA,CRES,METR,TGNO4,VALO,SUPV").split(",") if s.strip())

@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL","")
    http_timeout_s: float = float(os.getenv("GORILA_HTTP_TIMEOUT_S","15"))
    batch_workers: int = int(os.getenv("GORILA_BATCH_WORKERS","8"))
    symbols: Tuple[str,...] = DEFAULT_SYMBOLS
    twelve_data_api_key: str = os.getenv("TWELVE_DATA_API_KEY","").strip()
    twelve_data_interval: str = os.getenv("TWELVE_DATA_INTERVAL","1day").strip()
    # Production macro path uses the USD-specific BCRA endpoint to avoid
    # parsing every currency and to make the stored observation semantically
    # unambiguous.
    bcra_url: str = os.getenv(
        "BCRA_FX_URL",
        "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/USD",
    ).strip()
    argentina_datos_fx_url: str = os.getenv("ARGENTINA_DATOS_FX_URL","https://api.argentinadatos.com/v1/cotizaciones/dolares").strip()
    argentina_datos_risk_url: str = os.getenv("ARGENTINA_DATOS_RISK_URL","https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais").strip()
    byma_url: str = os.getenv("BYMA_MARKET_DATA_URL","").strip()
    core_symbols: Tuple[str,...] = ("GGAL","BMA","YPFD","PAMP","TGSU2","CEPU")

settings = Settings()
