from __future__ import annotations

import argparse
import json

from gorila_argentum.config import settings
from gorila_argentum.learning import run_learning_cycle


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Gorila Argentum continuous-learning candidates.")
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--horizon-days", type=int, default=5)
    args = parser.parse_args()

    symbols = args.symbols or list(settings.core_symbols)
    results = [
        run_learning_cycle(symbol, horizon_days=max(1, min(20, args.horizon_days)))
        for symbol in symbols
    ]
    print(json.dumps(results, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())