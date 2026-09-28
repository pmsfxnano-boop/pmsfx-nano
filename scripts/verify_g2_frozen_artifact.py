from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

EXPECTED_SHA256 = "aa3985e72c9af6009bd7e04da9372084025f647f7c1c1e1d46e8cbd48b29df6c"
EXPECTED_SNAPSHOT_SHA256 = "4f5274af78f6de30d8854ae80d6a9e62b2e6cfcd9eeea31c76ab2e8d31949946"
EXPECTED_SYMBOLS = ["BMA", "CEPU", "GGAL", "PAMP", "TGSU2", "YPFD"]
EXPECTED_FEATURE_COUNT = 12
EXPECTED_EXPANDED_FEATURE_COUNT = 60
EXPECTED_HORIZON = 10
EXPECTED_C = 0.25


def verify(path: Path) -> dict:
    raw = path.read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()
    if sha256 != EXPECTED_SHA256:
        raise ValueError(f"SHA256 mismatch: {sha256} != {EXPECTED_SHA256}")

    with np.load(path, allow_pickle=False) as package:
        required = {
            "symbols",
            "feature_names",
            "reg_names",
            "reg_mu",
            "reg_sd",
            "scaler_mean",
            "scaler_scale",
            "coef",
            "intercept",
            "Sigma",
            "beta",
            "train_idx",
            "pair_i",
            "pair_j",
            "H",
            "C",
        }
        missing = sorted(required - set(package.files))
        if missing:
            raise ValueError(f"missing arrays: {missing}")

        symbols = package["symbols"].tolist()
        feature_names = package["feature_names"].tolist()
        if symbols != EXPECTED_SYMBOLS:
            raise ValueError(f"symbol order mismatch: {symbols}")
        if len(feature_names) != EXPECTED_FEATURE_COUNT:
            raise ValueError(f"feature_count mismatch: {len(feature_names)}")
        if package["scaler_mean"].shape != (EXPECTED_EXPANDED_FEATURE_COUNT,):
            raise ValueError(f"scaler_mean shape mismatch: {package['scaler_mean'].shape}")
        if package["scaler_scale"].shape != (EXPECTED_EXPANDED_FEATURE_COUNT,):
            raise ValueError(f"scaler_scale shape mismatch: {package['scaler_scale'].shape}")
        if package["coef"].shape != (1, EXPECTED_EXPANDED_FEATURE_COUNT):
            raise ValueError(f"coef shape mismatch: {package['coef'].shape}")
        if package["Sigma"].shape != (6, 6):
            raise ValueError(f"Sigma shape mismatch: {package['Sigma'].shape}")
        if package["pair_i"].shape != (15,) or package["pair_j"].shape != (15,):
            raise ValueError("pair index shape mismatch")
        if int(package["H"][0]) != EXPECTED_HORIZON:
            raise ValueError(f"H mismatch: {package['H']}")
        if abs(float(package["C"][0]) - EXPECTED_C) > 1e-15:
            raise ValueError(f"C mismatch: {package['C']}")
        if not np.isfinite(package["coef"]).all():
            raise ValueError("coef contains non-finite values")
        if not np.isfinite(package["scaler_mean"]).all():
            raise ValueError("scaler_mean contains non-finite values")
        if not np.isfinite(package["scaler_scale"]).all():
            raise ValueError("scaler_scale contains non-finite values")
        if not np.all(package["scaler_scale"] > 0):
            raise ValueError("scaler_scale must be strictly positive")
        if not np.isfinite(package["Sigma"]).all():
            raise ValueError("Sigma contains non-finite values")

    return {
        "status": "VERIFIED",
        "sha256": sha256,
        "bytes": len(raw),
        "snapshot_sha256": EXPECTED_SNAPSHOT_SHA256,
        "symbols": EXPECTED_SYMBOLS,
        "feature_count": EXPECTED_FEATURE_COUNT,
        "expanded_feature_count": EXPECTED_EXPANDED_FEATURE_COUNT,
        "horizon": EXPECTED_HORIZON,
        "C": EXPECTED_C,
        "runtime_serving": "DISABLED_PENDING_SCIENTIFIC_SCORER_INTEGRATION",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.path), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
