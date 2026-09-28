from __future__ import annotations

import hashlib
import itertools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from gorila_argentum.canonical_data import canonical_daily_series
from gorila_argentum.storage import Store
from scripts.verify_g2_frozen_artifact import _validate_package_arrays, EXPECTED_SHA256


SYMBOLS = ("BMA", "CEPU", "GGAL", "PAMP", "TGSU2", "YPFD")
BASE_FEATURE_NAMES = (
    "momentum_3",
    "momentum_5",
    "momentum_20",
    "momentum_60",
    "momentum_120",
    "reversal_2",
    "vol_20",
    "vol_60",
    "momentum20_over_vol20",
    "momentum60_over_vol60",
    "momentum20_minus_momentum60_div3",
    "drawdown_120",
)
REGIME_NAMES = ("market20", "market60", "dispersion20", "marketvol20")
HORIZON_DAYS = 10
INTERACTION_BLOCKS = 4
EXPANDED_FEATURE_COUNT = len(BASE_FEATURE_NAMES) * (1 + INTERACTION_BLOCKS)


def _sigmoid(x: np.ndarray) -> np.ndarray:
    clipped = np.clip(x, -60.0, 60.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _shift(values: np.ndarray, periods: int) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=np.float64)
    out[periods:] = values[:-periods]
    return out


def _rolling_std(values: np.ndarray, window: int) -> np.ndarray:
    # Match pandas.Series.rolling(window).std() exactly: the first valid
    # window ends at index window-1 and uses sample standard deviation (ddof=1).
    out = np.full(values.shape, np.nan, dtype=np.float64)
    for t in range(window - 1, len(values)):
        sample = values[t - window + 1 : t + 1]
        if np.isfinite(sample).all():
            out[t] = float(np.std(sample, ddof=1))
    return out


def _rolling_max(values: np.ndarray, window: int) -> np.ndarray:
    out = np.full(values.shape, np.nan, dtype=np.float64)
    for t in range(window - 1, len(values)):
        sample = values[t - window + 1 : t + 1]
        if np.isfinite(sample).all():
            out[t] = float(np.max(sample))
    return out


def _feature_panel(prices: np.ndarray) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    logp = np.log(prices)
    ret = np.empty_like(logp)
    ret[0] = np.nan
    ret[1:] = np.diff(logp, axis=0)

    raw: list[np.ndarray] = []
    for horizon in (3, 5, 20, 60, 120):
        raw.append(logp - _shift(logp, horizon))

    raw.append(- (logp - _shift(logp, 2)))

    vol20 = np.column_stack([
        _rolling_std(ret[:, j], 20) for j in range(ret.shape[1])
    ])
    vol60 = np.column_stack([
        _rolling_std(ret[:, j], 60) for j in range(ret.shape[1])
    ])
    raw.extend([
        vol20,
        vol60,
        raw[2] / vol20,
        raw[3] / vol60,
        raw[2] - raw[3] / 3.0,
        np.exp(logp) / _rolling_max(np.exp(logp), 120) - 1.0,
    ])

    feature_names = list(BASE_FEATURE_NAMES)
    z0 = np.full((prices.shape[0], prices.shape[1], len(raw)), np.nan, dtype=np.float64)
    for feature_index, values in enumerate(raw):
        row_mean = np.nanmean(values, axis=1)
        row_std = np.nanstd(values, axis=1)
        row_std[row_std == 0] = np.nan
        z0[:, :, feature_index] = (
            values - row_mean[:, None]
        ) / row_std[:, None]

    regime_raw = np.column_stack([
        np.nanmean(raw[2], axis=1),
        np.nanmean(raw[3], axis=1),
        np.nanstd(raw[2], axis=1),
        np.nanmean(vol20, axis=1),
    ])

    return z0, regime_raw, feature_names, list(REGIME_NAMES)


@dataclass(frozen=True)
class G2FrozenModel:
    path: Path
    sha256: str
    symbols: tuple[str, ...]
    feature_names: tuple[str, ...]
    reg_mu: np.ndarray
    reg_sd: np.ndarray
    scaler_mean: np.ndarray
    scaler_scale: np.ndarray
    coef: np.ndarray
    intercept: float
    beta: float

    @classmethod
    def load(cls, path: str | os.PathLike[str], *, verify_hash: bool = True) -> "G2FrozenModel":
        artifact_path = Path(path)
        raw = artifact_path.read_bytes()
        sha256 = hashlib.sha256(raw).hexdigest()
        if verify_hash and sha256 != EXPECTED_SHA256:
            raise ValueError(
                f"G2 artifact SHA256 mismatch: {sha256} != {EXPECTED_SHA256}"
            )
        with np.load(artifact_path, allow_pickle=False) as package:
            _validate_package_arrays(package)
            return cls(
                path=artifact_path,
                sha256=sha256,
                symbols=tuple(package["symbols"].tolist()),
                feature_names=tuple(package["feature_names"].tolist()),
                reg_mu=package["reg_mu"].astype(np.float64),
                reg_sd=package["reg_sd"].astype(np.float64),
                scaler_mean=package["scaler_mean"].astype(np.float64),
                scaler_scale=package["scaler_scale"].astype(np.float64),
                coef=package["coef"].reshape(-1).astype(np.float64),
                intercept=float(package["intercept"][0]),
                beta=float(package["beta"][0]),
            )


def load_g2_frozen_model_from_env() -> G2FrozenModel:
    path = os.getenv(
        "GORILA_G2_ARTIFACT_PATH",
        "/mnt/data/gorila_work/block_r_live/block_r_prospective_freeze/frozen_g2_h10_arrays.npz",
    )
    return G2FrozenModel.load(path, verify_hash=True)


def score_g2_h10(store: Store, model: G2FrozenModel, *, limit: int = 2500) -> dict[str, Any]:
    series_by_symbol: dict[str, dict[str, float]] = {}
    for symbol in SYMBOLS:
        rows = canonical_daily_series(store, symbol, "close", limit=limit)
        series_by_symbol[symbol] = {str(day): float(value) for day, value in rows}

    common_dates = sorted(set.intersection(*(set(series_by_symbol[s]) for s in SYMBOLS)))
    if len(common_dates) < 121:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "G2_REQUIRES_121_COMMON_DAILY_SESSIONS",
            "common_dates": len(common_dates),
            "model_id": "G2_PIT_FIXED_C0.25_H10",
            "data_fabric": "CANONICAL_DAILY_V1",
            "research_only": True,
            "no_execution_authority": True,
        }

    prices = np.asarray(
        [[series_by_symbol[s][day] for s in SYMBOLS] for day in common_dates],
        dtype=np.float64,
    )
    if (prices <= 0).any() or not np.isfinite(prices).all():
        return {
            "status": "INVALID_DATA",
            "reason": "NON_POSITIVE_OR_NONFINITE_CANONICAL_CLOSE",
            "common_dates": len(common_dates),
            "research_only": True,
            "no_execution_authority": True,
        }

    z0, regime_raw, _, _ = _feature_panel(prices)
    rg = np.nan_to_num(
        (regime_raw - model.reg_mu) / model.reg_sd,
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    t = len(common_dates) - 1
    z = np.full(
        (prices.shape[0], prices.shape[1], EXPANDED_FEATURE_COUNT),
        np.nan,
        dtype=np.float64,
    )
    feature_count = len(BASE_FEATURE_NAMES)
    z[:, :, :feature_count] = z0
    for q in range(INTERACTION_BLOCKS):
        z[:, :, feature_count * (q + 1) : feature_count * (q + 2)] = (
            z0 * rg[:, None, q, None]
        )

    latest = z[t]
    valid = ~np.isnan(latest).any(axis=1)
    scores = np.zeros(len(SYMBOLS), dtype=np.float64)
    probabilities = []
    pair_indices = list(itertools.combinations(range(len(SYMBOLS)), 2))

    for i, j in pair_indices:
        if not valid[i] or not valid[j]:
            continue
        delta = latest[i] - latest[j]
        x = (delta - model.scaler_mean) / model.scaler_scale
        probability = float(_sigmoid(np.asarray([model.intercept + float(x @ model.coef)]))[0])
        probabilities.append({
            "pair": [SYMBOLS[i], SYMBOLS[j]],
            "p_i_gt_j": probability,
        })
        scores[i] += probability - 0.5
        scores[j] += 0.5 - probability

    valid_scores = scores[valid]
    if valid_scores.size:
        scores[valid] -= float(valid_scores.mean())

    order = np.argsort(-scores)
    items = []
    for rank, idx in enumerate(order, start=1):
        items.append({
            "symbol": SYMBOLS[int(idx)],
            "score": float(scores[int(idx)]),
            "alpha10_frozen_units": float(model.beta * scores[int(idx)]),
            "rank": rank,
            "valid": bool(valid[int(idx)]),
        })

    return {
        "status": "READY",
        "model_id": "G2_PIT_FIXED_C0.25_H10",
        "model_sha256": model.sha256,
        "snapshot_sha256": "4f5274af78f6de30d8854ae80d6a9e62b2e6cfcd9eeea31c76ab2e8d31949946",
        "common_dates": len(common_dates),
        "latest_date": common_dates[-1],
        "horizon_days": HORIZON_DAYS,
        "C": 0.25,
        "data_fabric": "CANONICAL_DAILY_V1",
        "items": items,
        "pairwise_probabilities": probabilities,
        "research_only": True,
        "no_execution_authority": True,
        "runtime_serving": "DISABLED",
    }
