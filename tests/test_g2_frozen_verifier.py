import numpy as np

from scripts.verify_g2_frozen_artifact import (
    EXPECTED_C,
    EXPECTED_EXPANDED_FEATURE_COUNT,
    EXPECTED_HORIZON,
    EXPECTED_SYMBOLS,
    _validate_package_arrays,
)


def test_g2_frozen_structure_verifier_accepts_expected_shapes(tmp_path):
    path = tmp_path / "synthetic_g2.npz"
    np.savez(
        path,
        symbols=np.array(EXPECTED_SYMBOLS),
        feature_names=np.array([f"f{i}" for i in range(12)]),
        reg_names=np.array(["market20", "market60", "dispersion20", "marketvol20"]),
        reg_mu=np.ones(4),
        reg_sd=np.ones(4),
        scaler_mean=np.zeros(EXPECTED_EXPANDED_FEATURE_COUNT),
        scaler_scale=np.ones(EXPECTED_EXPANDED_FEATURE_COUNT),
        coef=np.zeros((1, EXPECTED_EXPANDED_FEATURE_COUNT)),
        intercept=np.array([0.0]),
        Sigma=np.eye(6),
        beta=np.array([0.0]),
        train_idx=np.arange(1216),
        pair_i=np.array([0,0,0,0,0,1,1,1,1,2,2,2,3,3,4]),
        pair_j=np.array([1,2,3,4,5,2,3,4,5,3,4,5,4,5,5]),
        H=np.array([EXPECTED_HORIZON]),
        C=np.array([EXPECTED_C]),
    )

    with np.load(path, allow_pickle=False) as package:
        result = _validate_package_arrays(package)

    assert result["symbols"] == EXPECTED_SYMBOLS
    assert result["feature_count"] == 12
    assert result["expanded_feature_count"] == EXPECTED_EXPANDED_FEATURE_COUNT
    assert result["horizon"] == EXPECTED_HORIZON
    assert result["C"] == EXPECTED_C
