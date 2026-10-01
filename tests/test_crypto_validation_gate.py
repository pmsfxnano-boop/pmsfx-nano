from __future__ import annotations

import pytest

from gorila_crypto.validation import QualityGateBlocked, require_quality_gate


def test_quality_gate_blocks_failed_quality_report() -> None:
    with pytest.raises(QualityGateBlocked, match="quality_gate_status=FAIL"):
        require_quality_gate(
            {
                "status": "FAIL",
                "rows": 100000,
                "reasons": ("INTEGRITY_UNVERIFIED_OR_FAILED:bookUpdate:1 non-verified/0 verified",),
            },
            minimum_rows=1000,
        )


def test_quality_gate_blocks_short_replay_even_when_quality_passes() -> None:
    with pytest.raises(QualityGateBlocked, match="below required minimum"):
        require_quality_gate(
            {"status": "PASS", "rows": 999, "reasons": ()},
            minimum_rows=1000,
        )


def test_quality_gate_allows_explicitly_passed_replay() -> None:
    require_quality_gate(
        {"status": "PASS", "rows": 1000, "reasons": ()},
        minimum_rows=1000,
    )


def test_quality_gate_blocks_replay_fingerprint_mismatch() -> None:
    with pytest.raises(QualityGateBlocked, match="replay_fingerprint_mismatch"):
        require_quality_gate(
            {
                "status": "PASS",
                "rows": 1000,
                "replay_fingerprint": "wrong",
                "reasons": (),
            },
            minimum_rows=1000,
            expected_replay_fingerprint="expected",
        )
