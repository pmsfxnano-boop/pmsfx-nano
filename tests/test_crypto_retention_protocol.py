from dataclasses import replace

import pytest

from gorila_crypto.config import BINANCE_PROSPECTIVE_RETENTION_HOURS, settings


def test_binance_defaults_cover_full_prospective_cohort():
    assert BINANCE_PROSPECTIVE_RETENTION_HOURS == 168
    assert settings.provider == "binance"
    assert settings.retention_trade_hours >= BINANCE_PROSPECTIVE_RETENTION_HOURS
    assert settings.retention_bookticker_hours >= BINANCE_PROSPECTIVE_RETENTION_HOURS


def test_binance_retention_cannot_be_shorter_than_protocol_cohort():
    invalid = replace(
        settings,
        provider="binance",
        retention_trade_hours=48.0,
        retention_bookticker_hours=168.0,
    )
    with pytest.raises(ValueError, match="7-day prospective cohort"):
        invalid.validate()

    invalid = replace(
        settings,
        provider="binance",
        retention_trade_hours=168.0,
        retention_bookticker_hours=24.0,
    )
    with pytest.raises(ValueError, match="7-day prospective cohort"):
        invalid.validate()
