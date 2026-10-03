from gorila_crypto.execution import ExecutionCostModel, execution_cost_bps
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.research_contract import research_truth_decision


def test_current_v6_capture_is_not_research_lossless() -> None:
    decision = research_truth_decision(
        PREREGISTERED_CRYPTO_PROTOCOL,
        horizon_ms=5000,
    )
    assert decision.status == "BLOCKED"
    assert not decision.exact_trade_coverage
    assert any(reason.startswith("RAW_TRADE_LEDGER_NOT_LOSSLESS") for reason in decision.reasons)
    assert len(decision.contract_hash) == 64


def test_execution_proxy_is_explicitly_non_validated_and_spread_aware() -> None:
    model = ExecutionCostModel()
    assert model.validated_execution_model is False
    assert execution_cost_bps({"target_spread_bps": 3.0}, model) == 5.0
    assert len(model.model_hash) == 64
