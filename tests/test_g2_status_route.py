from gorila_argentum.app import gorila_g2_h10_status


def test_g2_h10_status_route_is_registered_and_non_serving():
    payload = gorila_g2_h10_status()
    assert payload["status"] == "REGISTERED"
    assert payload["model_id"] == "G2_PIT_FIXED_C0.25_H10"
    assert payload["promotion"] == "BLOCKED"
    assert payload["runtime_serving"] == "DISABLED_UNTIL_EXACT_PACKAGE_VERIFIED"
    assert payload["research_only"] is True
    assert payload["no_execution_authority"] is True
