import pytest

from gorila_argentum.security import require_internal_key


def test_internal_key_disabled_allows_requests(monkeypatch):
    monkeypatch.delenv("GORILA_INTERNAL_API_KEY", raising=False)
    require_internal_key(None)


def test_internal_key_rejects_missing_or_wrong_key(monkeypatch):
    monkeypatch.setenv("GORILA_INTERNAL_API_KEY", "secret")
    with pytest.raises(Exception) as missing:
        require_internal_key(None)
    assert getattr(missing.value, "status_code", None) == 401
    with pytest.raises(Exception) as wrong:
        require_internal_key("wrong")
    assert getattr(wrong.value, "status_code", None) == 401


def test_internal_key_accepts_matching_key(monkeypatch):
    monkeypatch.setenv("GORILA_INTERNAL_API_KEY", "secret")
    require_internal_key("secret")
