"""Architectural guardrails for the Crypto cleanroom.

These tests are intentionally static and dependency-focused. They are meant to
fail before runtime isolation can silently regress.
"""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CRYPTO_ROOT = REPO_ROOT / "gorila_crypto"

FORBIDDEN_TOP_LEVEL_IMPORTS = {
    "gorila_argentum",
    "scripts",
    "main",
    "quant",
}

FORBIDDEN_TEXT = (
    "BCRA",
    "BYMA",
    "ArgentinaDatos",
    "Rava",
    "Twelve Data",
    "TwelveData",
    "Yahoo",
    "Tiingo",
    "America/Argentina",
    "America/New_York",
    "GGAL",
    "BMA",
    "YPFD",
    "PAMP",
    "TGSU2",
    "CEPU",
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
)


def _python_files() -> list[Path]:
    return sorted(CRYPTO_ROOT.rglob("*.py"))


def _import_roots(tree: ast.AST) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_crypto_package_has_no_forbidden_import_roots() -> None:
    violations: list[tuple[str, str]] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for root in sorted(_import_roots(tree) & FORBIDDEN_TOP_LEVEL_IMPORTS):
            violations.append((path.relative_to(REPO_ROOT).as_posix(), root))
    assert not violations, violations


def test_crypto_package_has_no_legacy_market_text() -> None:
    violations: list[tuple[str, str]] = []
    for path in _python_files():
        text = path.read_text(encoding="utf-8")
        for needle in FORBIDDEN_TEXT:
            if needle.lower() in text.lower():
                violations.append((path.relative_to(REPO_ROOT).as_posix(), needle))
    assert not violations, violations


def test_crypto_app_routes_are_domain_scoped() -> None:
    module = importlib.import_module("gorila_crypto.app")
    routes = {route.path for route in module.app.routes}
    assert "/api/crypto/health" in routes
    assert "/api/crypto/config" in routes
    assert all(path == "/" or path.startswith("/api/crypto/") for path in routes)


def test_importing_crypto_app_does_not_load_legacy_domains() -> None:
    forbidden_loaded = FORBIDDEN_TOP_LEVEL_IMPORTS & set(sys.modules)
    assert not forbidden_loaded, sorted(forbidden_loaded)


def test_crypto_app_capture_is_opt_in_by_default() -> None:
    module = importlib.import_module("gorila_crypto.app")
    assert module.settings.ingest_enabled is False
    assert module.settings.environment == "cleanroom"
