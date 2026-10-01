"""Architectural guardrails for the Cryptonita Crypto cleanroom."""
from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CRYPTO_ROOT = REPO_ROOT / "gorila_crypto"
ALLOWED_REPO_IMPORT_ROOTS = {"gorila_core", "gorila_crypto"}


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


def test_crypto_package_imports_only_approved_repo_domains() -> None:
    repo_packages = {
        path.name
        for path in REPO_ROOT.iterdir()
        if path.is_dir() and (path / "__init__.py").exists()
    }
    repo_violations: list[tuple[str, str]] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for root in sorted(_import_roots(tree) & repo_packages - ALLOWED_REPO_IMPORT_ROOTS):
            repo_violations.append((path.relative_to(REPO_ROOT).as_posix(), root))
    assert not repo_violations, repo_violations


def test_crypto_app_routes_are_domain_scoped() -> None:
    module = importlib.import_module("gorila_crypto.app")
    routes = {route.path for route in module.app.routes}
    assert "/api/crypto/health" in routes
    assert "/api/crypto/config" in routes
    assert all(path == "/" or path.startswith("/api/crypto/") for path in routes)


def test_importing_crypto_app_does_not_load_unapproved_repo_domains() -> None:
    code = """
import sys
import gorila_crypto.app

allowed = {{"gorila_core", "gorila_crypto"}}
loaded = {{
    name.split(".")[0]
    for name in sys.modules
    if name.split(".")[0] in {{
        "gorila_core", "gorila_crypto"
    }}
}}
unexpected = loaded - allowed
if unexpected:
    raise SystemExit("UNAPPROVED_REPO_MODULES:" + ",".join(sorted(unexpected)))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_crypto_app_capture_is_opt_in_by_default() -> None:
    module = importlib.import_module("gorila_crypto.app")
    assert module.settings.ingest_enabled is False
    assert module.settings.environment == "cleanroom"
