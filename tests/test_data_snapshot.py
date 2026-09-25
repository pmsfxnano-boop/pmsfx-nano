from pathlib import Path

from research.gorila_data_snapshot import canonical_hash, load_or_fetch_series, write_snapshot


def test_snapshot_roundtrip(tmp_path: Path):
    series = {"AAA": {"2026-01-01": 100.0, "2026-01-02": 101.0}}
    path = tmp_path / "snapshot.json"
    digest = write_snapshot(str(path), series, {"provider": "test"})
    loaded, loaded_digest = load_or_fetch_series(
        ["AAA"],
        lambda _s: (_ for _ in ()).throw(AssertionError("network fetch should not happen")),
        str(path),
    )
    assert loaded == series
    assert digest == loaded_digest == canonical_hash(series)
