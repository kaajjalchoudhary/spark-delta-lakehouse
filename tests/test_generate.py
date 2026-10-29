"""Generator tests. Pure Python, no Spark needed."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from lakehouse.generate import GenerateConfig, generate


def _lines(landing: Path, feed: str) -> dict[str, list[str]]:
    return {p.stem: p.read_text().splitlines() for p in sorted((landing / feed).glob("*.jsonl"))}


def _parsed(lines: list[str]) -> list[dict]:
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out


def test_deterministic_and_append_only(tmp_path: Path) -> None:
    """More days must reproduce earlier files exactly, so reruns and backfills are stable."""
    generate(tmp_path / "a", GenerateConfig(days=4, trips_per_day=60))
    generate(tmp_path / "b", GenerateConfig(days=6, trips_per_day=60))
    for path in (tmp_path / "a").rglob("*.*"):
        assert path.read_bytes() == (tmp_path / "b" / path.relative_to(tmp_path / "a")).read_bytes()


def test_feed_contains_the_problems_silver_must_handle(tmp_path: Path) -> None:
    cfg = GenerateConfig(days=10, trips_per_day=120)
    generate(tmp_path, cfg)
    files = _lines(tmp_path, "trips")
    all_lines = [line for lines in files.values() for line in lines]
    events = _parsed(all_lines)

    assert len(events) < len(all_lines), "some lines are not JSON"
    assert any(n > 1 for n in Counter(all_lines).values()), "exact duplicate deliveries"

    late = [e for day, lines in files.items() for e in _parsed(lines) if e["pickup_at"][:10] < day]
    assert late, "events that arrive a day late"

    fares = {}
    for e in events:
        fares.setdefault(e["trip_id"], set()).add(e["fare_amount"])
    assert any(len(f) > 1 for f in fares.values()), "fare adjustments"
    assert any(e["fare_amount"] < 0 for e in events), "business-rule violations"


def test_schema_drift_is_introduced_on_schedule(tmp_path: Path) -> None:
    cfg = GenerateConfig(days=10, trips_per_day=40, tips_from_day=3, surge_from_day=8)
    generate(tmp_path, cfg)
    for i, (day, lines) in enumerate(_lines(tmp_path, "trips").items()):
        own_day = [e for e in _parsed(lines) if e["pickup_at"][:10] == day and e["event_ts"][:10] <= day]
        assert own_day
        assert all(("tip_amount" in e) == (i >= 3) for e in own_day), day
        assert all(("surge_multiplier" in e) == (i >= 8) for e in own_day), day


def test_driver_feed_has_changes_and_resends(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=10, trips_per_day=20))
    events = [json.loads(line) for lines in _lines(tmp_path, "drivers").values() for line in lines]
    per_driver = Counter(e["driver_id"] for e in events)
    assert any(n > 1 for n in per_driver.values())
    tracked = [(e["driver_id"], e["city"], e["vehicle_type"], e["status"]) for e in events]
    assert len(set(tracked)) < len(tracked), "re-sent records with unchanged attributes"


def test_every_trip_driver_exists_and_was_onboarded_before_pickup(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=6, trips_per_day=80))
    onboarded: dict[int, str] = {}
    for lines in _lines(tmp_path, "drivers").values():
        for e in map(json.loads, lines):
            onboarded.setdefault(e["driver_id"], e["updated_at"])
    for lines in _lines(tmp_path, "trips").values():
        for e in _parsed(lines):
            assert onboarded[e["driver_id"]] <= e["pickup_at"]
