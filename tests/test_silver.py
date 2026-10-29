"""Bronze + silver trip processing on a hand-written feed."""

from __future__ import annotations

import json
from datetime import date

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse import bronze, silver  # noqa: E402
from lakehouse.config import Lake  # noqa: E402


def trip(trip_id: str, fare: float, event_ts: str, pickup: str = "2026-01-01T10:00:00", **extra) -> str:
    event = {
        "trip_id": trip_id, "driver_id": 1, "rider_id": 9, "pickup_zone_id": 1, "dropoff_zone_id": 2,
        "requested_at": "2026-01-01T09:55:00", "pickup_at": pickup, "dropoff_at": pickup[:11] + "10:20:00",
        "status": "completed", "distance_km": 5.0, "fare_amount": fare, "currency": "AED", "event_ts": event_ts,
    }
    event.update(extra)
    return json.dumps(event)


def land(lake: Lake, day: str, lines: list[str]) -> None:
    path = lake.landing / "trips" / f"{day}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def process(spark, lake: Lake, day: str) -> silver.TripBatchStats:
    bronze.ingest(spark, lake, "trips", date.fromisoformat(day))
    return silver.merge_trips(spark, lake, date.fromisoformat(day))


def trips_table(spark, lake: Lake) -> dict:
    rows = spark.read.format("delta").load(lake.table("silver", "trips")).collect()
    return {r.trip_id: r for r in rows}


def test_bad_lines_are_quarantined_with_a_reason(spark, lake: Lake) -> None:
    land(lake, "2026-01-01", [
        trip("A", 20.0, "2026-01-01T10:20:00"),
        trip("B", -5.0, "2026-01-01T10:20:00"),
        trip("C", 20.0, "2026-01-01T10:20:00", dropoff_at="2026-01-01T09:00:00"),
        trip("D", 20.0, "2026-01-01T10:20:00", status="teleported"),
        "<html>502 Bad Gateway</html>",
    ])
    stats = process(spark, lake, "2026-01-01")

    assert stats.events == 5 and stats.valid == 1 and stats.quarantined == 4
    assert stats.quarantine_reasons == {
        "negative_fare": 1,
        "dropoff_before_pickup": 1,
        "unknown_status": 1,
        "malformed_or_missing_required_field": 1,
    }
    assert set(trips_table(spark, lake)) == {"A"}
    quarantine = spark.read.format("delta").load(lake.table("silver", "trips_quarantine"))
    assert quarantine.filter(F.col("raw").contains("502 Bad Gateway")).count() == 1


def test_duplicates_late_arrivals_and_adjustments(spark, lake: Lake) -> None:
    land(lake, "2026-01-01", [trip("A", 20.0, "2026-01-01T10:20:00")] * 2)
    first = process(spark, lake, "2026-01-01")
    assert first.upserted_trips == 1

    land(lake, "2026-01-02", [
        trip("A", 18.0, "2026-01-02T08:00:00"),  # fare adjustment for yesterday's trip
        trip("A", 25.0, "2026-01-01T09:00:00"),  # stale event: older than what silver has
        trip("B", 30.0, "2026-01-01T23:50:00", pickup="2026-01-01T23:30:00", dropoff_at="2026-01-01T23:50:00"),
        trip("C", 12.0, "2026-01-02T10:20:00", pickup="2026-01-02T10:00:00"),
    ])
    second = process(spark, lake, "2026-01-02")

    trips = trips_table(spark, lake)
    assert set(trips) == {"A", "B", "C"}
    assert trips["A"].fare_amount == 18.0
    assert trips["B"].pickup_date == date(2026, 1, 1)
    assert second.affected_dates == [date(2026, 1, 1), date(2026, 1, 2)]


def test_reprocessing_a_day_changes_nothing(spark, lake: Lake) -> None:
    land(lake, "2026-01-01", [trip("A", 20.0, "2026-01-01T10:20:00"), "not json"])
    process(spark, lake, "2026-01-01")
    before = sorted(trips_table(spark, lake))
    process(spark, lake, "2026-01-01")

    assert sorted(trips_table(spark, lake)) == before
    assert spark.read.format("delta").load(lake.table("bronze", "trips")).count() == 2
    assert spark.read.format("delta").load(lake.table("silver", "trips_quarantine")).count() == 1


def test_undeclared_fields_are_reported(spark, lake: Lake) -> None:
    land(lake, "2026-01-01", [trip("A", 20.0, "2026-01-01T10:20:00", tip_amount=2.0, surge_multiplier=1.5)])
    stats = process(spark, lake, "2026-01-01")
    assert stats.unexpected_fields == ["surge_multiplier"]
    assert trips_table(spark, lake)["A"].tip_amount == 2.0
