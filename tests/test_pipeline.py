"""End to end: generate several days, process them in order, then check gold and Delta features."""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse import pipeline  # noqa: E402
from lakehouse.config import Lake  # noqa: E402
from lakehouse.generate import GenerateConfig, generate  # noqa: E402
from lakehouse.report import build_report  # noqa: E402

START = date(2026, 1, 1)
DAYS = 5


@pytest.fixture(scope="module")
def built_lake(spark, tmp_path_factory) -> Lake:
    lake = Lake(tmp_path_factory.mktemp("e2e") / "lake")
    generate(lake.landing, GenerateConfig(start=START, days=DAYS, trips_per_day=80, tips_from_day=2, surge_from_day=4))
    pipeline.run_range(spark, lake, START, DAYS)
    return lake


def read(spark, lake: Lake, layer: str, name: str, **options):
    reader = spark.read.format("delta")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(lake.table(layer, name))


def test_every_batch_passed_its_quality_gates(built_lake: Lake) -> None:
    records = [json.loads(p.read_text()) for p in sorted(built_lake.runs.glob("*.json"))]
    assert len(records) == DAYS
    for record in records:
        failed = [c for c in record["checks"] if not c["passed"] and c["severity"] == "error"]
        assert not failed, record["batch_date"]
    drift = {f for r in records for f in r["trips"]["unexpected_fields"]}
    assert drift == {"surge_multiplier"}


def test_gold_covers_every_city_and_day(spark, built_lake: Lake) -> None:
    city = read(spark, built_lake, "gold", "daily_city_metrics")
    assert city.select("city").distinct().count() == 3
    dates = {r.pickup_date for r in city.select("pickup_date").distinct().collect()}
    assert dates == {START + timedelta(days=i) for i in range(DAYS)}
    assert city.filter((F.col("cancellation_rate") < 0) | (F.col("cancellation_rate") > 1)).count() == 0


def test_earnings_use_the_vehicle_type_at_pickup(spark, built_lake: Lake) -> None:
    trips = read(spark, built_lake, "silver", "trips").filter("status = 'completed'").alias("t")
    drivers = read(spark, built_lake, "silver", "drivers").alias("d")
    # Drivers whose vehicle changed: their gold rows must split across vehicle types correctly.
    matched = trips.join(
        drivers,
        (F.col("t.driver_id") == F.col("d.driver_id"))
        & (F.col("t.pickup_at") >= F.col("d.valid_from"))
        & (F.col("d.valid_to").isNull() | (F.col("t.pickup_at") < F.col("d.valid_to"))),
    )
    expected = {
        (r.pickup_date, r.driver_id, r.vehicle_type): r["count"]
        for r in matched.groupBy("t.pickup_date", "t.driver_id", "d.vehicle_type").count().collect()
    }
    gold = {
        (r.pickup_date, r.driver_id, r.vehicle_type): r.trips
        for r in read(spark, built_lake, "gold", "driver_daily_earnings").collect()
    }
    assert gold == expected
    assert sum(gold.values()) == trips.count()


def test_reprocessing_a_day_is_idempotent(spark, built_lake: Lake) -> None:
    def snapshot():
        return (
            read(spark, built_lake, "silver", "trips").count(),
            read(spark, built_lake, "silver", "drivers").count(),
            sorted(map(tuple, read(spark, built_lake, "gold", "daily_city_metrics").drop("_built_at").collect())),
        )

    before = snapshot()
    pipeline.run_batch(spark, built_lake, START + timedelta(days=2))
    assert snapshot() == before


def test_time_travel_shows_the_table_growing(spark, built_lake: Lake) -> None:
    first = read(spark, built_lake, "silver", "trips", versionAsOf=0).count()
    latest = read(spark, built_lake, "silver", "trips").count()
    assert 0 < first < latest


def test_report_renders(spark, built_lake: Lake) -> None:
    text = build_report(spark, built_lake)
    assert "Totals by city" in text and "surge_multiplier" in text
