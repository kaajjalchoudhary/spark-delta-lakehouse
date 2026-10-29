"""Process one daily batch end to end: bronze -> silver -> gold -> quality gates."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from datetime import date, timedelta

from pyspark.sql import SparkSession

from lakehouse import bronze, gold, quality, silver
from lakehouse.config import Lake

log = logging.getLogger(__name__)


def run_batch(spark: SparkSession, lake: Lake, batch_date: date) -> dict:
    """Process `batch_date`. Safe to re-run: every write is an idempotent replace or merge."""
    started = time.perf_counter()
    suite = quality.CheckSuite()

    # Bronze
    ingested = {}
    for feed in ("drivers", "trips"):
        ingested[feed] = bronze.ingest(spark, lake, feed, batch_date)
        expected = bronze.count_landing_lines(bronze.landing_file(lake, feed, batch_date))
        suite.add(f"bronze.{feed}.complete", ingested[feed] == expected, f"{ingested[feed]} of {expected} lines")

    # Silver
    silver.load_zones(spark, lake)
    drivers = silver.apply_driver_changes(spark, lake, batch_date)
    trips = silver.merge_trips(spark, lake, batch_date)
    quality.check_trips_unique(spark, lake, suite)
    quality.check_scd2_integrity(spark, lake, suite)
    quality.check_quarantine_rate(trips.events, trips.quarantined, suite)
    suite.add(
        "silver.trips.schema_drift",
        not trips.unexpected_fields,
        f"undeclared fields: {trips.unexpected_fields or 'none'}",
        severity="warn",
    )

    # Gold
    built = gold.build(spark, lake, trips.affected_dates)
    suite.add(
        "gold.driver_daily_earnings.point_in_time_match",
        built.unattributed_trips == 0,
        f"{built.unattributed_trips} completed trips without a driver version at pick-up",
    )
    quality.check_gold_reconciles(spark, lake, trips.affected_dates, suite)

    record = {
        "batch_date": batch_date.isoformat(),
        "seconds": round(time.perf_counter() - started, 1),
        "bronze_rows": ingested,
        "drivers": asdict(drivers),
        "trips": {**asdict(trips), "affected_dates": [d.isoformat() for d in trips.affected_dates]},
        "gold": asdict(built),
        "checks": suite.as_dicts(),
    }
    lake.runs.mkdir(parents=True, exist_ok=True)
    (lake.runs / f"{batch_date.isoformat()}.json").write_text(json.dumps(record, indent=2))

    warnings = [c.name for c in suite.checks if not c.passed and c.severity == "warn"]
    log.info(
        "batch %s: %d trip events, %d quarantined, %d trips upserted, %d new driver versions, "
        "gold dates %s, %.1fs%s",
        batch_date,
        trips.events,
        trips.quarantined,
        trips.upserted_trips,
        drivers.new_versions,
        [d.isoformat() for d in trips.affected_dates],
        record["seconds"],
        f", warnings: {warnings}" if warnings else "",
    )
    suite.raise_on_failure()
    return record


def run_range(spark: SparkSession, lake: Lake, start: date, days: int) -> list[dict]:
    return [run_batch(spark, lake, start + timedelta(days=i)) for i in range(days)]
