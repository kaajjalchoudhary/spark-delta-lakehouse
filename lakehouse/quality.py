"""Data-quality gates that run after every batch.

An `error` check failing stops the run before anyone reads a bad number. A `warn` check
is recorded in the run log but does not stop it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.config import Lake
from lakehouse.delta_io import sql_dates


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    severity: str = "error"


@dataclass
class CheckSuite:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str, severity: str = "error") -> None:
        self.checks.append(Check(name, bool(passed), detail, severity))

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "error"]

    def raise_on_failure(self) -> None:
        if self.failures:
            lines = "\n".join(f"  - {c.name}: {c.detail}" for c in self.failures)
            raise DataQualityError(f"{len(self.failures)} data-quality check(s) failed:\n{lines}")

    def as_dicts(self) -> list[dict]:
        return [asdict(c) for c in self.checks]


def check_trips_unique(spark: SparkSession, lake: Lake, suite: CheckSuite) -> None:
    trips = spark.read.format("delta").load(lake.table("silver", "trips"))
    total, distinct = trips.agg(F.count("*"), F.countDistinct("trip_id")).first()
    suite.add("silver.trips.trip_id_unique", total == distinct, f"{total} rows, {distinct} distinct trip_ids")


def check_scd2_integrity(spark: SparkSession, lake: Lake, suite: CheckSuite) -> None:
    drivers = spark.read.format("delta").load(lake.table("silver", "drivers"))
    bad_current = (
        drivers.groupBy("driver_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("current_rows"))
        .filter("current_rows <> 1")
        .count()
    )
    suite.add("silver.drivers.one_current_version", bad_current == 0, f"{bad_current} drivers without exactly one")

    by_time = Window.partitionBy("driver_id").orderBy("valid_from")
    broken_chain = (
        drivers.withColumn("next_from", F.lead("valid_from").over(by_time))
        .filter(
            (F.col("next_from").isNotNull() & ~F.col("valid_to").eqNullSafe(F.col("next_from")))
            | (F.col("valid_to") <= F.col("valid_from"))
        )
        .count()
    )
    suite.add("silver.drivers.contiguous_history", broken_chain == 0, f"{broken_chain} gaps or overlaps")


def check_quarantine_rate(events: int, quarantined: int, suite: CheckSuite, threshold: float = 0.05) -> None:
    rate = quarantined / events if events else 0.0
    suite.add(
        "silver.trips.quarantine_rate",
        rate <= threshold,
        f"{quarantined}/{events} = {rate:.2%} (threshold {threshold:.0%})",
        severity="warn",
    )


def check_gold_reconciles(spark: SparkSession, lake: Lake, dates: list[date], suite: CheckSuite) -> None:
    if not dates:
        return
    where = f"pickup_date IN ({sql_dates(dates)})"
    silver_total, silver_trips = (
        spark.read.format("delta").load(lake.table("silver", "trips")).filter(where)
        .agg(F.sum(F.when(F.col("status") == "completed", F.col("fare_amount")).otherwise(0.0)), F.count("*"))
        .first()
    )
    gold_total, gold_trips, groups = (
        spark.read.format("delta").load(lake.table("gold", "daily_city_metrics")).filter(where)
        .agg(F.sum("gross_fares"), F.sum("trips"), F.count("*"))
        .first()
    )
    tolerance = 0.005 * (groups or 1) + 1e-6  # per-group rounding to cents
    diff = abs((silver_total or 0.0) - (gold_total or 0.0))
    suite.add(
        "gold.daily_city_metrics.trips_reconcile",
        silver_trips == gold_trips,
        f"silver {silver_trips} vs gold {gold_trips}",
    )
    suite.add(
        "gold.daily_city_metrics.fares_reconcile",
        diff <= tolerance,
        f"difference {diff:.4f} (tolerance {tolerance:.4f})",
    )
