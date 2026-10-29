"""Silver: parse, validate, deduplicate and upsert.

trips    One row per trip, latest event wins. Upserted with MERGE so late arrivals and
         next-day fare adjustments land on the right row. Partitioned by pickup_date.
trips_quarantine
         Lines that could not be parsed or broke a business rule, with the reason and
         the raw text, so nothing is silently dropped.
drivers  SCD Type 2 history of each driver's city, vehicle type and duty status.
zones    Reference data, replaced on every run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.config import Lake
from lakehouse.contracts import DRIVER_EVENT, TRIP_EVENT, TRIP_STATUSES, ZONES
from lakehouse.delta_io import exists, overwrite_where, sql_dates
from lakehouse.scd2 import Scd2Stats, merge_scd2


@dataclass
class TripBatchStats:
    events: int = 0
    valid: int = 0
    quarantined: int = 0
    upserted_trips: int = 0
    affected_dates: list[date] = field(default_factory=list)
    unexpected_fields: list[str] = field(default_factory=list)
    quarantine_reasons: dict[str, int] = field(default_factory=dict)


def _bronze_batch(spark: SparkSession, lake: Lake, feed: str, batch_date: date) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(lake.table("bronze", feed))
        .filter(F.col("_batch_date") == F.lit(batch_date.isoformat()).cast("date"))
    )


def parse_trip_events(bronze: DataFrame) -> DataFrame:
    """Apply the trip contract and attach a rejection reason (NULL when the row is valid)."""
    e = F.from_json("raw", TRIP_EVENT).alias("e")
    parsed = bronze.select("raw", "_source_file", "_batch_date", e).select(
        "raw",
        "_source_file",
        "_batch_date",
        "e.trip_id",
        "e.driver_id",
        "e.rider_id",
        "e.pickup_zone_id",
        "e.dropoff_zone_id",
        F.to_timestamp("e.requested_at").alias("requested_at"),
        F.to_timestamp("e.pickup_at").alias("pickup_at"),
        F.to_timestamp("e.dropoff_at").alias("dropoff_at"),
        F.lower(F.trim("e.status")).alias("status"),
        "e.distance_km",
        "e.fare_amount",
        "e.tip_amount",
        F.upper("e.currency").alias("currency"),
        F.to_timestamp("e.event_ts").alias("event_ts"),
    )
    required = ["trip_id", "driver_id", "pickup_zone_id", "pickup_at", "status", "event_ts"]
    missing_required = F.lit(False)
    for column in required:
        missing_required = missing_required | F.col(column).isNull()
    reason = (
        F.when(missing_required, "malformed_or_missing_required_field")
        .when(~F.col("status").isin(*TRIP_STATUSES), "unknown_status")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when((F.col("status") == "completed") & F.col("dropoff_at").isNull(), "completed_without_dropoff")
        .when(F.col("dropoff_at") < F.col("pickup_at"), "dropoff_before_pickup")
    )
    return parsed.withColumn("_reject_reason", reason)


def unexpected_fields(bronze: DataFrame) -> list[str]:
    """Top-level JSON keys the producer sent that the contract does not declare."""
    keys = (
        bronze.select(F.explode(F.expr("json_object_keys(raw)")).alias("key"))
        .distinct()
        .collect()
    )
    return sorted({row.key for row in keys} - set(TRIP_EVENT.fieldNames()))


def merge_trips(spark: SparkSession, lake: Lake, batch_date: date) -> TripBatchStats:
    stats = TripBatchStats()
    bronze = _bronze_batch(spark, lake, "trips", batch_date)
    stats.unexpected_fields = unexpected_fields(bronze)

    events = parse_trip_events(bronze).cache()
    stats.events = events.count()

    rejected = events.filter(F.col("_reject_reason").isNotNull())
    overwrite_where(
        rejected.select(
            "raw",
            F.col("_reject_reason").alias("reason"),
            "trip_id",
            "_source_file",
            "_batch_date",
            F.current_timestamp().alias("_quarantined_at"),
        ),
        lake.table("silver", "trips_quarantine"),
        predicate=f"_batch_date = '{batch_date.isoformat()}'",
        partition_by=["_batch_date"],
    )
    stats.quarantine_reasons = {
        row["_reject_reason"]: row["count"] for row in rejected.groupBy("_reject_reason").count().collect()
    }
    stats.quarantined = sum(stats.quarantine_reasons.values())

    latest_first = Window.partitionBy("trip_id").orderBy(F.col("event_ts").desc(), F.col("_source_file").desc())
    latest = (
        events.filter(F.col("_reject_reason").isNull())
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter("_rn = 1")
        .select(
            "trip_id",
            "driver_id",
            "rider_id",
            "pickup_zone_id",
            "dropoff_zone_id",
            "requested_at",
            "pickup_at",
            "dropoff_at",
            F.to_date("pickup_at").alias("pickup_date"),
            "status",
            "distance_km",
            F.round(
                (F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60.0, 2
            ).alias("duration_min"),
            "fare_amount",
            "tip_amount",
            "currency",
            "event_ts",
            F.col("_batch_date").alias("_last_batch_date"),
            F.current_timestamp().alias("_updated_at"),
        )
        .cache()
    )
    stats.valid = events.filter(F.col("_reject_reason").isNull()).count()
    stats.upserted_trips = latest.count()
    stats.affected_dates = sorted(r.pickup_date for r in latest.select("pickup_date").distinct().collect())

    path = lake.table("silver", "trips")
    if not exists(spark, path):
        latest.write.format("delta").partitionBy("pickup_date").save(path)
    elif stats.affected_dates:
        # pickup_date never changes for a trip, so restricting the target to the batch's
        # dates is safe and lets Delta prune every other partition.
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(
                latest.alias("s"),
                f"t.pickup_date IN ({sql_dates(stats.affected_dates)}) AND t.trip_id = s.trip_id",
            )
            .whenMatchedUpdateAll(condition="s.event_ts > t.event_ts")
            .whenNotMatchedInsertAll()
            .execute()
        )
    events.unpersist()
    latest.unpersist()
    return stats


def apply_driver_changes(spark: SparkSession, lake: Lake, batch_date: date) -> Scd2Stats:
    e = F.from_json("raw", DRIVER_EVENT).alias("e")
    changes = (
        _bronze_batch(spark, lake, "drivers", batch_date)
        .select(e)
        .select(
            "e.driver_id",
            "e.full_name",
            "e.city",
            F.lower("e.vehicle_type").alias("vehicle_type"),
            F.lower("e.status").alias("status"),
            F.to_timestamp("e.updated_at").alias("updated_at"),
        )
        .filter(F.col("driver_id").isNotNull() & F.col("updated_at").isNotNull())
    )
    return merge_scd2(
        spark,
        lake.table("silver", "drivers"),
        changes,
        key="driver_id",
        tracked=["city", "vehicle_type", "status"],
        changed_at="updated_at",
    )


def load_zones(spark: SparkSession, lake: Lake) -> int:
    zones = spark.read.csv(str(lake.landing / "zones" / "zones.csv"), header=True, schema=ZONES)
    zones.write.format("delta").mode("overwrite").save(lake.table("silver", "zones"))
    return zones.count()
