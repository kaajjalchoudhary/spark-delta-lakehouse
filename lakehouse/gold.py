"""Gold: business-level tables, rebuilt incrementally for the pickup dates a batch touched.

daily_city_metrics       Trips, cancellations, fares, tips and trip-duration percentiles
                         per city and day.
driver_daily_earnings    Driver earnings per day, attributed to the vehicle type the
                         driver had AT PICK-UP TIME (point-in-time join to SCD2 history).

Both are written with replaceWhere on the affected dates, so a late event for Monday
recomputes Monday only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Lake
from lakehouse.delta_io import overwrite_where, sql_dates

PLATFORM_COMMISSION = 0.20


@dataclass
class GoldStats:
    city_day_rows: int = 0
    driver_day_rows: int = 0
    unattributed_trips: int = 0


def _trips_for(spark: SparkSession, lake: Lake, dates: list[date]) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(lake.table("silver", "trips"))
        .filter(f"pickup_date IN ({sql_dates(dates)})")
    )


def daily_city_metrics(spark: SparkSession, lake: Lake, dates: list[date]) -> DataFrame:
    trips = _trips_for(spark, lake, dates)
    zones = spark.read.format("delta").load(lake.table("silver", "zones"))
    completed = F.col("status") == "completed"
    return (
        trips.join(F.broadcast(zones), trips.pickup_zone_id == zones.zone_id)
        .groupBy("pickup_date", "city", "currency")
        .agg(
            F.count("*").alias("trips"),
            F.count(F.when(completed, 1)).alias("completed_trips"),
            F.count(F.when(F.col("status") == "cancelled", 1)).alias("cancelled_trips"),
            F.round(F.sum(F.when(completed, F.col("fare_amount")).otherwise(0.0)), 2).alias("gross_fares"),
            F.round(F.sum(F.coalesce("tip_amount", F.lit(0.0))), 2).alias("tips"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
            F.percentile_approx("duration_min", 0.5).alias("p50_duration_min"),
            F.percentile_approx("duration_min", 0.9).alias("p90_duration_min"),
            F.countDistinct("driver_id").alias("active_drivers"),
        )
        .withColumn("cancellation_rate", F.round(F.col("cancelled_trips") / F.col("trips"), 4))
        .withColumn("_built_at", F.current_timestamp())
    )


def driver_daily_earnings(spark: SparkSession, lake: Lake, dates: list[date]) -> DataFrame:
    trips = _trips_for(spark, lake, dates).filter("status = 'completed'").alias("t")
    drivers = spark.read.format("delta").load(lake.table("silver", "drivers")).alias("d")
    as_of_pickup = (
        (F.col("t.driver_id") == F.col("d.driver_id"))
        & (F.col("t.pickup_at") >= F.col("d.valid_from"))
        & (F.col("d.valid_to").isNull() | (F.col("t.pickup_at") < F.col("d.valid_to")))
    )
    fares = F.col("t.fare_amount") + F.coalesce(F.col("t.tip_amount"), F.lit(0.0))
    return (
        trips.join(drivers, as_of_pickup, "left")
        .groupBy(
            F.col("t.pickup_date").alias("pickup_date"),
            F.col("t.driver_id").alias("driver_id"),
            F.col("d.full_name").alias("driver_name"),
            F.col("d.city").alias("city"),
            F.col("d.vehicle_type").alias("vehicle_type"),
            F.col("t.currency").alias("currency"),
        )
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("t.distance_km"), 2).alias("distance_km"),
            F.round(F.sum("t.duration_min") / 60.0, 2).alias("hours_on_trip"),
            F.round(F.sum("t.fare_amount"), 2).alias("fares"),
            F.round(F.sum(F.coalesce(F.col("t.tip_amount"), F.lit(0.0))), 2).alias("tips"),
            F.round(
                F.sum(fares) - F.sum("t.fare_amount") * PLATFORM_COMMISSION, 2
            ).alias("driver_earnings"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )


def build(spark: SparkSession, lake: Lake, dates: list[date]) -> GoldStats:
    stats = GoldStats()
    if not dates:
        return stats
    predicate = f"pickup_date IN ({sql_dates(dates)})"

    city = daily_city_metrics(spark, lake, dates).cache()
    overwrite_where(city, lake.table("gold", "daily_city_metrics"), predicate)
    stats.city_day_rows = city.count()
    city.unpersist()

    earnings = driver_daily_earnings(spark, lake, dates).cache()
    stats.unattributed_trips = earnings.filter(F.col("vehicle_type").isNull()).agg(
        F.coalesce(F.sum("trips"), F.lit(0))
    ).first()[0]
    overwrite_where(earnings, lake.table("gold", "driver_daily_earnings"), predicate)
    stats.driver_day_rows = earnings.count()
    earnings.unpersist()
    return stats
