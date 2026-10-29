"""Schemas the silver layer expects from each feed.

Bronze stores the raw line untouched; these contracts are applied when parsing into silver.
Fields the producer adds later (like tip_amount) are declared here as nullable, and any key
that is NOT declared is reported as schema drift instead of being silently dropped.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRIP_EVENT = StructType(
    [
        StructField("trip_id", StringType()),
        StructField("driver_id", LongType()),
        StructField("rider_id", LongType()),
        StructField("pickup_zone_id", LongType()),
        StructField("dropoff_zone_id", LongType()),
        StructField("requested_at", StringType()),
        StructField("pickup_at", StringType()),
        StructField("dropoff_at", StringType()),
        StructField("status", StringType()),
        StructField("distance_km", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("event_ts", StringType()),
        StructField("tip_amount", DoubleType()),  # added by the producer in a later release
    ]
)

DRIVER_EVENT = StructType(
    [
        StructField("driver_id", LongType()),
        StructField("full_name", StringType()),
        StructField("city", StringType()),
        StructField("vehicle_type", StringType()),
        StructField("status", StringType()),
        StructField("updated_at", StringType()),
    ]
)

ZONES = StructType(
    [
        StructField("zone_id", LongType()),
        StructField("zone_name", StringType()),
        StructField("city", StringType()),
    ]
)

TRIP_STATUSES = ("completed", "cancelled")
