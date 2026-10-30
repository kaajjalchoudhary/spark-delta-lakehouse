"""Change Data Feed: read what changed in a Delta table since a given version.

Every table is created with `delta.enableChangeDataFeed = true` (see spark.py), so each
MERGE records row-level changes. Downstream jobs can consume only inserts and updates
instead of rescanning the table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Lake


def changes(spark: SparkSession, lake: Lake, layer: str, name: str, since_version: int) -> DataFrame:
    """Row-level changes with _change_type, _commit_version and _commit_timestamp columns."""
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", since_version)
        .load(lake.table(layer, name))
    )


def change_summary(spark: SparkSession, lake: Lake, since_version: int) -> DataFrame:
    """How many trips were inserted, updated or deleted by each commit to silver.trips."""
    return (
        changes(spark, lake, "silver", "trips", since_version)
        .filter(F.col("_change_type") != "update_preimage")
        .groupBy("_commit_version", "_change_type")
        .count()
        .orderBy("_commit_version", "_change_type")
    )
