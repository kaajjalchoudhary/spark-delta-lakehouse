"""Table maintenance: compact small files, co-locate data, and clean up old files."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse.config import Lake
from lakehouse.delta_io import exists

log = logging.getLogger(__name__)

# table -> Z-ORDER columns (most common filter/join keys after the partition column)
ZORDER = {
    ("silver", "trips"): ["driver_id"],
    ("gold", "driver_daily_earnings"): ["driver_id"],
}
TABLES = [
    ("bronze", "trips"),
    ("bronze", "drivers"),
    ("silver", "trips"),
    ("silver", "trips_quarantine"),
    ("silver", "drivers"),
    ("silver", "zones"),
    ("gold", "daily_city_metrics"),
    ("gold", "driver_daily_earnings"),
]


def maintain(spark: SparkSession, lake: Lake, retain_hours: float = 168) -> None:
    for layer, name in TABLES:
        path = lake.table(layer, name)
        if not exists(spark, path):
            continue
        table = DeltaTable.forPath(spark, path)
        columns = ZORDER.get((layer, name))
        metrics = (table.optimize().executeZOrderBy(*columns) if columns else table.optimize().executeCompaction())
        result = metrics.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        table.vacuum(retain_hours)
        log.info(
            "%s.%s: optimized (%s files -> %s)%s, vacuumed files older than %sh",
            layer, name, result.numFilesRemoved, result.numFilesAdded,
            f", z-ordered by {columns}" if columns else "", retain_hours,
        )
