"""Human-readable summary of the lake: gold numbers, quarantine, drift and table history."""

from __future__ import annotations

import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Lake
from lakehouse.delta_io import exists
from lakehouse.maintenance import TABLES


def _show(df, n: int = 50) -> str:
    rows = df.limit(n).collect()
    if not rows:
        return "  (empty)"
    cols = df.columns
    widths = [max(len(c), *(len(str(r[c])) for r in rows)) for c in cols]

    def line(values) -> str:
        return "  " + "  ".join(str(v).ljust(w) for v, w in zip(values, widths, strict=True))

    return "\n".join([line(cols), line("-" * w for w in widths), *(line(r[c] for c in cols) for r in rows)])


def build_report(spark: SparkSession, lake: Lake) -> str:
    out = []
    city = spark.read.format("delta").load(lake.table("gold", "daily_city_metrics"))
    out += ["", "Totals by city (gold.daily_city_metrics)", _show(
        city.groupBy("city", "currency")
        .agg(
            F.sum("trips").alias("trips"),
            F.round(F.sum("cancelled_trips") / F.sum("trips"), 3).alias("cancel_rate"),
            F.round(F.sum("gross_fares"), 2).alias("gross_fares"),
            F.round(F.sum("tips"), 2).alias("tips"),
            F.round(F.avg("p50_duration_min"), 1).alias("avg_p50_min"),
        )
        .orderBy("city")
    )]

    earnings = spark.read.format("delta").load(lake.table("gold", "driver_daily_earnings"))
    out += ["", "Driver earnings by vehicle type at pick-up (gold.driver_daily_earnings)", _show(
        earnings.groupBy("currency", "vehicle_type")
        .agg(F.sum("trips").alias("trips"), F.round(F.sum("driver_earnings"), 2).alias("driver_earnings"))
        .orderBy("currency", "vehicle_type")
    )]

    quarantine = spark.read.format("delta").load(lake.table("silver", "trips_quarantine"))
    out += ["", "Quarantined lines (silver.trips_quarantine)", _show(quarantine.groupBy("reason").count().orderBy("reason"))]

    drift = set()
    for path in sorted(lake.runs.glob("*.json")):
        drift.update(json.loads(path.read_text())["trips"]["unexpected_fields"])
    out += ["", f"Undeclared fields seen in the trip feed: {sorted(drift) or 'none'}"]

    out += ["", "Table versions (Delta transaction log)"]
    for layer, name in TABLES:
        path = lake.table(layer, name)
        if exists(spark, path):
            last = DeltaTable.forPath(spark, path).history(1).select("version", "operation").first()
            out.append(f"  {layer}.{name:<24} v{last.version:<4} last op: {last.operation}")
    return "\n".join(out)
