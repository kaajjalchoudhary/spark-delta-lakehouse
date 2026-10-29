"""Bronze: land each day's raw lines in Delta exactly as received.

No parsing happens here. Keeping the original text means a bug in parsing or a changed
contract can always be fixed by replaying bronze, without asking upstream to resend.
Each batch owns its `_batch_date` partition and is written with replaceWhere, so
re-running a day replaces that day and nothing else.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Lake
from lakehouse.delta_io import overwrite_where


def landing_file(lake: Lake, feed: str, batch_date: date) -> Path:
    return lake.landing / feed / f"{batch_date.isoformat()}.jsonl"


def count_landing_lines(path: Path) -> int:
    with path.open(encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip())


def ingest(spark: SparkSession, lake: Lake, feed: str, batch_date: date) -> int:
    """Copy one landing file into bronze.<feed>. Returns the number of rows written."""
    path = landing_file(lake, feed, batch_date)
    if not path.exists():
        raise FileNotFoundError(f"no landing file for {feed} on {batch_date}: {path}")

    df = (
        spark.read.text(str(path))
        .filter(F.length(F.trim("value")) > 0)
        .select(
            F.col("value").alias("raw"),
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit(batch_date.isoformat()).cast("date").alias("_batch_date"),
        )
        .cache()
    )
    rows = df.count()
    overwrite_where(
        df,
        lake.table("bronze", feed),
        predicate=f"_batch_date = '{batch_date.isoformat()}'",
        partition_by=["_batch_date"],
    )
    df.unpersist()
    return rows
