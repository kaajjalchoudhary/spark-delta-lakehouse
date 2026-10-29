"""Small helpers around Delta writes that every layer uses."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def sql_dates(dates: Iterable[date]) -> str:
    return ", ".join(f"'{d.isoformat()}'" for d in sorted(set(dates)))


def overwrite_where(
    df: DataFrame,
    path: str,
    predicate: str,
    partition_by: list[str] | None = None,
) -> None:
    """Idempotently replace the slice of a table matching `predicate` with `df`.

    Re-running a batch rewrites exactly the rows it owns and nothing else, so retries
    and backfills never duplicate data. Delta checks that every row in `df` satisfies
    the predicate, which guards against writing outside the slice by mistake.
    """
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if exists(df.sparkSession, path):
        writer = writer.option("replaceWhere", predicate)
    writer.save(path)
