"""Generic Slowly Changing Dimension Type 2 merge for Delta tables.

Given a batch of change records, `merge_scd2`:
- ignores records already applied (at or before the key's current `valid_from`), so a
  batch can be re-run safely;
- ignores re-sent records whose tracked attributes did not change;
- keeps every real change within the batch as its own version, chained by time;
- closes the open version and inserts the new ones in ONE atomic MERGE, using the
  "staged updates" pattern: each change appears once with a NULL merge key (insert) and
  each key's first change appears once more with the real key (close the open row).
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.delta_io import exists


@dataclass(frozen=True)
class Scd2Stats:
    changes_in_batch: int
    new_versions: int


def _row_hash(columns: list[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256)


def merge_scd2(
    spark: SparkSession,
    target_path: str,
    changes: DataFrame,
    key: str,
    tracked: list[str],
    changed_at: str,
) -> Scd2Stats:
    """Apply `changes` (one row per change event) to the SCD2 table at `target_path`.

    The target gets every column of `changes` except `changed_at`, plus
    row_hash, valid_from, valid_to and is_current.
    """
    attributes = [c for c in changes.columns if c not in (key, changed_at)]
    batch = (
        changes.dropDuplicates([key, changed_at])
        .withColumn("row_hash", _row_hash(tracked))
        .withColumnRenamed(changed_at, "valid_from")
    )
    batch_size = batch.count()

    if exists(spark, target_path):
        current = (
            spark.read.format("delta").load(target_path)
            .filter("is_current")
            .select(key, F.col("row_hash").alias("_current_hash"), F.col("valid_from").alias("_current_from"))
        )
    else:
        current = spark.createDataFrame(
            [], f"{key} {dict(batch.dtypes)[key]}, _current_hash string, _current_from timestamp"
        )

    by_time = Window.partitionBy(key).orderBy("valid_from")
    versions = (
        batch.join(current, key, "left")
        # Already applied in an earlier run of this or a later batch.
        .filter(F.col("_current_from").isNull() | (F.col("valid_from") > F.col("_current_from")))
        .withColumn("_prev_hash", F.coalesce(F.lag("row_hash").over(by_time), F.col("_current_hash")))
        # A re-send with identical tracked attributes is not a new version.
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("row_hash")))
        .withColumn("valid_to", F.lead("valid_from").over(by_time))
        .withColumn("is_current", F.col("valid_to").isNull())
        .select(key, *attributes, "row_hash", "valid_from", "valid_to", "is_current")
    ).cache()
    new_versions = versions.count()

    if not exists(spark, target_path):
        versions.write.format("delta").save(target_path)
    elif new_versions:
        first_change = versions.groupBy(key).agg(F.min("valid_from").alias("_close_at"))
        staged = versions.withColumn("_merge_key", F.lit(None).cast(dict(versions.dtypes)[key])).unionByName(
            first_change.withColumn("_merge_key", F.col(key)), allowMissingColumns=True
        )
        (
            DeltaTable.forPath(spark, target_path).alias("t")
            .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current")
            .whenMatchedUpdate(set={"valid_to": "s._close_at", "is_current": "false"})
            .whenNotMatchedInsert(
                condition="s._merge_key IS NULL",
                values={c: f"s.{c}" for c in versions.columns},
            )
            .execute()
        )
    versions.unpersist()
    return Scd2Stats(changes_in_batch=batch_size, new_versions=new_versions)
