"""SCD Type 2 merge behaviour on small hand-made batches."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from lakehouse.scd2 import merge_scd2  # noqa: E402

SCHEMA = "driver_id bigint, full_name string, city string, vehicle_type string, status string, updated_at timestamp"


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@pytest.fixture
def apply(spark, tmp_path: Path):
    path = str(tmp_path / "drivers")

    def _apply(rows):
        changes = spark.createDataFrame(rows, SCHEMA)
        stats = merge_scd2(spark, path, changes, "driver_id", ["city", "vehicle_type", "status"], "updated_at")
        table = spark.read.format("delta").load(path).orderBy("driver_id", "valid_from").collect()
        return stats, table

    return _apply


def test_initial_load_creates_one_current_version_per_key(apply) -> None:
    stats, rows = apply([
        (1, "Ana", "Dubai", "economy", "active", ts("2025-12-01 09:00:00")),
        (2, "Ben", "London", "xl", "active", ts("2025-12-02 09:00:00")),
    ])
    assert stats.new_versions == 2
    assert all(r.is_current and r.valid_to is None for r in rows)


def test_change_closes_old_version_and_opens_new_one(apply) -> None:
    apply([(1, "Ana", "Dubai", "economy", "active", ts("2025-12-01 09:00:00"))])
    _, rows = apply([(1, "Ana", "Dubai", "comfort", "active", ts("2026-01-05 10:30:00"))])

    old, new = rows
    assert (old.vehicle_type, old.is_current, old.valid_to) == ("economy", False, ts("2026-01-05 10:30:00"))
    assert (new.vehicle_type, new.is_current, new.valid_to) == ("comfort", True, None)


def test_resent_record_without_changes_is_ignored(apply) -> None:
    apply([(1, "Ana", "Dubai", "economy", "active", ts("2025-12-01 09:00:00"))])
    stats, rows = apply([(1, "Ana", "Dubai", "economy", "active", ts("2026-01-05 10:30:00"))])
    assert stats.new_versions == 0
    assert len(rows) == 1


def test_several_changes_in_one_batch_are_chained(apply) -> None:
    apply([(1, "Ana", "Dubai", "economy", "active", ts("2025-12-01 09:00:00"))])
    stats, rows = apply([
        (1, "Ana", "Dubai", "electric", "active", ts("2026-01-05 08:00:00")),
        (1, "Ana", "Dubai", "comfort", "active", ts("2026-01-05 12:00:00")),
        (1, "Ana", "Dubai", "comfort", "inactive", ts("2026-01-05 18:00:00")),
    ])
    assert stats.new_versions == 3
    assert [r.valid_from for r in rows[1:]] == [r.valid_to for r in rows[:-1]]
    assert [r.is_current for r in rows] == [False, False, False, True]


def test_reapplying_a_batch_is_a_no_op(apply) -> None:
    batch = [
        (1, "Ana", "Dubai", "economy", "active", ts("2025-12-01 09:00:00")),
        (1, "Ana", "Dubai", "comfort", "active", ts("2026-01-05 12:00:00")),
    ]
    _, first = apply(batch)
    stats, second = apply(batch)
    assert stats.new_versions == 0
    assert first == second
