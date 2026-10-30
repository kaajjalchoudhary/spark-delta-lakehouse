"""Command-line entry point: python -m lakehouse <command>."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from lakehouse.config import Lake
from lakehouse.generate import GenerateConfig, generate

log = logging.getLogger("lakehouse")


def main(argv: list[str] | None = None) -> int:
    defaults = GenerateConfig()
    parser = argparse.ArgumentParser(prog="lakehouse", description="Spark + Delta Lake medallion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_range(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start", type=date.fromisoformat, default=defaults.start, help="first day (YYYY-MM-DD)")
        p.add_argument("--days", type=int, default=defaults.days)

    gen = sub.add_parser("generate", help="write landing files")
    add_range(gen)
    gen.add_argument("--trips-per-day", type=int, default=defaults.trips_per_day)
    run = sub.add_parser("run", help="generate landing files, then process every day in order")
    add_range(run)
    run.add_argument("--trips-per-day", type=int, default=defaults.trips_per_day)
    one = sub.add_parser("process", help="(re)process a single day that has already landed")
    one.add_argument("--date", type=date.fromisoformat, required=True)
    cdf = sub.add_parser("changes", help="Change Data Feed summary for silver.trips")
    cdf.add_argument("--since", type=int, default=0, help="first table version to include")
    sub.add_parser("maintain", help="OPTIMIZE / Z-ORDER / VACUUM every table")
    sub.add_parser("report", help="print a summary of the lake")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s | %(message)s")
    lake = Lake.from_env()

    if args.command in ("generate", "run"):
        cfg = GenerateConfig(start=args.start, days=args.days, trips_per_day=args.trips_per_day)
        log.info("generated landing files %s in %s", generate(lake.landing, cfg), lake.landing)
        if args.command == "generate":
            return 0

    # Spark is imported lazily so `generate` works without a JVM.
    from lakehouse import changes, maintenance, pipeline, report
    from lakehouse.spark import get_spark

    spark = get_spark()
    try:
        if args.command == "run":
            pipeline.run_range(spark, lake, args.start, args.days)
            print(report.build_report(spark, lake))
        elif args.command == "process":
            pipeline.run_batch(spark, lake, args.date)
        elif args.command == "changes":
            changes.change_summary(spark, lake, args.since).show(200, truncate=False)
        elif args.command == "maintain":
            maintenance.maintain(spark, lake)
        elif args.command == "report":
            print(report.build_report(spark, lake))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
