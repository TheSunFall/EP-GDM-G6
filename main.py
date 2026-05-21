import argparse

from app.pipeline.bronze import BronzePipeline
from app.pipeline.silver import SilverPipeline
from app.utils.spark import SparkClient


def main():
    parser = argparse.ArgumentParser(
        description="Medallion Data Lakehouse pipeline for Peruvian government data (MEF/INEI)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Pipeline stage to run")

    subparsers.add_parser("bronze", help="Run the bronze pipeline (download and convert to parquet)")

    subparsers.add_parser("profile", help="Run data quality profiling on bronze parquet files")

    silver_parser = subparsers.add_parser("silver", help="Run the silver pipeline (quality, star schema, load to SQL Server)")
    silver_parser.add_argument(
        "step",
        nargs="?",
        choices=["quality", "schema", "load"],
        default=None,
        help="Run a specific silver step: quality (step 1), schema (step 2), load (step 3). Default: all three.",
    )
    silver_parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the existing silver schema and tables before creating and loading (idempotent run).",
    )

    args = parser.parse_args()

    if args.command is None:
        _run_all()
    elif args.command == "bronze":
        _run_bronze()
    elif args.command == "profile":
        _run_profile()
    elif args.command == "silver":
        _run_silver(args.step, getattr(args, "drop", False))


def _run_all():
    _run_bronze()
    _run_profile()
    _run_silver()


def _run_bronze():
    pipeline = BronzePipeline()
    pipeline.run()


def _run_profile():
    from app.pipeline.profile import run as run_profile

    run_profile(argparse.Namespace())


def _run_silver(step: str | None = None, drop: bool = False):
    spark = SparkClient()
    pipeline = SilverPipeline(spark)
    pipeline.run(step=step, drop=drop)


if __name__ == "__main__":
    main()
