from pathlib import Path

from pyspark.sql import DataFrame

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("GoldParquetLoader", "gold")


def save_all_to_parquet(
    dims_dict: dict[str, DataFrame],
    marts_dict: dict[str, DataFrame],
    output_dir: Path = Path("data/gold"),
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in {**dims_dict, **marts_dict}.items():
        path = output_dir / f"{name}.parquet"
        df.write.mode("overwrite").parquet(str(path))
        _logger.info(f"Saved {name} to {path}")
