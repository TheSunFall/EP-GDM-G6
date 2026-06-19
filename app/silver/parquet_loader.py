from pathlib import Path

from pyspark.sql import DataFrame

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("SilverParquetLoader", "silver")


def save_all_to_parquet(
    dims_dict: dict[str, DataFrame],
    facts_dict: dict[str, DataFrame],
    output_dir: Path = Path("data/silver"),
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dims_dict.items():
        if name.startswith("_"):
            continue  # entradas internas (p.ej. _SEC_EJEC_BRIDGE), no se persisten
        path = output_dir / f"{name}.parquet"
        df.write.mode("overwrite").parquet(str(path))
        _logger.info(f"Saved {name} to {path}")
    for name, df in facts_dict.items():
        path = output_dir / f"{name}.parquet"
        df.write.mode("overwrite").parquet(str(path))
        _logger.info(f"Saved {name} to {path}")
