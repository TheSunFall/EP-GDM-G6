"""Exportador PySpark para Power BI.

Consolida los directorios Spark de data/gold/ en archivos parquet únicos
usando coalesce(1) + renombrado del part-file, optimizados para el conector
nativo de Power BI. Escribe un manifest JSON con métricas por tabla.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("PowerBIExporter", "powerbi")

_TABLES = [
    "DIM_CALENDARIO",
    "DIM_ANIO",
    "DIM_GEOGRAFIA",
    "MART_INGRESOS_GEOGRAFICO",
    "MART_INGRESOS_CLASIFICADOR",
    "MART_INGRESOS_EJECUTORA",
    "MART_PREDIAL",
    "MART_RENAMU",
]


def _write_single_parquet(
    spark: SparkSession,
    src: Path,
    output_dir: Path,
    table_name: str,
) -> int:
    """Lee un directorio Spark y escribe un único archivo parquet snappy.

    Estrategia: coalesce(1) → directorio temporal → renombrar el part-file.
    Retorna el número de filas escritas.
    """
    df = spark.read.parquet(str(src))
    df = df.cache()
    row_count = df.count()

    tmp_dir = output_dir / f"_tmp_{table_name}"
    df.coalesce(1).write \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(str(tmp_dir))
    df.unpersist()

    part_files = list(tmp_dir.glob("part-*.parquet"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"Se esperaba 1 part-file en {tmp_dir}, se encontraron: {part_files}"
        )

    dst = output_dir / f"{table_name}.parquet"
    if dst.exists():
        dst.unlink()
    shutil.move(str(part_files[0]), str(dst))
    shutil.rmtree(str(tmp_dir))

    return row_count


def export_for_powerbi(
    spark: SparkSession,
    gold_dir: Path,
    output_dir: Path,
    drop: bool = False,
) -> dict:
    """Exporta los marts Gold a archivos parquet únicos listos para Power BI.

    Parámetros
    ----------
    spark:
        Sesión PySpark activa.
    gold_dir:
        Directorio con los outputs Gold (data/gold/).
    output_dir:
        Directorio de destino para Power BI (data/powerbi/).
    drop:
        Si True, elimina output_dir antes de exportar.

    Retorna
    -------
    Diccionario con métricas de exportación (también escrito en manifest.json).
    """
    if drop and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "gold_dir": str(gold_dir),
        "output_dir": str(output_dir),
        "tables": {},
    }

    for table_name in _TABLES:
        src = gold_dir / f"{table_name}.parquet"

        if not src.exists():
            _logger.warning(f"{table_name}: no encontrado en {src}, omitiendo")
            continue

        _logger.info(f"Exportando {table_name} ...")
        rows = _write_single_parquet(spark, src, output_dir, table_name)
        dst = output_dir / f"{table_name}.parquet"
        size_kb = round(dst.stat().st_size / 1024, 1)
        _logger.info(f"  {table_name}: {rows:,} filas — {size_kb} KB")

        manifest["tables"][table_name] = {
            "rows": rows,
            "size_kb": size_kb,
            "file": str(dst),
        }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _logger.info(f"Manifest escrito en {manifest_path}")

    return manifest
