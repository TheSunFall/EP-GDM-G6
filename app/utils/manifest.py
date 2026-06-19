"""Utilidades para tracking de manifests del pipeline (row counts, timestamps, tamaños)."""

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Esquemas Arrow equivalentes a los Spark — usados en write para evitar Python workers
_BRONZE_PA_SCHEMA = pa.schema([
    pa.field("source_name", pa.string()),
    pa.field("module_name", pa.string()),
    pa.field("row_count", pa.int64()),
    pa.field("file_size", pa.int64()),
    pa.field("downloaded_at", pa.string()),
])

_STAGE_PA_SCHEMA = pa.schema([
    pa.field("stage_name", pa.string()),
    pa.field("row_count", pa.int64()),
    pa.field("file_size", pa.int64()),
    pa.field("bronze_row_count_total", pa.int64()),
    pa.field("processed_at", pa.string()),
])

_BRONZE_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("module_name", StringType(), False),
    StructField("row_count", LongType(), False),
    StructField("file_size", LongType(), False),
    StructField("downloaded_at", StringType(), False),
])

_STAGE_SCHEMA = StructType([
    StructField("stage_name", StringType(), False),
    StructField("row_count", LongType(), False),
    StructField("file_size", LongType(), False),
    StructField("bronze_row_count_total", LongType(), False),
    StructField("processed_at", StringType(), False),
])


def bronze_entry(source_name: str, module_name: str, row_count: int, file_size: int) -> dict:
    return {
        "source_name": source_name,
        "module_name": module_name,
        "row_count": row_count,
        "file_size": file_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def stage_entry(stage_name: str, row_count: int, file_size: int, bronze_row_count_total: int) -> dict:
    return {
        "stage_name": stage_name,
        "row_count": row_count,
        "file_size": file_size,
        "bronze_row_count_total": bronze_row_count_total,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def _write_parquet_pyarrow(entries: list[dict], path: str | Path, schema: pa.Schema) -> None:
    """Escribe una lista de dicts como parquet único usando PyArrow.

    Evita spark.createDataFrame(python_list) que en PySpark 4.x usa pickle/Python workers
    y crashea en Windows tras procesar grandes volúmenes de datos en la misma sesión.
    Spark puede leer el archivo resultante normalmente con spark.read.parquet().
    """
    out = Path(path)
    if out.is_dir():
        shutil.rmtree(out)
    elif out.is_file():
        out.unlink()
    pq.write_table(pa.Table.from_pylist(entries, schema=schema), str(out))


def write_bronze_manifest(entries: list[dict], path: str | Path, spark: SparkSession) -> None:
    _write_parquet_pyarrow(entries, path, _BRONZE_PA_SCHEMA)


def write_stage_manifest(entries: list[dict], path: str | Path, spark: SparkSession) -> None:
    _write_parquet_pyarrow(entries, path, _STAGE_PA_SCHEMA)


def read_bronze_manifest(path: str | Path, spark: SparkSession) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return [row.asDict() for row in spark.read.parquet(str(p)).collect()]


def read_stage_manifest(path: str | Path, spark: SparkSession) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return [row.asDict() for row in spark.read.parquet(str(p)).collect()]


def bronze_row_count_map(
    manifest_path: str | Path, spark: SparkSession
) -> dict[tuple[str, str], int]:
    """Retorna {(source_name, module_name): row_count} desde el bronze manifest."""
    rows = read_bronze_manifest(manifest_path, spark)
    return {(r["source_name"], r["module_name"]): r["row_count"] for r in rows}


def stage_bronze_row_count_total_map(
    manifest_path: str | Path, spark: SparkSession
) -> dict[str, int]:
    """Retorna {stage_name: bronze_row_count_total} desde el stage manifest."""
    rows = read_stage_manifest(manifest_path, spark)
    return {r["stage_name"]: r["bronze_row_count_total"] for r in rows}


# Mapeo de cada tabla stage a sus fuentes bronze (source_name, module_prefix).
STAGE_TO_BRONZE_SOURCES: dict[str, list[tuple[str, str]]] = {
    "ingreso_unified": [("SIAF", y) for y in ("2021", "2022", "2023", "2024")],
    "rentas_preguntas": [("SISMEPRE", "rentas_preguntas")],
    "rentas_formulario": [("SISMEPRE", "rentas_formulario")],
    "rentas_esat_estadistica_atm": [("SISMEPRE", "rentas_esat_estadistica_atm")],
    "rentas_respuestas": [("SISMEPRE", "rentas_respuestas")],
    "rentas_ano_aplicacion": [("SISMEPRE", "rentas_ano_aplicacion")],
    "categorias_municipalidades": [],
}
for _y in ("2021", "2022", "2023", "2024", "2025"):
    STAGE_TO_BRONZE_SOURCES[f"renamu_{_y}"] = [
        ("RENAMU", _y if _y != "2025" else "984-Modulo1963")
    ]


def _compute_bronze_total(
    bronze_map: dict[tuple[str, str], int],
    sources: list[tuple[str, str]],
) -> int:
    total = 0
    for src_name, mod_prefix in sources:
        for (b_src, b_mod), b_rc in bronze_map.items():
            if b_src == src_name and b_mod.startswith(mod_prefix):
                total += b_rc
    return total


def bronze_unchanged(
    bronze_manifest_path: str | Path,
    stage_manifest_path: str | Path,
    spark: SparkSession,
) -> tuple[bool, str]:
    """
    Compara los row counts actuales del bronze manifest contra los
    bronze_row_count_total registrados en el stage manifest.

    Retorna (unchanged: bool, detail_message: str).
    """
    bronze_path = Path(bronze_manifest_path)
    stage_path = Path(stage_manifest_path)

    if not bronze_path.exists() or not stage_path.exists():
        return False, "Manifiestos no encontrados"

    bronze_map = bronze_row_count_map(bronze_path, spark)
    stage_map = stage_bronze_row_count_total_map(stage_path, spark)

    if not stage_map:
        return False, "Stage manifest vacío"

    mismatches: list[str] = []
    for stage_name, sources in STAGE_TO_BRONZE_SOURCES.items():
        if not sources:
            continue
        if stage_name not in stage_map:
            mismatches.append(f"{stage_name}: no registrado en stage manifest")
            continue
        current_total = _compute_bronze_total(bronze_map, sources)
        recorded_total = stage_map[stage_name]
        if current_total != recorded_total:
            mismatches.append(
                f"{stage_name}: bronze actual={current_total} vs stage registrado={recorded_total}"
            )

    if mismatches:
        return False, "; ".join(mismatches)
    return True, "Todos los conteos bronze coinciden con el stage manifest"
