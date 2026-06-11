"""Utilities for tracking pipeline manifests (row counts, timestamps, file sizes)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BRONZE_SCHEMA = pa.schema([
    pa.field("source_name", pa.string()),
    pa.field("module_name", pa.string()),
    pa.field("row_count", pa.int64()),
    pa.field("file_size", pa.int64()),
    pa.field("downloaded_at", pa.string()),
])

STAGE_SCHEMA = pa.schema([
    pa.field("stage_name", pa.string()),
    pa.field("row_count", pa.int64()),
    pa.field("file_size", pa.int64()),
    pa.field("bronze_row_count_total", pa.int64()),
    pa.field("processed_at", pa.string()),
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


def write_bronze_manifest(entries: list[dict], path: str | Path):
    table = pa.Table.from_pylist(entries, schema=BRONZE_SCHEMA)
    pq.write_table(table, str(path), compression="zstd")


def write_stage_manifest(entries: list[dict], path: str | Path):
    table = pa.Table.from_pylist(entries, schema=STAGE_SCHEMA)
    pq.write_table(table, str(path), compression="zstd")


def read_bronze_manifest(path: str | Path) -> pa.Table:
    p = Path(path)
    if not p.exists():
        return pa.Table.from_pylist([], schema=BRONZE_SCHEMA)
    return pq.read_table(str(p))


def read_stage_manifest(path: str | Path) -> pa.Table:
    p = Path(path)
    if not p.exists():
        return pa.Table.from_pylist([], schema=STAGE_SCHEMA)
    return pq.read_table(str(p))


def bronze_row_count_map(manifest_path: str | Path) -> dict[tuple[str, str], int]:
    """Return {(source_name, module_name): row_count} from bronze manifest."""
    table = read_bronze_manifest(manifest_path)
    result = {}
    for i in range(len(table)):
        src = table.column("source_name")[i].as_py()
        mod = table.column("module_name")[i].as_py()
        rc = table.column("row_count")[i].as_py()
        result[(src, mod)] = rc
    return result


def stage_bronze_row_count_total_map(manifest_path: str | Path) -> dict[str, int]:
    """Return {stage_name: bronze_row_count_total} from silver stage manifest."""
    table = read_stage_manifest(manifest_path)
    result = {}
    for i in range(len(table)):
        stage = table.column("stage_name")[i].as_py()
        total = table.column("bronze_row_count_total")[i].as_py()
        result[stage] = total
    return result


# Mapeo de cada tabla stage a sus fuentes bronze (source_name, module_prefix).
# Usado tanto por quality._write_stage_manifest() como por silver.py skip_unchanged.
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
    """Calcula la suma de filas bronze para una lista de fuentes (source_name, module_prefix)."""
    total = 0
    for src_name, mod_prefix in sources:
        for (b_src, b_mod), b_rc in bronze_map.items():
            if b_src == src_name and b_mod.startswith(mod_prefix):
                total += b_rc
    return total


def bronze_unchanged(
    bronze_manifest_path: str | Path,
    stage_manifest_path: str | Path,
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

    bronze_map = bronze_row_count_map(bronze_path)
    stage_map = stage_bronze_row_count_total_map(stage_path)

    if not stage_map:
        return False, "Stage manifest vacío"

    mismatches: list[str] = []
    for stage_name, sources in STAGE_TO_BRONZE_SOURCES.items():
        if not sources:
            continue  # e.g. categorias_municipalidades no tiene fuente bronze
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
