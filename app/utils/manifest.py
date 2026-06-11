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
