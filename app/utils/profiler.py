"""Data quality profiling — 8 criterios de calidad para archivos Parquet Bronze (PySpark)."""

import html as _html
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F

from app.utils.logging import UnifiedLogger
from app.utils.spark import SparkClient

_logger = UnifiedLogger("Profiler", "profiler")

PASS_THRESHOLD = 70.0
_YEAR_RANGE = (2000, 2030)

_ID_PATTERNS = re.compile(r"(id|codigo|ubigeo|sec_ejec|ccdd|ccpp|ccdi)", re.IGNORECASE)
_NUMERIC_COL_PATTERNS = re.compile(
    r"(monto|pia|pim|recaudado|importe|saldo|anio|año|ano|mes|codigo|cod|sec|rubro|pliego|sector)",
    re.IGNORECASE,
)
_AMOUNT_COL_PATTERNS = re.compile(
    r"(monto|pia|pim|recaudado|importe|saldo)", re.IGNORECASE
)
_YEAR_COL_PATTERNS = re.compile(r"(anio|año|ano|year)", re.IGNORECASE)
_CODE_COL_PATTERNS = re.compile(r"(codigo|cod|rubro|pliego|sector|tipo)", re.IGNORECASE)

_YEAR_LITERAL_RE = r"^(19|20)\d{2}($|-)"
_UBIGEO_RE = r"^\d{1,6}$"
_NUMERIC_RE = r"^-?\d+([.,]\d+)?$"

_CRITERION_META: Dict[str, Dict[str, str]] = {
    "exactitud": {
        "nombre": "Exactitud",
        "desc": "Valores sin outliers estadísticos extremos (±3σ de la media)",
    },
    "completitud": {
        "nombre": "Completitud",
        "desc": "Porcentaje de valores no nulos en todas las columnas del dataset",
    },
    "consistencia": {
        "nombre": "Consistencia",
        "desc": "Coherencia interna entre columnas relacionadas (años 4 dígitos, pares ccdd-ccpp)",
    },
    "integridad": {
        "nombre": "Integridad",
        "desc": "Columnas identificadoras y llaves primarias sin valores nulos",
    },
    "razonabilidad": {
        "nombre": "Razonabilidad",
        "desc": "Valores razonables para el dominio fiscal: montos ≥ 0, códigos no vacíos",
    },
    "oportunidad": {
        "nombre": "Oportunidad",
        "desc": "Datos temporalmente válidos: años en rango esperado [2000–2030]",
    },
    "unicidad": {
        "nombre": "Unicidad / Deduplicación",
        "desc": "Porcentaje de filas únicas; detecta filas completamente duplicadas",
    },
    "validez": {
        "nombre": "Validez",
        "desc": "Formatos correctos: numérico, ubigeo (1-6 dígitos), año (4 dígitos)",
    },
}

_CRITERIA_ORDER = [
    "exactitud",
    "completitud",
    "consistencia",
    "integridad",
    "razonabilidad",
    "oportunidad",
    "unicidad",
    "validez",
]

spark_client = SparkClient()
spark = spark_client.get_session()


# ── 8 Criterios ───────────────────────────────────────────────────────────────


def _score_exactitud(df, cols: List[str]) -> Dict[str, Any]:
    target = [c for c in cols if _NUMERIC_COL_PATTERNS.search(c)][:10]
    if not target:
        return {
            "score": 100.0,
            "observado": "Sin columnas numéricas para evaluar exactitud",
            "columnas_evaluadas": 0,
            "outliers_detectados": [],
        }

    col_scores: Dict[str, float] = {}
    outlier_details: List[Dict] = []

    for col_name in target:
        cast_col = F.expr(f"try_cast(`{col_name}` as double)")
        row = df.select(
            F.mean(cast_col).alias("mean"),
            F.stddev(cast_col).alias("std"),
            F.count(cast_col).alias("cnt"),
        ).collect()[0]
        mean, std, cnt = row["mean"], row["std"], row["cnt"]
        if cnt is None or cnt < 3 or mean is None or std is None or std == 0:
            col_scores[col_name] = 100.0
            continue
        inliers = df.filter(F.abs(cast_col - mean) <= 3 * std).count()
        outliers = cnt - inliers
        score = round(inliers / cnt * 100, 2)
        col_scores[col_name] = score
        if outliers > 0:
            outlier_details.append(
                {
                    "columna": col_name,
                    "outliers": outliers,
                    "total": cnt,
                    "score": score,
                }
            )

    if not col_scores:
        return {
            "score": 100.0,
            "observado": "Sin columnas numéricas válidas",
            "columnas_evaluadas": 0,
            "outliers_detectados": [],
        }

    avg = sum(col_scores.values()) / len(col_scores)
    aprobadas = sum(1 for s in col_scores.values() if s >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(col_scores)} columnas evaluadas; {len(outlier_details)} con outliers extremos",
        "columnas_evaluadas": len(col_scores),
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(col_scores) - aprobadas,
        "outliers_detectados": outlier_details,
        "por_columna": col_scores,
    }


_OPTIONAL_NULL_THRESHOLD = (
    0.85  # columnas con >85% nulls son opcionales y no penalizan el score
)


def _score_completitud(df, cols: List[str], total: int) -> Dict[str, Any]:
    if total == 0:
        return {
            "score": 0.0,
            "observado": "DataFrame vacío",
            "total_celdas": 0,
            "total_nulas": 0,
            "peores_columnas": [],
        }

    null_row = df.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols]
    ).collect()[0]
    null_counts = {c: (null_row[c] or 0) for c in cols}

    # Separar columnas requeridas (≤85% nulos) de opcionales (>85% nulos — encuestas RENAMU, etc.)
    required_cols = [
        c for c in cols if null_counts[c] / total <= _OPTIONAL_NULL_THRESHOLD
    ]
    optional_cols = [
        c for c in cols if null_counts[c] / total > _OPTIONAL_NULL_THRESHOLD
    ]

    scoring_cols = required_cols if required_cols else cols
    total_cells = total * len(scoring_cols)
    total_nulls_req = sum(null_counts[c] for c in scoring_cols)
    avg_null = total_nulls_req / total_cells * 100 if total_cells else 0
    score = round(max(0.0, 100.0 - avg_null), 2)

    worst = sorted(
        [(c, null_counts[c] / total * 100) for c in scoring_cols if null_counts[c] > 0],
        key=lambda x: x[1],
        reverse=True,
    )[:7]
    aprobadas = sum(1 for c in scoring_cols if null_counts[c] / total * 100 <= 30)
    obs = f"{total_nulls_req:,} nulos de {total_cells:,} celdas requeridas ({avg_null:.1f}% nulidad)"
    if optional_cols:
        obs += f" | {len(optional_cols)} columnas opcionales excluidas del score"
    return {
        "score": score,
        "observado": obs,
        "total_celdas": total_cells,
        "total_nulas": total_nulls_req,
        "avg_null_pct": round(avg_null, 2),
        "columnas_opcionales": len(optional_cols),
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(scoring_cols) - aprobadas,
        "peores_columnas": [{"columna": c, "pct_nulo": round(p, 1)} for c, p in worst],
    }


def _score_consistencia(df, cols: List[str], total: int) -> Dict[str, Any]:
    if total == 0:
        return {"score": 100.0, "observado": "Sin datos", "detalle": []}

    checks: List[Dict] = []
    cols_lower = {c.lower(): c for c in cols}

    for col_name in [c for c in cols if _YEAR_COL_PATTERNS.search(c)]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        valid = df.filter(
            F.expr(
                f"cast(`{col_name}` as string) rlike '^(19|20)\\\\d{{2}}' or try_cast(substr(cast(`{col_name}` as string), 1, 4) as int) between 1900 and 2100"
            )
        ).count()
        checks.append(
            {
                "verificacion": f"Año 4 dígitos en '{col_name}'",
                "aprobadas": valid,
                "total": non_null,
                "score": round(valid / non_null * 100, 2),
            }
        )

    for code_col, lo, hi in (("ccdd", 1, 25), ("ccpp", 1, 99), ("ccdi", 1, 99)):
        if code_col not in cols_lower:
            continue
        actual = cols_lower[code_col]
        non_null = df.filter(F.col(actual).isNotNull()).count()
        if non_null == 0:
            continue
        ok = df.filter(
            F.expr(
                f"try_cast(substr(`{actual}`, 1, 4) as int) >= {lo} and try_cast(substr(`{actual}`, 1, 4) as int) <= {hi}"
            )
        ).count()
        checks.append(
            {
                "verificacion": f"Código '{code_col}' válido (rango {lo}-{hi})",
                "aprobadas": ok,
                "total": non_null,
                "score": round(ok / non_null * 100, 2),
            }
        )

    if not checks:
        return {
            "score": 100.0,
            "observado": "Sin columnas sujetas a verificación de consistencia",
            "detalle": [],
        }
    avg = sum(c["score"] for c in checks) / len(checks)
    aprobadas = sum(1 for c in checks if c["score"] >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(checks)} verificaciones de consistencia interna realizadas",
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(checks) - aprobadas,
        "detalle": checks,
    }


def _score_integridad(df, cols: List[str], total: int) -> Dict[str, Any]:
    id_cols = [c for c in cols if _ID_PATTERNS.search(c)]
    if not id_cols:
        return {
            "score": 100.0,
            "observado": "Sin columnas identificadoras detectadas",
            "detalle": [],
        }
    if total == 0:
        return {"score": 0.0, "observado": "DataFrame vacío", "detalle": []}

    null_row = df.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in id_cols]
    ).collect()[0]
    col_results = []
    for col_name in id_cols:
        nulos = null_row[col_name] or 0
        s = round((total - nulos) / total * 100, 2)
        col_results.append(
            {"columna": col_name, "nulos": nulos, "total": total, "score": s}
        )

    avg = sum(c["score"] for c in col_results) / len(col_results)
    aprobadas = sum(1 for c in col_results if c["score"] >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(id_cols)} columnas identificadoras verificadas",
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(id_cols) - aprobadas,
        "detalle": col_results,
    }


def _score_razonabilidad(df, cols: List[str], total: int) -> Dict[str, Any]:
    checks: List[Dict] = []

    for col_name in [c for c in cols if _NUMERIC_COL_PATTERNS.search(c)]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        neg = df.filter(F.expr(f"try_cast(`{col_name}` as double) < 0")).count()
        checks.append(
            {
                "verificacion": f"Monto ≥ 0 en '{col_name}'",
                "aprobadas": non_null - neg,
                "total": non_null,
                "problemas": neg,
                "score": round((non_null - neg) / non_null * 100, 2),
            }
        )

    for col_name in [c for c in cols if _CODE_COL_PATTERNS.search(c)][:8]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        blanks = df.filter(F.trim(F.col(col_name).cast("string")) == "").count()
        if blanks > 0:
            checks.append(
                {
                    "verificacion": f"Código no vacío en '{col_name}'",
                    "aprobadas": non_null - blanks,
                    "total": non_null,
                    "problemas": blanks,
                    "score": round((non_null - blanks) / non_null * 100, 2),
                }
            )

    for col_name in [c for c in cols if _YEAR_COL_PATTERNS.search(c)]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        valid = df.filter(
            F.expr(
                f"try_cast(substr(`{col_name}`, 1, 4) as int) >= 2000 and try_cast(substr(`{col_name}`, 1, 4) as int) <= 2030"
            )
        ).count()
        checks.append(
            {
                "verificacion": f"Año razonable [2000-2030] en '{col_name}'",
                "aprobadas": valid,
                "total": non_null,
                "problemas": non_null - valid,
                "score": round(valid / non_null * 100, 2),
            }
        )

    if not checks:
        return {
            "score": 100.0,
            "observado": "Sin columnas de dominio fiscal para razonabilidad",
            "detalle": [],
        }
    avg = sum(c["score"] for c in checks) / len(checks)
    aprobadas = sum(1 for c in checks if c["score"] >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(checks)} verificaciones de razonabilidad del dominio fiscal",
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(checks) - aprobadas,
        "detalle": checks,
    }


def _score_oportunidad(df, cols: List[str]) -> Dict[str, Any]:
    year_cols = [c for c in cols if _YEAR_COL_PATTERNS.search(c)]
    if not year_cols:
        return {
            "score": 100.0,
            "observado": "Sin columnas de tiempo para evaluar oportunidad",
            "detalle": [],
        }

    col_results = []
    for col_name in year_cols:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        valid = df.filter(
            F.expr(
                f"try_cast(substr(`{col_name}`, 1, 4) as int) >= {_YEAR_RANGE[0]} and try_cast(substr(`{col_name}`, 1, 4) as int) <= {_YEAR_RANGE[1]}"
            )
        ).count()
        fuera_rows = (
            df.filter(
                F.col(col_name).isNotNull()
                & (
                    F.expr(
                        f"(try_cast(substr(`{col_name}`, 1, 4) as int) < {_YEAR_RANGE[0]} or try_cast(substr(`{col_name}`, 1, 4) as int) > {_YEAR_RANGE[1]})"
                    )
                )
            )
            .select(col_name)
            .distinct()
            .limit(5)
            .collect()
        )
        fuera = sorted({str(r[col_name]) for r in fuera_rows if r[col_name]})
        col_results.append(
            {
                "columna": col_name,
                "validos": valid,
                "total": non_null,
                "score": round(valid / non_null * 100, 2),
                "fuera_rango": fuera[:5],
            }
        )

    if not col_results:
        return {
            "score": 100.0,
            "observado": "Sin datos en columnas de tiempo",
            "detalle": [],
        }
    avg = sum(c["score"] for c in col_results) / len(col_results)
    aprobadas = sum(1 for c in col_results if c["score"] >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(col_results)} columna(s) evaluadas; rango válido: {_YEAR_RANGE[0]}–{_YEAR_RANGE[1]}",
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(col_results) - aprobadas,
        "detalle": col_results,
    }


def _score_unicidad(df, total: int) -> Dict[str, Any]:
    if total == 0:
        return {
            "score": 100.0,
            "observado": "DataFrame vacío",
            "total_filas": 0,
            "filas_duplicadas": 0,
        }
    unique = df.distinct().count()
    dups = total - unique
    score = round(unique / total * 100, 2)
    return {
        "score": score,
        "observado": f"{dups:,} filas duplicadas de {total:,} totales ({dups / total * 100:.1f}%)",
        "total_filas": total,
        "filas_unicas": unique,
        "filas_duplicadas": dups,
        "columnas_aprobadas": 1 if score >= PASS_THRESHOLD else 0,
        "columnas_reprobadas": 0 if score >= PASS_THRESHOLD else 1,
    }


def _score_validez(df, cols: List[str]) -> Dict[str, Any]:
    checks: List[Dict] = []

    for col_name in [c for c in cols if _NUMERIC_COL_PATTERNS.search(c)]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        parsed = df.filter(
            F.expr(f"try_cast(`{col_name}` as double) is not null")
        ).count()
        checks.append(
            {
                "columna": col_name,
                "formato": "Numérico",
                "aprobadas": parsed,
                "total": non_null,
                "score": round(parsed / non_null * 100, 2),
            }
        )

    for col_name in [c for c in cols if "ubigeo" in c.lower()]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        ok = df.filter(F.col(col_name).cast("string").rlike(_UBIGEO_RE)).count()
        checks.append(
            {
                "columna": col_name,
                "formato": "Ubigeo (1-6 dígitos)",
                "aprobadas": ok,
                "total": non_null,
                "score": round(ok / non_null * 100, 2),
            }
        )

    for col_name in [c for c in cols if _YEAR_COL_PATTERNS.search(c)]:
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        ok = df.filter(
            F.expr(
                f"cast(`{col_name}` as string) rlike '^(19|20)\\\\d{{2}}' or try_cast(substr(cast(`{col_name}` as string), 1, 4) as int) between 1900 and 2100"
            )
        ).count()
        checks.append(
            {
                "columna": col_name,
                "formato": "Año (4 dígitos)",
                "aprobadas": ok,
                "total": non_null,
                "score": round(ok / non_null * 100, 2),
            }
        )

    if not checks:
        return {
            "score": 100.0,
            "observado": "Sin columnas con formato verificable",
            "detalle": [],
        }
    avg = sum(c["score"] for c in checks) / len(checks)
    aprobadas = sum(1 for c in checks if c["score"] >= PASS_THRESHOLD)
    return {
        "score": round(avg, 2),
        "observado": f"{len(checks)} columnas con formato verificado",
        "columnas_aprobadas": aprobadas,
        "columnas_reprobadas": len(checks) - aprobadas,
        "detalle": checks,
    }


# ── Public API ────────────────────────────────────────────────────────────────


def _find_parquet_units(root: Path) -> List[Path]:
    """Return .parquet files and .parquet directories, skipping part-files inside PySpark dirs."""
    units = []
    for item in root.rglob("*.parquet"):
        if any(
            p.suffix == ".parquet" and p.is_dir()
            for p in item.parents
            if p != root and p.is_relative_to(root)
        ):
            continue
        units.append(item)
    return sorted(units)


def profile_parquet(parquet_path: Path) -> Dict[str, Any]:
    _logger.info(f"Perfilando archivo: {parquet_path}")

    df = spark.read.option("mergeSchema", "true").parquet(str(parquet_path))
    cols = df.columns
    total = df.count()

    _logger.info(f"Archivo {parquet_path.name}: {total} filas, {len(cols)} columnas")

    results: Dict[str, Any] = {
        "file": str(parquet_path),
        "rows": total,
        "columns": len(cols),
        "column_names": cols,
        "profiled_at": datetime.now().isoformat(),
        "criteria": {},
    }

    runners = {
        "exactitud": lambda: _score_exactitud(df, cols),
        "completitud": lambda: _score_completitud(df, cols, total),
        "consistencia": lambda: _score_consistencia(df, cols, total),
        "integridad": lambda: _score_integridad(df, cols, total),
        "razonabilidad": lambda: _score_razonabilidad(df, cols, total),
        "oportunidad": lambda: _score_oportunidad(df, cols),
        "unicidad": lambda: _score_unicidad(df, total),
        "validez": lambda: _score_validez(df, cols),
    }

    scores = []
    for name in _CRITERIA_ORDER:
        try:
            outcome = runners[name]()
            results["criteria"][name] = outcome
            scores.append(outcome["score"])
            _logger.debug(f"Criterio {name}: {outcome['score']}%")
        except Exception as exc:
            results["criteria"][name] = {
                "score": 0.0,
                "observado": f"Error al evaluar: {exc}",
                "error": str(exc),
            }
            scores.append(0.0)
            _logger.warning(
                f"Error evaluando criterio {name} para {parquet_path.name}: {exc}"
            )

    results["overall_score"] = round(sum(scores) / len(scores), 2) if scores else 0.0
    results["passed"] = results["overall_score"] >= PASS_THRESHOLD

    status = "APROBADO" if results["passed"] else "REPROBADO"
    _logger.info(
        f"Archivo {parquet_path.name}: Score {results['overall_score']}% - {status}"
    )

    return results


def profile_directory(parquet_root: Path, report_dir: Path) -> List[Dict[str, Any]]:
    _logger.info(f"Iniciando perfilado de directorio: {parquet_root}")
    report_dir.mkdir(parents=True, exist_ok=True)
    parquet_units = _find_parquet_units(parquet_root)

    _logger.info(f"Encontrados {len(parquet_units)} archivos parquet para perfilar")

    all_reports = []
    for i, pf in enumerate(parquet_units, 1):
        _logger.info(f"Procesando archivo {i}/{len(parquet_units)}: {pf.name}")
        report = profile_parquet(pf)
        all_reports.append(report)
        with open(report_dir / f"{pf.stem}_quality.json", "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    summary = {
        "profiled_at": datetime.now().isoformat(),
        "files_profiled": len(all_reports),
        "overall_avg_score": round(
            sum(r["overall_score"] for r in all_reports) / len(all_reports), 2
        )
        if all_reports
        else 0.0,
        "files_passed": sum(1 for r in all_reports if r.get("passed")),
        "files_failed": sum(1 for r in all_reports if not r.get("passed")),
        "file_summaries": [
            {
                "file": r["file"],
                "rows": r["rows"],
                "columns": r["columns"],
                "overall_score": r["overall_score"],
                "passed": r["passed"],
                "criteria_scores": {k: v["score"] for k, v in r["criteria"].items()},
            }
            for r in all_reports
        ],
    }

    with open(report_dir / "profiling_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    _logger.info(
        f"Resumen de perfilado guardado: {report_dir / 'profiling_summary.json'}"
    )

    _write_html_report(summary, all_reports, report_dir / "profiling_summary.html")
    _logger.info(f"Reporte HTML generado: {report_dir / 'profiling_summary.html'}")

    _logger.info(
        f"Perfilado de directorio completado: {len(all_reports)} archivos procesados"
    )
    return all_reports


# ── HTML Report ───────────────────────────────────────────────────────────────


def _e(v: Any) -> str:
    return _html.escape(str(v))


def _score_color(score: float) -> str:
    if score >= 80:
        return "#198754"
    if score >= PASS_THRESHOLD:
        return "#5cb85c"
    if score >= 50:
        return "#ca8a00"
    return "#dc3545"


def _fill_cls(score: float) -> str:
    if score >= PASS_THRESHOLD:
        return "fill-green"
    if score >= 50:
        return "fill-yellow"
    return "fill-red"


def _badge_status(passed: bool) -> str:
    if passed:
        return '<span class="badge badge-aprobado">APROBADO</span>'
    return '<span class="badge badge-reprobado">REPROBADO</span>'


def _score_cell(score: float) -> str:
    color = _score_color(score)
    fill = _fill_cls(score)
    return (
        f'<div class="score-cell" style="color:{color};font-weight:700">{score:.1f}%'
        f'<div class="progress-mini"><div class="progress-fill {fill}" style="width:{min(score, 100):.0f}%"></div></div></div>'
    )


def _crit_detail_table(data: Dict[str, Any], key: str) -> str:
    rows = data.get(key, [])
    if not rows:
        return ""
    cols = [k for k in rows[0].keys() if k != "score"]
    score_present = "score" in rows[0]
    header = "".join(f"<th>{_e(c)}</th>" for c in cols)
    if score_present:
        header += "<th>Score</th>"
    body = ""
    for row in rows:
        cells = "".join(f"<td>{_e(row.get(c, ''))}</td>" for c in cols)
        if score_present:
            s = row.get("score", 0)
            cells += (
                f'<td style="color:{_score_color(s)};font-weight:600">{s:.1f}%</td>'
            )
        body += f"<tr>{cells}</tr>"
    return f'<table class="detail-table"><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>'


def _render_criterion_card(key: str, data: Dict[str, Any]) -> str:
    meta = _CRITERION_META.get(key, {"nombre": key, "desc": ""})
    score = data.get("score", 0.0)
    color = _score_color(score)
    fill = _fill_cls(score)
    passed = score >= PASS_THRESHOLD
    badge = _badge_status(passed)
    observado = _e(data.get("observado", "—"))
    aprobadas = data.get("columnas_aprobadas", "—")
    reprobadas = data.get("columnas_reprobadas", "—")

    detail_html = ""
    if "peores_columnas" in data and data["peores_columnas"]:
        detail_html = _crit_detail_table(data, "peores_columnas")
    elif "detalle" in data and data["detalle"]:
        detail_html = _crit_detail_table(data, "detalle")
    elif "outliers_detectados" in data and data["outliers_detectados"]:
        detail_html = _crit_detail_table(data, "outliers_detectados")
    elif "por_columna" in data and data["por_columna"]:
        por = data["por_columna"]
        rows_html = "".join(
            f'<tr><td>{_e(c)}</td><td style="color:{_score_color(s)};font-weight:600">{s:.1f}%</td></tr>'
            for c, s in list(por.items())[:12]
        )
        detail_html = f'<table class="detail-table"><thead><tr><th>Columna</th><th>Score</th></tr></thead><tbody>{rows_html}</tbody></table>'

    stats_html = ""
    if aprobadas != "—" or reprobadas != "—":
        stats_html = (
            f'<div class="crit-stats">'
            f"<span>Aprobados: <b>{aprobadas}</b></span>"
            f"<span>Reprobados: <b>{reprobadas}</b></span>"
            f"</div>"
        )

    return f"""
<div class="crit-card">
  <div class="crit-top">
    <div>
      <div class="crit-name">{_e(meta["nombre"])}</div>
      <div class="crit-desc">{_e(meta["desc"])}</div>
    </div>
    <div style="text-align:right">
      <div class="crit-score-num" style="color:{color}">{score:.1f}%</div>
      {badge}
    </div>
  </div>
  <div class="crit-bar"><div class="crit-fill {fill}" style="width:{min(score, 100):.0f}%"></div></div>
  <div class="crit-obs">{observado}</div>
  {stats_html}
  {detail_html}
</div>"""


def _write_html_report(
    summary: Dict[str, Any], all_reports: List[Dict[str, Any]], path: Path
) -> None:
    date_str = summary.get("profiled_at", datetime.now().isoformat())[:19].replace(
        "T", " "
    )
    total = summary["files_profiled"]
    passed = summary["files_passed"]
    failed = summary["files_failed"]
    avg = summary["overall_avg_score"]

    css = """
:root{--green:#198754;--lime:#5cb85c;--yellow:#ca8a00;--red:#dc3545;--blue:#0d6efd;
  --dark:#1a1d23;--muted:#6c757d;--border:#dee2e6;--bg:#f0f2f5;--card:#fff;--radius:8px}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:#212529;font-size:14px}
.page-header{background:linear-gradient(135deg,#1a1d23 0%,#2d3142 100%);color:#fff;padding:1.8rem 2.5rem}
.page-header h1{font-size:1.45rem;font-weight:700;margin-bottom:.2rem}
.page-header .meta{color:#adb5bd;font-size:.85rem}
.container{max-width:1700px;margin:0 auto;padding:1.5rem 2rem}
.sec-title{font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin-bottom:.7rem;margin-top:1.8rem}
.metric-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin-bottom:1rem}
.metric-card{background:var(--card);border-radius:var(--radius);padding:1.1rem 1.3rem;box-shadow:0 1px 3px rgba(0,0,0,.08);border-top:3px solid var(--blue)}
.metric-card.mc-green{border-top-color:var(--green)}.metric-card.mc-red{border-top-color:var(--red)}.metric-card.mc-avg{border-top-color:#6f42c1}
.mc-label{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;margin-bottom:.25rem}
.mc-value{font-size:2rem;font-weight:800;line-height:1}.mc-sub{font-size:.75rem;color:var(--muted);margin-top:.15rem}
.mc-blue .mc-value{color:var(--blue)}.mc-green .mc-value{color:var(--green)}.mc-red .mc-value{color:var(--red)}.mc-avg .mc-value{color:#6f42c1}
table{width:100%;border-collapse:collapse;font-size:.8rem}
th{background:var(--dark);color:#fff;padding:.5rem .7rem;text-align:left;white-space:nowrap;font-weight:600;cursor:pointer;user-select:none;position:sticky;top:0}
th:hover{background:#2d3142}td{padding:.4rem .7rem;border-bottom:1px solid #f0f2f5;vertical-align:middle;white-space:nowrap}
tr:last-child td{border-bottom:none}tr:nth-child(even) td{background:#fafbfc}tr:hover td{background:#eef2f7!important}
.score-cell{font-weight:700;min-width:65px}.progress-mini{height:4px;background:#e9ecef;border-radius:2px;margin-top:3px}
.progress-fill{height:100%;border-radius:2px}.fill-green{background:var(--green)}.fill-yellow{background:#ffc107}.fill-red{background:var(--red)}
.badge{display:inline-block;padding:.18rem .5rem;border-radius:20px;font-size:.72rem;font-weight:700}
.badge-aprobado{background:#d1e7dd;color:#0f5132}.badge-reprobado{background:#f8d7da;color:#58151c}
.card-section{background:var(--card);border-radius:var(--radius);padding:1.3rem;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:1.5rem;overflow-x:auto}
details.file-block{background:var(--card);border-radius:var(--radius);margin-bottom:.55rem;box-shadow:0 1px 3px rgba(0,0,0,.07);overflow:hidden}
details.file-block[open]{box-shadow:0 2px 10px rgba(0,0,0,.12)}
details.file-block>summary{cursor:pointer;list-style:none;padding:.85rem 1.3rem;display:flex;align-items:center;gap:.8rem;border-bottom:1px solid transparent}
details.file-block[open]>summary{border-bottom-color:var(--border);background:#f8f9fa}
details.file-block>summary::-webkit-details-marker{display:none}
.sum-arrow{font-size:.7rem;color:var(--muted);transition:transform .2s;margin-left:auto;flex-shrink:0}
details[open] .sum-arrow{transform:rotate(90deg)}.sum-name{font-weight:700;font-size:.88rem;flex:1;word-break:break-all}
.sum-meta{font-size:.75rem;color:var(--muted);flex-shrink:0}.sum-score{font-size:1.25rem;font-weight:800;flex-shrink:0;min-width:52px;text-align:right}
.criteria-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(310px,1fr));gap:.7rem;padding:1.1rem}
.crit-card{border:1px solid var(--border);border-radius:7px;padding:.85rem;transition:border-color .15s}
.crit-card:hover{border-color:#adb5bd}.crit-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.3rem}
.crit-name{font-weight:700;font-size:.85rem}.crit-desc{font-size:.72rem;color:var(--muted);margin-top:1px}
.crit-score-num{font-size:1.3rem;font-weight:800}.crit-bar{height:5px;background:#e9ecef;border-radius:3px;margin:.4rem 0}
.crit-fill{height:100%;border-radius:3px}.crit-obs{background:#f8f9fa;border-left:3px solid #dee2e6;padding:.35rem .6rem;font-size:.76rem;border-radius:0 4px 4px 0;color:#495057;margin:.45rem 0}
.crit-stats{display:flex;gap:1rem;font-size:.73rem;color:var(--muted);margin:.3rem 0}.crit-stats span b{color:#212529}
.detail-table{width:100%;font-size:.73rem;border-collapse:collapse;margin-top:.35rem}
.detail-table th{background:#f0f2f5;color:var(--dark);font-weight:700;padding:.28rem .5rem;text-align:left}
.detail-table td{padding:.22rem .5rem;border-bottom:1px solid #f5f5f5}.detail-table tr:last-child td{border-bottom:none}
.charts-row{display:grid;grid-template-columns:320px 1fr;gap:1.5rem;margin-bottom:1.5rem}
@media(max-width:900px){.charts-row{grid-template-columns:1fr}}
.chart-card{background:var(--card);border-radius:var(--radius);padding:1.3rem 1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.chart-title{font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:var(--muted);margin-bottom:1rem}
.donut-wrap{display:flex;flex-direction:column;align-items:center;gap:1rem}
.donut-legend{display:flex;gap:1.4rem;font-size:.8rem;font-weight:600}
.donut-legend span{display:flex;align-items:center;gap:.4rem}.dot{width:11px;height:11px;border-radius:3px;display:inline-block;flex-shrink:0}
.bar-row{display:flex;align-items:center;gap:.9rem;margin-bottom:.65rem}.bar-row:last-child{margin-bottom:0}
.bar-label{font-size:.79rem;font-weight:600;width:170px;flex-shrink:0;text-align:right}
.bar-track{flex:1;height:20px;background:#eef0f3;border-radius:5px;overflow:hidden}
.bar-fill{height:100%;border-radius:5px}.bar-val{font-size:.8rem;font-weight:800;width:46px;flex-shrink:0;text-align:right}
"""

    legend_items = ""
    for i, key in enumerate(_CRITERIA_ORDER, 1):
        meta = _CRITERION_META[key]
        legend_items += (
            f'<div style="background:var(--card);border-radius:6px;padding:.65rem 1rem;box-shadow:0 1px 2px rgba(0,0,0,.06);border-left:3px solid var(--blue);display:flex;gap:.6rem;align-items:flex-start">'
            f'<span style="font-weight:800;color:var(--blue);font-size:.95rem;min-width:20px">{i}</span>'
            f'<div><div style="font-weight:700;font-size:.82rem">{_e(meta["nombre"])}</div>'
            f'<div style="font-size:.73rem;color:var(--muted);margin-top:1px">{_e(meta["desc"])}</div></div></div>'
        )

    crit_headers = "".join(
        f'<th onclick="sortTable(this)" title="{_e(_CRITERION_META[k]["desc"])}">'
        f"{_e(_CRITERION_META[k]['nombre'])}</th>"
        for k in _CRITERIA_ORDER
    )
    table_rows = ""
    for fs in summary.get("file_summaries", []):
        fname = Path(fs["file"]).name
        ov = fs["overall_score"]
        status_badge = _badge_status(fs["passed"])
        crit_cells = "".join(
            f"<td>{_score_cell(fs['criteria_scores'].get(k, 0))}</td>"
            for k in _CRITERIA_ORDER
        )
        table_rows += (
            f"<tr>"
            f'<td title="{_e(fs["file"])}" style="max-width:220px;overflow:hidden;text-overflow:ellipsis">{_e(fname)}</td>'
            f'<td style="text-align:right">{fs["rows"]:,}</td>'
            f'<td style="text-align:right">{fs["columns"]}</td>'
            f"{crit_cells}"
            f"<td>{_score_cell(ov)}</td>"
            f"<td>{status_badge}</td>"
            f"</tr>"
        )

    accordions = ""
    for report in all_reports:
        fname = Path(report["file"]).name
        ov = report["overall_score"]
        color = _score_color(ov)
        meta_str = f"{report['rows']:,} filas · {report['columns']} columnas"
        badge = _badge_status(report["passed"])
        cards = "".join(
            _render_criterion_card(k, report["criteria"].get(k, {}))
            for k in _CRITERIA_ORDER
        )
        accordions += f"""
<details class="file-block">
  <summary>
    <span class="sum-name">{_e(fname)}</span>
    <span class="sum-meta">{_e(meta_str)}</span>
    {badge}
    <span class="sum-score" style="color:{color}">{ov:.1f}%</span>
    <span class="sum-arrow">&#9654;</span>
  </summary>
  <div class="criteria-grid">{cards}</div>
</details>"""

    js = """
function sortTable(th) {
  var table = th.closest('table');
  var tbody = table.querySelector('tbody');
  var idx = Array.from(th.parentNode.children).indexOf(th);
  var asc = th.dataset.asc !== '1';
  th.dataset.asc = asc ? '1' : '0';
  var rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort(function(a, b) {
    var av = a.cells[idx].innerText.replace(/[^0-9.-]/g,'');
    var bv = b.cells[idx].innerText.replace(/[^0-9.-]/g,'');
    var an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(function(r){ tbody.appendChild(r); });
}
"""

    avg_color = _score_color(avg)
    _R = 60
    _CIRC = 2 * 3.14159265 * _R
    passed_pct = (passed / total * 100) if total else 0
    passed_len = (passed / total * _CIRC) if total else 0
    donut_svg = f"""<svg viewBox="0 0 160 160" width="170" height="170">
  <circle cx="80" cy="80" r="{_R}" fill="none" stroke="var(--red)" stroke-width="22"/>
  <circle cx="80" cy="80" r="{_R}" fill="none" stroke="var(--green)" stroke-width="22"
    stroke-dasharray="{passed_len:.1f} {_CIRC:.1f}" transform="rotate(-90 80 80)"/>
  <text x="80" y="76" text-anchor="middle" font-size="30" font-weight="800" fill="#212529">{passed_pct:.0f}%</text>
  <text x="80" y="96" text-anchor="middle" font-size="11" fill="#6c757d">aprobados</text>
</svg>"""

    file_sums = summary.get("file_summaries", [])
    crit_avgs = []
    for k in _CRITERIA_ORDER:
        scores = [fs["criteria_scores"].get(k, 0) for fs in file_sums]
        cavg = (sum(scores) / len(scores)) if scores else 0
        crit_avgs.append((k, cavg))
    crit_avgs.sort(key=lambda x: x[1], reverse=True)
    bar_rows = ""
    for k, cavg in crit_avgs:
        ccolor = _score_color(cavg)
        bar_rows += (
            f'<div class="bar-row">'
            f'<span class="bar-label">{_e(_CRITERION_META[k]["nombre"])}</span>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{cavg:.1f}%;background:{ccolor}"></div></div>'
            f'<span class="bar-val" style="color:{ccolor}">{cavg:.0f}%</span>'
            f"</div>"
        )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Calidad de Datos — Bronze Layer</title>
<style>{css}</style>
</head>
<body>
<div class="page-header">
  <h1>Reporte de Calidad de Datos &mdash; Capa Bronze</h1>
  <p class="meta">Generado: {_e(date_str)} &nbsp;|&nbsp; Pipeline MEF/INEI ETL &nbsp;|&nbsp; Umbral de aprobación: {PASS_THRESHOLD:.0f}%</p>
</div>
<div class="container">
<div class="sec-title">Resumen General</div>
<div class="metric-cards">
  <div class="metric-card mc-blue"><div class="mc-label">Archivos analizados</div><div class="mc-value mc-blue" style="color:var(--blue)">{total}</div></div>
  <div class="metric-card mc-green"><div class="mc-label">Aprobados (&ge;{PASS_THRESHOLD:.0f}%)</div><div class="mc-value" style="color:var(--green)">{passed}</div><div class="mc-sub">de {total} archivos</div></div>
  <div class="metric-card mc-red"><div class="mc-label">Reprobados (&lt;{PASS_THRESHOLD:.0f}%)</div><div class="mc-value" style="color:var(--red)">{failed}</div></div>
  <div class="metric-card mc-avg"><div class="mc-label">Promedio general</div><div class="mc-value" style="color:{avg_color}">{avg:.1f}%</div></div>
</div>
<div class="sec-title">Visualización</div>
<div class="charts-row">
  <div class="chart-card"><div class="chart-title">Aprobados vs Reprobados</div>
    <div class="donut-wrap">{donut_svg}
      <div class="donut-legend">
        <span><span class="dot" style="background:var(--green)"></span>Aprobados ({passed})</span>
        <span><span class="dot" style="background:var(--red)"></span>Reprobados ({failed})</span>
      </div>
    </div>
  </div>
  <div class="chart-card"><div class="chart-title">Promedio por Criterio de Calidad</div>{bar_rows}</div>
</div>
<div class="sec-title">Criterios de Calidad</div>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:.55rem;margin-bottom:1rem">{legend_items}</div>
<div class="sec-title">Tabla Resumen — haz clic en los encabezados para ordenar</div>
<div class="card-section">
<table id="sumTable">
<thead><tr>
  <th onclick="sortTable(this)">Archivo</th>
  <th onclick="sortTable(this)">Filas</th>
  <th onclick="sortTable(this)">Cols</th>
  {crit_headers}
  <th onclick="sortTable(this)">Score Total</th>
  <th>Estado</th>
</tr></thead>
<tbody>{table_rows}</tbody>
</table>
</div>
<div class="sec-title">Detalle por Archivo — haz clic para expandir</div>
{accordions}
</div>
<script>{js}</script>
</body>
</html>"""

    path.write_text(html, encoding="utf-8")
