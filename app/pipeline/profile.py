import argparse
from pathlib import Path

from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.profiler import profile_directory

logger = UnifiedLogger("Profiler", "profiler")


def run(args: argparse.Namespace) -> None:
    parquet_root = (
        Path(args.parquet_dir)
        if getattr(args, "parquet_dir", None)
        else (settings.project_root / settings.config.api.path)
    )
    report_dir = (
        Path(args.report_dir)
        if getattr(args, "report_dir", None)
        else (settings.project_root / "data" / "profiling")
    )

    if not parquet_root.exists():
        logger.error(f"Parquet directory not found: {parquet_root}")
        logger.error("Run 'bronze' first to generate Bronze Parquet files.")
        return

    all_parquet_files = list(parquet_root.rglob("*.parquet"))
    if not all_parquet_files:
        logger.error(f"No Parquet files found under: {parquet_root}")
        return

    parquet_files = all_parquet_files
    if getattr(args, "skip_existing", False) and report_dir.exists():
        existing_stems = {f.stem.replace("_quality", "") for f in report_dir.glob("*_quality.json")}
        parquet_files = [p for p in parquet_files if p.stem not in existing_stems]
        if not parquet_files:
            logger.info(
                f"Todos los {len(all_parquet_files)} archivos ya tienen reporte en {report_dir}, omitiendo"
            )
            return
        logger.info(
            f"skip_existing: {len(parquet_files)} nuevos de {len(all_parquet_files)} archivos totales"
        )

    logger.info(
        f"Starting data quality profiling: {len(parquet_files)} files, output directory: {str(report_dir)}"
    )
    logger.info(f"Scanning: {parquet_root}")
    logger.info(f"Files   : {len(parquet_files)}")
    logger.info(f"Output  : {report_dir}")

    reports = profile_directory(parquet_root, report_dir, parquet_files=parquet_files)

    logger.info(f"\n{'ARCHIVO':<45} {'FILAS':>8} {'SCORE':>7}  ESTADO")
    logger.info("-" * 70)
    for r in reports:
        name = Path(r["file"]).name[:44]
        score = r["overall_score"]
        status = "APROBADO" if r["passed"] else "REPROBADO"
        logger.info(f"{name:<45} {r['rows']:>8,} {score:>6.1f}%  {status}")

    avg = sum(r["overall_score"] for r in reports) / len(reports) if reports else 0
    passed_count = sum(1 for r in reports if r["passed"])
    logger.info(
        f"\nPromedio general: {avg:.1f}%  |  Aprobados: {passed_count}/{len(reports)}"
    )
    logger.info(f"\nReporte HTML    : {report_dir / 'profiling_summary.html'}")
    logger.info(f"Reportes JSON   : {report_dir}")
    logger.info("Profiling completado exitosamente")
