"""Punto de entrada del pipeline Medallion: Bronze → Profile → Silver → Gold → PowerBI."""

import argparse

from app.pipeline.bronze import BronzePipeline
from app.pipeline.silver import SilverPipeline
from app.utils.spark import SparkClient


def main():
    """Parsea argumentos y despacha al subcomando correspondiente."""
    parser = argparse.ArgumentParser(
        description="Medallion Data Lakehouse — datos fiscales peruanos (MEF/INEI)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Etapa del pipeline a ejecutar")

    bronze_parser = subparsers.add_parser(
        "bronze",
        help="Descarga fuentes MEF/INEI y convierte a parquet (data/bronze/)",
    )
    bronze_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Omite descarga de módulos que ya existen en el bronze manifest.",
    )

    profile_parser = subparsers.add_parser(
        "profile",
        help="Perfilado de calidad sobre bronze (data/profiling/)",
    )
    profile_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Omite perfilado de archivos que ya tienen reporte en data/profiling/.",
    )

    silver_parser = subparsers.add_parser(
        "silver",
        help="Limpieza + esquema estrella → parquet (data/silver/)",
    )
    silver_parser.add_argument(
        "step",
        nargs="?",
        choices=["quality", "schema", "load"],
        default=None,
        help="Paso silver: quality | schema | load. Sin valor: ejecuta los tres.",
    )
    silver_parser.add_argument(
        "--drop",
        action="store_true",
        help="Elimina data/silver/ antes de crear (ejecución idempotente).",
    )
    silver_parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="Omite silver si los row counts de bronze no cambiaron.",
    )
    silver_parser.add_argument(
        "--municipios",
        choices=["legacy", "drop", "remap"],
        default="drop",
        help=(
            "Estrategia de categoría y deduplicación de municipios en DIM_EJECUTORA. "
            "drop (por defecto, 'método C'): Lima=C / resto=G, una fila por nombre "
            "(primera encontrada), pierde los hechos de los SEC_EJEC no canónicos. "
            "legacy: A-G según Anexo IV DS 003-2026-EF, clave=SEC_EJEC (sin dedup por nombre). "
            "remap: Lima=C / resto=G, una fila por nombre, reasigna todos los SEC_EJEC "
            "al canónico en FACT_INGRESO y FACT_FORMULARIO_SISMEPRE (sin pérdida de datos)."
        ),
    )

    gold_parser = subparsers.add_parser(
        "gold",
        help="Construye marts de negocio desde silver → parquet (data/gold/)",
    )
    gold_parser.add_argument(
        "--drop",
        action="store_true",
        help="Elimina data/gold/ antes de crear (ejecución idempotente).",
    )

    powerbi_parser = subparsers.add_parser(
        "powerbi",
        help="Exporta marts Gold a archivos parquet únicos para Power BI (data/powerbi/)",
    )
    powerbi_parser.add_argument(
        "--drop",
        action="store_true",
        help="Elimina data/powerbi/ antes de exportar.",
    )

    args = parser.parse_args()

    if args.command is None:
        _run_all()
    elif args.command == "bronze":
        _run_bronze(getattr(args, "skip_existing", False))
    elif args.command == "profile":
        _run_profile(getattr(args, "skip_existing", False))
    elif args.command == "silver":
        _run_silver(
            args.step,
            getattr(args, "drop", False),
            getattr(args, "skip_unchanged", False),
            getattr(args, "municipios", "drop"),
        )
    elif args.command == "gold":
        _run_gold(getattr(args, "drop", False))
    elif args.command == "powerbi":
        _run_powerbi(getattr(args, "drop", False))


def _run_all():
    """Ejecuta el pipeline completo de inicio a fin con un solo comando.

    bronze → profile → silver (método C: --municipios drop) → gold → powerbi.
    Usa drop=True en cada etapa para una reconstrucción limpia e idempotente.
    """
    _run_bronze()
    _run_profile()
    _run_silver(drop=True, municipios="drop")
    _run_gold(drop=True)
    _run_powerbi(drop=True)


def _run_bronze(skip_existing: bool = False):
    """Descarga fuentes MEF/INEI y convierte CSV → parquet con PySpark."""
    spark = SparkClient()
    pipeline = BronzePipeline(spark)
    pipeline.run(skip_existing=skip_existing)


def _run_profile(skip_existing: bool = False):
    """Perfila la calidad de los parquets bronze con 8 criterios."""
    from app.pipeline.profile import run as run_profile

    run_profile(argparse.Namespace(skip_existing=skip_existing))


def _run_silver(
    step: str | None = None,
    drop: bool = False,
    skip_unchanged: bool = False,
    municipios: str = "drop",
):
    """Ejecuta limpieza de calidad, esquema estrella y carga a silver."""
    spark = SparkClient()
    pipeline = SilverPipeline(spark)
    pipeline.run(step=step, drop=drop, skip_unchanged=skip_unchanged, municipios=municipios)


def _run_gold(drop: bool = False):
    """Construye dimensiones y marts gold desde silver."""
    from app.pipeline.gold import GoldPipeline

    spark = SparkClient()
    pipeline = GoldPipeline(spark)
    pipeline.run(drop=drop)


def _run_powerbi(drop: bool = False):
    """Consolida marts gold en archivos parquet únicos para Power BI."""
    from app.pipeline.powerbi import PowerBIPipeline

    spark = SparkClient()
    pipeline = PowerBIPipeline(spark)
    pipeline.run(drop=drop)


if __name__ == "__main__":
    main()
