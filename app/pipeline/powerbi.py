"""Pipeline de exportación Power BI.

Lee los marts de data/gold/ y los consolida en archivos parquet únicos
en data/powerbi/ usando PySpark con coalesce(1).
"""

from pathlib import Path

from pyspark.sql import SparkSession

from app.gold.powerbi_exporter import export_for_powerbi
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.spark import SparkClient


class PowerBIPipeline:
    """Consolida los marts Gold en archivos parquet únicos optimizados para Power BI."""

    def __init__(self, spark_client: SparkClient) -> None:
        """Inicializa el pipeline con una sesión PySpark activa."""
        self.spark: SparkSession = spark_client.get_session()
        self.logger = UnifiedLogger("PowerBIPipeline", "powerbi")

    def run(self, drop: bool = False) -> None:
        """Exporta todos los marts Gold a data/powerbi/ como archivos parquet únicos."""
        self.logger.info("Iniciando PowerBI Pipeline")

        gold_dir = Path(settings.project_root) / "data" / "gold"
        output_dir = Path(settings.project_root) / "data" / "powerbi"

        if not gold_dir.exists():
            raise FileNotFoundError(
                f"Directorio gold no encontrado: {gold_dir}. "
                "Ejecuta 'python main.py gold' primero."
            )

        try:
            manifest = export_for_powerbi(
                spark=self.spark,
                gold_dir=gold_dir,
                output_dir=output_dir,
                drop=drop,
            )
        except Exception as e:
            self.logger.error(f"Error en PowerBI Pipeline: {e}", stack_trace=True)
            raise

        tables = manifest["tables"]
        total_rows = sum(t["rows"] for t in tables.values())
        total_kb = sum(t["size_kb"] for t in tables.values())
        self.logger.info(
            f"PowerBI Pipeline completado: {len(tables)} tablas exportadas, "
            f"{total_rows:,} filas totales, {total_kb / 1024:.1f} MB"
        )
        self.logger.info(f"Archivos listos en: {output_dir}")
