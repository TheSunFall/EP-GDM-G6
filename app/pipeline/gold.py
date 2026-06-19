"""Pipeline de la capa Gold con PySpark.

Recibe un SparkClient, lee silver desde parquet, construye las
dimensiones y marts de negocio y los escribe en parquet en data/gold/.
"""

from pathlib import Path

import shutil

from app.gold import transforms
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.spark import SparkClient


class GoldPipeline:
    def __init__(self, spark_client: SparkClient):
        self.spark = spark_client.get_session()
        self.logger = UnifiedLogger("GoldPipeline", "gold")

    def run(self, drop: bool = False):
        self.logger.info("Iniciando Gold Pipeline")

        try:
            silver_dir = Path(settings.project_root) / "data" / "silver"
            gold_dir = Path(settings.project_root) / "data" / "gold"

            if drop and gold_dir.exists():
                shutil.rmtree(gold_dir)

            # Construir dims + marts desde silver parquet
            self.logger.info("Construyendo dimensiones y marts...")
            dims, marts = transforms.build_all(self.spark, silver_dir)

            # Escribir a parquet
            self.logger.info("Escribiendo a parquet...")
            from app.gold.parquet_loader import save_all_to_parquet

            save_all_to_parquet(dims, marts, gold_dir)

            # Liberar caches
            for df in list(dims.values()) + list(marts.values()):
                df.unpersist()
        except Exception as e:
            self.logger.error(f"Error en Gold Pipeline: {e}", stack_trace=True)
            raise

        self.logger.info("Gold Pipeline completado")
