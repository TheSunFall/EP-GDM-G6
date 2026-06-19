from pathlib import Path

import shutil

from app.settings.settings import settings
from app.silver import quality, transforms
from app.utils.logging import UnifiedLogger
from app.utils.manifest import bronze_unchanged
from app.utils.spark import SparkClient


class SilverPipeline:
    def __init__(self, spark_client: SparkClient):
        self.spark = spark_client.get_session()
        self.logger = UnifiedLogger("SilverPipeline", "silver")

    def run(self, step: str | None = None, drop: bool = False, skip_unchanged: bool = False, municipios: str = "legacy"):
        self.logger.info(f"Iniciando Silver Pipeline (municipios={municipios})")

        if skip_unchanged:
            bronze_manifest_path = settings.project_root / settings.config.api.path / "manifest.parquet"
            stage_manifest_path = settings.project_root / settings.config.silver.path / "stage" / "manifest.parquet"
            unchanged, detail = bronze_unchanged(bronze_manifest_path, stage_manifest_path, self.spark)
            if unchanged:
                self.logger.info(
                    f"skip_unchanged: {detail} — datos sin cambios, omitiendo silver pipeline"
                )
                return
            else:
                self.logger.info(
                    f"skip_unchanged: {detail} — datos cambiados, ejecutando silver pipeline"
                )

        try:
            # 1. Quality
            self.logger.info("Paso 1/3: Quality fixes")
            stage_dir = Path(settings.project_root) / "data" / "silver" / "stage"
            stage = quality.fix_all(self.spark, municipios=municipios)

            # 2. Schema (transformaciones)
            self.logger.info("Paso 2/3: Building schema (dims + facts)")
            dims, facts = transforms.build_all(self.spark, stage, municipios=municipios)

            # 3. Write (parquet)
            self.logger.info("Paso 3/3: Writing to parquet")
            from app.silver.parquet_loader import save_all_to_parquet

            output_dir = Path(settings.project_root) / "data" / "silver"
            if drop and output_dir.exists():
                shutil.rmtree(output_dir)
            save_all_to_parquet(dims, facts, output_dir)

        except Exception as e:
            self.logger.error(f"Error en Silver Pipeline: {e}", stack_trace=True)
            raise

        self.logger.info("Silver Pipeline completado")
