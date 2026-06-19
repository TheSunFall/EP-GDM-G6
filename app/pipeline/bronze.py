from pyspark.sql import SparkSession

from app.client.mef_client import MefClient
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.manifest import bronze_entry, read_bronze_manifest, write_bronze_manifest
from app.utils.spark import SparkClient


class BronzePipeline:
    def __init__(self, spark_client: SparkClient):
        self.spark: SparkSession = spark_client.get_session()
        self.logger = UnifiedLogger("BronzePipeline", "bronze")

    def run(self, skip_existing: bool = False):
        self.logger.info("Iniciando Bronze Pipeline")
        manifest_entries = []
        seen_files: set[str] = set()
        api_path = settings.project_root / settings.config.api.path

        # Leer módulos ya descargados si se solicita omitir existentes
        existing_modules: set[tuple[str, str]] = set()
        if skip_existing:
            manifest_path = api_path / "manifest.parquet"
            for entry in read_bronze_manifest(manifest_path, self.spark):
                existing_modules.add((entry["source_name"], entry["module_name"]))
            self.logger.info(
                f"Modo skip_existing: {len(existing_modules)} módulos ya registrados en manifest"
            )

        for dataset in settings.config.datasets:
            client = MefClient(spark=self.spark, fs_url=dataset.url)
            schema = {"SIAF": "global", "SISMEPRE": "individual", "RENAMU": "individual"}.get(
                dataset.name, "none"
            )

            if skip_existing:
                new_module_names = [
                    m.name for m in dataset.modules
                    if (dataset.name, m.name) not in existing_modules
                ]
                if not new_module_names:
                    self.logger.info(
                        f"Dataset {dataset.name}: todos los módulos ya existen, omitiendo"
                    )
                else:
                    self.logger.info(
                        f"Dataset {dataset.name}: descargando {len(new_module_names)} módulo(s) nuevo(s)"
                    )
                    client.get_zip(dataset, use_schema=schema, module_names=new_module_names)
            else:
                self.logger.info(f"Descargando dataset: {dataset.name}")
                try:
                    client.get_zip(dataset, use_schema=schema)
                    self.logger.info(f"Dataset {dataset.name} descargado exitosamente")
                except Exception as e:
                    self.logger.error(
                        f"Error descargando dataset {dataset.name}: {e}", stack_trace=True
                    )
                    raise

            prefix = f"{dataset.name}-"
            for f in sorted(api_path.glob(f"{prefix}*.parquet")):
                if f.name in seen_files:
                    continue
                seen_files.add(f.name)

                # PySpark cuenta filas (soporta directorios Spark y archivos únicos)
                row_count = self.spark.read.parquet(str(f)).count()

                # Tamaño: suma de part-files si es directorio Spark, stat() si es archivo
                if f.is_dir():
                    file_size = sum(p.stat().st_size for p in f.glob("part-*.parquet"))
                else:
                    file_size = f.stat().st_size

                module_name = f.name[len(prefix): -len(".parquet")]
                manifest_entries.append(
                    bronze_entry(dataset.name, module_name, row_count, file_size)
                )
                self.logger.info(
                    f"Bronze manifest: {f.name} -> {row_count} filas, {file_size} bytes"
                )

        if manifest_entries:
            manifest_path = api_path / "manifest.parquet"
            write_bronze_manifest(manifest_entries, manifest_path, self.spark)
            self.logger.info(
                f"Bronze manifest escrito: {manifest_path} ({len(manifest_entries)} entradas)"
            )

        self.logger.info("Bronze Pipeline completado")
