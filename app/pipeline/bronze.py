import pyarrow.parquet as pq

from app.client.mef_client import MefClient
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.manifest import bronze_entry, write_bronze_manifest


class BronzePipeline:
    def __init__(self):
        self.logger = UnifiedLogger("BronzePipeline", "bronze")

    def run(self):
        self.logger.info("Iniciando Bronze Pipeline")
        manifest_entries = []
        seen_files: set[str] = set()
        api_path = settings.project_root / settings.config.api.path

        for dataset in settings.config.datasets:
            self.logger.info(f"Descargando dataset: {dataset.name}")
            client = MefClient(fs_url=dataset.url)
            try:
                if dataset.name == "SIAF":
                    self.logger.info("Usando esquema global para SIAF")
                    client.get_zip(dataset, use_schema="global")
                elif dataset.name == "SISMEPRE":
                    self.logger.info("Usando esquema individual para SISMEPRE")
                    client.get_zip(dataset, use_schema="individual")
                elif dataset.name == "RENAMU":
                    self.logger.info("Usando esquema individual para RENAMU")
                    client.get_zip(dataset, use_schema="individual")
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
                pf = pq.ParquetFile(f)
                row_count = pf.metadata.num_rows
                file_size = f.stat().st_size
                module_name = f.name[len(prefix) : -len(".parquet")]
                manifest_entries.append(
                    bronze_entry(dataset.name, module_name, row_count, file_size)
                )
                self.logger.info(f"Bronze manifest: {f.name} → {row_count} filas, {file_size} bytes")

        if manifest_entries:
            manifest_path = api_path / "manifest.parquet"
            write_bronze_manifest(manifest_entries, manifest_path)
            self.logger.info(f"Bronze manifest escrito: {manifest_path} ({len(manifest_entries)} entradas)")

        self.logger.info("Bronze Pipeline completado")
