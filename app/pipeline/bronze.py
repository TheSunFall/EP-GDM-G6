from app.client.mef_client import MefClient
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger


class BronzePipeline:
    def __init__(self):
        self.logger = UnifiedLogger("BronzePipeline", "bronze")

    def run(self):
        self.logger.info("Iniciando Bronze Pipeline")
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
        self.logger.info("Bronze Pipeline completado")
