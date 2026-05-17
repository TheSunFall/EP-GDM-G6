from app.client.mef_client import MefClient
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger


class BronzePipeline:
    def __init__(self):
        self.logger = UnifiedLogger("bronze_pipeline")
        self.client = MefClient()
        self.renamu_client = MefClient(
            "https://www.datosabiertos.gob.pe/",
            "https://www.inei.gob.pe/media/DATOS_ABIERTOS/RENAMU/DATA",
        )

    def run(self):
        for dataset in settings.config.datasets:
            if dataset.name == "SIAF":
                self.client.get_zip(dataset, use_schema="global")
            elif dataset.name == "SISMEPRE":
                self.client.get_zip(dataset, use_schema="individual")
            elif dataset.name == "RENAMU":
                self.renamu_client.get_zip(dataset, use_schema="individual")
