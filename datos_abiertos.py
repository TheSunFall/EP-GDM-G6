from datetime import date
from pathlib import Path

import pyarrow as pa

from app.client.base_client import BaseClient
from app.schemas.settings_schema import Dataset, DatasetDictionary
from app.settings.settings import settings
from app.utils.logging import ConsoleLogger

TYPES_MAP = {"Carácter": pa.string(), "Numérico": pa.float64(), "Fecha": pa.date32()}


class DatosAbiertosClient(BaseClient):
    def __init__(
        self,
        base_url: str = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search",
        timeout: float = settings.config.api.timeout,
    ):
        super().__init__(base_url, timeout)

    def get_data_dict(
        self,
        definition: DatasetDictionary,
        path: str | Path = Path(settings.config.api.path) / "dicts",
    ):
        res = super().get(params={"resource_id": definition.id})
        super().save(res, path, None, f"{definition.filename}.parquet", "parquet")
        return res.json()["records"]

    def get_resources(
        self,
        datasets: Dataset,
        path: str | None = None,
        use_schema: bool = False,
        save_path: str | Path = settings.config.api.path,
    ):
        """
        Fetches all specified resources from Datos Abiertos and saves them as parquet

        Args:
            resources: Dict of resources specified in the config
            path: Path to save the downloaded data
        """

        for module in datasets.modules:
            res = super().get(path, {"resource_id": module.id})
            res.next_request
            super().save(
                super().get(path, {"resource_id": module.id}),
                save_path,
                None,
                f"{datasets.name}-{module.name}.parquet",
                format=datasets.save_format,
            )
