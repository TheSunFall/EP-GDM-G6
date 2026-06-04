from pathlib import Path
from typing import Literal

import pyarrow as pa

from app.client.base_client import BaseClient
from app.schemas.settings_schema import Dataset, DatasetDictionary
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger

TYPES_MAP = {
    "Carácter": pa.string(),
    "Numérico": pa.float64(),
    "Fecha": pa.timestamp("s"),
}

_logger = UnifiedLogger("MEFClient", "mef_client")


class MefClient(BaseClient):
    """Client to download MEF data"""

    def __init__(
        self,
        base_url: str = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search",
        fs_url: str = "https://fs.datosabiertos.mef.gob.pe/datastorefiles",
        timeout: float = settings.config.api.timeout,
    ):
        super().__init__(timeout, base_url, fs_url)

    def get_data_dict(
        self,
        definition: DatasetDictionary,
        path: str | Path = Path(settings.config.api.path) / "dicts",
    ):
        """Downloads a data diccionary via the Datos Abiertos API"""
        _logger.info(f"Descargando diccionario de datos: {definition.filename}")
        res = super().get(params={"resource_id": definition.id}, type="api")

        super().save(
            res, path, None, "file", f"{definition.filename}.parquet", "parquet"
        )
        _logger.info(f"Diccionario de datos guardado: {definition.filename}")
        return res.json()

    def get_zip(
        self,
        datasets: Dataset,
        use_schema: Literal["global", "individual", "none"] = "none",
        save_path: str | Path = settings.config.api.path,
    ):
        """
        Fetches all specified resources from Datos Abiertos and saves them as parquet

        Args:
            datasets: Datasets to download
            use_schema: Either "global" (one data dict for the whole dataset), "individual" (one data dict per module) or "none" (no data dict)
            save_path: Path to save the downloaded data
        """
        if use_schema == "global" and datasets.dictionary:
            _logger.info(f"Cargando diccionario global para {datasets.name}")
            data_dict = self.get_data_dict(datasets.dictionary[0])

        for module in datasets.modules:
            _logger.info(f"Procesando módulo: {module.name}")
            if use_schema == "individual" and module.dictionary:
                _logger.info(f"Descargando diccionario individual para {module.name}")
                data_dict = self.get_data_dict(module.dictionary[0])
                schema = (
                    pa.schema(
                        [
                            pa.field(
                                record["VARIABLE"],
                                TYPES_MAP.get(record["TIPO_DATO"], pa.string()),
                            )
                            for record in data_dict["records"]
                        ]
                    )
                    if data_dict is not None
                    else None
                )
            else:
                schema = None
            _logger.info(f"Descargando archivo: {module.name}.zip")
            super().save(
                super().get(f"{self.fs_url}/{module.name}.zip", type=datasets.type),
                save_path,
                schema if schema is not None else None,
                "zip",
                f"{datasets.name}-{module.name}.parquet",
                datasets.save_format,
            )
            _logger.info(f"Módulo {module.name} guardado exitosamente")
