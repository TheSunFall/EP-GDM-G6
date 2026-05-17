from pathlib import Path
from typing import Literal

import pyarrow as pa

from app.client.base_client import BaseClient
from app.schemas.settings_schema import Dataset, DatasetDictionary
from app.settings.settings import settings

TYPES_MAP = {
    "Carácter": pa.string(),
    "Numérico": pa.float64(),
    "Fecha": pa.timestamp("s"),
}


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
        res = super().get(params={"resource_id": definition.id}, type="api")

        super().save(
            res, path, None, "file", f"{definition.filename}.parquet", "parquet"
        )
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
            data_dict = self.get_data_dict(datasets.dictionary[0])

        for module in datasets.modules:
            if use_schema == "individual" and module.dictionary:
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
            super().save(
                super().get(f"/{module.name}.zip", type=datasets.type),
                save_path,
                schema if schema is not None else None,
                "zip",
                f"{datasets.name}-{module.name}.parquet",
                datasets.save_format,
            )
