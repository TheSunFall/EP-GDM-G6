from pathlib import Path
from typing import Literal

from pyspark.sql import SparkSession

from app.client.base_client import BaseClient
from app.schemas.settings_schema import Dataset, DatasetDictionary
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("MEFClient", "mef_client")


class MefClient(BaseClient):
    """Cliente para descarga de datos MEF (Datos Abiertos)."""

    def __init__(
        self,
        spark: SparkSession,
        base_url: str = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search",
        fs_url: str = "https://fs.datosabiertos.mef.gob.pe/datastorefiles",
        timeout: float = settings.config.api.timeout,
    ):
        super().__init__(timeout, base_url, fs_url, spark=spark)

    def get_data_dict(
        self,
        definition: DatasetDictionary,
        path: str | Path = Path(settings.config.api.path) / "dicts",
    ):
        """Descarga un diccionario de datos vía la API Datos Abiertos."""
        _logger.info(f"Descargando diccionario de datos: {definition.filename}")
        res = super().get(params={"resource_id": definition.id}, type="api")
        super().save(res, path, None, "file", f"{definition.filename}.parquet", "parquet")
        _logger.info(f"Diccionario de datos guardado: {definition.filename}")
        return res.json()

    def get_zip(
        self,
        datasets: Dataset,
        use_schema: Literal["global", "individual", "none"] = "none",
        save_path: str | Path = settings.config.api.path,
        module_names: list[str] | None = None,
    ):
        """
        Descarga todos los recursos especificados y los guarda como parquet con PySpark.

        El schema del diccionario de datos ya no se aplica en bronze: PySpark infiere
        los tipos como string y la capa Silver (quality.py) realiza el casteo explícito.

        Args:
            datasets: Dataset a descargar
            use_schema: "global", "individual" o "none" — controla si se descarga el
                        diccionario (se descarga pero no se aplica como schema de tipos)
            save_path: Ruta de salida
            module_names: Lista opcional de nombres de módulos a descargar. Si es None,
                         se descargan todos los módulos del dataset.
        """
        modules = [m for m in datasets.modules if m.name in module_names] if module_names else datasets.modules
        if not modules:
            _logger.info(f"Dataset {datasets.name}: ningún módulo nuevo que descargar")
            return

        if use_schema == "global" and datasets.dictionary:
            _logger.info(f"Descargando diccionario global para {datasets.name}")
            self.get_data_dict(datasets.dictionary[0])

        for module in modules:
            _logger.info(f"Procesando módulo: {module.name}")

            if use_schema == "individual" and module.dictionary:
                _logger.info(f"Descargando diccionario individual para {module.name}")
                self.get_data_dict(module.dictionary[0])

            _logger.info(f"Descargando archivo: {module.name}.zip")
            super().save(
                super().get(f"{self.fs_url}/{module.name}.zip", type=datasets.type),
                save_path,
                None,
                "zip",
                f"{datasets.name}-{module.name}.parquet",
                datasets.save_format,
            )
            _logger.info(f"Módulo {module.name} guardado exitosamente")
