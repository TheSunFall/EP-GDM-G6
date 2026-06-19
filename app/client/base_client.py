import csv
import os
import shutil
import tempfile
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Literal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("BaseClient", "base_client")

# Valores de cadena equivalentes a nulo (además de "" que Spark trata como null por defecto).
_NULL_STRINGS = [" ", "NA", "NULL", "null", "N/A"]


class BaseClient:
    """
    Cliente HTTP genérico con soporte PySpark para conversión CSV → Parquet.

    Args:
        timeout: Timeout antes de cancelar una request
        base_url: URL base para requests HTTP regulares
        fs_url: URL para descarga directa de archivos
        http2: Usar HTTP/2
        spark: Sesión Spark para conversión CSV → Parquet
    """

    def __init__(
        self,
        timeout: float,
        base_url: str,
        fs_url: str | None = None,
        http2: bool = True,
        spark: SparkSession | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.spark = spark
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            http2=http2,
            follow_redirects=True,
        )

        if fs_url is not None:
            self.fs_url = fs_url.rstrip("/")
            self.fs_client = httpx.Client(
                base_url=self.fs_url,
                timeout=timeout,
                http2=http2,
                follow_redirects=True,
            )

    def get(
        self,
        path: str | None = None,
        params: dict[str, str] | None = None,
        type: Literal["fs", "api"] = "api",
    ) -> httpx.Response:
        """
        Envía una request GET a la URL base.

        Args:
            path: Sub-ruta de la URL base
            params: Query params adicionales
            type: "api" (GET regular) o "fs" (descarga directa de archivo)
        """
        url = path or ""
        max_attempts = 5
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                if url.startswith("http://") or url.startswith("https://"):
                    response = httpx.get(
                        url, params=params, timeout=self.timeout, follow_redirects=True
                    )
                elif type == "api":
                    response = self.client.get(url, params=params)
                else:
                    response = self.fs_client.get(url, params=params)

                response.raise_for_status()
                _logger.info(f"Request exitoso: {url}")
                return response
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt < max_attempts:
                    wait_time = 2 ** (attempt - 1)
                    _logger.warning(
                        f"Intento {attempt}/{max_attempts} fallido para {url}: {exc}. Reintentando en {wait_time}s"
                    )
                    time.sleep(wait_time)
                else:
                    _logger.error(f"Todos los intentos fallidos para {url}", stack_trace=True)

        raise last_error  # type: ignore[misc]

    def save(
        self,
        response: httpx.Response,
        path: str | Path,
        schema,
        type: Literal["zip", "file"] = "file",
        filename: str | None = None,
        format: str = "parquet",
    ):
        """
        Guarda la respuesta HTTP en disco usando PySpark para conversión de datos.

        Args:
            response: Respuesta HTTP 200 OK
            path: Directorio de salida
            schema: Ignorado (PySpark infiere tipos; Silver maneja el casteo)
            type: "zip" (ZIP con CSV) o "file" (JSON de API)
            filename: Nombre del archivo de salida
            format: Formato de salida (solo "parquet" soportado)
        """
        if response.status_code != 200:
            raise ValueError(f"La respuesta retornó {response.status_code}")
        if len(response.content) == 0:
            raise ValueError("La respuesta está vacía.")
        if format != "parquet":
            raise ValueError(f"Formato no soportado: {format}")
        if self.spark is None:
            raise RuntimeError("Se requiere una SparkSession. Inicializa BaseClient con spark=session.")

        path = Path(path)

        if type == "zip":
            self._save_zip(response, path, filename)
        else:
            self._save_api_json(response, path, filename)

    def _save_zip(
        self,
        response: httpx.Response,
        path: Path,
        filename: str | None,
    ) -> None:
        """Extrae CSVs del ZIP y los convierte a parquet con PySpark."""
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith(".csv")]
            _logger.info(f"Procesando {len(csv_files)} archivo(s) CSV del ZIP")

            for csv_name in csv_files:
                with z.open(csv_name) as csv_file:
                    # Detección de encoding y delimitador (Python nativo — no requiere Spark)
                    raw_sample = csv_file.read(4096)
                    try:
                        raw_sample.decode("utf-8")
                        encoding = "utf-8"
                    except UnicodeDecodeError:
                        encoding = "cp1252"
                        _logger.info(f"Usando codificación cp1252 para {csv_name}")

                    delimiter = csv.Sniffer().sniff(
                        raw_sample.decode(encoding, errors="replace")
                    ).delimiter

                    # Extraer CSV completo a archivo temporal en disco.
                    # El lector CSV de Spark (JVM) solo acepta un conjunto limitado de
                    # charsets (utf-8, iso-8859-1, ...) y rechaza "cp1252". Para preservar
                    # todos los caracteres (tildes, ñ, comillas tipográficas) se transcodifica
                    # a UTF-8 aquí y Spark siempre lee UTF-8.
                    csv_file.seek(0)
                    with tempfile.NamedTemporaryFile(
                        mode="wb", suffix=".csv", delete=False, dir=str(path)
                    ) as tmp:
                        if encoding == "utf-8":
                            shutil.copyfileobj(csv_file, tmp)
                        else:
                            tmp.write(csv_file.read().decode(encoding, errors="replace").encode("utf-8"))
                        tmp_path = tmp.name

                try:
                    out_path = self._resolve_out_path(path, filename, csv_name, csv_files)
                    self._spark_csv_to_parquet(tmp_path, out_path, "utf-8", delimiter)
                finally:
                    os.unlink(tmp_path)

    def _save_api_json(
        self,
        response: httpx.Response,
        path: Path,
        filename: str | None,
    ) -> None:
        """Guarda la respuesta JSON de la API como parquet con PySpark."""
        data = response.json()
        records = data["records"]
        alt_filename = data["result"]["resource_id"]

        out_path = path / (filename if filename is not None else f"{alt_filename}.parquet")
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Se escribe con PyArrow en vez de spark.createDataFrame(records).write.parquet():
        # en PySpark 4.x escribir un DataFrame respaldado por una colección Python en memoria
        # usa pickle/Python workers que crashean en Windows ("Python worker exited
        # unexpectedly" / EOFException). Spark lee el parquet resultante sin problema.
        if out_path.is_dir():
            shutil.rmtree(out_path)
        elif out_path.is_file():
            out_path.unlink()
        table = pa.Table.from_pylist(records)
        pq.write_table(table, str(out_path), compression="zstd")
        _logger.info(f"Archivo guardado: {out_path} ({table.num_rows} filas)")

    def _resolve_out_path(
        self,
        path: Path,
        filename: str | None,
        csv_name: str,
        csv_files: list[str],
    ) -> Path:
        if len(csv_files) == 1 and filename is not None:
            return path / filename
        if filename is not None:
            return path / f"{Path(filename).stem}_{Path(csv_name).stem}.parquet"
        return path / f"{Path(csv_name).stem}.parquet"

    def _spark_csv_to_parquet(
        self,
        csv_path: str,
        out_path: Path,
        encoding: str,
        delimiter: str,
    ) -> None:
        """Lee un CSV con PySpark, reemplaza nulos y escribe parquet zstd."""
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df = (
            self.spark.read
            .option("header", "true")
            .option("encoding", encoding)
            .option("sep", str(delimiter))
            .option("nullValue", "")
            .option("mode", "DROPMALFORMED")
            .option("inferSchema", "false")
            .csv(csv_path)
        )

        # Reemplazar cadenas equivalentes a nulo en todas las columnas
        df = df.select([
            F.when(F.col(c).isin(_NULL_STRINGS), None).otherwise(F.col(c)).alias(c)
            for c in df.columns
        ])

        df = df.cache()
        row_count = df.count()
        df.write.option("compression", "zstd").mode("overwrite").parquet(str(out_path))
        df.unpersist()

        _logger.info(f"Archivo guardado: {out_path} ({row_count} filas)")

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
