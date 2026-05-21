import csv
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Literal

import httpx
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from app.settings.settings import settings
from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("base_client")


class BaseClient:
    """
    Generic API client.

    Args:
        timeout: Timeout before canceling a request
        base_url: URL to point regular HTTP requests at
        fs_url: URL to download files directly (ej. .zip files)
        http2: Use HTTP2 instead of HTTP1
    """

    def __init__(
        self,
        timeout: float,
        base_url: str,
        fs_url: str | None = None,
        http2: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
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
        Send a GET request at the base URL.

        Args:
            path: Subpath of the base URL to query
            params: Query params to add
            type: "api" (use regular HTTP GET requests) or "fs" (download a file directly)
        """
        url = path or ""

        max_attempts = 5
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                if url.startswith("http://") or url.startswith("https://"):
                    response = httpx.get(url, params=params, timeout=self.timeout, follow_redirects=True)
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
        Save a completed response to disk.

        Args:
            response: A httpx.Response object to read from,. The response must be 200 OK.
            path: Path to save the file to.
            schema: PyArrow schema to apply to the file
            type: Either "zip" (the response contains a zipfile) or "api" (the response contains a regular HTTP response)
            filename: Name for the saved file.
            format: Format to save to. Currently supported: parquet
        """
        if response.status_code != 200:
            raise ValueError(f"The response returned {response.status_code}")
        elif len(response.content) == 0:
            raise ValueError("The response is empty.")

        path = Path(path)

        if type == "zip":
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                _logger.info(f"Procesando {len(csv_files)} archivo(s) CSV del ZIP")

                for csv_name in csv_files:
                    with z.open(csv_name) as csv_file:
                        try:
                            sample = csv_file.read(4096).decode("utf-8")
                            encoding = "utf-8"
                        except UnicodeDecodeError:
                            sample = csv_file.read(4096).decode("cp1252")
                            encoding = "cp1250"
                            _logger.info(f"Usando codificación cp1252 para {csv_name}")

                        delimiter = csv.Sniffer().sniff(sample).delimiter
                        csv_file.seek(0)

                        read_opts = pv.ReadOptions(encoding=encoding)
                        parse_opts = pv.ParseOptions(
                            delimiter=delimiter,
                            invalid_row_handler=lambda row: "skip",
                        )
                        convert_opts = pv.ConvertOptions(
                            column_types=schema if schema is not None else None,
                            null_values=["", " ", "NA", "NULL", "null", "N/A"],
                            strings_can_be_null=True,
                            quoted_strings_can_be_null=True,
                            timestamp_parsers=[
                                "%d/%m/%Y %H:%M:%S",
                                "%Y-%m-%d",
                                "%Y-%m-%d %H:%M:%S",
                            ],
                        )
                        table = pv.read_csv(
                            csv_file,
                            parse_options=parse_opts,
                            convert_options=convert_opts,
                        )

                    if len(csv_files) == 1 and filename is not None:
                        out_path = path / filename
                    elif filename is not None:
                        out_path = (
                            path
                            / f"{Path(filename).stem}_{Path(csv_name).stem}.parquet"
                        )
                    else:
                        out_path = path / f"{Path(csv_name).stem}.parquet"

                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    if format == "parquet":
                        pq.write_table(table, out_path, compression="zstd")
                    else:
                        raise ValueError(f"Unsupported format: {format}")

                    _logger.info(f"Archivo guardado: {out_path} ({len(table)} filas)")
        else:
            data = response.json()
            records = data["records"]
            table = pa.Table.from_pylist(records, schema=schema)
            alt_filename = data["result"]["resource_id"]

            out_path = path / (
                filename if filename is not None else f"{alt_filename}.parquet"
            )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if format == "parquet":
                pq.write_table(table, out_path, compression="zstd")
            else:
                raise ValueError(f"Unsupported format: {format}")

            _logger.info(f"Archivo guardado: {out_path} ({len(table)} filas)")

    def close(self):
        """
        Close the HTTP connection.
        """
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
