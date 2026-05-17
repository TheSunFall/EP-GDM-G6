import csv
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Literal

import httpx
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from app.settings.settings import settings


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
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            http2=http2,
        )

        if fs_url is not None:
            self.fs_url = fs_url.rstrip("/")
            self.fs_client = httpx.Client(
                base_url=self.fs_url,
                timeout=timeout,
                http2=http2,
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

        if type == "api":
            response = self.client.get(
                url,
                params=params,
            )
        else:
            response = self.fs_client.get(
                url,
                params=params,
            )

        response.raise_for_status()

        return response

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
                csv_name = [f for f in z.namelist() if f.endswith(".csv")][0]

                with z.open(csv_name) as csv_file:
                    # Read sample for delimiter detection
                    sample = csv_file.read(4096).decode("utf-8", errors="ignore")

                    # Detect delimiter
                    delimiter = csv.Sniffer().sniff(sample).delimiter

                    # Rewind stream
                    csv_file.seek(0)

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

                    alt_filename = csv_name
        else:
            data = response.json()
            records = data["records"]
            table = pa.Table.from_pylist(records, schema=schema)
            alt_filename = data["result"]["resource_id"]

        if filename is not None:
            path = path / filename
        else:
            path = path / f"{alt_filename}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)

        if format == "parquet":
            pq.write_table(
                table,
                path,
                compression="zstd",
            )
        else:
            raise ValueError(f"Unsupported format: {format}")

    def close(self):
        """
        Close the HTTP connection.
        """
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
