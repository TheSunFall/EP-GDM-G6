# AGENTS.md

## Developer Commands

- `uv sync` — install dependencies
- `python main.py` — run the bronze pipeline (downloads data from Peruvian government open data APIs)
- `python main.py profile` — run the profiler (evaluates bronze parquet files, generates JSON + HTML reports under `data/profiling/`)
- `python main.py silver` — run the silver pipeline (quality fixes, star schema, loads to SQL Server)
- `docker compose up` — start SQL Server container (maps host port **1434** → container 1433)

## Setup

- Requires **Java 17 or 21** with `JAVA_HOME` set. Higher Java versions break PySpark.
- Requires `uv` package manager, Python 3.12.
- SQL Server needs `MSSQL_SA_PASSWORD` env var (no `.env` file tracked; set manually).
- Run `docker compose up` before any pipeline that writes to the database.

## Architecture

Medallion Data Lakehouse pipeline for Peruvian government data (MEF/INEI):

```
main.py → BronzePipeline → downloads CSVs via HTTP, converts to parquet → data/bronze/
main.py silver → SilverPipeline → quality fixes, star schema dims/facts, loads to SQL Server → data/silver/stage/ + SQL Server
profile.py → profiler pipeline → evaluates bronze parquet files against 8 quality criteria → data/profiling/
gold.py — exists as empty stub
```

- **Config**: `config.yaml` defines 3 datasets (SIAF, SISMEPRE, RENAMU) with modules and dictionary IDs.
- **Settings**: singleton `app/settings/settings.py` loads config at import time from `config.yaml`.
- **Data clients**: `app/client/mef_client.py` (MEF API) and `app/client/renamu_client.py` (INEI). Both use HTTP2.
- **Spark**: `app/utils/spark.py` configures 4g driver memory + SQL Server JDBC driver. Used by profiler and silver pipeline.
- **Profiler**: `app/utils/profiler.py` evaluates bronze parquet files against 8 data quality criteria (exactitud, completitud, consistencia, integridad, razonabilidad, oportunidad, unicidad, validez) using PySpark. Generates JSON and HTML reports under `data/profiling/`. Run via `python main.py profile`.
- **Silver Pipeline**: `app/pipeline/silver.py` orchestrates 3 steps: (1) quality fixes on bronze parquet → `data/silver/stage/`, (2) builds star schema dimensions and fact tables, (3) creates SQL Server tables via `sql/tables_silver.sql` (pymssql) and loads data via Spark JDBC. Modules: `app/silver/quality.py` (type casts, null coalescing, dedup), `app/silver/transforms.py` (14 dims + 3 facts), `app/silver/loader.py` (DDL + append to SQL Server with IDENTITY column handling). Run via `python main.py silver`.
- **Output**: parquet files with zstd compression under `data/bronze/` (gitignored). Stage parquet under `data/silver/stage/` (gitignored). Profiling reports under `data/profiling/` (gitignored). SQL Server tables under `silver` schema. Logs under `logs/` (gitignored).

## Key Quirks

- `BronzePipeline` creates two `MefClient` instances — one for MEF API, one with a custom base URL for RENAMU.
- RENAMU data is fetched directly as `.zip` files from INEI, not via the MEF datastore API.
- SIAF uses a "global" data dictionary schema; SISMEPRE and RENAMU use "individual" per-module schemas.

## Rules

- All of the code should be written in English. Any form of user or developer oriented documentation (pydoc, README, etc) should be written in Spanish.

## Testing / CI

No tests, CI, linting, or type-checking configured.
