# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Medallion data lakehouse (**bronze → profile → silver → gold**) that downloads Peruvian
government fiscal data, validates it, and loads a star schema + BI marts into SQL Server.
PySpark 4.1 for compute, SQL Server 2022 (Docker) for storage. Three datasets:
**SIAF** (ingresos, MEF), **SISMEPRE** (impuesto predial, MEF), **RENAMU** (registro
municipal, INEI).

## Commands

Dependencies are managed with **uv**; the pipeline runs **locally** (only SQL Server is
containerized — there is no app container).

```bash
uv sync                          # install deps into .venv
docker compose up -d             # start SQL Server (host 11423 → 1433); REQUIRED before silver/gold

# pipeline stages — run from repo root with .venv active, or prefix `uv run`
python main.py                   # everything: bronze → profile → silver → gold
python main.py bronze            # download + convert to zstd parquet (data/bronze/)
python main.py profile           # 8-criteria quality report (data/profiling/*.html|json)
python main.py silver --drop     # quality+filter → star schema → SQL Server `silver` schema
python main.py gold --drop       # build BI marts from silver → SQL Server `gold` schema
python main.py silver quality    # single silver step: quality | schema | load
```

- **Requires Java 17 or 21** with `JAVA_HOME` set — newer Java breaks PySpark.
- SA password is read from `.env` (`MSSQL_SA_PASSWORD`); the database is `DLKH_MEF` (see `config.yaml`).
- **No tests, linting, or CI** are configured.
- `--drop` recreates the schema; loads truncate+overwrite, so reruns are idempotent.
- Inspect results (the quick path — Power BI is the only other consumer):
  ```bash
  docker exec ep-gdm-g6-sqlserver-1 /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d DLKH_MEF -Q "<sql>"
  ```

## Architecture

`main.py` (argparse subcommands) drives four stages; each is a pipeline class in `app/pipeline/`:

- **bronze** (`bronze.py`): HTTP clients in `app/client/` fetch SIAF/SISMEPRE (MEF datastore)
  and RENAMU (INEI `.zip`) → parquet in `data/bronze/` + `manifest.parquet`.
- **profile** (`profile.py` + `app/utils/profiler.py`): scores each bronze parquet against 8
  quality criteria → `data/profiling/`.
- **silver** (`silver.py`): 3 ordered steps — `quality` (`app/silver/quality.py`: corrections +
  filters → `data/silver/stage/*.parquet`), `schema` (`app/silver/transforms.py`: ~14 dimensions
  + 3 facts), `load` (`app/silver/loader.py`).
- **gold** (`gold.py` + `app/gold/transforms.py::build_all`): pre-aggregated BI marts read from
  the silver SQL tables.

**Loader pattern (silver & gold both):** table DDL is created with **pymssql** by executing
`sql/tables_{silver,gold}.sql` (split on the `go` keyword), then data is written with **Spark
JDBC** (`mode("overwrite")`, `truncate=true`). Large marts set `isolationLevel=NONE` to avoid
lock exhaustion — see `app/gold/loader.py::write_table`. `MART_RENAMU` is ~12.7M rows and
dominates gold runtime (~3h).

**Config:** `config.yaml` defines the datasets and the SQL Server connection (`database: DLKH_MEF`,
`port: 11423`, schemas `silver`/`gold`). `app/settings/settings.py` is a singleton that loads it
at import and interpolates `${MSSQL_SA_PASSWORD}` from `.env`.

## Hard constraints & conventions

- **No Python UDFs in Spark.** On Windows the Python workers crash (`Python worker exited
  unexpectedly` / `EOFException`). All transformations must use native `pyspark.sql.functions`
  (`F.translate`, `F.regexp_replace`, `F.split`, `F.when`, …).
- **Language rule (AGENTS.md):** all *code* in English; all *human-facing docs* (pydoc, README,
  doc comments) in **Spanish**.
- **Mixed time grain:** SIAF facts are monthly (`AnioMes` → `DIM_CALENDARIO`); SISMEPRE & RENAMU
  are annual (`Anio` → `DIM_ANIO`). Don't force a single date table across marts.
- `.env` is committed on purpose in this repo — do not untrack or rewrite it.

## Municipality category enrichment (A–G)

`DIM_EJECUTORA.CATEGORIA` and the gold `Categoria` columns (`MART_INGRESOS_EJECUTORA`,
`MART_PREDIAL`) classify municipalities A–G. The mapping is **curated, not matched at runtime**:

- **Source of truth:** `data/categorias_secejec.csv` (`SecEjec,Categoria`, ~99.6% of 1,892
  municipalities). `app/silver/quality.py::fix_categorias_municipalidades` simply reads it.
- **Regenerate** with `python tools/generar_categorias.py` (run from repo root; reads bronze
  parquet via PyArrow, no Spark/DB). It cross-matches `data/CategoriasMunicipalidades.csv`
  (MEF A–G list — names only, no ubigeo) against SIAF names: normalization + homonym resolution
  by department order + fuzzy matching + hand-verified overrides. Ambiguous cases stay blank.
- `quality.py::fix_all` filters SIAF to municipalities only: `NIVEL_GOBIERNO` LOCAL **and** name
  contains `MUNICIPALIDAD` and not `MANCOMUNIDAD`. SISMEPRE/RENAMU rows are not filtered.

## Gotchas

- `README.md` and `AGENTS.md` are partly **stale**: they cite host port **1434** (real: **11423**),
  call gold an "empty stub" (it is a full pipeline now), and predate the category/mancomunidad work.
- RENAMU indicators (`NombreCampo`) are opaque questionnaire codes (`P03`, `C96_1`…) with no
  readable labels in the data — labeling them needs the external INEI dictionary.
