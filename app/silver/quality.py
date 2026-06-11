"""Correcciones de calidad sobre archivos Parquet de la capa Bronze."""

from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.manifest import STAGE_TO_BRONZE_SOURCES, bronze_row_count_map, stage_entry, write_stage_manifest

_logger = UnifiedLogger("SilverQuality", "silver")

# Rutas absolutas basadas en la raíz del proyecto
_BRONZE = settings.project_root / settings.config.api.path
_STAGE = settings.project_root / settings.config.silver.path / "stage"


# ── helpers ───────────────────────────────────────────────────────────────────


def _year_float(col_name: str):
    """Corrige años codificados como float científico (2.021e3) → int 2021."""
    return F.col(col_name).cast("double").cast("int")


def _smallint_float(col_name: str):
    return F.col(col_name).cast("double").cast("smallint")


def _int_float(col_name: str):
    return F.col(col_name).cast("double").cast("int")


def _log_count(name: str, df: DataFrame) -> DataFrame:
    _logger.info(f"{name}: {df.count()} filas después de correcciones")
    return df


# ── correcciones por dataset ───────────────────────────────────────────────────


def fix_ingreso(spark: SparkSession) -> DataFrame:
    """Une todos los SIAF-*-Ingreso*.parquet y aplica correcciones de calidad."""
    files = sorted(_BRONZE.glob("SIAF-*-Ingreso*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos SIAF-*-Ingreso*.parquet en {_BRONZE}"
        )

    dfs = [spark.read.parquet(str(f)) for f in files]
    unified = dfs[0]
    for df in dfs[1:]:
        unified = unified.unionByName(df, allowMissingColumns=True)

    unified = (
        unified.withColumn("SEC_EJEC", _int_float("SEC_EJEC"))
        .withColumn("ANO_DOC", _int_float("ANO_DOC"))
        .withColumn("MES_DOC", _int_float("MES_DOC"))
        .withColumn(
            "MONTO_PIA",
            F.coalesce(F.col("MONTO_PIA").cast("bigint"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "MONTO_PIM",
            F.coalesce(F.col("MONTO_PIM").cast("bigint"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "MONTO_RECAUDADO",
            F.coalesce(F.col("MONTO_RECAUDADO").cast("double"), F.lit(0.0)),
        )
        .withColumn("DEPARTAMENTO_EJECUTORA", _smallint_float("DEPARTAMENTO_EJECUTORA"))
        .withColumn("PROVINCIA_EJECUTORA", _smallint_float("PROVINCIA_EJECUTORA"))
        .withColumn("DISTRITO_EJECUTORA", _smallint_float("DISTRITO_EJECUTORA"))
        .withColumn("ESPECIFICA", _smallint_float("ESPECIFICA"))
        .withColumn("ESPECIFICA_DET", _smallint_float("ESPECIFICA_DET"))
        .withColumn("GENERICA", _smallint_float("GENERICA"))
        .withColumn("SUBGENERICA", _smallint_float("SUBGENERICA"))
        .withColumn("SUBGENERICA_DET", _smallint_float("SUBGENERICA_DET"))
        .withColumn("RUBRO", _smallint_float("RUBRO"))
        .withColumn("FUENTE_FINANCIAMIENTO", _smallint_float("FUENTE_FINANCIAMIENTO"))
        .dropDuplicates()
    )
    return _log_count("ingreso_unified", unified)


def fix_rentas_preguntas(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(str(_BRONZE / "SISMEPRE-rentas_preguntas.parquet"))
    df = (
        df.withColumn("FORMULARIO_ID", _smallint_float("FORMULARIO_ID"))
        .withColumn("PREGUNTA_ID", _smallint_float("PREGUNTA_ID"))
        .withColumn(
            "PREGUNTA_PADRE_ID",
            F.coalesce(_smallint_float("PREGUNTA_PADRE_ID"), F.lit(0).cast("smallint")),
        )
        .withColumn("TIPO_CUESTIONARIO_ID", _smallint_float("TIPO_CUESTIONARIO_ID"))
        .withColumn(
            "DESCRIPCION", F.coalesce(F.col("DESCRIPCION").cast("string"), F.lit(""))
        )
        .dropDuplicates()
    )
    return _log_count("rentas_preguntas", df)


def fix_rentas_formulario(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(str(_BRONZE / "SISMEPRE-rentas_formulario.parquet"))
    df = (
        df.withColumn("FORMULARIO_ID", _smallint_float("FORMULARIO_ID"))
        .withColumn("TITULO", F.coalesce(F.col("TITULO").cast("string"), F.lit("")))
        .withColumn(
            "SUB_TITULO", F.coalesce(F.col("SUB_TITULO").cast("string"), F.lit(""))
        )
        .withColumn(
            "ABREVIATURA", F.coalesce(F.col("ABREVIATURA").cast("string"), F.lit(""))
        )
        .withColumn(
            "CLASIFICACION",
            F.coalesce(F.col("CLASIFICACION").cast("string"), F.lit("")),
        )
        .withColumn(
            "TIPO_FORMULARIO",
            F.coalesce(F.col("TIPO_FORMULARIO").cast("string"), F.lit("")),
        )
        .dropDuplicates()
    )
    return _log_count("rentas_formulario", df)


def fix_rentas_esat(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(
        str(_BRONZE / "SISMEPRE-rentas_esat_estadistica_atm.parquet")
    )
    df = (
        df.withColumn("SEC_EJEC", _int_float("SEC_EJEC"))
        .withColumn("DEPARTAMENTO", _smallint_float("DEPARTAMENTO"))
        .withColumn("PROVINCIA", _smallint_float("PROVINCIA"))
        .withColumn("DISTRITO", _smallint_float("DISTRITO"))
        .withColumn(
            "MUNICIPALIDAD_NOMBRE",
            F.coalesce(F.col("MUNICIPALIDAD_NOMBRE").cast("string"), F.lit("")),
        )
        .dropDuplicates()
    )
    return _log_count("rentas_esat_estadistica_atm", df)


def fix_rentas_respuestas(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(str(_BRONZE / "SISMEPRE-rentas_respuestas.parquet"))
    df = (
        df.withColumn("SEC_EJEC", _int_float("SEC_EJEC"))
        .withColumn("ANO_APLICACION", _year_float("ANO_APLICACION"))
        .withColumn("PERIODO", _smallint_float("PERIODO"))
        .withColumn("FORMULARIO_ID", _smallint_float("FORMULARIO_ID"))
        .withColumn("PREGUNTA_ID", _smallint_float("PREGUNTA_ID"))
        .withColumn("RESPUESTA_ID", _smallint_float("RESPUESTA_ID"))
        .withColumn("RESPUESTA_TEXTO", F.col("RESPUESTA_TEXTO").cast("string"))
        .withColumn("RESPUESTA_DECIMAL", F.col("RESPUESTA_DECIMAL").cast("double"))
        .withColumn("RESPUESTA_ENTERO", _int_float("RESPUESTA_ENTERO"))
        .withColumn("RESPUESTA_FECHA", F.col("RESPUESTA_FECHA").cast("string"))
        .dropDuplicates()
    )
    return _log_count("rentas_respuestas", df)


def fix_rentas_ano_aplicacion(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(str(_BRONZE / "SISMEPRE-rentas_ano_aplicacion.parquet"))
    df = (
        df.withColumn("ANO_APLICACION", _year_float("ANO_APLICACION"))
        .withColumn("ANO_APLICACION_INICIO", _year_float("ANO_APLICACION_INICIO"))
        .withColumn("ANO_APLICACION_FIN", _year_float("ANO_APLICACION_FIN"))
        .dropDuplicates()
    )
    return _log_count("rentas_ano_aplicacion", df)


def fix_renamu(spark: SparkSession, year: str) -> DataFrame:
    path = _BRONZE / f"RENAMU-{year}.parquet"
    df = spark.read.parquet(str(path))
    df = (
        df.withColumn("ccdd", _smallint_float("ccdd"))
        .withColumn("ccpp", _smallint_float("ccpp"))
        .withColumn("ccdi", _smallint_float("ccdi"))
        .withColumn("Año", _year_float("Año"))
        .dropDuplicates()
    )
    return _log_count(f"renamu_{year}", df)


def _normalize_municipalidad():
    """UDF que normaliza nombres de municipalidad a una clave común para join."""
    import re
    import unicodedata

    PREFIXES = [
        "MUNICIPALIDAD DISTRITAL DE ",
        "MUNICIPALIDAD PROVINCIAL DE ",
        "MUNICIPALIDAD METROPOLITANA DE ",
        "MUNICIPALIDAD DISTRITAL DEL ",
        "MUNICIPALIDAD PROVINCIAL DEL ",
        "MUNICIPALIDAD DISTRITAL ",
        "MUNICIPALIDAD PROVINCIAL ",
        "MUNICIPALIDAD METROPOLITANA ",
        "M. D. DE ",
        "M. P. DE ",
        "M. P. DEL ",
        "M. D. DEL ",
        "M. D . DE ",
        "M. D . ",
        "M.P. DE ",
        "M. D. ",
        "M. P. ",
        "M.P. ",
    ]

    def _norm(name: str) -> str:
        if not name:
            return ""
        n = unicodedata.normalize("NFKD", name.upper())
        n = n.encode("ascii", "ignore").decode("ascii")
        n = re.sub(r"\s*\([^)]*\)\s*", " ", n)
        for prefix in PREFIXES:
            if n.startswith(prefix):
                n = n[len(prefix) :]
                break
        n = n.lower().strip()
        n = re.sub(r"\bsta\.?\b", "santa", n)
        n = re.sub(r"\bsto\.?\b", "santo", n)
        n = re.sub(r"\bstgo\.?\b", "santiago", n)
        n = re.sub(r"\bj\.?\b", "jose", n)
        n = re.sub(r"\s*-\s*", "-", n)
        n = re.sub(r"\s+", " ", n).strip()
        return n

    return F.udf(_norm, "string")


def fix_categorias_municipalidades(
    spark: SparkSession, esat_df: DataFrame
) -> DataFrame:
    csv_path = str(settings.project_root / "data" / "CategoriasMunicipalidades.csv")
    categorias = (
        spark.read.option("header", True)
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .csv(csv_path)
    )
    norm = _normalize_municipalidad()
    esat_norm = esat_df.select(
        "SEC_EJEC",
        F.col("MUNICIPALIDAD_NOMBRE").alias("MUNICIPALIDAD_NOMBRE_ORIG"),
        norm("MUNICIPALIDAD_NOMBRE").alias("_NORMALIZED"),
    )
    csv_norm = categorias.select(
        F.col("Municipalidad").alias("MUNICIPALIDAD_ORIG"),
        F.col("Categoria").alias("CATEGORIA"),
        norm("Municipalidad").alias("_NORMALIZED"),
    )
    joined = (
        esat_norm.join(csv_norm, on="_NORMALIZED", how="inner")
        .select("SEC_EJEC", "CATEGORIA")
        .dropDuplicates(["SEC_EJEC"])
    )
    _logger.info(
        f"categorias_municipalidades: {joined.count()} ejecutoras mapeadas desde CategoriasMunicipalidades.csv"
    )
    return joined


def fix_renamu_984(spark: SparkSession, year: str = "2025") -> DataFrame | None:
    path = _BRONZE / "RENAMU-984-Modulo1963.parquet"
    if not path.exists():
        _logger.warning(f"Archivo {path} no encontrado, se omite")
        return None
    df = spark.read.parquet(str(path))
    df = (
        df.withColumn("ccdd", _smallint_float("ccdd"))
        .withColumn("ccpp", _smallint_float("ccpp"))
        .withColumn("ccdi", _smallint_float("ccdi"))
        .withColumn("Año", _year_float("Año"))
        .dropDuplicates()
    )
    return _log_count(f"renamu_{year}", df)


# ── orquestador ────────────────────────────────────────────────────────────────


def fix_all(spark: SparkSession) -> dict[str, DataFrame | list[DataFrame]]:
    """
    Aplica correcciones de calidad a todos los archivos Bronze.
    Guarda parquet corregidos en data/silver/stage/ y devuelve un dict de DataFrames.
    """
    _logger.info("Iniciando correcciones de calidad para todos los datasets")
    _STAGE.mkdir(parents=True, exist_ok=True)

    result: dict = {}

    def _save(name: str, df: DataFrame) -> DataFrame:
        out = str(_STAGE / f"{name}.parquet")
        df.write.mode("overwrite").parquet(out)
        _logger.info(f"Guardado: {out}")
        return df

    _logger.info("Procesando dataset SIAF - Ingreso")
    ingreso_df = fix_ingreso(spark)

    _logger.info("Procesando dataset SISMEPRE")
    preguntas_df = fix_rentas_preguntas(spark)
    formulario_df = fix_rentas_formulario(spark)
    esat_df = fix_rentas_esat(spark)
    respuestas_df = fix_rentas_respuestas(spark)
    ano_df = fix_rentas_ano_aplicacion(spark)

    # Construir filtro de categorías municipales y filtrar datasets
    _logger.info("Construyendo filtro de categorías municipales desde CategoriasMunicipalidades.csv")
    categorias_df = fix_categorias_municipalidades(spark, esat_df)
    result["categorias_municipalidades"] = _save(
        "categorias_municipalidades", categorias_df
    )

    valid_sec_ejec = categorias_df.select("SEC_EJEC").distinct()
    n_valid = valid_sec_ejec.count()
    _logger.info(f"Filtrando por {n_valid} ejecutoras válidas según CategoriasMunicipalidades.csv")

    ingreso_df = ingreso_df.join(valid_sec_ejec, on="SEC_EJEC", how="inner")
    result["ingreso_unified"] = _save("ingreso_unified", ingreso_df)

    esat_df = esat_df.join(valid_sec_ejec, on="SEC_EJEC", how="inner")
    result["rentas_esat"] = _save("rentas_esat_estadistica_atm", esat_df)

    respuestas_df = respuestas_df.join(valid_sec_ejec, on="SEC_EJEC", how="inner")
    result["rentas_respuestas"] = _save("rentas_respuestas", respuestas_df)

    result["rentas_preguntas"] = _save("rentas_preguntas", preguntas_df)
    result["rentas_formulario"] = _save("rentas_formulario", formulario_df)
    result["rentas_ano_aplicacion"] = _save("rentas_ano_aplicacion", ano_df)

    _logger.info("Procesando dataset RENAMU")
    renamu_dfs: list[DataFrame] = []
    for year in ("2021", "2022", "2023", "2024"):
        p = _BRONZE / f"RENAMU-{year}.parquet"
        if p.exists():
            renamu_dfs.append(_save(f"renamu_{year}", fix_renamu(spark, year)))
        else:
            _logger.warning(f"RENAMU-{year}.parquet no encontrado, se omite")

    df_984 = fix_renamu_984(spark)
    if df_984 is not None:
        _save("renamu_2025", df_984)
        renamu_dfs.append(df_984)
    result["renamu_dfs"] = renamu_dfs
    result["renamu_984"] = df_984

    _write_stage_manifest()
    _logger.info("Correcciones de calidad completadas para todos los datasets")
    return result


def _write_stage_manifest():
    """Scan stage parquet files and write a manifest with row counts and bronze source totals."""
    bronze_map = bronze_row_count_map(_BRONZE / "manifest.parquet")

    entries = []
    for stage_name, bronze_sources in STAGE_TO_BRONZE_SOURCES.items():
        stage_path = _STAGE / f"{stage_name}.parquet"
        if not stage_path.exists():
            continue
        dataset = pq.ParquetDataset(str(stage_path))
        row_count = sum(fragment.metadata.num_rows for fragment in dataset.fragments)
        file_size = sum(f.stat().st_size for f in stage_path.rglob("*") if f.is_file())

        bronze_total = 0
        if bronze_sources:
            for src_name, mod_prefix in bronze_sources:
                for (b_src, b_mod), b_rc in bronze_map.items():
                    if b_src == src_name and b_mod.startswith(mod_prefix):
                        bronze_total += b_rc

        entries.append(stage_entry(stage_name, row_count, file_size, bronze_total))
        _logger.info(
            f"Stage manifest: {stage_name}.parquet → {row_count} filas, "
            f"{file_size} bytes, bronze_total={bronze_total}"
        )

    if entries:
        manifest_path = _STAGE / "manifest.parquet"
        write_stage_manifest(entries, manifest_path)
        _logger.info(f"Stage manifest escrito: {manifest_path} ({len(entries)} entradas)")
