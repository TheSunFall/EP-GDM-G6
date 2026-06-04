"""Construcción del modelo estrella Silver a partir de los DataFrames de stage."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("SilverTransforms", "silver")


# ── helpers ───────────────────────────────────────────────────────────────────


def _dedup_by(df: DataFrame, partition_cols: list[str], order_col: str) -> DataFrame:
    """Mantiene la primera fila por clave natural, equivalente al ROW_NUMBER() del SQL proc."""
    # Verifica si la columna de orden existe; si no, usa las columnas de partición para ordenar
    if order_col not in df.columns:
        _logger.warning(
            f"Columna de orden '{order_col}' no encontrada en DataFrame con columnas: {df.columns}. "
            f"Usando deduplicación con ordenamiento por columnas de partición."
        )
        w = Window.partitionBy(*partition_cols).orderBy(*partition_cols)
    else:
        w = Window.partitionBy(*partition_cols).orderBy(order_col)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ── dimensiones ───────────────────────────────────────────────────────────────


def build_dim_tiempo(
    ingreso_df: DataFrame,
    rentas_ano_df: DataFrame,
    renamu_dfs: list[DataFrame],
) -> DataFrame:
    """
    IdTiempo = AÑO*100+MES para SIAF (mensual).
    IdTiempo = AÑO para RENAMU y SISMEPRE (anual).
    Incluye fila centinela IdTiempo=0 requerida por DIM_ANIO_APLICACION.
    """
    # SIAF mensual: IdTiempo = año*100 + mes
    siaf = (
        ingreso_df.select(
            (F.col("ANO_DOC") * 100 + F.col("MES_DOC")).cast("int").alias("IdTiempo"),
            F.col("ANO_DOC").cast("smallint").alias("ANIO"),
            F.col("MES_DOC").cast("smallint").alias("MES"),
        )
        .filter(F.col("ANO_DOC").isNotNull() & F.col("MES_DOC").isNotNull())
        .withColumn("DIA", F.lit(None).cast("smallint"))
        .withColumn("HORA", F.lit(None).cast("smallint"))
        .withColumn("MINUTO", F.lit(None).cast("smallint"))
        .withColumn("SEGUNDO", F.lit(None).cast("smallint"))
    )

    # SISMEPRE anual: cada año en ANO_APLICACION / INICIO / FIN → fila única
    sismepre_years = (
        rentas_ano_df.select(
            F.col("ANO_APLICACION").alias("ANIO_VAL"),
            F.col("ANO_APLICACION_INICIO").alias("ANIO_INICIO"),
            F.col("ANO_APLICACION_FIN").alias("ANIO_FIN"),
        )
        .selectExpr("stack(3, ANIO_VAL, ANIO_INICIO, ANIO_FIN) as ANIO_RAW")
        .filter(F.col("ANIO_RAW").isNotNull())
        .select(
            F.col("ANIO_RAW").cast("int").alias("IdTiempo"),
            F.col("ANIO_RAW").cast("smallint").alias("ANIO"),
            F.lit(None).cast("smallint").alias("MES"),
            F.lit(None).cast("smallint").alias("DIA"),
            F.lit(None).cast("smallint").alias("HORA"),
            F.lit(None).cast("smallint").alias("MINUTO"),
            F.lit(None).cast("smallint").alias("SEGUNDO"),
        )
    )

    # RENAMU anual: IdTiempo = año
    renamu_rows = []
    for rdf in renamu_dfs:
        year_col = next((c for c in rdf.columns if c.lower() == "año"), None)
        if year_col is None:
            continue
        renamu_rows.append(
            rdf.select(F.col(year_col).cast("int").alias("IdTiempo"))
            .filter(F.col("IdTiempo").isNotNull())
            .withColumn("ANIO", F.col("IdTiempo").cast("smallint"))
            .withColumn("MES", F.lit(None).cast("smallint"))
            .withColumn("DIA", F.lit(None).cast("smallint"))
            .withColumn("HORA", F.lit(None).cast("smallint"))
            .withColumn("MINUTO", F.lit(None).cast("smallint"))
            .withColumn("SEGUNDO", F.lit(None).cast("smallint"))
        )

    # Centinela requerido por DIM_ANIO_APLICACION (fechas sin dato → FK = 0).
    # Usa spark.range(1) en vez de createDataFrame para evitar serialización Python en shuffles.
    sentinel = siaf.sparkSession.range(1).select(
        F.lit(0).cast("int").alias("IdTiempo"),
        F.lit(0).cast("smallint").alias("ANIO"),
        F.lit(0).cast("smallint").alias("MES"),
        F.lit(0).cast("smallint").alias("DIA"),
        F.lit(0).cast("smallint").alias("HORA"),
        F.lit(0).cast("smallint").alias("MINUTO"),
        F.lit(0).cast("smallint").alias("SEGUNDO"),
    )

    all_parts = [sentinel, siaf, sismepre_years] + renamu_rows
    combined = all_parts[0]
    for part in all_parts[1:]:
        combined = combined.unionByName(part, allowMissingColumns=True)

    return combined.dropDuplicates(["IdTiempo"])


def build_dim_ejecutora(
    ingreso_df: DataFrame,
    esat_df: DataFrame,
    respuestas_df: DataFrame,
) -> DataFrame:
    # EJECUTORA puede ser NULL en algunos registros → coalesce con SEC_EJEC (igual que el SQL proc)
    from_ingreso = ingreso_df.select(
        F.col("SEC_EJEC").cast("int"),
        F.coalesce(F.col("EJECUTORA").cast("int"), F.col("SEC_EJEC").cast("int")).alias(
            "EJECUTORA"
        ),
        F.col("EJECUTORA_NOMBRE").cast("string").alias("EJECUTORA_NOMBRE"),
    ).filter(F.col("SEC_EJEC").isNotNull())
    from_esat = esat_df.select(
        F.col("SEC_EJEC").cast("int"),
        F.col("SEC_EJEC").cast("int").alias("EJECUTORA"),
        F.col("MUNICIPALIDAD_NOMBRE").cast("string").alias("EJECUTORA_NOMBRE"),
    ).filter(F.col("SEC_EJEC").isNotNull())
    from_resp = respuestas_df.select(
        F.col("SEC_EJEC").cast("int"),
        F.col("SEC_EJEC").cast("int").alias("EJECUTORA"),
        F.col("SEC_EJEC").cast("string").alias("EJECUTORA_NOMBRE"),
    ).filter(F.col("SEC_EJEC").isNotNull())

    return (
        from_ingreso.unionByName(from_esat, allowMissingColumns=True)
        .unionByName(from_resp, allowMissingColumns=True)
        .dropDuplicates(["SEC_EJEC"])
        .filter(F.col("EJECUTORA").isNotNull() & F.col("EJECUTORA_NOMBRE").isNotNull())
    )


def build_dim_ubigeo(
    ingreso_df: DataFrame,
    esat_df: DataFrame,
    renamu_dfs: list[DataFrame],
) -> DataFrame:
    from_ingreso = ingreso_df.select(
        F.col("DEPARTAMENTO_EJECUTORA").cast("smallint").alias("CODIGODEPARTAMENTO"),
        F.col("DEPARTAMENTO_EJECUTORA_NOMBRE").cast("string").alias("DEPARTAMENTO"),
        F.col("PROVINCIA_EJECUTORA").cast("smallint").alias("CODIGOPROVINCIA"),
        F.col("PROVINCIA_EJECUTORA_NOMBRE").cast("string").alias("PROVINCIA"),
        F.col("DISTRITO_EJECUTORA").cast("smallint").alias("CODIGODISTRITO"),
        F.col("DISTRITO_EJECUTORA_NOMBRE").cast("string").alias("DISTRITO"),
    ).filter(
        F.col("CODIGODEPARTAMENTO").isNotNull()
        & F.col("CODIGOPROVINCIA").isNotNull()
        & F.col("CODIGODISTRITO").isNotNull()
    )
    from_esat = esat_df.select(
        F.col("DEPARTAMENTO").cast("smallint").alias("CODIGODEPARTAMENTO"),
        F.col("DEPARTAMENTO_NOMBRE").cast("string").alias("DEPARTAMENTO"),
        F.col("PROVINCIA").cast("smallint").alias("CODIGOPROVINCIA"),
        F.col("PROVINCIA_NOMBRE").cast("string").alias("PROVINCIA"),
        F.col("DISTRITO").cast("smallint").alias("CODIGODISTRITO"),
        F.col("DISTRITO_NOMBRE").cast("string").alias("DISTRITO"),
    ).filter(
        F.col("CODIGODEPARTAMENTO").isNotNull()
        & F.col("CODIGOPROVINCIA").isNotNull()
        & F.col("CODIGODISTRITO").isNotNull()
    )

    parts = [from_ingreso, from_esat]
    for rdf in renamu_dfs:
        cols_lower = {c.lower(): c for c in rdf.columns}
        if not all(k in cols_lower for k in ("ccdd", "ccpp", "ccdi")):
            continue
        dep_col = cols_lower.get("departamento", cols_lower["ccdd"])
        prov_col = cols_lower.get("provincia", cols_lower["ccpp"])
        dist_col = cols_lower.get("distrito", cols_lower["ccdi"])
        parts.append(
            rdf.select(
                F.col(cols_lower["ccdd"]).cast("smallint").alias("CODIGODEPARTAMENTO"),
                F.col(dep_col).cast("string").alias("DEPARTAMENTO"),
                F.col(cols_lower["ccpp"]).cast("smallint").alias("CODIGOPROVINCIA"),
                F.col(prov_col).cast("string").alias("PROVINCIA"),
                F.col(cols_lower["ccdi"]).cast("smallint").alias("CODIGODISTRITO"),
                F.col(dist_col).cast("string").alias("DISTRITO"),
            ).filter(
                F.col("CODIGODEPARTAMENTO").isNotNull()
                & F.col("CODIGOPROVINCIA").isNotNull()
                & F.col("CODIGODISTRITO").isNotNull()
            )
        )

    combined = parts[0]
    for p in parts[1:]:
        combined = combined.unionByName(p, allowMissingColumns=True)

    return combined.dropDuplicates(
        ["CODIGODEPARTAMENTO", "CODIGOPROVINCIA", "CODIGODISTRITO"]
    )


def build_dim_nivel_gobierno(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("NIVEL_GOBIERNO").cast("string"),
            F.col("NIVEL_GOBIERNO_NOMBRE").cast("string"),
        )
        .filter(
            F.col("NIVEL_GOBIERNO").isNotNull()
            & F.col("NIVEL_GOBIERNO_NOMBRE").isNotNull()
        )
        .dropDuplicates(["NIVEL_GOBIERNO"])
    )


def build_dim_sector(ingreso_df: DataFrame) -> DataFrame:
    sentinel = ingreso_df.sparkSession.range(1).select(
        F.lit("0").alias("SECTOR"),
        F.lit("Desconocido").alias("SECTOR_NOMBRE"),
    )
    return sentinel.union(
        ingreso_df.select(
            F.col("SECTOR").cast("string"),
            F.col("SECTOR_NOMBRE").cast("string"),
        ).filter(F.col("SECTOR").isNotNull() & F.col("SECTOR_NOMBRE").isNotNull())
    ).dropDuplicates(["SECTOR"])


def build_dim_pliego(ingreso_df: DataFrame) -> DataFrame:
    sentinel = ingreso_df.sparkSession.range(1).select(
        F.lit("0").alias("PLIEGO"),
        F.lit("Desconocido").alias("PLIEGO_NOMBRE"),
    )
    return sentinel.union(
        ingreso_df.select(
            F.col("PLIEGO").cast("string"),
            F.col("PLIEGO_NOMBRE").cast("string"),
        ).filter(F.col("PLIEGO").isNotNull() & F.col("PLIEGO_NOMBRE").isNotNull())
    ).dropDuplicates(["PLIEGO"])


def build_dim_rubro(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("RUBRO").cast("smallint"),
            F.col("RUBRO_NOMBRE").cast("string"),
        )
        .filter(F.col("RUBRO").isNotNull() & F.col("RUBRO_NOMBRE").isNotNull())
        .dropDuplicates(["RUBRO"])
    )


def build_dim_tipo_recurso(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("TIPO_RECURSO").cast("string"),
            F.col("TIPO_RECURSO_NOMBRE").cast("string"),
        )
        .filter(
            F.col("TIPO_RECURSO").isNotNull() & F.col("TIPO_RECURSO_NOMBRE").isNotNull()
        )
        .dropDuplicates(["TIPO_RECURSO"])
    )


def build_dim_fuente_financiamiento(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("FUENTE_FINANCIAMIENTO").cast("smallint"),
            F.col("FUENTE_FINANCIAMIENTO_NOMBRE").cast("string"),
        )
        .filter(
            F.col("FUENTE_FINANCIAMIENTO").isNotNull()
            & F.col("FUENTE_FINANCIAMIENTO_NOMBRE").isNotNull()
        )
        .dropDuplicates(["FUENTE_FINANCIAMIENTO"])
    )


def build_dim_generica(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("GENERICA").cast("smallint"),
            F.col("GENERICA_NOMBRE").cast("string"),
            F.col("SUBGENERICA").cast("smallint"),
            F.col("SUBGENERICA_NOMBRE").cast("string"),
            F.col("SUBGENERICA_DET").cast("smallint"),
            F.col("SUBGENERICA_DET_NOMBRE").cast("string"),
        )
        .filter(
            F.col("GENERICA").isNotNull()
            & F.col("SUBGENERICA").isNotNull()
            & F.col("SUBGENERICA_DET").isNotNull()
        )
        .dropDuplicates(["GENERICA", "SUBGENERICA", "SUBGENERICA_DET"])
    )


def build_dim_especifica(ingreso_df: DataFrame) -> DataFrame:
    return (
        ingreso_df.select(
            F.col("ESPECIFICA").cast("smallint"),
            F.col("ESPECIFICA_NOMBRE").cast("string"),
            F.col("ESPECIFICA_DET").cast("smallint"),
            F.col("ESPECIFICA_DET_NOMBRE").cast("string"),
        )
        .filter(F.col("ESPECIFICA").isNotNull() & F.col("ESPECIFICA_DET").isNotNull())
        .dropDuplicates(["ESPECIFICA", "ESPECIFICA_DET"])
    )


def build_dim_anio_aplicacion(rentas_ano_df: DataFrame) -> DataFrame:
    """
    FKs ANO_APLICACION, ANO_APLICACION_INICIO, ANO_APLICACION_FIN → DIM_TIEMPO.IdTiempo (= año).
    FECHA_CIERRE, FECHA_PRES_OFICIO, FECHA_INI_CIERRE, FECHA_ING → 0 (centinela, igual que SQL proc).
    """
    return (
        rentas_ano_df.select(
            F.col("ANO_APLICACION").cast("int"),
            F.coalesce(F.col("ANO_APLICACION_INICIO").cast("int"), F.lit(0)).alias(
                "ANO_APLICACION_INICIO"
            ),
            F.coalesce(F.col("ANO_APLICACION_FIN").cast("int"), F.lit(0)).alias(
                "ANO_APLICACION_FIN"
            ),
        )
        .filter(F.col("ANO_APLICACION").isNotNull())
        .withColumn("FECHA_CIERRE", F.lit(0).cast("int"))
        .withColumn("FECHA_PRES_OFICIO", F.lit(0).cast("int"))
        .withColumn("FECHA_INI_CIERRE", F.lit(0).cast("int"))
        .withColumn("FECHA_ING", F.lit(0).cast("int"))
        .dropDuplicates(["ANO_APLICACION"])
    )


def build_dim_formulario_sismepre(rentas_formulario_df: DataFrame) -> DataFrame:
    return (
        rentas_formulario_df.select(
            F.col("FORMULARIO_ID").cast("smallint"),
            F.col("TITULO").cast("string"),
            F.col("SUB_TITULO").cast("string").alias("SUBTITULO"),
            F.col("ABREVIATURA").cast("string"),
            F.col("CLASIFICACION").cast("string"),
            F.col("TIPO_FORMULARIO").cast("string"),
        )
        .filter(F.col("FORMULARIO_ID").isNotNull())
        .dropDuplicates(["FORMULARIO_ID"])
    )


def build_dim_pregunta_sismepre(
    rentas_preguntas_df: DataFrame,
    server_dim_formulario: DataFrame,
) -> DataFrame:
    """
    Requiere server_dim_formulario (leído de SQL Server) para obtener IdFormSismepre real.
    Devuelve DataFrame con: IdFormSismepre, PREGUNTA_ID, PREGUNTA_PADRE_ID, DESCRIPCION, TIPO_CUESTIONARIO_ID.
    NO incluye IdPreguntaSismepre — SQL Server lo genera al insertar (IDENTITY).
    """
    form_dedup = _dedup_by(server_dim_formulario, ["FORMULARIO_ID"], "IdFormSismepre")
    return (
        rentas_preguntas_df.select(
            F.col("FORMULARIO_ID").cast("smallint"),
            F.col("PREGUNTA_ID").cast("smallint"),
            F.col("PREGUNTA_PADRE_ID").cast("smallint"),
            F.col("DESCRIPCION").cast("string"),
            F.col("TIPO_CUESTIONARIO_ID").cast("smallint"),
        )
        .filter(
            F.col("FORMULARIO_ID").isNotNull()
            & F.col("PREGUNTA_ID").isNotNull()
            & F.col("TIPO_CUESTIONARIO_ID").isNotNull()
        )
        .join(
            form_dedup.select("FORMULARIO_ID", "IdFormSismepre"),
            on="FORMULARIO_ID",
            how="inner",
        )
        .drop("FORMULARIO_ID")
        .dropDuplicates(["IdFormSismepre", "PREGUNTA_ID"])
        .select(
            F.col("IdFormSismepre").cast("int"),
            F.col("PREGUNTA_ID").cast("smallint"),
            F.col("PREGUNTA_PADRE_ID").cast("smallint"),
            F.col("DESCRIPCION").cast("string"),
            F.col("TIPO_CUESTIONARIO_ID").cast("smallint"),
        )
    )


def build_dim_pregunta_renamu(renamu_dfs: list[DataFrame]) -> DataFrame:
    """
    Unpivot todas las columnas-pregunta de todos los años RENAMU.
    Cada (NOMBRE_CAMPO, VALOR) único es una fila en DIM_PREGUNTA_RENAMU.
    VALOR truncado a 300 chars y NOMBRE_CAMPO a 100 chars (límites del DDL).
    """
    fixed_cols_lower = {
        "año",
        "idmunici",
        "ccdd",
        "ccpp",
        "ccdi",
        "ubigeo",
        "departamento",
        "provincia",
        "distrito",
        "tipomuni",
    }

    all_parts = []
    for rdf in renamu_dfs:
        question_cols = [c for c in rdf.columns if c.lower() not in fixed_cols_lower]
        if not question_cols:
            continue

        n = len(question_cols)
        pairs = ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in question_cols)
        stack_expr = f"stack({n}, {pairs}) as (NOMBRE_CAMPO, VALOR)"

        all_parts.append(
            rdf.select(F.expr(stack_expr)).filter(F.col("NOMBRE_CAMPO").isNotNull())
        )

    if not all_parts:
        raise ValueError(
            "No se encontraron columnas de preguntas en los archivos RENAMU"
        )

    combined = all_parts[0]
    for p in all_parts[1:]:
        combined = combined.unionByName(p, allowMissingColumns=True)

    return (
        combined.select(
            F.substring(F.col("NOMBRE_CAMPO").cast("string"), 1, 100).alias(
                "NOMBRE_CAMPO"
            ),
            F.substring(
                F.coalesce(F.col("VALOR").cast("string"), F.lit("")), 1, 300
            ).alias("VALOR"),
        )
        .dropDuplicates(["NOMBRE_CAMPO", "VALOR"])
        .withColumn("DESCRIPCION", F.col("NOMBRE_CAMPO"))
        .withColumn("METADATA", F.lit(""))
        .withColumn("IdPadre", F.lit(None).cast("int"))
    )


# ── hechos ────────────────────────────────────────────────────────────────────


def build_fact_ingreso(
    ingreso_df: DataFrame,
    dim_nivel: DataFrame,
    dim_sector: DataFrame,
    dim_pliego: DataFrame,
    dim_ejecutora: DataFrame,
    dim_ubigeo: DataFrame,
    dim_rubro: DataFrame,
    dim_tipo_recurso: DataFrame,
    dim_generica: DataFrame,
    dim_especifica: DataFrame,
) -> DataFrame:
    # Deduplicar cada dimensión por clave natural antes de unir (igual que CTEs del SQL proc)
    nivel_d = _dedup_by(dim_nivel, ["NIVEL_GOBIERNO"], "IdNivelGobierno")
    sector_d = _dedup_by(dim_sector, ["SECTOR"], "IdSector")
    pliego_d = _dedup_by(dim_pliego, ["PLIEGO"], "IdPliego")
    ejec_d = _dedup_by(dim_ejecutora, ["SEC_EJEC"], "IdEjecutora")
    ubigeo_d = _dedup_by(
        dim_ubigeo,
        ["CODIGODEPARTAMENTO", "CODIGOPROVINCIA", "CODIGODISTRITO"],
        "IdUbigeo",
    )
    rubro_d = _dedup_by(dim_rubro, ["RUBRO"], "IdRubro")
    tipo_d = _dedup_by(dim_tipo_recurso, ["TIPO_RECURSO"], "IdTipoRecurso")
    generica_d = _dedup_by(
        dim_generica, ["GENERICA", "SUBGENERICA", "SUBGENERICA_DET"], "IdGenerica"
    )
    especifica_d = _dedup_by(
        dim_especifica, ["ESPECIFICA", "ESPECIFICA_DET"], "IdEspecifica"
    )

    return (
        ingreso_df.withColumn(
            "IdTiempo", (F.col("ANO_DOC") * 100 + F.col("MES_DOC")).cast("int")
        )
        .filter(F.col("IdTiempo").isNotNull())
        .withColumn("SECTOR", F.coalesce(F.col("SECTOR"), F.lit("0")))
        .withColumn("PLIEGO", F.coalesce(F.col("PLIEGO"), F.lit("0")))
        .join(
            nivel_d.select("NIVEL_GOBIERNO", "IdNivelGobierno"),
            on="NIVEL_GOBIERNO",
            how="inner",
        )
        .join(sector_d.select("SECTOR", "IdSector"), on="SECTOR", how="left")
        .join(pliego_d.select("PLIEGO", "IdPliego"), on="PLIEGO", how="left")
        .join(ejec_d.select("SEC_EJEC", "IdEjecutora"), on="SEC_EJEC", how="inner")
        .join(
            ubigeo_d.select(
                "CODIGODEPARTAMENTO", "CODIGOPROVINCIA", "CODIGODISTRITO", "IdUbigeo"
            ),
            on=[
                F.col("DEPARTAMENTO_EJECUTORA") == F.col("CODIGODEPARTAMENTO"),
                F.col("PROVINCIA_EJECUTORA") == F.col("CODIGOPROVINCIA"),
                F.col("DISTRITO_EJECUTORA") == F.col("CODIGODISTRITO"),
            ],
            how="inner",
        )
        .join(rubro_d.select("RUBRO", "IdRubro"), on="RUBRO", how="inner")
        .join(
            tipo_d.select("TIPO_RECURSO", "IdTipoRecurso"),
            on="TIPO_RECURSO",
            how="inner",
        )
        .join(
            generica_d.select(
                "GENERICA", "SUBGENERICA", "SUBGENERICA_DET", "IdGenerica"
            ),
            on=["GENERICA", "SUBGENERICA", "SUBGENERICA_DET"],
            how="inner",
        )
        .join(
            especifica_d.select("ESPECIFICA", "ESPECIFICA_DET", "IdEspecifica"),
            on=["ESPECIFICA", "ESPECIFICA_DET"],
            how="inner",
        )
        .select(
            F.col("IdTiempo"),
            F.col("IdNivelGobierno").cast("int"),
            F.col("IdSector").cast("int"),
            F.col("IdPliego").cast("int"),
            F.col("IdEjecutora").cast("int"),
            F.col("IdUbigeo").cast("int"),
            F.col("IdRubro").cast("int"),
            F.col("IdTipoRecurso").cast("int"),
            F.col("IdGenerica").cast("int"),
            F.col("IdEspecifica").cast("int"),
            F.coalesce(
                F.col("MONTO_PIA").cast("bigint"), F.lit(0).cast("bigint")
            ).alias("MONTO_PIA"),
            F.coalesce(
                F.col("MONTO_PIM").cast("bigint"), F.lit(0).cast("bigint")
            ).alias("MONTO_PIM"),
            # numeric(18,2) — usar decimal para que JDBC lo mapee correctamente
            F.coalesce(
                F.col("MONTO_RECAUDADO").cast("decimal(18,2)"),
                F.lit(0).cast("decimal(18,2)"),
            ).alias("MONTO_RECAUDADO"),
        )
    )


def build_fact_formulario_sismepre(
    respuestas_df: DataFrame,
    dim_ejecutora: DataFrame,
    dim_anio: DataFrame,
    dim_formulario: DataFrame,
    dim_pregunta_sismepre: DataFrame,
) -> DataFrame:
    """
    dim_pregunta_sismepre debe ser el DataFrame leído de SQL Server
    (con IdPreguntaSismepre real como IDENTITY).
    """
    ejec_d = _dedup_by(dim_ejecutora, ["SEC_EJEC"], "IdEjecutora")
    anio_d = _dedup_by(dim_anio, ["ANO_APLICACION"], "IdAnioAplicacion")
    form_d = _dedup_by(dim_formulario, ["FORMULARIO_ID"], "IdFormSismepre")
    preg_d = _dedup_by(
        dim_pregunta_sismepre, ["IdFormSismepre", "PREGUNTA_ID"], "IdPreguntaSismepre"
    )

    return (
        respuestas_df.filter(
            F.col("PERIODO").isNotNull() & F.col("RESPUESTA_ID").isNotNull()
        )
        .join(ejec_d.select("SEC_EJEC", "IdEjecutora"), on="SEC_EJEC", how="inner")
        .join(
            anio_d.select("ANO_APLICACION", "IdAnioAplicacion"),
            on="ANO_APLICACION",
            how="inner",
        )
        .join(
            form_d.select("FORMULARIO_ID", "IdFormSismepre"),
            on="FORMULARIO_ID",
            how="inner",
        )
        .join(
            preg_d.select("IdFormSismepre", "PREGUNTA_ID", "IdPreguntaSismepre"),
            on=["IdFormSismepre", "PREGUNTA_ID"],
            how="inner",
        )
        .select(
            F.col("IdEjecutora").cast("int"),
            F.col("IdAnioAplicacion").cast("int"),
            F.col("PERIODO").cast("smallint"),
            F.col("IdFormSismepre").cast("int").alias("IdFormulario"),
            F.col("IdPreguntaSismepre").cast("int").alias("IdPregunta"),
            F.coalesce(
                F.col("RESPUESTA_ID").cast("smallint"), F.lit(0).cast("smallint")
            ).alias("RESPUESTA_ID"),
            F.col("RESPUESTA_TEXTO").cast("string"),
            # numeric(18,2) — decimal para mapeo correcto en JDBC
            F.col("RESPUESTA_DECIMAL").cast("decimal(18,2)"),
            F.col("RESPUESTA_ENTERO").cast("int"),
            F.col("RESPUESTA_FECHA").cast("string"),
        )
    )


def build_fact_renamu(
    renamu_dfs: list[DataFrame],
    dim_ubigeo: DataFrame,
    dim_pregunta_renamu: DataFrame,
) -> DataFrame:
    fixed_cols_lower = {
        "año",
        "idmunici",
        "ccdd",
        "ccpp",
        "ccdi",
        "ubigeo",
        "departamento",
        "provincia",
        "distrito",
        "tipomuni",
    }

    ubigeo_d = _dedup_by(
        dim_ubigeo,
        ["CODIGODEPARTAMENTO", "CODIGOPROVINCIA", "CODIGODISTRITO"],
        "IdUbigeo",
    )
    preg_d = _dedup_by(dim_pregunta_renamu, ["NOMBRE_CAMPO", "VALOR"], "IdPregunta")

    all_parts = []
    for rdf in renamu_dfs:
        cols_lower = {c.lower(): c for c in rdf.columns}
        question_cols = [c for c in rdf.columns if c.lower() not in fixed_cols_lower]
        if not question_cols:
            continue

        year_col = cols_lower.get("año")
        tipomuni_col = cols_lower.get("tipomuni")
        ccdd_col = cols_lower["ccdd"]
        ccpp_col = cols_lower["ccpp"]
        ccdi_col = cols_lower["ccdi"]

        if year_col is None:
            _logger.warning(
                "RENAMU DataFrame sin columna 'año', se omite en FACT_RENAMU"
            )
            continue

        n = len(question_cols)
        pairs = ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in question_cols)
        stack_expr = f"stack({n}, {pairs}) as (NOMBRE_CAMPO, VALOR)"

        fixed_select = [
            F.col(year_col).cast("int").alias("ANO"),
            F.col(ccdd_col).cast("smallint").alias("CCDD"),
            F.col(ccpp_col).cast("smallint").alias("CCPP"),
            F.col(ccdi_col).cast("smallint").alias("CCDI"),
            F.col(tipomuni_col).cast("string").alias("TIPOMUNI")
            if tipomuni_col
            else F.lit("").alias("TIPOMUNI"),
        ]

        unpivoted = (
            rdf.repartition(8)
            .select(*fixed_select, F.expr(stack_expr))
            .filter(F.col("NOMBRE_CAMPO").isNotNull())
            # Truncar VALOR a 300 chars (coherente con build_dim_pregunta_renamu)
            .withColumn(
                "VALOR", F.substring(F.coalesce(F.col("VALOR"), F.lit("")), 1, 300)
            )
            .withColumn("NOMBRE_CAMPO", F.substring(F.col("NOMBRE_CAMPO"), 1, 100))
        )
        all_parts.append(unpivoted)

    if not all_parts:
        raise ValueError("No hay datos RENAMU para construir FACT_RENAMU")

    combined = all_parts[0]
    for p in all_parts[1:]:
        combined = combined.unionByName(p, allowMissingColumns=True)

    return (
        combined.filter(F.col("ANO").isNotNull() & F.col("CCDD").isNotNull())
        .join(
            ubigeo_d.select(
                "CODIGODEPARTAMENTO", "CODIGOPROVINCIA", "CODIGODISTRITO", "IdUbigeo"
            ),
            on=[
                F.col("CCDD") == F.col("CODIGODEPARTAMENTO"),
                F.col("CCPP") == F.col("CODIGOPROVINCIA"),
                F.col("CCDI") == F.col("CODIGODISTRITO"),
            ],
            how="inner",
        )
        .join(
            preg_d.select("NOMBRE_CAMPO", "VALOR", "IdPregunta"),
            on=["NOMBRE_CAMPO", "VALOR"],
            how="inner",
        )
        .select(
            F.col("ANO").cast("int").alias("IdTiempo"),
            F.col("IdUbigeo").cast("int"),
            F.col("TIPOMUNI").cast("string"),
            F.col("IdPregunta").cast("int"),
        )
    )


# ── orquestadores ─────────────────────────────────────────────────────────────


def build_dims(spark: SparkSession, stage: dict) -> dict[str, DataFrame]:
    """
    Construye todas las dimensiones EXCEPTO DIM_PREGUNTA_SISMEPRE,
    que requiere los IDs reales del servidor (la crea loader.create_and_load).
    """
    _logger.info("Construyendo dimensiones del modelo estrella")
    ingreso = stage["ingreso_unified"]
    esat = stage["rentas_esat"]
    respuestas = stage["rentas_respuestas"]
    formulario = stage["rentas_formulario"]
    ano_aplic = stage["rentas_ano_aplicacion"]
    renamu_dfs = stage["renamu_dfs"]

    dims = {
        "DIM_TIEMPO": build_dim_tiempo(ingreso, ano_aplic, renamu_dfs),
        "DIM_EJECUTORA": build_dim_ejecutora(ingreso, esat, respuestas),
        "DIM_UBIGEO": build_dim_ubigeo(ingreso, esat, renamu_dfs),
        "DIM_NIVEL_GOBIERNO": build_dim_nivel_gobierno(ingreso),
        "DIM_SECTOR": build_dim_sector(ingreso),
        "DIM_PLIEGO": build_dim_pliego(ingreso),
        "DIM_RUBRO": build_dim_rubro(ingreso),
        "DIM_TIPO_RECURSO": build_dim_tipo_recurso(ingreso),
        "DIM_FUENTE_FINANCIAMIENTO": build_dim_fuente_financiamiento(ingreso),
        "DIM_GENERICA": build_dim_generica(ingreso),
        "DIM_ESPECIFICA": build_dim_especifica(ingreso),
        "DIM_ANIO_APLICACION": build_dim_anio_aplicacion(ano_aplic),
        "DIM_FORMULARIO_SISMEPRE": build_dim_formulario_sismepre(formulario),
        "DIM_PREGUNTA_RENAMU": build_dim_pregunta_renamu(renamu_dfs),
    }

    _logger.info(f"Dimensiones construidas: {len(dims)} dimensiones")
    return dims


def build_facts(
    spark: SparkSession, stage: dict, server_dims: dict
) -> dict[str, DataFrame]:
    """
    Construye las tablas de hechos.
    Requiere server_dims con TODOS los IDs reales del servidor, incluido DIM_PREGUNTA_SISMEPRE.
    """
    _logger.info("Construyendo tablas de hechos")
    respuestas = stage["rentas_respuestas"]
    renamu_dfs = stage["renamu_dfs"]

    # DIM_PREGUNTA_SISMEPRE debe venir ya poblado en server_dims por loader.create_and_load
    dim_pregunta_sismepre = server_dims["DIM_PREGUNTA_SISMEPRE"]

    facts = {
        "FACT_INGRESO": build_fact_ingreso(
            stage["ingreso_unified"],
            server_dims["DIM_NIVEL_GOBIERNO"],
            server_dims["DIM_SECTOR"],
            server_dims["DIM_PLIEGO"],
            server_dims["DIM_EJECUTORA"],
            server_dims["DIM_UBIGEO"],
            server_dims["DIM_RUBRO"],
            server_dims["DIM_TIPO_RECURSO"],
            server_dims["DIM_GENERICA"],
            server_dims["DIM_ESPECIFICA"],
        ),
        "FACT_FORMULARIO_SISMEPRE": build_fact_formulario_sismepre(
            respuestas,
            server_dims["DIM_EJECUTORA"],
            server_dims["DIM_ANIO_APLICACION"],
            server_dims["DIM_FORMULARIO_SISMEPRE"],
            dim_pregunta_sismepre,
        ),
        "FACT_RENAMU": build_fact_renamu(
            renamu_dfs,
            server_dims["DIM_UBIGEO"],
            server_dims["DIM_PREGUNTA_RENAMU"],
        ),
    }

    _logger.info(f"Tablas de hechos construidas: {len(facts)} fact tables")
    return facts
