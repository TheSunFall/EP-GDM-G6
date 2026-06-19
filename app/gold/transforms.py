"""Transformaciones de la capa Gold con PySpark.

Lee las tablas silver desde parquet y construye las dimensiones de presentacion y los
marts de negocio como DataFrames.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("GoldTransforms", "gold")


def _read_parquet(spark: SparkSession, table: str, silver_dir: Path) -> DataFrame:
    path = silver_dir / f"{table}.parquet"
    return spark.read.parquet(str(path))


def _read_partitioned_parquet(
    spark: SparkSession,
    table: str,
    silver_dir: Path,
    partition_column: str,
    num_partitions: int = 16,
) -> DataFrame:
    df = _read_parquet(spark, table, silver_dir)
    return df.repartition(num_partitions, partition_column)


# ── Dimensiones de presentacion ─────────────────────────────────────────────


def build_dim_calendario(spark: SparkSession, dim_tiempo: DataFrame) -> DataFrame:
    dt = dim_tiempo.filter((F.col("ANIO") >= 2000) & (F.col("ANIO") <= 2100))
    b = dt.agg(F.min("ANIO").alias("mn"), F.max("ANIO").alias("mx")).collect()[0]
    mn, mx = int(b["mn"]), int(b["mx"])

    return spark.sql(
        f"""
        WITH anios AS (SELECT explode(sequence({mn}, {mx})) AS Anio),
             meses AS (SELECT explode(sequence(1, 12)) AS Mes)
        SELECT CAST(Anio * 100 + Mes AS INT)        AS AnioMes,
               CAST(Anio AS SMALLINT)               AS Anio,
               CAST(Mes AS TINYINT)                 AS Mes,
               element_at(array('Enero','Febrero','Marzo','Abril','Mayo','Junio',
                                'Julio','Agosto','Septiembre','Octubre','Noviembre',
                                'Diciembre'), Mes)  AS NombreMes,
               CAST(((Mes - 1) div 3) + 1 AS TINYINT) AS Trimestre,
               make_date(Anio, Mes, 1)              AS FechaInicioMes
        FROM anios CROSS JOIN meses
        """
    )


def build_dim_anio(spark: SparkSession, dim_tiempo: DataFrame) -> DataFrame:
    return (
        dim_tiempo
        .filter((F.col("ANIO") >= 2000) & (F.col("ANIO") <= 2100))
        .select(F.col("ANIO").cast("smallint").alias("Anio"))
        .distinct()
    )


def build_dim_geografia(spark: SparkSession, dim_ubigeo: DataFrame) -> DataFrame:
    return dim_ubigeo.select(
        F.col("IdUbigeo"),
        F.col("CODIGODEPARTAMENTO").alias("CodigoDepartamento"),
        F.col("DEPARTAMENTO").alias("Departamento"),
        F.col("CODIGOPROVINCIA").alias("CodigoProvincia"),
        F.col("PROVINCIA").alias("Provincia"),
        F.col("CODIGODISTRITO").alias("CodigoDistrito"),
        F.col("DISTRITO").alias("Distrito"),
        F.concat(
            F.lpad(F.col("CODIGODEPARTAMENTO").cast("string"), 2, "0"),
            F.lpad(F.col("CODIGOPROVINCIA").cast("string"), 2, "0"),
            F.lpad(F.col("CODIGODISTRITO").cast("string"), 2, "0"),
        ).alias("Ubigeo"),
    )


# ── Marts SIAF (ingresos) ───────────────────────────────────────────────────


def _pct(rec, pim):
    return F.col(rec).cast("double") / F.when(F.col(pim) == 0, None).otherwise(F.col(pim))


def build_mart_ingresos_geografico(
    spark: SparkSession,
    fact_ingreso: DataFrame,
    dim_tiempo: DataFrame,
    dim_ubigeo: DataFrame,
    dim_nivel_gobierno: DataFrame,
) -> DataFrame:
    ng = dim_nivel_gobierno.select("IdNivelGobierno", "NIVEL_GOBIERNO_NOMBRE")
    t = dim_tiempo.select("IdTiempo", "ANIO", "MES")
    u = dim_ubigeo.select("IdUbigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO")
    return (
        fact_ingreso.join(F.broadcast(t), "IdTiempo")
        .join(F.broadcast(u), "IdUbigeo")
        .join(F.broadcast(ng), "IdNivelGobierno")
        .groupBy(
            F.col("IdTiempo").alias("AnioMes"),
            F.col("ANIO").alias("Anio"),
            F.col("MES").alias("Mes"),
            F.col("IdUbigeo"),
            F.col("DEPARTAMENTO").alias("Departamento"),
            F.col("PROVINCIA").alias("Provincia"),
            F.col("DISTRITO").alias("Distrito"),
            F.col("NIVEL_GOBIERNO_NOMBRE").alias("NivelGobierno"),
        )
        .agg(
            F.sum("MONTO_PIA").alias("MontoPIA"),
            F.sum("MONTO_PIM").alias("MontoPIM"),
            F.sum("MONTO_RECAUDADO").alias("MontoRecaudado"),
        )
        .select(
            F.col("AnioMes").cast("int"),
            F.col("Anio").cast("smallint"),
            F.col("Mes").cast("tinyint"),
            F.col("IdUbigeo").cast("int"),
            "Departamento", "Provincia", "Distrito", "NivelGobierno",
            F.col("MontoPIA").cast("bigint"),
            F.col("MontoPIM").cast("bigint"),
            F.col("MontoRecaudado").cast("decimal(18,2)"),
            _pct("MontoRecaudado", "MontoPIM").alias("PctEjecucion"),
            (F.col("MontoPIM").cast("decimal(18,2)") - F.col("MontoRecaudado")).cast("decimal(18,2)").alias("Brecha"),
            (F.col("MontoPIM") - F.col("MontoPIA")).cast("bigint").alias("VarPIA_PIM"),
        )
    )


def build_mart_ingresos_clasificador(
    spark: SparkSession,
    fact_ingreso: DataFrame,
    dim_tiempo: DataFrame,
    dim_rubro: DataFrame,
    dim_tipo_recurso: DataFrame,
    dim_generica: DataFrame,
) -> DataFrame:
    t = dim_tiempo.select("IdTiempo", "ANIO")
    r = dim_rubro.select("IdRubro", "RUBRO_NOMBRE")
    tr = dim_tipo_recurso.select("IdTipoRecurso", "TIPO_RECURSO_NOMBRE")
    g = dim_generica.select("IdGenerica", "GENERICA_NOMBRE", "SUBGENERICA_NOMBRE")
    return (
        fact_ingreso.join(F.broadcast(t), "IdTiempo")
        .join(F.broadcast(r), "IdRubro")
        .join(F.broadcast(tr), "IdTipoRecurso")
        .join(F.broadcast(g), "IdGenerica")
        .groupBy(
            F.col("ANIO").alias("Anio"),
            F.col("RUBRO_NOMBRE").alias("Rubro"),
            F.col("TIPO_RECURSO_NOMBRE").alias("TipoRecurso"),
            F.col("GENERICA_NOMBRE").alias("Generica"),
            F.col("SUBGENERICA_NOMBRE").alias("SubGenerica"),
        )
        .agg(
            F.sum("MONTO_PIA").alias("MontoPIA"),
            F.sum("MONTO_PIM").alias("MontoPIM"),
            F.sum("MONTO_RECAUDADO").alias("MontoRecaudado"),
        )
        .select(
            F.col("Anio").cast("smallint"),
            "Rubro", "TipoRecurso", "Generica", "SubGenerica",
            F.col("MontoPIA").cast("bigint"),
            F.col("MontoPIM").cast("bigint"),
            F.col("MontoRecaudado").cast("decimal(18,2)"),
            _pct("MontoRecaudado", "MontoPIM").alias("PctEjecucion"),
            (F.col("MontoPIM").cast("decimal(18,2)") - F.col("MontoRecaudado")).cast("decimal(18,2)").alias("Brecha"),
        )
    )


def build_mart_ingresos_ejecutora(
    spark: SparkSession,
    fact_ingreso: DataFrame,
    dim_tiempo: DataFrame,
    dim_ejecutora: DataFrame,
    dim_ubigeo: DataFrame,
) -> DataFrame:
    t = dim_tiempo.select("IdTiempo", "ANIO")
    e = dim_ejecutora.select("IdEjecutora", "SEC_EJEC", "EJECUTORA_NOMBRE", "CATEGORIA")
    u = dim_ubigeo.select("IdUbigeo", "DEPARTAMENTO")
    return (
        fact_ingreso.join(F.broadcast(t), "IdTiempo")
        .join(F.broadcast(e), "IdEjecutora")
        .join(F.broadcast(u), "IdUbigeo")
        .groupBy(
            F.col("ANIO").alias("Anio"),
            F.col("IdEjecutora"),
            F.col("SEC_EJEC").alias("SecEjec"),
            F.col("EJECUTORA_NOMBRE").alias("Ejecutora"),
            F.coalesce(F.col("CATEGORIA"), F.lit("")).alias("Categoria"),
            F.col("DEPARTAMENTO").alias("Departamento"),
        )
        .agg(
            F.sum("MONTO_PIA").alias("MontoPIA"),
            F.sum("MONTO_PIM").alias("MontoPIM"),
            F.sum("MONTO_RECAUDADO").alias("MontoRecaudado"),
        )
        .select(
            F.col("Anio").cast("smallint"),
            F.col("IdEjecutora").cast("int"),
            F.col("SecEjec").cast("int"),
            "Ejecutora", "Categoria", "Departamento",
            F.col("MontoPIA").cast("bigint"),
            F.col("MontoPIM").cast("bigint"),
            F.col("MontoRecaudado").cast("decimal(18,2)"),
            _pct("MontoRecaudado", "MontoPIM").alias("PctEjecucion"),
            (F.col("MontoPIM").cast("decimal(18,2)") - F.col("MontoRecaudado")).cast("decimal(18,2)").alias("Brecha"),
        )
    )


# ── Mart SISMEPRE (predial) ─────────────────────────────────────────────────


def build_mart_predial(
    spark: SparkSession,
    fact_formulario_sismepre: DataFrame,
    dim_ejecutora: DataFrame,
    dim_anio_aplicacion: DataFrame,
    dim_formulario_sismepre: DataFrame,
    dim_pregunta_sismepre: DataFrame,
) -> DataFrame:
    aa = dim_anio_aplicacion.select("IdAnioAplicacion", "ANO_APLICACION")
    e = dim_ejecutora.select("IdEjecutora", "SEC_EJEC", "EJECUTORA_NOMBRE", "CATEGORIA")
    fo = dim_formulario_sismepre.select(
        F.col("IdFormSismepre").alias("IdFormulario"), F.col("TITULO")
    )
    p = dim_pregunta_sismepre.select(
        F.col("IdPreguntaSismepre").alias("IdPregunta"), F.col("DESCRIPCION")
    )
    return (
        fact_formulario_sismepre.join(F.broadcast(aa), "IdAnioAplicacion")
        .join(F.broadcast(e), "IdEjecutora")
        .join(F.broadcast(fo), "IdFormulario")
        .join(F.broadcast(p), "IdPregunta")
        .select(
            F.col("ANO_APLICACION").cast("smallint").alias("Anio"),
            F.col("PERIODO").cast("smallint").alias("Periodo"),
            F.col("IdEjecutora").cast("int"),
            F.col("SEC_EJEC").cast("int").alias("SecEjec"),
            F.col("EJECUTORA_NOMBRE").alias("Municipalidad"),
            F.coalesce(F.col("CATEGORIA"), F.lit("")).alias("Categoria"),
            F.col("IdFormulario").cast("int"),
            F.col("TITULO").alias("FormularioTitulo"),
            F.col("IdPregunta").cast("int"),
            F.col("DESCRIPCION").alias("PreguntaDescripcion"),
            F.col("RESPUESTA_ID").cast("smallint").alias("RespuestaId"),
            F.col("RESPUESTA_TEXTO").alias("RespuestaTexto"),
            F.col("RESPUESTA_DECIMAL").cast("decimal(18,2)").alias("RespuestaDecimal"),
            F.col("RESPUESTA_ENTERO").cast("int").alias("RespuestaEntero"),
            F.col("RESPUESTA_FECHA").alias("RespuestaFecha"),
            F.coalesce(
                F.col("RESPUESTA_DECIMAL").cast("decimal(18,2)"),
                F.col("RESPUESTA_ENTERO").cast("decimal(18,2)"),
            ).alias("ValorNumerico"),
        )
    )


# ── Mart RENAMU (indicadores municipales) ───────────────────────────────────


def build_mart_renamu(
    spark: SparkSession,
    fact_renamu: DataFrame,
    dim_ubigeo: DataFrame,
    dim_pregunta_renamu: DataFrame,
) -> DataFrame:
    u = dim_ubigeo.select("IdUbigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO")
    p = dim_pregunta_renamu.select("IdPregunta", "NOMBRE_CAMPO", "DESCRIPCION", "VALOR")

    valor_num = F.expr("try_cast(VALOR as decimal(18,2))")
    es_afirm = F.coalesce(
        (valor_num == 1)
        | (F.upper(F.trim(F.col("VALOR"))).isin("SI", "X", "TRUE", "VERDADERO", "S")),
        F.lit(False),
    )
    return (
        fact_renamu.join(F.broadcast(u), "IdUbigeo")
        .join(F.broadcast(p), "IdPregunta")
        .select(
            F.col("IdTiempo").cast("smallint").alias("Anio"),
            F.col("IdUbigeo").cast("int"),
            F.col("DEPARTAMENTO").alias("Departamento"),
            F.col("PROVINCIA").alias("Provincia"),
            F.col("DISTRITO").alias("Distrito"),
            F.col("TIPOMUNI").alias("TipoMuni"),
            F.col("IdPregunta").cast("int"),
            F.col("NOMBRE_CAMPO").alias("NombreCampo"),
            F.col("DESCRIPCION").alias("Descripcion"),
            F.col("VALOR").alias("ValorTexto"),
            valor_num.alias("ValorNumerico"),
            es_afirm.cast("boolean").alias("EsAfirmativo"),
        )
    )


# ── Orquestador ─────────────────────────────────────────────────────────────


def build_all(
    spark: SparkSession,
    silver_dir: Path = Path("data/silver"),
) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    _logger.info("Leyendo silver parquets")
    dim_tiempo = _read_parquet(spark, "DIM_TIEMPO", silver_dir).cache()
    dim_ubigeo = _read_parquet(spark, "DIM_UBIGEO", silver_dir).cache()
    dim_ejecutora = _read_parquet(spark, "DIM_EJECUTORA", silver_dir).cache()
    dim_nivel_gobierno = _read_parquet(spark, "DIM_NIVEL_GOBIERNO", silver_dir).cache()
    dim_rubro = _read_parquet(spark, "DIM_RUBRO", silver_dir).cache()
    dim_tipo_recurso = _read_parquet(spark, "DIM_TIPO_RECURSO", silver_dir).cache()
    dim_generica = _read_parquet(spark, "DIM_GENERICA", silver_dir).cache()
    dim_anio_aplicacion = _read_parquet(spark, "DIM_ANIO_APLICACION", silver_dir).cache()
    dim_formulario_sismepre = _read_parquet(spark, "DIM_FORMULARIO_SISMEPRE", silver_dir).cache()
    dim_pregunta_sismepre = _read_parquet(spark, "DIM_PREGUNTA_SISMEPRE", silver_dir).cache()
    dim_pregunta_renamu = _read_parquet(spark, "DIM_PREGUNTA_RENAMU", silver_dir).cache()

    _logger.info("Leyendo fact tables de silver")
    fact_ingreso = _read_partitioned_parquet(spark, "FACT_INGRESO", silver_dir, "IdTiempo").cache()
    fact_formulario_sismepre = _read_partitioned_parquet(
        spark, "FACT_FORMULARIO_SISMEPRE", silver_dir, "IdEjecutora"
    ).cache()
    fact_renamu = _read_partitioned_parquet(spark, "FACT_RENAMU", silver_dir, "IdPregunta").cache()

    cached = [
        fact_ingreso, fact_formulario_sismepre, fact_renamu,
        dim_tiempo, dim_ubigeo, dim_ejecutora, dim_nivel_gobierno,
        dim_rubro, dim_tipo_recurso, dim_generica, dim_anio_aplicacion,
        dim_formulario_sismepre, dim_pregunta_sismepre, dim_pregunta_renamu,
    ]

    dims = {
        "DIM_CALENDARIO": build_dim_calendario(spark, dim_tiempo),
        "DIM_ANIO": build_dim_anio(spark, dim_tiempo),
        "DIM_GEOGRAFIA": build_dim_geografia(spark, dim_ubigeo),
    }

    marts = {
        "MART_INGRESOS_GEOGRAFICO": build_mart_ingresos_geografico(
            spark, fact_ingreso, dim_tiempo, dim_ubigeo, dim_nivel_gobierno,
        ),
        "MART_INGRESOS_CLASIFICADOR": build_mart_ingresos_clasificador(
            spark, fact_ingreso, dim_tiempo, dim_rubro, dim_tipo_recurso, dim_generica,
        ),
        "MART_INGRESOS_EJECUTORA": build_mart_ingresos_ejecutora(
            spark, fact_ingreso, dim_tiempo, dim_ejecutora, dim_ubigeo,
        ),
        "MART_PREDIAL": build_mart_predial(
            spark, fact_formulario_sismepre, dim_ejecutora,
            dim_anio_aplicacion, dim_formulario_sismepre, dim_pregunta_sismepre,
        ),
        "MART_RENAMU": build_mart_renamu(
            spark, fact_renamu, dim_ubigeo, dim_pregunta_renamu,
        ),
    }

    _logger.info(f"Construidas {len(dims)} dims y {len(marts)} marts gold")
    return dims, marts
