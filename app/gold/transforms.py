"""Transformaciones de la capa Gold con PySpark.

Lee las tablas silver por JDBC y construye las dimensiones de presentacion y los
marts de negocio como DataFrames. Los joins hecho->dimension son por surrogate key
(Id*), que es unica en cada dimension, por lo que no hay fan-out.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("GoldTransforms", "gold")


def _read(spark: SparkSession, url: str, props: dict, table: str) -> DataFrame:
    return spark.read.jdbc(url=url, table=f"silver.{table}", properties=props)


def _read_partitioned(
    spark: SparkSession,
    url: str,
    props: dict,
    table: str,
    partition_column: str,
    num_partitions: int = 16,
) -> DataFrame:
    """Lee una tabla grande por JDBC con lectura paralela en múltiples particiones.

    Sin particionamiento, spark.read.jdbc usa UNA sola conexión JDBC y crea UN solo
    partition, lo que significa que millones de filas se leen secuencialmente en un
    solo hilo. Esto es el cuello de botella principal para FACT_RENAMU (~12M filas)
    y FACT_FORMULARIO_SISMEPRE.
    """
    full_table = f"silver.{table}"
    bounds = spark.read.jdbc(url=url, table=full_table, properties=props).agg(
        F.min(partition_column).alias("lo"),
        F.max(partition_column).alias("hi"),
    ).collect()[0]

    lo, hi = int(bounds["lo"]), int(bounds["hi"])
    _logger.info(
        f"Leyendo {full_table} particionado por {partition_column} "
        f"[{lo}, {hi}] en {num_partitions} particiones"
    )
    return spark.read.jdbc(
        url=url,
        table=full_table,
        column=partition_column,
        lowerBound=lo,
        upperBound=hi + 1,
        numPartitions=num_partitions,
        properties=props,
    )


# ── Dimensiones de presentacion ─────────────────────────────────────────────

def build_dim_calendario(spark: SparkSession, url: str, props: dict) -> DataFrame:
    """Calendario contiguo (sin huecos) entre el min y max anio de silver.DIM_TIEMPO.

    Implementado en Spark SQL puro (sequence/explode/make_date) para no requerir
    workers de Python (evita crashes del worker en Windows).
    """
    dt = _read(spark, url, props, "DIM_TIEMPO").filter(
        (F.col("ANIO") >= 2000) & (F.col("ANIO") <= 2100)
    )
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


def build_dim_anio(spark: SparkSession, url: str, props: dict) -> DataFrame:
    return (
        _read(spark, url, props, "DIM_TIEMPO")
        .filter((F.col("ANIO") >= 2000) & (F.col("ANIO") <= 2100))
        .select(F.col("ANIO").cast("smallint").alias("Anio"))
        .distinct()
    )


def build_dim_geografia(spark: SparkSession, url: str, props: dict) -> DataFrame:
    u = _read(spark, url, props, "DIM_UBIGEO")
    return u.select(
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
    spark: SparkSession, url: str, props: dict,
    fact_ingreso: DataFrame, dim_tiempo: DataFrame, dim_ubigeo: DataFrame,
) -> DataFrame:
    ng = _read(spark, url, props, "DIM_NIVEL_GOBIERNO").select(
        "IdNivelGobierno", "NIVEL_GOBIERNO_NOMBRE"
    )
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
    spark: SparkSession, url: str, props: dict,
    fact_ingreso: DataFrame, dim_tiempo: DataFrame,
) -> DataFrame:
    t = dim_tiempo.select("IdTiempo", "ANIO")
    r = _read(spark, url, props, "DIM_RUBRO").select("IdRubro", "RUBRO_NOMBRE")
    tr = _read(spark, url, props, "DIM_TIPO_RECURSO").select("IdTipoRecurso", "TIPO_RECURSO_NOMBRE")
    g = _read(spark, url, props, "DIM_GENERICA").select("IdGenerica", "GENERICA_NOMBRE", "SUBGENERICA_NOMBRE")
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
    spark: SparkSession, url: str, props: dict,
    fact_ingreso: DataFrame, dim_tiempo: DataFrame, dim_ejecutora: DataFrame,
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


# ── Mart SISMEPRE (predial) - tall ──────────────────────────────────────────

def build_mart_predial(
    spark: SparkSession, url: str, props: dict,
    dim_ejecutora: DataFrame,
) -> DataFrame:
    f = _read_partitioned(spark, url, props, "FACT_FORMULARIO_SISMEPRE", "IdEjecutora")
    aa = _read(spark, url, props, "DIM_ANIO_APLICACION").select("IdAnioAplicacion", "ANO_APLICACION")
    e = dim_ejecutora.select("IdEjecutora", "SEC_EJEC", "EJECUTORA_NOMBRE", "CATEGORIA")
    fo = _read(spark, url, props, "DIM_FORMULARIO_SISMEPRE").select(
        F.col("IdFormSismepre").alias("IdFormulario"), F.col("TITULO")
    )
    p = _read(spark, url, props, "DIM_PREGUNTA_SISMEPRE").select(
        F.col("IdPreguntaSismepre").alias("IdPregunta"), F.col("DESCRIPCION")
    )
    return (
        f.join(F.broadcast(aa), "IdAnioAplicacion")
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


# ── Mart RENAMU (indicadores municipales) - tall, resuelve factless ─────────

def build_mart_renamu(
    spark: SparkSession, url: str, props: dict,
    dim_ubigeo: DataFrame,
) -> DataFrame:
    f = _read_partitioned(spark, url, props, "FACT_RENAMU", "IdPregunta")
    u = dim_ubigeo.select("IdUbigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO")
    p = _read(spark, url, props, "DIM_PREGUNTA_RENAMU").select("IdPregunta", "NOMBRE_CAMPO", "DESCRIPCION", "VALOR")

    valor_num = F.expr("try_cast(VALOR as decimal(18,2))")
    es_afirm = F.coalesce(
        (valor_num == 1)
        | (F.upper(F.trim(F.col("VALOR"))).isin("SI", "X", "TRUE", "VERDADERO", "S")),
        F.lit(False),
    )
    return (
        f.join(F.broadcast(u), "IdUbigeo")
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
    spark: SparkSession, url: str, props: dict
) -> tuple[dict[str, DataFrame], list[DataFrame]]:
    """Devuelve ({nombre_tabla_gold: DataFrame}, [cached_dfs]) en orden de carga.

    Las dimensiones compartidas (UBIGEO, EJECUTORA, TIEMPO) se leen UNA sola vez
    y se cachean para evitar lecturas JDBC redundantes entre marts.
    Las tablas de hechos grandes se leen con lectura JDBC particionada.

    Devuelve la lista de DataFrames cacheados para que el llamador los libere
    tras completar las escrituras (los DataFrames son lazy y necesitan el cache
    vivo durante la materialización).
    """
    # Dimensiones compartidas: leer una vez, cachear
    _logger.info("Leyendo y cacheando dimensiones compartidas de silver")
    dim_tiempo = _read(spark, url, props, "DIM_TIEMPO").cache()
    dim_ubigeo = _read(spark, url, props, "DIM_UBIGEO").cache()
    dim_ejecutora = _read(spark, url, props, "DIM_EJECUTORA").cache()

    # Hecho compartido: FACT_INGRESO lo usan 3 marts SIAF
    _logger.info("Leyendo FACT_INGRESO con lectura particionada")
    fact_ingreso = _read_partitioned(
        spark, url, props, "FACT_INGRESO", "IdTiempo"
    ).cache()

    cached = [fact_ingreso, dim_tiempo, dim_ubigeo, dim_ejecutora]

    tables = {
        "DIM_CALENDARIO": build_dim_calendario(spark, url, props),
        "DIM_ANIO": build_dim_anio(spark, url, props),
        "DIM_GEOGRAFIA": build_dim_geografia(spark, url, props),
        "MART_INGRESOS_GEOGRAFICO": build_mart_ingresos_geografico(
            spark, url, props, fact_ingreso, dim_tiempo, dim_ubigeo,
        ),
        "MART_INGRESOS_CLASIFICADOR": build_mart_ingresos_clasificador(
            spark, url, props, fact_ingreso, dim_tiempo,
        ),
        "MART_INGRESOS_EJECUTORA": build_mart_ingresos_ejecutora(
            spark, url, props, fact_ingreso, dim_tiempo, dim_ejecutora, dim_ubigeo,
        ),
        "MART_PREDIAL": build_mart_predial(spark, url, props, dim_ejecutora),
        "MART_RENAMU": build_mart_renamu(spark, url, props, dim_ubigeo),
    }

    _logger.info(f"Tablas gold construidas: {len(tables)} tablas")
    return tables, cached
