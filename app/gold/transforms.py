"""Transformaciones de la capa Gold con PySpark.

Lee las tablas silver por JDBC y construye las dimensiones de presentacion y los
marts de negocio como DataFrames. Los joins hecho->dimension son por surrogate key
(Id*), que es unica en cada dimension, por lo que no hay fan-out.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def _read(spark: SparkSession, url: str, props: dict, table: str) -> DataFrame:
    return spark.read.jdbc(url=url, table=f"silver.{table}", properties=props)


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


def build_mart_ingresos_geografico(spark: SparkSession, url: str, props: dict) -> DataFrame:
    f = _read(spark, url, props, "FACT_INGRESO")
    t = _read(spark, url, props, "DIM_TIEMPO").select("IdTiempo", "ANIO", "MES")
    u = _read(spark, url, props, "DIM_UBIGEO").select(
        "IdUbigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"
    )
    ng = _read(spark, url, props, "DIM_NIVEL_GOBIERNO").select(
        "IdNivelGobierno", "NIVEL_GOBIERNO_NOMBRE"
    )
    return (
        f.join(t, "IdTiempo").join(u, "IdUbigeo").join(ng, "IdNivelGobierno")
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


def build_mart_ingresos_clasificador(spark: SparkSession, url: str, props: dict) -> DataFrame:
    f = _read(spark, url, props, "FACT_INGRESO")
    t = _read(spark, url, props, "DIM_TIEMPO").select("IdTiempo", "ANIO")
    r = _read(spark, url, props, "DIM_RUBRO").select("IdRubro", "RUBRO_NOMBRE")
    tr = _read(spark, url, props, "DIM_TIPO_RECURSO").select("IdTipoRecurso", "TIPO_RECURSO_NOMBRE")
    g = _read(spark, url, props, "DIM_GENERICA").select("IdGenerica", "GENERICA_NOMBRE", "SUBGENERICA_NOMBRE")
    return (
        f.join(t, "IdTiempo").join(r, "IdRubro").join(tr, "IdTipoRecurso").join(g, "IdGenerica")
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


def build_mart_ingresos_ejecutora(spark: SparkSession, url: str, props: dict) -> DataFrame:
    f = _read(spark, url, props, "FACT_INGRESO")
    t = _read(spark, url, props, "DIM_TIEMPO").select("IdTiempo", "ANIO")
    e = _read(spark, url, props, "DIM_EJECUTORA").select("IdEjecutora", "SEC_EJEC", "EJECUTORA_NOMBRE")
    u = _read(spark, url, props, "DIM_UBIGEO").select("IdUbigeo", "DEPARTAMENTO")
    return (
        f.join(t, "IdTiempo").join(e, "IdEjecutora").join(u, "IdUbigeo")
        .groupBy(
            F.col("ANIO").alias("Anio"),
            F.col("IdEjecutora"),
            F.col("SEC_EJEC").alias("SecEjec"),
            F.col("EJECUTORA_NOMBRE").alias("Ejecutora"),
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
            "Ejecutora", "Departamento",
            F.col("MontoPIA").cast("bigint"),
            F.col("MontoPIM").cast("bigint"),
            F.col("MontoRecaudado").cast("decimal(18,2)"),
            _pct("MontoRecaudado", "MontoPIM").alias("PctEjecucion"),
            (F.col("MontoPIM").cast("decimal(18,2)") - F.col("MontoRecaudado")).cast("decimal(18,2)").alias("Brecha"),
        )
    )


# ── Mart SISMEPRE (predial) - tall ──────────────────────────────────────────

def build_mart_predial(spark: SparkSession, url: str, props: dict) -> DataFrame:
    f = _read(spark, url, props, "FACT_FORMULARIO_SISMEPRE")
    aa = _read(spark, url, props, "DIM_ANIO_APLICACION").select("IdAnioAplicacion", "ANO_APLICACION")
    e = _read(spark, url, props, "DIM_EJECUTORA").select("IdEjecutora", "SEC_EJEC", "EJECUTORA_NOMBRE")
    fo = _read(spark, url, props, "DIM_FORMULARIO_SISMEPRE").select(
        F.col("IdFormSismepre").alias("IdFormulario"), F.col("TITULO")
    )
    p = _read(spark, url, props, "DIM_PREGUNTA_SISMEPRE").select(
        F.col("IdPreguntaSismepre").alias("IdPregunta"), F.col("DESCRIPCION")
    )
    return (
        f.join(aa, "IdAnioAplicacion").join(e, "IdEjecutora")
        .join(fo, "IdFormulario").join(p, "IdPregunta")
        .select(
            F.col("ANO_APLICACION").cast("smallint").alias("Anio"),
            F.col("PERIODO").cast("smallint").alias("Periodo"),
            F.col("IdEjecutora").cast("int"),
            F.col("SEC_EJEC").cast("int").alias("SecEjec"),
            F.col("EJECUTORA_NOMBRE").alias("Municipalidad"),
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

def build_mart_renamu(spark: SparkSession, url: str, props: dict) -> DataFrame:
    f = _read(spark, url, props, "FACT_RENAMU")
    u = _read(spark, url, props, "DIM_UBIGEO").select("IdUbigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO")
    p = _read(spark, url, props, "DIM_PREGUNTA_RENAMU").select("IdPregunta", "NOMBRE_CAMPO", "DESCRIPCION", "VALOR")

    valor_num = F.expr("try_cast(VALOR as decimal(18,2))")
    es_afirm = F.coalesce(
        (valor_num == 1)
        | (F.upper(F.trim(F.col("VALOR"))).isin("SI", "X", "TRUE", "VERDADERO", "S")),
        F.lit(False),
    )
    return (
        f.join(u, "IdUbigeo").join(p, "IdPregunta")
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

def build_all(spark: SparkSession, url: str, props: dict) -> dict[str, DataFrame]:
    """Devuelve {nombre_tabla_gold: DataFrame} en orden de carga (dims primero)."""
    return {
        "DIM_CALENDARIO": build_dim_calendario(spark, url, props),
        "DIM_ANIO": build_dim_anio(spark, url, props),
        "DIM_GEOGRAFIA": build_dim_geografia(spark, url, props),
        "MART_INGRESOS_GEOGRAFICO": build_mart_ingresos_geografico(spark, url, props),
        "MART_INGRESOS_CLASIFICADOR": build_mart_ingresos_clasificador(spark, url, props),
        "MART_INGRESOS_EJECUTORA": build_mart_ingresos_ejecutora(spark, url, props),
        "MART_PREDIAL": build_mart_predial(spark, url, props),
        "MART_RENAMU": build_mart_renamu(spark, url, props),
    }
