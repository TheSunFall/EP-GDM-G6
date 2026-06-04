"""Carga de datos Silver a SQL Server: DDL vía pymssql y escritura con Spark."""

import os
import re
from pathlib import Path

import pymssql
from pyspark.sql import DataFrame, SparkSession

from app.schemas.settings_schema import SilverConfig
from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("SilverLoader", "silver")

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_SQL_DIR = _PROJECT_ROOT / "sql"

_ALL_SILVER_TABLES = [
    "FACT_RENAMU",
    "FACT_FORMULARIO_SISMEPRE",
    "FACT_INGRESO",
    "DIM_PREGUNTA_SISMEPRE",
    "DIM_PREGUNTA_RENAMU",
    "DIM_FORMULARIO_SISMEPRE",
    "DIM_ANIO_APLICACION",
    "DIM_TIEMPO",
    "DIM_UBIGEO",
    "DIM_TIPO_RECURSO",
    "DIM_RUBRO",
    "DIM_PLIEGO",
    "DIM_SECTOR",
    "DIM_NIVEL_GOBIERNO",
    "DIM_GENERICA",
    "DIM_FUENTE_FINANCIAMIENTO",
    "DIM_ESPECIFICA",
    "DIM_EJECUTORA",
]

# Tablas IDENTITY: SQL Server genera el Id — no lo incluimos al escribir.
_IDENTITY_COLS = {
    "silver.DIM_EJECUTORA": "IdEjecutora",
    "silver.DIM_ESPECIFICA": "IdEspecifica",
    "silver.DIM_FORMULARIO_SISMEPRE": "IdFormSismepre",
    "silver.DIM_FUENTE_FINANCIAMIENTO": "IdFuenteFinanciamiento",
    "silver.DIM_GENERICA": "IdGenerica",
    "silver.DIM_NIVEL_GOBIERNO": "IdNivelGobierno",
    "silver.DIM_PLIEGO": "IdPliego",
    "silver.DIM_PREGUNTA_RENAMU": "IdPregunta",
    "silver.DIM_PREGUNTA_SISMEPRE": "IdPreguntaSismepre",
    "silver.DIM_RUBRO": "IdRubro",
    "silver.DIM_SECTOR": "IdSector",
    "silver.DIM_TIPO_RECURSO": "IdTipoRecurso",
    "silver.DIM_UBIGEO": "IdUbigeo",
    "silver.DIM_ANIO_APLICACION": "IdAnioAplicacion",
    # DIM_TIEMPO no está aquí: IdTiempo es PK explícita, no IDENTITY
}

# Columnas necesarias para los joins FK en las tablas de hechos.
_FK_JOIN_COLS = {
    "DIM_TIEMPO": ["IdTiempo"],
    "DIM_EJECUTORA": ["SEC_EJEC", "IdEjecutora"],
    "DIM_UBIGEO": [
        "CODIGODEPARTAMENTO",
        "CODIGOPROVINCIA",
        "CODIGODISTRITO",
        "IdUbigeo",
    ],
    "DIM_NIVEL_GOBIERNO": ["NIVEL_GOBIERNO", "IdNivelGobierno"],
    "DIM_SECTOR": ["SECTOR", "IdSector"],
    "DIM_PLIEGO": ["PLIEGO", "IdPliego"],
    "DIM_RUBRO": ["RUBRO", "IdRubro"],
    "DIM_TIPO_RECURSO": ["TIPO_RECURSO", "IdTipoRecurso"],
    "DIM_FUENTE_FINANCIAMIENTO": ["FUENTE_FINANCIAMIENTO", "IdFuenteFinanciamiento"],
    "DIM_GENERICA": ["GENERICA", "SUBGENERICA", "SUBGENERICA_DET", "IdGenerica"],
    "DIM_ESPECIFICA": ["ESPECIFICA", "ESPECIFICA_DET", "IdEspecifica"],
    "DIM_ANIO_APLICACION": ["ANO_APLICACION", "IdAnioAplicacion"],
    "DIM_FORMULARIO_SISMEPRE": ["FORMULARIO_ID", "IdFormSismepre"],
    "DIM_PREGUNTA_RENAMU": ["NOMBRE_CAMPO", "VALOR", "IdPregunta"],
    "DIM_PREGUNTA_SISMEPRE": ["IdFormSismepre", "PREGUNTA_ID", "IdPreguntaSismepre"],
}


def _build_jdbc_url(cfg: SilverConfig) -> str:
    return (
        f"jdbc:sqlserver://{cfg.database.host}:{cfg.database.port};"
        f"databaseName={cfg.database.database};"
        f"encrypt=false;trustServerCertificate=true"
    )


def _pymssql_exec(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql: str,
    ignore_codes: set[int] | None = {2714},
    connect_to_master: bool = False,
    autocommit: bool = False,
) -> None:
    ignore_codes = ignore_codes or set()
    conn = None
    cursor = None
    # Use 'master' database to perform DDL if necessary to create the DB first
    db = "master" if connect_to_master else database
    try:
        conn = pymssql.connect(
            server=host,
            port=port,
            user=user,
            password=password,
            database=db,
            autocommit=autocommit,
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        if not autocommit:
            conn.commit()
    except pymssql.DatabaseError as exc:
        if conn and not autocommit:
            conn.rollback()
        error_code = None
        if hasattr(exc, "args") and exc.args:
            error_code = exc.args[0]
        if error_code in ignore_codes:
            _logger.warning(f"Ignored SQL error code {error_code}")
        else:
            raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_schema_and_tables(
    cfg: SilverConfig, password: str, drop: bool = False
) -> None:
    """Ejecuta tables_silver.sql dividiendo en sentencias individuales por GO."""
    # Ensure database exists
    _pymssql_exec(
        host=cfg.database.host,
        port=cfg.database.port,
        database=cfg.database.database,
        user=cfg.database.user,
        password=password,
        sql=f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{cfg.database.database}') CREATE DATABASE {cfg.database.database}",
        connect_to_master=True,
        autocommit=True,
    )

    if drop:
        _drop_schema(cfg, password)

    sql_path = _SQL_DIR / "tables_silver.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"No se encontró {sql_path}")

    raw = sql_path.read_text(encoding="utf-8")
    statements = [
        s.strip() for s in re.split(r"\bgo\b", raw, flags=re.IGNORECASE) if s.strip()
    ]

    _logger.info(f"Ejecutando {len(statements)} sentencias DDL")
    for stmt in statements:
        _pymssql_exec(
            host=cfg.database.host,
            port=cfg.database.port,
            database=cfg.database.database,
            user=cfg.database.user,
            password=password,
            sql=stmt,
        )
    _logger.info("Esquema y tablas creados/verificados")


def _drop_schema(cfg: SilverConfig, password: str) -> None:
    """Elimina el esquema silver y todas sus tablas."""
    _logger.info("Eliminando esquema silver existente")
    _pymssql_exec(
        host=cfg.database.host,
        port=cfg.database.port,
        database=cfg.database.database,
        user=cfg.database.user,
        password=password,
        sql="""
            DECLARE @sql NVARCHAR(MAX) = '';
            SELECT @sql += 'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(fk.schema_id))
                + '.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id))
                + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            WHERE SCHEMA_NAME(fk.schema_id) = 'silver';
            EXEC sp_executesql @sql;
        """,
        ignore_codes=None,
    )
    for table in _ALL_SILVER_TABLES:
        _pymssql_exec(
            host=cfg.database.host,
            port=cfg.database.port,
            database=cfg.database.database,
            user=cfg.database.user,
            password=password,
            sql=f"IF OBJECT_ID('silver.{table}', 'U') IS NOT NULL DROP TABLE silver.{table}",
            ignore_codes=None,
        )
    _pymssql_exec(
        host=cfg.database.host,
        port=cfg.database.port,
        database=cfg.database.database,
        user=cfg.database.user,
        password=password,
        sql="IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') DROP SCHEMA silver",
        ignore_codes=None,
    )
    _logger.info("Esquema silver eliminado")


def write_dimension(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    url: str,
    props: dict,
    table_short_name: str = "",
) -> DataFrame:
    """
    Escribe una dimensión y devuelve el DataFrame con los IDs reales del servidor.
    """
    id_col = _IDENTITY_COLS.get(table)
    write_df = df.drop(id_col) if (id_col and id_col in df.columns) else df

    _logger.info(f"Escribiendo {table}")
    write_df.write.jdbc(url=url, table=table, mode="append", properties=props)

    fk_cols = _FK_JOIN_COLS.get(table_short_name)
    if fk_cols:
        server_df = spark.read.jdbc(url=url, table=table, properties=props).select(
            *fk_cols
        )
    else:
        server_df = spark.read.jdbc(url=url, table=table, properties=props)
    _logger.info(f"{table} cargada y leída de vuelta")
    return server_df


def write_fact(df: DataFrame, table: str, url: str, props: dict) -> None:
    """Escribe una tabla de hechos en SQL Server."""
    _logger.info(f"Escribiendo {table}")
    df.repartition(4).write.jdbc(url=url, table=table, mode="append", properties=props)
    _logger.info(f"{table} cargada correctamente")


def create_and_load(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    stage: dict,
    cfg: SilverConfig,
    drop: bool = False,
) -> None:
    """
    Orquesta la creación de tablas y carga completa en SQL Server.
    """
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")

    password = os.environ.get("MSSQL_SA_PASSWORD", "")
    url = _build_jdbc_url(cfg)
    jdbc_props = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": cfg.database.user,
        "password": password,
    }

    # 1. DDL via pymssql
    _logger.info("Iniciando creación de esquema y tablas en SQL Server")
    create_schema_and_tables(cfg, password, drop=drop)
    _logger.info("Esquema y tablas creados/verificados exitosamente")

    # 2. Escribir dimensiones en orden, acumular versiones del servidor con IDs reales
    dim_write_order = [
        "DIM_TIEMPO",
        "DIM_EJECUTORA",
        "DIM_UBIGEO",
        "DIM_NIVEL_GOBIERNO",
        "DIM_SECTOR",
        "DIM_PLIEGO",
        "DIM_RUBRO",
        "DIM_TIPO_RECURSO",
        "DIM_FUENTE_FINANCIAMIENTO",
        "DIM_GENERICA",
        "DIM_ESPECIFICA",
        "DIM_ANIO_APLICACION",
        "DIM_FORMULARIO_SISMEPRE",
        "DIM_PREGUNTA_RENAMU",
    ]

    server_dims: dict[str, DataFrame] = {}
    for name in dim_write_order:
        if name not in dims:
            _logger.warning(f"{name} no encontrado en dims, se omite")
            continue
        _logger.info(f"Cargando dimensión {name} en SQL Server")
        server_dims[name] = write_dimension(
            spark,
            dims[name],
            f"silver.{name}",
            url,
            jdbc_props,
            table_short_name=name,
        )
        server_dims[name].cache()
        _logger.info(f"Dimensión {name} cargada exitosamente")

    # 3. DIM_PREGUNTA_SISMEPRE
    if "DIM_FORMULARIO_SISMEPRE" in server_dims:
        from app.silver.transforms import build_dim_pregunta_sismepre

        _logger.info("Construyendo y cargando DIM_PREGUNTA_SISMEPRE")
        dim_preg_sis = build_dim_pregunta_sismepre(
            stage["rentas_preguntas"],
            server_dims["DIM_FORMULARIO_SISMEPRE"],
        )
        server_dims["DIM_PREGUNTA_SISMEPRE"] = write_dimension(
            spark,
            dim_preg_sis,
            "silver.DIM_PREGUNTA_SISMEPRE",
            url,
            jdbc_props,
            table_short_name="DIM_PREGUNTA_SISMEPRE",
        )
        server_dims["DIM_PREGUNTA_SISMEPRE"].cache()
        _logger.info("DIM_PREGUNTA_SISMEPRE cargada exitosamente")

    # 4. Construir y escribir hechos
    from app.silver.transforms import build_facts

    _logger.info("Construyendo y cargando tablas de hechos")
    facts = build_facts(spark, stage, server_dims)

    for name in ("FACT_INGRESO", "FACT_FORMULARIO_SISMEPRE", "FACT_RENAMU"):
        if name in facts:
            _logger.info(f"Cargando tabla de hechos {name}")
            write_fact(facts[name], f"silver.{name}", url, jdbc_props)
            _logger.info(f"Tabla de hechos {name} cargada exitosamente")

    for sdf in server_dims.values():
        sdf.unpersist()
    _logger.info("Pipeline Silver completado: todas las tablas cargadas en SQL Server")
