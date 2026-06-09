"""Carga de la capa Gold: DDL via pymssql + escritura con Spark JDBC.

Espeja la arquitectura de silver: el esquema/tablas se crean con T-SQL (pymssql) y
los datos se escriben con Spark (spark.write.jdbc). Las transformaciones viven en
app/gold/transforms.py.
"""

import re
from pathlib import Path

import pymssql
from pyspark.sql import DataFrame

from app.schemas.settings_schema import SilverDatabaseConfig
from app.utils.logging import UnifiedLogger

_logger = UnifiedLogger("GoldLoader", "gold")

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_SQL_DIR = _PROJECT_ROOT / "sql"


def build_jdbc_url(db: SilverDatabaseConfig) -> str:
    return (
        f"jdbc:sqlserver://{db.host}:{db.port};"
        f"databaseName={db.database};encrypt=false;trustServerCertificate=true"
    )


def _pymssql_exec(
    db: SilverDatabaseConfig,
    password: str,
    sql: str,
    ignore_codes: set[int] | None = None,
) -> None:
    ignore_codes = ignore_codes or set()
    conn = None
    cursor = None
    try:
        conn = pymssql.connect(
            server=db.host, port=db.port, user=db.user,
            password=password, database=db.database, autocommit=True,
        )
        cursor = conn.cursor()
        cursor.execute(sql)
    except pymssql.DatabaseError as exc:
        code = exc.args[0] if getattr(exc, "args", None) else None
        if code in ignore_codes:
            _logger.warning(f"Codigo SQL ignorado {code}")
        else:
            raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _drop_gold(db: SilverDatabaseConfig, password: str) -> None:
    _logger.info("Eliminando esquema gold existente")
    _pymssql_exec(
        db, password,
        """
        DECLARE @sql NVARCHAR(MAX) = '';
        SELECT @sql += 'DROP PROCEDURE ' + QUOTENAME(s.name) + '.' + QUOTENAME(p.name) + ';'
        FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id
        WHERE s.name = 'gold';
        SELECT @sql += 'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';'
        FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'gold';
        IF @sql <> '' EXEC sp_executesql @sql;
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold') EXEC('DROP SCHEMA gold');
        """,
    )


def create_schema_and_tables(
    db: SilverDatabaseConfig, password: str, drop: bool = False
) -> None:
    """Crea el esquema y las tablas gold ejecutando tables_gold.sql via pymssql."""
    if drop:
        _drop_gold(db, password)

    sql_path = _SQL_DIR / "tables_gold.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"No se encontro {sql_path}")

    statements = [
        s.strip()
        for s in re.split(r"\bgo\b", sql_path.read_text(encoding="utf-8"), flags=re.IGNORECASE)
        if s.strip()
    ]
    _logger.info(f"Ejecutando {len(statements)} sentencias DDL gold")
    for stmt in statements:
        _pymssql_exec(db, password, stmt, ignore_codes={2714})
    _logger.info("Esquema y tablas gold creados/verificados")


def write_table(
    df: DataFrame, table: str, url: str, props: dict, num_partitions: int = 8
) -> None:
    """Escribe un DataFrame en gold.<table> truncando la tabla (preserva el DDL)."""
    _logger.info(f"Escribiendo gold.{table}")
    (
        df.repartition(num_partitions)
        .write.format("jdbc")
        .option("url", url)
        .option("dbtable", f"gold.{table}")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .option("truncate", "true")
        .option("batchsize", "20000")
        # isolationLevel NONE -> autocommit por lote: evita acumular millones de
        # bloqueos en una sola transaccion (error 1204 "cannot obtain a LOCK
        # resource") al cargar marts grandes como MART_RENAMU (~10M filas).
        .option("isolationLevel", "NONE")
        .mode("overwrite")
        .save()
    )
    _logger.info(f"gold.{table} cargada")
