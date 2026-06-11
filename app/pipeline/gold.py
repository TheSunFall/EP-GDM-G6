"""Pipeline de la capa Gold con PySpark.

Espeja a SilverPipeline: recibe un SparkClient, lee silver por JDBC, construye las
dimensiones y marts de negocio (app/gold/transforms.py) y los escribe en el esquema
gold via Spark JDBC. El DDL se crea con pymssql (app/gold/loader.py).
"""

import os

from dotenv import load_dotenv

from app.gold import loader, transforms
from app.settings.settings import settings
from app.utils.logging import UnifiedLogger
from app.utils.spark import SparkClient


class GoldPipeline:
    def __init__(self, spark_client: SparkClient):
        self.spark = spark_client.get_session()
        self.logger = UnifiedLogger("GoldPipeline", "gold")

    def run(self, drop: bool = False):
        self.logger.info("Iniciando Gold Pipeline")

        load_dotenv(settings.project_root / ".env")
        password = os.environ.get("MSSQL_SA_PASSWORD", "")
        db = settings.config.silver.database
        url = loader.build_jdbc_url(db)
        props = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": db.user,
            "password": password,
        }

        try:
            # 1. DDL (esquema + tablas) via pymssql
            self.logger.info("Paso 1/2: Creacion de esquema y tablas gold")
            loader.create_schema_and_tables(db, password, drop=drop)

            # 2. Construir y escribir dimensiones + marts via Spark JDBC
            self.logger.info("Paso 2/2: Construccion y carga de marts con Spark")
            tables, cached = transforms.build_all(self.spark, url, props)
            for name, df in tables.items():
                loader.write_table(df, name, url, props)

            # Liberar caches compartidos tras completar todas las escrituras
            for cdf in cached:
                cdf.unpersist()
        except Exception as e:
            self.logger.error(f"Error en Gold Pipeline: {e}", stack_trace=True)
            raise

        self.logger.info("Gold Pipeline completado exitosamente")
