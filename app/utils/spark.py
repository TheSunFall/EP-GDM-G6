import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(Path(__file__).parent.parent.parent / ".env")

_LIB_DIR = Path(__file__).parent.parent.parent / "lib"

# Requerido para Spark en Windows: hadoop.dll debe estar en PATH antes de que la JVM arranque
if hadoop_home := os.environ.get("HADOOP_HOME"):
    os.environ["hadoop.home.dir"] = hadoop_home
    hadoop_bin = str(Path(hadoop_home) / "bin")
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def _resolve_python_executable() -> str:
    """Return the Python executable Spark workers should use.

    Prefers the venv interpreter so Spark workers inherit the same packages.
    Falls back to sys.executable, which is always the running interpreter.
    """
    import sys

    venv_python = (
        Path(__file__).parent.parent.parent / ".venv" / "Scripts" / "python.exe"
    )
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


class SparkClient:
    def __init__(self) -> None:
        python_exec = _resolve_python_executable()
        os.environ.setdefault("PYSPARK_PYTHON", python_exec)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

        mssql_jar = str(_LIB_DIR / "mssql-jdbc-13.4.0.jre11.jar")
        hadoop_bin = str(Path(os.environ.get("HADOOP_HOME", "")) / "bin")
        self.spark = (
            SparkSession.builder.appName("Analisis_Presupuesto_MEF")
            .config("spark.driver.memory", "8g")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.jars", mssql_jar)
            .config(
                "spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11"
            )
            .config(
                "spark.driver.extraJavaOptions", f"-Djava.library.path={hadoop_bin}"
            )
            .config(
                "spark.executor.extraJavaOptions", f"-Djava.library.path={hadoop_bin}"
            )
            .config("spark.pyspark.python", python_exec)
            .getOrCreate()
        )

    def get_session(self):
        return self.spark
