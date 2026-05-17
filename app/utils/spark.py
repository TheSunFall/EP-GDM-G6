# Importar librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    countDistinct,
    current_date,
    date_format,
    dense_rank,
    length,
    lit,
    lower,
    max,
    min,
    rank,
    round,
    row_number,
    sum,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    ByteType,
    FloatType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("Analisis_Presupuesto_MEF")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

print(f"- Spark {spark.version} iniciado correctamente")
