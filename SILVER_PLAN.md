# Plan de Implementación: Capa Silver

## 1. Estructura del Proyecto Resultante

```
app/
├── pipeline/
│   ├── __init__.py
│   ├── bronze.py          # existente — sin cambios
│   ├── silver.py          # NUEVO — orquestador (clase SilverPipeline)
│   ├── gold.py            # existente — stub vacío
│   └── profile.py         # existente — sin cambios
├── silver/
│   ├── __init__.py        # NUEVO
│   ├── quality.py         # NUEVO — correcciones de calidad sobre datos bronze
│   ├── transforms.py      # NUEVO — transformaciones estrella (dimensiones + hechos)
│   ├── loader.py          # NUEVO — creación de esquema SQL Server + bulk insert
│   └── models.py          # NUEVO — clases de dominio para tipado y consistencia
├── schemas/
│   └── settings_schema.py # existente — posible extensión para config silver
├── settings/
│   └── settings.py        # existente — sin cambios
├── client/
│   ├── mef_client.py      # existente — sin cambios
│   └── ...                # existente
└── utils/
    ├── spark.py           # existente — sin cambios
    ├── profiler.py        # existente — sin cambios (sus reportes guían quality.py)
    └── logging.py         # existente

data/
├── bronze/                # existente — entrada (parquet crudos)
├── silver/
│   ├── stage/             # NUEVO — parquet con correcciones de calidad aplicadas
│   └── csv/               # NUEVO — archivos CSV temporales para BULK INSERT
└── profiling/             # existente — reportes de calidad (guían las correcciones)

sql/
├── tables_silver.sql      # existente — DDL de referencia para las tablas silver
└── proc_slver.sql         # existente — solo como referencia conceptual (no se ejecuta)
```

## 2. Flujo de la Aplicación

### 2.1. Modificación en `main.py`

Se agrega un comando `silver` que ejecute `SilverPipeline`:

```python
# main.py (modificado)
import sys
from app.pipeline.bronze import BronzePipeline
from app.pipeline.silver import SilverPipeline
from app.utils.spark import SparkClient

def main():
    spark = SparkClient()
    if len(sys.argv) > 1 and sys.argv[1] == "silver":
        pipeline = SilverPipeline(spark)
        pipeline.run()
    else:
        pipeline = BronzePipeline()
        pipeline.run()

if __name__ == "__main__":
    main()
```

Comando de ejecución: `python main.py silver`

### 2.2. Orquestación en `SilverPipeline.run()`

El método `run()` ejecuta estos pasos en orden secuencial:

```
1. quality.fix_all()
   ├── Lee todos los parquet de data/bronze/
   ├── Aplica correcciones de calidad (ver sección 3)
   └── Guarda en data/silver/stage/ (un archivo por tabla lógica)

2. transforms.build_star_schema()
   ├── Lee parquet de data/silver/stage/
   ├── Construye tablas de dimensiones (DIM_*)
   ├── Construye tablas de hechos (FACT_*) uniendo a las dimensiones
   └── Guarda cada dimensión/hecho como CSV en data/silver/csv/{TablaNombre}/

3. loader.create_and_load()
   ├── Conecta a SQL Server vía JDBC
   ├── Crea el esquema silver y las tablas (DDL equivalente a tables_silver.sql)
   ├── Para cada dimensión (ordenadas por dependencias): ejecuta BULK INSERT
   └── Para cada hecho (ordenados por dependencias): ejecuta BULK INSERT
```

## 3. Limpieza y Corrección de Calidad (`quality.py`)

### 3.1. Entrada

Archivos parquet en `data/bronze/`:
- `SIAF-*-Ingreso*.parquet` (múltiples años, ej. 2021-Ingreso, 2022-Ingreso, etc.) → se unifican en un solo DataFrame `ingreso_unified`
- `SISMEPRE-rentas_preguntas.parquet`
- `SISMEPRE-rentas_estadistica.parquet`
- `SISMEPRE-rentas_formulario.parquet`
- `SISMEPRE-rentas_esat_estadistica_atm.parquet`
- `SISMEPRE-rentas_respuestas.parquet`
- `SISMEPRE-rentas_ano_aplicacion.parquet`
- `SISMEPRE-rentas_entidad_estado.parquet`
- `RENAMU-2021.parquet`, `RENAMU-2022.parquet`, `RENAMU-2023.parquet`, `RENAMU-2024.parquet`
- `RENAMU-984-Modulo1963.parquet`
- Diccionarios (parquet en `data/bronze/dicts/`)

### 3.2. Correcciones Aplicadas

Cada corrección se implementa como una transformación Spark.

| # | Problema | Corrección | Dataset Afectado |
|---|----------|-----------|------------------|
| 1 | Columnas numéricas leídas como string | `CAST`/`TRY_CAST` a tipo numérico (`Int`, `SmallInt`, `Numeric(18,2)`, `BigInt`) según destino en dimensión/hecho | Todos |
| 2 | Valores `NULL` en columnas que deben tener valores por defecto | `COALESCE(valor, 0)` o `COALESCE(valor, '')` según el contexto | Hechos (montos), descripciones |
| 3 | `ANO_APLICACION`/`PERIODO` como float científico (ej. `2.021e3`) | `CAST(CAST(col AS DOUBLE) AS INT)` | SISMEPRE: rentas_ano_aplicacion, rentas_respuestas |
| 4 | `SEC_EJEC` como string numérico | `CAST(col AS INT)` | SIAF, SISMEPRE |
| 5 | Códigos ubigeo con padding inconsistente | `LPAD(TRIM(col), 2, '0')` para departamento/provincia/distrito; luego `CAST AS SMALLINT` | SIAF (ubicación ejecutora), RENAMU (ccdd, ccpp, ccdi) |
| 6 | Columnas de preguntas RENAMU como columnas individuales | Se estandarizan tipos en quality.py; el unpivot se hace en transforms.py | RENAMU |
| 7 | `FORMULARIO_ID`, `PREGUNTA_ID` como float científico | `CAST(CAST(col AS DOUBLE) AS SMALLINT)` | SISMEPRE |
| 8 | Fechas inconsistentes | Estandarizar a formato `YYYY-MM-DD` donde se pueda parsear; de lo contrario mantener como string | SISMEPRE respuestas |
| 9 | Duplicados explícitos | `dropDuplicates()` en combinaciones de columnas que debieran ser únicas | Todas las dimensiones |

Cada corrección se registra en el log (cuántas filas se afectaron, cuántos valores se transformaron).

### 3.3. Salida

Se guarda un archivo parquet por "tabla lógica" en `data/silver/stage/`:

```
data/silver/stage/
├── ingreso_unified.parquet          # unión de todos los SIAF-*-Ingreso*.parquet
├── rentas_ano_aplicacion.parquet
├── rentas_formulario.parquet
├── rentas_preguntas.parquet
├── rentas_esat_estadistica_atm.parquet
├── rentas_respuestas.parquet
├── renamu_2021.parquet
├── renamu_2022.parquet
├── renamu_2023.parquet
├── renamu_2024.parquet
└── renamu_984_modulo1963.parquet
```

## 4. Transformaciones a Modelo Estrella (`transforms.py`)

### 4.1. Arquitectura de Destino

Basado en `sql/tables_silver.sql`, el modelo estrella contiene:

**Dimensiones compartidas entre datasets:**
| Tabla | Columnas | Origen |
|-------|----------|--------|
| `DIM_TIEMPO` | IdTiempo (INT = AÑO*100+MES para SIAF, solo AÑO para RENAMU), ANIO, MES, DIA, HORA, MINUTO, SEGUNDO | Derivado de ANO_DOC/MES_DOC (SIAF) y año (RENAMU/SISMEPRE). Sin registro artificial 0. |
| `DIM_EJECUTORA` | IdEjecutora, SEC_EJEC, EJECUTORA, EJECUTORA_NOMBRE | ingreso_unified + rentas_esat_estadistica_atm + rentas_respuestas |
| `DIM_UBIGEO` | IdUbigeo, CODIGODEPARTAMENTO, DEPARTAMENTO, CODIGOPROVINCIA, PROVINCIA, CODIGODISTRITO, DISTRITO | ingreso_unified + rentas_esat_estadistica_atm + RENAMU |

**Dimensiones SIAF (FACT_INGRESO):**
| Tabla | Origen |
|-------|--------|
| `DIM_NIVEL_GOBIERNO`, `DIM_SECTOR`, `DIM_PLIEGO` | ingreso_unified |
| `DIM_RUBRO`, `DIM_TIPO_RECURSO` | ingreso_unified |
| `DIM_GENERICA`, `DIM_ESPECIFICA` | ingreso_unified |
| `DIM_FUENTE_FINANCIAMIENTO` | ingreso_unified |

**Dimensiones SISMEPRE (FACT_FORMULARIO_SISMEPRE):**
| Tabla | Origen |
|-------|--------|
| `DIM_FORMULARIO_SISMEPRE` | rentas_formulario |
| `DIM_PREGUNTA_SISMEPRE` | rentas_preguntas (+ join a formulario) |
| `DIM_ANIO_APLICACION` | rentas_ano_aplicacion |

**Dimensiones RENAMU (FACT_RENAMU):**
| Tabla | Origen |
|-------|--------|
| `DIM_PREGUNTA_RENAMU` | Columnas-pregunta de RENAMU por año → unpivot → dedup |
| (reutiliza DIM_TIEMPO, DIM_UBIGEO) | |

**Tablas de hechos:**
| Tabla | Granularidad | Origen |
|-------|-------------|--------|
| `FACT_INGRESO` | Un registro por combinación (tiempo, ejecutora, ubicación, clasificador) con montos PIA, PIM, Recaudado | ingreso_unified |
| `FACT_FORMULARIO_SISMEPRE` | Un registro por (ejecutora, año_aplicación, formulario, pregunta, período) y su respuesta | rentas_respuestas |
| `FACT_RENAMU` | Un registro por (año, ubigeo, tipomuni, pregunta) con el valor de respuesta | renamu_XXXX |

### 4.2. Lógica de Construcción

**Fase 4.2a — Carga de Dimensiones**

Para cada dimensión se sigue este patrón general:

1. `SELECT DISTINCT` de la(s) columna(s) de clave natural desde la tabla stage correspondiente.
2. `LEFT ANTI JOIN` contra la dimensión para evitar insertar claves que ya existen.
3. Conversión de tipos segura: en lugar de `TRY_CONVERT` de T-SQL, se usa `col(...).cast(tipo)` con filtro `.isNotNull()`.
4. Asignación de `Id` auto-incremental con `monotonically_increasing_id()` o `row_number()`.

**DIM_UBIGEO (desde ingreso_unified):**
```python
def build_dim_ubigeo(stage_df):
    distinct_ubigeo = (
        stage_df
        .select(
            col("DEPARTAMENTO_EJECUTORA").cast("smallint").alias("CODIGODEPARTAMENTO"),
            col("DEPARTAMENTO_EJECUTORA_NOMBRE").cast("string").alias("DEPARTAMENTO"),
            col("PROVINCIA_EJECUTORA").cast("smallint").alias("CODIGOPROVINCIA"),
            col("PROVINCIA_EJECUTORA_NOMBRE").cast("string").alias("PROVINCIA"),
            col("DISTRITO_EJECUTORA").cast("smallint").alias("CODIGODISTRITO"),
            col("DISTRITO_EJECUTORA_NOMBRE").cast("string").alias("DISTRITO"),
        )
        .dropDuplicates()
        .filter(
            col("CODIGODEPARTAMENTO").isNotNull() &
            col("CODIGOPROVINCIA").isNotNull() &
            col("CODIGODISTRITO").isNotNull()
        )
    )
    return distinct_ubigeo.withColumn("IdUbigeo", monotonically_increasing_id())
```

**DIM_UBIGEO también se alimenta desde RENAMU y SISMEPRE:**
Se unifican los ubigeos de todas las fuentes (ingreso_unified, rentas_esat_estadistica_atm, y cada renamu_XXXX) en un solo set antes de insertar.

**DIM_TIEMPO:**
- IdTiempo = `AÑO * 100 + MES` para datos mensuales (SIAF).
- IdTiempo = `AÑO` para datos anuales (RENAMU, SISMEPRE).
- DIA, HORA, MINUTO, SEGUNDO = NULL cuando no aplican.
- Sin registro artificial con IdTiempo = 0.

**DIM_ANIO_APLICACION:**
Se alimenta desde `rentas_ano_aplicacion`. Los campos de fechas (FECHA_CIERRE, etc.) que no tienen un correlato directo en DIM_TIEMPO se manejan como NULL en lugar de apuntar a un IdTiempo = 0 inexistente.

**Fase 4.2b — Carga de Hechos**

Para cada hecho:

1. `SELECT` las columnas necesarias desde la tabla stage correspondiente.
2. `INNER JOIN` a cada dimensión usando las claves naturales.
3. Se usa `CAST` con manejo de nulos en montos/respuestas (`when(col.isNull(), 0).otherwise(col(...).cast(...))`).
4. **Deduplicación de dimensiones**: Antes de unir a los hechos, cada dimensión se procesa con una subquery que aplica `row_number() OVER (PARTITION BY clave_natural ORDER BY Id)` y filtra `rn = 1`. Esto asegura que aunque una clave natural tenga múltiples Ids en la dimensión, solo se use uno. Equivalente a las CTEs del SQL:
   ```sql
   dim_ejecutora AS (
       SELECT IdEjecutora, SEC_EJEC
       FROM (
           SELECT *, ROW_NUMBER() OVER (PARTITION BY SEC_EJEC ORDER BY IdEjecutora) AS rn
           FROM silver.DIM_EJECUTORA
       ) x WHERE rn = 1
   )
   ```
   En Spark:
   ```python
   from pyspark.sql import Window
   dim_ejecutora_dedup = (
       dim_ejecutora
       .withColumn("rn", row_number().over(Window.partitionBy("SEC_EJEC").orderBy("IdEjecutora")))
       .filter(col("rn") == 1)
       .drop("rn")
   )
   ```

**FACT_INGRESO:**
- Se resuelve desde `ingreso_unified` uniendo a: DIM_TIEMPO (IdTiempo = ANO_DOC*100 + MES_DOC), DIM_NIVEL_GOBIERNO, DIM_SECTOR, DIM_PLIEGO, DIM_EJECUTORA, DIM_UBIGEO, DIM_RUBRO, DIM_TIPO_RECURSO, DIM_GENERICA, DIM_ESPECIFICA.
- MONTO_PIA, MONTO_PIM, MONTO_RECAUDADO: `CAST` a BigInt / Numeric(18,2) con COALESCE a 0.

**FACT_FORMULARIO_SISMEPRE:**
- Se resuelve desde `rentas_respuestas` uniendo a: DIM_EJECUTORA, DIM_ANIO_APLICACION, DIM_FORMULARIO_SISMEPRE, DIM_PREGUNTA_SISMEPRE.
- Las respuestas se dividen en 5 columnas tipadas: RESPUESTA_ID (smallint), RESPUESTA_TEXTO (varchar), RESPUESTA_DECIMAL (numeric), RESPUESTA_ENTERO (int), RESPUESTA_FECHA (varchar). Cada columna se llena según el tipo del valor original y las demás quedan NULL.

### 4.3. Transformación Clave: RENAMU — Unpivot de Preguntas por Año

**Problema:** Los archivos RENAMU tienen una columna por cada pregunta del cuestionario (formato ancho).

| año | ccdd | ccpp | ccdi | ubigeo | tipomuni | p1_1 | p1_2 | p2_1 | ... |
|-----|------|------|------|--------|----------|------|------|------|-----|
| 2021| 15   | 01   | 01   | 150101 | DM       | Sí   | No   | 10   | ... |

**Enfoque:** Descarga y procesamiento por año (no por módulo). Cada archivo anual contiene todas las preguntas de todos los módulos en un formato ancho.

**Solución:**

1. **Identificar columnas-pregunta:** Se lee el esquema del archivo y se excluyen las columnas fijas: `año`, `idmunici`, `ccdd`, `ccpp`, `ccdi`, `ubigeo`, `departamento`, `provincia`, `distrito`, `tipomuni`. Todo lo demás es una pregunta.

2. **Unpivot con `melt()` o `stack()`:** Por cada fila original, se generan N filas (una por columna-pregunta):
   ```python
   from pyspark.sql.functions import expr

   # Construir expresiones stack: "NOMBRE_CAMPO1, VALOR1, NOMBRE_CAMPO2, VALOR2, ..."
   stack_expr = "stack({n}, {pairs}) as (NOMBRE_CAMPO, VALOR)".format(
       n=len(question_cols),
       pairs=", ".join(f"'{c}', `{c}`" for c in question_cols)
   )

   unpivoted = df.select(
       "año", "ccdd", "ccpp", "ccdi", "tipomuni",
       expr(stack_expr)
   )
   ```

3. **Construir DIM_PREGUNTA_RENAMU:**
   ```python
   dim_pregunta = (
       unpivoted
       .select("NOMBRE_CAMPO", "VALOR")
       .distinct()
       .filter(col("NOMBRE_CAMPO").isNotNull())
       .withColumn("DESCRIPCION", col("NOMBRE_CAMPO"))
       .withColumn("METADATA", lit(""))
       .withColumn("IdPadre", lit(None).cast("int"))
       .withColumn("IdPregunta", monotonically_increasing_id())
   )
   ```

4. **Construir FACT_RENAMU:**
   ```python
   fact_renamu = (
       unpivoted
       .join(dim_ubigeo_dedup,
             on=[col("ccdd").cast("smallint") == col("CODIGODEPARTAMENTO"),
                 col("ccpp").cast("smallint") == col("CODIGOPROVINCIA"),
                 col("ccdi").cast("smallint") == col("CODIGODISTRITO")])
       .join(dim_pregunta_dedup, on=["NOMBRE_CAMPO", "VALOR"])
       .select(
           col("año").cast("int").alias("IdTiempo"),
           col("IdUbigeo"),
           col("tipomuni").cast("string").alias("TIPOMUNI"),
           col("IdPregunta"),
       )
   )
   ```

5. **Iteración por año:** Se itera sobre `renamu_2021`, `renamu_2022`, `renamu_2023`, `renamu_2024`. Las preguntas de cada año pueden diferir; `DIM_PREGUNTA_RENAMU` acumula todas las combinaciones (NOMBRE_CAMPO, VALOR) de todos los años.

**Consideración de volumen:** Dado que cada año puede tener cientos de columnas-pregunta y miles de municipalidades, el unpivot puede generar millones de filas. Se debe particionar adecuadamente (ej. repartición en 8-16 particiones antes del unpivot) y usar `coalesce(1)` solo al exportar a CSV.

## 5. Integración con SQL Server (`loader.py`)

### 5.1. Creación de Esquema y Tablas

Se utiliza la sesión Spark configurada con el driver JDBC (`com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11`). La conexión se realiza con:

```python
url = f"jdbc:sqlserver://localhost:1434;databaseName=master;user={user};password={password}"
```

Donde `user` y `password` se leen desde `os.environ` (el `.env` provee `MSSQL_SA_PASSWORD`).

**Creación de esquema:**
```python
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
```

**Creación de tablas:** Se ejecuta DDL equivalente a `sql/tables_silver.sql`, construido desde el modelo en `models.py`. El orden de creación respeta las dependencias de FK:

1. `DIM_TIEMPO`
2. `DIM_UBIGEO`, `DIM_EJECUTORA`, `DIM_SECTOR`, `DIM_NIVEL_GOBIERNO`, `DIM_PLIEGO`
3. `DIM_RUBRO`, `DIM_TIPO_RECURSO`, `DIM_GENERICA`, `DIM_ESPECIFICA`, `DIM_FUENTE_FINANCIAMIENTO`
4. `DIM_ANIO_APLICACION` (FK a DIM_TIEMPO)
5. `DIM_FORMULARIO_SISMEPRE`, `DIM_PREGUNTA_SISMEPRE` (FK a formulario)
6. `DIM_PREGUNTA_RENAMU` (FK a sí misma — IdPadre)
7. `FACT_INGRESO`, `FACT_FORMULARIO_SISMEPRE`, `FACT_RENAMU`

### 5.2. Exportación a CSV

Cada DataFrame de dimensión/hecho se guarda como CSV en `data/silver/csv/{TablaNombre}/`:

```python
df.coalesce(1).write \
    .mode("overwrite") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("header", "false") \
    .option("nullValue", "") \
    .csv(f"data/silver/csv/DIM_EJECUTORA/")
```

El uso de `coalesce(1)` produce un solo archivo CSV por tabla, necesario para BULK INSERT que lee un solo archivo. Para tablas muy grandes (ej. FACT_RENAMU), `coalesce(1)` puede ser un cuello de botella: se evaluará si es necesario dividir en múltiples archivos y ejecutar BULK INSERT múltiples veces.

### 5.3. BULK INSERT

Se ejecuta una sentencia SQL por tabla a través de la conexión JDBC. El contenedor SQL Server tiene montado `./data/silver/csv:/data/silver/csv` como volumen.

```sql
BULK INSERT silver.DIM_EJECUTORA
FROM '/data/silver/csv/DIM_EJECUTORA/part-00000-*.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1,
    TABLOCK,
    BATCHSIZE = 100000
)
```

**Orden de BULK INSERT (mismo orden que CREATE TABLE, para respetar FKs):**
1. DIM_TIEMPO
2. DIM_UBIGEO, DIM_EJECUTORA, DIM_SECTOR, DIM_NIVEL_GOBIERNO, DIM_PLIEGO
3. DIM_RUBRO, DIM_TIPO_RECURSO, DIM_GENERICA, DIM_ESPECIFICA, DIM_FUENTE_FINANCIAMIENTO
4. DIM_ANIO_APLICACION
5. DIM_FORMULARIO_SISMEPRE, DIM_PREGUNTA_SISMEPRE
6. DIM_PREGUNTA_RENAMU
7. FACT_INGRESO, FACT_FORMULARIO_SISMEPRE, FACT_RENAMU

## 6. Configuración

### 6.1. Extensiones a `config.yaml`

```yaml
silver:
  path: "data/silver"
  profiling_path: "data/profiling"       # ruta donde el profiler guarda sus reportes
  database:
    host: "localhost"
    port: 1434
    database: "master"
    schema: "silver"
    user: "sa"

logs:
  path: "logs"
api:
  path: "data/bronze"
  timeout: 30
# ... datasets se mantienen igual
```

La contraseña (`MSSQL_SA_PASSWORD`) se lee desde `os.environ` (cargado por `python-dotenv` desde `.env`).

### 6.2. Modificaciones a `docker-compose.yml`

```yaml
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    ports:
      - "1434:1433"
    environment:
      - MSSQL_SA_PASSWORD=${MSSQL_SA_PASSWORD}
      - ACCEPT_EULA=Y
    volumes:
      - ./data/silver/csv:/data/silver/csv
```

## 7. Dependencias

Sin nuevas dependencias externas. Todo se resuelve con PySpark (ya incluido). Si no existe, se agrega `python-dotenv` a `pyproject.toml` para cargar `.env`.
