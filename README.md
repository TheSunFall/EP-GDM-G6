# 📊 Data Lakehouse — Pipeline Medallion para Datos Fiscales del Perú

Pipeline ETL que descarga, valida, transforma y consolida datos gubernamentales del Perú
(MEF, INEI) en un Data Lakehouse con arquitectura **Medallion** (Bronze → Profile → Silver → Gold).

> **100% Parquet con PySpark.** El proyecto fue migrado de SQL Server a parquet puro: ya **no
> requiere SQL Server ni Docker**. Todo el almacenamiento es parquet local bajo `data/`, y todo
> el pipeline es explorable y ejecutable desde **Jupyter** (carpeta `notebooks/`).

---

## 📋 Descripción general

| Aspecto | Detalle |
|--------|---------|
| **Datos** | SIAF (ingresos/ejecución), SISMEPRE (impuesto predial), RENAMU (registro municipal) |
| **Fuentes** | MEF (Portal de Transparencia Económica) e INEI |
| **Arquitectura** | Medallion: Bronze → Profile → Silver → Gold |
| **Storage** | Parquet (Zstandard en bronze, Snappy en silver/gold) bajo `data/` |
| **Motor** | PySpark 4.1.1 (local, sin base de datos) |
| **Calidad** | 8 criterios (exactitud, completitud, consistencia, integridad, razonabilidad, oportunidad, unicidad, validez) |
| **Modelo** | Esquema estrella: 15 dimensiones + 3 hechos (silver); 3 dimensiones + 5 marts (gold) |

---

## 🔄 Flujo del pipeline

```
APIs externas (MEF, INEI)
        ↓
BRONZE    → descarga + conversión a parquet (zstd)            → data/bronze/
        ↓
PROFILE   → 8 criterios de calidad sobre bronze              → data/profiling/ (HTML + JSON)
        ↓
SILVER    → calidad + filtro + esquema estrella (15 dims/3 hechos) → data/silver/ (parquet)
        ↓
GOLD      → marts BI pre-agregados desde silver (3 dims/5 marts)   → data/gold/ (parquet)
```

### Resumen de etapas

| Etapa | Comando | Entrada | Salida |
|-------|---------|---------|--------|
| **Bronze** | `python main.py bronze` | APIs MEF/INEI | `data/bronze/*.parquet` + manifest |
| **Profile** | `python main.py profile` | parquet bronze | `data/profiling/*.html|json` |
| **Silver** | `python main.py silver` | parquet bronze | `data/silver/*.parquet` |
| **Gold** | `python main.py gold` | parquet silver | `data/gold/*.parquet` |
| **Todo** | `python main.py` | APIs externas | bronze → profile → silver → gold |

---

## 📦 Requisitos

| Requisito | Versión | Propósito |
|-----------|---------|-----------|
| **Python** | 3.12+ | Lenguaje principal |
| **Java** | **17 o 21** | Motor PySpark (versiones mayores rompen PySpark) |
| **uv** | última | Gestor de dependencias |

> **No se necesita Docker ni SQL Server.** Recomendado: 16 GB de RAM (el driver de Spark usa 6 GB).

```bash
python --version   # 3.12+
java -version      # 17 o 21
echo $JAVA_HOME    # debe estar configurado
uv --version
```

---

## 🚀 Instalación

```bash
git clone <repo> && cd EP-GDM-G6
uv sync                              # instala dependencias en .venv
```

- **`.env`** está versionado a propósito (no lo borres ni reescribas). Contiene `MSSQL_SA_PASSWORD`
  (legado, no se usa para almacenar) y rutas `JAVA_HOME`/`HADOOP_HOME` para entornos Linux.
- **Java**: asegúrate de tener `JAVA_HOME` apuntando a un JDK 17 o 21 de Windows.

---

## 🎯 Uso

### Por línea de comandos

```bash
python main.py                   # todo: bronze → profile → silver → gold
python main.py bronze            # descarga + parquet
python main.py profile           # reporte de calidad (abrir data/profiling/profiling_summary.html)
python main.py silver            # calidad + esquema estrella → data/silver/
python main.py silver --drop     # recrea data/silver/ desde cero
python main.py silver quality    # un solo paso: quality | schema | load
python main.py gold --drop       # marts BI → data/gold/
```

Todas las escrituras usan `mode("overwrite")`, así que las re-ejecuciones son idempotentes.

### Por Jupyter (carpeta `notebooks/`)

8 notebooks que ejecutan el pipeline de punta a punta:

```
01_run_bronze   02_run_profiling   03_explore_bronze   04_run_silver
05_explore_silver   06_run_gold   07_explore_gold   08_consultas_finales
```

> ⚠️ **Windows + VSCode:** cada notebook arranca con una **celda de bootstrap** que corrige el
> entorno (ver Troubleshooting). Ejecuta siempre las celdas **de arriba hacia abajo**.

---

## 🗂️ Estructura del proyecto

```
EP-GDM-G6/
├── main.py                     # punto de entrada (argparse: bronze/profile/silver/gold)
├── config.yaml                 # datasets + rutas de salida (sin sección de BD)
├── pyproject.toml / uv.lock    # dependencias (uv)
├── .env                        # versionado a propósito (no tocar)
│   (Anexo IV *.pdf opcional: su contenido ya está en data/categorias_ubigeo.csv)
│
├── app/
│   ├── client/                 # clientes HTTP (MEF datastore, RENAMU/INEI) — HTTP/2
│   ├── pipeline/               # bronze.py, profile.py, silver.py, gold.py
│   ├── silver/                 # quality.py, transforms.py (15 dims + 3 hechos), parquet_loader.py
│   ├── gold/                   # transforms.py (3 dims + 5 marts), parquet_loader.py
│   ├── settings/               # settings.py (singleton, carga config.yaml)
│   ├── schemas/                # esquemas Pydantic
│   └── utils/                  # spark.py (SparkClient), profiler.py (8 criterios), logging.py
│
├── tools/
│   └── generar_categorias.py   # OFFLINE: cruza el Anexo IV por ubigeo → categorias_secejec.csv
│
├── notebooks/                  # 8 notebooks (01–08), pipeline completo en Jupyter
│
├── data/                       # (gitignored salvo CSVs de categorías)
│   ├── bronze/                 # parquet zstd + manifest.parquet
│   ├── silver/ (+ stage/)      # 15 dims + 3 hechos (parquet)
│   ├── gold/                   # 3 dims + 5 marts (parquet)
│   ├── profiling/              # reportes HTML + JSON
│   ├── categorias_ubigeo.csv   # catálogo oficial (Anexo IV) ubigeo → categoría
│   └── categorias_secejec.csv  # mapeo SecEjec → categoría (lo lee quality.py)
│
└── logs/                       # logs de ejecución (gitignored)
```

> **Obsoletos** (de la era SQL Server, ya no se usan): la carpeta `sql/` (`tables_*.sql`,
> `proc_slver.sql`) y la carpeta `spark-warehouse/` (warehouse de Spark que queda vacío porque no
> se usan tablas administradas). Se pueden borrar.

---

## 📈 Datasets

| Dataset | Descripción | Fuente |
|---------|-------------|--------|
| **SIAF** | Presupuesto y ejecución de gasto/ingreso (PIA, PIM, Recaudado), 2021–2025, mensual | MEF |
| **SISMEPRE** | Seguimiento de la Meta de Impuesto Predial (varios módulos), anual | MEF |
| **RENAMU** | Registro Nacional de Municipalidades (indicadores estadísticos), 2021–2024, anual | INEI |

- **Grano de tiempo mixto:** SIAF es mensual (`AnioMes` → `DIM_CALENDARIO`); SISMEPRE y RENAMU son
  anuales (`Anio` → `DIM_ANIO`). No se fuerza una sola tabla de fechas.

---

## 🏷️ Categorías municipales (A–G) — por ubigeo, oficial

`DIM_EJECUTORA.CATEGORIA` y las columnas `Categoria` de gold (`MART_INGRESOS_EJECUTORA`,
`MART_PREDIAL`) clasifican las municipalidades A–G desde la **fuente oficial**:

- **Fuente:** **Anexo IV del DS 003-2026-EF** (incluye **ubigeo**). Su contenido ya está
  versionado en `data/categorias_ubigeo.csv`, así que el **PDF es opcional**.
- **Regenerar:** `python tools/generar_categorias.py` (offline, PyArrow + pypdf, sin Spark). Si el
  PDF `*anexo-iv*.pdf` está en la raíz, lo parsea y refresca `data/categorias_ubigeo.csv`; si no,
  usa ese CSV ya guardado. Luego cruza cada ejecutora municipal del SIAF **por ubigeo**
  (departamento+provincia+distrito) → `data/categorias_secejec.csv`. Match **exacto, 100% de
  cobertura**, con un override manual (`160405` Santa Rosa de Loreto = `G`).
- En tiempo de ejecución, `quality.py` solo **lee** `categorias_secejec.csv` (determinístico).
- **El mismo nombre puede tener distinta categoría** (las diez "Santa Rosa": Lima=C, otras E/F/G).
  Es correcto: son **municipios distintos** (ubigeo / `SEC_EJEC` únicos). No se descarta ninguno.

---

## 🎓 Criterios de calidad (profiling)

El profiler (`app/utils/profiler.py`) puntúa cada parquet de bronze contra 8 criterios:

1. **Exactitud** — sin outliers extremos (±3σ en columnas numéricas).
2. **Completitud** — ≤30% de nulidad por columna.
3. **Consistencia** — años de 4 dígitos, pares ccdd/ccpp/ccdi válidos.
4. **Integridad** — columnas identificadoras sin nulos.
5. **Razonabilidad** — montos ≥ 0, códigos no vacíos.
6. **Oportunidad** — años en rango válido.
7. **Unicidad** — sin filas completamente duplicadas.
8. **Validez** — formatos correctos (numérico, ubigeo, años).

Salida: `data/profiling/profiling_summary.html` (visual) + JSON por archivo.

---

## ❓ Troubleshooting

### El kernel de Jupyter se cae al crear la `SparkSession` (Windows)

VSCode auto-carga el `.env` del repo en el kernel, y ese `.env` trae `JAVA_HOME`/`HADOOP_HOME`
**Linux** que no existen en Windows → la JVM no arranca y el kernel muere. **Mitigaciones ya
incluidas:**
- Cada notebook tiene una **celda de bootstrap** (primera celda) que restaura rutas válidas de
  Windows y fija el cwd en la raíz del repo. Ejecútala primero.
- `.vscode/settings.json` apunta `python.envFile` a un archivo vacío y fija
  `jupyter.notebookFileRoot` a la raíz (red de seguridad).

Tras instalar paquetes nuevos con `uv add`, **reinicia el kernel** para que los tome.

### `JAVA_HOME not found` / PySpark no arranca

Configura `JAVA_HOME` a un JDK **17 o 21** (Java 22+ no es compatible). Verifica con `java -version`.

### No usar UDFs de Python en Spark

En Windows los workers de Python se caen (`Python worker exited unexpectedly` / `EOFException`).
Todas las transformaciones usan funciones nativas (`pyspark.sql.functions`).

---

## 📝 Convenciones

- **Código en inglés; documentación humana (pydoc, README, comentarios) en español** (ver `AGENTS.md`).
- Sin tests, linting ni CI configurados.
- Para guía orientada a agentes de IA, ver `AGENTS.md` y `CLAUDE.md`.
