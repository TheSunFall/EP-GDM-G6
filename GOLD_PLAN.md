# Plan de Implementación: Capa Gold + Dashboards

> Documento de diseño para construir la capa **Gold** del lakehouse medallón
> (sobre la capa Silver ya existente) y, a partir de ella, los **dashboards** de BI.
> Redactado en español (convención del proyecto: código en inglés, documentación en español).

> **⚠️ NOTA DE IMPLEMENTACIÓN (estado final):** este documento es el *diseño*. La
> implementación construida finalmente usa la **Opción B (PySpark)**, no la Opción A
> (T-SQL) que el plan recomendaba — para mantener toda la arquitectura medallón en
> PySpark. En la práctica: el DDL se crea con pymssql (`sql/tables_gold.sql`) y la carga
> se hace con Spark (`app/gold/transforms.py` lee silver por JDBC, agrega/denormaliza y
> escribe en `gold` con `spark.write.jdbc`). El archivo `sql/proc_gold.sql` mencionado en
> el plan **no existe** (se reemplazó por las transformaciones PySpark). Además, los marts
> de predial y RENAMU se entregan en formato **"tall"** (con `ValorNumerico`/`EsAfirmativo`
> ya casteados), no pivoteados.

---

## Tabla de Contenidos

1. [Análisis del estado actual (Bronze → Silver)](#1-análisis-del-estado-actual-bronze--silver)
2. [Objetivo de la capa Gold](#2-objetivo-de-la-capa-gold)
3. [Conflictos detectados en Silver y cómo Gold los resuelve](#3-conflictos-detectados-en-silver-y-cómo-gold-los-resuelve)
4. [Decisiones de arquitectura](#4-decisiones-de-arquitectura)
5. [Diseño de los Data Marts Gold](#5-diseño-de-los-data-marts-gold)
6. [Estructura del proyecto resultante](#6-estructura-del-proyecto-resultante)
7. [Implementación paso a paso](#7-implementación-paso-a-paso)
8. [Capa de presentación: Dashboards](#8-capa-de-presentación-dashboards)
9. [Orden de ejecución y criterios de éxito](#9-orden-de-ejecución-y-criterios-de-éxito)
10. [Checklist de entregables](#10-checklist-de-entregables)

---

## 1. Análisis del estado actual (Bronze → Silver)

### 1.1. Lo que ya está construido

| Capa | Estado | Salida |
|------|--------|--------|
| **Bronze** | ✅ Completa | 24 parquet en `data/bronze/` (zstd) |
| **Profiling** | ✅ Completa | Reportes JSON + HTML en `data/profiling/` (94% calidad) |
| **Silver** | ✅ Completa | Esquema estrella en SQL Server (esquema `silver`) + stage en `data/silver/stage/` |
| **Gold** | ⛔ Stub vacío (`app/pipeline/gold.py`) | — |

El pipeline Silver (`python main.py silver`) ejecuta: `quality` (bronze parquet → `data/silver/stage/`) → `schema` (`build_dims`) → `load` (DDL vía pymssql + escritura Spark JDBC, con lectura de vuelta de IDs IDENTITY para resolver FKs). **La carga real la hace el loader de Python, no los procedimientos de `sql/proc_slver.sql`.**

### 1.2. Modelo estrella Silver (fuente de la capa Gold)

**14 dimensiones** y **3 hechos**, todos en el esquema `silver`:

#### Hecho 1 — `silver.FACT_INGRESO` (SIAF, el más rico para análisis)

| Atributo | Tipo | Notas |
|----------|------|-------|
| 10 claves foráneas | int | Tiempo, NivelGobierno, Sector, Pliego, Ejecutora, Ubigeo, Rubro, TipoRecurso, Generica, Especifica |
| `MONTO_PIA` | bigint | Presupuesto Institucional de Apertura |
| `MONTO_PIM` | bigint | Presupuesto Institucional Modificado |
| `MONTO_RECAUDADO` | numeric(18,2) | Ingreso efectivamente recaudado |

- **Grano:** transaccional (una fila por combinación de las 10 dimensiones). **No está pre-agregado.**
- **Tiempo:** `IdTiempo = AÑO*100 + MES` (mensual).
- ⚠️ Pese a que el `full_name` del dataset menciona "Gasto", las medidas reales son de **ingresos/recaudación** (PIA, PIM, Recaudado). Los dashboards serán sobre **ingresos**, no ejecución de gasto.

#### Hecho 2 — `silver.FACT_FORMULARIO_SISMEPRE` (impuesto predial)

| Atributo | Tipo | Notas |
|----------|------|-------|
| `IdEjecutora`, `IdAnioAplicacion`, `IdFormulario`, `IdPregunta` | int | FKs |
| `PERIODO` | smallint | |
| `RESPUESTA_ID`, `RESPUESTA_TEXTO`, `RESPUESTA_DECIMAL`, `RESPUESTA_ENTERO`, `RESPUESTA_FECHA` | varios | **Respuesta en formato EAV** |

- **Grano:** una fila por `(ejecutora, año_aplicación, periodo, formulario, pregunta)`.
- ⚠️ Las métricas (recaudación predial, nº de contribuyentes, etc.) están en formato **clave-valor**: hay que **pivotear** las preguntas relevantes a columnas para obtener un mart usable.
- **Tiempo:** anual vía `DIM_ANIO_APLICACION → DIM_TIEMPO`.

#### Hecho 3 — `silver.FACT_RENAMU` (municipalidades)

| Atributo | Tipo | Notas |
|----------|------|-------|
| `IdTiempo`, `IdUbigeo`, `IdPregunta` | int | FKs |
| `TIPOMUNI` | varchar(100) | Tipo de municipalidad |

- **Grano:** una fila por `(año, ubigeo, pregunta)`.
- ⚠️ **Tabla factless (sin medida).** El valor del indicador vive como **string** en `silver.DIM_PREGUNTA_RENAMU.VALOR`. Para análisis numérico hay que **castear/pivotear** `VALOR`.
- **Tiempo:** anual (`IdTiempo = AÑO`).

#### Dimensiones compartidas y específicas

- **Compartidas:** `DIM_TIEMPO`, `DIM_UBIGEO`, `DIM_EJECUTORA`.
- **SIAF:** `DIM_NIVEL_GOBIERNO`, `DIM_SECTOR`, `DIM_PLIEGO`, `DIM_RUBRO`, `DIM_TIPO_RECURSO`, `DIM_FUENTE_FINANCIAMIENTO`, `DIM_GENERICA`, `DIM_ESPECIFICA`.
- **SISMEPRE:** `DIM_ANIO_APLICACION`, `DIM_FORMULARIO_SISMEPRE`, `DIM_PREGUNTA_SISMEPRE`.
- **RENAMU:** `DIM_PREGUNTA_RENAMU` (auto-referenciada por `IdPadre`).

---

## 2. Objetivo de la capa Gold

La capa Gold convierte el modelo estrella normalizado (optimizado para integridad) en **tablas de negocio denormalizadas, pre-agregadas y listas para consumo de BI**, de modo que los dashboards:

- No tengan que recorrer 10 joins por consulta.
- Tengan KPIs ya calculados (% ejecución, brecha, variaciones, rankings).
- Trabajen sobre una **dimensión de calendario limpia** y nombres legibles (no códigos).
- Carguen rápido en modo *Import* de la herramienta de BI.

**Principio de diseño:** Gold = *marts* orientados a consumo. Cada mart responde a un grupo de preguntas de negocio y alimenta una página de dashboard.

---

## 3. Conflictos detectados en Silver y cómo Gold los resuelve

> Esta es la sección crítica. Estos conflictos **deben** resolverse en Gold o los dashboards saldrán mal.

| # | Conflicto en Silver | Impacto | Resolución en Gold |
|---|---------------------|---------|--------------------|
| **C1** | `DIM_TIEMPO` mezcla grano **mensual** (`202401`, SIAF) y **anual** (`2024`, RENAMU/SISMEPRE) en la misma tabla y PK. | Un join cruzado entre hechos por `IdTiempo` da resultados incorrectos (un `2024` anual no cruza con `202401`). | Crear **`gold.DIM_CALENDARIO`** contigua a nivel mes, con claves explícitas `AnioMes` (int) y `Anio` (int). Cada mart expone **`Anio`** (y `Mes` solo donde aplica). Las relaciones en BI se hacen por `Anio`/`AnioMes`, nunca por el `IdTiempo` crudo. |
| **C2** | `FACT_RENAMU` es **factless** y el valor del indicador es **string** en la dimensión. | No se puede `SUM`/`AVG`; un mapa de "nº de municipalidades con X servicio" requiere lógica. | Gold **pivotea** los indicadores RENAMU seleccionados a columnas y **castea** `VALOR` a numérico (`TRY_CONVERT`) donde corresponda; los categóricos (Sí/No) se cuentan. Ver `gold.MART_RENAMU`. |
| **C3** | Medidas predial en formato **EAV** (una fila por pregunta). | Un dashboard predial necesita columnas (recaudación, contribuyentes…), no filas. | Gold **pivotea** las preguntas clave de `DIM_PREGUNTA_SISMEPRE` (identificadas por `DESCRIPCION`) a columnas en `gold.MART_PREDIAL`. Requiere un **mapeo de preguntas → métricas** (paso 7.3). |
| **C4** | `proc_slver.sql` asume un esquema **`bronze.*`** (`bronze.ingreso_raw_unified`, etc.) que el pipeline Python **no crea**. | Si Gold se apoyara en esos procs, fallaría: las tablas no existen. | Gold se construye **exclusivamente sobre `silver.*`** (las tablas reales que produce el loader Python). `proc_slver.sql` se ignora como ruta de carga. |
| **C5** | `MONTO_PIA`/`MONTO_PIM` son `bigint` (sin decimales); solo `MONTO_RECAUDADO` es `numeric(18,2)`. | Pérdida de céntimos en PIA/PIM (aceptable en soles, pero a documentar). | En Gold las medidas se mantienen pero los KPIs derivados (%) se calculan en `numeric/float` para no truncar. |
| **C6** | Dimensiones pueden tener **claves naturales duplicadas** (varios `IdEjecutora` por `SEC_EJEC`). Los hechos ya resolvieron a un único Id por dedup. | Si Gold re-une la dimensión por clave natural sin dedup, hay *fan-out* (filas multiplicadas y medidas infladas). | Gold une **hecho → dimensión por la clave surrogate (`Id*`)**, nunca por la clave natural; o aplica `ROW_NUMBER()=1` al denormalizar. |
| **C7** | No existe calendario contiguo ni jerarquía geográfica explícita. | BI (Power BI) necesita una *date table* marcada y jerarquías Depto→Prov→Distrito. | Gold entrega `gold.DIM_CALENDARIO` (contigua, marcable como tabla de fechas) y `gold.DIM_GEOGRAFIA` (jerarquía limpia desde `DIM_UBIGEO`). |

---

## 4. Decisiones de arquitectura

### 4.1. ¿Cómo poblar Gold? — **T-SQL set-based (recomendado)** vs Spark

Los datos de Silver **ya están en SQL Server**. Hay dos caminos:

| Opción | Cómo | Ventajas | Desventajas | Veredicto |
|--------|------|----------|-------------|-----------|
| **A. Procedimientos T-SQL** (silver → gold dentro de SQL Server) | `sql/tables_gold.sql` (DDL) + `sql/proc_gold.sql` (INSERT…SELECT con agregación). Disparados desde Python con **pymssql** (igual que el loader silver crea el esquema). | No hay round-trip por Spark; agregación *set-based* nativa y rapidísima; reusa el patrón pymssql ya existente; menos dependencias. | La lógica de pivote/cast queda en SQL. | ✅ **Recomendado.** Gold vive en la misma BD; agregar es lo que SQL Server hace mejor. |
| **B. Pipeline Spark** (lee `silver.*` por JDBC, agrega, reescribe `gold.*`) | `app/gold/transforms.py` + `app/gold/loader.py`, espejo de Silver. | Consistente con el estilo del pipeline silver; pivotes complejos más legibles en PySpark. | Round-trip innecesario (extraer de SQL → Spark → volver a SQL); más lento; arranque de JVM. | Alternativa válida si se prefiere homogeneidad de código. |

**Recomendación:** **Opción A (T-SQL + pymssql)** para la carga, manteniendo el orquestador en Python (`GoldPipeline`) para integrarse a `main.py`. Es el camino de menor fricción y mayor rendimiento, y reutiliza `_pymssql_exec` del loader silver.

### 4.2. ¿Tablas materializadas vs vistas?

| | Vistas (`CREATE VIEW`) | Tablas materializadas (`CREATE TABLE` + INSERT) |
|---|---|---|
| Frescura | Siempre al día | Requiere refresh (re-ejecutar proc) |
| Rendimiento dashboard | Recalcula en cada query (lento sobre 2M+ filas) | Pre-calculado, lectura instantánea |
| Modo BI | Obliga DirectQuery o Import lento | **Import** rápido |

**Recomendación:** **Tablas materializadas** para los marts agregados (rendimiento en dashboards) + opcionalmente **vistas** finas (`gold.VW_*`) encima para exponer nombres amigables a BI. Refresh full-reload (TRUNCATE + INSERT) en cada corrida — el volumen lo permite y es idempotente.

### 4.3. Esquema y nomenclatura

- Nuevo esquema **`gold`** en la misma BD (`test`, según `config.yaml`).
- Convención: `gold.FACT_*` (hechos denormalizados), `gold.MART_*` (marts agregados de negocio), `gold.DIM_*` (dimensiones de presentación), `gold.VW_*` (vistas de consumo BI).

---

## 5. Diseño de los Data Marts Gold

> Cada mart está atado a un hecho silver real y a una página de dashboard.

### 5.1. Dimensiones de presentación

#### `gold.DIM_CALENDARIO` *(resuelve C1, C7)*
Calendario **contiguo** generado por rango (no solo años presentes en datos).

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `AnioMes` | int (PK) | `AÑO*100+MES` (ej. 202401) |
| `Anio` | smallint | |
| `Mes` | tinyint | 1–12 |
| `NombreMes` | varchar(20) | "Enero"… |
| `Trimestre` | tinyint | 1–4 |
| `FechaInicioMes` | date | Para marcar como *date table* en Power BI |

Generación: `WITH` recursivo o tabla de números desde `MIN(Anio)` a `MAX(Anio)` × 12 meses. También se materializa una fila por año (`Mes = NULL`/agregado) si se necesita cruzar con datos anuales, o bien los marts anuales se relacionan solo por `Anio`.

#### `gold.DIM_GEOGRAFIA` *(resuelve C7)*
Jerarquía limpia desde `silver.DIM_UBIGEO` (deduplicada por ubigeo).

| Columna | Tipo |
|---------|------|
| `IdUbigeo` (PK) | int |
| `CodigoDepartamento`, `Departamento` | smallint / varchar |
| `CodigoProvincia`, `Provincia` | smallint / varchar |
| `CodigoDistrito`, `Distrito` | smallint / varchar |
| `Ubigeo` | varchar(6) | concatenado, para mapas |

### 5.2. Marts SIAF (ingresos) — desde `FACT_INGRESO`

#### `gold.FACT_INGRESOS` — hecho denormalizado (grano detallado, opcional)
FACT_INGRESO con dimensiones ya unidas por surrogate key y nombres resueltos. Útil si se quiere un modelo estrella en BI sin re-modelar. Columnas: `AnioMes`, `Anio`, `Mes`, `IdUbigeo`, `NivelGobierno`, `Sector`, `Rubro`, `TipoRecurso`, `Generica`, `Especifica`, `MONTO_PIA`, `MONTO_PIM`, `MONTO_RECAUDADO`.

#### `gold.MART_INGRESOS_GEOGRAFICO` — **principal para dashboards**
**Grano:** `(Anio, Mes, IdUbigeo, NivelGobierno)`.

| Columna | Cálculo |
|---------|---------|
| `Anio`, `Mes`, `AnioMes` | de `DIM_TIEMPO` |
| `IdUbigeo`, `Departamento`, `Provincia`, `Distrito` | de `DIM_UBIGEO` |
| `NivelGobierno` | de `DIM_NIVEL_GOBIERNO` |
| `MontoPIA`, `MontoPIM`, `MontoRecaudado` | `SUM(...)` |
| `PctEjecucion` | `MontoRecaudado / NULLIF(MontoPIM,0)` |
| `Brecha` | `MontoPIM - MontoRecaudado` |
| `VarPIA_PIM` | `MontoPIM - MontoPIA` |

#### `gold.MART_INGRESOS_CLASIFICADOR`
**Grano:** `(Anio, Rubro, Generica, TipoRecurso)` → `SUM` de las 3 medidas + KPIs. Para analizar **de dónde viene el ingreso** (tipo de recurso, rubro, genérica).

#### `gold.MART_INGRESOS_EJECUTORA`
**Grano:** `(Anio, IdEjecutora, EjecutoraNombre, Departamento)` → `SUM` medidas + KPIs. Para **ranking de ejecutoras** y tablas top-N.

### 5.3. Mart SISMEPRE (predial) — desde `FACT_FORMULARIO_SISMEPRE` *(resuelve C3)*

#### `gold.MART_PREDIAL`
**Grano:** `(Anio, IdEjecutora, Municipalidad, Departamento)`.

Construcción: pivotear las **preguntas clave** identificadas en `DIM_PREGUNTA_SISMEPRE.DESCRIPCION` (paso 7.3) usando `CASE WHEN IdPregunta = X THEN RESPUESTA_DECIMAL/ENTERO`. Ejemplos de columnas resultantes (a confirmar con el diccionario): `RecaudacionPredialAnual`, `NumeroContribuyentes`, `NumeroPredios`, `MontoEmitido`, `MontoRecaudado`, `PctCumplimientoMeta`.

> ⚠️ **Dependencia:** requiere mapear qué `PREGUNTA_ID`/`DESCRIPCION` corresponde a cada métrica (no es evidente sin el diccionario). Paso 7.3 lo aborda.

### 5.4. Mart RENAMU (municipalidades) — desde `FACT_RENAMU` *(resuelve C2)*

#### `gold.MART_RENAMU`
**Grano:** `(Anio, IdUbigeo, TipoMuni)`.

Construcción: seleccionar **indicadores de interés** (`NOMBRE_CAMPO`) de `DIM_PREGUNTA_RENAMU`, pivotearlos a columnas y **castear `VALOR`**:
- Numéricos → `TRY_CONVERT(numeric, VALOR)` (ej. población, nº de trabajadores).
- Categóricos (Sí/No) → indicador `0/1` para poder sumar/contar cobertura.

Resultado: una fila por municipalidad-año con N indicadores en columnas, lista para mapas de cobertura de servicios municipales.

---

## 6. Estructura del proyecto resultante

```
app/
├── pipeline/
│   └── gold.py              # MODIFICAR — implementar clase GoldPipeline (hoy es stub)
├── gold/                    # NUEVO — módulo de la capa gold
│   ├── __init__.py
│   └── loader.py            # NUEVO — ejecuta DDL + procs gold vía pymssql (reusa patrón de silver/loader.py)
│
sql/
├── tables_gold.sql         # NUEVO — DDL: esquema gold + DIM_CALENDARIO, DIM_GEOGRAFIA, FACT/MART_*
└── proc_gold.sql           # NUEVO — procedimientos que pueblan gold desde silver (INSERT...SELECT con agregación/pivote)

main.py                     # MODIFICAR — añadir subcomando `gold` y meterlo en _run_all()
config.yaml                 # MODIFICAR — añadir bloque `gold:` (esquema, opciones)

docs/
└── dashboards/             # NUEVO (opcional) — .pbix / definiciones de dashboard y capturas
```

**No se requieren dependencias nuevas** (pymssql y pyspark ya están). Si se elige la Opción B (Spark), se añadiría `app/gold/transforms.py`.

---

## 7. Implementación paso a paso

### 7.1. DDL — `sql/tables_gold.sql`
1. `CREATE SCHEMA gold` (patrón idéntico a `tables_silver.sql`, idempotente con `IF NOT EXISTS`).
2. Crear `gold.DIM_CALENDARIO`, `gold.DIM_GEOGRAFIA`.
3. Crear `gold.MART_INGRESOS_GEOGRAFICO`, `gold.MART_INGRESOS_CLASIFICADOR`, `gold.MART_INGRESOS_EJECUTORA`, `gold.MART_PREDIAL`, `gold.MART_RENAMU` (+ `gold.FACT_INGRESOS` si se desea el detalle).
4. (Opcional) Vistas `gold.VW_*` con nombres amigables para BI.

### 7.2. Procedimientos — `sql/proc_gold.sql`
Un procedimiento por mart, más un orquestador `gold.sp_Load_Gold_All`:
- `gold.sp_Load_Dim_Calendario` — genera calendario contiguo desde el rango de `silver.DIM_TIEMPO`.
- `gold.sp_Load_Dim_Geografia` — dedup de `silver.DIM_UBIGEO`.
- `gold.sp_Load_Mart_Ingresos_*` — `TRUNCATE` + `INSERT...SELECT ... GROUP BY` uniendo `FACT_INGRESO` a sus dims **por surrogate key** (evita C6) y calculando KPIs.
- `gold.sp_Load_Mart_Predial` — pivote de preguntas clave (C3).
- `gold.sp_Load_Mart_Renamu` — pivote + cast de `VALOR` (C2).
- `gold.sp_Load_Gold_All` — ejecuta todos en orden (dims → marts).

Patrón de cada mart (idempotente, full reload):
```sql
TRUNCATE TABLE gold.MART_INGRESOS_GEOGRAFICO;
INSERT INTO gold.MART_INGRESOS_GEOGRAFICO (Anio, Mes, AnioMes, IdUbigeo, Departamento, Provincia, Distrito, NivelGobierno, MontoPIA, MontoPIM, MontoRecaudado, PctEjecucion, Brecha)
SELECT  t.ANIO, t.MES, f.IdTiempo,
        u.IdUbigeo, u.DEPARTAMENTO, u.PROVINCIA, u.DISTRITO,
        ng.NIVEL_GOBIERNO_NOMBRE,
        SUM(f.MONTO_PIA), SUM(f.MONTO_PIM), SUM(f.MONTO_RECAUDADO),
        CAST(SUM(f.MONTO_RECAUDADO) AS float) / NULLIF(SUM(f.MONTO_PIM),0),
        SUM(f.MONTO_PIM) - SUM(f.MONTO_RECAUDADO)
FROM    silver.FACT_INGRESO f
JOIN    silver.DIM_TIEMPO   t  ON t.IdTiempo  = f.IdTiempo
JOIN    silver.DIM_UBIGEO   u  ON u.IdUbigeo  = f.IdUbigeo          -- surrogate key (C6)
JOIN    silver.DIM_NIVEL_GOBIERNO ng ON ng.IdNivelGobierno = f.IdNivelGobierno
GROUP BY t.ANIO, t.MES, f.IdTiempo, u.IdUbigeo, u.DEPARTAMENTO, u.PROVINCIA, u.DISTRITO, ng.NIVEL_GOBIERNO_NOMBRE;
```

### 7.3. Mapeo de preguntas clave (predial y RENAMU)
Paso de **descubrimiento** (antes de fijar el pivote): consultar los catálogos y elegir las métricas:
```sql
-- Preguntas predial candidatas (recaudación, contribuyentes, predios, metas)
SELECT IdPreguntaSismepre, PREGUNTA_ID, DESCRIPCION
FROM   silver.DIM_PREGUNTA_SISMEPRE
WHERE  DESCRIPCION LIKE '%recaud%' OR DESCRIPCION LIKE '%predial%'
    OR DESCRIPCION LIKE '%contribuyente%' OR DESCRIPCION LIKE '%predio%'
    OR DESCRIPCION LIKE '%meta%';

-- Indicadores RENAMU candidatos
SELECT DISTINCT NOMBRE_CAMPO, DESCRIPCION
FROM   silver.DIM_PREGUNTA_RENAMU
ORDER BY NOMBRE_CAMPO;
```
Con esa salida se fija el `CASE WHEN` de cada mart. *(Este paso necesita revisión humana / del diccionario; lo dejo explícito como dependencia.)*

### 7.4. Orquestador Python — `app/gold/loader.py` + `app/pipeline/gold.py`
- `app/gold/loader.py`: reusa el patrón `_pymssql_exec` / `create_schema_and_tables` de `app/silver/loader.py`. Funciones:
  - `create_gold_schema(cfg, password, drop)` — divide `sql/tables_gold.sql` por `GO` y ejecuta.
  - `load_gold(cfg, password)` — ejecuta `sql/proc_gold.sql` (crea los procs) y luego `EXEC gold.sp_Load_Gold_All`.
- `app/pipeline/gold.py`:
  ```python
  class GoldPipeline:
      def __init__(self, spark_client): ...
      def run(self, drop: bool = False):
          # 1. create_gold_schema (DDL)
          # 2. load_gold (procs + sp_Load_Gold_All)
  ```
  > Nota: aunque la carga es T-SQL, se mantiene la firma con `SparkClient` por consistencia con `SilverPipeline`; internamente Gold no necesita Spark (Opción A).

### 7.5. CLI — `main.py`
```python
subparsers.add_parser("gold", help="Run the gold pipeline (build marts from silver)")
# en _run_all(): _run_bronze(); _run_profile(); _run_silver(); _run_gold()
def _run_gold(drop=False):
    GoldPipeline(SparkClient()).run(drop=drop)
```
Comandos resultantes: `python main.py gold` y `python main.py gold --drop`.

### 7.6. Configuración — `config.yaml`
```yaml
gold:
  schema: "gold"
  calendar_start_year: 2021   # rango para DIM_CALENDARIO
  calendar_end_year: 2025
```
(La conexión a BD se reutiliza de `silver.database`; la password sigue viniendo de `MSSQL_SA_PASSWORD` en `.env`.)

---

## 8. Capa de presentación: Dashboards

### 8.1. Qué necesitas (herramienta) — recomendación

| Herramienta | Encaja porque | A considerar | Recom. |
|-------------|---------------|--------------|--------|
| **Power BI Desktop** | Estás en **Windows**; gratis para desarrollo; conector nativo SQL Server; ideal para sector público en Perú; modo *Import* rápido sobre marts gold. | Publicar/compartir en la nube requiere licencia Pro; `.pbix` es binario (versionar con cuidado). | ✅ **Primaria** |
| **Metabase** | Open-source; se añade a `docker-compose.yml` en minutos; UI sencilla; bueno para compartir por URL. | Menos potente en modelado/medidas que Power BI. | ✅ Alternativa OSS |
| **Apache Superset** | Open-source, muy potente, vía Docker. | Curva de aprendizaje y setup mayores. | Opcional |

> **Recomendación:** **Power BI Desktop** como primaria (conectas directo a `gold.*`). Si quieres algo 100% open-source y dockerizado junto al stack actual, **Metabase** (servicio extra en `docker-compose.yml`, puerto p.ej. 3000, apuntando a SQL Server `host.docker.internal:11423`).

### 8.2. Cómo conectar y modelar (Power BI)

1. **Get Data → SQL Server** → servidor `localhost,11423`, base `test`, modo **Import**. Seleccionar solo tablas `gold.*` (y `gold.VW_*`).
2. **Marcar `gold.DIM_CALENDARIO` como tabla de fechas** (`FechaInicioMes`) → habilita *time intelligence* (resuelve C1/C7).
3. **Relaciones:**
   - `MART_INGRESOS_GEOGRAFICO[AnioMes]` → `DIM_CALENDARIO[AnioMes]`.
   - `MART_*[IdUbigeo]` → `DIM_GEOGRAFIA[IdUbigeo]`.
   - Marts anuales (predial, RENAMU) → relacionar por **`Anio`** (no por `AnioMes`).
4. **Medidas DAX** (ejemplos):
   ```DAX
   Recaudado Total   = SUM ( MART_INGRESOS_GEOGRAFICO[MontoRecaudado] )
   PIM Total         = SUM ( MART_INGRESOS_GEOGRAFICO[MontoPIM] )
   % Ejecución       = DIVIDE ( [Recaudado Total], [PIM Total] )
   Brecha            = [PIM Total] - [Recaudado Total]
   Var. Recaudación YoY =
       VAR Actual = [Recaudado Total]
       VAR Previo = CALCULATE ( [Recaudado Total], DATEADD ( DIM_CALENDARIO[FechaInicioMes], -1, YEAR ) )
       RETURN DIVIDE ( Actual - Previo, Previo )
   ```

### 8.3. Dashboards a construir (páginas y visuales)

**Dashboard 1 — Ejecución de Ingresos (SIAF)** *(mart: `MART_INGRESOS_GEOGRAFICO` + `_CLASIFICADOR` + `_EJECUTORA`)*
- KPIs (tarjetas): Recaudado Total, PIM Total, % Ejecución, Brecha.
- Evolución temporal (línea): Recaudado vs PIM por mes/año.
- **Mapa de Perú** por departamento: % Ejecución o Recaudado (usa `DIM_GEOGRAFIA`).
- Barras: Recaudación por Rubro / Tipo de Recurso (`MART_INGRESOS_CLASIFICADOR`).
- Tabla top-N: ejecutoras por recaudación y % ejecución (`MART_INGRESOS_EJECUTORA`).
- Segmentadores: Año, Departamento, Nivel de Gobierno.

**Dashboard 2 — Impuesto Predial (SISMEPRE)** *(mart: `MART_PREDIAL`)*
- KPIs: Recaudación predial total, Nº contribuyentes, % cumplimiento de meta.
- Mapa/barras: recaudación predial por departamento/municipalidad.
- Ranking de municipalidades por cumplimiento.
- Tendencia anual.

**Dashboard 3 — Indicadores Municipales (RENAMU)** *(mart: `MART_RENAMU`)*
- Mapa de cobertura de servicios municipales (indicadores Sí/No agregados como %).
- Comparativa por tipo de municipalidad (`TipoMuni`).
- Evolución de indicadores clave por año.

### 8.4. Conflictos esperados en la capa de dashboards

| Conflicto | Síntoma | Mitigación |
|-----------|---------|------------|
| **Grano de tiempo mixto** (C1) | Filtrar por mes "vacía" los visuales de RENAMU/predial (anuales). | Relacionar marts anuales por `Anio`; en cada página usar el segmentador de tiempo adecuado al grano. |
| **Mapas de Perú** | Power BI no trae shapefiles de distritos de Perú por defecto; el mapa por coordenadas puede geocodificar mal nombres. | Usar visual **Shape Map** con TopoJSON de departamentos/provincias de Perú, o el mapa por `Ubigeo`. Preparar el TopoJSON como entregable. |
| **Valores RENAMU string** (C2) | Indicadores no suman / aparecen como texto. | Ya resuelto en `MART_RENAMU` (cast en gold); en BI usar las columnas numéricas/indicadoras, no las crudas. |
| **Cardinalidad de `FACT_INGRESOS` detallado** | Modelo pesado si se importa el detalle transaccional. | Importar solo los **marts agregados**; dejar `gold.FACT_INGRESOS` para *drill-through* puntual o no importarlo. |
| **Nombres con tildes/ñ** | Etiquetas mal renderizadas. | Gold ya viene en UTF-8 desde silver; verificar *collation* de la BD y codificación al conectar. |
| **Refresh** | Datos desactualizados tras re-correr el pipeline. | Tras `python main.py gold`, hacer *Refresh* en Power BI (o programar refresh si se publica). |

---

## 9. Orden de ejecución y criterios de éxito

### Flujo completo
```bash
python main.py                 # bronze → profile → silver → gold (tras integrar gold en _run_all)
# o por etapas:
python main.py silver          # prerequisito: tablas silver pobladas
python main.py gold            # construye el esquema gold y los marts
python main.py gold --drop     # recrea gold desde cero (idempotente)
```

### Criterios de éxito de Gold
- ✅ Esquema `gold` creado con `DIM_CALENDARIO`, `DIM_GEOGRAFIA` y los 5 marts.
- ✅ `SELECT COUNT(*)` > 0 en cada mart.
- ✅ Cuadre de control: `SUM(MontoRecaudado)` en `gold.MART_INGRESOS_GEOGRAFICO` = `SUM(MONTO_RECAUDADO)` en `silver.FACT_INGRESO` (sin pérdida por joins; los INNER JOIN no deben descartar filas — validar con LEFT JOIN de control).
- ✅ `PctEjecucion` en rango razonable (0–~1, con outliers explicables).
- ✅ `gold.MART_RENAMU`/`MART_PREDIAL` con columnas numéricas pobladas (pivote correcto).

### Validación de no pérdida de datos (importante)
Los marts SIAF usan los mismos INNER JOIN que `FACT_INGRESO` ya resolvió, así que no deberían perder filas; aun así, ejecutar un control:
```sql
SELECT (SELECT SUM(MONTO_RECAUDADO) FROM silver.FACT_INGRESO)        AS silver_total,
       (SELECT SUM(MontoRecaudado)  FROM gold.MART_INGRESOS_GEOGRAFICO) AS gold_total;
-- Ambos deben coincidir.
```

---

## 10. Checklist de entregables

- [ ] `sql/tables_gold.sql` — DDL esquema gold (dims + marts).
- [ ] `sql/proc_gold.sql` — procedimientos de carga + `sp_Load_Gold_All`.
- [ ] Mapeo de preguntas clave predial/RENAMU confirmado (paso 7.3).
- [ ] `app/gold/__init__.py` + `app/gold/loader.py`.
- [ ] `app/pipeline/gold.py` — `GoldPipeline` implementado.
- [ ] `main.py` — subcomando `gold` + integración en `_run_all`.
- [ ] `config.yaml` — bloque `gold:`.
- [ ] Consultas de validación/cuadre ejecutadas y OK.
- [ ] (Dashboards) Herramienta elegida (Power BI / Metabase) y conexión a `gold.*`.
- [ ] (Dashboards) `DIM_CALENDARIO` marcada como tabla de fechas; relaciones creadas.
- [ ] (Dashboards) 3 dashboards (Ingresos, Predial, RENAMU) + TopoJSON de Perú para mapas.
- [ ] Actualizar `README.md` / `CLAUDE.md` describiendo la capa gold (doc en español).

---

### Decisiones que necesito que confirmes antes de implementar
1. **Herramienta de dashboard:** ¿**Power BI Desktop** (recomendado, Windows) o **Metabase** dockerizado (open-source)?
2. **Poblado de Gold:** ¿confirmas la **Opción A (T-SQL + pymssql)** o prefieres homogeneidad con un **pipeline Spark (Opción B)**?
3. **Mapeo de métricas predial/RENAMU:** ¿tienes el diccionario a mano para fijar qué preguntas pivotear, o lo derivamos de `DESCRIPCION`/`NOMBRE_CAMPO` con las consultas del paso 7.3?
```
