# Revisión BI y guía de ejecución del pipeline

> Fecha de la revisión: 2026-06-08. Rama auditada: `mods`.

Este documento registra (1) el resultado de la auditoría técnica, (2) las
correcciones aplicadas y (3) cómo ejecutar el pipeline completo Bronze → Silver → Gold.

---

## 1. Qué es realmente este proyecto

Es un **lakehouse Medallion (Bronze → Silver → Gold)** construido con **PySpark 4.1**
sobre **datos abiertos fiscales y municipales** del Perú:

| Dataset  | Fuente | Contenido |
|----------|--------|-----------|
| **SIAF**     | MEF  | Ejecución de **ingresos** presupuestales (PIA, PIM, Recaudado) |
| **SISMEPRE** | MEF  | Seguimiento de la meta de **impuesto predial** (formularios) |
| **RENAMU**   | INEI | **Registro Nacional de Municipalidades** (indicadores municipales) |

> ⚠️ **Importante:** el proyecto **NO** contiene datos de adquisiciones / contrataciones
> públicas (OSCE / SEACE / CONOSCE), ni proveedores, ni contratos, ni órdenes de compra,
> ni medicamentos, ni EsSalud. Cualquier requerimiento de KPIs o dashboards de
> "eficiencia de adquisición de medicamentos en EsSalud" **no es realizable con estos
> datos**: requeriría ingestar otras fuentes (SEACE/CONOSCE). Ver sección 5.

---

## 2. Hallazgo principal y correcciones aplicadas

La capa **Gold estaba ausente en la rama `mods`**: el código fuente había sido eliminado
(solo quedaba el bytecode en `app/gold/__pycache__/`), `app/pipeline/gold.py` estaba
vacío, `sql/tables_gold.sql` no existía, `main.py` no exponía el comando `gold` y
`config.yaml` apuntaba a la base de datos `test` en lugar de `DLKH_MEF`.

La capa Gold completa sí existía en el commit `915b5b9` ("capa gold") y se **recuperó**
desde ahí (es 100 % compatible con el esquema silver actual).

| Archivo | Acción | Motivo |
|---------|--------|--------|
| `sql/tables_gold.sql` | Restaurado | DDL de las 8 tablas gold (faltaba por completo) |
| `app/gold/__init__.py` | Restaurado | Paquete del módulo gold |
| `app/gold/loader.py` | Restaurado | DDL vía pymssql + escritura Spark JDBC |
| `app/gold/transforms.py` | Restaurado | Construcción de dimensiones y marts |
| `app/pipeline/gold.py` | Restaurado | Orquestador `GoldPipeline` (estaba vacío) |
| `main.py` | Modificado | Se reconectó el subcomando `gold` y `_run_gold()` en `_run_all()` |
| `config.yaml` | Modificado | `database: "test"` → `"DLKH_MEF"` (BD real con silver+gold) |
| `GOLD_PLAN.md` | Restaurado | Documento de diseño de la capa gold |
| `DASHBOARDS.md` | Restaurado | Guía de consumo BI |

Verificaciones realizadas tras la restauración:
- `python -m py_compile` de los 4 archivos Python → **OK**.
- `python main.py --help` muestra `{bronze, profile, silver, gold}` → **OK**.
- Imports de la cadena gold (`app.pipeline.gold` → `app.gold.loader/transforms`) → **OK**.
- `tables_gold.sql` sin riesgo de "go-split" en comentarios → **OK**.

> La **ejecución real** del cargue gold (Spark → SQL Server) no se pudo correr en esta
> revisión porque requiere Java 17/21, el contenedor de SQL Server activo y la contraseña
> SA correcta. Es un paso manual (ver sección 4).

---

## 3. Prerrequisitos

1. **Java 17 o 21** con `JAVA_HOME` definido (Java 22+ rompe PySpark).
2. **`.env`** en la raíz con `MSSQL_SA_PASSWORD=<tu_password>`.
3. Dependencias: `uv sync`.
4. SQL Server arriba: `docker compose up -d` (puerto host 11423 → contenedor 1433).
5. La base `DLKH_MEF` debe existir en el servidor. Si pymssql devuelve `Login failed (18456)`,
   verificar el password real del contenedor con
   `docker exec <contenedor> printenv MSSQL_SA_PASSWORD` y/o que la base `DLKH_MEF` exista.

---

## 4. Ejecutar el pipeline completo (en orden)

```bash
# 0. Preparación (una sola vez)
uv sync
docker compose up -d

# 1. Bronze: descarga de APIs MEF/INEI y conversión a parquet -> data/bronze/
python main.py bronze

# 2. Profiling de calidad de bronze -> data/profiling/*.json + .html
python main.py profile

# 3. Silver: calidad -> esquema estrella -> carga a SQL Server (esquema silver)
python main.py silver --drop      # --drop = recarga idempotente

# 4. Gold: marts de negocio sobre silver -> SQL Server (esquema gold)
python main.py gold --drop        # --drop = recrea el esquema gold

# Alternativa: todo de corrido (bronze -> profile -> silver -> gold)
python main.py
```

Notas de secuencia:
- Cada etapa lee la salida en disco/BD de la anterior, así que **el orden importa**.
- `silver` y `gold` son **idempotentes** con `--drop`.
- El mart `gold.MART_RENAMU` (~10M filas) tarda ~20 min en cargar por JDBC; el resto
  termina en segundos.

---

## 5. KPIs y dashboards

### Soportados por el modelo actual (datos fiscales/municipales)
- Ingresos PIA / PIM / Recaudado por **geografía**, **nivel de gobierno**, **clasificador**
  (rubro, tipo de recurso, genérica) y **unidad ejecutora**.
- **% de ejecución** (Recaudado / PIM) y **brecha** (PIM − Recaudado).
- Variación PIA → PIM.
- Indicadores de **impuesto predial** (mart `MART_PREDIAL`, formato "tall").
- Indicadores **RENAMU** y cobertura de servicios municipales (mart `MART_RENAMU`).
- Registros con datos faltantes / inconsistentes (capa de profiling).

### NO soportados con estos datos (requieren OSCE/SEACE/CONOSCE)
Lead time convocatoria→adjudicación, monto adjudicado, % de contratación directa,
adicionales/sobrecostos de contrato, concentración de proveedores, contratos por vencer,
compras fragmentadas, alertas de proveedores de riesgo, gasto por proveedor/modalidad/
ítem de medicamento. **No existe la materia prima (proveedores, contratos, adjudicaciones)
en el proyecto.**
