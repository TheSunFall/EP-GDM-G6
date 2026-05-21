# 📊 Data Lakehouse - Pipeline Medallion para Datos Gubernamentales del Perú

Pipeline ETL que descarga, valida, transforma y carga datos gubernamentales del Perú (MEF, INEI) en un Data Lakehouse con arquitectura Medallion (Bronze → Silver → Gold).

**Estado:** ✅ Completado (24/24 archivos pasan validación con 94%+ calidad)

---

## 📋 Descripción General

Este proyecto implementa un pipeline de datos robusto para consolidar información fiscal y estadística del gobierno peruano:

| Aspecto | Detalle |
|--------|---------|
| **Datos** | SIAF (presupuestos), SISMEPRE (impuestos prediales), RENAMU (municipalidades) |
| **Volumen** | 24 archivos parquet (2M+ filas, 1,300+ columnas) |
| **Arquitectura** | Medallion (Bronze → Silver → Gold) |
| **Storage** | Parquet comprimido (Zstandard) |
| **BBDD** | SQL Server 2022 en Docker |
| **Motor** | PySpark 4.1.1 |
| **Calidad** | 8 criterios validados (exactitud, completitud, consistencia, integridad, razonabilidad, oportunidad, unicidad, validez) |

---

## 🔄 Flujo del Pipeline (Arquitectura Medallion)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        APIs EXTERNAS (MEF, INEI)                    │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  CAPA BRONZE: Ingesta Cruda (24 archivos parquet, 2.1M filas)       │
│  └─ Datos sin procesar, comprimidos con Zstandard                  │
│  └─ Descarga via HTTPClient (HTTP/2 con soporte gzip)              │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  PROFILING: Validación de Calidad (8 Criterios)                    │
│  ├─ ✓ Exactitud: sin outliers estadísticos (±3σ)                  │
│  ├─ ✓ Completitud: < 30% nulidad por columna                      │
│  ├─ ✓ Consistencia: años 4-dígitos, pares ccdd-ccpp válidos       │
│  ├─ ✓ Integridad: columnas ID sin nulls                           │
│  ├─ ✓ Razonabilidad: montos ≥ 0, códigos no vacíos                │
│  ├─ ✓ Oportunidad: años en [2000-2030]                            │
│  ├─ ✓ Unicidad: filas sin duplicados completos                    │
│  └─ ✓ Validez: formatos correctos (numérico, ubigeo, años)        │
│  └─ Salida: Reportes JSON + HTML (data/profiling/)                │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  CAPA SILVER: Transformación y Limpieza                            │
│  ├─ Correcciones de calidad (type cast, null coalesce, dedup)    │
│  ├─ Construcción de esquema estrella:                             │
│  │  ├─ 14 Dimensiones (tiempo, geografía, presupuesto, etc.)    │
│  │  ├─ 3 Tablas de hechos (SIAF, SISMEPRE, RENAMU)              │
│  └─ Carga a SQL Server vía Spark JDBC                            │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  CAPA GOLD: Agregaciones y Vistas (Futuro)                        │
│  └─ Stub vacío para análisis de nivel ejecutivo                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 📌 Resumen de Etapas

| Etapa | Comando | Entrada | Salida | Duración |
|-------|---------|---------|--------|----------|
| **Bronze** | `python main.py bronze` | CSV via APIs | 24 archivos parquet | ~5-10 min |
| **Profiling** | `python main.py profile` | Parquets bronze | Reportes JSON + HTML | ~3-5 min |
| **Silver** | `python main.py silver` | Parquets bronze | SQL Server + Parquets silver | ~10-15 min |
| **Todo** | `python main.py` | APIs externas | SQL Server + Reportes | ~20-30 min |

## Estructura del Proyecto

```
EP-GDM-G6/
├── main.py                    # Punto de entrada del pipeline
├── config.yaml                # Configuración de datasets, APIs y base de datos
├── pyproject.toml             # Dependencias del proyecto (uv)
├── docker-compose.yml         # SQL Server container (puerto 1434 → 1433)
├── AGENTS.md                  # Documentación para agentes de IA
│
├── app/
│   ├── client/                # Clientes HTTP para APIs externas
│   │   ├── base_client.py     # Cliente HTTP base (HTTP/2)
│   │   └── mef_client.py      # Cliente para MEF API y RENAMU (INEI)
│   │
│   ├── pipeline/              # Pipelines principales
│   │   ├── bronze.py          # BronzePipeline: descarga CSVs → parquet
│   │   ├── profile.py         # Profiler: evaluación de calidad
│   │   ├── silver.py          # SilverPipeline: calidad, schema, carga
│   │   └── gold.py            # Stub vacío (futuro)
│   │
│   ├── silver/                # Transformaciones de capa silver
│   │   ├── quality.py         # Correcciones de calidad (type cast, null coalesce, dedup)
│   │   ├── transforms.py      # 14 dimensiones + 3 tablas de hechos
│   │   └── loader.py          # DDL + carga a SQL Server vía Spark JDBC
│   │
│   ├── settings/              # Configuración de la aplicación
│   │   └── settings.py        # Singleton que carga config.yaml
│   │
│   ├── schemas/               # Esquemas de validación (Pydantic)
│   └── utils/                 # Utilidades
│       ├── spark.py           # Configuración de Spark + JDBC driver
│       ├── profiler.py        # Lógica de profiling (8 criterios de calidad)
│       └── logging.py         # Configuración de logging
│
├── sql/
│   ├── tables_silver.sql      # DDL para tablas silver
│   └── proc_slver.sql         # Procedimientos almacenados
│
├── data/                      # Datos (gitignored)
│   ├── bronze/                # Archivos parquet de capa bronze
│   ├── silver/stage/          # Archivos parquet intermedios silver
│   └── profiling/             # Reportes JSON y HTML de profiling
│
└── logs/                      # Logs de ejecución (gitignored)
```

## Datasets

El pipeline descarga y procesa 3 datasets principales configurados en `config.yaml`:

| Dataset | Descripción | Fuente |
|---------|-------------|--------|
| **SIAF** | Presupuesto y Ejecución de Gasto (PIA, PIM, Compromiso, Devengado, Girado) | MEF - Portal de Transparencia Económica |
| **SISMEPRE** | Seguimiento de la Meta de Impuesto Predial (6 módulos con diccionarios individuales) | MEF - Portal de Transparencia Económica |
| **RENAMU** | Registro Nacional de Municipalidades (datos estadísticos municipales) | INEI |

---

## 📦 Requisitos Totales

### Software Requerido

| Requisito | Versión | Propósito | Instalación |
|-----------|---------|----------|-------------|
| **Python** | 3.12+ | Lenguaje principal | [python.org](https://www.python.org) |
| **Java** | 17 o 21 | Motor PySpark | [adoptopenjdk.net](https://adoptopenjdk.net) |
| **uv** | Última | Gestor de dependencias | `pip install uv` o [astral.sh/uv](https://astral.sh/uv) |
| **Docker** | 20.10+ | Contenedores | [docker.com](https://www.docker.com) |
| **Docker Compose** | 2.0+ | Orquestación de contenedores | Incluido en Docker Desktop |
| **Git** | Cualquiera | Control de versiones | [git-scm.com](https://git-scm.com) |

### Recursos Computacionales

| Recurso | Mínimo | Recomendado |
|---------|--------|------------|
| **RAM** | 8 GB | 16 GB |
| **CPU** | 4 cores | 8 cores |
| **Espacio en disco** | 50 GB | 100 GB |
| **Network** | 100 Mbps | 500 Mbps (para descargas de APIs) |

### Configuración del Sistema

```bash
# Verificar Python
python --version  # Debe ser 3.12+

# Verificar Java
java -version  # Debe ser 17 o 21
echo $JAVA_HOME  # Debe estar configurado

# Verificar uv
uv --version

# Verificar Docker
docker --version
docker compose version
```

---

## 🚀 Instalación Paso a Paso

### Paso 1: Clonar el Repositorio

```bash
git clone https://github.com/yourusername/EP-GDM-G6.git
cd EP-GDM-G6
```

### Paso 2: Instalar Dependencias Python

```bash
# Instalar todas las dependencias (incluye PySpark, PyArrow, Pydantic, etc.)
uv sync

# Activar el entorno virtual (si es necesario)
source .venv/bin/activate  # Linux/Mac
# o
.\.venv\Scripts\activate  # Windows
```

### Paso 3: Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```bash
# Contraseña de SQL Server (mínimo 8 caracteres, con mayúscula, minúscula, número, símbolo)
MSSQL_SA_PASSWORD=TuPasswordSeguro123!

# (Opcional) Credenciales por defecto para conexiones
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1434
SQLSERVER_USER=sa
SQLSERVER_DB=ETL_DB

# (Opcional) Configuración de Spark
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
```

### Paso 4: Iniciar SQL Server en Docker

```bash
# Levantar el contenedor de SQL Server
docker compose up -d

# Verificar que está corriendo
docker compose ps
# Esperar ~30 segundos a que se inicialize la BD

# (Opcional) Ver logs
docker compose logs -f mssql
```

### Paso 5: Verificar Conectividad

```bash
# Testear conexión a SQL Server
# Esperar a que el contenedor esté listo (~30-60 segundos)
sleep 30

# Verificar que la BD responde
python -c "
from app.utils.spark import SparkClient
sc = SparkClient()
spark = sc.get_session()
print('✓ Spark session iniciado')
"
```

---

## 🎯 Uso del Pipeline

### Ejecutar Todo el Pipeline (Recomendado para Primera Vez)

```bash
# Ejecuta: bronze → profile → silver (20-30 minutos)
python main.py

# Después de completar:
# ✓ 24 archivos parquet en data/bronze/
# ✓ Reportes de calidad en data/profiling/ (abrir profiling_summary.html)
# ✓ Tablas cargadas en SQL Server
```

### Ejecutar Etapas Individuales

#### 1️⃣ Solo Bronze (Descarga y Conversión)

```bash
python main.py bronze
# Genera: data/bronze/*.parquet (2M+ filas)
# Tiempo: ~5-10 minutos
```

#### 2️⃣ Solo Profiling (Validación de Calidad)

```bash
python main.py profile
# Genera: data/profiling/profiling_summary.html + JSONs detallados
# Tiempo: ~3-5 minutos
# Abre en navegador: file://$(pwd)/data/profiling/profiling_summary.html
```

#### 3️⃣ Solo Silver (Transformación y Carga)

```bash
# Pipeline completo silver
python main.py silver
# Tiempo: ~10-15 minutos

# O partes específicas:
python main.py silver quality    # Solo correcciones de calidad
python main.py silver schema     # Solo construcción de esquema
python main.py silver load       # Solo carga a SQL Server

# Recrear tablas (DROP + CREATE)
python main.py silver --drop
```

### Combinaciones Útiles

```bash
# Bronze + Profiling (sin carga)
python main.py bronze profile

# Profiling + Silver (reutilizar bronze)
python main.py profile silver

# Silver solo en limpieza (sin descargas)
python main.py silver --force
```

---

## 📊 Validar Resultados

### ✓ Bronze Completado

```bash
ls -lh data/bronze/
# Debe mostrar:
# - 24 archivos .parquet
# - ~2.1 GB totales
```

### ✓ Profiling Completado

```bash
# Abrir reporte HTML
open data/profiling/profiling_summary.html  # Mac
xdg-open data/profiling/profiling_summary.html  # Linux
start data/profiling/profiling_summary.html  # Windows

# Verificar en JSON
cat data/profiling/profiling_summary.json | jq '.overall_avg_score'
# Debe mostrar: 94.03 (o superior)
```

### ✓ Silver Completado (SQL Server)

```bash
# Conectar a SQL Server y verificar tablas
docker compose exec mssql sqlcmd -S localhost -U sa -P $MSSQL_SA_PASSWORD << 'EOF'
USE ETL_DB
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'
GO
EOF

# Debe mostrar ~17 tablas (14 dimensiones + 3 hechos)
```

---

## ⚙️ Configuración Detallada

### config.yaml - Configuración Principal

El archivo `config.yaml` define toda la orquestación del pipeline:

```yaml
# Datasets a descargar
datasets:
  siaf:
    modules:
      - "2021-Ingreso"    # Ingresos fiscal año 2021
      - "2022-Ingreso"    # Ingresos fiscal año 2022
      # ... más años
    dictionaries: []      # Diccionarios globales (no aplica)
    
  sismepre:
    modules:
      - "rentas_ano_aplicacion"           # Módulo de años
      - "rentas_esat_estadistica_atm"     # Estadísticas ATM
      # ... más módulos
    dictionaries:
      - "rentas_ano_aplicacion_diccionario"
      - "rentas_esat_estadistica_atm_diccionario"
      # ... más diccionarios
      
  renamu:
    modules:
      - "RENAMU-2021"     # Registro 2021
      - "RENAMU-2022"     # Registro 2022
      # ... más años
    dictionaries: []

# Configuración de silver (transformación y carga)
silver:
  output_path: "data/silver/stage"
  sqlserver:
    host: "localhost"
    port: 1434
    database: "ETL_DB"
    user: "sa"
    password: "${MSSQL_SA_PASSWORD}"  # Desde .env

# APIs
api:
  base_url: "https://www.datos.gob.pe/api/3/action"
  timeout: 30
  output_path: "data/bronze"

# Logging
logs:
  path: "logs"
  level: "INFO"
```

### Variables de Entorno (.env)

```bash
# SQL Server - OBLIGATORIO
MSSQL_SA_PASSWORD=TuPasswordSeguro123!

# Spark (Opcional, valores por defecto: 4g cada uno)
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_MAX_CORES=4

# Logging (Opcional)
LOG_LEVEL=INFO
```

---

## 🗂️ Estructura del Proyecto Completa

```
EP-GDM-G6/
├── README.md                          # Este archivo
├── main.py                            # ⭐ Punto de entrada principal
├── config.yaml                        # Configuración de datasets y APIs
├── .env                               # Variables de entorno (NO en git)
├── pyproject.toml                     # Dependencias Python (uv)
├── uv.lock                            # Lock file de dependencias
├── docker-compose.yml                 # SQL Server container
│
├── app/                               # Código principal de la aplicación
│   ├── client/                        # Clientes HTTP para APIs
│   │   ├── __init__.py
│   │   ├── base_client.py             # ClienteBase (HTTP/2, gzip)
│   │   └── mef_client.py              # Cliente MEF + INEI
│   │
│   ├── pipeline/                      # Pipelines ETL principales
│   │   ├── __init__.py
│   │   ├── bronze.py                  # BronzePipeline: descarga CSVs → Parquet
│   │   ├── profile.py                 # Profiler: evaluación de calidad
│   │   ├── silver.py                  # SilverPipeline: transformación + carga
│   │   └── gold.py                    # Stub vacío (futuro)
│   │
│   ├── silver/                        # Transformaciones de capa silver
│   │   ├── __init__.py
│   │   ├── quality.py                 # Type casting, null coalesce, dedup
│   │   ├── transforms.py              # Dimensiones (14) + Hechos (3)
│   │   └── loader.py                  # DDL + Carga JDBC
│   │
│   ├── settings/
│   │   ├── __init__.py
│   │   └── settings.py                # Singleton carga config.yaml
│   │
│   ├── schemas/                       # Esquemas Pydantic (removidos)
│   │   └── __init__.py
│   │
│   └── utils/                         # Utilidades
│       ├── __init__.py
│       ├── spark.py                   # 🔧 Configuración Spark + JDBC
│       ├── profiler.py                # 📊 Lógica de profiling (8 criterios)
│       └── logging.py                 # 📝 Logging centralizado
│
├── sql/                               # Scripts SQL
│   ├── tables_silver.sql              # DDL de tablas silver
│   └── procedures.sql                 # Procedimientos almacenados
│
├── data/                              # Datos (⚠️ en .gitignore)
│   ├── bronze/                        # 24 archivos parquet (~2.1 GB)
│   │   ├── dicts/                     # Diccionarios de referencia
│   │   ├── SIAF-*.parquet             # Datos de ejecución presupuestaria
│   │   ├── SISMEPRE-*.parquet         # Datos de impuesto predial
│   │   └── RENAMU-*.parquet           # Datos de municipalidades
│   │
│   ├── silver/
│   │   └── stage/                     # Archivos intermedios
│   │
│   └── profiling/                     # Reportes de calidad
│       ├── profiling_summary.html     # 📊 Reporte visual interactivo
│       ├── profiling_summary.json     # JSON con métricas
│       ├── SIAF-*.json                # Detalles por archivo
│       └── ...
│
├── logs/                              # Logs de ejecución (⚠️ en .gitignore)
│   ├── app.log
│   └── errors.log
│
├── lib/                               # (Futuro) Librerías locales
├── sql/                               # (Futuro) Scripts SQL avanzados
└── AGENTS.md                          # Documentación para agentes IA

# Archivos de configuración Git
├── .gitignore
├── .git/
└── .gitattributes
```

---

## 📈 Datasets Detallados

### 1. SIAF (Sistema Integrado de Administración Financiera)

**Fuente:** MEF - Portal de Transparencia Económica  
**Descripción:** Ejecución presupuestaria (PIA, PIM, Gasto)

| Módulo | Años | Filas | Columnas | Descripción |
|--------|------|-------|----------|------------|
| Ingreso | 2021-2025 | 828K-878K | 36 | Ingresos tributarios y no tributarios |

**Tabla de Hechos en Silver:**
- `fact_siaf_ingreso`: Ingresos desglosados por dimensiones

### 2. SISMEPRE (Sistema de Seguimiento a la Meta de Impuesto Predial)

**Fuente:** MEF - Portal de Transparencia Económica  
**Descripción:** Seguimiento de recaudación de impuesto predial

| Módulo | Años | Filas | Columnas | Descripción |
|--------|------|-------|----------|------------|
| rentas_ano_aplicacion | 2024-2025 | 26 | 9 | Años de aplicación del impuesto |
| rentas_esat_estadistica_atm | 2024-2025 | 133K | 42 | Estadísticas de ATM |
| rentas_entidad_estado | 2024-2025 | 19K | 13 | Entidades y estados |
| rentas_estadistica | 2024-2025 | 233 | 7 | Estadísticas generales |
| rentas_formulario | 2024-2025 | 94 | 10 | Formularios |
| rentas_preguntas | 2024-2025 | 696 | 15 | Preguntas |
| rentas_respuestas | 2024-2025 | 248K | 11 | Respuestas |

**Tablas de Hechos en Silver:**
- `fact_sismepre_*`: Una por cada módulo

### 3. RENAMU (Registro Nacional de Municipalidades)

**Fuente:** INEI - ZIP descargable  
**Descripción:** Registro estadístico de municipalidades

| Módulo | Años | Filas | Columnas | Descripción |
|--------|------|-------|----------|------------|
| RENAMU | 2021-2024 | 1.8K-1.9K | 1.3K-1.4K | Datos estadísticos municipales |

**Tabla de Hechos en Silver:**
- `fact_renamu`: Indicadores municipales

---

## ❓ Troubleshooting

### Error: "JAVA_HOME not found"

```bash
# Linux/Mac: Configurar JAVA_HOME
export JAVA_HOME=/path/to/java/17
# Agregar a ~/.bashrc o ~/.zshrc para persistencia

# Windows: Configurar variable de entorno
# Panel de Control → Sistema → Variables de Entorno
# Nueva variable: JAVA_HOME = C:\Program Files\Java\jdk-17
```

### Error: "Failed to connect to SQL Server"

```bash
# 1. Verificar que el contenedor esté corriendo
docker compose ps

# 2. Ver logs del contenedor
docker compose logs mssql

# 3. Esperar más tiempo (SQL Server tarda ~60 segundos)
sleep 60

# 4. Recrear el contenedor
docker compose down
docker compose up -d
docker compose logs -f mssql
```

### Error: "Permission denied on data/profiling"

```bash
# Dar permisos de escritura
chmod -R 777 data/

# O ejecutar con permisos correctos
chown -R $(whoami) data/
```

### Error: "PySpark Java ClassNotFound"

```bash
# Actualizar JAVA_HOME en spark.py
# Verificar que Java 17 o 21 está instalado
java -version

# Si es Java 22+, puede no ser compatible
# Desinstalar e instalar Java 21 (recomendado)
```

### Error: "UnicodeDecodeError en profiling"

```bash
# El archivo tiene caracteres especiales (ñ, é, etc.)
# Ya está corregido en profiler.py con backticks en SQL
# Si persiste, reportar error
```

---

## 🔍 Monitoreo y Validación

### Durante Ejecución

```bash
# Ver logs en tiempo real
tail -f logs/app.log

# Monitorear recursos Spark
watch docker compose stats  # Docker
# o
htop  # Sistema operativo
```

### Verificación Post-Ejecución

```bash
# 1. Bronze: Contar archivos y tamaño
find data/bronze -name "*.parquet" -type f | wc -l  # Debe ser 24
du -sh data/bronze/                                  # ~2.1 GB

# 2. Profiling: Validar score promedio
python -c "
import json
with open('data/profiling/profiling_summary.json') as f:
    data = json.load(f)
    print(f'Score promedio: {data[\"overall_avg_score\"]}%')
    print(f'Archivos pasados: {data[\"files_passed\"]}/{data[\"files_profiled\"]}')
"

# 3. Silver: Contar filas en SQL Server
docker compose exec mssql sqlcmd -S localhost -U sa -P $MSSQL_SA_PASSWORD << 'EOF'
USE ETL_DB
SELECT TABLE_NAME, (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo') as total_tables
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME
GO
EOF
```

---

## 🎓 Arquitectura Detallada de Calidad (8 Criterios)

### 1. **Exactitud** ✓
Detección de outliers estadísticos extremos (±3σ de la media)
- **Aplica a:** Columnas numéricas (monto, pim, pia, etc.)
- **Umbral:** Score ≥ 70% para pasar
- **Métrica:** % de valores dentro de ±3σ

### 2. **Completitud** ✓
Porcentaje de valores no nulos
- **Aplica a:** Todas las columnas
- **Umbral:** ≤30% nulidad por columna
- **Manejo especial:** Columnas >85% nulos son opcionales

### 3. **Consistencia** ✓
Coherencia interna de formatos y rangos
- **Aplica a:** Años (4 dígitos), códigos (ccdd, ccpp, ccdi)
- **Validaciones:**
  - Años: `^(19|20)\d{2}` o `^(19|20)\d{2}-`
  - ccdd: [1-25], ccpp: [1-99], ccdi: [1-99]
- **Umbral:** Score ≥ 70%

### 4. **Integridad** ✓
Columnas identificadoras sin valores nulos
- **Aplica a:** Columnas con "id", "codigo", "ubigeo", etc.
- **Umbral:** 0% nulidad (100% score)

### 5. **Razonabilidad** ✓
Valores sensatos para el dominio fiscal
- **Validaciones:**
  - Montos ≥ 0
  - Códigos no vacíos
  - Años [2000-2030]
- **Umbral:** Score ≥ 70%

### 6. **Oportunidad** ✓
Datos temporalmente válidos
- **Rango de años:** [2000-2030]
- **Extrae primeros 4 dígitos:** Maneja "2023-I" como 2023
- **Umbral:** Score ≥ 70%

### 7. **Unicidad** ✓
Sin filas completamente duplicadas
- **Métrica:** % de filas únicas
- **Umbral:** Score ≥ 70%

### 8. **Validez** ✓
Formatos correctos
- **Numéricos:** Casteables a DOUBLE
- **Ubigeo:** 1-6 dígitos
- **Años:** 4 dígitos o 4-dígitos-letra
- **Umbral:** Score ≥ 70%

**Resultado Final:** 
- Score promedio de todos los criterios
- Pasa si: `overall_score ≥ 70%`
- **Status actual:** 24/24 archivos pasan (94.03% promedio)

---

## 🔐 Consideraciones de Seguridad

### SQL Server

- **Contraseña:** Mínimo 8 caracteres (mayúscula + minúscula + número + símbolo)
- **Puerto:** Mapeado internamente 1433 → 1434 externo
- **Autenticación:** SA (SQL Server Authentication)
- **Credenciales:** Nunca en git, siempre en `.env`

```bash
# ❌ NO hacer
git add .env
echo "password123" > credentials.txt

# ✅ Hacer
echo ".env" >> .gitignore
export MSSQL_SA_PASSWORD="SecurePass123!"
```

### Datos Sensibles

- Bronze: Datos públicos (portales del gobierno)
- Silver: Pueden contener información a nivel municipal
- Gold: (Futuro) Agregar validación de acceso

---

## 📊 Métricas de Éxito

### Bronze
- ✓ 24 archivos descargados
- ✓ 2.1M filas totales
- ✓ Compresión Zstandard activa

### Profiling
- ✓ Todos los archivos pasan (≥70%)
- ✓ Score promedio ≥ 90%
- ✓ Reporte HTML generado

### Silver
- ✓ 14 dimensiones creadas
- ✓ 3 tablas de hechos cargadas
- ✓ ≥1M filas en SQL Server

---

## 🚀 Próximos Pasos (Roadmap)

- [ ] **Capa Gold:** Vistas agregadas para ejecutivos
- [ ] **Dashboard:** Power BI / Tableau
