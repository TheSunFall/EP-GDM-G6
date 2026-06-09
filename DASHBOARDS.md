# Guia de Dashboards (Capa Gold)

> Documento operativo para construir los dashboards sobre la capa **Gold** ya
> materializada y validada. Complementa el diseno en `GOLD_PLAN.md`.

## 0. Estado actual (validado)

La capa gold esta cargada en SQL Server, base **`DLKH_MEF`**, esquema **`gold`**.
Cuadre verificado: `SUM(MontoRecaudado)` en los marts SIAF = `SUM(MONTO_RECAUDADO)`
de `silver.FACT_INGRESO` (S/ 377,728,937,461.86), sin perdida de filas.

| Objeto gold | Filas | Uso en dashboards |
|-------------|------:|-------------------|
| `DIM_CALENDARIO` | 216 | Tabla de fechas (grano mes) para marts SIAF |
| `DIM_ANIO` | 18 | Eje anual para marts RENAMU / SISMEPRE |
| `DIM_GEOGRAFIA` | 1,892 | Jerarquia Departamento > Provincia > Distrito (mapas) |
| `MART_INGRESOS_GEOGRAFICO` | 19,014 | Dashboard 1 (principal) |
| `MART_INGRESOS_CLASIFICADOR` | 2,023 | Dashboard 1 (origen del ingreso) |
| `MART_INGRESOS_EJECUTORA` | 4,583 | Dashboard 1 (ranking) |
| `MART_PREDIAL` | 248,582 | Dashboard 2 (impuesto predial) |
| `MART_RENAMU` | 10,164,493 | Dashboard 3 (indicadores municipales) |

Para regenerar/actualizar: `python main.py gold` (o `gold --drop` para recrear).

---

## 1. Que necesitas

| Item | Detalle |
|------|---------|
| **Herramienta** | **Power BI Desktop** (recomendado, Windows, gratis) o **Metabase** (open-source, dockerizable). |
| **Conexion** | Servidor `localhost,11423` · Base `DLKH_MEF` · Esquema `gold` · Usuario `sa`. |
| **Password** | Variable `MSSQL_SA_PASSWORD` del `.env` (no versionado). Ver nota de seguridad abajo. |
| **Modo** | *Import* (los marts ya estan pre-agregados; carga rapida). |
| **Mapas** | TopoJSON/Shape de departamentos del Peru para los visuales de mapa (Power BI Shape Map). |

> **Nota de seguridad:** la SA password vive solo en `.env` (no versionado). El `.env`
> tenia un valor que no coincidia con el del contenedor; se alineo. Para produccion
> conviene rotarla: `ALTER LOGIN sa WITH PASSWORD = '<nueva>'` y actualizar `.env`.

---

## 2. Conexion en Power BI Desktop

1. **Inicio > Obtener datos > SQL Server**.
2. Servidor: `localhost,11423` · Base de datos: `DLKH_MEF` · Modo: **Import**.
3. (Si pide credenciales) pestania **Base de datos**: usuario `sa`, password del `.env`.
   - Si falla por TLS, en *Opciones avanzadas* desmarca "Cifrar conexion" o confia en el certificado del servidor.
4. Selecciona solo las tablas del esquema **`gold`** (las 8 de la tabla anterior).
5. **Cargar**.

### Metabase (alternativa OSS)
Anadir un servicio al `docker-compose.yml` y conectar a la base `DLKH_MEF`:
```yaml
  metabase:
    image: metabase/metabase:latest
    ports: ["3000:3000"]
    depends_on: [sqlserver]
```
En Metabase: *Add database > SQL Server*, host `sqlserver` (red interna de compose) o
`host.docker.internal`, puerto `1433` (interno) / `11423` (desde host), base `DLKH_MEF`.

---

## 3. Modelo de datos (relaciones)

Crear estas relaciones (todas *uno-a-muchos*, direccion simple del lado dimension):

| Desde (mart) | Columna | Hacia (dimension) | Columna |
|--------------|---------|-------------------|---------|
| `MART_INGRESOS_GEOGRAFICO` | `AnioMes` | `DIM_CALENDARIO` | `AnioMes` |
| `MART_INGRESOS_GEOGRAFICO` | `IdUbigeo` | `DIM_GEOGRAFIA` | `IdUbigeo` |
| `MART_INGRESOS_CLASIFICADOR` | `Anio` | `DIM_ANIO` | `Anio` |
| `MART_INGRESOS_EJECUTORA` | `Anio` | `DIM_ANIO` | `Anio` |
| `MART_PREDIAL` | `Anio` | `DIM_ANIO` | `Anio` |
| `MART_RENAMU` | `Anio` | `DIM_ANIO` | `Anio` |
| `MART_RENAMU` | `IdUbigeo` | `DIM_GEOGRAFIA` | `IdUbigeo` |

**Importante:** marca `DIM_CALENDARIO` como *tabla de fechas* usando `FechaInicioMes`
(Modelado > Marcar como tabla de fechas). Esto habilita inteligencia de tiempo (YoY, MTD).

> Los marts anuales (predial, RENAMU) se relacionan por `Anio` con `DIM_ANIO`, **no**
> por `AnioMes` (resuelve el grano mixto de silver descrito en `GOLD_PLAN.md`, C1).

---

## 4. Medidas DAX base

```DAX
Recaudado Total = SUM ( MART_INGRESOS_GEOGRAFICO[MontoRecaudado] )
PIM Total       = SUM ( MART_INGRESOS_GEOGRAFICO[MontoPIM] )
PIA Total       = SUM ( MART_INGRESOS_GEOGRAFICO[MontoPIA] )

-- Calcular el % desde sumas (NO promediar la columna PctEjecucion fila a fila)
% Ejecucion = DIVIDE ( [Recaudado Total], [PIM Total] )
Brecha      = [PIM Total] - [Recaudado Total]

Recaudado YoY % =
    VAR Previo =
        CALCULATE ( [Recaudado Total],
                    DATEADD ( DIM_CALENDARIO[FechaInicioMes], -1, YEAR ) )
    RETURN DIVIDE ( [Recaudado Total] - Previo, Previo )

-- RENAMU: cobertura de un indicador (sobre el mart tall)
Municipios con indicador =
    CALCULATE ( DISTINCTCOUNT ( MART_RENAMU[IdUbigeo] ),
                MART_RENAMU[EsAfirmativo] = 1 )
```

> **Por que medir desde sumas:** la columna `PctEjecucion` materializada es por grano
> (mes-ubigeo-nivel) y puede salir negativa o enorme cuando el PIM de ese grano es ~0.
> Para KPIs globales usa siempre `DIVIDE(SUM(Rec), SUM(PIM))`.

---

## 5. Dashboards

### Dashboard 1 — Ejecucion de Ingresos (SIAF)
Marts: `MART_INGRESOS_GEOGRAFICO`, `_CLASIFICADOR`, `_EJECUTORA`.
- Tarjetas KPI: `Recaudado Total`, `PIM Total`, `% Ejecucion`, `Brecha`.
- Linea temporal: `Recaudado Total` vs `PIM Total` por `DIM_CALENDARIO[FechaInicioMes]`.
- Mapa (Shape Map de departamentos): `Recaudado Total` o `% Ejecucion` por `DIM_GEOGRAFIA[Departamento]`.
- Barras: recaudacion por `Rubro` / `TipoRecurso` (`MART_INGRESOS_CLASIFICADOR`).
- Tabla top-N: `MART_INGRESOS_EJECUTORA` por `Ejecutora` (Recaudado, % Ejecucion).
- Segmentadores: `DIM_ANIO[Anio]`, `DIM_GEOGRAFIA[Departamento]`, `NivelGobierno`.

### Dashboard 2 — Impuesto Predial (SISMEPRE)
Mart: `MART_PREDIAL` (formato tall: una fila por pregunta).
- Explorar metricas con una **matriz**: filas = `Municipalidad`, columnas = `PreguntaDescripcion`,
  valores = `SUM(ValorNumerico)`. Filtra a las preguntas de interes con un segmentador de `PreguntaDescripcion`.
- Ejemplos de preguntas con valor numerico (reales en los datos):
  - "20. La municipalidad utiliza el 5% de la recaudacion predial..."
  - "6. Numero total de personas que laboran y/o prestan servicios..."
  - "11.1. Indique Porcentaje al cierre del anio..."
- Para fijar KPIs concretos, crea **medidas filtradas**, p.ej.:
  ```DAX
  Personas area predial =
      CALCULATE ( SUM ( MART_PREDIAL[ValorNumerico] ),
          MART_PREDIAL[PreguntaDescripcion] = "6. Numero total de personas que laboran..." )
  ```
- Tendencia por `DIM_ANIO[Anio]`; ranking de municipalidades.

### Dashboard 3 — Indicadores Municipales (RENAMU)
Mart: `MART_RENAMU` (10.1M filas, formato tall).
- Cobertura de servicios: medida `Municipios con indicador` por `NombreCampo`, sobre el mapa.
- Comparativa por `TipoMuni` (provincial / distrital / centro poblado).
- **Mapeo de codigos:** `NombreCampo`/`Descripcion` son codigos del cuestionario
  (ej. `P70_2`). Para nombres legibles, cruza con el **diccionario RENAMU** (INEI) como
  tabla puente `NombreCampo -> EtiquetaLegible` (importala como CSV en Power BI).
- Por volumen, aplica filtros (anio, departamento, conjunto de indicadores) antes de visualizar.

---

## 6. Conflictos de la capa BI y mitigaciones

| Conflicto | Sintoma | Mitigacion |
|-----------|---------|------------|
| Grano de tiempo mixto | Filtrar por mes vacia los visuales anuales (predial/RENAMU). | Relacionar marts anuales por `Anio`/`DIM_ANIO`; usar el segmentador adecuado por pagina. |
| `PctEjecucion` por fila | Promedios enganiosos (valores negativos/enormes). | Medir `% Ejecucion` con `DIVIDE(SUM,SUM)` en DAX. |
| Mapas de Peru | Sin shapes nativos de distritos. | Usar Shape Map con TopoJSON de departamentos, o mapa por `Ubigeo`. |
| Codigos RENAMU | Indicadores ilegibles. | Tabla puente con el diccionario RENAMU. |
| Volumen RENAMU (10M) | Modelo pesado / lento. | Import con filtros; considerar agregacion adicional si hace falta. |
| Refresh | Datos viejos tras re-cargar. | Tras `python main.py gold`, **Actualizar** en Power BI. |

---

## 7. Checklist de dashboards

- [ ] Herramienta instalada (Power BI Desktop / Metabase).
- [ ] Conexion a `DLKH_MEF` / esquema `gold` en modo Import.
- [ ] `DIM_CALENDARIO` marcada como tabla de fechas.
- [ ] Relaciones creadas (seccion 3).
- [ ] Medidas DAX base (seccion 4).
- [ ] Dashboard 1 (Ingresos) con mapa + KPIs + ranking.
- [ ] Dashboard 2 (Predial) con matriz de preguntas.
- [ ] Dashboard 3 (RENAMU) con tabla puente de codigos.
- [ ] TopoJSON de departamentos para los mapas.
- [ ] (Opcional) Publicar en Power BI Service / compartir Metabase.
