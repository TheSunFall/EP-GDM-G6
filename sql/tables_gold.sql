-- ============================================================================
-- DDL de la capa Gold del lakehouse medallon.
-- Se construye EXCLUSIVAMENTE sobre el esquema silver (no depende de bronze).
-- Separador de lotes por batch: el loader divide el script en bloques.
-- Disenio: tablas materializadas (full reload via TRUNCATE+INSERT en proc_gold.sql).
-- Nota: evitar la palabra reservada de batch dentro de comentarios (rompe el split).
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC ('CREATE SCHEMA gold');
GO

-- == Dimensiones de presentacion ==

-- Calendario contiguo a grano mensual. Resuelve el grano mixto de silver.DIM_TIEMPO.
-- Usado por los marts SIAF (mensuales). Marcable como tabla de fechas en Power BI.
create table gold.DIM_CALENDARIO
(
    AnioMes        int         not null
        primary key,
    Anio           smallint    not null,
    Mes            tinyint     not null,
    NombreMes      varchar(20) not null,
    Trimestre      tinyint     not null,
    FechaInicioMes date        not null
)
go

-- Dimension anual. Resuelve relaciones de marts anuales (RENAMU, SISMEPRE) y
-- rollups anuales de SIAF. Clave unica por anio para relacionar sin many-to-many.
create table gold.DIM_ANIO
(
    Anio smallint not null
        primary key
)
go

-- Jerarquia geografica limpia (Departamento > Provincia > Distrito).
-- Copia 1:1 de silver.DIM_UBIGEO (conserva IdUbigeo como surrogate para FKs).
create table gold.DIM_GEOGRAFIA
(
    IdUbigeo           int          not null
        primary key,
    CodigoDepartamento smallint     not null,
    Departamento       varchar(100) not null,
    CodigoProvincia    smallint     not null,
    Provincia          varchar(100) not null,
    CodigoDistrito     smallint     not null,
    Distrito           varchar(100) not null,
    Ubigeo             varchar(6)   not null
)
go

-- == Marts SIAF (ingresos) desde silver.FACT_INGRESO ==

-- Mart principal: ejecucion de ingresos por geografia y nivel de gobierno.
-- Grano: (AnioMes, IdUbigeo, NivelGobierno).
create table gold.MART_INGRESOS_GEOGRAFICO
(
    AnioMes        int            not null,
    Anio           smallint       not null,
    Mes            tinyint        not null,
    IdUbigeo       int            not null,
    Departamento   varchar(100)   not null,
    Provincia      varchar(100)   not null,
    Distrito       varchar(100)   not null,
    NivelGobierno  varchar(100)   not null,
    MontoPIA       bigint         not null,
    MontoPIM       bigint         not null,
    MontoRecaudado numeric(18, 2) not null,
    PctEjecucion   float              null,  -- Recaudado / PIM
    Brecha         numeric(18, 2) not null,  -- PIM - Recaudado
    VarPIA_PIM     bigint         not null   -- PIM - PIA
)
go

-- Mart por clasificador presupuestal: de donde viene el ingreso.
-- Grano: (Anio, Rubro, TipoRecurso, Generica, SubGenerica).
create table gold.MART_INGRESOS_CLASIFICADOR
(
    Anio           smallint       not null,
    Rubro          varchar(100)   not null,
    TipoRecurso    varchar(100)   not null,
    Generica       varchar(100)   not null,
    SubGenerica    varchar(100)   not null,
    MontoPIA       bigint         not null,
    MontoPIM       bigint         not null,
    MontoRecaudado numeric(18, 2) not null,
    PctEjecucion   float              null,
    Brecha         numeric(18, 2) not null
)
go

-- Mart por ejecutora: ranking de unidades ejecutoras.
-- Grano: (Anio, IdEjecutora, Departamento).
create table gold.MART_INGRESOS_EJECUTORA
(
    Anio           smallint       not null,
    IdEjecutora    int            not null,
    SecEjec        int            not null,
    Ejecutora      varchar(200)   not null,
    Categoria      varchar(10)    not null default '',
    Departamento   varchar(100)   not null,
    MontoPIA       bigint         not null,
    MontoPIM       bigint         not null,
    MontoRecaudado numeric(18, 2) not null,
    PctEjecucion   float              null,
    Brecha         numeric(18, 2) not null
)
go

-- == Mart SISMEPRE (impuesto predial) desde silver.FACT_FORMULARIO_SISMEPRE ==
-- Formato analitico "tall": una fila por (ejecutora, anio, periodo, pregunta).
-- ValorNumerico unifica respuesta decimal/entera para medidas directas en BI.
-- El pivote a metricas concretas se hace en BI o en una vista, tras identificar
-- las preguntas clave por DESCRIPCION.
create table gold.MART_PREDIAL
(
    Anio                smallint       not null,
    Periodo             smallint       not null,
    IdEjecutora         int            not null,
    SecEjec             int            not null,
    Municipalidad       varchar(200)   not null,
    Categoria           varchar(10)    not null default '',
    IdFormulario        int            not null,
    FormularioTitulo    varchar(100)   not null,
    IdPregunta          int            not null,
    PreguntaDescripcion varchar(400)   not null,
    RespuestaId         smallint       not null,
    RespuestaTexto      varchar(1000)      null,
    RespuestaDecimal    numeric(18, 2)     null,
    RespuestaEntero     int                null,
    RespuestaFecha      varchar(100)       null,
    ValorNumerico       numeric(18, 2)     null  -- COALESCE(decimal, entero)
)
go

-- == Mart RENAMU (indicadores municipales) desde silver.FACT_RENAMU ==
-- Resuelve el hecho factless: castea VALOR (string en DIM_PREGUNTA_RENAMU) a
-- numerico y deriva un flag Si/No para medir cobertura de servicios.
-- Grano: (Anio, IdUbigeo, TipoMuni, IdPregunta).
create table gold.MART_RENAMU
(
    Anio          smallint       not null,
    IdUbigeo      int            not null,
    Departamento  varchar(100)   not null,
    Provincia     varchar(100)   not null,
    Distrito      varchar(100)   not null,
    TipoMuni      varchar(100)   not null,
    IdPregunta    int            not null,
    NombreCampo   varchar(100)   not null,
    Descripcion   varchar(100)   not null,
    ValorTexto    varchar(300)       null,
    ValorNumerico numeric(18, 2)     null,  -- TRY_CONVERT(numeric, VALOR)
    EsAfirmativo  bit            not null   -- 1 si VALOR en {Si, 1, X, True}
)
go
