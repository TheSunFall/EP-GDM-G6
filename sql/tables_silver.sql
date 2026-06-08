IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

create table silver.DIM_EJECUTORA
(
    IdEjecutora      int identity
        primary key,
    SEC_EJEC         int not null,
    EJECUTORA        int not null,
    EJECUTORA_NOMBRE varchar(200) not null,
    CATEGORIA        varchar(10) not null default ''
)
go

create table silver.DIM_ESPECIFICA
(
    IdEspecifica          int identity
        primary key,
    ESPECIFICA            smallint not null,
    ESPECIFICA_NOMBRE     varchar(100) not null,
    ESPECIFICA_DET        smallint not null,
    ESPECIFICA_DET_NOMBRE varchar(100) not null
)
go

create table silver.DIM_FORMULARIO_SISMEPRE
(
    IdFormSismepre  int identity
        primary key,
    FORMULARIO_ID   smallint not null,
    TITULO          varchar(100) not null,
    SUBTITULO       varchar(200) not null,
    ABREVIATURA     varchar(100) not null,
    CLASIFICACION   varchar(100) not null,
    TIPO_FORMULARIO varchar(100) not null
)
go

create table silver.DIM_FUENTE_FINANCIAMIENTO
(
    IdFuenteFinanciamiento       int identity
        primary key,
    FUENTE_FINANCIAMIENTO        smallint not null,
    FUENTE_FINANCIAMIENTO_NOMBRE varchar(100) not null
)
go

create table silver.DIM_GENERICA
(
    IdGenerica             int identity
        primary key,
    GENERICA               smallint not null,
    GENERICA_NOMBRE        varchar(100) not null,
    SUBGENERICA            smallint not null,
    SUBGENERICA_NOMBRE     varchar(100) not null,
    SUBGENERICA_DET        smallint not null,
    SUBGENERICA_DET_NOMBRE varchar(100) not null
)
go

create table silver.DIM_NIVEL_GOBIERNO
(
    IdNivelGobierno       int identity
        primary key,
    NIVEL_GOBIERNO        varchar(100) not null,
    NIVEL_GOBIERNO_NOMBRE varchar(100) not null
)
go

create table silver.DIM_PLIEGO
(
    IdPliego      int identity
        primary key,
    PLIEGO        varchar(100) not null,
    PLIEGO_NOMBRE varchar(200) not null
)
go

create table silver.DIM_PREGUNTA_RENAMU
(
    IdPregunta   int identity
        primary key,
    IdPadre      int
        constraint FK_DIM_PREGUNTA_RENAMU_PADRE
            references silver.DIM_PREGUNTA_RENAMU,
    NOMBRE_CAMPO varchar(100) not null,
    DESCRIPCION  varchar(100) not null,
    VALOR        varchar(300) not null,
    METADATA     varchar(100) not null
)
go

create table silver.DIM_PREGUNTA_SISMEPRE
(
    IdPreguntaSismepre   int identity
        primary key,
    IdFormSismepre       int           not null
        constraint FK_DIM_PREGUNTA_SISMEPRE_FORM
            references silver.DIM_FORMULARIO_SISMEPRE,
    PREGUNTA_ID          smallint not null,
    PREGUNTA_PADRE_ID    smallint not null,
    DESCRIPCION          varchar(400) not null,
    TIPO_CUESTIONARIO_ID smallint not null
)
go

create table silver.DIM_RUBRO
(
    IdRubro      int identity
        primary key,
    RUBRO        smallint not null,
    RUBRO_NOMBRE varchar(100) not null
)
go

create table silver.DIM_SECTOR
(
    IdSector      int identity
        primary key,
    SECTOR        varchar(100) not null,
    SECTOR_NOMBRE varchar(100) not null
)
go

create table silver.DIM_TIEMPO
(
    IdTiempo int not null
        primary key,
    ANIO     smallint not null,
    MES      smallint,
    DIA      smallint,
    HORA     smallint,
    MINUTO   smallint,
    SEGUNDO  smallint
)
go

create table silver.DIM_ANIO_APLICACION
(
    IdAnioAplicacion      int identity
        primary key,
    ANO_APLICACION        int not null
        constraint FK_DIM_ANIO_APLICACION_ANO
            references silver.DIM_TIEMPO,
    ANO_APLICACION_INICIO int not null
        constraint FK_DIM_ANIO_APLICACION_INICIO
            references silver.DIM_TIEMPO,
    ANO_APLICACION_FIN    int not null
        constraint FK_DIM_ANIO_APLICACION_FIN
            references silver.DIM_TIEMPO,
    FECHA_CIERRE          int not null
        constraint FK_DIM_ANIO_APLICACION_CIERRE
            references silver.DIM_TIEMPO,
    FECHA_PRES_OFICIO     int not null
        constraint FK_DIM_ANIO_APLICACION_PRES_OFICIO
            references silver.DIM_TIEMPO,
    FECHA_INI_CIERRE      int not null
        constraint FK_DIM_ANIO_APLICACION_INI_CIERRE
            references silver.DIM_TIEMPO,
    FECHA_ING             int not null
        constraint FK_DIM_ANIO_APLICACION_FECHA_ING
            references silver.DIM_TIEMPO
)
go

create table silver.DIM_TIPO_RECURSO
(
    IdTipoRecurso       int identity
        primary key,
    TIPO_RECURSO        varchar(100) not null,
    TIPO_RECURSO_NOMBRE varchar(100) not null
)
go

create table silver.DIM_UBIGEO
(
    IdUbigeo           int identity
        primary key,
    CODIGODEPARTAMENTO smallint      not null,
    DEPARTAMENTO       varchar(100)  not null,
    CODIGOPROVINCIA    smallint      not null,
    PROVINCIA          varchar(100)  not null,
    CODIGODISTRITO     smallint      not null,
    DISTRITO           varchar(100)  not null
)
go

create table silver.FACT_FORMULARIO_SISMEPRE
(
    IdEjecutora       int           not null
        constraint FK_FACT_FORM_EJECUTORA
            references silver.DIM_EJECUTORA,
    IdAnioAplicacion  int           not null
        constraint FK_FACT_FORM_ANIO
            references silver.DIM_ANIO_APLICACION,
    PERIODO           smallint      not null,
    IdFormulario      int           not null
        constraint FK_FACT_FORM_FORMULARIO
            references silver.DIM_FORMULARIO_SISMEPRE,
    IdPregunta        int           not null
        constraint FK_FACT_FORM_PREGUNTA
            references silver.DIM_PREGUNTA_SISMEPRE,
    RESPUESTA_ID      smallint      not null,
    RESPUESTA_TEXTO   varchar(1000),
    RESPUESTA_DECIMAL numeric(18,2),
    RESPUESTA_ENTERO  int,
    RESPUESTA_FECHA   varchar(100)
)
go

create table silver.FACT_INGRESO
(
    IdTiempo        int            not null
        constraint FK_FACT_ING_TIEMPO
            references silver.DIM_TIEMPO,
    IdNivelGobierno int            not null
        constraint FK_FACT_ING_NIVEL
            references silver.DIM_NIVEL_GOBIERNO,
    IdSector        int            not null
        constraint FK_FACT_ING_SECTOR
            references silver.DIM_SECTOR,
    IdPliego        int            not null
        constraint FK_FACT_ING_PLIEGO
            references silver.DIM_PLIEGO,
    IdEjecutora     int            not null
        constraint FK_FACT_ING_EJECUTORA
            references silver.DIM_EJECUTORA,
    IdUbigeo        int            not null
        constraint FK_FACT_ING_UBIGEO
            references silver.DIM_UBIGEO,
    IdRubro         int            not null
        constraint FK_FACT_ING_RUBRO
            references silver.DIM_RUBRO,
    IdTipoRecurso   int            not null
        constraint FK_FACT_ING_TIPO_RECURSO
            references silver.DIM_TIPO_RECURSO,
    IdGenerica      int            not null
        constraint FK_FACT_ING_GENERICA
            references silver.DIM_GENERICA,
    IdEspecifica    int            not null
        constraint FK_FACT_ING_ESPECIFICA
            references silver.DIM_ESPECIFICA,
    MONTO_PIA       bigint         not null,
    MONTO_PIM       bigint         not null,
    MONTO_RECAUDADO numeric(18, 2) not null
)
go

create table silver.FACT_RENAMU
(
    IdTiempo   int not null
        constraint FK_FACT_RENAMU_TIEMPO
            references silver.DIM_TIEMPO,
    IdUbigeo   int not null
        constraint FK_FACT_RENAMU_UBIGEO
            references silver.DIM_UBIGEO,
    TIPOMUNI   varchar(100) not null,
    IdPregunta int not null
        constraint FK_FACT_RENAMU_PREGUNTA
            references silver.DIM_PREGUNTA_RENAMU
)
go

