CREATE OR ALTER PROCEDURE silver.sp_Silver_Load_Dimensions_Ingresos
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.DIM_EJECUTORA (SEC_EJEC, EJECUTORA, EJECUTORA_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(INT, b.SEC_EJEC),
                    ISNULL(TRY_CONVERT(INT, b.EJECUTORA), TRY_CONVERT(INT, b.SEC_EJEC)),
                    CAST(b.EJECUTORA_NOMBRE AS VARCHAR(200))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_EJECUTORA d ON d.SEC_EJEC = TRY_CONVERT(INT, b.SEC_EJEC)
    WHERE TRY_CONVERT(INT, b.SEC_EJEC) IS NOT NULL AND d.SEC_EJEC IS NULL;

    INSERT INTO silver.DIM_ESPECIFICA (ESPECIFICA, ESPECIFICA_NOMBRE, ESPECIFICA_DET, ESPECIFICA_DET_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, b.ESPECIFICA),
                    CAST(b.ESPECIFICA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, b.ESPECIFICA_DET),
                    CAST(b.ESPECIFICA_DET_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_ESPECIFICA d ON d.ESPECIFICA = TRY_CONVERT(SMALLINT, b.ESPECIFICA)
                                      AND d.ESPECIFICA_DET = TRY_CONVERT(SMALLINT, b.ESPECIFICA_DET)
    WHERE TRY_CONVERT(SMALLINT, b.ESPECIFICA) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, b.ESPECIFICA_DET) IS NOT NULL AND d.ESPECIFICA IS NULL;

    INSERT INTO silver.DIM_FUENTE_FINANCIAMIENTO (FUENTE_FINANCIAMIENTO, FUENTE_FINANCIAMIENTO_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, b.FUENTE_FINANCIAMIENTO),
                    CAST(b.FUENTE_FINANCIAMIENTO_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_FUENTE_FINANCIAMIENTO d ON d.FUENTE_FINANCIAMIENTO = TRY_CONVERT(SMALLINT, b.FUENTE_FINANCIAMIENTO)
    WHERE TRY_CONVERT(SMALLINT, b.FUENTE_FINANCIAMIENTO) IS NOT NULL AND d.FUENTE_FINANCIAMIENTO IS NULL;

    INSERT INTO silver.DIM_GENERICA
    (GENERICA, GENERICA_NOMBRE, SUBGENERICA, SUBGENERICA_NOMBRE, SUBGENERICA_DET, SUBGENERICA_DET_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, b.GENERICA),
                    CAST(b.GENERICA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, b.SUBGENERICA),
                    CAST(b.SUBGENERICA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, b.SUBGENERICA_DET),
                    CAST(b.SUBGENERICA_DET_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_GENERICA d ON d.GENERICA = TRY_CONVERT(SMALLINT, b.GENERICA)
                                    AND d.SUBGENERICA = TRY_CONVERT(SMALLINT, b.SUBGENERICA)
                                    AND d.SUBGENERICA_DET = TRY_CONVERT(SMALLINT, b.SUBGENERICA_DET)
    WHERE TRY_CONVERT(SMALLINT, b.GENERICA) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, b.SUBGENERICA) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, b.SUBGENERICA_DET) IS NOT NULL AND d.GENERICA IS NULL;

    INSERT INTO silver.DIM_NIVEL_GOBIERNO (NIVEL_GOBIERNO, NIVEL_GOBIERNO_NOMBRE)
    SELECT DISTINCT CAST(b.NIVEL_GOBIERNO AS VARCHAR(100)),
                    CAST(b.NIVEL_GOBIERNO_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_NIVEL_GOBIERNO d ON d.NIVEL_GOBIERNO = CAST(b.NIVEL_GOBIERNO AS VARCHAR(100))
    WHERE b.NIVEL_GOBIERNO IS NOT NULL AND b.NIVEL_GOBIERNO_NOMBRE IS NOT NULL AND d.NIVEL_GOBIERNO IS NULL;

    INSERT INTO silver.DIM_PLIEGO (PLIEGO, PLIEGO_NOMBRE)
    SELECT DISTINCT CAST(b.PLIEGO AS VARCHAR(100)),
                    CAST(b.PLIEGO_NOMBRE AS VARCHAR(200))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_PLIEGO d ON d.PLIEGO = CAST(b.PLIEGO AS VARCHAR(100))
    WHERE b.PLIEGO IS NOT NULL AND b.PLIEGO_NOMBRE IS NOT NULL AND d.PLIEGO IS NULL;

    INSERT INTO silver.DIM_RUBRO (RUBRO, RUBRO_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, b.RUBRO),
                    CAST(b.RUBRO_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_RUBRO d ON d.RUBRO = TRY_CONVERT(SMALLINT, b.RUBRO)
    WHERE TRY_CONVERT(SMALLINT, b.RUBRO) IS NOT NULL AND b.RUBRO_NOMBRE IS NOT NULL AND d.RUBRO IS NULL;

    INSERT INTO silver.DIM_SECTOR (SECTOR, SECTOR_NOMBRE)
    SELECT DISTINCT CAST(b.SECTOR AS VARCHAR(100)),
                    CAST(b.SECTOR_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_SECTOR d ON d.SECTOR = CAST(b.SECTOR AS VARCHAR(100))
    WHERE b.SECTOR IS NOT NULL AND b.SECTOR_NOMBRE IS NOT NULL AND d.SECTOR IS NULL;

    INSERT INTO silver.DIM_TIPO_RECURSO (TIPO_RECURSO, TIPO_RECURSO_NOMBRE)
    SELECT DISTINCT CAST(b.TIPO_RECURSO AS VARCHAR(100)),
                    CAST(b.TIPO_RECURSO_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_TIPO_RECURSO d ON d.TIPO_RECURSO = CAST(b.TIPO_RECURSO AS VARCHAR(100))
    WHERE b.TIPO_RECURSO IS NOT NULL AND b.TIPO_RECURSO_NOMBRE IS NOT NULL AND d.TIPO_RECURSO IS NULL;

    INSERT INTO silver.DIM_UBIGEO
    (CODIGODEPARTAMENTO, DEPARTAMENTO, CODIGOPROVINCIA, PROVINCIA, CODIGODISTRITO, DISTRITO)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, b.DEPARTAMENTO_EJECUTORA),
                    CAST(b.DEPARTAMENTO_EJECUTORA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, b.PROVINCIA_EJECUTORA),
                    CAST(b.PROVINCIA_EJECUTORA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, b.DISTRITO_EJECUTORA),
                    CAST(b.DISTRITO_EJECUTORA_NOMBRE AS VARCHAR(100))
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_UBIGEO d ON d.CODIGODEPARTAMENTO = TRY_CONVERT(SMALLINT, b.DEPARTAMENTO_EJECUTORA)
                                  AND d.CODIGOPROVINCIA = TRY_CONVERT(SMALLINT, b.PROVINCIA_EJECUTORA)
                                  AND d.CODIGODISTRITO = TRY_CONVERT(SMALLINT, b.DISTRITO_EJECUTORA)
    WHERE TRY_CONVERT(SMALLINT, b.DEPARTAMENTO_EJECUTORA) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, b.PROVINCIA_EJECUTORA) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, b.DISTRITO_EJECUTORA) IS NOT NULL AND d.CODIGODEPARTAMENTO IS NULL;

    INSERT INTO silver.DIM_TIEMPO (IdTiempo, ANIO, MES, DIA, HORA, MINUTO, SEGUNDO)
    SELECT DISTINCT TRY_CONVERT(INT, b.ANO_DOC) * 100 + TRY_CONVERT(INT, b.MES_DOC),
                    TRY_CONVERT(SMALLINT, b.ANO_DOC),
                    TRY_CONVERT(SMALLINT, b.MES_DOC),
                    NULL, NULL, NULL, NULL
    FROM bronze.ingreso_raw_unified b
    LEFT JOIN silver.DIM_TIEMPO d ON d.IdTiempo = TRY_CONVERT(INT, b.ANO_DOC) * 100 + TRY_CONVERT(INT, b.MES_DOC)
    WHERE TRY_CONVERT(INT, b.ANO_DOC) IS NOT NULL
      AND TRY_CONVERT(INT, b.MES_DOC) IS NOT NULL AND d.IdTiempo IS NULL;
END;
go

CREATE OR ALTER PROCEDURE silver.sp_Silver_Load_Dimensions_Sismepre
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM silver.DIM_TIEMPO WHERE IdTiempo = 0)
        BEGIN
            INSERT INTO silver.DIM_TIEMPO (IdTiempo, ANIO, MES, DIA, HORA, MINUTO, SEGUNDO)
            VALUES (0, 0, NULL, NULL, NULL, NULL, NULL);
        END;

    INSERT INTO silver.DIM_TIEMPO (IdTiempo, ANIO, MES, DIA, HORA, MINUTO, SEGUNDO)
    SELECT DISTINCT v.Anio,
                    TRY_CONVERT(SMALLINT, v.Anio),
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL
    FROM bronze.rentas_anio_aplicacion_raw b
             CROSS APPLY (VALUES (TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION))),
                                 (TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION_INICIO))),
                                 (TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION_FIN)))) v(Anio)
    WHERE v.Anio IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM silver.DIM_TIEMPO d WHERE d.IdTiempo = v.Anio);

    INSERT INTO silver.DIM_ANIO_APLICACION
    (ANO_APLICACION, ANO_APLICACION_INICIO, ANO_APLICACION_FIN, FECHA_CIERRE, FECHA_PRES_OFICIO, FECHA_INI_CIERRE,
     FECHA_ING)
    SELECT DISTINCT ISNULL(TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION)), 0),
                    ISNULL(TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION_INICIO)), 0),
                    ISNULL(TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION_FIN)), 0), 0, 0, 0, 0
    FROM bronze.rentas_anio_aplicacion_raw b
    LEFT JOIN silver.DIM_ANIO_APLICACION d ON d.ANO_APLICACION = TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION))
    WHERE TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.ANO_APLICACION)) IS NOT NULL AND d.ANO_APLICACION IS NULL;

    INSERT INTO silver.DIM_FORMULARIO_SISMEPRE
    (FORMULARIO_ID, TITULO, SUBTITULO, ABREVIATURA, CLASIFICACION, TIPO_FORMULARIO)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.FORMULARIO_ID)),
                    CAST(b.TITULO AS VARCHAR(100)),
                    CAST(b.SUB_TITULO AS VARCHAR(200)),
                    CAST(b.ABREVIATURA AS VARCHAR(100)),
                    CAST(b.CLASIFICACION AS VARCHAR(100)),
                    CAST(b.TIPO_FORMULARIO AS VARCHAR(100))
    FROM bronze.rentas_formulario_raw b
    LEFT JOIN silver.DIM_FORMULARIO_SISMEPRE d ON d.FORMULARIO_ID = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.FORMULARIO_ID))
    WHERE TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.FORMULARIO_ID)) IS NOT NULL AND d.FORMULARIO_ID IS NULL;

    INSERT INTO silver.DIM_PREGUNTA_SISMEPRE
    (IdFormSismepre, PREGUNTA_ID, PREGUNTA_PADRE_ID, DESCRIPCION, TIPO_CUESTIONARIO_ID)
    SELECT DISTINCT f.IdFormSismepre,
                    TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.PREGUNTA_ID)),
                    ISNULL(TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.PREGUNTA_PADRE_ID)), 0),
                    CAST(p.DESCRIPCION AS VARCHAR(400)),
                    TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.TIPO_CUESTIONARIO_ID))
    FROM bronze.rentas_preguntas_raw p
    INNER JOIN silver.DIM_FORMULARIO_SISMEPRE f ON f.FORMULARIO_ID = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.FORMULARIO_ID))
    LEFT JOIN silver.DIM_PREGUNTA_SISMEPRE d ON d.PREGUNTA_ID = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.PREGUNTA_ID))
                                            AND d.IdFormSismepre = f.IdFormSismepre
    WHERE TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.PREGUNTA_ID)) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, p.TIPO_CUESTIONARIO_ID)) IS NOT NULL
      AND p.DESCRIPCION IS NOT NULL AND d.PREGUNTA_ID IS NULL;

    INSERT INTO silver.DIM_EJECUTORA (SEC_EJEC, EJECUTORA, EJECUTORA_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)),
                    TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)),
                    CAST(b.MUNICIPALIDAD_NOMBRE AS VARCHAR(200))
    FROM bronze.rentas_esat_estadistica_atm_raw b
    LEFT JOIN silver.DIM_EJECUTORA d ON d.SEC_EJEC = TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC))
    WHERE TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)) IS NOT NULL AND b.MUNICIPALIDAD_NOMBRE IS NOT NULL AND d.SEC_EJEC IS NULL;

    INSERT INTO silver.DIM_EJECUTORA (SEC_EJEC, EJECUTORA, EJECUTORA_NOMBRE)
    SELECT DISTINCT TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)),
                    TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)),
                    CAST(b.SEC_EJEC AS VARCHAR(200))
    FROM bronze.rentas_respuestas_raw b
    LEFT JOIN silver.DIM_EJECUTORA d ON d.SEC_EJEC = TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC))
    WHERE TRY_CONVERT(INT, TRY_CONVERT(FLOAT, b.SEC_EJEC)) IS NOT NULL AND d.SEC_EJEC IS NULL;

    INSERT INTO silver.DIM_UBIGEO
    (CODIGODEPARTAMENTO, DEPARTAMENTO, CODIGOPROVINCIA, PROVINCIA, CODIGODISTRITO, DISTRITO)
    SELECT DISTINCT TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DEPARTAMENTO)),
                    CAST(b.DEPARTAMENTO_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.PROVINCIA)),
                    CAST(b.PROVINCIA_NOMBRE AS VARCHAR(100)),
                    TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DISTRITO)),
                    CAST(b.DISTRITO_NOMBRE AS VARCHAR(100))
    FROM bronze.rentas_esat_estadistica_atm_raw b
    LEFT JOIN silver.DIM_UBIGEO d ON d.CODIGODEPARTAMENTO = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DEPARTAMENTO))
                                  AND d.CODIGOPROVINCIA = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.PROVINCIA))
                                  AND d.CODIGODISTRITO = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DISTRITO))
    WHERE TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DEPARTAMENTO)) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.PROVINCIA)) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, b.DISTRITO)) IS NOT NULL AND d.CODIGODEPARTAMENTO IS NULL;
END;
go

CREATE OR ALTER PROCEDURE silver.sp_Silver_Load_Fact_Formulario_Sismepre
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM silver.FACT_FORMULARIO_SISMEPRE;
    ;
    WITH dim_ejecutora AS (SELECT IdEjecutora, SEC_EJEC
                           FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY SEC_EJEC ORDER BY IdEjecutora) AS rn
                                 FROM silver.DIM_EJECUTORA) x
                           WHERE rn = 1),
         dim_anio AS (SELECT IdAnioAplicacion, ANO_APLICACION
                      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ANO_APLICACION ORDER BY IdAnioAplicacion) AS rn
                            FROM silver.DIM_ANIO_APLICACION) x
                      WHERE rn = 1),
         dim_formulario AS (SELECT IdFormSismepre, FORMULARIO_ID
                            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY FORMULARIO_ID ORDER BY IdFormSismepre) AS rn
                                  FROM silver.DIM_FORMULARIO_SISMEPRE) x
                            WHERE rn = 1),
         dim_pregunta AS (SELECT IdPreguntaSismepre, IdFormSismepre, PREGUNTA_ID
                          FROM (SELECT *,
                                       ROW_NUMBER() OVER (
                                           PARTITION BY IdFormSismepre, PREGUNTA_ID
                                           ORDER BY IdPreguntaSismepre
                                           ) AS rn
                                FROM silver.DIM_PREGUNTA_SISMEPRE) x
                          WHERE rn = 1)
    INSERT
    INTO silver.FACT_FORMULARIO_SISMEPRE
    (IdEjecutora,
     IdAnioAplicacion,
     PERIODO,
     IdFormulario,
     IdPregunta,
     RESPUESTA_ID,
     RESPUESTA_TEXTO,
     RESPUESTA_DECIMAL,
     RESPUESTA_ENTERO,
     RESPUESTA_FECHA)
    SELECT e.IdEjecutora,
           aa.IdAnioAplicacion,
           TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.PERIODO)),
           f.IdFormSismepre,
           p.IdPreguntaSismepre,
           TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.RESPUESTA_ID)),
           CAST(r.RESPUESTA_TEXTO AS VARCHAR(1000)),
           TRY_CONVERT(NUMERIC(18, 2), r.RESPUESTA_DECIMAL),
           TRY_CONVERT(INT, TRY_CONVERT(FLOAT, r.RESPUESTA_ENTERO)),
           CAST(r.RESPUESTA_FECHA AS VARCHAR(100))
    FROM bronze.rentas_respuestas_raw r
             INNER JOIN dim_ejecutora e
                        ON e.SEC_EJEC = TRY_CONVERT(INT, TRY_CONVERT(FLOAT, r.SEC_EJEC))
             INNER JOIN dim_anio aa
                        ON aa.ANO_APLICACION = TRY_CONVERT(INT, TRY_CONVERT(FLOAT, r.ANO_APLICACION))
             INNER JOIN dim_formulario f
                        ON f.FORMULARIO_ID = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.FORMULARIO_ID))
             INNER JOIN dim_pregunta p
                        ON p.PREGUNTA_ID = TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.PREGUNTA_ID))
                            AND p.IdFormSismepre = f.IdFormSismepre
    WHERE TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.PERIODO)) IS NOT NULL
      AND TRY_CONVERT(SMALLINT, TRY_CONVERT(FLOAT, r.RESPUESTA_ID)) IS NOT NULL;
END;
go

CREATE OR ALTER PROCEDURE silver.sp_Silver_Load_Fact_Ingreso
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM silver.FACT_INGRESO;
    ;
    WITH dim_tiempo AS (SELECT IdTiempo
                        FROM silver.DIM_TIEMPO),
         dim_nivel AS (SELECT IdNivelGobierno, NIVEL_GOBIERNO
                       FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY NIVEL_GOBIERNO ORDER BY IdNivelGobierno) AS rn
                             FROM silver.DIM_NIVEL_GOBIERNO) x
                       WHERE rn = 1),
         dim_sector AS (SELECT IdSector, SECTOR
                        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY SECTOR ORDER BY IdSector) AS rn
                              FROM silver.DIM_SECTOR) x
                        WHERE rn = 1),
         dim_pliego AS (SELECT IdPliego, PLIEGO
                        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY PLIEGO ORDER BY IdPliego) AS rn
                              FROM silver.DIM_PLIEGO) x
                        WHERE rn = 1),
         dim_ejecutora AS (SELECT IdEjecutora, SEC_EJEC
                           FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY SEC_EJEC ORDER BY IdEjecutora) AS rn
                                 FROM silver.DIM_EJECUTORA) x
                           WHERE rn = 1),
         dim_ubigeo AS (SELECT IdUbigeo, CODIGODEPARTAMENTO, CODIGOPROVINCIA, CODIGODISTRITO
                        FROM (SELECT *,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY CODIGODEPARTAMENTO, CODIGOPROVINCIA, CODIGODISTRITO
                                         ORDER BY IdUbigeo
                                         ) AS rn
                              FROM silver.DIM_UBIGEO) x
                        WHERE rn = 1),
         dim_rubro AS (SELECT IdRubro, RUBRO
                       FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY RUBRO ORDER BY IdRubro) AS rn
                             FROM silver.DIM_RUBRO) x
                       WHERE rn = 1),
         dim_tipo_recurso AS (SELECT IdTipoRecurso, TIPO_RECURSO
                              FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY TIPO_RECURSO ORDER BY IdTipoRecurso) AS rn
                                    FROM silver.DIM_TIPO_RECURSO) x
                              WHERE rn = 1),
         dim_generica AS (SELECT IdGenerica,
                                 GENERICA,
                                 SUBGENERICA,
                                 SUBGENERICA_DET
                          FROM (SELECT *,
                                       ROW_NUMBER() OVER (
                                           PARTITION BY GENERICA, SUBGENERICA, SUBGENERICA_DET
                                           ORDER BY IdGenerica
                                           ) AS rn
                                FROM silver.DIM_GENERICA) x
                          WHERE rn = 1),
         dim_especifica AS (SELECT IdEspecifica,
                                   ESPECIFICA,
                                   ESPECIFICA_DET
                            FROM (SELECT *,
                                         ROW_NUMBER() OVER (
                                             PARTITION BY ESPECIFICA, ESPECIFICA_DET
                                             ORDER BY IdEspecifica
                                             ) AS rn
                                  FROM silver.DIM_ESPECIFICA) x
                            WHERE rn = 1)
    INSERT
    INTO silver.FACT_INGRESO
    (IdTiempo,
     IdNivelGobierno,
     IdSector,
     IdPliego,
     IdEjecutora,
     IdUbigeo,
     IdRubro,
     IdTipoRecurso,
     IdGenerica,
     IdEspecifica,
     MONTO_PIA,
     MONTO_PIM,
     MONTO_RECAUDADO)
    SELECT t.IdTiempo,
           ng.IdNivelGobierno,
           s.IdSector,
           p.IdPliego,
           e.IdEjecutora,
           u.IdUbigeo,
           r.IdRubro,
           tr.IdTipoRecurso,
           g.IdGenerica,
           esp.IdEspecifica,
           ISNULL(TRY_CONVERT(BIGINT, b.MONTO_PIA), 0),
           ISNULL(TRY_CONVERT(BIGINT, b.MONTO_PIM), 0),
           ISNULL(TRY_CONVERT(NUMERIC(18, 2), b.MONTO_RECAUDADO), 0)
    FROM bronze.ingreso_raw_unified b
             INNER JOIN dim_tiempo t
                        ON t.IdTiempo = TRY_CONVERT(INT, b.ANO_DOC) * 100 + TRY_CONVERT(INT, b.MES_DOC)
             INNER JOIN dim_nivel ng
                        ON ng.NIVEL_GOBIERNO = CAST(b.NIVEL_GOBIERNO AS VARCHAR(100))
             INNER JOIN dim_sector s
                        ON s.SECTOR = CAST(b.SECTOR AS VARCHAR(100))
             INNER JOIN dim_pliego p
                        ON p.PLIEGO = CAST(b.PLIEGO AS VARCHAR(100))
             INNER JOIN dim_ejecutora e
                        ON e.SEC_EJEC = TRY_CONVERT(INT, b.SEC_EJEC)
             INNER JOIN dim_ubigeo u
                        ON u.CODIGODEPARTAMENTO = TRY_CONVERT(SMALLINT, b.DEPARTAMENTO_EJECUTORA)
                            AND u.CODIGOPROVINCIA = TRY_CONVERT(SMALLINT, b.PROVINCIA_EJECUTORA)
                            AND u.CODIGODISTRITO = TRY_CONVERT(SMALLINT, b.DISTRITO_EJECUTORA)
             INNER JOIN dim_rubro r
                        ON r.RUBRO = TRY_CONVERT(SMALLINT, b.RUBRO)
             INNER JOIN dim_tipo_recurso tr
                        ON tr.TIPO_RECURSO = CAST(b.TIPO_RECURSO AS VARCHAR(100))
             INNER JOIN dim_generica g
                        ON g.GENERICA = TRY_CONVERT(SMALLINT, b.GENERICA)
                            AND g.SUBGENERICA = TRY_CONVERT(SMALLINT, b.SUBGENERICA)
                            AND g.SUBGENERICA_DET = TRY_CONVERT(SMALLINT, b.SUBGENERICA_DET)
             INNER JOIN dim_especifica esp
                        ON esp.ESPECIFICA = TRY_CONVERT(SMALLINT, b.ESPECIFICA)
                            AND esp.ESPECIFICA_DET = TRY_CONVERT(SMALLINT, b.ESPECIFICA_DET);
END;
go

CREATE OR ALTER PROCEDURE silver.sp_Silver_Load_Renamu @Module char(2)
AS
BEGIN
    SET NOCOUNT ON;
    IF @Module NOT LIKE '[0-9][0-9]'
        BEGIN
            RAISERROR ('Invalid module format. Use 2-digit values like 01, 02...', 16, 1);
        END;
    DECLARE @tableName NVARCHAR(200);
    SET @tableName = QUOTENAME('bronze') + '.' +
                     QUOTENAME('renamu_municipalidades_raw_mod' + @Module);
    DECLARE @sqlUbigeo NVARCHAR(MAX);
    SET @sqlUbigeo = '
        INSERT INTO silver.DIM_UBIGEO
        (CODIGODEPARTAMENTO, DEPARTAMENTO, CODIGOPROVINCIA, PROVINCIA, CODIGODISTRITO, DISTRITO)
        SELECT DISTINCT
            TRY_CONVERT(SMALLINT, ccdd),
            CAST(Departamento AS VARCHAR(100)),
            TRY_CONVERT(SMALLINT, ccpp),
            CAST(Provincia AS VARCHAR(100)),
            TRY_CONVERT(SMALLINT, ccdi),
            CAST(Distrito AS VARCHAR(100))
        FROM ' + @tableName + ' r
        WHERE TRY_CONVERT(SMALLINT, ccdd) IS NOT NULL
          AND TRY_CONVERT(SMALLINT, ccpp) IS NOT NULL
          AND TRY_CONVERT(SMALLINT, ccdi) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM silver.DIM_UBIGEO u
              WHERE u.CODIGODEPARTAMENTO = TRY_CONVERT(SMALLINT, r.ccdd)
                AND u.CODIGOPROVINCIA = TRY_CONVERT(SMALLINT, r.ccpp)
                AND u.CODIGODISTRITO = TRY_CONVERT(SMALLINT, r.ccdi)
          );';

    EXEC sp_executesql @sqlUbigeo;

    -- Inserta los años del módulo RENAMU en DIM_TIEMPO si no existen,
    -- eliminando la dependencia de que rentas_anio_aplicacion_raw tenga datos.
    DECLARE @sqlTiempo NVARCHAR(MAX);
    SET @sqlTiempo = N'
        INSERT INTO silver.DIM_TIEMPO (IdTiempo, ANIO, MES, DIA, HORA, MINUTO, SEGUNDO)
        SELECT DISTINCT
            TRY_CONVERT(INT, r.año),
            TRY_CONVERT(SMALLINT, r.año),
            NULL, NULL, NULL, NULL, NULL
        FROM ' + @tableName + N' r
        WHERE TRY_CONVERT(INT, r.año) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM silver.DIM_TIEMPO d
              WHERE d.IdTiempo = TRY_CONVERT(INT, r.año)
          );';
    EXEC sp_executesql @sqlTiempo;

    DECLARE @values NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);

    SELECT @values = STRING_AGG(
            CAST('(''' + COLUMN_NAME + ''', CAST(r.' + QUOTENAME(COLUMN_NAME) + ' AS VARCHAR(300)))' AS NVARCHAR(MAX)),
            ','
                     )
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'bronze'
      AND TABLE_NAME = 'renamu_municipalidades_raw_mod' + @Module
      AND LOWER(COLUMN_NAME) NOT IN (
                                     N'año', 'idmunici', 'ccdd', 'ccpp', 'ccdi',
                                     'ubigeo', 'departamento', 'provincia', 'distrito', 'tipomuni'
      );

    IF @values IS NULL
        BEGIN
            PRINT 'sp_Silver_Load_Renamu: módulo ' + @Module + ' sin tabla o sin columnas — omitiendo.';
            RETURN;
        END;

    SET @sql = N'
        INSERT INTO silver.DIM_PREGUNTA_RENAMU
        (IdPadre, NOMBRE_CAMPO, DESCRIPCION, VALOR, METADATA)
        SELECT DISTINCT
            NULL,
            CAST(v.NOMBRE_CAMPO AS VARCHAR(100)),
            CAST(v.NOMBRE_CAMPO AS VARCHAR(100)),
            ISNULL(v.VALOR, ''''),
            ''''
        FROM ' + @tableName + ' r
        CROSS APPLY (VALUES ' + @values + N') v(NOMBRE_CAMPO, VALOR)
        WHERE v.NOMBRE_CAMPO IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM silver.DIM_PREGUNTA_RENAMU d
              WHERE d.NOMBRE_CAMPO = v.NOMBRE_CAMPO
                AND d.VALOR = ISNULL(v.VALOR, '''')
                AND d.METADATA = ''''
          );

        INSERT INTO silver.FACT_RENAMU
        (IdTiempo, IdUbigeo, TIPOMUNI, IdPregunta)
        SELECT
            TRY_CONVERT(INT, r.año),
            u.IdUbigeo,
            CAST(ISNULL(r.Tipomuni, '''') AS VARCHAR(100)),
            d.IdPregunta
        FROM ' + @tableName + ' r
        INNER JOIN silver.DIM_UBIGEO u
            ON u.CODIGODEPARTAMENTO = TRY_CONVERT(SMALLINT, r.ccdd)
           AND u.CODIGOPROVINCIA = TRY_CONVERT(SMALLINT, r.ccpp)
           AND u.CODIGODISTRITO = TRY_CONVERT(SMALLINT, r.ccdi)
        CROSS APPLY (VALUES ' + @values + N') v(NOMBRE_CAMPO, VALOR)
        INNER JOIN silver.DIM_PREGUNTA_RENAMU d
            ON d.NOMBRE_CAMPO = v.NOMBRE_CAMPO
           AND d.VALOR = ISNULL(v.VALOR, '''')
           AND d.METADATA = ''''
        WHERE TRY_CONVERT(INT, r.año) IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM silver.FACT_RENAMU f
            WHERE f.IdTiempo = TRY_CONVERT(INT, r.año)
              AND f.IdUbigeo = u.IdUbigeo
              AND f.IdPregunta = d.IdPregunta
        );
    ';

    EXEC sp_executesql @sql;
END;
go

CREATE OR ALTER PROCEDURE silver.sp_Load_Silver_All
AS
BEGIN
    SET NOCOUNT ON;

    EXEC silver.sp_Silver_Load_Dimensions_Ingresos;
    EXEC silver.sp_Silver_Load_Dimensions_Sismepre;
    EXEC silver.sp_Silver_Load_Fact_Ingreso;
    EXEC silver.sp_Silver_Load_Fact_Formulario_Sismepre;
    DECLARE @i INT = 1;
    DECLARE @Module CHAR(2);

    WHILE @i <= 13
        BEGIN
            -- Format as 2 digits (01, 02, ..., 13)
            SET @Module = RIGHT('00' + CAST(@i AS VARCHAR(2)), 2);

            PRINT 'Executing module ' + @Module;

            EXEC silver.sp_Silver_Load_Renamu @Module = @Module;

            SET @i = @i + 1;
        END;
END;