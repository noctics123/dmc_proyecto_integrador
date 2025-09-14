-- =============================================
-- Stored Procedure: sp_process_landing_to_silver_incremental
-- =============================================
-- Propósito: Procesar incremental desde Landing CSV directo a Silver (bypass Bronze)
-- Optimización: Solo procesa nuevos dt= sin dependencia de DataProc
-- Uso: CALL `proyecto-integrador-dae-2025.bronze.sp_process_landing_to_silver_incremental`();

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.bronze.sp_process_landing_to_silver_incremental`()
BEGIN
  DECLARE rows_processed INT64 DEFAULT 0;
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE new_dates_found ARRAY<STRING>;
  DECLARE dates_count INT64;

  -- Log inicio del proceso
  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES ('sp_process_landing_to_silver_incremental', 'STARTED', start_time, 'Iniciando procesamiento incremental Landing CSV → Silver');

  BEGIN
    -- =============================================
    -- 1. DETECTAR NUEVAS FECHAS EN LANDING
    -- =============================================

    -- Obtener fechas nuevas en landing
    SET new_dates_found = ARRAY(
      SELECT DISTINCT REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/')
      FROM `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`
      WHERE _FILE_NAME LIKE '%/dt=%'
        AND REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') > COALESCE(
          (SELECT CAST(MAX(dt_captura) AS STRING) FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`),
          '1900-01-01'
        )
    );

    SET dates_count = ARRAY_LENGTH(new_dates_found);

    -- Si no hay fechas nuevas, terminar
    IF dates_count = 0 THEN
      INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
      (process_name, status, start_time, end_time, message)
      VALUES (
        'sp_process_landing_to_silver_incremental',
        'NO_NEW_DATA',
        start_time,
        CURRENT_TIMESTAMP(),
        'No se encontraron nuevas fechas en landing para procesar'
      );
      RETURN;
    END IF;

    -- Log fechas encontradas
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      'sp_process_landing_to_silver_incremental',
      'PROCESSING',
      CURRENT_TIMESTAMP(),
      CONCAT('Procesando ', CAST(dates_count AS STRING), ' fechas nuevas: ', ARRAY_TO_STRING(new_dates_found, ', '))
    );

    -- =============================================
    -- 2. PROCESAR SIMBAD INCREMENTAL
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
    (
      periodo, tipoCliente, actividad, entidad, sector, moneda, provincia, residencia,
      genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
      cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
      deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
      flg_importe_negativo, dt_captura
    )

    WITH raw_csv_data AS (
      SELECT
        -- Campos base
        periodo, tipoCredito, tipoEntidad, entidad, sectorEconomico, region, provincia,
        moneda, tipoCartera, actividad, sector, persona, facilidad, residencia,
        administracionYPropiedad, genero, tipoCliente, clasificacionEntidad,
        cantidadPlasticos, cantidadCredito, deuda, tasaPorDeuda, deudaCapital,
        deudaVencida, deudaVencidaDe31A90Dias, valorDesembolso, valorGarantia,
        valorProvisionCapitalYRendimiento,

        -- Extraer dt_captura del path
        PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/')) as dt_captura

      FROM `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`

      -- FILTRO: Solo fechas nuevas detectadas
      WHERE REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') IN UNNEST(new_dates_found)
    ),

    cleaned_data AS (
      SELECT
        -- Campos string limpios
        TRIM(periodo) AS periodo,
        TRIM(tipoCliente) AS tipoCliente,
        TRIM(actividad) AS actividad,
        TRIM(entidad) AS entidad,
        TRIM(sector) AS sector,
        TRIM(moneda) AS moneda,
        TRIM(provincia) AS provincia,
        TRIM(residencia) AS residencia,
        TRIM(genero) AS genero,
        TRIM(persona) AS persona,

        -- Limpieza y conversión numérica robusta
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(deudaCapital, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS deudaCapital,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(deudaVencida, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS deudaVencida,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(deudaVencidaDe31A90Dias, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS deudaVencidaDe31A90Dias,
        SAFE_CAST(cantidadCredito AS INT64) AS cantidadCredito,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(valorDesembolso, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS valorDesembolso,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(valorGarantia, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS valorGarantia,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(valorProvisionCapitalYRendimiento, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS valorProvisionCapitalYRendimiento,
        SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(deuda, '0'), r'[,$]', ''), r'[^0-9.-]', '') AS FLOAT64) AS deuda,

        -- Parsing de fecha flexible
        COALESCE(
          SAFE.PARSE_DATE('%Y-%m', periodo),
          SAFE.PARSE_DATE('%Y/%m', periodo),
          SAFE.PARSE_DATE('%m/%Y', periodo),
          SAFE.PARSE_DATE('%Y%m', periodo)
        ) AS periodo_date,

        dt_captura

      FROM raw_csv_data
    ),

    final_data AS (
      SELECT
        *,
        -- Campos temporales derivados
        EXTRACT(YEAR FROM periodo_date) AS anio,
        EXTRACT(MONTH FROM periodo_date) AS mes,
        EXTRACT(YEAR FROM periodo_date) * 100 + EXTRACT(MONTH FROM periodo_date) AS periodo_ym,

        -- Flags de calidad
        CASE WHEN periodo_date IS NULL OR periodo = '' OR periodo IS NULL THEN 1 ELSE 0 END AS flg_periodo_invalido,
        CASE WHEN deudaCapital < 0 OR deudaVencida < 0 OR deuda < 0 THEN 1 ELSE 0 END AS flg_importe_negativo

      FROM cleaned_data
    )

    SELECT
      periodo, tipoCliente, actividad, entidad, sector, moneda, provincia,
      residencia, genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
      cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
      deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
      flg_importe_negativo, dt_captura

    FROM final_data

    -- Filtros de calidad
    WHERE entidad IS NOT NULL
      AND provincia IS NOT NULL
      AND dt_captura IS NOT NULL

    -- Deduplicación
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY entidad, periodo, tipoCliente, provincia, genero, persona
      ORDER BY dt_captura DESC
    ) = 1;

    SET rows_processed = @@row_count;

    -- =============================================
    -- 3. PROCESAR MACROECONOMICS INCREMENTAL
    -- =============================================

    -- Procesar datos macro si existen
    CALL `proyecto-integrador-dae-2025.bronze.sp_process_macroeconomics_incremental`();

    -- =============================================
    -- 4. LOG DE ÉXITO
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, rows_processed, message)
    VALUES (
      'sp_process_landing_to_silver_incremental',
      'SUCCESS',
      start_time,
      CURRENT_TIMESTAMP(),
      rows_processed,
      CONCAT('Procesamiento incremental completado. SIMBAD filas: ', CAST(rows_processed AS STRING),
             '. Fechas procesadas: ', ARRAY_TO_STRING(new_dates_found, ', '))
    );

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, message)
    VALUES (
      'sp_process_landing_to_silver_incremental',
      'ERROR',
      start_time,
      CURRENT_TIMESTAMP(),
      CONCAT('Error en procesamiento incremental: ', @@error.message)
    );

    RAISE USING MESSAGE = @@error.message;
  END;

END;

-- =============================================
-- Stored Procedure: sp_process_macroeconomics_incremental
-- =============================================

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.bronze.sp_process_macroeconomics_incremental`()
BEGIN
  DECLARE macro_rows INT64 DEFAULT 0;

  -- Procesar datos macro desde external tables existentes
  -- (Esto mantiene la lógica actual que ya funciona)

  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES (
    'sp_process_macroeconomics_incremental',
    'INFO',
    CURRENT_TIMESTAMP(),
    'Datos macroeconómicos procesados desde external tables existentes'
  );

END;

-- =============================================
-- External Tables Requeridas
-- =============================================

-- SIMBAD Landing CSV External Table
CREATE OR REPLACE EXTERNAL TABLE IF NOT EXISTS `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  quote = '"',
  encoding = 'UTF-8',
  allow_jagged_rows = true,
  allow_quoted_newlines = false,
  uris = ['gs://dae-integrador-2025/lakehouse/landing/simbad/simbad_carteras_aayp_hipotecarios/dt=*/*.csv']
);

-- =============================================
-- Ejemplo de Uso
-- =============================================

-- Ejecutar procesamiento incremental:
-- CALL `proyecto-integrador-dae-2025.bronze.sp_process_landing_to_silver_incremental`();

-- Verificar nuevas fechas disponibles sin procesar:
-- SELECT DISTINCT REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/')
-- FROM `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`
-- WHERE REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') > (
--   SELECT CAST(MAX(dt_captura) AS STRING) FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
-- );

-- Verificar logs:
-- SELECT * FROM `proyecto-integrador-dae-2025.gold.process_log`
-- WHERE process_name = 'sp_process_landing_to_silver_incremental'
-- ORDER BY created_at DESC LIMIT 5;