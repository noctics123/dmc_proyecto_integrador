-- =============================================
-- Stored Procedure: sp_process_bronze_to_silver
-- =============================================
-- Propósito: Procesar datos desde Bronze (external tables) hacia Silver (cleaned tables)
-- Patrón: Incremental - solo nuevos dt_captura
-- Uso: CALL `proyecto-integrador-dae-2025.bronze.sp_process_bronze_to_silver`();

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.bronze.sp_process_bronze_to_silver`()
BEGIN
  DECLARE rows_processed INT64 DEFAULT 0;
  DECLARE max_dt_captura_processed DATE;
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Log inicio del proceso
  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES ('sp_process_bronze_to_silver', 'STARTED', start_time, 'Iniciando procesamiento Bronze → Silver');

  BEGIN
    -- =============================================
    -- 1. PROCESAR SIMBAD BRONZE → SILVER
    -- =============================================

    -- Obtener último dt_captura procesado
    SET max_dt_captura_processed = (
      SELECT COALESCE(MAX(dt_captura), '1900-01-01')
      FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
    );

    -- Insertar datos limpios desde bronze
    INSERT INTO `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
    (
      periodo, tipoCliente, actividad, entidad, sector, moneda, provincia, residencia,
      genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
      cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
      deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
      flg_importe_negativo, dt_captura
    )

    WITH bronze_data AS (
      SELECT
        -- Campos base con limpieza
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

        -- Campos numéricos con validación
        SAFE_CAST(deudaCapital AS FLOAT64) AS deudaCapital,
        SAFE_CAST(deudaVencida AS FLOAT64) AS deudaVencida,
        SAFE_CAST(deudaVencidaDe31A90Dias AS FLOAT64) AS deudaVencidaDe31A90Dias,
        SAFE_CAST(cantidadCredito AS INT64) AS cantidadCredito,
        SAFE_CAST(valorDesembolso AS FLOAT64) AS valorDesembolso,
        SAFE_CAST(valorGarantia AS FLOAT64) AS valorGarantia,
        SAFE_CAST(valorProvisionCapitalYRendimiento AS FLOAT64) AS valorProvisionCapitalYRendimiento,
        SAFE_CAST(deuda AS FLOAT64) AS deuda,

        -- Derivación de fecha
        SAFE.PARSE_DATE('%Y-%m', periodo) AS periodo_date,
        dt_captura

      FROM `proyecto-integrador-dae-2025.bronze.simbad_bronze_parquet_ext`

      -- FILTRO INCREMENTAL: Solo procesar nuevos dt_captura
      WHERE dt_captura > max_dt_captura_processed
    ),

    cleaned_data AS (
      SELECT
        *,
        -- Derivar campos temporales
        EXTRACT(YEAR FROM periodo_date) AS anio,
        EXTRACT(MONTH FROM periodo_date) AS mes,
        EXTRACT(YEAR FROM periodo_date) * 100 + EXTRACT(MONTH FROM periodo_date) AS periodo_ym,

        -- Flags de calidad
        CASE WHEN periodo_date IS NULL OR periodo = '' THEN 1 ELSE 0 END AS flg_periodo_invalido,
        CASE WHEN deudaCapital < 0 OR deudaVencida < 0 OR deuda < 0 THEN 1 ELSE 0 END AS flg_importe_negativo

      FROM bronze_data
    )

    SELECT
      periodo, tipoCliente, actividad, entidad, sector, moneda, provincia,
      residencia, genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
      cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
      deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
      flg_importe_negativo, dt_captura

    FROM cleaned_data

    -- Deduplicación
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY entidad, periodo, tipoCliente, provincia, genero, persona
      ORDER BY dt_captura DESC
    ) = 1;

    -- Obtener filas procesadas
    SET rows_processed = @@row_count;

    -- =============================================
    -- 2. LOG DE RESULTADOS
    -- =============================================

    -- Log éxito
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, rows_processed, message)
    VALUES (
      'sp_process_bronze_to_silver',
      'SUCCESS',
      start_time,
      CURRENT_TIMESTAMP(),
      rows_processed,
      CONCAT('SIMBAD procesado exitosamente. Filas: ', CAST(rows_processed AS STRING),
             '. Último dt_captura: ', CAST(max_dt_captura_processed AS STRING))
    );

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, message)
    VALUES (
      'sp_process_bronze_to_silver',
      'ERROR',
      start_time,
      CURRENT_TIMESTAMP(),
      CONCAT('Error en procesamiento: ', @@error.message)
    );

    -- Re-raise error
    RAISE USING MESSAGE = @@error.message;
  END;

END;

-- =============================================
-- Tabla de Log (crear si no existe)
-- =============================================

CREATE TABLE IF NOT EXISTS `proyecto-integrador-dae-2025.gold.process_log`
(
  log_id STRING DEFAULT GENERATE_UUID(),
  process_name STRING NOT NULL,
  status STRING NOT NULL,  -- 'STARTED', 'SUCCESS', 'ERROR'
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  rows_processed INT64,
  message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY process_name, status;

-- =============================================
-- Ejemplo de Uso
-- =============================================

-- Ejecutar proceso incremental:
-- CALL `proyecto-integrador-dae-2025.bronze.sp_process_bronze_to_silver`();

-- Verificar logs:
-- SELECT * FROM `proyecto-integrador-dae-2025.gold.process_log`
-- WHERE process_name = 'sp_process_bronze_to_silver'
-- ORDER BY created_at DESC LIMIT 10;

-- Verificar datos procesados:
-- SELECT COUNT(*), MAX(dt_captura), MIN(dt_captura)
-- FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`;