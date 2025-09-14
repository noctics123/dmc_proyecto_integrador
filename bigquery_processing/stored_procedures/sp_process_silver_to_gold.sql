-- =============================================
-- Stored Procedure: sp_process_silver_to_gold
-- =============================================
-- Propósito: Procesar métricas desde Silver hacia Gold con datos macroeconómicos
-- Patrón: Incremental - solo períodos nuevos/actualizados
-- Uso: CALL `proyecto-integrador-dae-2025.silver_clean.sp_process_silver_to_gold`();

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.silver_clean.sp_process_silver_to_gold`()
BEGIN
  DECLARE rows_affected INT64 DEFAULT 0;
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE min_periodo_processed DATE;

  -- Log inicio del proceso
  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES ('sp_process_silver_to_gold', 'STARTED', start_time, 'Iniciando procesamiento Silver → Gold');

  BEGIN
    -- =============================================
    -- 1. DETERMINAR RANGO DE PERÍODOS A PROCESAR
    -- =============================================

    -- Procesar últimos 2 meses para capturar actualizaciones
    SET min_periodo_processed = COALESCE(
      (SELECT DATE_SUB(MAX(periodo_date), INTERVAL 2 MONTH)
       FROM `proyecto-integrador-dae-2025.gold.simbad_gold`),
      '2012-01-01'
    );

    -- =============================================
    -- 2. MERGE SILVER → GOLD CON MÉTRICAS
    -- =============================================

    MERGE `proyecto-integrador-dae-2025.gold.simbad_gold` AS target
    USING (

      WITH simbad_aggregated AS (
        -- Agregaciones principales por provincia/período
        SELECT
          periodo_date,
          EXTRACT(YEAR FROM periodo_date) AS ANIO,
          EXTRACT(MONTH FROM periodo_date) AS MES,
          provincia AS PROVINCIA,

          -- Métricas financieras sumadas
          SUM(COALESCE(deudaCapital, 0)) AS DEUDACAPITAL,
          SUM(COALESCE(deudaVencida, 0)) AS DEUDAVENCIDA,
          SUM(COALESCE(deudaVencidaDe31A90Dias, 0)) AS DEUDAVENCIDADE31A90DIAS,
          SUM(COALESCE(cantidadCredito, 0)) AS CANTIDADCREDITO,
          SUM(COALESCE(valorDesembolso, 0)) AS VALORDESEMBOLSO,
          SUM(COALESCE(valorGarantia, 0)) AS VALORGARANTIA,
          SUM(COALESCE(valorProvisionCapitalYRendimiento, 0)) AS VALORPROVISIONCAPITALYRENDIMIENTO,
          SUM(COALESCE(deuda, 0)) AS DEUDA,

          -- Conteos únicos por dimensión
          COUNT(DISTINCT genero) AS GENERO,
          COUNT(DISTINCT persona) AS PERSONA,
          COUNT(DISTINCT moneda) AS MONEDA,
          COUNT(DISTINCT sector) AS SECTOR,
          COUNT(DISTINCT entidad) AS ENTIDAD,
          COUNT(DISTINCT residencia) AS RESIDENCIA,

          -- Indicador de agregación
          1 AS PD_AGREGADA

        FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`

        -- FILTRO INCREMENTAL
        WHERE periodo_date >= min_periodo_processed
          AND flg_periodo_invalido = 0
          AND provincia IS NOT NULL
          AND periodo_date IS NOT NULL

        GROUP BY periodo_date, provincia
      ),

      -- Datos macroeconómicos - Inflación
      macro_inflacion AS (
        SELECT
          DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH) AS periodo_date,
          AVG(SAFE_CAST(inflacion_anual AS FLOAT64)) AS inflacion

        FROM `proyecto-integrador-dae-2025.bronze.bronze_inflacion_12m_ext`
        WHERE SAFE.PARSE_DATE('%Y-%m-%d', fecha) >= DATE_SUB(min_periodo_processed, INTERVAL 1 MONTH)

        GROUP BY DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH)
      ),

      -- Datos macroeconómicos - Tipo de Cambio
      macro_tipo_cambio AS (
        SELECT
          DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH) AS periodo_date,
          AVG(CASE WHEN tipo_cambio = 'VENTA' THEN SAFE_CAST(tasa_cambio AS FLOAT64) END) AS tc_venta,
          AVG(CASE WHEN tipo_cambio = 'COMPRA' THEN SAFE_CAST(tasa_cambio AS FLOAT64) END) AS tc_compra

        FROM `proyecto-integrador-dae-2025.bronze.bronze_tipo_cambio_ext`
        WHERE SAFE.PARSE_DATE('%Y-%m-%d', fecha) >= DATE_SUB(min_periodo_processed, INTERVAL 1 MONTH)

        GROUP BY DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH)
      ),

      -- Métricas finales combinadas
      final_metrics AS (
        SELECT
          s.*,

          -- KPIs calculados
          CASE
            WHEN s.DEUDACAPITAL > 0 THEN s.DEUDAVENCIDA / s.DEUDACAPITAL
            ELSE 0
          END AS TASA_MORA,

          CASE
            WHEN s.DEUDACAPITAL > 0 THEN s.VALORGARANTIA / s.DEUDACAPITAL
            ELSE 0
          END AS COBERTURA_GARANTIA,

          CASE
            WHEN s.DEUDACAPITAL > 0 THEN s.VALORPROVISIONCAPITALYRENDIMIENTO / s.DEUDACAPITAL
            ELSE 0
          END AS PROPORCION_PROVISIONADA,

          -- Datos macroeconómicos
          COALESCE(inf.inflacion, 0) AS INFLACION,
          COALESCE(tc.tc_venta, 0) AS TC_VENTA,
          COALESCE(tc.tc_compra, 0) AS TC_COMPRA

        FROM simbad_aggregated s
        LEFT JOIN macro_inflacion inf ON s.periodo_date = inf.periodo_date
        LEFT JOIN macro_tipo_cambio tc ON s.periodo_date = tc.periodo_date
      )

      SELECT * FROM final_metrics

    ) AS source

    -- MERGE conditions
    ON target.periodo_date = source.periodo_date
       AND target.PROVINCIA = source.PROVINCIA

    -- Actualizar registros existentes
    WHEN MATCHED THEN UPDATE SET
      ANIO = source.ANIO,
      MES = source.MES,
      DEUDACAPITAL = source.DEUDACAPITAL,
      DEUDAVENCIDA = source.DEUDAVENCIDA,
      DEUDAVENCIDADE31A90DIAS = source.DEUDAVENCIDADE31A90DIAS,
      CANTIDADCREDITO = source.CANTIDADCREDITO,
      VALORDESEMBOLSO = source.VALORDESEMBOLSO,
      VALORGARANTIA = source.VALORGARANTIA,
      VALORPROVISIONCAPITALYRENDIMIENTO = source.VALORPROVISIONCAPITALYRENDIMIENTO,
      DEUDA = source.DEUDA,
      GENERO = source.GENERO,
      PERSONA = source.PERSONA,
      MONEDA = source.MONEDA,
      SECTOR = source.SECTOR,
      ENTIDAD = source.ENTIDAD,
      RESIDENCIA = source.RESIDENCIA,
      TASA_MORA = source.TASA_MORA,
      COBERTURA_GARANTIA = source.COBERTURA_GARANTIA,
      PROPORCION_PROVISIONADA = source.PROPORCION_PROVISIONADA,
      PD_AGREGADA = source.PD_AGREGADA,
      INFLACION = source.INFLACION,
      TC_VENTA = source.TC_VENTA,
      TC_COMPRA = source.TC_COMPRA

    -- Insertar nuevos registros
    WHEN NOT MATCHED THEN INSERT (
      periodo_date, ANIO, MES, PROVINCIA, DEUDACAPITAL, DEUDAVENCIDA,
      DEUDAVENCIDADE31A90DIAS, CANTIDADCREDITO, VALORDESEMBOLSO, VALORGARANTIA,
      VALORPROVISIONCAPITALYRENDIMIENTO, DEUDA, GENERO, PERSONA, MONEDA, SECTOR,
      ENTIDAD, RESIDENCIA, TASA_MORA, COBERTURA_GARANTIA, PROPORCION_PROVISIONADA,
      PD_AGREGADA, INFLACION, TC_VENTA, TC_COMPRA
    )
    VALUES (
      source.periodo_date, source.ANIO, source.MES, source.PROVINCIA,
      source.DEUDACAPITAL, source.DEUDAVENCIDA, source.DEUDAVENCIDADE31A90DIAS,
      source.CANTIDADCREDITO, source.VALORDESEMBOLSO, source.VALORGARANTIA,
      source.VALORPROVISIONCAPITALYRENDIMIENTO, source.DEUDA, source.GENERO,
      source.PERSONA, source.MONEDA, source.SECTOR, source.ENTIDAD, source.RESIDENCIA,
      source.TASA_MORA, source.COBERTURA_GARANTIA, source.PROPORCION_PROVISIONADA,
      source.PD_AGREGADA, source.INFLACION, source.TC_VENTA, source.TC_COMPRA
    );

    -- Obtener filas afectadas
    SET rows_affected = @@row_count;

    -- =============================================
    -- 3. ACTUALIZAR TABLA DE ALERTAS
    -- =============================================

    -- Regenerar alertas para períodos procesados
    CALL `proyecto-integrador-dae-2025.gold.sp_update_alertas_provincia`(min_periodo_processed);

    -- =============================================
    -- 4. LOG DE RESULTADOS
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, rows_processed, message)
    VALUES (
      'sp_process_silver_to_gold',
      'SUCCESS',
      start_time,
      CURRENT_TIMESTAMP(),
      rows_affected,
      CONCAT('Gold actualizado exitosamente. Filas: ', CAST(rows_affected AS STRING),
             '. Desde período: ', CAST(min_periodo_processed AS STRING))
    );

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, message)
    VALUES (
      'sp_process_silver_to_gold',
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
-- Ejemplo de Uso
-- =============================================

-- Ejecutar proceso incremental:
-- CALL `proyecto-integrador-dae-2025.silver_clean.sp_process_silver_to_gold`();

-- Verificar resultados:
-- SELECT COUNT(*), MIN(periodo_date), MAX(periodo_date)
-- FROM `proyecto-integrador-dae-2025.gold.simbad_gold`;

-- Verificar logs:
-- SELECT * FROM `proyecto-integrador-dae-2025.gold.process_log`
-- WHERE process_name = 'sp_process_silver_to_gold'
-- ORDER BY created_at DESC LIMIT 5;