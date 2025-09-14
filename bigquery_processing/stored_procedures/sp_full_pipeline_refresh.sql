-- =============================================
-- Stored Procedure: sp_full_pipeline_refresh
-- =============================================
-- Propósito: Ejecutar pipeline completo Bronze → Silver → Gold
-- Patrón: Orquestador principal con manejo de errores
-- Uso: CALL `proyecto-integrador-dae-2025.gold.sp_full_pipeline_refresh`();

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.gold.sp_full_pipeline_refresh`()
BEGIN
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE pipeline_id STRING DEFAULT GENERATE_UUID();

  -- Log inicio del pipeline
  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES (
    CONCAT('sp_full_pipeline_refresh_', pipeline_id),
    'STARTED',
    start_time,
    'Iniciando pipeline completo Bronze → Silver → Gold'
  );

  BEGIN
    -- =============================================
    -- 1. PASO 1: BRONZE → SILVER
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      'Ejecutando paso 1: Bronze → Silver'
    );

    -- Ejecutar procesamiento bronze to silver
    CALL `proyecto-integrador-dae-2025.bronze.sp_process_bronze_to_silver`();

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      'Paso 1 completado exitosamente'
    );

    -- =============================================
    -- 2. PASO 2: SILVER → GOLD
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      'Ejecutando paso 2: Silver → Gold'
    );

    -- Ejecutar procesamiento silver to gold
    CALL `proyecto-integrador-dae-2025.silver_clean.sp_process_silver_to_gold`();

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      'Paso 2 completado exitosamente'
    );

    -- =============================================
    -- 3. PASO 3: VALIDACIONES FINALES
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      'Ejecutando validaciones finales'
    );

    -- Ejecutar validaciones de calidad
    CALL `proyecto-integrador-dae-2025.gold.sp_data_quality_checks`();

    -- =============================================
    -- 4. LOG DE ÉXITO
    -- =============================================

    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'SUCCESS',
      start_time,
      CURRENT_TIMESTAMP(),
      CONCAT('Pipeline completo ejecutado exitosamente. Duración: ',
             CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND) AS STRING), ' segundos')
    );

  EXCEPTION WHEN ERROR THEN
    -- Log error del pipeline
    INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
    (process_name, status, start_time, end_time, message)
    VALUES (
      CONCAT('sp_full_pipeline_refresh_', pipeline_id),
      'ERROR',
      start_time,
      CURRENT_TIMESTAMP(),
      CONCAT('Error en pipeline: ', @@error.message)
    );

    -- Re-raise error
    RAISE USING MESSAGE = CONCAT('Pipeline failed: ', @@error.message);
  END;

END;

-- =============================================
-- Stored Procedure: sp_data_quality_checks
-- =============================================
-- Propósito: Validaciones de calidad de datos en todo el pipeline

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.gold.sp_data_quality_checks`()
BEGIN
  DECLARE check_results STRING DEFAULT '';
  DECLARE error_count INT64 DEFAULT 0;

  -- =============================================
  -- 1. VALIDAR SILVER LAYER
  -- =============================================

  -- Check 1: Registros con períodos inválidos
  SET error_count = (
    SELECT COUNT(*)
    FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
    WHERE flg_periodo_invalido = 1
  );

  IF error_count > 0 THEN
    SET check_results = CONCAT(check_results, 'ALERTA: ', CAST(error_count AS STRING), ' registros con períodos inválidos en Silver. ');
  END IF;

  -- Check 2: Registros con importes negativos
  SET error_count = (
    SELECT COUNT(*)
    FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
    WHERE flg_importe_negativo = 1
  );

  IF error_count > 0 THEN
    SET check_results = CONCAT(check_results, 'ALERTA: ', CAST(error_count AS STRING), ' registros con importes negativos en Silver. ');
  END IF;

  -- =============================================
  -- 2. VALIDAR GOLD LAYER
  -- =============================================

  -- Check 3: Consistencia de volúmenes Silver vs Gold
  SET error_count = (
    WITH volume_check AS (
      SELECT
        COUNT(DISTINCT CONCAT(provincia, '_', periodo)) as gold_combinations,
        (SELECT COUNT(DISTINCT CONCAT(provincia, '_', periodo))
         FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
         WHERE provincia IS NOT NULL AND periodo_date IS NOT NULL) as silver_combinations
      FROM `proyecto-integrador-dae-2025.gold.simbad_gold`
    )
    SELECT ABS(gold_combinations - silver_combinations)
    FROM volume_check
  );

  IF error_count > 100 THEN  -- Tolerance of 100 combinations difference
    SET check_results = CONCAT(check_results, 'ALERTA: Diferencia significativa entre Silver y Gold: ', CAST(error_count AS STRING), ' combinaciones. ');
  END IF;

  -- Check 4: Datos macroeconómicos faltantes
  SET error_count = (
    SELECT COUNT(*)
    FROM `proyecto-integrador-dae-2025.gold.simbad_gold`
    WHERE periodo_date >= '2020-01-01'  -- Data should have macro data from 2020
      AND (INFLACION = 0 OR INFLACION IS NULL)
  );

  IF error_count > 0 THEN
    SET check_results = CONCAT(check_results, 'ALERTA: ', CAST(error_count AS STRING), ' registros sin datos de inflación recientes. ');
  END IF;

  -- =============================================
  -- 3. LOG RESULTADOS
  -- =============================================

  IF LENGTH(check_results) = 0 THEN
    SET check_results = 'Todas las validaciones de calidad pasaron exitosamente.';
  END IF;

  INSERT INTO `proyecto-integrador-dae-2025.gold.process_log`
  (process_name, status, start_time, message)
  VALUES (
    'sp_data_quality_checks',
    IF(REGEXP_CONTAINS(check_results, 'ALERTA'), 'WARNING', 'SUCCESS'),
    CURRENT_TIMESTAMP(),
    check_results
  );

END;

-- =============================================
-- Stored Procedure: sp_update_alertas_provincia
-- =============================================
-- Propósito: Actualizar tabla de alertas por provincia

CREATE OR REPLACE PROCEDURE `proyecto-integrador-dae-2025.gold.sp_update_alertas_provincia`(
  IN min_fecha_proceso DATE
)
BEGIN

  -- Limpiar alertas existentes para el rango
  DELETE FROM `proyecto-integrador-dae-2025.gold.alertas_provincia_12m`
  WHERE DATE(CONCAT(CAST(anio AS STRING), '-', LPAD(CAST(mes AS STRING), 2, '0'), '-01')) >= min_fecha_proceso;

  -- Insertar nuevas alertas
  INSERT INTO `proyecto-integrador-dae-2025.gold.alertas_provincia_12m`
  (provincia, anio, mes, tasa_mora_promedio, tendencia_mora, nivel_alerta, entidades_afectadas, fecha_calculo)

  WITH alertas_calculadas AS (
    SELECT
      PROVINCIA as provincia,
      ANIO as anio,
      MES as mes,
      AVG(TASA_MORA) as tasa_mora_promedio,

      -- Calcular tendencia (comparar con mes anterior)
      CASE
        WHEN AVG(TASA_MORA) > LAG(AVG(TASA_MORA)) OVER (PARTITION BY PROVINCIA ORDER BY ANIO, MES) THEN 'SUBIENDO'
        WHEN AVG(TASA_MORA) < LAG(AVG(TASA_MORA)) OVER (PARTITION BY PROVINCIA ORDER BY ANIO, MES) THEN 'BAJANDO'
        ELSE 'ESTABLE'
      END as tendencia_mora,

      -- Calcular nivel de alerta
      CASE
        WHEN AVG(TASA_MORA) > 0.15 THEN 'ALTO'
        WHEN AVG(TASA_MORA) > 0.08 THEN 'MEDIO'
        ELSE 'BAJO'
      END as nivel_alerta,

      COUNT(DISTINCT ENTIDAD) as entidades_afectadas,
      CURRENT_DATE() as fecha_calculo

    FROM `proyecto-integrador-dae-2025.gold.simbad_gold`
    WHERE periodo_date >= min_fecha_proceso
    GROUP BY PROVINCIA, ANIO, MES
  )

  SELECT * FROM alertas_calculadas;

END;

-- =============================================
-- Ejemplos de Uso
-- =============================================

-- Ejecutar pipeline completo:
-- CALL `proyecto-integrador-dae-2025.gold.sp_full_pipeline_refresh`();

-- Ver logs del pipeline:
-- SELECT * FROM `proyecto-integrador-dae-2025.gold.process_log`
-- WHERE process_name LIKE 'sp_full_pipeline_refresh_%'
-- ORDER BY created_at DESC LIMIT 20;

-- Ejecutar solo validaciones:
-- CALL `proyecto-integrador-dae-2025.gold.sp_data_quality_checks`();