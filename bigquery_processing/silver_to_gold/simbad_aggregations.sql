-- =============================================
-- Silver → Gold: SIMBAD Aggregations & KPIs
-- =============================================
-- Propósito: Crear métricas agregadas por provincia/período con datos macroeconómicos
-- Patrón: Procesar solo períodos nuevos o actualizados (incremental)

-- Actualizar tabla gold con nuevos períodos desde silver
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

    -- FILTRO INCREMENTAL: Solo procesar períodos nuevos o actualizados
    WHERE periodo_date >= COALESCE(
      (SELECT DATE_SUB(MAX(periodo_date), INTERVAL 2 MONTH)
       FROM `proyecto-integrador-dae-2025.gold.simbad_gold`),
      '2012-01-01'
    )

    -- Filtrar solo registros válidos
    AND flg_periodo_invalido = 0
    AND provincia IS NOT NULL
    AND periodo_date IS NOT NULL

    GROUP BY periodo_date, provincia
  ),

  -- Obtener datos macroeconómicos más recientes por período
  macro_data AS (
    SELECT
      DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH) AS periodo_date,

      -- Promediar indicadores del mes
      AVG(SAFE_CAST(inflacion_anual AS FLOAT64)) AS inflacion,
      AVG(SAFE_CAST(tasa_cambio AS FLOAT64)) AS tc_promedio

    FROM `proyecto-integrador-dae-2025.bronze.bronze_inflacion_12m_ext` inf

    -- Solo datos recientes para performance
    WHERE SAFE.PARSE_DATE('%Y-%m-%d', fecha) >= '2020-01-01'

    GROUP BY DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH)
  ),

  tipo_cambio_data AS (
    SELECT
      DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH) AS periodo_date,

      AVG(CASE WHEN tipo_cambio = 'VENTA' THEN SAFE_CAST(tasa_cambio AS FLOAT64) END) AS tc_venta,
      AVG(CASE WHEN tipo_cambio = 'COMPRA' THEN SAFE_CAST(tasa_cambio AS FLOAT64) END) AS tc_compra

    FROM `proyecto-integrador-dae-2025.bronze.bronze_tipo_cambio_ext`

    WHERE SAFE.PARSE_DATE('%Y-%m-%d', fecha) >= '2020-01-01'

    GROUP BY DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-%d', fecha), MONTH)
  ),

  -- Combinar SIMBAD con datos macroeconómicos
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
      COALESCE(m.inflacion, 0) AS INFLACION,
      COALESCE(tc.tc_venta, 0) AS TC_VENTA,
      COALESCE(tc.tc_compra, 0) AS TC_COMPRA

    FROM simbad_aggregated s
    LEFT JOIN macro_data m ON s.periodo_date = m.periodo_date
    LEFT JOIN tipo_cambio_data tc ON s.periodo_date = tc.periodo_date
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

-- =============================================
-- Notas de Optimización
-- =============================================

-- 1. INCREMENTAL PROCESSING:
--    - Procesa solo últimos 2 meses para capturar actualizaciones
--    - MERGE permite INSERT/UPDATE atómico
--    - Filtro por fecha mantiene volumen controlado

-- 2. CALIDAD Y PERFORMANCE:
--    - LEFT JOIN para manejar datos macro faltantes
--    - COALESCE con valores por defecto
--    - Agregaciones optimizadas con índices de silver

-- 3. KPIs CALCULADOS:
--    - Tasa de mora: deudaVencida / deudaCapital
--    - Cobertura de garantía: valorGarantia / deudaCapital
--    - Proporción provisionada: valorProvision / deudaCapital

-- 4. ENRIQUECIMIENTO MACRO:
--    - Inflación promedio del mes
--    - Tipo de cambio promedio (compra/venta)
--    - JOIN por período normalizado a mes

-- 5. MONITOREO:
--    - Verificar filas affected en MERGE
--    - Validar consistencia temporal silver vs gold
--    - Alertar si faltan datos macroeconómicos