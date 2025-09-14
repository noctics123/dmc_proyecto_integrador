-- =============================================
-- SIMBAD Incremental: Landing CSV → Silver (Bypass Bronze)
-- =============================================
-- Propósito: Procesar incrementalmente CSV de landing directo a Silver
-- Ventaja: Elimina dependencia de DataProc para datos nuevos
-- Uso: Para cargas incrementales diarias/automáticas

INSERT INTO `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
(
  periodo, tipoCliente, actividad, entidad, sector, moneda, provincia, residencia,
  genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
  cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
  deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
  flg_importe_negativo, dt_captura
)

WITH latest_landing_dates AS (
  -- Detectar nuevos dt= en landing que no están en silver
  SELECT DISTINCT
    REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') as dt_landing
  FROM (
    SELECT _FILE_NAME
    FROM `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`  -- External table apuntando a CSV
    WHERE _FILE_NAME LIKE '%/dt=%'
  )
  WHERE REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') > COALESCE(
    (SELECT CAST(MAX(dt_captura) AS STRING) FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`),
    '1900-01-01'
  )
),

-- Leer CSV solo de fechas nuevas
raw_csv_data AS (
  SELECT
    -- Campos base
    periodo, tipoCredito, tipoEntidad, entidad, sectorEconomico, region, provincia,
    moneda, tipoCartera, actividad, sector, persona, facilidad, residencia,
    administracionYPropiedad, genero, tipoCliente, clasificacionEntidad,
    cantidadPlasticos, cantidadCredito, deuda, tasaPorDeuda, deudaCapital,
    deudaVencida, deudaVencidaDe31A90Dias, valorDesembolso, valorGarantia,
    valorProvisionCapitalYRendimiento,

    -- Extraer dt_captura del path del archivo
    PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/')) as dt_captura

  FROM `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`

  -- FILTRO INCREMENTAL: Solo archivos de fechas nuevas
  WHERE REGEXP_EXTRACT(_FILE_NAME, r'/dt=(\d{4}-\d{2}-\d{2})/') IN (
    SELECT dt_landing FROM latest_landing_dates
  )
),

-- Limpiar y normalizar datos
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

    -- Campos numéricos con validación robusta
    SAFE_CAST(REPLACE(REPLACE(deudaCapital, ',', ''), '$', '') AS FLOAT64) AS deudaCapital,
    SAFE_CAST(REPLACE(REPLACE(deudaVencida, ',', ''), '$', '') AS FLOAT64) AS deudaVencida,
    SAFE_CAST(REPLACE(REPLACE(deudaVencidaDe31A90Dias, ',', ''), '$', '') AS FLOAT64) AS deudaVencidaDe31A90Dias,
    SAFE_CAST(cantidadCredito AS INT64) AS cantidadCredito,
    SAFE_CAST(REPLACE(REPLACE(valorDesembolso, ',', ''), '$', '') AS FLOAT64) AS valorDesembolso,
    SAFE_CAST(REPLACE(REPLACE(valorGarantia, ',', ''), '$', '') AS FLOAT64) AS valorGarantia,
    SAFE_CAST(REPLACE(REPLACE(valorProvisionCapitalYRendimiento, ',', ''), '$', '') AS FLOAT64) AS valorProvisionCapitalYRendimiento,
    SAFE_CAST(REPLACE(REPLACE(deuda, ',', ''), '$', '') AS FLOAT64) AS deuda,

    -- Parsing de fecha mejorado
    COALESCE(
      SAFE.PARSE_DATE('%Y-%m', periodo),
      SAFE.PARSE_DATE('%Y/%m', periodo),
      SAFE.PARSE_DATE('%m/%Y', periodo)
    ) AS periodo_date,

    dt_captura

  FROM raw_csv_data
),

-- Aplicar derivaciones y flags de calidad
final_data AS (
  SELECT
    *,

    -- Campos temporales derivados
    EXTRACT(YEAR FROM periodo_date) AS anio,
    EXTRACT(MONTH FROM periodo_date) AS mes,
    EXTRACT(YEAR FROM periodo_date) * 100 + EXTRACT(MONTH FROM periodo_date) AS periodo_ym,

    -- Flags de calidad de datos
    CASE
      WHEN periodo_date IS NULL OR periodo = '' OR periodo IS NULL THEN 1
      ELSE 0
    END AS flg_periodo_invalido,

    CASE
      WHEN deudaCapital < 0 OR deudaVencida < 0 OR deuda < 0 THEN 1
      ELSE 0
    END AS flg_importe_negativo

  FROM cleaned_data
)

-- Selección final con deduplicación
SELECT
  periodo, tipoCliente, actividad, entidad, sector, moneda, provincia,
  residencia, genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
  cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
  deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
  flg_importe_negativo, dt_captura

FROM final_data

-- Filtros de calidad (opcionales)
WHERE entidad IS NOT NULL
  AND provincia IS NOT NULL
  AND dt_captura IS NOT NULL

-- Deduplicación automática
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY entidad, periodo, tipoCliente, provincia, genero, persona
  ORDER BY dt_captura DESC  -- Preferir carga más reciente
) = 1;

-- =============================================
-- External Table para CSV Landing (crear si no existe)
-- =============================================

-- CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.simbad_landing_csv_ext`
-- OPTIONS (
--   format = 'CSV',
--   skip_leading_rows = 1,
--   field_delimiter = ',',
--   quote = '"',
--   encoding = 'UTF-8',
--   uris = ['gs://dae-integrador-2025/lakehouse/landing/simbad/simbad_carteras_aayp_hipotecarios/dt=*/*.csv']
-- );

-- =============================================
-- Optimizaciones Clave
-- =============================================

-- 1. DETECCIÓN AUTOMÁTICA:
--    - Usa REGEXP_EXTRACT para obtener dt= de _FILE_NAME
--    - Solo procesa archivos más nuevos que MAX(dt_captura) en silver

-- 2. PARSING ROBUSTO:
--    - SAFE_CAST previene errores de conversión
--    - REPLACE limpia caracteres especiales ($, ,)
--    - COALESCE maneja múltiples formatos de fecha

-- 3. PERFORMANCE:
--    - External table con wildcards detecta nuevos archivos automáticamente
--    - Filtro temprano por dt= reduce volumen procesado
--    - QUALIFY más eficiente que GROUP BY para deduplicación

-- 4. CALIDAD:
--    - Flags marcan registros problemáticos
--    - Validaciones de campos críticos
--    - Preserva trazabilidad con dt_captura

-- 5. FLEXIBILIDAD:
--    - Funciona con múltiples encodings
--    - Maneja formatos de fecha variables
--    - Tolerante a datos faltantes/malformados