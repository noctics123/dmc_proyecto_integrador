-- =============================================
-- Bronze → Silver: SIMBAD Data Cleaning
-- =============================================
-- Propósito: Limpiar, normalizar y validar datos SIMBAD desde bronze hacia silver
-- Patrón: Procesar solo nuevos dt_captura (incremental)

-- Insertar/Actualizar datos desde bronze hacia silver
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

    -- Derivación de fecha y validaciones
    SAFE.PARSE_DATE('%Y-%m', periodo) AS periodo_date,
    dt_captura

  FROM `proyecto-integrador-dae-2025.bronze.simbad_bronze_parquet_ext`

  -- FILTRO INCREMENTAL: Solo procesar nuevos dt_captura
  WHERE dt_captura > COALESCE(
    (SELECT MAX(dt_captura) FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`),
    '1900-01-01'
  )
),

-- Aplicar transformaciones y flags de calidad
cleaned_data AS (
  SELECT
    *,

    -- Derivar campos temporales
    EXTRACT(YEAR FROM periodo_date) AS anio,
    EXTRACT(MONTH FROM periodo_date) AS mes,
    EXTRACT(YEAR FROM periodo_date) * 100 + EXTRACT(MONTH FROM periodo_date) AS periodo_ym,

    -- Flags de calidad de datos
    CASE
      WHEN periodo_date IS NULL OR periodo = '' THEN 1
      ELSE 0
    END AS flg_periodo_invalido,

    CASE
      WHEN deudaCapital < 0 OR deudaVencida < 0 OR deuda < 0 THEN 1
      ELSE 0
    END AS flg_importe_negativo

  FROM bronze_data
)

-- Selección final con filtros de calidad
SELECT
  periodo, tipoCliente, actividad, entidad, sector, moneda, provincia,
  residencia, genero, persona, deudaCapital, deudaVencida, deudaVencidaDe31A90Dias,
  cantidadCredito, valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento,
  deuda, periodo_date, anio, mes, periodo_ym, flg_periodo_invalido,
  flg_importe_negativo, dt_captura

FROM cleaned_data

-- Filtros de calidad (opcional - comentar si se quiere preservar todo)
-- WHERE flg_periodo_invalido = 0  -- Solo períodos válidos
--   AND entidad IS NOT NULL       -- Solo registros con entidad
--   AND provincia IS NOT NULL     -- Solo registros con provincia

-- Deduplicación por entidad + período + otros campos clave
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY entidad, periodo, tipoCliente, provincia, genero, persona
  ORDER BY dt_captura DESC  -- En caso de duplicados, tomar el más reciente
) = 1;

-- =============================================
-- Notas de Optimización
-- =============================================

-- 1. INCREMENTAL LOADING:
--    - Solo procesa dt_captura nuevos
--    - Usa COALESCE para manejar tabla vacía inicial

-- 2. CALIDAD DE DATOS:
--    - SAFE_CAST previene errores de conversión
--    - Flags marcan registros problemáticos
--    - QUALIFY elimina duplicados automáticamente

-- 3. PERFORMANCE:
--    - Índices: Tabla clustered por entidad, tipoCliente
--    - Filtros tempranos reducen volumen procesado
--    - ROW_NUMBER es más eficiente que GROUP BY para dedup

-- 4. MONITOREO:
--    - Contar registros con flags = 1 para alertas
--    - Verificar dt_captura máximo procesado
--    - Validar rangos de fecha período vs dt_captura