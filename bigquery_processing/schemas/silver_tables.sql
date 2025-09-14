-- =============================================
-- Silver Layer Tables - Datos Limpios y Normalizados
-- =============================================

-- SIMBAD Silver Table (clustered para mejor performance)
CREATE OR REPLACE TABLE `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
(
  -- Campos originales (limpios)
  periodo STRING,
  tipoCliente STRING,
  actividad STRING,
  entidad STRING,
  sector STRING,
  moneda STRING,
  provincia STRING,
  residencia STRING,
  genero STRING,
  persona STRING,

  -- Campos numéricos
  deudaCapital FLOAT64,
  deudaVencida FLOAT64,
  deudaVencidaDe31A90Dias FLOAT64,
  cantidadCredito INT64,
  valorDesembolso FLOAT64,
  valorGarantia FLOAT64,
  valorProvisionCapitalYRendimiento FLOAT64,
  deuda FLOAT64,

  -- Campos derivados
  periodo_date DATE,
  anio INT64,
  mes INT64,
  periodo_ym INT64,              -- YYYYMM para ordenamiento

  -- Flags de calidad
  flg_periodo_invalido INT64,    -- 1 si período no parseó correctamente
  flg_importe_negativo INT64,    -- 1 si hay importes negativos

  -- Metadatos
  dt_captura DATE                -- Fecha de carga desde bronze
)
CLUSTER BY entidad, tipoCliente;

-- =============================================
-- Índices y Optimizaciones
-- =============================================

-- Clustering por entidad y tipoCliente mejora:
-- 1. Queries por entidad específica
-- 2. Agregaciones por tipo de cliente
-- 3. Performance en JOINs

-- Recomendaciones adicionales:
-- - Particionar por dt_captura si hay múltiples cargas
-- - Considerar clustering adicional por provincia si se consulta frecuentemente

-- Ejemplo de uso:
-- SELECT entidad, provincia, SUM(deudaCapital)
-- FROM silver_clean.simbad_hipotecarios
-- WHERE entidad = 'BANCO_XYZ'
-- GROUP BY entidad, provincia;