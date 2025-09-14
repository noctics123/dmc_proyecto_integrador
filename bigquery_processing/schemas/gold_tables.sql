-- =============================================
-- Gold Layer Tables - Métricas y KPIs de Negocio
-- =============================================

-- SIMBAD Gold - Métricas Agregadas por Provincia/Período
CREATE OR REPLACE TABLE `proyecto-integrador-dae-2025.gold.simbad_gold`
(
  -- Dimensiones temporales
  periodo_date DATE,
  ANIO INT64,
  MES INT64,

  -- Dimensión geográfica
  PROVINCIA STRING,

  -- Métricas Financieras Básicas
  DEUDACAPITAL FLOAT64,
  DEUDAVENCIDA FLOAT64,
  DEUDAVENCIDADE31A90DIAS FLOAT64,
  CANTIDADCREDITO INT64,
  VALORDESEMBOLSO FLOAT64,
  VALORGARANTIA FLOAT64,
  VALORPROVISIONCAPITALYRENDIMIENTO FLOAT64,
  DEUDA FLOAT64,

  -- Métricas por Dimensión (conteos únicos)
  GENERO FLOAT64,           -- Cantidad de géneros distintos
  PERSONA FLOAT64,          -- Cantidad de tipos persona
  MONEDA FLOAT64,           -- Cantidad de monedas
  SECTOR FLOAT64,           -- Cantidad de sectores
  ENTIDAD FLOAT64,          -- Cantidad de entidades
  RESIDENCIA FLOAT64,       -- Cantidad de tipos residencia

  -- KPIs Calculados
  TASA_MORA FLOAT64,                     -- deudaVencida / deudaCapital
  COBERTURA_GARANTIA FLOAT64,           -- valorGarantia / deudaCapital
  PROPORCION_PROVISIONADA FLOAT64,       -- valorProvision / deudaCapital

  -- Métricas de Concentración
  PD_AGREGADA INT64,                     -- Indicador agregado

  -- Datos Macroeconómicos (enriquecimiento)
  INFLACION FLOAT64,
  TC_VENTA FLOAT64,
  TC_COMPRA FLOAT64
)
PARTITION BY DATE_TRUNC(periodo_date, MONTH)
CLUSTER BY PROVINCIA;

-- =============================================
-- Alertas y Tableros
-- =============================================

-- Tabla de Alertas por Provincia (12 meses)
CREATE OR REPLACE TABLE `proyecto-integrador-dae-2025.gold.alertas_provincia_12m`
(
  provincia STRING,
  anio INT64,
  mes INT64,
  tasa_mora_promedio FLOAT64,
  tendencia_mora STRING,      -- 'SUBIENDO', 'BAJANDO', 'ESTABLE'
  nivel_alerta STRING,        -- 'ALTO', 'MEDIO', 'BAJO'
  entidades_afectadas INT64,
  fecha_calculo DATE
);

-- Tabla de Alertas para Looker (con particionado temporal)
CREATE OR REPLACE TABLE `proyecto-integrador-dae-2025.gold.alertas_provincia_12m_looker`
(
  PROVINCIA STRING,
  ANIO INT64,
  MES INT64,
  periodo_date DATE,
  tasa_mora FLOAT64,
  variacion_mensual FLOAT64,
  percentil_nacional FLOAT64,
  clasificacion_riesgo STRING
)
PARTITION BY DATE_TRUNC(periodo_date, DAY)
CLUSTER BY PROVINCIA, ANIO, MES;

-- =============================================
-- Optimizaciones y Notas
-- =============================================

-- Particionado por MONTH en simbad_gold:
-- - Mejora queries por rango de fechas
-- - Facilita mantenimiento de datos históricos
-- - Optimiza agregaciones temporales

-- Clustering por PROVINCIA:
-- - Optimiza queries por región
-- - Mejora performance en reportes geográficos
-- - Acelera JOINs por ubicación

-- Ejemplo de query optimizada:
-- SELECT PROVINCIA, SUM(DEUDACAPITAL), AVG(TASA_MORA)
-- FROM gold.simbad_gold
-- WHERE periodo_date BETWEEN '2024-01-01' AND '2024-12-31'
--   AND PROVINCIA IN ('SANTO DOMINGO', 'SANTIAGO')
-- GROUP BY PROVINCIA;